package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
)

const (
	indexRebuildLockKey         = "index/rebuild/lock.json"
	indexRebuildLockMaxAge      = 2 * time.Hour
	indexRebuildCleanupDelay    = 30 * time.Second
	indexRebuildManifestRetries = 5
	indexRebuildManifestDelay   = 25 * time.Millisecond
)

var errIndexRebuildInProgress = errors.New("index rebuild already in progress")

type indexRebuildState struct {
	running bool
	waiters []chan IndexRebuildResult
}

type indexRebuildLock struct {
	Owner     string    `json:"owner"`
	StartedAt time.Time `json:"started_at"`
}

// RebuildIndex rebuilds the namespace index in-place using the current backend.
func (s *Service) RebuildIndex(ctx context.Context, namespace string, opts IndexRebuildOptions) (IndexRebuildResult, error) {
	if s == nil {
		return IndexRebuildResult{}, fmt.Errorf("index rebuild: service unavailable")
	}
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = strings.TrimSpace(s.defaultNamespace)
	}
	if ns == "" {
		return IndexRebuildResult{}, fmt.Errorf("index rebuild: namespace required")
	}
	mode := strings.ToLower(strings.TrimSpace(opts.Mode))
	if mode == "" {
		mode = "wait"
	}
	result := IndexRebuildResult{Namespace: ns, Mode: mode}
	if s.indexManager == nil || s.store == nil {
		return result, storage.ErrNotImplemented
	}

	switch mode {
	case "async":
		s.indexRebuildMu.Lock()
		state := s.indexRebuildStateLocked(ns)
		if state.running {
			s.indexRebuildMu.Unlock()
			result.Pending = true
			return result, nil
		}
		state.running = true
		s.indexRebuildMu.Unlock()
		opts.Mode = "async"
		go s.runIndexRebuildAsync(ns, opts)
		result.Accepted = true
		result.Pending = true
		return result, nil
	case "wait":
		s.indexRebuildMu.Lock()
		state := s.indexRebuildStateLocked(ns)
		if state.running {
			ch := make(chan IndexRebuildResult, 1)
			state.waiters = append(state.waiters, ch)
			s.indexRebuildMu.Unlock()
			select {
			case res := <-ch:
				return res, nil
			case <-ctx.Done():
				return result, ctx.Err()
			}
		}
		state.running = true
		s.indexRebuildMu.Unlock()
		result, err := s.runIndexRebuild(ctx, ns, opts)
		s.finishIndexRebuild(ns, result)
		return result, err
	default:
		return result, fmt.Errorf("index rebuild: unsupported mode %q (expected wait or async)", mode)
	}
}

func (s *Service) maybeScheduleIndexRebuild(ctx context.Context, namespace string, format uint32) (bool, error) {
	if s == nil || s.indexManager == nil {
		return false, nil
	}
	if format >= indexer.IndexFormatVersionV4 {
		return false, nil
	}
	opts := IndexRebuildOptions{
		Mode:         "wait",
		Cleanup:      true,
		CleanupDelay: 0,
	}
	res, err := s.RebuildIndex(ctx, namespace, opts)
	if err != nil {
		return false, err
	}
	return res.Rebuilt, nil
}

func (s *Service) runIndexRebuildAsync(namespace string, opts IndexRebuildOptions) {
	res, err := s.runIndexRebuild(context.Background(), namespace, opts)
	if err != nil && s.logger != nil {
		s.logger.Warn("index.rebuild.async.error", "namespace", namespace, "error", err)
	}
	s.finishIndexRebuild(namespace, res)
}

func (s *Service) finishIndexRebuild(namespace string, result IndexRebuildResult) {
	s.indexRebuildMu.Lock()
	state := s.indexRebuildStateLocked(namespace)
	state.running = false
	waiters := state.waiters
	state.waiters = nil
	s.indexRebuildMu.Unlock()
	for _, ch := range waiters {
		ch <- result
		close(ch)
	}
}

func (s *Service) indexRebuildStateLocked(namespace string) *indexRebuildState {
	if s.indexRebuilds == nil {
		s.indexRebuilds = make(map[string]*indexRebuildState)
	}
	state := s.indexRebuilds[namespace]
	if state == nil {
		state = &indexRebuildState{}
		s.indexRebuilds[namespace] = state
	}
	return state
}

func (s *Service) runIndexRebuild(ctx context.Context, namespace string, opts IndexRebuildOptions) (IndexRebuildResult, error) {
	result := IndexRebuildResult{Namespace: namespace, Mode: strings.ToLower(strings.TrimSpace(opts.Mode))}
	if result.Mode == "" {
		result.Mode = "wait"
	}
	if s.indexManager == nil || s.store == nil {
		return result, storage.ErrNotImplemented
	}
	release, err := s.acquireIndexRebuildLock(ctx, namespace)
	if err != nil {
		if errors.Is(err, errIndexRebuildInProgress) {
			result.Pending = true
			return result, nil
		}
		return result, err
	}
	defer release()

	targetFormat, err := s.indexRebuildTargetFormat(ctx, namespace)
	if err != nil {
		return result, err
	}

	if opts.Reset {
		if err := s.resetIndexArtifacts(ctx, namespace); err != nil {
			return result, err
		}
		if err := s.bootstrapIndexManifest(ctx, namespace, targetFormat); err != nil {
			return result, err
		}
	} else {
		if err := s.ensureIndexManifestFormat(ctx, namespace, targetFormat); err != nil {
			return result, err
		}
	}

	keys, err := s.store.ListMetaKeys(ctx, namespace)
	if err != nil {
		return result, err
	}
	sort.Strings(keys)

	var hardErr error
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		metaRes, err := s.store.LoadMeta(ctx, namespace, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			hardErr = err
			break
		}
		meta := metaRes.Meta
		if meta == nil {
			continue
		}
		visible := meta.PublishedVersion > 0 && !meta.QueryExcluded()
		if err := s.indexManager.UpdateVisibility(namespace, key, visible); err != nil && hardErr == nil {
			hardErr = err
		}
		if meta.StateETag == "" {
			continue
		}
		stateCtx := ctx
		if len(meta.StateDescriptor) > 0 {
			stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
		}
		if meta.StatePlaintextBytes > 0 {
			stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
		}
		stateRes, err := s.store.ReadState(stateCtx, namespace, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if hardErr == nil {
				hardErr = err
			}
			continue
		}
		reader := stateRes.Reader
		info := stateRes.Info
		if reader == nil {
			continue
		}
		if info != nil && info.ETag != "" && meta.StateETag != "" && info.ETag != meta.StateETag {
			reader.Close()
			continue
		}
		doc, err := buildDocumentFromJSON(key, reader)
		_ = reader.Close()
		if err != nil {
			if hardErr == nil {
				hardErr = err
			}
			continue
		}
		if docMeta := buildIndexDocumentMeta(meta); docMeta != nil {
			doc.Meta = docMeta
		}
		if err := s.indexManager.Insert(namespace, doc); err != nil && hardErr == nil {
			hardErr = err
		}
	}

	if err := s.indexManager.FlushNamespace(ctx, namespace); err != nil {
		return result, err
	}

	seq, _ := s.indexManager.ManifestSeq(ctx, namespace)
	result.IndexSeq = seq
	result.Rebuilt = true

	if opts.Cleanup && hardErr == nil {
		delay := opts.CleanupDelay
		if delay < 0 {
			delay = 0
		}
		if err := s.cleanupLegacySegments(ctx, namespace, delay, targetFormat); err != nil && hardErr == nil {
			hardErr = err
		}
	}

	if hardErr != nil {
		return result, hardErr
	}
	return result, nil
}

func (s *Service) indexRebuildTargetFormat(ctx context.Context, namespace string) (uint32, error) {
	if s == nil || s.indexManager == nil {
		return indexer.IndexFormatVersionV4, nil
	}
	manifestRes, err := s.indexManager.LoadManifest(ctx, namespace)
	if err != nil {
		return 0, err
	}
	manifest := manifestRes.Manifest
	if manifest == nil || manifest.Format < indexer.IndexFormatVersionV4 {
		return indexer.IndexFormatVersionV4, nil
	}
	return manifest.Format, nil
}

func (s *Service) ensureIndexManifestFormat(ctx context.Context, namespace string, targetFormat uint32) error {
	if s == nil || s.indexManager == nil {
		return storage.ErrNotImplemented
	}
	if targetFormat < indexer.IndexFormatVersionV4 {
		targetFormat = indexer.IndexFormatVersionV4
	}
	var lastErr error
	for attempt := 0; attempt < indexRebuildManifestRetries; attempt++ {
		manifestRes, err := s.indexManager.LoadManifest(ctx, namespace)
		if err != nil {
			return err
		}
		manifest := manifestRes.Manifest
		etag := manifestRes.ETag
		if manifest == nil {
			manifest = indexer.NewManifest()
		}
		changed := false
		if manifest.Format != targetFormat {
			manifest.Format = targetFormat
			manifest.Seq++
			manifest.UpdatedAt = time.Now().UTC()
			changed = true
		}
		if !changed && etag != "" {
			return nil
		}
		if _, err := s.indexManager.SaveManifest(ctx, namespace, manifest, etag); err == nil {
			return nil
		}
		lastErr = err
		if !errors.Is(err, storage.ErrCASMismatch) {
			return err
		}
		if attempt+1 < indexRebuildManifestRetries {
			s.clock.Sleep(indexRebuildManifestDelay)
		}
	}
	return lastErr
}

func (s *Service) bootstrapIndexManifest(ctx context.Context, namespace string, targetFormat uint32) error {
	if s == nil || s.indexManager == nil {
		return storage.ErrNotImplemented
	}
	if targetFormat < indexer.IndexFormatVersionV4 {
		targetFormat = indexer.IndexFormatVersionV4
	}
	manifest := indexer.NewManifest()
	manifest.Format = targetFormat
	manifest.Shards[0] = &indexer.Shard{ID: 0}
	_, err := s.indexManager.SaveManifest(ctx, namespace, manifest, "")
	return err
}

func (s *Service) acquireIndexRebuildLock(ctx context.Context, namespace string) (func(), error) {
	if s.store == nil {
		return nil, storage.ErrNotImplemented
	}
	payload, contentType, err := s.encodeIndexRebuildLock()
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 2; attempt++ {
		info, err := s.store.PutObject(ctx, namespace, indexRebuildLockKey, bytes.NewReader(payload), storage.PutObjectOptions{
			IfNotExists: true,
			ContentType: contentType,
		})
		if err == nil {
			release := func() {
				_ = s.store.DeleteObject(context.Background(), namespace, indexRebuildLockKey, storage.DeleteObjectOptions{
					ExpectedETag:   info.ETag,
					IgnoreNotFound: true,
				})
			}
			return release, nil
		}
		if !errors.Is(err, storage.ErrCASMismatch) {
			return nil, err
		}
		stale, etag, err := s.indexRebuildLockStale(ctx, namespace)
		if err != nil {
			return nil, err
		}
		if !stale {
			return nil, errIndexRebuildInProgress
		}
		_ = s.store.DeleteObject(ctx, namespace, indexRebuildLockKey, storage.DeleteObjectOptions{ExpectedETag: etag, IgnoreNotFound: true})
	}
	return nil, errIndexRebuildInProgress
}

func (s *Service) encodeIndexRebuildLock() ([]byte, string, error) {
	owner := indexRebuildOwner()
	lock := indexRebuildLock{
		Owner:     owner,
		StartedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(lock)
	if err != nil {
		return nil, "", err
	}
	contentType := storage.ContentTypeJSON
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.EncryptMetadata(payload)
		if err != nil {
			return nil, "", err
		}
		contentType = storage.ContentTypeJSONEncrypted
	}
	return payload, contentType, nil
}

func (s *Service) indexRebuildLockStale(ctx context.Context, namespace string) (bool, string, error) {
	obj, err := s.store.GetObject(ctx, namespace, indexRebuildLockKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return true, "", nil
		}
		return false, "", err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		return false, "", err
	}
	etag := ""
	if obj.Info != nil {
		etag = obj.Info.ETag
	}
	if obj.Info != nil && obj.Info.ContentType == storage.ContentTypeJSONEncrypted {
		if s.crypto == nil || !s.crypto.Enabled() {
			return false, "", fmt.Errorf("index rebuild lock encrypted but crypto disabled")
		}
		payload, err = s.crypto.DecryptMetadata(payload)
		if err != nil {
			return false, "", err
		}
	}
	var lock indexRebuildLock
	if err := json.Unmarshal(payload, &lock); err != nil {
		return false, "", err
	}
	if lock.StartedAt.IsZero() {
		return false, etag, nil
	}
	stale := time.Since(lock.StartedAt) > indexRebuildLockMaxAge
	return stale, etag, nil
}

func indexRebuildOwner() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), runtime.Version())
}

func (s *Service) resetIndexArtifacts(ctx context.Context, namespace string) error {
	if s.store == nil {
		return storage.ErrNotImplemented
	}
	startAfter := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		list, err := s.store.ListObjects(ctx, namespace, storage.ListOptions{
			Prefix:     "index/",
			StartAfter: startAfter,
			Limit:      500,
		})
		if err != nil {
			return err
		}
		for _, obj := range list.Objects {
			if err := s.store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil {
				return err
			}
			startAfter = obj.Key
		}
		if !list.Truncated {
			break
		}
		if list.NextStartAfter != "" {
			startAfter = list.NextStartAfter
		}
	}
	return nil
}

func (s *Service) cleanupLegacySegments(ctx context.Context, namespace string, delay time.Duration, targetFormat uint32) error {
	if delay > 0 {
		if err := s.waitWithContext(ctx, delay); err != nil {
			return err
		}
	}
	if targetFormat < indexer.IndexFormatVersionV4 {
		targetFormat = indexer.IndexFormatVersionV4
	}
	legacyIDs, err := s.collectLegacySegmentIDs(ctx, namespace, targetFormat)
	if err != nil {
		return err
	}
	if len(legacyIDs) == 0 {
		return nil
	}
	var lastErr error
	for attempt := 0; attempt < indexRebuildManifestRetries; attempt++ {
		manifestRes, err := s.indexManager.LoadManifest(ctx, namespace)
		if err != nil {
			return err
		}
		manifest := manifestRes.Manifest
		etag := manifestRes.ETag
		if manifest == nil || len(manifest.Shards) == 0 {
			return nil
		}
		manifest.Format = targetFormat
		changed := false
		for _, shard := range manifest.Shards {
			if shard == nil || len(shard.Segments) == 0 {
				continue
			}
			filtered := shard.Segments[:0]
			for _, ref := range shard.Segments {
				if _, drop := legacyIDs[ref.ID]; drop {
					changed = true
					continue
				}
				filtered = append(filtered, ref)
			}
			shard.Segments = filtered
		}
		if !changed {
			lastErr = nil
			break
		}
		manifest.Seq++
		manifest.UpdatedAt = time.Now().UTC()
		if _, err := s.indexManager.SaveManifest(ctx, namespace, manifest, etag); err == nil {
			lastErr = nil
			break
		} else {
			lastErr = err
			if !errors.Is(err, storage.ErrCASMismatch) {
				return err
			}
			if attempt+1 < indexRebuildManifestRetries {
				s.clock.Sleep(indexRebuildManifestDelay)
			}
		}
	}
	if lastErr != nil {
		return lastErr
	}
	for id := range legacyIDs {
		_ = s.indexManager.DeleteSegment(ctx, namespace, id)
	}
	return nil
}

func (s *Service) collectLegacySegmentIDs(ctx context.Context, namespace string, targetFormat uint32) (map[string]struct{}, error) {
	manifestRes, err := s.indexManager.LoadManifest(ctx, namespace)
	if err != nil {
		return nil, err
	}
	manifest := manifestRes.Manifest
	if manifest == nil || len(manifest.Shards) == 0 {
		return map[string]struct{}{}, nil
	}
	legacy := make(map[string]struct{})
	seen := make(map[string]struct{})
	for _, shard := range manifest.Shards {
		if shard == nil {
			continue
		}
		for _, ref := range shard.Segments {
			if _, ok := seen[ref.ID]; ok {
				continue
			}
			seen[ref.ID] = struct{}{}
			segment, err := s.indexManager.LoadSegment(ctx, namespace, ref.ID)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				return nil, err
			}
			if segment != nil && segment.Format < targetFormat {
				legacy[ref.ID] = struct{}{}
			}
		}
	}
	return legacy, nil
}
