package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"pkt.systems/lockd/internal/storage"
)

func normalizeETag(etag string) string {
	return strings.Trim(strings.TrimSpace(etag), "\"")
}

// UpdateCommand captures state write parameters.
type UpdateCommand struct {
	Namespace      string
	Key            string
	LeaseID        string
	FencingToken   int64
	IfVersion      int64
	IfVersionSet   bool
	IfStateETag    string
	Body           io.Reader
	CompactWriter  func(io.Writer, io.Reader, int64) error
	MaxBytes       int64
	SpoolThreshold int64
	KnownMeta      *storage.Meta
	KnownMetaETag  string
}

// UpdateResult describes the new state metadata after a successful update.
type UpdateResult struct {
	NewVersion   int64
	NewStateETag string
	Bytes        int64
	Meta         *storage.Meta
	MetaETag     string
	Metadata     map[string]any
	Namespace    string
	Key          string
}

// Update streams new state into storage with lease/CAS enforcement.
func (s *Service) Update(ctx context.Context, cmd UpdateCommand) (*UpdateResult, error) {
	if err := s.maybeThrottleLock(); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.Key) == "" {
		return nil, Failure{Code: "missing_key", Detail: "key query required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "X-Lease-ID required", HTTPStatus: http.StatusBadRequest}
	}
	if cmd.Body == nil {
		return nil, Failure{Code: "missing_body", Detail: "state body required", HTTPStatus: http.StatusBadRequest}
	}

	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)

	// Enforce optional payload size limit
	limited := cmd.Body
	if cmd.MaxBytes > 0 {
		limited = io.LimitReader(cmd.Body, cmd.MaxBytes+1)
	}

	// Compact and spill to disk if needed
	spool := newPayloadSpool(cmd.SpoolThreshold)
	defer spool.Close()
	if err := cmd.CompactWriter(spool, limited, cmd.MaxBytes); err != nil {
		var fail Failure
		if errors.As(err, &fail) {
			return nil, fail
		}
		return nil, httpErrorFrom(err)
	}
	reader, err := spool.Reader()
	if err != nil {
		return nil, err
	}

	for {
		now := s.clock.Now()
		meta, metaETag, err := s.loadMetaMaybeCached(ctx, namespace, storageKey, cmd.KnownMeta, cmd.KnownMetaETag)
		if err != nil {
			return nil, err
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, now); err != nil {
			return nil, err
		}
		if cmd.IfVersionSet && meta.Version != cmd.IfVersion {
			return nil, Failure{
				Code:       "version_conflict",
				Detail:     "state version mismatch",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}
		if cmd.IfStateETag != "" && normalizeETag(meta.StateETag) != normalizeETag(cmd.IfStateETag) {
			return nil, Failure{
				Code:       "etag_mismatch",
				Detail:     "current state etag does not match",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}

		stateCtx := storage.ContextWithStateDescriptor(ctx, meta.StateDescriptor)
		result, err := s.store.WriteState(stateCtx, namespace, keyComponent, reader, storage.PutStateOptions{
			ExpectedETag: meta.StateETag,
			Descriptor:   meta.StateDescriptor,
		})
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, fmt.Errorf("write state: %w", err)
		}

		meta.StateETag = result.NewETag
		meta.StateDescriptor = result.Descriptor
		meta.StatePlaintextBytes = result.BytesWritten
		meta.Version++
		meta.PublishedVersion = meta.Version
		meta.UpdatedAtUnix = now.Unix()
		if meta.FencingToken == 0 {
			meta.FencingToken = cmd.FencingToken
		}

		newMetaETag, err := s.store.StoreMeta(ctx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, fmt.Errorf("store meta: %w", err)
		}
		res := &UpdateResult{
			NewVersion:   meta.Version,
			NewStateETag: meta.StateETag,
			Bytes:        result.BytesWritten,
			Meta:         meta,
			MetaETag:     newMetaETag,
			Metadata:     map[string]any{},
			Namespace:    namespace,
			Key:          cmd.Key,
		}
		if s.indexManager != nil {
			if rdr, rerr := spool.Reader(); rerr == nil {
				if doc, derr := buildDocumentFromJSON(keyComponent, rdr); derr == nil {
					_ = s.indexManager.Insert(namespace, doc)
				}
			}
		}
		return res, nil
	}
}

// Remove deletes state and meta if lease matches.
func (s *Service) Remove(ctx context.Context, cmd RemoveCommand) (*RemoveResult, error) {
	if err := s.maybeThrottleLock(); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.Key) == "" {
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
	}
	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)

	for {
		now := s.clock.Now()
		meta, metaETag, err := s.loadMetaMaybeCached(ctx, namespace, storageKey, cmd.KnownMeta, cmd.KnownMetaETag)
		if err != nil {
			return nil, err
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, now); err != nil {
			return nil, err
		}
		if cmd.IfVersionSet && meta.Version != cmd.IfVersion {
			return nil, Failure{
				Code:       "version_conflict",
				Detail:     "state version mismatch",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}
		if cmd.IfStateETag != "" && normalizeETag(meta.StateETag) != normalizeETag(cmd.IfStateETag) {
			return nil, Failure{
				Code:       "etag_mismatch",
				Detail:     "state etag mismatch",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}

		hadState := meta.StateETag != ""
		expectedETag := cmd.IfStateETag
		if expectedETag != "" && normalizeETag(expectedETag) == normalizeETag(meta.StateETag) {
			// Preserve backend-specific formatting (Azure includes quotes).
			expectedETag = meta.StateETag
		}

		removeErr := s.store.Remove(ctx, namespace, keyComponent, expectedETag)
		removed := false
		switch {
		case removeErr == nil:
			removed = true
		case errors.Is(removeErr, storage.ErrNotFound):
			removed = false
		case errors.Is(removeErr, storage.ErrCASMismatch):
			return nil, Failure{
				Code:       "etag_mismatch",
				Detail:     "state etag mismatch",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		default:
			return nil, fmt.Errorf("remove state: %w", removeErr)
		}

		// Only bump version / clear state when we actually removed something or state existed.
		if removed || hadState {
			meta.Version++
			meta.UpdatedAtUnix = now.Unix()
			meta.StateETag = ""
			meta.StateDescriptor = nil
			meta.StatePlaintextBytes = 0
			meta.PublishedVersion = meta.Version
			newMetaETag, err := s.store.StoreMeta(ctx, namespace, keyComponent, meta, metaETag)
			if err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				return nil, fmt.Errorf("store meta: %w", err)
			}
			metaETag = newMetaETag
		}

		return &RemoveResult{
			Removed:    removed,
			NewVersion: meta.Version,
			Meta:       meta,
			MetaETag:   metaETag,
		}, nil
	}
}

// Metadata mutates metadata attributes guarded by fencing.
func (s *Service) Metadata(ctx context.Context, cmd MetadataCommand) (*MetadataResult, error) {
	if err := s.maybeThrottleLock(); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.Key) == "" {
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
	}
	if cmd.Mutation.empty() {
		return nil, Failure{Code: "invalid_body", Detail: "no metadata fields provided", HTTPStatus: http.StatusBadRequest}
	}

	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)

	for {
		now := s.clock.Now()
		meta, metaETag, err := s.loadMetaMaybeCached(ctx, namespace, storageKey, cmd.KnownMeta, cmd.KnownMetaETag)
		if err != nil {
			return nil, err
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, now); err != nil {
			return nil, err
		}
		if cmd.IfVersionSet && meta.Version != cmd.IfVersion {
			return nil, Failure{
				Code:       "version_conflict",
				Detail:     "state version mismatch",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}
		if cmd.IfStateETag != "" && normalizeETag(meta.StateETag) != normalizeETag(cmd.IfStateETag) {
			return nil, Failure{
				Code:       "etag_mismatch",
				Detail:     "state etag mismatch",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}
		cmd.Mutation.apply(meta)
		meta.UpdatedAtUnix = now.Unix()
		meta.Version++
		meta.PublishedVersion = meta.Version
		newMetaETag, err := s.store.StoreMeta(ctx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, fmt.Errorf("store meta: %w", err)
		}
		return &MetadataResult{
			Version:  meta.Version,
			Meta:     meta,
			MetaETag: newMetaETag,
		}, nil
	}
}

// --- helpers for spooling / error mapping ---

type payloadSpool struct {
	buf       []byte
	file      *os.File
	threshold int64
}

func newPayloadSpool(threshold int64) *payloadSpool {
	if threshold <= 0 {
		threshold = 4 << 20
	}
	return &payloadSpool{threshold: threshold}
}

func (p *payloadSpool) Write(data []byte) (int, error) {
	if p.file != nil {
		return p.file.Write(data)
	}
	if int64(len(p.buf)+len(data)) <= p.threshold {
		p.buf = append(p.buf, data...)
		return len(data), nil
	}
	f, err := os.CreateTemp("", "lockd-spool-*.tmp")
	if err != nil {
		return 0, err
	}
	if len(p.buf) > 0 {
		if _, err := f.Write(p.buf); err != nil {
			f.Close()
			os.Remove(f.Name())
			return 0, err
		}
	}
	p.file = f
	p.buf = nil
	return f.Write(data)
}

func (p *payloadSpool) Reader() (io.ReadSeeker, error) {
	if p.file != nil {
		if _, err := p.file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return p.file, nil
	}
	return strings.NewReader(string(p.buf)), nil
}

func (p *payloadSpool) Close() error {
	if p.file != nil {
		name := p.file.Name()
		err := p.file.Close()
		_ = os.Remove(name)
		p.file = nil
		return err
	}
	p.buf = nil
	return nil
}

func httpErrorFrom(err error) Failure {
	return Failure{Code: "invalid_body", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
}
