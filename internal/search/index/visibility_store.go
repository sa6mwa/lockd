package index

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

// VisibilityManifestResult captures a visibility manifest and its ETag.
type VisibilityManifestResult struct {
	Manifest *VisibilityManifest
	ETag     string
}

// LoadVisibilityManifest returns the visibility manifest (or an empty one when missing).
func (s *Store) LoadVisibilityManifest(ctx context.Context, namespace string) (VisibilityManifestResult, error) {
	if s == nil || s.backend == nil {
		return VisibilityManifestResult{Manifest: NewVisibilityManifest()}, nil
	}
	start := time.Now()
	logger := pslog.LoggerFromContext(ctx)
	if logger != nil {
		logger.Trace("index.store.load_visibility_manifest.begin", "namespace", namespace, "object", visibilityManifest)
	}
	obj, err := s.backend.GetObject(ctx, namespace, visibilityManifest)
	if err != nil {
		if err == storage.ErrNotFound {
			if logger != nil {
				logger.Trace("index.store.load_visibility_manifest.not_found", "namespace", namespace, "object", visibilityManifest, "elapsed", time.Since(start))
			}
			return VisibilityManifestResult{Manifest: NewVisibilityManifest()}, nil
		}
		if logger != nil {
			logger.Trace("index.store.load_visibility_manifest.error", "namespace", namespace, "object", visibilityManifest, "error", err, "elapsed", time.Since(start))
		}
		return VisibilityManifestResult{}, err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		if logger != nil {
			logger.Trace("index.store.load_visibility_manifest.read_error", "namespace", namespace, "object", visibilityManifest, "error", err, "elapsed", time.Since(start))
		}
		return VisibilityManifestResult{}, fmt.Errorf("read visibility manifest: %w", err)
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.DecryptMetadata(payload)
		if err != nil {
			if logger != nil {
				logger.Trace("index.store.load_visibility_manifest.decrypt_error", "namespace", namespace, "object", visibilityManifest, "error", err, "elapsed", time.Since(start))
			}
			return VisibilityManifestResult{}, fmt.Errorf("decrypt visibility manifest: %w", err)
		}
	}
	manifest := NewVisibilityManifest()
	if err := manifest.UnmarshalJSON(payload); err != nil {
		if logger != nil {
			logger.Trace("index.store.load_visibility_manifest.decode_error", "namespace", namespace, "object", visibilityManifest, "error", err, "elapsed", time.Since(start))
		}
		return VisibilityManifestResult{}, fmt.Errorf("decode visibility manifest: %w", err)
	}
	if logger != nil {
		logger.Trace("index.store.load_visibility_manifest.success", "namespace", namespace, "object", visibilityManifest, "segments", len(manifest.Segments), "elapsed", time.Since(start))
	}
	return VisibilityManifestResult{Manifest: manifest, ETag: obj.Info.ETag}, nil
}

// SaveVisibilityManifest writes the visibility manifest with optional CAS semantics.
func (s *Store) SaveVisibilityManifest(ctx context.Context, namespace string, manifest *VisibilityManifest, expectedETag string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("visibility store unavailable")
	}
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	payload, err := manifest.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("encode visibility manifest: %w", err)
	}
	contentType := storage.ContentTypeJSON
	if s.crypto != nil && s.crypto.Enabled() {
		contentType = storage.ContentTypeJSONEncrypted
		payload, err = s.crypto.EncryptMetadata(payload)
		if err != nil {
			return "", fmt.Errorf("encrypt visibility manifest: %w", err)
		}
	}
	info, err := s.backend.PutObject(ctx, namespace, visibilityManifest, bytes.NewReader(payload), storage.PutObjectOptions{
		ExpectedETag: expectedETag,
		ContentType:  contentType,
	})
	if err != nil {
		return "", err
	}
	if s.vis != nil {
		s.vis.set(namespace, nil)
	}
	return info.ETag, nil
}

// WriteVisibilitySegment writes a visibility segment and returns its ETag and size.
func (s *Store) WriteVisibilitySegment(ctx context.Context, namespace string, segment *VisibilitySegment) (string, int64, error) {
	if s == nil || s.backend == nil {
		return "", 0, fmt.Errorf("visibility store unavailable")
	}
	if err := segment.Validate(); err != nil {
		return "", 0, err
	}
	payload, err := segment.MarshalJSON()
	if err != nil {
		return "", 0, fmt.Errorf("encode visibility segment: %w", err)
	}
	contentType := storage.ContentTypeJSON
	if s.crypto != nil && s.crypto.Enabled() {
		contentType = storage.ContentTypeJSONEncrypted
		payload, err = s.crypto.EncryptMetadata(payload)
		if err != nil {
			return "", 0, fmt.Errorf("encrypt visibility segment: %w", err)
		}
	}
	object := visibilitySegmentObject(segment.ID)
	if _, err := s.backend.PutObject(ctx, namespace, object, bytes.NewReader(payload), storage.PutObjectOptions{
		ContentType: contentType,
	}); err != nil {
		return "", 0, err
	}
	if s.vis != nil {
		s.vis.set(namespace, nil)
	}
	return object, int64(len(payload)), nil
}

// LoadVisibilitySegment loads a visibility segment by ID.
func (s *Store) LoadVisibilitySegment(ctx context.Context, namespace, segmentID string) (*VisibilitySegment, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("visibility store unavailable")
	}
	start := time.Now()
	logger := pslog.LoggerFromContext(ctx)
	object := visibilitySegmentObject(segmentID)
	if logger != nil {
		logger.Trace("index.store.load_visibility_segment.begin", "namespace", namespace, "segment_id", segmentID, "object", object)
	}
	obj, err := s.backend.GetObject(ctx, namespace, object)
	if err != nil {
		if logger != nil {
			logger.Trace("index.store.load_visibility_segment.error", "namespace", namespace, "segment_id", segmentID, "object", object, "error", err, "elapsed", time.Since(start))
		}
		return nil, err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		if logger != nil {
			logger.Trace("index.store.load_visibility_segment.read_error", "namespace", namespace, "segment_id", segmentID, "object", object, "error", err, "elapsed", time.Since(start))
		}
		return nil, fmt.Errorf("read visibility segment: %w", err)
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.DecryptMetadata(payload)
		if err != nil {
			if logger != nil {
				logger.Trace("index.store.load_visibility_segment.decrypt_error", "namespace", namespace, "segment_id", segmentID, "object", object, "error", err, "elapsed", time.Since(start))
			}
			return nil, fmt.Errorf("decrypt visibility segment: %w", err)
		}
	}
	segment := &VisibilitySegment{}
	if err := segment.UnmarshalJSON(payload); err != nil {
		if logger != nil {
			logger.Trace("index.store.load_visibility_segment.decode_error", "namespace", namespace, "segment_id", segmentID, "object", object, "error", err, "elapsed", time.Since(start))
		}
		return nil, fmt.Errorf("decode visibility segment: %w", err)
	}
	if logger != nil {
		logger.Trace("index.store.load_visibility_segment.success", "namespace", namespace, "segment_id", segmentID, "object", object, "bytes", len(payload), "elapsed", time.Since(start))
	}
	return segment, nil
}

// DeleteVisibilitySegment removes a visibility segment.
func (s *Store) DeleteVisibilitySegment(ctx context.Context, namespace, segmentID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("visibility store unavailable")
	}
	if s.vis != nil {
		s.vis.set(namespace, nil)
	}
	return s.backend.DeleteObject(ctx, namespace, visibilitySegmentObject(segmentID), storage.DeleteObjectOptions{IgnoreNotFound: true})
}

func (s *Store) applyVisibilitySegment(namespace string, segment *VisibilitySegment, seq uint64, updatedAt time.Time) {
	if s == nil || s.vis == nil || segment == nil {
		return
	}
	state := s.vis.get(namespace)
	if state == nil || state.entries == nil {
		state = &visibilityState{
			entries: make(map[string]bool),
		}
	}
	for _, entry := range segment.Entries {
		if entry.Key == "" {
			continue
		}
		state.entries[entry.Key] = entry.Visible
	}
	state.seq = seq
	state.updatedAt = updatedAt
	state.checkedAt = time.Now()
	s.vis.set(namespace, state)
}

// VisibilityEntries loads (and caches) the visibility map for a namespace.
func (s *Store) VisibilityEntries(ctx context.Context, namespace string) (map[string]bool, uint64, error) {
	if s == nil {
		return nil, 0, nil
	}
	if s.vis == nil {
		return nil, 0, nil
	}
	now := time.Now()
	state := s.vis.get(namespace)
	if state != nil && now.Sub(state.checkedAt) < 500*time.Millisecond {
		return state.entries, state.seq, nil
	}
	manifestRes, err := s.LoadVisibilityManifest(ctx, namespace)
	if err != nil {
		return nil, 0, err
	}
	manifest := manifestRes.Manifest
	if manifest == nil || len(manifest.Segments) == 0 {
		entries := make(map[string]bool)
		s.vis.set(namespace, &visibilityState{
			seq:       0,
			updatedAt: time.Time{},
			entries:   entries,
			checkedAt: now,
		})
		return entries, 0, nil
	}
	if state != nil && state.seq == manifest.Seq {
		state.checkedAt = now
		s.vis.set(namespace, state)
		return state.entries, state.seq, nil
	}
	entries := make(map[string]bool)
	for _, ref := range manifest.Segments {
		segment, err := s.LoadVisibilitySegment(ctx, namespace, ref.ID)
		if err != nil {
			return nil, 0, err
		}
		if segment == nil {
			continue
		}
		for _, entry := range segment.Entries {
			if entry.Key == "" {
				continue
			}
			if _, exists := entries[entry.Key]; exists {
				continue
			}
			entries[entry.Key] = entry.Visible
		}
	}
	s.vis.set(namespace, &visibilityState{
		seq:       manifest.Seq,
		updatedAt: manifest.UpdatedAt,
		entries:   entries,
		checkedAt: now,
	})
	return entries, manifest.Seq, nil
}

func visibilitySegmentObject(segmentID string) string {
	clean := strings.TrimSpace(segmentID)
	if clean == "" {
		clean = "segment"
	}
	return path.Join(visibilitySegmentDir, clean) + ".json"
}
