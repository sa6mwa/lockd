package index

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	indexproto "pkt.systems/lockd/internal/proto"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

const (
	manifestObject       = "index/manifest.pb"
	segmentPrefix        = "index/segments"
	visibilityManifest   = "index/visibility/manifest.json"
	visibilitySegmentDir = "index/visibility/segments"
	contentTypePlain     = storage.ContentTypeProtobuf
	contentTypeEncrypted = storage.ContentTypeProtobufEncrypted
)

// Store persists index manifests and segments on a storage backend.
type Store struct {
	backend  storage.Backend
	crypto   *storage.Crypto
	cache    *segmentCache
	vis      *visibilityCache
	manifest *manifestCache
}

// ManifestLoadResult captures a manifest and its ETag.
type ManifestLoadResult struct {
	Manifest *Manifest
	ETag     string
}

// NewStore constructs an index store backed by the provided storage backend.
func NewStore(backend storage.Backend, crypto *storage.Crypto) *Store {
	if backend == nil {
		return nil
	}
	return &Store{
		backend:  backend,
		crypto:   crypto,
		cache:    newSegmentCache(defaultSegmentCacheLimit),
		vis:      newVisibilityCache(),
		manifest: newManifestCache(),
	}
}

// WarmNamespace preloads manifest, visibility, and segments into cache.
func (s *Store) WarmNamespace(ctx context.Context, namespace string) error {
	if s == nil || s.backend == nil {
		return nil
	}
	if _, _, err := s.VisibilityEntries(ctx, namespace); err != nil {
		return err
	}
	manifestRes, err := s.LoadManifest(ctx, namespace)
	if err != nil {
		return err
	}
	manifest := manifestRes.Manifest
	if manifest == nil {
		return nil
	}
	for _, shard := range manifest.Shards {
		if shard == nil {
			continue
		}
		for _, ref := range shard.Segments {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := s.LoadSegment(ctx, namespace, ref.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadManifest returns the index manifest and ETag for the namespace, defaulting to an empty manifest when missing.
func (s *Store) LoadManifest(ctx context.Context, namespace string) (ManifestLoadResult, error) {
	if s == nil || s.backend == nil {
		return ManifestLoadResult{Manifest: NewManifest()}, nil
	}
	start := time.Now()
	logger := pslog.LoggerFromContext(ctx)
	if logger != nil {
		logger.Trace("index.store.load_manifest.begin", "namespace", namespace, "object", manifestObject)
	}
	if s.manifest != nil {
		if cached, ok := s.manifest.get(namespace); ok {
			if logger != nil {
				logger.Trace("index.store.load_manifest.cache_hit", "namespace", namespace, "object", manifestObject, "elapsed", time.Since(start))
			}
			return ManifestLoadResult{Manifest: cloneManifest(cached.manifest), ETag: cached.etag}, nil
		}
	}
	obj, err := s.backend.GetObject(ctx, namespace, manifestObject)
	if err != nil {
		if err == storage.ErrNotFound {
			if logger != nil {
				logger.Trace("index.store.load_manifest.not_found", "namespace", namespace, "object", manifestObject, "elapsed", time.Since(start))
			}
			return ManifestLoadResult{Manifest: NewManifest()}, nil
		}
		if logger != nil {
			logger.Trace("index.store.load_manifest.error", "namespace", namespace, "object", manifestObject, "error", err, "elapsed", time.Since(start))
		}
		return ManifestLoadResult{}, err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		if logger != nil {
			logger.Trace("index.store.load_manifest.read_error", "namespace", namespace, "object", manifestObject, "error", err, "elapsed", time.Since(start))
		}
		return ManifestLoadResult{}, fmt.Errorf("read manifest: %w", err)
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.DecryptMetadata(payload)
		if err != nil {
			if logger != nil {
				logger.Trace("index.store.load_manifest.decrypt_error", "namespace", namespace, "object", manifestObject, "error", err, "elapsed", time.Since(start))
			}
			return ManifestLoadResult{}, fmt.Errorf("decrypt manifest: %w", err)
		}
	}
	var msg indexproto.IndexManifest
	if err := proto.Unmarshal(payload, &msg); err != nil {
		if logger != nil {
			logger.Trace("index.store.load_manifest.decode_error", "namespace", namespace, "object", manifestObject, "error", err, "elapsed", time.Since(start))
		}
		return ManifestLoadResult{}, fmt.Errorf("decode manifest: %w", err)
	}
	manifest := ManifestFromProto(&msg)
	if s.manifest != nil {
		s.manifest.set(namespace, manifest, obj.Info.ETag)
	}
	if logger != nil {
		segments := 0
		for _, shard := range manifest.Shards {
			if shard == nil {
				continue
			}
			segments += len(shard.Segments)
		}
		logger.Trace("index.store.load_manifest.success", "namespace", namespace, "object", manifestObject, "segments", segments, "elapsed", time.Since(start))
	}
	return ManifestLoadResult{Manifest: manifest, ETag: obj.Info.ETag}, nil
}

// SaveManifest writes the manifest with optional expected ETag for CAS semantics.
func (s *Store) SaveManifest(ctx context.Context, namespace string, manifest *Manifest, expectedETag string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("index store unavailable")
	}
	if manifest != nil && manifest.Format == 0 {
		manifest.Format = IndexFormatVersionV4
	}
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	msg := manifest.ToProto()
	payload, err := proto.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("encode manifest: %w", err)
	}
	contentType := contentTypePlain
	if s.crypto != nil && s.crypto.Enabled() {
		contentType = contentTypeEncrypted
		payload, err = s.crypto.EncryptMetadata(payload)
		if err != nil {
			return "", fmt.Errorf("encrypt manifest: %w", err)
		}
	}
	info, err := s.backend.PutObject(ctx, namespace, manifestObject, bytes.NewReader(payload), storage.PutObjectOptions{
		ExpectedETag: expectedETag,
		ContentType:  contentType,
	})
	if err != nil {
		return "", err
	}
	if s.manifest != nil {
		s.manifest.set(namespace, manifest, info.ETag)
	}
	return info.ETag, nil
}

// WriteSegment persists a segment and returns its ETag and payload size.
func (s *Store) WriteSegment(ctx context.Context, namespace string, segment *Segment) (string, int64, error) {
	if s == nil || s.backend == nil {
		return "", 0, fmt.Errorf("index store unavailable")
	}
	if err := segment.Validate(); err != nil {
		return "", 0, err
	}
	msg := segment.ToProto()
	payload, err := proto.Marshal(msg)
	if err != nil {
		return "", 0, fmt.Errorf("encode segment: %w", err)
	}
	contentType := contentTypePlain
	if s.crypto != nil && s.crypto.Enabled() {
		contentType = contentTypeEncrypted
		payload, err = s.crypto.EncryptMetadata(payload)
		if err != nil {
			return "", 0, fmt.Errorf("encrypt segment: %w", err)
		}
	}
	object := segmentObject(segment.ID)
	if _, err := s.backend.PutObject(ctx, namespace, object, bytes.NewReader(payload), storage.PutObjectOptions{
		ContentType: contentType,
	}); err != nil {
		return "", 0, err
	}
	if s.cache != nil {
		s.cache.put(cacheKey(namespace, segment.ID), segment)
	}
	return object, int64(len(payload)), nil
}

// LoadSegment retrieves and decrypts a segment by ID.
func (s *Store) LoadSegment(ctx context.Context, namespace, segmentID string) (*Segment, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("index store unavailable")
	}
	start := time.Now()
	logger := pslog.LoggerFromContext(ctx)
	object := segmentObject(segmentID)
	if logger != nil {
		logger.Trace("index.store.load_segment.begin", "namespace", namespace, "segment_id", segmentID, "object", object)
	}
	if s.cache != nil {
		if seg, ok := s.cache.get(cacheKey(namespace, segmentID)); ok {
			if logger != nil {
				logger.Trace("index.store.load_segment.cache_hit", "namespace", namespace, "segment_id", segmentID, "object", object, "elapsed", time.Since(start))
			}
			return seg, nil
		}
	}
	obj, err := s.backend.GetObject(ctx, namespace, object)
	if err != nil {
		if logger != nil {
			logger.Trace("index.store.load_segment.error", "namespace", namespace, "segment_id", segmentID, "object", object, "error", err, "elapsed", time.Since(start))
		}
		return nil, err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		if logger != nil {
			logger.Trace("index.store.load_segment.read_error", "namespace", namespace, "segment_id", segmentID, "object", object, "error", err, "elapsed", time.Since(start))
		}
		return nil, fmt.Errorf("read segment: %w", err)
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.DecryptMetadata(payload)
		if err != nil {
			if logger != nil {
				logger.Trace("index.store.load_segment.decrypt_error", "namespace", namespace, "segment_id", segmentID, "object", object, "error", err, "elapsed", time.Since(start))
			}
			return nil, fmt.Errorf("decrypt segment: %w", err)
		}
	}
	var msg indexproto.IndexSegment
	if err := proto.Unmarshal(payload, &msg); err != nil {
		if logger != nil {
			logger.Trace("index.store.load_segment.decode_error", "namespace", namespace, "segment_id", segmentID, "object", object, "error", err, "elapsed", time.Since(start))
		}
		return nil, fmt.Errorf("decode segment: %w", err)
	}
	segment := SegmentFromProto(&msg)
	if s.cache != nil {
		s.cache.put(cacheKey(namespace, segmentID), segment)
	}
	if logger != nil {
		logger.Trace("index.store.load_segment.success", "namespace", namespace, "segment_id", segmentID, "object", object, "bytes", len(payload), "elapsed", time.Since(start))
	}
	return segment, nil
}

// DeleteSegment removes the persisted segment object.
func (s *Store) DeleteSegment(ctx context.Context, namespace, segmentID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("index store unavailable")
	}
	if s.cache != nil {
		s.cache.drop(cacheKey(namespace, segmentID))
	}
	return s.backend.DeleteObject(ctx, namespace, segmentObject(segmentID), storage.DeleteObjectOptions{IgnoreNotFound: true})
}

func segmentObject(segmentID string) string {
	clean := strings.TrimSpace(segmentID)
	if clean == "" {
		clean = "segment"
	}
	return path.Join(segmentPrefix, clean) + ".pb"
}
