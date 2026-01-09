package index

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"google.golang.org/protobuf/proto"

	indexproto "pkt.systems/lockd/internal/proto"
	"pkt.systems/lockd/internal/storage"
)

const (
	manifestObject       = "index/manifest.pb"
	segmentPrefix        = "index/segments"
	contentTypePlain     = storage.ContentTypeProtobuf
	contentTypeEncrypted = storage.ContentTypeProtobufEncrypted
)

// Store persists index manifests and segments on a storage backend.
type Store struct {
	backend storage.Backend
	crypto  *storage.Crypto
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
	return &Store{backend: backend, crypto: crypto}
}

// LoadManifest returns the index manifest and ETag for the namespace, defaulting to an empty manifest when missing.
func (s *Store) LoadManifest(ctx context.Context, namespace string) (ManifestLoadResult, error) {
	if s == nil || s.backend == nil {
		return ManifestLoadResult{Manifest: NewManifest()}, nil
	}
	obj, err := s.backend.GetObject(ctx, namespace, manifestObject)
	if err != nil {
		if err == storage.ErrNotFound {
			return ManifestLoadResult{Manifest: NewManifest()}, nil
		}
		return ManifestLoadResult{}, err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		return ManifestLoadResult{}, fmt.Errorf("read manifest: %w", err)
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.DecryptMetadata(payload)
		if err != nil {
			return ManifestLoadResult{}, fmt.Errorf("decrypt manifest: %w", err)
		}
	}
	var msg indexproto.IndexManifest
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return ManifestLoadResult{}, fmt.Errorf("decode manifest: %w", err)
	}
	manifest := ManifestFromProto(&msg)
	return ManifestLoadResult{Manifest: manifest, ETag: obj.Info.ETag}, nil
}

// SaveManifest writes the manifest with optional expected ETag for CAS semantics.
func (s *Store) SaveManifest(ctx context.Context, namespace string, manifest *Manifest, expectedETag string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("index store unavailable")
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
	return object, int64(len(payload)), nil
}

// LoadSegment retrieves and decrypts a segment by ID.
func (s *Store) LoadSegment(ctx context.Context, namespace, segmentID string) (*Segment, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("index store unavailable")
	}
	obj, err := s.backend.GetObject(ctx, namespace, segmentObject(segmentID))
	if err != nil {
		return nil, err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		return nil, fmt.Errorf("read segment: %w", err)
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.DecryptMetadata(payload)
		if err != nil {
			return nil, fmt.Errorf("decrypt segment: %w", err)
		}
	}
	var msg indexproto.IndexSegment
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("decode segment: %w", err)
	}
	return SegmentFromProto(&msg), nil
}

// DeleteSegment removes the persisted segment object.
func (s *Store) DeleteSegment(ctx context.Context, namespace, segmentID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("index store unavailable")
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
