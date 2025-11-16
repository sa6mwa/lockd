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

type Store struct {
	backend storage.Backend
	crypto  *storage.Crypto
}

func NewStore(backend storage.Backend, crypto *storage.Crypto) *Store {
	if backend == nil {
		return nil
	}
	return &Store{backend: backend, crypto: crypto}
}

func (s *Store) LoadManifest(ctx context.Context, namespace string) (*Manifest, string, error) {
	if s == nil || s.backend == nil {
		return NewManifest(), "", nil
	}
	reader, info, err := s.backend.GetObject(ctx, namespace, manifestObject)
	if err != nil {
		if err == storage.ErrNotFound {
			return NewManifest(), "", nil
		}
		return nil, "", err
	}
	defer reader.Close()
	payload, err := io.ReadAll(reader)
	if err != nil {
		return nil, "", fmt.Errorf("read manifest: %w", err)
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.DecryptMetadata(payload)
		if err != nil {
			return nil, "", fmt.Errorf("decrypt manifest: %w", err)
		}
	}
	var msg indexproto.IndexManifest
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, "", fmt.Errorf("decode manifest: %w", err)
	}
	manifest := ManifestFromProto(&msg)
	return manifest, info.ETag, nil
}

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

func (s *Store) WriteSegment(ctx context.Context, namespace string, segment *Segment) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("index store unavailable")
	}
	if err := segment.Validate(); err != nil {
		return "", err
	}
	msg := segment.ToProto()
	payload, err := proto.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("encode segment: %w", err)
	}
	contentType := contentTypePlain
	if s.crypto != nil && s.crypto.Enabled() {
		contentType = contentTypeEncrypted
		payload, err = s.crypto.EncryptMetadata(payload)
		if err != nil {
			return "", fmt.Errorf("encrypt segment: %w", err)
		}
	}
	object := segmentObject(segment.ID)
	if _, err := s.backend.PutObject(ctx, namespace, object, bytes.NewReader(payload), storage.PutObjectOptions{
		ContentType: contentType,
	}); err != nil {
		return "", err
	}
	return object, nil
}

func (s *Store) LoadSegment(ctx context.Context, namespace, segmentID string) (*Segment, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("index store unavailable")
	}
	reader, _, err := s.backend.GetObject(ctx, namespace, segmentObject(segmentID))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	payload, err := io.ReadAll(reader)
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
