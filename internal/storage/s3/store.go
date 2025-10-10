package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"

	"pkt.systems/lockd/internal/storage"
)

// Config controls the behaviour of the S3 storage backend.
type Config struct {
	Endpoint       string
	Region         string
	Bucket         string
	Prefix         string
	Secure         bool
	ForcePathStyle bool
	PartSize       int64
	ServerSideEnc  string
	KMSKeyID       string
	CustomCreds    *credentials.Credentials
	Transport      http.RoundTripper
}

// Store implements storage.Backend backed by S3-compatible object storage.
type Store struct {
	client *minio.Client
	cfg    Config
}

// New constructs a Store using the provided configuration.
func New(cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3: bucket is required")
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		if cfg.Region != "" {
			endpoint = fmt.Sprintf("s3.%s.amazonaws.com", cfg.Region)
		} else {
			endpoint = "s3.amazonaws.com"
		}
	}
	if cfg.Transport == nil {
		cfg.Transport = http.DefaultTransport
	}
	var creds *credentials.Credentials
	if cfg.CustomCreds != nil {
		creds = cfg.CustomCreds
	} else {
		chain := []credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.EnvMinio{},
			&credentials.FileAWSCredentials{},
			&credentials.IAM{},
		}
		creds = credentials.NewChainCredentials(chain)
	}
	options := &minio.Options{
		Creds:     creds,
		Secure:    cfg.Secure,
		Region:    cfg.Region,
		Transport: cfg.Transport,
	}
	if cfg.ForcePathStyle {
		options.BucketLookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint, options)
	if err != nil {
		return nil, fmt.Errorf("s3: create client: %w", err)
	}
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")
	return &Store{client: client, cfg: cfg}, nil
}

func (s *Store) Close() error { return nil }

// Client exposes the underlying MinIO client for diagnostics.
func (s *Store) Client() *minio.Client {
	return s.client
}

func (s *Store) LoadMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	object := s.metaObject(key)
	info, err := s.client.StatObject(ctx, s.cfg.Bucket, object, minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			return nil, "", storage.ErrNotFound
		}
		return nil, "", fmt.Errorf("s3: stat meta: %w", err)
	}
	reader, err := s.client.GetObject(ctx, s.cfg.Bucket, object, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("s3: get meta: %w", err)
	}
	defer reader.Close()
	var meta storage.Meta
	if err := json.NewDecoder(io.LimitReader(reader, 1<<20)).Decode(&meta); err != nil {
		return nil, "", fmt.Errorf("s3: decode meta: %w", err)
	}
	return &meta, stripETag(info.ETag), nil
}

func (s *Store) StoreMeta(ctx context.Context, key string, meta *storage.Meta, expectedETag string) (string, error) {
	payload, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	options := minio.PutObjectOptions{ContentType: "application/json"}
	s.applySSE(&options)
	if expectedETag != "" {
		options.SetMatchETag(expectedETag)
	} else {
		options.SetMatchETagExcept("*")
	}
	info, err := s.client.PutObject(ctx, s.cfg.Bucket, s.metaObject(key), bytes.NewReader(payload), int64(len(payload)), options)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", storage.ErrCASMismatch
		}
		return "", fmt.Errorf("s3: put meta: %w", err)
	}
	return stripETag(info.ETag), nil
}

func (s *Store) DeleteMeta(ctx context.Context, key string, expectedETag string) error {
	object := s.metaObject(key)
	if expectedETag != "" {
		info, err := s.client.StatObject(ctx, s.cfg.Bucket, object, minio.StatObjectOptions{})
		if err != nil {
			if isNotFound(err) {
				return storage.ErrNotFound
			}
			return fmt.Errorf("s3: stat meta: %w", err)
		}
		if stripETag(info.ETag) != expectedETag {
			return storage.ErrCASMismatch
		}
	}
	if err := s.client.RemoveObject(ctx, s.cfg.Bucket, object, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("s3: remove meta: %w", err)
	}
	return nil
}

func (s *Store) ListMetaKeys(ctx context.Context) ([]string, error) {
	metaPrefix := path.Join(strings.Trim(s.cfg.Prefix, "/"), "meta")
	if metaPrefix == "." || metaPrefix == "" {
		metaPrefix = "meta"
	}
	metaPrefix += "/"
	opts := minio.ListObjectsOptions{Prefix: metaPrefix, Recursive: true}
	var keys []string
	for object := range s.client.ListObjects(ctx, s.cfg.Bucket, opts) {
		if object.Err != nil {
			return nil, fmt.Errorf("s3: list meta: %w", object.Err)
		}
		key := strings.TrimPrefix(object.Key, metaPrefix)
		key = strings.TrimSuffix(key, ".json")
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (s *Store) ReadState(ctx context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	object := s.stateObject(key)
	info, err := s.client.StatObject(ctx, s.cfg.Bucket, object, minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			return nil, nil, storage.ErrNotFound
		}
		return nil, nil, fmt.Errorf("s3: stat state: %w", err)
	}
	obj, err := s.client.GetObject(ctx, s.cfg.Bucket, object, minio.GetObjectOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("s3: get state: %w", err)
	}
	infoOut := &storage.StateInfo{
		Size:       info.Size,
		ETag:       stripETag(info.ETag),
		Version:    0,
		ModifiedAt: info.LastModified.Unix(),
	}
	return obj, infoOut, nil
}

func (s *Store) WriteState(ctx context.Context, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	object := s.stateObject(key)
	putOpts := minio.PutObjectOptions{
		ContentType: "application/json",
	}
	if s.cfg.PartSize > 0 {
		putOpts.PartSize = uint64(s.cfg.PartSize)
	}
	s.applySSE(&putOpts)
	if opts.ExpectedETag != "" {
		putOpts.SetMatchETag(opts.ExpectedETag)
	}
	length := int64(-1)
	if seeker, ok := body.(io.Seeker); ok {
		if current, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			if end, err := seeker.Seek(0, io.SeekEnd); err == nil {
				length = end - current
				_, _ = seeker.Seek(current, io.SeekStart)
			}
		}
	}
	info, err := s.client.PutObject(ctx, s.cfg.Bucket, object, body, length, putOpts)
	if err != nil {
		if isPreconditionFailed(err) {
			return nil, storage.ErrCASMismatch
		}
		return nil, fmt.Errorf("s3: put state: %w", err)
	}
	return &storage.PutStateResult{
		BytesWritten: info.Size,
		NewETag:      stripETag(info.ETag),
	}, nil
}

func (s *Store) RemoveState(ctx context.Context, key string, expectedETag string) error {
	object := s.stateObject(key)
	if expectedETag != "" {
		info, err := s.client.StatObject(ctx, s.cfg.Bucket, object, minio.StatObjectOptions{})
		if err != nil {
			if isNotFound(err) {
				return storage.ErrNotFound
			}
			return fmt.Errorf("s3: stat state: %w", err)
		}
		if stripETag(info.ETag) != expectedETag {
			return storage.ErrCASMismatch
		}
	}
	if err := s.client.RemoveObject(ctx, s.cfg.Bucket, object, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("s3: remove state: %w", err)
	}
	return nil
}

func (s *Store) metaObject(key string) string {
	return s.withPrefix(path.Join("meta", key+".json"))
}

func (s *Store) stateObject(key string) string {
	return s.withPrefix(path.Join("state", key+".json"))
}

func (s *Store) withPrefix(p string) string {
	if s.cfg.Prefix == "" {
		return p
	}
	return path.Join(s.cfg.Prefix, p)
}

func (s *Store) applySSE(opts *minio.PutObjectOptions) {
	switch strings.ToUpper(s.cfg.ServerSideEnc) {
	case "AES256":
		opts.ServerSideEncryption = encrypt.NewSSE()
	case "AWS:KMS", "KMS":
		if s.cfg.KMSKeyID != "" {
			if enc, err := encrypt.NewSSEKMS(s.cfg.KMSKeyID, nil); err == nil {
				opts.ServerSideEncryption = enc
			}
		}
	}
}

func stripETag(etag string) string {
	return strings.Trim(etag, "\"")
}

func isNotFound(err error) bool {
	errResp := minio.ErrorResponse{}
	if errors.As(err, &errResp) {
		return errResp.StatusCode == http.StatusNotFound
	}
	return false
}

func isPreconditionFailed(err error) bool {
	errResp := minio.ErrorResponse{}
	if errors.As(err, &errResp) {
		return errResp.StatusCode == http.StatusPreconditionFailed
	}
	return false
}
