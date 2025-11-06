package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

// Config controls the behaviour of the S3 storage backend.
type Config struct {
	Endpoint       string
	Region         string
	Bucket         string
	Prefix         string
	Insecure       bool
	ForcePathStyle bool
	PartSize       int64
	ServerSideEnc  string
	KMSKeyID       string
	CustomCreds    *credentials.Credentials
	Transport      http.RoundTripper
	Crypto         *storage.Crypto
}

// Store implements storage.Backend backed by S3-compatible object storage.
type Store struct {
	client *minio.Client
	cfg    Config
	crypto *storage.Crypto
}

type countingWriter struct {
	n int64
	w io.Writer
}

const descriptorMetadataKey = "lockd-descriptor"

func (cw *countingWriter) Write(p []byte) (int, error) {
	if cw.w == nil {
		return len(p), nil
	}
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

func encodeDescriptor(desc []byte) string {
	return base64.StdEncoding.EncodeToString(desc)
}

func decodeDescriptor(meta map[string]string) ([]byte, error) {
	if meta == nil {
		return nil, nil
	}
	var val string
	for k, v := range meta {
		if strings.EqualFold(k, descriptorMetadataKey) {
			val = v
			break
		}
	}
	if val == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(val)
	if err != nil {
		return nil, fmt.Errorf("s3: decode descriptor: %w", err)
	}
	return decoded, nil
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
		Secure:    !cfg.Insecure,
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
	return &Store{client: client, cfg: cfg, crypto: cfg.Crypto}, nil
}

// Close satisfies storage.Backend and is a no-op for the S3 client.
func (s *Store) Close() error { return nil }

// Client exposes the underlying MinIO client for diagnostics.
func (s *Store) Client() *minio.Client {
	return s.client
}

// Config returns a copy of the configuration used to build the store.
func (s *Store) Config() Config {
	return s.cfg
}

func (s *Store) loggers(ctx context.Context) (pslog.Logger, pslog.Logger) {
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = loggingutil.NoopLogger()
	}
	logger = logger.With("storage_backend", "s3", "bucket", s.cfg.Bucket)
	if s.cfg.Prefix != "" {
		logger = logger.With("prefix", s.cfg.Prefix)
	}
	return logger, logger
}

// LoadMeta downloads the metadata object for key and returns its ETag.
func (s *Store) LoadMeta(ctx context.Context, namespace, key string) (*storage.Meta, string, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	object, err := s.metaObject(namespace, key)
	if err != nil {
		logger.Debug("s3.load_meta.resolve_error", "namespace", namespace, "key", key, "error", err)
		return nil, "", err
	}
	verbose.Trace("s3.load_meta.begin", "namespace", namespace, "key", key, "object", object)

	obj, err := s.client.GetObject(ctx, s.cfg.Bucket, object, minio.GetObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			verbose.Debug("s3.load_meta.not_found", "key", key, "object", object, "elapsed", time.Since(start))
			return nil, "", storage.ErrNotFound
		}
		logger.Debug("s3.load_meta.get_error", "key", key, "object", object, "error", err)
		return nil, "", s.wrapError(err, "s3: get meta")
	}
	defer obj.Close()

	payload, err := io.ReadAll(io.LimitReader(obj, 1<<20))
	if err != nil {
		foundErr := false
		if errors.Is(err, io.EOF) {
			foundErr = true
		} else {
			var errResp minio.ErrorResponse
			if errors.As(err, &errResp) && errResp.StatusCode == http.StatusNotFound {
				foundErr = true
			}
		}
		if foundErr {
			verbose.Debug("s3.load_meta.not_found", "key", key, "object", object, "elapsed", time.Since(start))
			return nil, "", storage.ErrNotFound
		}
		logger.Debug("s3.load_meta.read_error", "key", key, "object", object, "error", err)
		return nil, "", fmt.Errorf("s3: read meta: %w", err)
	}
	info, err := obj.Stat()
	if err != nil {
		if isNotFound(err) {
			verbose.Debug("s3.load_meta.not_found", "key", key, "object", object, "elapsed", time.Since(start))
			return nil, "", storage.ErrNotFound
		}
		logger.Debug("s3.load_meta.stat_error", "key", key, "object", object, "error", err)
		return nil, "", s.wrapError(err, "s3: stat meta")
	}
	meta, err := storage.UnmarshalMeta(payload, s.crypto)
	if err != nil {
		logger.Debug("s3.load_meta.decode_error", "key", key, "object", object, "error", err)
		return nil, "", err
	}
	leaseOwner := ""
	leaseExpires := int64(0)
	if meta.Lease != nil {
		leaseOwner = meta.Lease.Owner
		leaseExpires = meta.Lease.ExpiresAtUnix
	}
	etag := stripETag(info.ETag)
	verbose.Debug("s3.load_meta.success",
		"key", key,
		"object", object,
		"meta_etag", etag,
		"version", meta.Version,
		"state_etag", meta.StateETag,
		"lease_owner", leaseOwner,
		"lease_expires_at", leaseExpires,
		"elapsed", time.Since(start),
	)
	return meta, etag, nil
}

// StoreMeta uploads the metadata protobuf, applying conditional copy semantics via expectedETag.
func (s *Store) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	object, err := s.metaObject(namespace, key)
	if err != nil {
		logger.Debug("s3.store_meta.resolve_error", "namespace", namespace, "key", key, "error", err)
		return "", err
	}
	leaseOwner := ""
	leaseExpires := int64(0)
	if meta != nil && meta.Lease != nil {
		leaseOwner = meta.Lease.Owner
		leaseExpires = meta.Lease.ExpiresAtUnix
	}
	verbose.Trace("s3.store_meta.begin",
		"namespace", namespace,
		"key", key,
		"object", object,
		"expected_etag", expectedETag,
		"lease_owner", leaseOwner,
		"lease_expires_at", leaseExpires,
	)
	payload, err := storage.MarshalMeta(meta, s.crypto)
	if err != nil {
		logger.Debug("s3.store_meta.marshal_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return "", err
	}
	options := minio.PutObjectOptions{ContentType: s.metaContentType()}
	s.applySSE(&options)
	if expectedETag != "" {
		options.SetMatchETag(expectedETag)
	} else {
		options.SetMatchETagExcept("*")
	}
	info, err := s.client.PutObject(ctx, s.cfg.Bucket, object, bytes.NewReader(payload), int64(len(payload)), options)
	if err != nil {
		if isPreconditionFailed(err) {
			logger.Debug("s3.store_meta.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag)
			return "", storage.ErrCASMismatch
		}
		if isNotFound(err) {
			logger.Debug("s3.store_meta.not_found", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag)
			return "", storage.ErrNotFound
		}
		logger.Debug("s3.store_meta.put_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return "", s.wrapError(err, "s3: put meta")
	}
	newETag := stripETag(info.ETag)
	verbose.Debug("s3.store_meta.success",
		"namespace", namespace,
		"key", key,
		"object", object,
		"new_etag", newETag,
		"elapsed", time.Since(start),
	)
	return newETag, nil
}

// DeleteMeta removes the metadata object, enforcing CAS when expectedETag is supplied.
func (s *Store) DeleteMeta(ctx context.Context, namespace, key string, expectedETag string) error {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	object, err := s.metaObject(namespace, key)
	if err != nil {
		logger.Debug("s3.delete_meta.resolve_error", "namespace", namespace, "key", key, "error", err)
		return err
	}
	verbose.Trace("s3.delete_meta.begin", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag)

	if expectedETag != "" {
		info, err := s.client.StatObject(ctx, s.cfg.Bucket, object, minio.StatObjectOptions{})
		if err != nil {
			if isNotFound(err) {
				verbose.Debug("s3.delete_meta.not_found", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
				return storage.ErrNotFound
			}
			logger.Debug("s3.delete_meta.stat_error", "namespace", namespace, "key", key, "object", object, "error", err)
			return fmt.Errorf("s3: stat meta: %w", err)
		}
		if stripETag(info.ETag) != expectedETag {
			logger.Debug("s3.delete_meta.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag, "current_etag", stripETag(info.ETag))
			return storage.ErrCASMismatch
		}
	}
	if err := s.client.RemoveObject(ctx, s.cfg.Bucket, object, minio.RemoveObjectOptions{}); err != nil {
		logger.Debug("s3.delete_meta.remove_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return s.wrapError(err, "s3: remove meta")
	}
	verbose.Debug("s3.delete_meta.success", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
	return nil
}

// ListMetaKeys enumerates metadata documents within the provided namespace.
func (s *Store) ListMetaKeys(ctx context.Context, namespace string) ([]string, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	prefixPath := path.Join(namespace, "meta") + "/"
	fullPrefix := s.withPrefix(prefixPath)
	verbose.Trace("s3.list_meta_keys.begin", "namespace", namespace, "prefix", fullPrefix)
	opts := minio.ListObjectsOptions{Prefix: fullPrefix, Recursive: true}
	var keys []string
	for object := range s.client.ListObjects(ctx, s.cfg.Bucket, opts) {
		if object.Err != nil {
			logger.Debug("s3.list_meta_keys.error", "namespace", namespace, "error", object.Err)
			return nil, s.wrapError(object.Err, "s3: list meta")
		}
		rel := strings.TrimPrefix(object.Key, fullPrefix)
		if rel == "" || strings.HasSuffix(rel, "/") {
			continue
		}
		if strings.HasPrefix(rel, "tmp/") {
			continue
		}
		if !strings.HasSuffix(rel, ".pb") {
			continue
		}
		entry := strings.TrimSuffix(rel, ".pb")
		if entry == "" {
			continue
		}
		keys = append(keys, entry)
	}
	verbose.Debug("s3.list_meta_keys.success", "namespace", namespace, "count", len(keys), "elapsed", time.Since(start))
	return keys, nil
}

// ReadState streams the state object for key within the namespace and returns metadata.
func (s *Store) ReadState(ctx context.Context, namespace, key string) (io.ReadCloser, *storage.StateInfo, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	object, err := s.stateObject(namespace, key)
	if err != nil {
		logger.Debug("s3.read_state.resolve_error", "namespace", namespace, "key", key, "error", err)
		return nil, nil, err
	}
	verbose.Trace("s3.read_state.begin", "namespace", namespace, "key", key, "object", object)
	info, err := s.client.StatObject(ctx, s.cfg.Bucket, object, minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			verbose.Debug("s3.read_state.not_found", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
			return nil, nil, storage.ErrNotFound
		}
		logger.Debug("s3.read_state.stat_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return nil, nil, s.wrapError(err, "s3: stat state")
	}
	obj, err := s.client.GetObject(ctx, s.cfg.Bucket, object, minio.GetObjectOptions{})
	if err != nil {
		logger.Debug("s3.read_state.get_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return nil, nil, s.wrapError(err, "s3: get state")
	}
	etag := stripETag(info.ETag)
	infoOut := &storage.StateInfo{
		Size:       info.Size,
		CipherSize: info.Size,
		ETag:       etag,
		Version:    0,
		ModifiedAt: info.LastModified.Unix(),
	}
	if desc, err := decodeDescriptor(info.UserMetadata); err == nil && len(desc) > 0 {
		infoOut.Descriptor = append([]byte(nil), desc...)
	} else if err != nil {
		obj.Close()
		return nil, nil, err
	}
	encrypted := s.crypto != nil && s.crypto.Enabled()
	var descriptor []byte
	if descFromCtx, ok := storage.StateDescriptorFromContext(ctx); ok && len(descFromCtx) > 0 {
		descriptor = append([]byte(nil), descFromCtx...)
	} else if len(infoOut.Descriptor) > 0 {
		descriptor = append([]byte(nil), infoOut.Descriptor...)
	}
	if len(descriptor) > 0 && len(infoOut.Descriptor) == 0 {
		infoOut.Descriptor = append([]byte(nil), descriptor...)
	}
	if plain, ok := storage.StatePlaintextSizeFromContext(ctx); ok && plain > 0 {
		infoOut.Size = plain
	}
	objectCtx := storage.StateObjectContext(path.Join(namespace, key))
	if encrypted {
		if len(descriptor) == 0 {
			obj.Close()
			logger.Debug("s3.read_state.missing_descriptor", "namespace", namespace, "key", key)
			return nil, nil, fmt.Errorf("s3: missing state descriptor for %q", key)
		}
		mat, err := s.crypto.MaterialFromDescriptor(objectCtx, descriptor)
		if err != nil {
			obj.Close()
			logger.Debug("s3.read_state.material_error", "namespace", namespace, "key", key, "error", err)
			return nil, nil, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(obj, mat)
		if err != nil {
			obj.Close()
			logger.Debug("s3.read_state.decrypt_error", "namespace", namespace, "key", key, "error", err)
			return nil, nil, err
		}
		verbose.Debug("s3.read_state.success",
			"namespace", namespace,
			"key", key,
			"object", object,
			"etag", etag,
			"size", infoOut.Size,
			"cipher_size", info.Size,
			"elapsed", time.Since(start),
		)
		return decReader, infoOut, nil
	}
	verbose.Debug("s3.read_state.success",
		"namespace", namespace,
		"key", key,
		"object", object,
		"etag", etag,
		"size", infoOut.Size,
		"cipher_size", info.Size,
		"elapsed", time.Since(start),
	)
	return obj, infoOut, nil
}

// WriteState uploads a new state object, optionally guarding against stale ETags.
func (s *Store) WriteState(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	object, err := s.stateObject(namespace, key)
	if err != nil {
		logger.Debug("s3.write_state.resolve_error", "namespace", namespace, "key", key, "error", err)
		return nil, err
	}
	verbose.Trace("s3.write_state.begin", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag)
	putOpts := minio.PutObjectOptions{ContentType: storage.ContentTypeJSON}
	if s.cfg.PartSize > 0 {
		putOpts.PartSize = uint64(s.cfg.PartSize)
	}
	s.applySSE(&putOpts)
	if opts.ExpectedETag != "" {
		putOpts.SetMatchETag(opts.ExpectedETag)
	}
	length := int64(-1)
	encrypted := s.crypto != nil && s.crypto.Enabled()
	var descriptor []byte
	var plainBytes atomic.Int64
	reader := body
	objectCtx := storage.StateObjectContext(path.Join(namespace, key))
	if encrypted {
		putOpts.ContentType = storage.ContentTypeJSONEncrypted
		descFromCtx, ok := storage.StateDescriptorFromContext(ctx)
		var mat kryptograf.Material
		var err error
		if ok && len(descFromCtx) > 0 {
			descriptor = append([]byte(nil), descFromCtx...)
			mat, err = s.crypto.MaterialFromDescriptor(objectCtx, descriptor)
			if err != nil {
				logger.Debug("s3.write_state.material_error", "namespace", namespace, "key", key, "object", object, "error", err)
				return nil, err
			}
		} else {
			var descBytes []byte
			mat, descBytes, err = s.crypto.MintMaterial(objectCtx)
			if err != nil {
				logger.Debug("s3.write_state.mint_descriptor_error", "namespace", namespace, "key", key, "object", object, "error", err)
				return nil, err
			}
			descriptor = append([]byte(nil), descBytes...)
		}
		if len(descriptor) > 0 {
			if putOpts.UserMetadata == nil {
				putOpts.UserMetadata = make(map[string]string)
			}
			putOpts.UserMetadata[descriptorMetadataKey] = encodeDescriptor(descriptor)
		}
		pr, pw := io.Pipe()
		encWriter, err := s.crypto.EncryptWriterForMaterial(pw, mat)
		if err != nil {
			pw.Close()
			logger.Debug("s3.write_state.encrypt_writer_error", "namespace", namespace, "key", key, "object", object, "error", err)
			return nil, err
		}
		go func() {
			cw := &countingWriter{w: encWriter}
			if _, err := io.Copy(cw, body); err != nil {
				encWriter.Close()
				pw.CloseWithError(err)
				return
			}
			if err := encWriter.Close(); err != nil {
				pw.CloseWithError(err)
				return
			}
			plainBytes.Store(cw.n)
			pw.Close()
		}()
		reader = pr
		length = -1
	} else if seeker, ok := body.(io.Seeker); ok {
		if current, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			if end, err := seeker.Seek(0, io.SeekEnd); err == nil {
				length = end - current
				_, _ = seeker.Seek(current, io.SeekStart)
			}
		}
	}
	info, err := s.client.PutObject(ctx, s.cfg.Bucket, object, reader, length, putOpts)
	if err != nil {
		if isPreconditionFailed(err) {
			logger.Debug("s3.write_state.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag)
			return nil, storage.ErrCASMismatch
		}
		logger.Debug("s3.write_state.put_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return nil, s.wrapError(err, "s3: put state")
	}
	bytesWritten := info.Size
	if encrypted {
		if v := plainBytes.Load(); v > 0 {
			bytesWritten = v
		}
	}
	result := &storage.PutStateResult{
		BytesWritten: bytesWritten,
		NewETag:      stripETag(info.ETag),
	}
	if len(descriptor) > 0 {
		result.Descriptor = append([]byte(nil), descriptor...)
	}
	verbose.Debug("s3.write_state.success",
		"namespace", namespace,
		"key", key,
		"object", object,
		"bytes", bytesWritten,
		"new_etag", result.NewETag,
		"elapsed", time.Since(start),
	)
	return result, nil
}

// Remove deletes the state object, applying CAS when expectedETag is provided.
func (s *Store) Remove(ctx context.Context, namespace, key string, expectedETag string) error {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	object, err := s.stateObject(namespace, key)
	if err != nil {
		logger.Debug("s3.remove_state.resolve_error", "namespace", namespace, "key", key, "error", err)
		return err
	}
	verbose.Trace("s3.remove_state.begin", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag)
	if expectedETag != "" {
		info, err := s.client.StatObject(ctx, s.cfg.Bucket, object, minio.StatObjectOptions{})
		if err != nil {
			if isNotFound(err) {
				verbose.Debug("s3.remove_state.not_found", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
				return storage.ErrNotFound
			}
			logger.Debug("s3.remove_state.stat_error", "namespace", namespace, "key", key, "object", object, "error", err)
			return fmt.Errorf("s3: stat state: %w", err)
		}
		if stripETag(info.ETag) != expectedETag {
			logger.Debug("s3.remove_state.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag, "current_etag", stripETag(info.ETag))
			return storage.ErrCASMismatch
		}
	}
	if err := s.client.RemoveObject(ctx, s.cfg.Bucket, object, minio.RemoveObjectOptions{}); err != nil {
		logger.Debug("s3.remove_state.remove_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return s.wrapError(err, "s3: remove state")
	}
	verbose.Debug("s3.remove_state.success", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
	return nil
}

// ListObjects enumerates raw objects within the namespace matching opts.Prefix.
func (s *Store) ListObjects(ctx context.Context, namespace string, opts storage.ListOptions) (*storage.ListResult, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("s3.list_objects.begin",
		"namespace", namespace,
		"prefix", opts.Prefix,
		"start_after", opts.StartAfter,
		"limit", opts.Limit,
	)

	nsRoot := strings.TrimSuffix(s.objectKey(namespace, ""), "/")
	if nsRoot != "" {
		nsRoot += "/"
	}
	actualPrefix := nsRoot + strings.TrimPrefix(opts.Prefix, "/")
	listOpts := minio.ListObjectsOptions{
		Prefix:    actualPrefix,
		Recursive: true,
	}
	if opts.StartAfter != "" {
		listOpts.StartAfter = nsRoot + strings.TrimPrefix(opts.StartAfter, "/")
	}
	if opts.Limit > 0 {
		listOpts.MaxKeys = opts.Limit + 1
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := &storage.ListResult{}
	lastKey := ""
	exceeded := false
	for object := range s.client.ListObjects(ctx, s.cfg.Bucket, listOpts) {
		if object.Err != nil {
			logger.Debug("s3.list_objects.error", "namespace", namespace, "error", object.Err)
			return nil, s.wrapError(object.Err, "s3: list objects")
		}
		logicalKey := strings.TrimPrefix(object.Key, nsRoot)
		if logicalKey == object.Key {
			continue
		}
		info := storage.ObjectInfo{
			Key:          logicalKey,
			ETag:         stripETag(object.ETag),
			Size:         object.Size,
			LastModified: object.LastModified,
			ContentType:  object.ContentType,
		}
		if opts.Limit > 0 && len(result.Objects) >= opts.Limit {
			exceeded = true
			break
		}
		result.Objects = append(result.Objects, info)
		lastKey = logicalKey
	}
	if exceeded {
		result.Truncated = true
		result.NextStartAfter = lastKey
	} else if lastKey != "" {
		result.NextStartAfter = lastKey
	}
	verbose.Debug("s3.list_objects.success",
		"namespace", namespace,
		"prefix", opts.Prefix,
		"count", len(result.Objects),
		"truncated", result.Truncated,
		"elapsed", time.Since(start),
	)
	return result, nil
}

// GetObject downloads the raw payload for key within the namespace.
func (s *Store) GetObject(ctx context.Context, namespace, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	logger, verbose := s.loggers(ctx)
	object := s.objectKey(namespace, key)
	verbose.Trace("s3.get_object.begin", "namespace", namespace, "key", key, "object", object)
	info, err := s.client.StatObject(ctx, s.cfg.Bucket, object, minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			verbose.Debug("s3.get_object.not_found", "namespace", namespace, "key", key, "object", object)
			return nil, nil, storage.ErrNotFound
		}
		logger.Debug("s3.get_object.stat_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return nil, nil, s.wrapError(err, "s3: stat object")
	}
	obj, err := s.client.GetObject(ctx, s.cfg.Bucket, object, minio.GetObjectOptions{})
	if err != nil {
		logger.Debug("s3.get_object.get_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return nil, nil, s.wrapError(err, "s3: get object")
	}
	reader := &notFoundAwareObject{object: obj}
	meta := &storage.ObjectInfo{
		Key:          key,
		ETag:         stripETag(info.ETag),
		Size:         info.Size,
		LastModified: info.LastModified,
		ContentType:  info.ContentType,
	}
	if desc, err := decodeDescriptor(info.UserMetadata); err == nil && len(desc) > 0 {
		meta.Descriptor = append([]byte(nil), desc...)
	} else if err != nil {
		obj.Close()
		return nil, nil, err
	}
	if plain, ok := storage.ObjectPlaintextSizeFromContext(ctx); ok && plain > 0 {
		meta.Size = plain
	}
	verbose.Debug("s3.get_object.success", "namespace", namespace, "key", key, "object", object, "etag", meta.ETag, "size", meta.Size)
	return reader, meta, nil
}

// PutObject uploads raw object bytes with conditional guards within the namespace.
func (s *Store) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	logger, verbose := s.loggers(ctx)
	object := s.objectKey(namespace, key)
	verbose.Trace("s3.put_object.begin",
		"namespace", namespace,
		"key", key,
		"object", object,
		"expected_etag", opts.ExpectedETag,
		"if_not_exists", opts.IfNotExists,
	)
	putOpts := minio.PutObjectOptions{ContentType: opts.ContentType}
	if putOpts.ContentType == "" {
		putOpts.ContentType = storage.ContentTypeOctetStream
	}
	s.applySSE(&putOpts)
	if len(opts.Descriptor) > 0 {
		if putOpts.UserMetadata == nil {
			putOpts.UserMetadata = make(map[string]string)
		}
		putOpts.UserMetadata[descriptorMetadataKey] = encodeDescriptor(opts.Descriptor)
	}
	if opts.ExpectedETag != "" {
		putOpts.SetMatchETag(opts.ExpectedETag)
	} else if opts.IfNotExists {
		putOpts.SetMatchETagExcept("*")
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
			logger.Debug("s3.put_object.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag)
			return nil, storage.ErrCASMismatch
		}
		logger.Debug("s3.put_object.put_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return nil, s.wrapError(err, "s3: put object")
	}
	meta := &storage.ObjectInfo{
		Key:          key,
		ETag:         stripETag(info.ETag),
		Size:         info.Size,
		LastModified: time.Now().UTC(),
		ContentType:  putOpts.ContentType,
		Descriptor:   append([]byte(nil), opts.Descriptor...),
	}
	verbose.Debug("s3.put_object.success", "namespace", namespace, "key", key, "object", object, "etag", meta.ETag, "size", meta.Size)
	return meta, nil
}

// DeleteObject removes an object with optional CAS within the namespace.
func (s *Store) DeleteObject(ctx context.Context, namespace, key string, opts storage.DeleteObjectOptions) error {
	logger, verbose := s.loggers(ctx)
	object := s.objectKey(namespace, key)
	verbose.Trace("s3.delete_object.begin", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag, "ignore_not_found", opts.IgnoreNotFound)
	if opts.ExpectedETag != "" {
		info, err := s.client.StatObject(ctx, s.cfg.Bucket, object, minio.StatObjectOptions{})
		if err != nil {
			if isNotFound(err) {
				if opts.IgnoreNotFound {
					return nil
				}
				return storage.ErrNotFound
			}
			logger.Debug("s3.delete_object.stat_error", "namespace", namespace, "key", key, "object", object, "error", err)
			return fmt.Errorf("s3: stat object: %w", err)
		}
		if stripETag(info.ETag) != opts.ExpectedETag {
			logger.Debug("s3.delete_object.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag, "current_etag", stripETag(info.ETag))
			return storage.ErrCASMismatch
		}
	}
	if err := s.client.RemoveObject(ctx, s.cfg.Bucket, object, minio.RemoveObjectOptions{}); err != nil {
		if isNotFound(err) && opts.IgnoreNotFound {
			return nil
		}
		logger.Debug("s3.delete_object.remove_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return s.wrapError(err, "s3: delete object")
	}
	verbose.Debug("s3.delete_object.success", "namespace", namespace, "key", key, "object", object)
	return nil
}

const (
	metaContentTypePlain     = "application/x-protobuf"
	metaContentTypeEncrypted = "application/vnd.lockd+protobuf-encrypted"
)

func (s *Store) metaObject(namespace, key string) (string, error) {
	object, err := storage.NamespacedMetaObject(namespace, key)
	if err != nil {
		return "", err
	}
	return s.withPrefix(object), nil
}

func (s *Store) stateObject(namespace, key string) (string, error) {
	object, err := storage.NamespacedStateObject(namespace, key)
	if err != nil {
		return "", err
	}
	return s.withPrefix(object), nil
}

func (s *Store) metaContentType() string {
	if s.crypto != nil && s.crypto.Enabled() {
		return metaContentTypeEncrypted
	}
	return metaContentTypePlain
}

func (s *Store) objectKey(namespace, key string) string {
	combined := path.Join(namespace, strings.TrimPrefix(key, "/"))
	return s.withPrefix(strings.TrimPrefix(combined, "/"))
}

func (s *Store) trimObjectPrefix(object string) string {
	if s.cfg.Prefix == "" {
		return strings.TrimPrefix(object, "/")
	}
	prefix := strings.Trim(s.cfg.Prefix, "/")
	if prefix == "" {
		return strings.TrimPrefix(object, "/")
	}
	object = strings.TrimPrefix(object, prefix+"/")
	return strings.TrimPrefix(object, "/")
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

type objectReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

type notFoundAwareObject struct {
	object objectReader
}

func (o *notFoundAwareObject) Read(p []byte) (int, error) {
	n, err := o.object.Read(p)
	if err != nil && isNotFound(err) {
		err = storage.ErrNotFound
	}
	return n, err
}

func (o *notFoundAwareObject) ReadAt(p []byte, offset int64) (int, error) {
	n, err := o.object.ReadAt(p, offset)
	if err != nil && isNotFound(err) {
		err = storage.ErrNotFound
	}
	return n, err
}

func (o *notFoundAwareObject) Seek(offset int64, whence int) (int64, error) {
	pos, err := o.object.Seek(offset, whence)
	if err != nil && isNotFound(err) {
		err = storage.ErrNotFound
	}
	return pos, err
}

func (o *notFoundAwareObject) Close() error {
	if o.object == nil {
		return nil
	}
	return o.object.Close()
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

func (s *Store) wrapError(err error, msg string) error {
	if err == nil {
		return nil
	}
	retryable := isRetryable(err)
	if msg != "" {
		err = fmt.Errorf("%s: %w", msg, err)
	}
	if retryable {
		return storage.NewTransientError(err)
	}
	return err
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if isNetworkConnectionError(err) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsTemporary {
			return true
		}
		if isNetworkConnectionError(err) {
			return true
		}
	}
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode >= http.StatusInternalServerError && resp.StatusCode != 0 {
		return true
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusRequestTimeout, 0:
		if resp.StatusCode != 0 {
			return true
		}
	}
	return false
}

func isNetworkConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if isNetworkConnectionError(opErr.Err) {
			return true
		}
	}
	return false
}
