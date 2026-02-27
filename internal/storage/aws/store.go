package aws

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

// Config controls the behaviour of the AWS S3 storage backend.
type Config struct {
	Endpoint                 string
	Region                   string
	Bucket                   string
	Prefix                   string
	Insecure                 bool
	PartSize                 int64
	SmallEncryptBufferBudget int64
	ServerSideEnc            string
	KMSKeyID                 string
	Crypto                   *storage.Crypto
}

// Store implements storage.Backend backed by AWS S3.
type Store struct {
	client *s3.Client
	cfg    Config
	crypto *storage.Crypto
	budget *byteBudget
}

const (
	awsSmallEncryptedStateLimit = 4 << 20 // 4 MiB; aligns with default spool threshold
	awsOpTimeout                = 5 * time.Minute
)

const descriptorMetadataKey = "lockd-descriptor"

// DefaultNamespaceConfig returns the preferred namespace defaults for AWS backends.
func (s *Store) DefaultNamespaceConfig() namespaces.Config {
	cfg := namespaces.DefaultConfig()
	cfg.Query.Preferred = search.EngineIndex
	cfg.Query.Fallback = namespaces.FallbackNone
	return cfg
}

type countingWriter struct {
	n int64
	w io.Writer
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	if cw.w == nil {
		return len(p), nil
	}
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

type byteBudget struct {
	max  int64
	used atomic.Int64
}

func newByteBudget(max int64) *byteBudget {
	if max <= 0 {
		return nil
	}
	return &byteBudget{max: max}
}

func (b *byteBudget) tryAcquire(size int64) (func(), bool) {
	if b == nil || size <= 0 {
		return func() {}, true
	}
	if size > b.max {
		return nil, false
	}
	for {
		cur := b.used.Load()
		if cur+size > b.max {
			return nil, false
		}
		if b.used.CompareAndSwap(cur, cur+size) {
			return func() { b.used.Add(-size) }, true
		}
	}
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
		return nil, fmt.Errorf("aws: decode descriptor: %w", err)
	}
	return decoded, nil
}

// New constructs a Store using the provided configuration.
func New(cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("aws: bucket is required")
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("aws: region is required")
	}
	if cfg.Endpoint != "" {
		cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	}
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")

	httpClient := &http.Client{Transport: defaultTransport(cfg.Insecure)}
	awsCfg, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("aws: load config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			endpoint := cfg.Endpoint
			if !strings.Contains(endpoint, "://") {
				scheme := "https"
				if cfg.Insecure {
					scheme = "http"
				}
				endpoint = scheme + "://" + endpoint
			}
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	return &Store{client: client, cfg: cfg, crypto: cfg.Crypto, budget: newByteBudget(cfg.SmallEncryptBufferBudget)}, nil
}

func defaultTransport(insecure bool) http.RoundTripper {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return http.DefaultTransport
	}
	clone := base.Clone()
	if clone.MaxIdleConns == 0 {
		clone.MaxIdleConns = 256
	}
	if clone.MaxIdleConnsPerHost == 0 {
		clone.MaxIdleConnsPerHost = 64
	}
	if clone.IdleConnTimeout == 0 {
		clone.IdleConnTimeout = 90 * time.Second
	}
	if clone.TLSHandshakeTimeout == 0 {
		clone.TLSHandshakeTimeout = 10 * time.Second
	}
	if clone.ExpectContinueTimeout == 0 {
		clone.ExpectContinueTimeout = 1 * time.Second
	}
	if insecure {
		clone.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return clone
}

// Close satisfies storage.Backend and is a no-op for the AWS client.
func (s *Store) Close() error { return nil }

// BackendHash returns the stable identity hash for this backend.
func (s *Store) BackendHash(ctx context.Context) (string, error) {
	endpoint := strings.TrimSpace(s.cfg.Endpoint)
	bucket := strings.TrimSpace(s.cfg.Bucket)
	prefix := strings.Trim(s.cfg.Prefix, "/")
	desc := fmt.Sprintf("aws|endpoint=%s|bucket=%s|prefix=%s", endpoint, bucket, prefix)
	result, err := storage.ResolveBackendHash(ctx, s, desc, s.crypto)
	return result.Hash, err
}

// Client exposes the underlying AWS client for diagnostics.
func (s *Store) Client() *s3.Client {
	return s.client
}

// Config returns a copy of the configuration used to build the store.
func (s *Store) Config() Config {
	return s.cfg
}

func (s *Store) loggers(ctx context.Context) (pslog.Logger, pslog.Logger) {
	logger := pslog.LoggerFromContext(ctx)
	return logger, logger
}

func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= awsOpTimeout {
			return ctx, func() {}
		}
	}
	return context.WithTimeout(ctx, awsOpTimeout)
}

// BucketExists returns whether the configured bucket exists.
func (s *Store) BucketExists(ctx context.Context) (bool, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.cfg.Bucket)})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// LoadMeta downloads the metadata object for key and returns its ETag.
func (s *Store) LoadMeta(ctx context.Context, namespace, key string) (storage.LoadMetaResult, error) {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	start := time.Now()
	object, err := s.metaObject(namespace, key)
	if err != nil {
		logger.Debug("aws.load_meta.resolve_error", "namespace", namespace, "key", key, "error", err)
		return storage.LoadMetaResult{}, err
	}
	verbose.Trace("aws.load_meta.begin", "namespace", namespace, "key", key, "object", object)

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		if isNotFound(err) {
			verbose.Debug("aws.load_meta.not_found", "key", key, "object", object, "elapsed", time.Since(start))
			return storage.LoadMetaResult{}, storage.ErrNotFound
		}
		logger.Debug("aws.load_meta.get_error", "key", key, "object", object, "error", err)
		return storage.LoadMetaResult{}, s.wrapError(err, "aws: get meta")
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		logger.Debug("aws.load_meta.read_error", "key", key, "object", object, "error", err)
		return storage.LoadMetaResult{}, fmt.Errorf("aws: read meta: %w", err)
	}
	meta, err := storage.UnmarshalMeta(payload, s.crypto)
	if err != nil {
		logger.Debug("aws.load_meta.decode_error", "key", key, "object", object, "error", err)
		return storage.LoadMetaResult{}, err
	}
	etag := stripETag(aws.ToString(resp.ETag))
	leaseOwner := ""
	leaseExpires := int64(0)
	if meta.Lease != nil {
		leaseOwner = meta.Lease.Owner
		leaseExpires = meta.Lease.ExpiresAtUnix
	}
	verbose.Debug("aws.load_meta.success",
		"key", key,
		"object", object,
		"meta_etag", etag,
		"version", meta.Version,
		"state_etag", meta.StateETag,
		"lease_owner", leaseOwner,
		"lease_expires_at", leaseExpires,
		"elapsed", time.Since(start),
	)
	return storage.LoadMetaResult{Meta: meta, ETag: etag}, nil
}

// ScanMetaSummaries enumerates key+summary rows for the namespace.
func (s *Store) ScanMetaSummaries(ctx context.Context, req storage.ScanMetaSummariesRequest, visit func(storage.ScanMetaSummaryRow) error) (storage.ScanMetaSummariesResult, error) {
	return storage.ScanMetaSummariesFallback(ctx, s, req, visit)
}

// StoreMeta uploads the metadata protobuf, applying conditional copy semantics via expectedETag.
func (s *Store) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	start := time.Now()
	object, err := s.metaObject(namespace, key)
	if err != nil {
		logger.Debug("aws.store_meta.resolve_error", "namespace", namespace, "key", key, "error", err)
		return "", err
	}
	leaseOwner := ""
	leaseExpires := int64(0)
	if meta != nil && meta.Lease != nil {
		leaseOwner = meta.Lease.Owner
		leaseExpires = meta.Lease.ExpiresAtUnix
	}
	verbose.Trace("aws.store_meta.begin",
		"namespace", namespace,
		"key", key,
		"object", object,
		"expected_etag", expectedETag,
		"lease_owner", leaseOwner,
		"lease_expires_at", leaseExpires,
	)
	payload, err := storage.MarshalMeta(meta, s.crypto)
	if err != nil {
		logger.Debug("aws.store_meta.marshal_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return "", err
	}
	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.Bucket),
		Key:         aws.String(object),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String(s.metaContentType()),
		Metadata:    map[string]string{},
	}
	input.ContentLength = aws.Int64(int64(len(payload)))
	if expectedETag != "" {
		input.IfMatch = aws.String(expectedETag)
	} else {
		input.IfNoneMatch = aws.String("*")
	}
	applySSEToPut(input, s.cfg.ServerSideEnc, s.cfg.KMSKeyID)
	_, err = s.client.PutObject(ctx, input)
	if err != nil {
		if isPreconditionFailed(err) {
			logger.Debug("aws.store_meta.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag)
			return "", storage.ErrCASMismatch
		}
		if isNotFound(err) {
			logger.Debug("aws.store_meta.not_found", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag)
			return "", storage.ErrNotFound
		}
		logger.Debug("aws.store_meta.put_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return "", s.wrapError(err, "aws: put meta")
	}
	stat, statErr := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(object)})
	newETag := ""
	if statErr == nil {
		newETag = stripETag(aws.ToString(stat.ETag))
	}
	verbose.Debug("aws.store_meta.success",
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
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	start := time.Now()
	object, err := s.metaObject(namespace, key)
	if err != nil {
		logger.Debug("aws.delete_meta.resolve_error", "namespace", namespace, "key", key, "error", err)
		return err
	}
	verbose.Trace("aws.delete_meta.begin", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag)

	if expectedETag != "" {
		stat, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(object)})
		if err != nil {
			if isNotFound(err) {
				verbose.Debug("aws.delete_meta.not_found", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
				return storage.ErrNotFound
			}
			logger.Debug("aws.delete_meta.stat_error", "namespace", namespace, "key", key, "object", object, "error", err)
			return fmt.Errorf("aws: stat meta: %w", err)
		}
		if stripETag(aws.ToString(stat.ETag)) != expectedETag {
			logger.Debug("aws.delete_meta.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag, "current_etag", stripETag(aws.ToString(stat.ETag)))
			return storage.ErrCASMismatch
		}
	}
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(object)})
	if err != nil {
		logger.Debug("aws.delete_meta.remove_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return s.wrapError(err, "aws: remove meta")
	}
	verbose.Debug("aws.delete_meta.success", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
	return nil
}

// ListMetaKeys enumerates metadata documents within the provided namespace.
func (s *Store) ListMetaKeys(ctx context.Context, namespace string) ([]string, error) {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	start := time.Now()
	fullPrefix := s.withPrefix(path.Join(namespace, "meta")) + "/"
	verbose.Trace("aws.list_meta_keys.begin", "namespace", namespace, "prefix", fullPrefix)
	var keys []string
	var token *string
	for {
		resp, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.cfg.Bucket),
			Prefix:            aws.String(fullPrefix),
			ContinuationToken: token,
		})
		if err != nil {
			logger.Debug("aws.list_meta_keys.error", "namespace", namespace, "error", err)
			return nil, s.wrapError(err, "aws: list meta")
		}
		for _, object := range resp.Contents {
			rel := strings.TrimPrefix(aws.ToString(object.Key), fullPrefix)
			if rel == "" || strings.HasSuffix(rel, "/") {
				continue
			}
			if strings.HasPrefix(rel, "inflight/") {
				continue
			}
			if !strings.HasSuffix(rel, ".pb") {
				continue
			}
			entry := strings.TrimPrefix(strings.TrimSuffix(rel, ".pb"), "/")
			if entry == "" {
				continue
			}
			keys = append(keys, entry)
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}
	verbose.Debug("aws.list_meta_keys.success", "namespace", namespace, "count", len(keys), "elapsed", time.Since(start))
	return keys, nil
}

// ReadState streams the state object for key within the namespace and returns metadata.
func (s *Store) ReadState(ctx context.Context, namespace, key string) (storage.ReadStateResult, error) {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	start := time.Now()
	object, err := s.stateObject(namespace, key)
	if err != nil {
		cancel()
		logger.Debug("aws.read_state.resolve_error", "namespace", namespace, "key", key, "error", err)
		return storage.ReadStateResult{}, err
	}
	verbose.Trace("aws.read_state.begin", "namespace", namespace, "key", key, "object", object)

	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		cancel()
		if isNotFound(err) {
			verbose.Debug("aws.read_state.not_found", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
			return storage.ReadStateResult{}, storage.ErrNotFound
		}
		logger.Debug("aws.read_state.get_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return storage.ReadStateResult{}, s.wrapError(err, "aws: get state")
	}

	etag := stripETag(aws.ToString(resp.ETag))
	infoOut := &storage.StateInfo{
		Size:       aws.ToInt64(resp.ContentLength),
		CipherSize: aws.ToInt64(resp.ContentLength),
		ETag:       etag,
		Version:    0,
		ModifiedAt: time.Now().UTC().Unix(),
	}
	if resp.LastModified != nil {
		infoOut.ModifiedAt = resp.LastModified.Unix()
	}
	if desc, err := decodeDescriptor(resp.Metadata); err == nil && len(desc) > 0 {
		infoOut.Descriptor = append([]byte(nil), desc...)
	} else if err != nil {
		resp.Body.Close()
		cancel()
		return storage.ReadStateResult{}, err
	}
	encrypted := s.crypto != nil && s.crypto.Enabled()
	descriptor := append([]byte(nil), infoOut.Descriptor...)
	if len(descriptor) == 0 {
		if descFromCtx, ok := storage.StateDescriptorFromContext(ctx); ok && len(descFromCtx) > 0 {
			descriptor = append([]byte(nil), descFromCtx...)
		}
	}
	if len(descriptor) > 0 && len(infoOut.Descriptor) == 0 {
		infoOut.Descriptor = append([]byte(nil), descriptor...)
	}
	if plain, ok := storage.StatePlaintextSizeFromContext(ctx); ok && plain > 0 {
		infoOut.Size = plain
	}
	defaultCtx := storage.StateObjectContext(path.Join(namespace, key))
	objectCtx := storage.StateObjectContextFromContext(ctx, defaultCtx)
	if encrypted {
		if len(descriptor) == 0 {
			resp.Body.Close()
			cancel()
			logger.Debug("aws.read_state.missing_descriptor", "namespace", namespace, "key", key)
			return storage.ReadStateResult{}, fmt.Errorf("aws: missing state descriptor for %q", key)
		}
		mat, err := s.crypto.MaterialFromDescriptor(objectCtx, descriptor)
		if err != nil {
			resp.Body.Close()
			cancel()
			logger.Debug("aws.read_state.material_error", "namespace", namespace, "key", key, "error", err)
			return storage.ReadStateResult{}, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(resp.Body, mat)
		if err != nil {
			resp.Body.Close()
			cancel()
			logger.Debug("aws.read_state.decrypt_error", "namespace", namespace, "key", key, "error", err)
			return storage.ReadStateResult{}, err
		}
		verbose.Debug("aws.read_state.success",
			"namespace", namespace,
			"key", key,
			"object", object,
			"etag", etag,
			"size", infoOut.Size,
			"cipher_size", aws.ToInt64(resp.ContentLength),
			"elapsed", time.Since(start),
		)
		return storage.ReadStateResult{Reader: wrapReadCloser(decReader, cancel), Info: infoOut}, nil
	}
	verbose.Debug("aws.read_state.success",
		"namespace", namespace,
		"key", key,
		"object", object,
		"etag", etag,
		"size", infoOut.Size,
		"cipher_size", aws.ToInt64(resp.ContentLength),
		"elapsed", time.Since(start),
	)
	return storage.ReadStateResult{Reader: wrapReadCloser(resp.Body, cancel), Info: infoOut}, nil
}

// WriteState uploads a new state object, optionally guarding against stale ETags.
func (s *Store) WriteState(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	start := time.Now()
	object, err := s.stateObject(namespace, key)
	if err != nil {
		logger.Debug("aws.write_state.resolve_error", "namespace", namespace, "key", key, "error", err)
		return nil, err
	}
	verbose.Trace("aws.write_state.begin", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag)

	contentType := storage.ContentTypeJSON
	length := int64(-1)
	plainSize := int64(-1)
	if plain, ok := storage.StatePlaintextSizeFromContext(ctx); ok && plain > 0 {
		plainSize = plain
	}
	reader := body
	defaultCtx := storage.StateObjectContext(path.Join(namespace, key))
	objectCtx := storage.StateObjectContextFromContext(ctx, defaultCtx)
	if seeker, ok := body.(io.Seeker); ok {
		if current, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			if end, err := seeker.Seek(0, io.SeekEnd); err == nil {
				length = end - current
				if plainSize <= 0 {
					plainSize = length
				}
				_, _ = seeker.Seek(current, io.SeekStart)
			}
		}
	}
	if plainSize > 0 && length < 0 && (s.crypto == nil || !s.crypto.Enabled()) {
		length = plainSize
	}

	encrypted := s.crypto != nil && s.crypto.Enabled()
	var descriptor []byte
	var plainBytes atomic.Int64
	if encrypted {
		contentType = storage.ContentTypeJSONEncrypted
		descFromCtx, ok := storage.StateDescriptorFromContext(ctx)
		var mat kryptograf.Material
		if ok && len(descFromCtx) > 0 {
			descriptor = append([]byte(nil), descFromCtx...)
			mat, err = s.crypto.MaterialFromDescriptor(objectCtx, descriptor)
			if err != nil {
				logger.Debug("aws.write_state.descriptor_error", "namespace", namespace, "key", key, "object", object, "error", err)
				return nil, err
			}
		} else {
			var minted storage.MaterialResult
			minted, err = s.crypto.MintMaterial(objectCtx)
			if err != nil {
				logger.Debug("aws.write_state.mint_descriptor_error", "namespace", namespace, "key", key, "object", object, "error", err)
				return nil, err
			}
			descriptor = append([]byte(nil), minted.Descriptor...)
			mat = minted.Material
		}
		usedBuffer := false
		if plainSize > 0 && plainSize <= awsSmallEncryptedStateLimit {
			release, ok := s.reserveExactBuffer(plainSize)
			if ok {
				defer release()
				var cipherBuf bytes.Buffer
				cipherBuf.Grow(int(plainSize) + 256)
				encWriter, err := s.crypto.EncryptWriterForMaterial(&cipherBuf, mat)
				if err != nil {
					logger.Debug("aws.write_state.encrypt_writer_error", "namespace", namespace, "key", key, "object", object, "error", err)
					return nil, err
				}
				cw := &countingWriter{w: encWriter}
				if _, err := io.Copy(cw, body); err != nil {
					encWriter.Close()
					logger.Debug("aws.write_state.encrypt_write_error", "namespace", namespace, "key", key, "object", object, "error", err)
					return nil, err
				}
				if err := encWriter.Close(); err != nil {
					logger.Debug("aws.write_state.encrypt_close_error", "namespace", namespace, "key", key, "object", object, "error", err)
					return nil, err
				}
				plainBytes.Store(cw.n)
				reader = bytes.NewReader(cipherBuf.Bytes())
				length = int64(cipherBuf.Len())
				usedBuffer = true
			}
		}
		if !usedBuffer {
			pr, pw := io.Pipe()
			encWriter, err := s.crypto.EncryptWriterForMaterial(pw, mat)
			if err != nil {
				pw.Close()
				logger.Debug("aws.write_state.encrypt_writer_error", "namespace", namespace, "key", key, "object", object, "error", err)
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
		}
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.Bucket),
		Key:         aws.String(object),
		Body:        reader,
		ContentType: aws.String(contentType),
		Metadata:    map[string]string{},
	}
	if length >= 0 {
		input.ContentLength = aws.Int64(length)
	}
	if len(descriptor) > 0 {
		input.Metadata[descriptorMetadataKey] = encodeDescriptor(descriptor)
	}
	if opts.ExpectedETag != "" {
		input.IfMatch = aws.String(opts.ExpectedETag)
	} else if opts.IfNotExists {
		input.IfNoneMatch = aws.String("*")
	}
	applySSEToPut(input, s.cfg.ServerSideEnc, s.cfg.KMSKeyID)

	out, err := s.client.PutObject(ctx, input)
	if err != nil {
		if isPreconditionFailed(err) {
			logger.Debug("aws.write_state.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag)
			return nil, storage.ErrCASMismatch
		}
		logger.Debug("aws.write_state.put_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return nil, s.wrapError(err, "aws: put state")
	}
	etag := stripETag(aws.ToString(out.ETag))

	bytesWritten := length
	if bytesWritten < 0 {
		bytesWritten = plainSize
	}
	if encrypted {
		if v := plainBytes.Load(); v > 0 {
			bytesWritten = v
		}
	}

	result := &storage.PutStateResult{
		BytesWritten: bytesWritten,
		NewETag:      etag,
		Descriptor:   append([]byte(nil), descriptor...),
	}
	verbose.Debug("aws.write_state.success",
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
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	start := time.Now()
	object, err := s.stateObject(namespace, key)
	if err != nil {
		logger.Debug("aws.remove_state.resolve_error", "namespace", namespace, "key", key, "error", err)
		return err
	}
	verbose.Trace("aws.remove_state.begin", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag)

	input := &s3.DeleteObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(object)}
	if expectedETag != "" {
		input.IfMatch = aws.String(expectedETag)
	}
	_, err = s.client.DeleteObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			verbose.Debug("aws.remove_state.not_found", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
			return storage.ErrNotFound
		}
		if isPreconditionFailed(err) {
			logger.Debug("aws.remove_state.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", expectedETag)
			return storage.ErrCASMismatch
		}
		logger.Debug("aws.remove_state.remove_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return s.wrapError(err, "aws: remove state")
	}
	verbose.Debug("aws.remove_state.success", "namespace", namespace, "key", key, "object", object, "elapsed", time.Since(start))
	return nil
}

// ListObjects enumerates raw objects within the namespace matching opts.Prefix.
func (s *Store) ListObjects(ctx context.Context, namespace string, opts storage.ListOptions) (*storage.ListResult, error) {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	start := time.Now()
	verbose.Trace("aws.list_objects.begin",
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

	result := &storage.ListResult{}
	lastKey := ""
	limit := opts.Limit

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.cfg.Bucket),
		Prefix: aws.String(actualPrefix),
	}
	if opts.StartAfter != "" {
		input.StartAfter = aws.String(nsRoot + strings.TrimPrefix(opts.StartAfter, "/"))
	}
	if limit > 0 {
		input.MaxKeys = aws.Int32(int32(limit + 1))
		resp, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			logger.Debug("aws.list_objects.error", "namespace", namespace, "error", err)
			return nil, s.wrapError(err, "aws: list objects")
		}
		for _, object := range resp.Contents {
			logicalKey := strings.TrimPrefix(aws.ToString(object.Key), nsRoot)
			logicalKey = strings.TrimPrefix(logicalKey, "/")
			if nsRoot != "" && logicalKey == aws.ToString(object.Key) {
				continue
			}
			if len(result.Objects) >= limit {
				result.Truncated = true
				break
			}
			info := storage.ObjectInfo{
				Key:          logicalKey,
				ETag:         stripETag(aws.ToString(object.ETag)),
				Size:         aws.ToInt64(object.Size),
				LastModified: aws.ToTime(object.LastModified),
			}
			result.Objects = append(result.Objects, info)
			lastKey = logicalKey
		}
		if lastKey != "" {
			result.NextStartAfter = lastKey
		}
		if result.Truncated {
			verbose.Debug("aws.list_objects.success",
				"namespace", namespace,
				"prefix", opts.Prefix,
				"count", len(result.Objects),
				"truncated", result.Truncated,
				"elapsed", time.Since(start),
			)
			return result, nil
		}
		if aws.ToBool(resp.IsTruncated) && lastKey != "" {
			result.Truncated = true
			result.NextStartAfter = lastKey
		}
		verbose.Debug("aws.list_objects.success",
			"namespace", namespace,
			"prefix", opts.Prefix,
			"count", len(result.Objects),
			"truncated", result.Truncated,
			"elapsed", time.Since(start),
		)
		return result, nil
	}

	var token *string
	for {
		input.ContinuationToken = token
		resp, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			logger.Debug("aws.list_objects.error", "namespace", namespace, "error", err)
			return nil, s.wrapError(err, "aws: list objects")
		}
		for _, object := range resp.Contents {
			logicalKey := strings.TrimPrefix(aws.ToString(object.Key), nsRoot)
			if logicalKey == aws.ToString(object.Key) {
				continue
			}
			info := storage.ObjectInfo{
				Key:          logicalKey,
				ETag:         stripETag(aws.ToString(object.ETag)),
				Size:         aws.ToInt64(object.Size),
				LastModified: aws.ToTime(object.LastModified),
			}
			result.Objects = append(result.Objects, info)
			lastKey = logicalKey
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}
	if lastKey != "" {
		result.NextStartAfter = lastKey
	}
	verbose.Debug("aws.list_objects.success",
		"namespace", namespace,
		"prefix", opts.Prefix,
		"count", len(result.Objects),
		"truncated", result.Truncated,
		"elapsed", time.Since(start),
	)
	return result, nil
}

// ListNamespaces enumerates namespaces stored under the configured prefix.
func (s *Store) ListNamespaces(ctx context.Context) ([]string, error) {
	if s == nil || s.client == nil {
		return nil, storage.ErrNotImplemented
	}
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	prefix := strings.Trim(s.cfg.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	set := make(map[string]struct{})
	var token *string
	for {
		resp, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.cfg.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, s.wrapError(err, "aws: list namespaces")
		}
		for _, object := range resp.Contents {
			rel := strings.TrimPrefix(aws.ToString(object.Key), prefix)
			if rel == "" || rel == aws.ToString(object.Key) {
				continue
			}
			parts := strings.SplitN(rel, "/", 2)
			if len(parts) == 0 {
				continue
			}
			ns := strings.TrimSpace(parts[0])
			if ns == "" {
				continue
			}
			set[ns] = struct{}{}
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}

	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// GetObject downloads the raw payload for key within the namespace.
func (s *Store) GetObject(ctx context.Context, namespace, key string) (storage.GetObjectResult, error) {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	object := s.objectKey(namespace, key)
	verbose.Trace("aws.get_object.begin", "namespace", namespace, "key", key, "object", object)
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		cancel()
		if isNotFound(err) {
			verbose.Debug("aws.get_object.not_found", "namespace", namespace, "key", key, "object", object)
			return storage.GetObjectResult{}, storage.ErrNotFound
		}
		logger.Debug("aws.get_object.get_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return storage.GetObjectResult{}, s.wrapError(err, "aws: get object")
	}
	meta := &storage.ObjectInfo{
		Key:          key,
		ETag:         stripETag(aws.ToString(resp.ETag)),
		Size:         aws.ToInt64(resp.ContentLength),
		LastModified: aws.ToTime(resp.LastModified),
		ContentType:  aws.ToString(resp.ContentType),
	}
	if desc, err := decodeDescriptor(resp.Metadata); err == nil && len(desc) > 0 {
		meta.Descriptor = append([]byte(nil), desc...)
	} else if err != nil {
		resp.Body.Close()
		cancel()
		return storage.GetObjectResult{}, err
	}
	if plain, ok := storage.ObjectPlaintextSizeFromContext(ctx); ok && plain > 0 {
		meta.Size = plain
	}
	verbose.Debug("aws.get_object.success", "namespace", namespace, "key", key, "object", object, "etag", meta.ETag, "size", meta.Size)
	return storage.GetObjectResult{Reader: wrapReadCloser(resp.Body, cancel), Info: meta}, nil
}

// PutObject uploads raw object bytes with conditional guards within the namespace.
func (s *Store) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	object := s.objectKey(namespace, key)
	verbose.Trace("aws.put_object.begin",
		"namespace", namespace,
		"key", key,
		"object", object,
		"expected_etag", opts.ExpectedETag,
		"if_not_exists", opts.IfNotExists,
	)
	contentType := opts.ContentType
	if contentType == "" {
		contentType = storage.ContentTypeOctetStream
	}
	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.cfg.Bucket),
		Key:         aws.String(object),
		Body:        body,
		ContentType: aws.String(contentType),
		Metadata:    map[string]string{},
	}
	if len(opts.Descriptor) > 0 {
		input.Metadata[descriptorMetadataKey] = encodeDescriptor(opts.Descriptor)
	}
	if opts.ExpectedETag != "" {
		input.IfMatch = aws.String(opts.ExpectedETag)
	} else if opts.IfNotExists {
		input.IfNoneMatch = aws.String("*")
	}
	applySSEToPut(input, s.cfg.ServerSideEnc, s.cfg.KMSKeyID)

	length := int64(-1)
	if seeker, ok := body.(io.Seeker); ok {
		if current, err := seeker.Seek(0, io.SeekCurrent); err == nil {
			if end, err := seeker.Seek(0, io.SeekEnd); err == nil {
				length = end - current
				_, _ = seeker.Seek(current, io.SeekStart)
			}
		}
	}
	var releaseBudget func()
	if length < 0 {
		reader, newLen, release, ok, err := s.maybeBufferObject(body)
		if err != nil {
			logger.Debug("aws.put_object.buffer_error", "namespace", namespace, "key", key, "object", object, "error", err)
			return nil, err
		}
		if ok {
			body = reader
			length = newLen
			releaseBudget = release
			input.Body = body
		}
	}
	if length >= 0 {
		input.ContentLength = aws.Int64(length)
	}
	if releaseBudget != nil {
		defer releaseBudget()
	}

	out, err := s.client.PutObject(ctx, input)
	if err != nil {
		switch classifyPutObjectError(err, opts.ExpectedETag != "") {
		case storage.ErrCASMismatch:
			logger.Debug("aws.put_object.cas_mismatch", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag)
			return nil, storage.ErrCASMismatch
		case storage.ErrNotFound:
			logger.Debug("aws.put_object.not_found", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag)
			return nil, storage.ErrNotFound
		default:
			logger.Debug("aws.put_object.put_error", "namespace", namespace, "key", key, "object", object, "error", err)
			return nil, s.wrapError(err, "aws: put object")
		}
	}
	etag := stripETag(aws.ToString(out.ETag))
	meta := &storage.ObjectInfo{
		Key:          key,
		ETag:         etag,
		Size:         length,
		LastModified: time.Now().UTC(),
		ContentType:  contentType,
		Descriptor:   append([]byte(nil), opts.Descriptor...),
	}
	if meta.Size < 0 {
		meta.Size = 0
	}
	verbose.Debug("aws.put_object.success", "namespace", namespace, "key", key, "object", object, "etag", meta.ETag, "size", meta.Size)
	return meta, nil
}

// DeleteObject removes an object with optional CAS within the namespace.
func (s *Store) DeleteObject(ctx context.Context, namespace, key string, opts storage.DeleteObjectOptions) error {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	object := s.objectKey(namespace, key)
	verbose.Trace("aws.delete_object.begin", "namespace", namespace, "key", key, "object", object, "expected_etag", opts.ExpectedETag, "ignore_not_found", opts.IgnoreNotFound)
	input := &s3.DeleteObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(object)}
	if opts.ExpectedETag != "" {
		input.IfMatch = aws.String(opts.ExpectedETag)
	}
	_, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			if opts.IgnoreNotFound {
				return nil
			}
			return storage.ErrNotFound
		}
		if isPreconditionFailed(err) {
			return storage.ErrCASMismatch
		}
		logger.Debug("aws.delete_object.remove_error", "namespace", namespace, "key", key, "object", object, "error", err)
		return s.wrapError(err, "aws: delete object")
	}
	verbose.Debug("aws.delete_object.success", "namespace", namespace, "key", key, "object", object)
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
	if namespace == "" && strings.TrimSpace(key) == "" {
		return s.withPrefix("")
	}
	combined := path.Join(namespace, strings.TrimPrefix(key, "/"))
	if combined == "." {
		combined = ""
	}
	return s.withPrefix(strings.TrimPrefix(combined, "/"))
}

func (s *Store) withPrefix(p string) string {
	if s.cfg.Prefix == "" {
		return p
	}
	return path.Join(s.cfg.Prefix, p)
}

func wrapReadCloser(rc io.ReadCloser, cancel context.CancelFunc) io.ReadCloser {
	if cancel == nil {
		return rc
	}
	return &cancelReadCloser{ReadCloser: rc, cancel: cancel}
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func applySSEToPut(input *s3.PutObjectInput, mode, keyID string) {
	switch strings.ToUpper(mode) {
	case "AES256":
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	case "AWS:KMS", "KMS":
		input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		if keyID != "" {
			input.SSEKMSKeyId = aws.String(keyID)
		}
	}
}

func applySSEToCopy(input *s3.CopyObjectInput, mode, keyID string) {
	switch strings.ToUpper(mode) {
	case "AES256":
		input.ServerSideEncryption = types.ServerSideEncryptionAes256
	case "AWS:KMS", "KMS":
		input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
		if keyID != "" {
			input.SSEKMSKeyId = aws.String(keyID)
		}
	}
}

func (s *Store) reserveExactBuffer(size int64) (func(), bool) {
	if size <= 0 || s.budget == nil {
		return nil, false
	}
	return s.budget.tryAcquire(size)
}

func (s *Store) reserveUpTo(limit int64) (func(), int64, bool) {
	if limit <= 0 || s.budget == nil {
		return nil, 0, false
	}
	size := limit
	if s.budget.max < size {
		size = s.budget.max
	}
	if size <= 0 {
		return nil, 0, false
	}
	release, ok := s.budget.tryAcquire(size)
	if !ok {
		return nil, 0, false
	}
	return release, size, true
}

func (s *Store) maybeBufferObject(body io.Reader) (io.Reader, int64, func(), bool, error) {
	release, limit, ok := s.reserveUpTo(awsSmallEncryptedStateLimit)
	if !ok {
		return nil, 0, nil, false, nil
	}
	buf, complete, err := readUpTo(body, limit)
	if err != nil {
		release()
		return nil, 0, nil, false, err
	}
	reader := bytes.NewReader(buf)
	if complete {
		return reader, int64(len(buf)), release, true, nil
	}
	return io.MultiReader(reader, body), -1, release, true, nil
}

func readUpTo(r io.Reader, limit int64) ([]byte, bool, error) {
	if limit <= 0 {
		return nil, false, nil
	}
	lr := &io.LimitedReader{R: r, N: limit + 1}
	buf, err := io.ReadAll(lr)
	if err != nil {
		return buf, false, err
	}
	complete := len(buf) <= int(limit) && lr.N > 0
	return buf, complete, nil
}

func classifyPutObjectError(err error, hasExpectedETag bool) error {
	if err == nil {
		return nil
	}
	if isPreconditionFailed(err) {
		return storage.ErrCASMismatch
	}
	if hasExpectedETag && isNotFound(err) {
		return storage.ErrNotFound
	}
	return nil
}

func stripETag(etag string) string {
	return strings.Trim(etag, "\"")
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
	if status, ok := httpStatusCode(err); ok {
		if status >= http.StatusInternalServerError {
			return true
		}
		switch status {
		case http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusRequestTimeout:
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

func httpStatusCode(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		return statusErr.HTTPStatusCode(), true
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode(), true
	}
	return 0, false
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket":
			return true
		}
	}
	if status, ok := httpStatusCode(err); ok {
		return status == http.StatusNotFound
	}
	return false
}

func isPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict", "OperationAborted":
			return true
		}
	}
	if status, ok := httpStatusCode(err); ok {
		if status == http.StatusPreconditionFailed {
			return true
		}
		if status == http.StatusConflict {
			return true
		}
	}
	return false
}
