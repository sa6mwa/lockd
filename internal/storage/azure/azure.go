package azure

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/storage"
)

// Config controls connectivity to Azure Blob Storage.
type Config struct {
	Account    string
	AccountKey string
	Endpoint   string
	SASToken   string
	Container  string
	Prefix     string
	Crypto     *storage.Crypto
}

// Store implements storage.Backend backed by Azure Blob Storage.
type Store struct {
	client    *azblob.Client
	container string
	prefix    string
	crypto    *storage.Crypto
}

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

const (
	descriptorMetadataKey       = "lockd_descriptor"
	legacyDescriptorMetadataKey = "lockd-descriptor"
)

func (cr countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 && cr.n != nil {
		cr.n.Add(int64(n))
	}
	return n, err
}

func encodeAzureDescriptor(desc []byte) string {
	return hex.EncodeToString(desc)
}

func decodeAzureDescriptor(meta map[string]*string) ([]byte, error) {
	if meta == nil {
		return nil, nil
	}
	var candidate *string
	for key, val := range meta {
		if val == nil || *val == "" {
			continue
		}
		switch strings.ToLower(key) {
		case descriptorMetadataKey, legacyDescriptorMetadataKey:
			candidate = val
		}
	}
	if candidate == nil || *candidate == "" {
		return nil, nil
	}
	decoded, err := hex.DecodeString(*candidate)
	if err != nil {
		return nil, fmt.Errorf("azure: decode descriptor: %w", err)
	}
	return decoded, nil
}

// New constructs a Store using the provided configuration.
func New(cfg Config) (*Store, error) {
	if cfg.Account == "" {
		return nil, fmt.Errorf("azure: account is required")
	}
	if cfg.Container == "" {
		return nil, fmt.Errorf("azure: container is required")
	}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", cfg.Account)
	}
	var (
		client *azblob.Client
		err    error
	)
	if cfg.SASToken != "" {
		endpointWithSAS, serr := appendSASToken(endpoint, cfg.SASToken)
		if serr != nil {
			return nil, serr
		}
		client, err = azblob.NewClientWithNoCredential(endpointWithSAS, nil)
	} else {
		if cfg.AccountKey == "" {
			return nil, fmt.Errorf("azure: account key or SAS token required")
		}
		cred, credErr := azblob.NewSharedKeyCredential(cfg.Account, cfg.AccountKey)
		if credErr != nil {
			return nil, fmt.Errorf("azure: build credentials: %w", credErr)
		}
		client, err = azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("azure: create client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.CreateContainer(ctx, cfg.Container, nil)
	if err != nil {
		if !isContainerExists(err) {
			return nil, fmt.Errorf("azure: create container: %w", err)
		}
	}

	return &Store{
		client:    client,
		container: cfg.Container,
		prefix:    strings.Trim(cfg.Prefix, "/"),
		crypto:    cfg.Crypto,
	}, nil
}

// Client exposes the underlying Azure Blob client (primarily for diagnostics).
func (s *Store) Client() *azblob.Client {
	return s.client
}

func appendSASToken(endpoint, sas string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("azure: parse endpoint: %w", err)
	}
	sas = strings.TrimPrefix(sas, "?")
	if u.RawQuery != "" {
		u.RawQuery = u.RawQuery + "&" + sas
	} else {
		u.RawQuery = sas
	}
	return u.String(), nil
}

func isContainerExists(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == http.StatusConflict && strings.EqualFold(respErr.ErrorCode, "ContainerAlreadyExists")
	}
	return false
}

// Close satisfies storage.Backend by releasing resources held by Store (no-op for Azure).
func (s *Store) Close() error { return nil }

func (s *Store) prefixed(parts ...string) string {
	name := path.Join(parts...)
	if s.prefix == "" {
		return name
	}
	return path.Join(s.prefix, name)
}

const (
	metaContentTypePlain     = "application/x-protobuf"
	metaContentTypeEncrypted = "application/vnd.lockd+protobuf-encrypted"
)

func (s *Store) metaBlob(key string) string {
	return s.prefixed("meta", url.PathEscape(key)+".pb")
}

func (s *Store) stateBlob(key string) string {
	return s.prefixed("state", url.PathEscape(key)+".json")
}

func (s *Store) objectBlob(key string) string {
	return s.prefixed(strings.TrimPrefix(key, "/"))
}

func (s *Store) trimObjectPrefix(name string) string {
	if s.prefix == "" {
		return strings.TrimPrefix(name, "/")
	}
	prefix := strings.Trim(s.prefix, "/")
	name = strings.TrimPrefix(name, prefix+"/")
	return strings.TrimPrefix(name, "/")
}

func (s *Store) metaContentType() string {
	if s.crypto != nil && s.crypto.Enabled() {
		return metaContentTypeEncrypted
	}
	return metaContentTypePlain
}

// LoadMeta fetches the protobuf metadata document for key and returns its ETag.
func (s *Store) LoadMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	resp, err := s.client.DownloadStream(ctx, s.container, s.metaBlob(key), nil)
	if err != nil {
		if isNotFound(err) {
			return nil, "", storage.ErrNotFound
		}
		return nil, "", fmt.Errorf("azure: download meta: %w", err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("azure: read meta: %w", err)
	}
	meta, err := storage.UnmarshalMeta(payload, s.crypto)
	if err != nil {
		return nil, "", err
	}
	etag := ""
	if resp.ETag != nil {
		etag = string(*resp.ETag)
	}
	return meta, etag, nil
}

// StoreMeta writes the protobuf metadata document using conditional semantics when expectedETag is supplied.
func (s *Store) StoreMeta(ctx context.Context, key string, meta *storage.Meta, expectedETag string) (string, error) {
	payload, err := storage.MarshalMeta(meta, s.crypto)
	if err != nil {
		return "", err
	}
	opts := &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr(s.metaContentType()),
		},
	}
	if expectedETag != "" {
		opts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: to.Ptr(azcore.ETag(expectedETag)),
			},
		}
	} else {
		opts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: to.Ptr(azcore.ETag("*")),
			},
		}
	}
	resp, err := s.client.UploadStream(ctx, s.container, s.metaBlob(key), bytes.NewReader(payload), opts)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", storage.ErrCASMismatch
		}
		return "", fmt.Errorf("azure: upload meta: %w", err)
	}
	if resp.ETag == nil {
		return "", fmt.Errorf("azure: upload meta: missing etag")
	}
	return string(*resp.ETag), nil
}

// DeleteMeta removes the metadata document for key, respecting the expected ETag when provided.
func (s *Store) DeleteMeta(ctx context.Context, key string, expectedETag string) error {
	blobName := s.metaBlob(key)
	if expectedETag != "" {
		opts := &azblob.DeleteBlobOptions{
			AccessConditions: &blob.AccessConditions{
				ModifiedAccessConditions: &blob.ModifiedAccessConditions{
					IfMatch: to.Ptr(azcore.ETag(expectedETag)),
				},
			},
		}
		_, err := s.client.DeleteBlob(ctx, s.container, blobName, opts)
		if err != nil {
			if isPreconditionFailed(err) {
				return storage.ErrCASMismatch
			}
			if isNotFound(err) {
				return storage.ErrNotFound
			}
			return fmt.Errorf("azure: delete meta: %w", err)
		}
		return nil
	}
	_, err := s.client.DeleteBlob(ctx, s.container, blobName, nil)
	if err != nil {
		if isNotFound(err) {
			return storage.ErrNotFound
		}
		return fmt.Errorf("azure: delete meta: %w", err)
	}
	return nil
}

// ListMetaKeys enumerates the keys with stored metadata beneath the configured prefix.
func (s *Store) ListMetaKeys(ctx context.Context) ([]string, error) {
	prefix := s.prefixed("meta") + "/"
	pager := s.client.NewListBlobsFlatPager(s.container, &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	var keys []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure: list meta: %w", err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			name := strings.TrimPrefix(*item.Name, prefix)
			name = strings.TrimSuffix(name, ".pb")
			if name == "" {
				continue
			}
			key, err := url.PathUnescape(name)
			if err != nil {
				continue
			}
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// ReadState streams the JSON state blob for key and returns associated metadata.
func (s *Store) ReadState(ctx context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	resp, err := s.client.DownloadStream(ctx, s.container, s.stateBlob(key), nil)
	if err != nil {
		if isNotFound(err) {
			return nil, nil, storage.ErrNotFound
		}
		return nil, nil, fmt.Errorf("azure: download state: %w", err)
	}
	info := &storage.StateInfo{}
	if resp.ETag != nil {
		info.ETag = string(*resp.ETag)
	}
	if resp.ContentLength != nil {
		info.Size = *resp.ContentLength
		info.CipherSize = *resp.ContentLength
	}
	if resp.LastModified != nil {
		info.ModifiedAt = resp.LastModified.Unix()
	}
	encrypted := s.crypto != nil && s.crypto.Enabled()
	var descriptor []byte
	if descFromCtx, ok := storage.StateDescriptorFromContext(ctx); ok && len(descFromCtx) > 0 {
		descriptor = append([]byte(nil), descFromCtx...)
	} else if desc, err := decodeAzureDescriptor(resp.Metadata); err == nil && len(desc) > 0 {
		descriptor = append([]byte(nil), desc...)
	} else if err != nil {
		resp.Body.Close()
		return nil, nil, err
	}
	if len(descriptor) > 0 {
		info.Descriptor = append([]byte(nil), descriptor...)
	}
	if plain, ok := storage.StatePlaintextSizeFromContext(ctx); ok && plain > 0 {
		info.Size = plain
	}
	if encrypted {
		if len(descriptor) == 0 {
			resp.Body.Close()
			return nil, nil, fmt.Errorf("azure: missing state descriptor for %q", key)
		}
		mat, err := s.crypto.MaterialFromDescriptor(storage.StateObjectContext(key), descriptor)
		if err != nil {
			resp.Body.Close()
			return nil, nil, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(resp.Body, mat)
		if err != nil {
			resp.Body.Close()
			return nil, nil, err
		}
		return decReader, info, nil
	}
	return resp.Body, info, nil
}

// WriteState uploads a new state blob, optionally enforcing a previous ETag.
func (s *Store) WriteState(ctx context.Context, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	uploadOpts := &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr(storage.ContentTypeJSON),
		},
	}
	if opts.ExpectedETag != "" {
		uploadOpts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: to.Ptr(azcore.ETag(opts.ExpectedETag)),
			},
		}
	}
	encrypted := s.crypto != nil && s.crypto.Enabled()
	var descriptor []byte
	var plainBytes atomic.Int64
	reader := body
	if encrypted {
		uploadOpts.HTTPHeaders.BlobContentType = to.Ptr(storage.ContentTypeJSONEncrypted)
		descFromCtx, ok := storage.StateDescriptorFromContext(ctx)
		var mat kryptograf.Material
		var err error
		if ok && len(descFromCtx) > 0 {
			descriptor = append([]byte(nil), descFromCtx...)
			mat, err = s.crypto.MaterialFromDescriptor(storage.StateObjectContext(key), descriptor)
			if err != nil {
				return nil, err
			}
		} else {
			var descBytes []byte
			mat, descBytes, err = s.crypto.MintMaterial(storage.StateObjectContext(key))
			if err != nil {
				return nil, err
			}
			descriptor = append([]byte(nil), descBytes...)
		}
		encoded := encodeAzureDescriptor(descriptor)
		uploadOpts.Metadata = map[string]*string{descriptorMetadataKey: to.Ptr(encoded)}
		pr, pw := io.Pipe()
		encWriter, err := s.crypto.EncryptWriterForMaterial(pw, mat)
		if err != nil {
			pw.Close()
			return nil, err
		}
		go func() {
			defer pw.Close()
			reader := countingReader{r: body, n: &plainBytes}
			if _, err := io.Copy(encWriter, reader); err != nil {
				encWriter.Close()
				pw.CloseWithError(err)
				return
			}
			if err := encWriter.Close(); err != nil {
				pw.CloseWithError(err)
				return
			}
		}()
		reader = pr
	} else {
		reader = countingReader{r: body, n: &plainBytes}
	}
	resp, err := s.client.UploadStream(ctx, s.container, s.stateBlob(key), reader, uploadOpts)
	if err != nil {
		if isPreconditionFailed(err) {
			return nil, storage.ErrCASMismatch
		}
		return nil, fmt.Errorf("azure: upload state: %w", err)
	}
	if resp.ETag == nil {
		return nil, fmt.Errorf("azure: upload state: missing etag")
	}
	bytesWritten := plainBytes.Load()
	result := &storage.PutStateResult{
		BytesWritten: bytesWritten,
		NewETag:      string(*resp.ETag),
	}
	if len(descriptor) > 0 {
		result.Descriptor = append([]byte(nil), descriptor...)
	}
	return result, nil
}

// RemoveState deletes the state blob for key, applying CAS when expectedETag is set.
func (s *Store) RemoveState(ctx context.Context, key string, expectedETag string) error {
	opts := &azblob.DeleteBlobOptions{}
	if expectedETag != "" {
		opts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: to.Ptr(azcore.ETag(expectedETag)),
			},
		}
	}
	_, err := s.client.DeleteBlob(ctx, s.container, s.stateBlob(key), opts)
	if err != nil {
		if isPreconditionFailed(err) {
			return storage.ErrCASMismatch
		}
		if isNotFound(err) {
			return storage.ErrNotFound
		}
		return fmt.Errorf("azure: delete state: %w", err)
	}
	return nil
}

// ListObjects enumerates blobs stored under opts.Prefix.
func (s *Store) ListObjects(ctx context.Context, opts storage.ListOptions) (*storage.ListResult, error) {
	prefix := s.objectBlob(strings.TrimPrefix(opts.Prefix, "/"))
	pager := s.client.NewListBlobsFlatPager(s.container, &azblob.ListBlobsFlatOptions{
		Prefix: to.Ptr(prefix),
	})
	result := &storage.ListResult{}
	limit := opts.Limit
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}
	lastKey := ""
outer:
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure: list objects: %w", err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			logical := s.trimObjectPrefix(*item.Name)
			if opts.Prefix != "" && !strings.HasPrefix(logical, opts.Prefix) {
				continue
			}
			if opts.StartAfter != "" && logical <= opts.StartAfter {
				continue
			}
			if len(result.Objects) >= limit {
				result.Truncated = true
				break outer
			}
			info := storage.ObjectInfo{
				Key: logical,
			}
			if item.Properties != nil {
				if item.Properties.ETag != nil {
					info.ETag = string(*item.Properties.ETag)
				}
				if item.Properties.ContentLength != nil {
					info.Size = *item.Properties.ContentLength
				}
				if item.Properties.LastModified != nil {
					info.LastModified = item.Properties.LastModified.UTC()
				}
				if item.Properties.ContentType != nil {
					info.ContentType = *item.Properties.ContentType
				}
			}
			result.Objects = append(result.Objects, info)
			lastKey = logical
		}
	}
	if lastKey != "" {
		result.NextStartAfter = lastKey
	}
	return result, nil
}

// GetObject opens the blob referenced by key.
func (s *Store) GetObject(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	blobName := s.objectBlob(strings.TrimPrefix(key, "/"))
	resp, err := s.client.DownloadStream(ctx, s.container, blobName, nil)
	if err != nil {
		if isNotFound(err) {
			return nil, nil, storage.ErrNotFound
		}
		return nil, nil, fmt.Errorf("azure: download object: %w", err)
	}
	info := &storage.ObjectInfo{Key: key}
	if resp.ETag != nil {
		info.ETag = string(*resp.ETag)
	}
	if resp.ContentLength != nil {
		info.Size = *resp.ContentLength
	}
	if resp.LastModified != nil {
		info.LastModified = resp.LastModified.UTC()
	}
	if resp.ContentType != nil {
		info.ContentType = *resp.ContentType
	}
	if desc, err := decodeAzureDescriptor(resp.Metadata); err == nil && len(desc) > 0 {
		info.Descriptor = append([]byte(nil), desc...)
	} else if err != nil {
		resp.Body.Close()
		return nil, nil, err
	}
	if plain, ok := storage.ObjectPlaintextSizeFromContext(ctx); ok && plain > 0 {
		info.Size = plain
	}
	return resp.Body, info, nil
}

// PutObject uploads a blob with CAS/creation semantics.
func (s *Store) PutObject(ctx context.Context, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	blobName := s.objectBlob(strings.TrimPrefix(key, "/"))
	uploadOpts := &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{},
	}
	if opts.ContentType != "" {
		uploadOpts.HTTPHeaders.BlobContentType = to.Ptr(opts.ContentType)
	}
	if len(opts.Descriptor) > 0 {
		encoded := encodeAzureDescriptor(opts.Descriptor)
		uploadOpts.Metadata = map[string]*string{descriptorMetadataKey: to.Ptr(encoded)}
	}
	if opts.ExpectedETag != "" {
		uploadOpts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: to.Ptr(azcore.ETag(opts.ExpectedETag)),
			},
		}
	} else if opts.IfNotExists {
		uploadOpts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfNoneMatch: to.Ptr(azcore.ETag("*")),
			},
		}
	}
	resp, err := s.client.UploadStream(ctx, s.container, blobName, body, uploadOpts)
	if err != nil {
		if isPreconditionFailed(err) {
			return nil, storage.ErrCASMismatch
		}
		return nil, fmt.Errorf("azure: upload object: %w", err)
	}
	info := &storage.ObjectInfo{Key: key}
	if resp.ETag != nil {
		info.ETag = string(*resp.ETag)
	}
	info.ContentType = opts.ContentType
	if len(opts.Descriptor) > 0 {
		info.Descriptor = append([]byte(nil), opts.Descriptor...)
	}
	return info, nil
}

// DeleteObject removes the blob, optionally enforcing a matching ETag.
func (s *Store) DeleteObject(ctx context.Context, key string, opts storage.DeleteObjectOptions) error {
	blobName := s.objectBlob(strings.TrimPrefix(key, "/"))
	deleteOpts := &azblob.DeleteBlobOptions{}
	if opts.ExpectedETag != "" {
		deleteOpts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: to.Ptr(azcore.ETag(opts.ExpectedETag)),
			},
		}
	}
	_, err := s.client.DeleteBlob(ctx, s.container, blobName, deleteOpts)
	if err != nil {
		if isPreconditionFailed(err) {
			return storage.ErrCASMismatch
		}
		if isNotFound(err) {
			if opts.IgnoreNotFound {
				return nil
			}
			return storage.ErrNotFound
		}
		return fmt.Errorf("azure: delete object: %w", err)
	}
	return nil
}

func isPreconditionFailed(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		if respErr.StatusCode == http.StatusPreconditionFailed || respErr.StatusCode == http.StatusConflict {
			return true
		}
	}
	return false
}

func isNotFound(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == http.StatusNotFound
	}
	return false
}
