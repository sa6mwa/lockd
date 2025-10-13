package azure

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

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
}

// Store implements storage.Backend backed by Azure Blob Storage.
type Store struct {
	client    *azblob.Client
	container string
	prefix    string
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
	}, nil
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

func (s *Store) Close() error { return nil }

func (s *Store) prefixed(parts ...string) string {
	name := path.Join(parts...)
	if s.prefix == "" {
		return name
	}
	return path.Join(s.prefix, name)
}

func (s *Store) metaBlob(key string) string {
	return s.prefixed("meta", url.PathEscape(key)+".json")
}

func (s *Store) stateBlob(key string) string {
	return s.prefixed("state", url.PathEscape(key)+".json")
}

func (s *Store) LoadMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	resp, err := s.client.DownloadStream(ctx, s.container, s.metaBlob(key), nil)
	if err != nil {
		if isNotFound(err) {
			return nil, "", storage.ErrNotFound
		}
		return nil, "", fmt.Errorf("azure: download meta: %w", err)
	}
	defer resp.Body.Close()
	var meta storage.Meta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, "", fmt.Errorf("azure: decode meta: %w", err)
	}
	etag := ""
	if resp.ETag != nil {
		etag = string(*resp.ETag)
	}
	return &meta, etag, nil
}

func (s *Store) StoreMeta(ctx context.Context, key string, meta *storage.Meta, expectedETag string) (string, error) {
	payload, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	opts := &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr("application/json"),
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
			name = strings.TrimSuffix(name, ".json")
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
	}
	if resp.LastModified != nil {
		info.ModifiedAt = resp.LastModified.Unix()
	}
	return resp.Body, info, nil
}

func (s *Store) WriteState(ctx context.Context, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	uploadOpts := &azblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr("application/json"),
		},
	}
	if opts.ExpectedETag != "" {
		uploadOpts.AccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: to.Ptr(azcore.ETag(opts.ExpectedETag)),
			},
		}
	}
	resp, err := s.client.UploadStream(ctx, s.container, s.stateBlob(key), body, uploadOpts)
	if err != nil {
		if isPreconditionFailed(err) {
			return nil, storage.ErrCASMismatch
		}
		return nil, fmt.Errorf("azure: upload state: %w", err)
	}
	if resp.ETag == nil {
		return nil, fmt.Errorf("azure: upload state: missing etag")
	}
	return &storage.PutStateResult{
		BytesWritten: 0,
		NewETag:      string(*resp.ETag),
	}, nil
}

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
