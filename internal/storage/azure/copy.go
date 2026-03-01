package azure

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"pkt.systems/lockd/internal/storage"
)

// CopyObject performs a server-side copy within the container.
func (s *Store) CopyObject(ctx context.Context, namespace, srcKey, dstKey string, opts storage.CopyObjectOptions) (*storage.ObjectInfo, error) {
	srcBlob, err := s.objectBlob(namespace, srcKey)
	if err != nil {
		return nil, err
	}
	dstBlob, err := s.objectBlob(namespace, dstKey)
	if err != nil {
		return nil, err
	}
	container := s.client.ServiceClient().NewContainerClient(s.container)
	srcURL := container.NewBlobClient(srcBlob).URL()
	dstClient := container.NewBlobClient(dstBlob)

	copyOpts := &blob.CopyFromURLOptions{}
	if opts.ExpectedETag != "" || opts.IfNotExists {
		cond := &blob.ModifiedAccessConditions{}
		if opts.ExpectedETag != "" {
			cond.IfMatch = to.Ptr(azcore.ETag(opts.ExpectedETag))
		} else if opts.IfNotExists {
			cond.IfNoneMatch = to.Ptr(azcore.ETag("*"))
		}
		copyOpts.BlobAccessConditions = &blob.AccessConditions{ModifiedAccessConditions: cond}
	}

	resp, err := dstClient.CopyFromURL(ctx, srcURL, copyOpts)
	if err != nil {
		if isPreconditionFailed(err) {
			return nil, storage.ErrCASMismatch
		}
		if isCopySourceAuthError(err) {
			return s.copyObjectByStream(ctx, namespace, srcKey, dstKey, opts)
		}
		return nil, fmt.Errorf("azure: copy object: %w", err)
	}
	if resp.ETag == nil {
		return nil, fmt.Errorf("azure: copy object missing etag")
	}
	out := &storage.ObjectInfo{
		Key:          dstKey,
		ETag:         string(*resp.ETag),
		LastModified: time.Now().UTC(),
	}
	if resp.LastModified != nil {
		out.LastModified = *resp.LastModified
	}
	return out, nil
}

func (s *Store) copyObjectByStream(ctx context.Context, namespace, srcKey, dstKey string, opts storage.CopyObjectOptions) (*storage.ObjectInfo, error) {
	src, err := s.GetObject(ctx, namespace, srcKey)
	if err != nil {
		return nil, err
	}
	defer src.Reader.Close()

	putOpts := storage.PutObjectOptions{
		ContentType:  src.Info.ContentType,
		Descriptor:   append([]byte(nil), src.Info.Descriptor...),
		ExpectedETag: opts.ExpectedETag,
	}
	if opts.ExpectedETag == "" {
		putOpts.IfNotExists = opts.IfNotExists
	}
	copyCtx := storage.ContextWithObjectPlaintextSize(ctx, src.Info.Size)
	return s.PutObject(copyCtx, namespace, dstKey, src.Reader, putOpts)
}
