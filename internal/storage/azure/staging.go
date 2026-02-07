package azure

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"pkt.systems/lockd/internal/storage"
)

func (s *Store) stagingKey(key, txnID string) string {
	key = strings.TrimSuffix(key, "/")
	if key == "" {
		return ".staging/" + txnID
	}
	return key + "/.staging/" + txnID
}

// StageState writes state into a per-transaction staging key.
func (s *Store) StageState(ctx context.Context, namespace, key, txnID string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	return s.WriteState(ctx, namespace, s.stagingKey(key, txnID), body, opts)
}

// LoadStagedState reads staged state for a transaction.
func (s *Store) LoadStagedState(ctx context.Context, namespace, key, txnID string) (storage.ReadStateResult, error) {
	return s.ReadState(ctx, namespace, s.stagingKey(key, txnID))
}

// PromoteStagedState copies staged state into the committed key.
func (s *Store) PromoteStagedState(ctx context.Context, namespace, key, txnID string, opts storage.PromoteStagedOptions) (*storage.PutStateResult, error) {
	stagedKey := s.stagingKey(key, txnID)
	srcBlob, err := s.stateBlob(namespace, stagedKey)
	if err != nil {
		return nil, err
	}
	dstBlob, err := s.stateBlob(namespace, key)
	if err != nil {
		return nil, err
	}
	container := s.client.ServiceClient().NewContainerClient(s.container)
	srcURL := container.NewBlobClient(srcBlob).URL()
	dstClient := container.NewBlobClient(dstBlob)
	copyOpts := &blob.CopyFromURLOptions{}
	if opts.ExpectedHeadETag != "" {
		copyOpts.BlobAccessConditions = &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{
				IfMatch: to.Ptr(azcore.ETag(opts.ExpectedHeadETag)),
			},
		}
	}
	resp, err := dstClient.CopyFromURL(ctx, srcURL, copyOpts)
	if err != nil {
		if isPreconditionFailed(err) {
			return nil, storage.ErrCASMismatch
		}
		return nil, err
	}
	if resp.ETag == nil {
		return nil, fmt.Errorf("azure: promote staged copy missing etag")
	}
	_ = s.Remove(ctx, namespace, stagedKey, "")
	desc, _ := storage.StateDescriptorFromContext(ctx)
	bytesWritten, _ := storage.StatePlaintextSizeFromContext(ctx)
	return &storage.PutStateResult{
		BytesWritten: bytesWritten,
		NewETag:      string(*resp.ETag),
		Descriptor:   append([]byte(nil), desc...),
	}, nil
}

// DiscardStagedState deletes staged state for a transaction.
func (s *Store) DiscardStagedState(ctx context.Context, namespace, key, txnID string, opts storage.DiscardStagedOptions) error {
	return s.Remove(ctx, namespace, s.stagingKey(key, txnID), opts.ExpectedETag)
}

// ListStagedState lists staged state keys for a namespace.
func (s *Store) ListStagedState(ctx context.Context, namespace string, opts storage.ListStagedOptions) (*storage.ListResult, error) {
	return storage.ListStagedStateFromObjects(ctx, s, namespace, opts)
}
