package s3

import (
	"context"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"

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
	logger, verbose := s.loggers(ctx)
	stagedKey := s.stagingKey(key, txnID)
	srcObject, err := s.stateObject(namespace, stagedKey)
	if err != nil {
		return nil, err
	}
	dstObject, err := s.stateObject(namespace, key)
	if err != nil {
		return nil, err
	}
	if opts.ExpectedHeadETag != "" {
		if info, statErr := s.client.StatObject(ctx, s.cfg.Bucket, dstObject, minio.StatObjectOptions{}); statErr == nil {
			if stripETag(info.ETag) != opts.ExpectedHeadETag {
				return nil, storage.ErrCASMismatch
			}
		} else if !isNotFound(statErr) {
			return nil, s.wrapError(statErr, "s3: promote staged stat")
		} else {
			return nil, storage.ErrNotFound
		}
	}
	verbose.Trace("s3.promote_staged.begin", "namespace", namespace, "key", key, "staged", stagedKey, "object", dstObject)
	srcOpts := minio.CopySrcOptions{Bucket: s.cfg.Bucket, Object: srcObject}
	dstOpts := minio.CopyDestOptions{Bucket: s.cfg.Bucket, Object: dstObject}
	info, err := s.client.CopyObject(ctx, dstOpts, srcOpts)
	if err != nil {
		logger.Debug("s3.promote_staged.copy_error", "namespace", namespace, "key", key, "staged", stagedKey, "object", dstObject, "error", err)
		return nil, s.wrapError(err, "s3: promote staged copy")
	}
	_ = s.Remove(ctx, namespace, stagedKey, "")
	desc, _ := storage.StateDescriptorFromContext(ctx)
	bytesWritten, _ := storage.StatePlaintextSizeFromContext(ctx)
	result := &storage.PutStateResult{
		BytesWritten: bytesWritten,
		NewETag:      stripETag(info.ETag),
		Descriptor:   append([]byte(nil), desc...),
	}
	verbose.Trace("s3.promote_staged.success", "namespace", namespace, "key", key, "staged", stagedKey, "object", dstObject, "etag", result.NewETag)
	return result, nil
}

// DiscardStagedState deletes staged state for a transaction.
func (s *Store) DiscardStagedState(ctx context.Context, namespace, key, txnID string, opts storage.DiscardStagedOptions) error {
	return s.Remove(ctx, namespace, s.stagingKey(key, txnID), opts.ExpectedETag)
}

// ListStagedState lists staged state keys for a namespace.
func (s *Store) ListStagedState(ctx context.Context, namespace string, opts storage.ListStagedOptions) (*storage.ListResult, error) {
	prefix := ".staging/"
	if opts.TxnPrefix != "" {
		trimmed := strings.TrimPrefix(opts.TxnPrefix, "/")
		if trimmed != "" {
			prefix += trimmed
		}
	}
	return s.ListObjects(ctx, namespace, storage.ListOptions{Prefix: prefix, StartAfter: opts.StartAfter, Limit: opts.Limit})
}
