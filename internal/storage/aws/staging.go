package aws

import (
	"context"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

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
	ctx, cancel := withTimeout(ctx)
	defer cancel()
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
		stat, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(dstObject)})
		if err == nil {
			if stripETag(aws.ToString(stat.ETag)) != opts.ExpectedHeadETag {
				return nil, storage.ErrCASMismatch
			}
		} else if !isNotFound(err) {
			return nil, s.wrapError(err, "aws: promote staged stat")
		} else {
			return nil, storage.ErrNotFound
		}
	}
	verbose.Trace("aws.promote_staged.begin", "namespace", namespace, "key", key, "staged", stagedKey, "object", dstObject)
	copySource := url.PathEscape(s.cfg.Bucket + "/" + srcObject)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(s.cfg.Bucket),
		Key:        aws.String(dstObject),
		CopySource: aws.String(copySource),
	}
	if opts.ExpectedHeadETag != "" {
		input.IfMatch = aws.String(opts.ExpectedHeadETag)
	}
	applySSEToCopy(input, s.cfg.ServerSideEnc, s.cfg.KMSKeyID)
	out, err := s.client.CopyObject(ctx, input)
	if err != nil {
		logger.Debug("aws.promote_staged.copy_error", "namespace", namespace, "key", key, "staged", stagedKey, "object", dstObject, "error", err)
		return nil, s.wrapError(err, "aws: promote staged copy")
	}
	_ = s.Remove(ctx, namespace, stagedKey, "")
	desc, _ := storage.StateDescriptorFromContext(ctx)
	bytesWritten, _ := storage.StatePlaintextSizeFromContext(ctx)
	etag := ""
	if out.CopyObjectResult != nil {
		etag = stripETag(aws.ToString(out.CopyObjectResult.ETag))
	}
	result := &storage.PutStateResult{
		BytesWritten: bytesWritten,
		NewETag:      etag,
		Descriptor:   append([]byte(nil), desc...),
	}
	verbose.Trace("aws.promote_staged.success", "namespace", namespace, "key", key, "staged", stagedKey, "object", dstObject, "etag", result.NewETag)
	return result, nil
}

// DiscardStagedState deletes staged state for a transaction.
func (s *Store) DiscardStagedState(ctx context.Context, namespace, key, txnID string, opts storage.DiscardStagedOptions) error {
	return s.Remove(ctx, namespace, s.stagingKey(key, txnID), opts.ExpectedETag)
}

// ListStagedState lists staged state keys for a namespace.
func (s *Store) ListStagedState(ctx context.Context, namespace string, opts storage.ListStagedOptions) (*storage.ListResult, error) {
	return storage.ListStagedStateFromObjects(ctx, s, namespace, opts)
}
