package core

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"pkt.systems/lockd/internal/storage"
)

func txnDecisionBucketsObjectKey() string {
	return txnDecisionBucketsKey
}

func (s *Service) updateDecisionIndex(ctx context.Context, txnID string, oldExpires, newExpires int64) error {
	if s == nil {
		return nil
	}
	oldBucket := sweepBucketFromUnix(oldExpires)
	newBucket := sweepBucketFromUnix(newExpires)
	if oldBucket == newBucket && newBucket == "" {
		return nil
	}
	if newBucket != "" {
		cacheKey := newBucket
		if err := s.ensureBucket(ctx, txnDecisionNamespace, txnDecisionBucketsObjectKey(), newBucket, cacheKey, &s.decisionBucketCache); err != nil {
			return err
		}
		payload := bytes.NewReader([]byte("{}"))
		_, err := s.store.PutObject(ctx, txnDecisionNamespace, txnDecisionIndexKey(newBucket, txnID), payload, storage.PutObjectOptions{ContentType: storage.ContentTypeJSON})
		if err != nil && !errors.Is(err, storage.ErrNotImplemented) {
			return err
		}
	}
	if oldBucket != "" && oldBucket != newBucket {
		if err := s.store.DeleteObject(ctx, txnDecisionNamespace, txnDecisionIndexKey(oldBucket, txnID), storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil {
			if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *Service) deleteTxnDecisionMarker(ctx context.Context, marker *txnDecisionMarker) error {
	if s == nil || marker == nil {
		return nil
	}
	_ = s.store.DeleteObject(ctx, txnDecisionNamespace, txnDecisionMarkerKey(marker.TxnID), storage.DeleteObjectOptions{IgnoreNotFound: true})
	if marker.ExpiresAtUnix > 0 {
		_ = s.store.DeleteObject(ctx, txnDecisionNamespace, txnDecisionIndexKey(sweepBucketFromUnix(marker.ExpiresAtUnix), marker.TxnID), storage.DeleteObjectOptions{IgnoreNotFound: true})
	}
	return nil
}

func (s *Service) sweepDecisionIndex(ctx context.Context, budget *sweepBudget, mode sweepMode) (int, int, error) {
	if s == nil || budget == nil {
		return 0, 0, nil
	}
	now := budget.clock.Now()
	buckets, _, err := s.loadBucketIndex(ctx, txnDecisionNamespace, txnDecisionBucketsObjectKey())
	if err != nil {
		if errors.Is(err, storage.ErrNotImplemented) || errors.Is(err, storage.ErrNotFound) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	if len(buckets) == 0 {
		return 0, 0, nil
	}
	cursor := &s.decisionSweepCursor
	cursor.mu.Lock()
	bucket := cursor.bucket
	startAfter := cursor.startAfter
	cursor.mu.Unlock()

	if bucket == "" || !containsBucket(buckets, bucket) {
		bucket = pickEligibleBucket(buckets, now)
		startAfter = ""
	}
	if bucket == "" || !sweepBucketExpired(bucket, now) {
		return 0, 0, nil
	}
	prefix := txnDecisionIndexKey(bucket, "")
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	limit := remainingOps(budget)
	if limit <= 0 {
		return 0, 0, nil
	}
	list, err := s.store.ListObjects(ctx, txnDecisionNamespace, storage.ListOptions{
		Prefix:     prefix,
		StartAfter: startAfter,
		Limit:      limit,
	})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	deleted := 0
	failed := 0
	for _, obj := range list.Objects {
		if !budget.allowed() {
			if s.txnMetrics != nil {
				s.txnMetrics.recordDecisionMarkerSweep(ctx, mode, deleted, failed)
			}
			return deleted, failed, nil
		}
		txnID := strings.TrimPrefix(obj.Key, prefix)
		if txnID == "" {
			continue
		}
		marker, _, err := s.loadTxnDecisionMarkerWithMode(ctx, txnID, mode)
		switch {
		case errors.Is(err, storage.ErrNotFound):
			_ = s.store.DeleteObject(ctx, txnDecisionNamespace, obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true})
			budget.consume()
			continue
		case errors.Is(err, storage.ErrNotImplemented):
			return deleted, failed, nil
		case err != nil:
			return deleted, failed, err
		}
		if marker.ExpiresAtUnix <= 0 || marker.ExpiresAtUnix > now.Unix() {
			_ = s.store.DeleteObject(ctx, txnDecisionNamespace, obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true})
			budget.consume()
			continue
		}
		delErr := s.deleteTxnDecisionMarker(ctx, marker)
		if delErr != nil && !errors.Is(delErr, storage.ErrNotFound) {
			failed++
			return deleted, failed, delErr
		}
		if delErr == nil || errors.Is(delErr, storage.ErrNotFound) {
			deleted++
		}
		_ = s.store.DeleteObject(ctx, txnDecisionNamespace, obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true})
		if !budget.consume() {
			if s.txnMetrics != nil {
				s.txnMetrics.recordDecisionMarkerSweep(ctx, mode, deleted, failed)
			}
			return deleted, failed, nil
		}
	}
	if list.Truncated {
		cursor.mu.Lock()
		cursor.bucket = bucket
		cursor.startAfter = list.NextStartAfter
		cursor.mu.Unlock()
		if s.txnMetrics != nil {
			s.txnMetrics.recordDecisionMarkerSweep(ctx, mode, deleted, failed)
		}
		return deleted, failed, nil
	}
	cursor.mu.Lock()
	cursor.bucket = ""
	cursor.startAfter = ""
	cursor.mu.Unlock()
	_ = s.removeBucket(ctx, txnDecisionNamespace, txnDecisionBucketsObjectKey(), bucket)
	if s.txnMetrics != nil {
		s.txnMetrics.recordDecisionMarkerSweep(ctx, mode, deleted, failed)
	}
	return deleted, failed, nil
}
