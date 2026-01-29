package core

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"pkt.systems/lockd/internal/storage"
)

const leaseIndexPrefix = ".lease-index"

func leaseIndexBucketsKey() string {
	return leaseIndexPrefix + "/buckets.json"
}

func leaseIndexKey(bucket, key string) string {
	if bucket == "" {
		return leaseIndexPrefix + "/" + key
	}
	return leaseIndexPrefix + "/" + bucket + "/" + key
}

func (s *Service) updateLeaseIndex(ctx context.Context, namespace, key string, oldExpires, newExpires int64) error {
	if s == nil {
		return nil
	}
	oldBucket := sweepBucketFromUnix(oldExpires)
	newBucket := sweepBucketFromUnix(newExpires)
	if oldBucket == newBucket && newBucket == "" {
		return nil
	}
	if newBucket != "" {
		cacheKey := namespace + "|" + newBucket
		if err := s.ensureBucket(ctx, namespace, leaseIndexBucketsKey(), newBucket, cacheKey, &s.leaseBucketCache); err != nil {
			return err
		}
		payload := bytes.NewReader([]byte("{}"))
		_, err := s.store.PutObject(ctx, namespace, leaseIndexKey(newBucket, key), payload, storage.PutObjectOptions{ContentType: storage.ContentTypeJSON})
		if err != nil && !errors.Is(err, storage.ErrNotImplemented) {
			return err
		}
	}
	if oldBucket != "" && oldBucket != newBucket {
		if err := s.store.DeleteObject(ctx, namespace, leaseIndexKey(oldBucket, key), storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil {
			if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *Service) sweepLeaseIndex(ctx context.Context, budget *sweepBudget) error {
	if s == nil || budget == nil {
		return nil
	}
	now := budget.clock.Now()
	namespaces := s.namespaceTracker.All()
	if len(namespaces) == 0 {
		return nil
	}
	for _, namespace := range namespaces {
		if !budget.allowed() {
			return nil
		}
		buckets, _, err := s.loadBucketIndex(ctx, namespace, leaseIndexBucketsKey())
		if err != nil {
			if errors.Is(err, storage.ErrNotImplemented) || errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return err
		}
		if len(buckets) == 0 {
			continue
		}
		cursor := s.leaseSweepCursor(namespace)
		bucket := cursor.bucket
		if bucket == "" || !containsBucket(buckets, bucket) {
			bucket = pickEligibleBucket(buckets, now)
			cursor.bucket = bucket
			cursor.startAfter = ""
		}
		if bucket == "" || !sweepBucketExpired(bucket, now) {
			continue
		}
		prefix := leaseIndexKey(bucket, "")
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		limit := remainingOps(budget)
		if limit <= 0 {
			return nil
		}
		list, err := s.store.ListObjects(ctx, namespace, storage.ListOptions{
			Prefix:     prefix,
			StartAfter: cursor.startAfter,
			Limit:      limit,
		})
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
		for _, obj := range list.Objects {
			if !budget.allowed() {
				return nil
			}
			key := strings.TrimPrefix(obj.Key, prefix)
			if key == "" {
				continue
			}
			meta, metaETag, err := s.ensureMeta(ctx, namespace, key)
			switch {
			case errors.Is(err, storage.ErrNotFound):
				_ = s.store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true})
				budget.consume()
				continue
			case errors.Is(err, storage.ErrNotImplemented):
				return nil
			case err != nil:
				return err
			}
			nowUnix := now.Unix()
			if meta.Lease == nil {
				_ = s.store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true})
				budget.consume()
				continue
			}
			if meta.Lease.ExpiresAtUnix > nowUnix {
				_ = s.store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true})
				budget.consume()
				continue
			}
			if _, _, err := s.clearExpiredLease(ctx, namespace, key, meta, metaETag, now, sweepModeIdle, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				return err
			}
			_ = s.store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true})
			if !budget.consume() {
				return nil
			}
		}
		if list.Truncated {
			cursor.startAfter = list.NextStartAfter
			return nil
		}
		cursor.bucket = ""
		cursor.startAfter = ""
		_ = s.removeBucket(ctx, namespace, leaseIndexBucketsKey(), bucket)
	}
	return nil
}

func (s *Service) leaseSweepCursor(namespace string) *sweepCursor {
	if s == nil {
		return &sweepCursor{}
	}
	val, _ := s.leaseSweepCursors.LoadOrStore(namespace, &sweepCursor{})
	return val.(*sweepCursor)
}

func containsBucket(list []string, bucket string) bool {
	for _, val := range list {
		if val == bucket {
			return true
		}
	}
	return false
}

func pickEligibleBucket(buckets []string, now time.Time) string {
	for _, bucket := range buckets {
		if sweepBucketExpired(bucket, now) {
			return bucket
		}
	}
	return ""
}

func remainingOps(budget *sweepBudget) int {
	if budget == nil {
		return 0
	}
	if budget.maxOps <= 0 {
		return 0
	}
	remaining := budget.maxOps - budget.ops
	if remaining <= 0 {
		return 0
	}
	if remaining > 1000 {
		return 1000
	}
	return remaining
}
