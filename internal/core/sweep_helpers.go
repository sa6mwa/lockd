package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"pkt.systems/lockd/internal/storage"
)

const sweepBucketLayout = "2006010215"

type sweepMode string

const (
	sweepModeTransparent sweepMode = "transparent"
	sweepModeIdle        sweepMode = "idle"
	sweepModeReplay      sweepMode = "replay"
	sweepModeManual      sweepMode = "manual"
)

type IdleSweepOptions struct {
	Now        time.Time
	MaxOps     int
	MaxRuntime time.Duration
	OpDelay    time.Duration
	ShouldStop func() bool
}

type sweepBudget struct {
	clock       clockSource
	start       time.Time
	maxOps      int
	maxRuntime  time.Duration
	opDelay     time.Duration
	ops         int
	shouldStop  func() bool
	stopReason string
}

type clockSource interface {
	Now() time.Time
	Sleep(time.Duration)
}

type sweepCursor struct {
	mu         sync.Mutex
	bucket     string
	startAfter string
}

func newSweepBudget(clock clockSource, opts IdleSweepOptions) *sweepBudget {
	start := opts.Now
	if start.IsZero() {
		start = clock.Now()
	}
	return &sweepBudget{
		clock:      clock,
		start:      start,
		maxOps:     opts.MaxOps,
		maxRuntime: opts.MaxRuntime,
		opDelay:    opts.OpDelay,
		shouldStop: opts.ShouldStop,
	}
}

func (b *sweepBudget) allowed() bool {
	if b == nil {
		return false
	}
	if b.maxOps > 0 && b.ops >= b.maxOps {
		b.setStopReason("budget_ops")
		return false
	}
	if b.maxRuntime > 0 && b.clock.Now().Sub(b.start) >= b.maxRuntime {
		b.setStopReason("budget_runtime")
		return false
	}
	if b.shouldStop != nil && b.shouldStop() {
		b.setStopReason("activity")
		return false
	}
	return true
}

func (b *sweepBudget) consume() bool {
	if b == nil {
		return false
	}
	b.ops++
	if b.opDelay > 0 {
		b.clock.Sleep(b.opDelay)
	}
	return b.allowed()
}

func (b *sweepBudget) opsCount() int {
	if b == nil {
		return 0
	}
	return b.ops
}

func (b *sweepBudget) stopResult() string {
	if b == nil {
		return "complete"
	}
	if b.stopReason == "" {
		return "complete"
	}
	return b.stopReason
}

func (b *sweepBudget) setStopReason(reason string) {
	if b == nil || reason == "" || b.stopReason != "" {
		return
	}
	b.stopReason = reason
}

func sweepBucketFromUnix(expiresAtUnix int64) string {
	if expiresAtUnix <= 0 {
		return ""
	}
	return time.Unix(expiresAtUnix, 0).UTC().Format(sweepBucketLayout)
}

func sweepBucketStart(bucket string) (time.Time, error) {
	if strings.TrimSpace(bucket) == "" {
		return time.Time{}, errors.New("bucket empty")
	}
	return time.ParseInLocation(sweepBucketLayout, bucket, time.UTC)
}

func sweepBucketExpired(bucket string, now time.Time) bool {
	start, err := sweepBucketStart(bucket)
	if err != nil {
		return false
	}
	return !now.Before(start.Add(time.Hour))
}

func (s *Service) loadBucketIndex(ctx context.Context, namespace, key string) ([]string, string, error) {
	obj, err := s.store.GetObject(ctx, namespace, key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, "", nil
		}
		return nil, "", err
	}
	defer obj.Reader.Close()
	var buckets []string
	if err := json.NewDecoder(obj.Reader).Decode(&buckets); err != nil {
		return nil, "", err
	}
	return normalizeBuckets(buckets), obj.Info.ETag, nil
}

func (s *Service) storeBucketIndex(ctx context.Context, namespace, key string, buckets []string, expectedETag string) error {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(buckets); err != nil {
		return err
	}
	_, err := s.store.PutObject(ctx, namespace, key, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{
		ExpectedETag: expectedETag,
		ContentType:  storage.ContentTypeJSON,
	})
	return err
}

func (s *Service) ensureBucket(ctx context.Context, namespace, key, bucket, cacheKey string, cache *sync.Map) error {
	if s == nil || strings.TrimSpace(bucket) == "" {
		return nil
	}
	if cache != nil {
		if _, ok := cache.Load(cacheKey); ok {
			return nil
		}
	}
	for {
		buckets, etag, err := s.loadBucketIndex(ctx, namespace, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
		for _, existing := range buckets {
			if existing == bucket {
				if cache != nil {
					cache.Store(cacheKey, struct{}{})
				}
				return nil
			}
		}
		buckets = append(buckets, bucket)
		sort.Strings(buckets)
		if err := s.storeBucketIndex(ctx, namespace, key, buckets, etag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
		if cache != nil {
			cache.Store(cacheKey, struct{}{})
		}
		return nil
	}
}

func (s *Service) removeBucket(ctx context.Context, namespace, key, bucket string) error {
	if s == nil || strings.TrimSpace(bucket) == "" {
		return nil
	}
	for {
		buckets, etag, err := s.loadBucketIndex(ctx, namespace, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
		if len(buckets) == 0 {
			return nil
		}
		changed := false
		out := buckets[:0]
		for _, existing := range buckets {
			if existing == bucket {
				changed = true
				continue
			}
			out = append(out, existing)
		}
		if !changed {
			return nil
		}
		if err := s.storeBucketIndex(ctx, namespace, key, out, etag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
		return nil
	}
}

func normalizeBuckets(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, bucket := range in {
		bucket = strings.TrimSpace(bucket)
		if bucket == "" {
			continue
		}
		if _, ok := seen[bucket]; ok {
			continue
		}
		seen[bucket] = struct{}{}
		out = append(out, bucket)
	}
	sort.Strings(out)
	return out
}
