//go:build integration && minio && bench

package miniointegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
)

const (
	largeJSONSize      = 5 << 20 // 5 MiB
	smallJSONSize      = 512     // bytes
	smallJSONPerBatch  = 8
	benchmarkLeaseTTL  = 60
	benchmarkBlockSecs = 5
)

type benchmarkEnv struct {
	cfg       lockd.Config
	minioCfg  s3.Config
	rawClient *minio.Client
	store     *s3.Store
	client    *lockdclient.Client
}

func setupBenchmarkEnv(b *testing.B, withLockd bool) *benchmarkEnv {
	b.Helper()

	cfg := loadMinioConfig(b)
	ensureMinioBucket(b, cfg)
	ensureStoreReady(b, context.Background(), cfg)

	minioCfg, err := lockd.BuildMinioConfig(cfg)
	if err != nil {
		b.Fatalf("build minio config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		b.Fatalf("new minio store: %v", err)
	}
	b.Cleanup(func() {
		_ = store.Close()
	})

	var client *lockdclient.Client
	if withLockd {
		client = startLockdServer(b, cfg)
	}

	return &benchmarkEnv{
		cfg:       cfg,
		minioCfg:  minioCfg,
		rawClient: store.Client(),
		store:     store,
		client:    client,
	}
}

func (env *benchmarkEnv) cleanupKey(b testing.TB, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := env.store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		b.Fatalf("cleanup remove state: %v", err)
	}
	if err := env.store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		b.Fatalf("cleanup delete meta: %v", err)
	}
}

func makeLargeJSON() []byte {
	payload := make(map[string]any)
	payload["records"] = strings.Repeat("X", largeJSONSize-32)
	out, _ := json.Marshal(payload)
	if len(out) < largeJSONSize {
		pad := largeJSONSize - len(out)
		buf := bytes.NewBuffer(out)
		buf.Write(bytes.Repeat([]byte("Y"), pad))
		return buf.Bytes()
	}
	return out
}

func makeSmallJSONBatch() [][]byte {
	batch := make([][]byte, 0, smallJSONPerBatch)
	for i := 0; i < smallJSONPerBatch; i++ {
		payload := map[string]any{
			"idx":       i,
			"timestamp": time.Now().UnixNano(),
			"value":     strings.Repeat("v", smallJSONSize-48),
		}
		b, _ := json.Marshal(payload)
		batch = append(batch, b)
	}
	return batch
}

func nextKey(prefix string, i int) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), i)
}

func BenchmarkMinioRawLargeJSON(b *testing.B) {
	env := setupBenchmarkEnv(b, false)
	payload := makeLargeJSON()
	ctx := context.Background()
	bucket := env.minioCfg.Bucket
	basePrefix := path.Join(strings.Trim(env.minioCfg.Prefix, "/"), "bench/raw-large")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := path.Join(basePrefix, strconv.Itoa(i))
		reader := bytes.NewReader(payload)
		_, err := env.rawClient.PutObject(ctx, bucket, key, reader, int64(len(payload)), minio.PutObjectOptions{ContentType: "application/json"})
		if err != nil {
			b.Fatalf("put object: %v", err)
		}
		if err := env.rawClient.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil {
			b.Fatalf("remove object: %v", err)
		}
	}
}

func BenchmarkLockdLargeJSON(b *testing.B) {
	env := setupBenchmarkEnv(b, true)
	payload := makeLargeJSON()
	ctx := context.Background()
	client := env.client

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextKey("bench-lockd-large", i)
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{Key: key, Owner: "bench-large", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		if _, err := client.UpdateState(ctx, key, lease.LeaseID, payload, opts); err != nil {
			b.Fatalf("update state: %v", err)
		}
		if _, err := client.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
		env.cleanupKey(b, key)
	}
}

func BenchmarkMinioRawSmallJSON(b *testing.B) {
	env := setupBenchmarkEnv(b, false)
	batch := makeSmallJSONBatch()
	ctx := context.Background()
	bucket := env.minioCfg.Bucket
	basePrefix := path.Join(strings.Trim(env.minioCfg.Prefix, "/"), "bench/raw-small")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, payload := range batch {
			key := path.Join(basePrefix, fmt.Sprintf("%d-%d", i, j))
			reader := bytes.NewReader(payload)
			_, err := env.rawClient.PutObject(ctx, bucket, key, reader, int64(len(payload)), minio.PutObjectOptions{ContentType: "application/json"})
			if err != nil {
				b.Fatalf("put object: %v", err)
			}
			if err := env.rawClient.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil {
				b.Fatalf("remove object: %v", err)
			}
		}
	}
}

func BenchmarkLockdSmallJSON(b *testing.B) {
	env := setupBenchmarkEnv(b, true)
	batch := makeSmallJSONBatch()
	ctx := context.Background()
	client := env.client

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextKey("bench-lockd-small", i)
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{Key: key, Owner: "bench-small", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		version := strconv.FormatInt(lease.Version, 10)
		for _, payload := range batch {
			if _, err := client.UpdateState(ctx, key, lease.LeaseID, payload, lockdclient.UpdateStateOptions{IfVersion: version}); err != nil {
				b.Fatalf("update state: %v", err)
			}
			version = "" // after first update we rely on lease sequencing
		}
		if _, err := client.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
		env.cleanupKey(b, key)
	}
}

func BenchmarkLockdConcurrentDistinctKeys(b *testing.B) {
	env := setupBenchmarkEnv(b, true)
	payload := makeSmallJSONBatch()[0]
	ctx := context.Background()
	client := env.client
	var counter uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := atomic.AddUint64(&counter, 1)
		var seq int
		for pb.Next() {
			key := fmt.Sprintf("bench-lockd-concurrent-%d-%d", id, seq)
			seq++
			lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{Key: key, Owner: fmt.Sprintf("worker-%d", id), TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
			if err != nil {
				b.Fatalf("acquire: %v", err)
			}
			opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
			if _, err := client.UpdateState(ctx, key, lease.LeaseID, payload, opts); err != nil {
				b.Fatalf("update state: %v", err)
			}
			if _, err := client.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
				b.Fatalf("release: %v", err)
			}
			env.cleanupKey(b, key)
		}
	})
}

func BenchmarkMinioRawConcurrent(b *testing.B) {
	env := setupBenchmarkEnv(b, false)
	payload := makeSmallJSONBatch()[0]
	ctx := context.Background()
	bucket := env.minioCfg.Bucket
	basePrefix := path.Join(strings.Trim(env.minioCfg.Prefix, "/"), "bench/raw-concurrent")
	var counter uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := atomic.AddUint64(&counter, 1)
		var seq int
		for pb.Next() {
			key := path.Join(basePrefix, fmt.Sprintf("%d-%d", id, seq))
			seq++
			reader := bytes.NewReader(payload)
			_, err := env.rawClient.PutObject(ctx, bucket, key, reader, int64(len(payload)), minio.PutObjectOptions{ContentType: "application/json"})
			if err != nil {
				b.Fatalf("put object: %v", err)
			}
			if err := env.rawClient.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil {
				b.Fatalf("remove object: %v", err)
			}
		}
	})
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
