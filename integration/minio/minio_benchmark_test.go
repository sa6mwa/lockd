//go:build integration && minio && bench

package miniointegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	payload := map[string]any{
		"records": strings.Repeat("X", largeJSONSize),
	}
	out, _ := json.Marshal(payload)
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

type jsonPayloadStream struct {
	prefix    []byte
	remaining int64
	suffix    []byte
	stage     int
}

func newJSONPayloadStream(bodyLen int64) io.Reader {
	return &jsonPayloadStream{
		prefix:    []byte(`{"payload":"`),
		remaining: bodyLen,
		suffix:    []byte(`"}`),
	}
}

func (s *jsonPayloadStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var total int
	for len(p) > 0 {
		switch s.stage {
		case 0:
			if len(s.prefix) == 0 {
				s.stage = 1
				continue
			}
			n := copy(p, s.prefix)
			s.prefix = s.prefix[n:]
			total += n
			p = p[n:]
		case 1:
			if s.remaining == 0 {
				s.stage = 2
				continue
			}
			chunk := int64(len(p))
			if chunk > s.remaining {
				chunk = s.remaining
			}
			for i := 0; i < int(chunk); i++ {
				p[i] = 'x'
			}
			s.remaining -= chunk
			total += int(chunk)
			p = p[chunk:]
		case 2:
			if len(s.suffix) == 0 {
				s.stage = 3
				continue
			}
			n := copy(p, s.suffix)
			s.suffix = s.suffix[n:]
			total += n
			p = p[n:]
		case 3:
			if total == 0 {
				return 0, io.EOF
			}
			return total, nil
		}
	}
	return total, nil
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
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextKey("bench-lockd-large", i)
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{Key: key, Owner: "bench-large", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		if _, err := client.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
			b.Fatalf("update state: %v", err)
		}
		if _, err := client.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
		env.cleanupKey(b, key)
	}
}

func BenchmarkLockdLargeJSONStream(b *testing.B) {
	env := setupBenchmarkEnv(b, true)
	ctx := context.Background()
	client := env.client
	payloadSize := len(makeLargeJSON())

	b.ReportAllocs()
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextKey("bench-lockd-large-stream", i)
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{Key: key, Owner: "bench-large-stream", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		reader := newJSONPayloadStream(largeJSONSize)
		if _, err := client.UpdateState(ctx, key, lease.LeaseID, reader, opts); err != nil {
			b.Fatalf("update state (stream): %v", err)
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
	if len(batch) > 0 {
		b.SetBytes(int64(len(batch[0])))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextKey("bench-lockd-small", i)
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{Key: key, Owner: "bench-small", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		version := strconv.FormatInt(lease.Version, 10)
		for _, payload := range batch {
			if _, err := client.UpdateStateBytes(ctx, key, lease.LeaseID, payload, lockdclient.UpdateStateOptions{IfVersion: version}); err != nil {
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

func BenchmarkLockdSmallJSONStream(b *testing.B) {
	env := setupBenchmarkEnv(b, true)
	ctx := context.Background()
	client := env.client
	payloadSize := len(makeSmallJSONBatch()[0])

	b.ReportAllocs()
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextKey("bench-lockd-small-stream", i)
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{Key: key, Owner: "bench-small-stream", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		version := strconv.FormatInt(lease.Version, 10)
		stream := newJSONPayloadStream(int64(smallJSONSize))
		if _, err := client.UpdateState(ctx, key, lease.LeaseID, stream, lockdclient.UpdateStateOptions{IfVersion: version}); err != nil {
			b.Fatalf("update state (stream): %v", err)
		}
		if _, err := client.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
		env.cleanupKey(b, key)
	}
}

func BenchmarkLockdConcurrentDistinctKeys(b *testing.B) {
	b.Helper()
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
			if _, err := client.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
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
	b.Helper()
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

func BenchmarkMinioRawConcurrentLarge(b *testing.B) {
	b.Helper()
	env := setupBenchmarkEnv(b, false)
	payload := makeLargeJSON()
	ctx := context.Background()
	bucket := env.minioCfg.Bucket
	basePrefix := path.Join(strings.Trim(env.minioCfg.Prefix, "/"), "bench/raw-concurrent-large")
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

func BenchmarkLockdConcurrentLarge(b *testing.B) {
	b.Helper()
	env := setupBenchmarkEnv(b, true)
	payload := makeLargeJSON()
	ctx := context.Background()
	client := env.client
	var counter uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := atomic.AddUint64(&counter, 1)
		var seq int
		for pb.Next() {
			key := fmt.Sprintf("bench-lockd-concurrent-large-%d-%d", id, seq)
			seq++
			lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{Key: key, Owner: fmt.Sprintf("worker-large-%d", id), TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
			if err != nil {
				b.Fatalf("acquire: %v", err)
			}
			opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
			if _, err := client.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
				b.Fatalf("update state: %v", err)
			}
			if _, err := client.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
				b.Fatalf("release: %v", err)
			}
			env.cleanupKey(b, key)
		}
	})
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
