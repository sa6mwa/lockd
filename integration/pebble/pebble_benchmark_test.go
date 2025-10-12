//go:build integration && pebble && bench

package pebbleintegration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	port "pkt.systems/logport"
)

const (
	pebbleLargeJSONSize      = 5 << 20 // 5 MiB
	pebbleSmallJSONSize      = 512     // bytes
	pebbleSmallJSONPerBatch  = 8
	pebbleBenchmarkLeaseTTL  = 60
	pebbleBenchmarkBlockSecs = 5
)

type pebbleBenchmarkEnv struct {
	cfg    lockd.Config
	db     *pebble.DB
	client *lockdclient.Client
}

func setupPebbleBenchmarkEnv(b *testing.B, withLockd bool) *pebbleBenchmarkEnv {
	b.Helper()

	tmpDir := b.TempDir()
	var db *pebble.DB
	if !withLockd {
		var err error
		db, err = pebble.Open(tmpDir, &pebble.Options{})
		if err != nil {
			b.Fatalf("open pebble: %v", err)
		}
		b.Cleanup(func() {
			_ = db.Close()
		})
	}

	cfg := lockd.Config{
		Store:                   "pebble:///" + tmpDir,
		Listen:                  "127.0.0.1:0",
		MTLS:                    false,
		DefaultTTL:              30 * time.Second,
		MaxTTL:                  2 * time.Minute,
		AcquireBlock:            10 * time.Second,
		SweeperInterval:         3 * time.Second,
		StorageRetryMaxAttempts: 4,
		StorageRetryBaseDelay:   50 * time.Millisecond,
		StorageRetryMaxDelay:    500 * time.Millisecond,
		StorageRetryMultiplier:  2.0,
	}
	if err := cfg.Validate(); err != nil {
		b.Fatalf("validate config: %v", err)
	}

	var client *lockdclient.Client
	if withLockd {
		client = startBenchLockdServer(b, cfg)
	}

	return &pebbleBenchmarkEnv{
		cfg:    cfg,
		db:     db,
		client: client,
	}
}

func startBenchLockdServer(b *testing.B, cfg lockd.Config) *lockdclient.Client {
	b.Helper()
	addr := pickPortFromBench(b)
	cfg.Listen = addr

	srv, err := lockd.NewServer(cfg, lockd.WithLogger(port.NoopLogger()))
	if err != nil {
		b.Fatalf("new server: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Start() }()
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	})

	httpClient := &http.Client{Timeout: 15 * time.Second}
	baseURL := "http://" + addr
	waitForReadyBench(b, httpClient, baseURL)

	cli, err := lockdclient.New(baseURL, lockdclient.WithHTTPClient(httpClient))
	if err != nil {
		b.Fatalf("client: %v", err)
	}
	return cli
}

func pickPortFromBench(b *testing.B) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func waitForReadyBench(b *testing.B, client *http.Client, baseURL string) {
	b.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := client.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return
		}
		if time.Now().After(deadline) {
			b.Fatalf("server not ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func makePebbleLargeJSON() []byte {
	payload := map[string]any{
		"records": strings.Repeat("X", pebbleLargeJSONSize),
	}
	out, _ := json.Marshal(payload)
	return out
}

func makePebbleSmallJSONBatch() [][]byte {
	batch := make([][]byte, 0, pebbleSmallJSONPerBatch)
	for i := 0; i < pebbleSmallJSONPerBatch; i++ {
		payload := map[string]any{
			"idx":       i,
			"timestamp": time.Now().UnixNano(),
			"value":     strings.Repeat("v", pebbleSmallJSONSize-48),
		}
		b, _ := json.Marshal(payload)
		batch = append(batch, b)
	}
	return batch
}

type jsonStream struct {
	prefix    []byte
	remaining int64
	suffix    []byte
	stage     int
}

func newJSONStream(bodyLen int64) io.Reader {
	return &jsonStream{
		prefix:    []byte(`{"payload":"`),
		remaining: bodyLen,
		suffix:    []byte(`"}`),
	}
}

func (s *jsonStream) Read(p []byte) (int, error) {
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

func BenchmarkPebbleRawLargeJSON(b *testing.B) {
	env := setupPebbleBenchmarkEnv(b, false)
	payload := makePebbleLargeJSON()
	db := env.db

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("state/%s", uuid.NewString())
		if err := db.Set([]byte(key), payload, pebble.Sync); err != nil {
			b.Fatalf("set: %v", err)
		}
		if err := db.Delete([]byte(key), pebble.Sync); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func BenchmarkLockdPebbleLargeJSON(b *testing.B) {
	env := setupPebbleBenchmarkEnv(b, true)
	payload := makePebbleLargeJSON()
	ctx := context.Background()
	client := env.client

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench-large-" + uuid.NewString()
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{
			Key:        key,
			Owner:      "bench-large",
			TTLSeconds: pebbleBenchmarkLeaseTTL,
			BlockSecs:  pebbleBenchmarkBlockSecs,
		})
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
	}
}

func BenchmarkLockdPebbleLargeJSONStream(b *testing.B) {
	env := setupPebbleBenchmarkEnv(b, true)
	ctx := context.Background()
	client := env.client

	b.ReportAllocs()
	b.SetBytes(pebbleLargeJSONSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench-large-stream-" + uuid.NewString()
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{
			Key:        key,
			Owner:      "bench-large-stream",
			TTLSeconds: pebbleBenchmarkLeaseTTL,
			BlockSecs:  pebbleBenchmarkBlockSecs,
		})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		stream := newJSONStream(pebbleLargeJSONSize)
		if _, err := client.UpdateState(ctx, key, lease.LeaseID, stream, opts); err != nil {
			b.Fatalf("update state (stream): %v", err)
		}
		if _, err := client.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkPebbleRawSmallJSON(b *testing.B) {
	env := setupPebbleBenchmarkEnv(b, false)
	batch := makePebbleSmallJSONBatch()
	db := env.db

	b.ReportAllocs()
	if len(batch) > 0 {
		b.SetBytes(int64(len(batch[0])))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for idx, payload := range batch {
			key := fmt.Sprintf("state/%s-%d", uuid.NewString(), idx)
			if err := db.Set([]byte(key), payload, pebble.Sync); err != nil {
				b.Fatalf("set: %v", err)
			}
			if err := db.Delete([]byte(key), pebble.Sync); err != nil {
				b.Fatalf("delete: %v", err)
			}
		}
	}
}

func BenchmarkLockdPebbleSmallJSON(b *testing.B) {
	env := setupPebbleBenchmarkEnv(b, true)
	batch := makePebbleSmallJSONBatch()
	ctx := context.Background()
	client := env.client

	b.ReportAllocs()
	if len(batch) > 0 {
		b.SetBytes(int64(len(batch[0])))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench-small-" + uuid.NewString()
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{
			Key:        key,
			Owner:      "bench-small",
			TTLSeconds: pebbleBenchmarkLeaseTTL,
			BlockSecs:  pebbleBenchmarkBlockSecs,
		})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		version := strconv.FormatInt(lease.Version, 10)
		for _, payload := range batch {
			if _, err := client.UpdateStateBytes(ctx, key, lease.LeaseID, payload, lockdclient.UpdateStateOptions{IfVersion: version}); err != nil {
				b.Fatalf("update state: %v", err)
			}
			version = ""
		}
		if _, err := client.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkLockdPebbleSmallJSONStream(b *testing.B) {
	env := setupPebbleBenchmarkEnv(b, true)
	ctx := context.Background()
	client := env.client

	b.ReportAllocs()
	b.SetBytes(pebbleSmallJSONSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench-small-stream-" + uuid.NewString()
		lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{
			Key:        key,
			Owner:      "bench-small-stream",
			TTLSeconds: pebbleBenchmarkLeaseTTL,
			BlockSecs:  pebbleBenchmarkBlockSecs,
		})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		version := strconv.FormatInt(lease.Version, 10)
		stream := newJSONStream(pebbleSmallJSONSize)
		if _, err := client.UpdateState(ctx, key, lease.LeaseID, stream, lockdclient.UpdateStateOptions{IfVersion: version}); err != nil {
			b.Fatalf("update state (stream): %v", err)
		}
		if _, err := client.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkPebbleRawConcurrent(b *testing.B) {
	env := setupPebbleBenchmarkEnv(b, false)
	payload := makePebbleSmallJSONBatch()[0]
	db := env.db
	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&counter, 1)
			key := fmt.Sprintf("state/concurrent-%d", id)
			if err := db.Set([]byte(key), payload, pebble.Sync); err != nil {
				b.Fatalf("set: %v", err)
			}
			if err := db.Delete([]byte(key), pebble.Sync); err != nil {
				b.Fatalf("delete: %v", err)
			}
		}
	})
}

func BenchmarkLockdPebbleConcurrent(b *testing.B) {
	env := setupPebbleBenchmarkEnv(b, true)
	payload := makePebbleSmallJSONBatch()[0]
	ctx := context.Background()
	client := env.client
	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&counter, 1)
			key := "bench-concurrent-" + uuid.NewString()
			owner := fmt.Sprintf("worker-%d", id)
			lease, err := client.Acquire(ctx, lockdclient.AcquireRequest{
				Key:        key,
				Owner:      owner,
				TTLSeconds: pebbleBenchmarkLeaseTTL,
				BlockSecs:  pebbleBenchmarkBlockSecs,
			})
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
		}
	})
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
