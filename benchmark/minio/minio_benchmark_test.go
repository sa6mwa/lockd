//go:build bench && minio

package miniointegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/logport"
	"pkt.systems/logport/adapters/psl"
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
	storeCfg  s3.Config
	rawClient *minio.Client
	store     *s3.Store
	client    *lockdclient.Client
}

func setupBenchmarkEnv(b *testing.B, withLockd bool) *benchmarkEnv {
	b.Helper()

	cfg := loadMinioConfig(b)
	ensureMinioBucket(b, cfg)
	ensureStoreReady(b, context.Background(), cfg)

	storeCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		b.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(storeCfg)
	if err != nil {
		b.Fatalf("new minio store: %v", err)
	}
	b.Cleanup(func() {
		_ = store.Close()
	})

	var client *lockdclient.Client
	if withLockd {
		client = startMinioBenchServer(b, cfg)
	}

	return &benchmarkEnv{
		cfg:       cfg,
		storeCfg:  storeCfg,
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
	bucket := env.storeCfg.Bucket
	basePrefix := path.Join(strings.Trim(env.storeCfg.Prefix, "/"), "bench/raw-large")

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
		lease, err := client.Acquire(ctx, api.AcquireRequest{Key: key, Owner: "bench-large", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		if _, err := client.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
			b.Fatalf("update state: %v", err)
		}
		if _, err := client.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
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
		lease, err := client.Acquire(ctx, api.AcquireRequest{Key: key, Owner: "bench-large-stream", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		reader := newJSONPayloadStream(largeJSONSize)
		if _, err := client.UpdateState(ctx, key, lease.LeaseID, reader, opts); err != nil {
			b.Fatalf("update state (stream): %v", err)
		}
		if _, err := client.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
		env.cleanupKey(b, key)
	}
}

func BenchmarkMinioRawSmallJSON(b *testing.B) {
	env := setupBenchmarkEnv(b, false)
	batch := makeSmallJSONBatch()
	ctx := context.Background()
	bucket := env.storeCfg.Bucket
	basePrefix := path.Join(strings.Trim(env.storeCfg.Prefix, "/"), "bench/raw-small")

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
	runLockdSmallJSONMinio(b, env)
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
		lease, err := client.Acquire(ctx, api.AcquireRequest{Key: key, Owner: "bench-small-stream", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
		if err != nil {
			b.Fatalf("acquire: %v", err)
		}
		version := strconv.FormatInt(lease.Version, 10)
		stream := newJSONPayloadStream(int64(smallJSONSize))
		if _, err := client.UpdateState(ctx, key, lease.LeaseID, stream, lockdclient.UpdateStateOptions{IfVersion: version}); err != nil {
			b.Fatalf("update state (stream): %v", err)
		}
		if _, err := client.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
		env.cleanupKey(b, key)
	}
}

func BenchmarkLockdSmallJSONLogLevels(b *testing.B) {
	levels := []string{"info", "trace"}
	for _, level := range levels {
		level := level
		b.Run(level, func(b *testing.B) {
			tmpFile, err := os.CreateTemp("", "lockd-minio-bench-*.log")
			if err != nil {
				b.Fatalf("create bench log: %v", err)
			}
			logPath := tmpFile.Name()
			tmpFile.Close()
			if err := os.Setenv("LOCKD_BENCH_LOG_LEVEL", level); err != nil {
				b.Fatalf("set log level env: %v", err)
			}
			if err := os.Setenv("LOCKD_BENCH_LOG_PATH", logPath); err != nil {
				b.Fatalf("set log path env: %v", err)
			}
			b.Cleanup(func() {
				_ = os.Unsetenv("LOCKD_BENCH_LOG_LEVEL")
				_ = os.Unsetenv("LOCKD_BENCH_LOG_PATH")
				_ = os.Remove(logPath)
			})

			env := setupBenchmarkEnv(b, true)
			runLockdSmallJSONMinio(b, env)
		})
	}
}

func startMinioBenchServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()

	cfg.Listen = "127.0.0.1:0"
	cfg.ListenProto = "tcp"
	cfg.DisableMTLS = true
	cfg.JSONMaxBytes = 100 << 20
	cfg.DefaultTTL = 30 * time.Second
	cfg.MaxTTL = 2 * time.Minute
	cfg.AcquireBlock = 5 * time.Second
	cfg.SweeperInterval = 5 * time.Second
	if cfg.StorageRetryMaxAttempts < 12 {
		cfg.StorageRetryMaxAttempts = 12
	}
	if cfg.StorageRetryBaseDelay < 500*time.Millisecond {
		cfg.StorageRetryBaseDelay = 500 * time.Millisecond
	}
	if cfg.StorageRetryMaxDelay < 15*time.Second {
		cfg.StorageRetryMaxDelay = 15 * time.Second
	}

	serverLoggerOpt, clientLoggerOpt := minioBenchLoggerOptions(tb)

	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(15 * time.Second),
		lockdclient.WithCloseTimeout(30 * time.Second),
		lockdclient.WithKeepAliveTimeout(30 * time.Second),
		clientLoggerOpt,
	}

	ts := lockd.StartTestServer(tb,
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		serverLoggerOpt,
		lockd.WithTestClientOptions(clientOpts...),
	)
	return ts.Client
}

func minioBenchLoggerOptions(tb testing.TB) (lockd.TestServerOption, lockdclient.Option) {
	tb.Helper()
	levelStr := os.Getenv("LOCKD_BENCH_LOG_LEVEL")
	if levelStr == "" {
		return lockd.WithTestLogger(logport.NoopLogger()), lockdclient.WithLogger(logport.NoopLogger())
	}

	logPath := os.Getenv("LOCKD_BENCH_LOG_PATH")
	created := false
	if logPath == "" {
		tmpFile, err := os.CreateTemp("", "lockd-minio-bench-*.log")
		if err != nil {
			tb.Fatalf("create bench log: %v", err)
		}
		logPath = tmpFile.Name()
		_ = tmpFile.Close()
		created = true
	}

	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		tb.Fatalf("prepare bench log directory: %v", err)
	}

	writer, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		tb.Fatalf("open bench log file: %v", err)
	}
	tb.Cleanup(func() { _ = writer.Close() })
	if created {
		tb.Cleanup(func() { _ = os.Remove(logPath) })
	}

	baseLogger := psl.NewStructured(writer).With("app", "lockd").With("sys", "bench.minio")
	if level, ok := logport.ParseLevel(levelStr); ok {
		baseLogger = baseLogger.LogLevel(level)
	} else {
		tb.Fatalf("invalid LOCKD_BENCH_LOG_LEVEL %q", levelStr)
	}

	serverLogger := baseLogger.With("sys", "bench.minio.server").WithLogLevel()
	clientLogger := baseLogger.With("sys", "bench.minio.client").WithLogLevel()

	return lockd.WithTestLogger(serverLogger), lockdclient.WithLogger(clientLogger)
}

func runLockdSmallJSONMinio(b *testing.B, env *benchmarkEnv) {
	b.Helper()
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
		lease, err := client.Acquire(ctx, api.AcquireRequest{Key: key, Owner: "bench-small", TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
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
		if _, err := client.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
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
			lease, err := client.Acquire(ctx, api.AcquireRequest{Key: key, Owner: fmt.Sprintf("worker-%d", id), TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
			if err != nil {
				b.Fatalf("acquire: %v", err)
			}
			opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
			if _, err := client.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
				b.Fatalf("update state: %v", err)
			}
			if _, err := client.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
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
	bucket := env.storeCfg.Bucket
	basePrefix := path.Join(strings.Trim(env.storeCfg.Prefix, "/"), "bench/raw-concurrent")
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
	bucket := env.storeCfg.Bucket
	basePrefix := path.Join(strings.Trim(env.storeCfg.Prefix, "/"), "bench/raw-concurrent-large")
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
			lease, err := client.Acquire(ctx, api.AcquireRequest{Key: key, Owner: fmt.Sprintf("worker-large-%d", id), TTLSeconds: benchmarkLeaseTTL, BlockSecs: benchmarkBlockSecs})
			if err != nil {
				b.Fatalf("acquire: %v", err)
			}
			opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
			if _, err := client.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
				b.Fatalf("update state: %v", err)
			}
			if _, err := client.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
				b.Fatalf("release: %v", err)
			}
			env.cleanupKey(b, key)
		}
	})
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func loadMinioConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	ensureMinioCredentials(tb)

	store := os.Getenv("LOCKD_STORE")
	if store == "" {
		store = "s3://localhost:9000/lockd-integration?insecure=1"
	}
	if !strings.HasPrefix(store, "s3://") {
		tb.Skip("LOCKD_STORE must reference an s3:// URI for MinIO benchmarks")
	}

	cfg := lockd.Config{
		Store:           store,
		S3MaxPartSize:   8 << 20,
		SweeperInterval: time.Second,
	}
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config validation: %v", err)
	}
	return cfg
}

func ensureMinioCredentials(tb testing.TB) {
	tb.Helper()
	if _, ok := os.LookupEnv("MINIO_ROOT_USER"); !ok {
		tb.Setenv("MINIO_ROOT_USER", "minioadmin")
	}
	if _, ok := os.LookupEnv("MINIO_ROOT_PASSWORD"); !ok {
		tb.Setenv("MINIO_ROOT_PASSWORD", "minioadmin")
	}
	if _, ok := os.LookupEnv("MINIO_ACCESS_KEY"); !ok {
		tb.Setenv("MINIO_ACCESS_KEY", "minioadmin")
	}
	if _, ok := os.LookupEnv("MINIO_SECRET_KEY"); !ok {
		tb.Setenv("MINIO_SECRET_KEY", "minioadmin")
	}
	if _, ok := os.LookupEnv("LOCKD_S3_ACCESS_KEY_ID"); !ok {
		tb.Setenv("LOCKD_S3_ACCESS_KEY_ID", "minioadmin")
	}
	if _, ok := os.LookupEnv("LOCKD_S3_SECRET_ACCESS_KEY"); !ok {
		tb.Setenv("LOCKD_S3_SECRET_ACCESS_KEY", "minioadmin")
	}
	if _, ok := os.LookupEnv("LOCKD_S3_ROOT_USER"); !ok {
		tb.Setenv("LOCKD_S3_ROOT_USER", "minioadmin")
	}
	if _, ok := os.LookupEnv("LOCKD_S3_ROOT_PASSWORD"); !ok {
		tb.Setenv("LOCKD_S3_ROOT_PASSWORD", "minioadmin")
	}
}

func ensureMinioBucket(tb testing.TB, cfg lockd.Config) {
	tb.Helper()
	minioCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new minio store: %v", err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exists, err := store.Client().BucketExists(ctx, minioCfg.Bucket)
	if err != nil {
		tb.Fatalf("bucket exists: %v", err)
	}
	if !exists {
		if err := store.Client().MakeBucket(ctx, minioCfg.Bucket, minio.MakeBucketOptions{Region: minioCfg.Region}); err != nil {
			tb.Fatalf("make bucket: %v", err)
		}
	}
}

func ensureStoreReady(tb testing.TB, ctx context.Context, cfg lockd.Config) {
	tb.Helper()
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		tb.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		tb.Fatalf("store verification failed: %+v", res)
	}
}
