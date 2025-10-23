//go:build bench && disk

package diskbench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/logport"
	"pkt.systems/logport/adapters/psl"
)

const (
	diskLargeJSONSize      = 5 << 20
	diskSmallJSONSize      = 512
	diskSmallJSONPerBatch  = 8
	diskBenchmarkLeaseTTL  = 60
	diskBenchmarkBlockSecs = 5
)

type diskBenchmarkEnv struct {
	root   string
	cfg    lockd.Config
	store  *disk.Store
	client *lockdclient.Client
}

func setupDiskBenchmarkEnv(b *testing.B, root string, withLockd bool) *diskBenchmarkEnv {
	b.Helper()

	if root == "" {
		b.Skip("disk root unavailable")
	}

	if withLockd {
		cfg := buildDiskConfig(b, root, 0)
		cli := startDiskBenchServer(b, cfg)
		return &diskBenchmarkEnv{
			root:   root,
			cfg:    cfg,
			client: cli,
		}
	}

	store, err := disk.New(disk.Config{Root: root})
	if err != nil {
		b.Fatalf("disk store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })
	return &diskBenchmarkEnv{
		root:  root,
		store: store,
	}
}

func prepareDiskRoot(tb testing.TB, base string) string {
	tb.Helper()
	var root string
	if base != "" {
		if info, err := os.Stat(base); err != nil || !info.IsDir() {
			tb.Fatalf("disk base %q unavailable: %v", base, err)
		}
		root = filepath.Join(base, "lockd-"+uuidv7.NewString())
	} else if env := os.Getenv("LOCKD_DISK_ROOT"); env != "" {
		root = filepath.Join(env, "lockd-"+uuidv7.NewString())
	} else {
		tb.Fatalf("LOCKD_DISK_ROOT must be set (source .env.disk before running disk benchmarks)")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		tb.Fatalf("mkdir disk root: %v", err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func nfsBasePath() string {
	return os.Getenv("LOCKD_NFS_ROOT")
}

func buildDiskConfig(tb testing.TB, root string, retention time.Duration) lockd.Config {
	tb.Helper()
	storeURL := diskStoreURL(root)
	cfg := lockd.Config{
		Store:           storeURL,
		MTLS:            false,
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
		DiskRetention:   retention,
	}
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config validation failed: %v", err)
	}
	return cfg
}

func diskStoreURL(root string) string {
	if root == "" {
		root = "/tmp/lockd-disk"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return (&url.URL{Scheme: "disk", Path: root}).String()
}

func BenchmarkDiskRawLargeJSON(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, false)
	payload := makeDiskLargeJSON()
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("raw-large", i)
		if _, err := env.store.WriteState(ctx, key, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
			b.Fatalf("write state: %v", err)
		}
		if err := env.store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			b.Fatalf("remove state: %v", err)
		}
	}
}

func BenchmarkLockdDiskLargeJSON(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	payload := makeDiskLargeJSON()
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-large", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-large", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
			b.Fatalf("update state: %v", err)
		}
		if _, err := cli.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkLockdDiskLargeJSONStream(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	b.SetBytes(diskLargeJSONSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-large-stream", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-large-stream", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		stream := newDiskJSONStream(diskLargeJSONSize)
		if _, err := cli.UpdateState(ctx, key, lease.LeaseID, stream, opts); err != nil {
			b.Fatalf("update state stream: %v", err)
		}
		if _, err := cli.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkDiskRawSmallJSON(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, false)
	batch := makeDiskSmallJSONBatch()
	ctx := context.Background()

	b.ReportAllocs()
	if len(batch) > 0 {
		b.SetBytes(int64(len(batch[0])))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for idx, payload := range batch {
			key := nextDiskKey(fmt.Sprintf("raw-small-%d", idx), i)
			if _, err := env.store.WriteState(ctx, key, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
				b.Fatalf("write state: %v", err)
			}
			if err := env.store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				b.Fatalf("remove state: %v", err)
			}
		}
	}
}

func BenchmarkLockdDiskSmallJSON(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	runLockdDiskSmallJSON(b, env)
}

func BenchmarkLockdDiskSmallJSONStream(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	b.SetBytes(diskSmallJSONSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-small-stream", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-small-stream", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		stream := newDiskJSONStream(diskSmallJSONSize)
		if _, err := cli.UpdateState(ctx, key, lease.LeaseID, stream, lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}); err != nil {
			b.Fatalf("update state stream: %v", err)
		}
		if _, err := cli.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkLockdDiskSmallJSONLogLevels(b *testing.B) {
	levels := []string{"info", "trace"}
	for _, level := range levels {
		level := level
		b.Run(level, func(b *testing.B) {
			tmpFile, err := os.CreateTemp("", "lockd-bench-*.log")
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

			root := prepareDiskRoot(b, "")
			env := setupDiskBenchmarkEnv(b, root, true)
			runLockdDiskSmallJSON(b, env)
		})
	}
}

func runLockdDiskSmallJSON(b *testing.B, env *diskBenchmarkEnv) {
	b.Helper()
	batch := makeDiskSmallJSONBatch()
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	if len(batch) > 0 {
		b.SetBytes(int64(len(batch[0])))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-small", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-small", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		version := strconv.FormatInt(lease.Version, 10)
		for _, payload := range batch {
			if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, lockdclient.UpdateStateOptions{IfVersion: version}); err != nil {
				b.Fatalf("update state: %v", err)
			}
			version = ""
		}
		if _, err := cli.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func BenchmarkDiskRawConcurrent(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, false)
	payload := makeDiskSmallJSONBatch()[0]
	ctx := context.Background()
	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&counter, 1)
			key := fmt.Sprintf("raw-concurrent-%d", id)
			if _, err := env.store.WriteState(ctx, key, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
				b.Fatalf("write state: %v", err)
			}
			if err := env.store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				b.Fatalf("remove state: %v", err)
			}
		}
	})
}

func BenchmarkLockdDiskConcurrent(b *testing.B) {
	root := prepareDiskRoot(b, "")
	env := setupDiskBenchmarkEnv(b, root, true)
	payload := makeDiskSmallJSONBatch()[0]
	ctx := context.Background()
	cli := env.client
	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&counter, 1)
			key := nextDiskKey("lockd-concurrent", int(id))
			owner := fmt.Sprintf("worker-%d", id)
			lease := acquireWithRetry(b, ctx, cli, key, owner, diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
			opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
			if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
				b.Fatalf("update state: %v", err)
			}
			if _, err := cli.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
				b.Fatalf("release: %v", err)
			}
		}
	})
}

func BenchmarkLockdDiskLargeJSONNFS(b *testing.B) {
	root := prepareDiskRoot(b, nfsBasePath())
	if root == "" {
		b.Skip("nfs root unavailable")
	}
	env := setupDiskBenchmarkEnv(b, root, true)
	payload := makeDiskLargeJSON()
	ctx := context.Background()
	cli := env.client

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := nextDiskKey("lockd-large-nfs", i)
		lease := acquireWithRetry(b, ctx, cli, key, "bench-large-nfs", diskBenchmarkLeaseTTL, diskBenchmarkBlockSecs)
		opts := lockdclient.UpdateStateOptions{IfVersion: strconv.FormatInt(lease.Version, 10)}
		if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
			b.Fatalf("update state: %v", err)
		}
		if _, err := cli.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			b.Fatalf("release: %v", err)
		}
	}
}

func makeDiskLargeJSON() []byte {
	payload := map[string]any{
		"records": strings.Repeat("X", diskLargeJSONSize),
	}
	out, _ := json.Marshal(payload)
	return out
}

func makeDiskSmallJSONBatch() [][]byte {
	batch := make([][]byte, 0, diskSmallJSONPerBatch)
	for i := 0; i < diskSmallJSONPerBatch; i++ {
		payload := map[string]any{
			"idx":       i,
			"timestamp": time.Now().UnixNano(),
			"value":     strings.Repeat("v", diskSmallJSONSize-48),
		}
		data, _ := json.Marshal(payload)
		batch = append(batch, data)
	}
	return batch
}

type diskJSONStream struct {
	prefix    []byte
	remaining int64
	suffix    []byte
	stage     int
}

func newDiskJSONStream(bodyLen int64) io.Reader {
	return &diskJSONStream{
		prefix:    []byte(`{"payload":"`),
		remaining: bodyLen,
		suffix:    []byte(`"}`),
	}
}

func (s *diskJSONStream) Read(p []byte) (int, error) {
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

func startDiskBenchServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()

	cfg.Listen = "127.0.0.1:0"
	cfg.ListenProto = "tcp"
	cfg.MTLS = false

	serverLoggerOpt, clientLoggerOpt := diskBenchLoggerOptions(tb)

	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
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

func diskBenchLoggerOptions(tb testing.TB) (lockd.TestServerOption, lockdclient.Option) {
	tb.Helper()
	levelStr := os.Getenv("LOCKD_BENCH_LOG_LEVEL")
	if levelStr == "" {
		return lockd.WithTestLogger(logport.NoopLogger()), lockdclient.WithLogger(logport.NoopLogger())
	}

	logPath := os.Getenv("LOCKD_BENCH_LOG_PATH")
	created := false
	if logPath == "" {
		tmpFile, err := os.CreateTemp("", "lockd-bench-*.log")
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

	baseLogger := psl.NewStructured(writer).With("bench", "disk")
	if level, ok := logport.ParseLevel(levelStr); ok {
		baseLogger = baseLogger.LogLevel(level)
	} else {
		tb.Fatalf("invalid LOCKD_BENCH_LOG_LEVEL %q", levelStr)
	}

	serverLogger := baseLogger.With("svc", "server").WithLogLevel()
	clientLogger := baseLogger.With("svc", "client").WithLogLevel()

	return lockd.WithTestLogger(serverLogger), lockdclient.WithLogger(clientLogger)
}

func nextDiskKey(prefix string, idx int) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), idx)
}

func acquireWithRetry(tb testing.TB, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl, block int64) *lockdclient.LeaseSession {
	tb.Helper()
	var lastErr error
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		sess, err := cli.Acquire(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      owner,
			TTLSeconds: ttl,
			BlockSecs:  block,
		})
		if err == nil {
			return sess
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	tb.Fatalf("acquire %s/%s failed: %v", key, owner, lastErr)
	return nil
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
