//go:build integration && minio

package miniointegration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func loadMinioConfig(tb testing.TB) lockd.Config {
	ensureMinioCredentials(tb)
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		tb.Fatalf("LOCKD_STORE must be set to an s3:// URI for MinIO integration tests")
	}
	if !strings.HasPrefix(store, "s3://") {
		tb.Fatalf("LOCKD_STORE must reference an s3:// URI for MinIO integration tests, got %q", store)
	}

	cfg := lockd.Config{
		Store:           store,
		S3MaxPartSize:   8 << 20,
		SweeperInterval: time.Second,
	}
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	return cfg
}

func ensureMinioCredentials(tb testing.TB) {
	accessKey := strings.TrimSpace(os.Getenv("LOCKD_S3_ACCESS_KEY_ID"))
	secretKey := strings.TrimSpace(os.Getenv("LOCKD_S3_SECRET_ACCESS_KEY"))
	rootUser := strings.TrimSpace(os.Getenv("LOCKD_S3_ROOT_USER"))
	rootPass := strings.TrimSpace(os.Getenv("LOCKD_S3_ROOT_PASSWORD"))
	if (accessKey == "" || secretKey == "") && (rootUser == "" || rootPass == "") {
		tb.Fatalf("MinIO integration tests require either LOCKD_S3_ACCESS_KEY_ID/LOCKD_S3_SECRET_ACCESS_KEY or LOCKD_S3_ROOT_USER/LOCKD_S3_ROOT_PASSWORD to be set")
	}
}

func ensureMinioBucket(tb testing.TB, cfg lockd.Config) {
	minioCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new minio store: %v", err)
	}
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
	WithMinioStorageLock(tb, func() {
		ResetMinioBucketForCrypto(tb, cfg)
		res, err := storagecheck.VerifyStore(ctx, cfg)
		if err != nil {
			tb.Fatalf("verify store: %v", err)
		}
		if !res.Passed() {
			tb.Fatalf("store verification failed: %+v", res)
		}
	})
}

func startLockdServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()

	cfg.Listen = "127.0.0.1:0"
	cfg.ListenProto = "tcp"
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

	loggerOption := minioTestLoggerOption(tb)
	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(15 * time.Second),
		lockdclient.WithCloseTimeout(30 * time.Second),
		lockdclient.WithKeepAliveTimeout(30 * time.Second),
		lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
	}

	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		loggerOption,
		lockd.WithTestClientOptions(clientOpts...),
	}
	options = append(options, cryptotest.SharedMTLSOptions(tb)...)

	ts := lockd.StartTestServer(tb, options...)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := ts.Stop(ctx); err != nil {
			tb.Logf("minio test server stop: %v", err)
		}
		CleanupNamespaces(tb, cfg, namespaces.Default)
	})
	return ts.Client
}

func startMinioTestServer(tb testing.TB, cfg lockd.Config, opts ...lockd.TestServerOption) *lockd.TestServer {
	tb.Helper()
	cfgCopy := cfg
	if cfgCopy.JSONMaxBytes == 0 {
		cfgCopy.JSONMaxBytes = 100 << 20
	}
	if cfgCopy.DefaultTTL == 0 {
		cfgCopy.DefaultTTL = 30 * time.Second
	}
	if cfgCopy.MaxTTL == 0 {
		cfgCopy.MaxTTL = 2 * time.Minute
	}
	if cfgCopy.AcquireBlock == 0 {
		cfgCopy.AcquireBlock = 5 * time.Second
	}
	if cfgCopy.SweeperInterval == 0 {
		cfgCopy.SweeperInterval = 5 * time.Second
	}
	if cfgCopy.StorageRetryMaxAttempts < 12 {
		cfgCopy.StorageRetryMaxAttempts = 12
	}
	if cfgCopy.StorageRetryBaseDelay < 500*time.Millisecond {
		cfgCopy.StorageRetryBaseDelay = 500 * time.Millisecond
	}
	if cfgCopy.StorageRetryMaxDelay < 15*time.Second {
		cfgCopy.StorageRetryMaxDelay = 15 * time.Second
	}

	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfgCopy),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		minioTestLoggerOption(tb),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithFailureRetries(5),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
		lockd.WithTestCloseDefaults(
			lockd.WithDrainLeases(-1),
			lockd.WithShutdownTimeout(10*time.Second),
		),
	}
	var sharedCreds lockd.TestMTLSCredentials
	if cryptotest.TestMTLSEnabled() {
		sharedCreds = cryptotest.SharedMTLSCredentials(tb)
	}
	options = append(options, cryptotest.SharedMTLSOptions(tb, sharedCreds)...)
	options = append(options, opts...)
	ts := lockd.StartTestServer(tb, options...)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := ts.Stop(ctx); err != nil {
			tb.Logf("minio test server stop: %v", err)
		}
		CleanupNamespaces(tb, cfgCopy, namespaces.Default)
	})
	return ts
}

func directClient(tb testing.TB, ts *lockd.TestServer) *lockdclient.Client {
	tb.Helper()
	if ts == nil || ts.Server == nil {
		tb.Fatalf("nil test server")
	}
	addr := ts.Server.ListenerAddr()
	if addr == nil {
		tb.Fatalf("listener not initialized")
	}
	scheme := "http"
	if ts.Config.MTLSEnabled() {
		scheme = "https"
	}
	endpoint := fmt.Sprintf("%s://%s", scheme, addr.String())
	cli, err := ts.NewEndpointsClient([]string{endpoint},
		lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		lockdclient.WithHTTPTimeout(30*time.Second),
		lockdclient.WithEndpointShuffle(false),
	)
	if err != nil {
		tb.Fatalf("direct client: %v", err)
	}
	return cli
}

func minioTestLoggerOption(tb testing.TB) lockd.TestServerOption {
	tb.Helper()
	levelStr := os.Getenv("LOCKD_BENCH_LOG_LEVEL")
	if levelStr == "" {
		return lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel)
	}

	logPath := os.Getenv("LOCKD_BENCH_LOG_PATH")
	if logPath == "" {
		tmpFile, err := os.CreateTemp("", "lockd-minio-log-*.log")
		if err != nil {
			tb.Fatalf("create temp log: %v", err)
		}
		logPath = tmpFile.Name()
		tb.Cleanup(func() { _ = os.Remove(logPath) })
		if err := os.Setenv("LOCKD_BENCH_LOG_PATH", logPath); err != nil {
			tb.Fatalf("set log path env: %v", err)
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
			tb.Fatalf("prepare log directory: %v", err)
		}
	}

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		tb.Fatalf("open log file: %v", err)
	}
	tb.Cleanup(func() { _ = file.Close() })

	logger := loggingutil.WithSubsystem(pslog.NewStructured(file).With("app", "lockd"), "bench.minio.harness")
	if level, ok := pslog.ParseLevel(levelStr); ok {
		logger = logger.LogLevel(level)
	} else {
		tb.Fatalf("invalid LOCKD_BENCH_LOG_LEVEL %q", levelStr)
	}
	return lockd.WithTestLogger(logger.WithLogLevel())
}

func acquireWithRetry(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl, block int64) *lockdclient.LeaseSession {
	for attempt := 0; attempt < 60; attempt++ {
		lease, err := cli.Acquire(ctx, api.AcquireRequest{Key: key, Owner: owner, TTLSeconds: ttl, BlockSecs: block})
		if err == nil {
			return lease
		}
		if apiErr := (*lockdclient.APIError)(nil); errors.As(err, &apiErr) {
			if apiErr.Status == http.StatusConflict {
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}
		t.Fatalf("acquire failed: %v", err)
	}
	t.Fatal("acquire retry limit exceeded")
	return nil
}

func getStateJSON(ctx context.Context, cli *lockdclient.Client, key, leaseID string) (map[string]any, string, string, error) {
	resp, err := cli.Get(ctx, key,
		lockdclient.WithGetLeaseID(leaseID),
		lockdclient.WithGetPublicDisabled(true),
	)
	if err != nil {
		return nil, "", "", err
	}
	if resp == nil {
		return nil, "", "", nil
	}
	defer resp.Close()
	if !resp.HasState {
		return nil, resp.ETag, resp.Version, nil
	}
	data, err := resp.Bytes()
	if err != nil {
		return nil, "", "", err
	}
	if len(data) == 0 {
		return nil, resp.ETag, resp.Version, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, "", "", err
	}
	return payload, resp.ETag, resp.Version, nil
}

func releaseLease(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, leaseID string) bool {
	resp, err := cli.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: leaseID})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !resp.Released {
		return false
	}
	return true
}

func cleanupMinio(tb testing.TB, cfg lockd.Config, key string) {
	minioCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new minio store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := store.Remove(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Logf("remove state failed: %v", err)
	}
	if err := store.DeleteMeta(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Logf("delete meta failed: %v", err)
	}
}
