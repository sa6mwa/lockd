//go:build integration && minio

package miniointegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	"pkt.systems/lockd/integration/internal/storepath"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/internal/svcfields"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func retryableTransportError(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "all endpoints unreachable") {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "unexpected EOF") {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}
	return false
}

func loadMinioConfig(tb testing.TB) lockd.Config {
	ensureMinioCredentials(tb)
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		tb.Fatalf("LOCKD_STORE must be set to an s3:// URI for MinIO integration tests")
	}
	if !strings.HasPrefix(store, "s3://") {
		tb.Fatalf("LOCKD_STORE must reference an s3:// URI for MinIO integration tests, got %q", store)
	}
	store = storepath.Scoped(tb, store, "minio")

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
	minioResult, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	minioCfg := minioResult.Config
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
		var lastRes storagecheck.Result
		var lastErr error
		for attempt := 0; attempt < 5; attempt++ {
			res, err := storagecheck.VerifyStore(ctx, cfg)
			lastRes = res
			lastErr = err
			if err == nil && res.Passed() {
				return
			}
			if err != nil {
				if !retryableTransportError(err) {
					tb.Fatalf("verify store: %v", err)
				}
			} else if !shouldRetryStoreVerification(res) {
				tb.Fatalf("store verification failed: %+v", res)
			}
			time.Sleep(500 * time.Millisecond)
		}
		if lastErr != nil {
			tb.Fatalf("verify store: %v", lastErr)
		}
		tb.Fatalf("store verification failed: %+v", lastRes)
	})
}

func shouldRetryStoreVerification(res storagecheck.Result) bool {
	for _, check := range res.Checks {
		if check.Err != nil && retryableTransportError(check.Err) {
			return true
		}
	}
	return false
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
	cryptotest.ConfigureTCAuth(tb, &cfgCopy)

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
	options = append(options, opts...)
	options = append(options, cryptotest.SharedMTLSOptions(tb)...)
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

	logger := svcfields.WithSubsystem(pslog.NewStructured(context.Background(), file).With("app", "lockd"), "bench.minio.harness")
	if level, ok := pslog.ParseLevel(levelStr); ok {
		logger = logger.LogLevel(level)
	} else {
		tb.Fatalf("invalid LOCKD_BENCH_LOG_LEVEL %q", levelStr)
	}
	return lockd.WithTestLogger(logger.WithLogLevel())
}

func acquireWithRetry(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl, block int64) *lockdclient.LeaseSession {
	for attempt := 0; attempt < 60; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		lease, err := cli.Acquire(attemptCtx, api.AcquireRequest{Key: key, Owner: owner, TTLSeconds: ttl, BlockSecs: block})
		cancel()
		if err == nil {
			return lease
		}
		retryDelay := 200 * time.Millisecond
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) {
			switch {
			case apiErr.Status == http.StatusConflict,
				apiErr.Status == http.StatusTooManyRequests,
				apiErr.Status >= http.StatusInternalServerError,
				apiErr.Response.ErrorCode == "node_passive",
				apiErr.Response.ErrorCode == "shutdown_draining":
				if retryAfter := apiErr.RetryAfterDuration(); retryAfter > retryDelay {
					retryDelay = retryAfter
				}
				time.Sleep(retryDelay)
				continue
			}
		}
		if errors.Is(err, context.DeadlineExceeded) || retryableTransportError(err) {
			time.Sleep(retryDelay)
			continue
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

func runAttachmentTest(t *testing.T, ctx context.Context, cli *lockdclient.Client, key string) {
	t.Helper()
	lease := acquireWithRetry(t, ctx, cli, key, "attach-worker", 45, lockdclient.BlockWaitForever)
	alpha := []byte("alpha")
	bravo := []byte("bravo")
	if _, err := lease.Attach(ctx, lockdclient.AttachRequest{
		Name: "alpha.bin",
		Body: bytes.NewReader(alpha),
	}); err != nil {
		t.Fatalf("attach alpha: %v", err)
	}
	if _, err := lease.Attach(ctx, lockdclient.AttachRequest{
		Name:        "bravo.bin",
		Body:        bytes.NewReader(bravo),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("attach bravo: %v", err)
	}
	list, err := lease.ListAttachments(ctx)
	if err != nil {
		t.Fatalf("list attachments: %v", err)
	}
	if list == nil || len(list.Attachments) != 2 {
		t.Fatalf("expected 2 attachments, got %+v", list)
	}
	att, err := lease.RetrieveAttachment(ctx, lockdclient.AttachmentSelector{Name: "alpha.bin"})
	if err != nil {
		t.Fatalf("retrieve alpha: %v", err)
	}
	data, err := io.ReadAll(att)
	att.Close()
	if err != nil {
		t.Fatalf("read alpha: %v", err)
	}
	if !bytes.Equal(data, alpha) {
		t.Fatalf("unexpected alpha payload: %q", data)
	}
	if !releaseLease(t, ctx, lease) {
		t.Fatalf("expected release success")
	}

	resp, err := cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("public get: %v", err)
	}
	publicList, err := resp.ListAttachments(ctx)
	if err != nil {
		resp.Close()
		t.Fatalf("public list: %v", err)
	}
	if len(publicList.Attachments) != 2 {
		resp.Close()
		t.Fatalf("expected 2 public attachments, got %+v", publicList.Attachments)
	}
	publicAtt, err := resp.RetrieveAttachment(ctx, lockdclient.AttachmentSelector{Name: "bravo.bin"})
	if err != nil {
		resp.Close()
		t.Fatalf("public retrieve bravo: %v", err)
	}
	publicData, err := io.ReadAll(publicAtt)
	publicAtt.Close()
	resp.Close()
	if err != nil {
		t.Fatalf("read public bravo: %v", err)
	}
	if !bytes.Equal(publicData, bravo) {
		t.Fatalf("unexpected bravo payload: %q", publicData)
	}

	lease2 := acquireWithRetry(t, ctx, cli, key, "attach-delete", 45, lockdclient.BlockWaitForever)
	if _, err := lease2.DeleteAttachment(ctx, lockdclient.AttachmentSelector{Name: "alpha.bin"}); err != nil {
		t.Fatalf("delete alpha: %v", err)
	}
	if !releaseLease(t, ctx, lease2) {
		t.Fatalf("expected release success")
	}
	listAfter, err := cli.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{Key: key, Public: true})
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if len(listAfter.Attachments) != 1 || listAfter.Attachments[0].Name != "bravo.bin" {
		t.Fatalf("expected bravo only, got %+v", listAfter.Attachments)
	}

	lease3 := acquireWithRetry(t, ctx, cli, key, "attach-clear", 45, lockdclient.BlockWaitForever)
	if _, err := lease3.DeleteAllAttachments(ctx); err != nil {
		t.Fatalf("delete all: %v", err)
	}
	if !releaseLease(t, ctx, lease3) {
		t.Fatalf("expected release success")
	}
	finalList, err := cli.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{Key: key, Public: true})
	if err != nil {
		t.Fatalf("final list: %v", err)
	}
	if len(finalList.Attachments) != 0 {
		t.Fatalf("expected no attachments, got %+v", finalList.Attachments)
	}
}

func releaseLease(t *testing.T, ctx context.Context, lease *lockdclient.LeaseSession) bool {
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	return true
}

func cleanupMinio(tb testing.TB, cfg lockd.Config, key string) {
	minioResult, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	minioCfg := minioResult.Config
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
