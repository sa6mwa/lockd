//go:build integration && minio

package miniointegration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	port "pkt.systems/logport"
)

func TestMinioStoreVerification(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
}

func TestMinioLockLifecycle(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "minio-lifecycle-" + uuid.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "minio-lifecycle-worker", 30, 5)

	state, etag, version, err := getStateJSON(ctx, cli, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if state != nil {
		t.Fatal("expected no initial state")
	}

	payload, _ := json.Marshal(map[string]any{"cursor": 7, "source": "minio"})
	opts := lockdclient.UpdateStateOptions{IfVersion: version, IfETag: etag}
	if opts.IfVersion == "" {
		opts.IfVersion = strconv.FormatInt(lease.Version, 10)
	}
	if _, err := cli.UpdateState(ctx, key, lease.LeaseID, payload, opts); err != nil {
		t.Fatalf("update state: %v", err)
	}

	state, _, _, err = getStateJSON(ctx, cli, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if cursor, ok := state["cursor"].(float64); !ok || cursor != 7 {
		t.Fatalf("expected cursor 7, got %v", state["cursor"])
	}

	if !releaseLease(t, ctx, cli, key, lease.LeaseID) {
		t.Fatalf("expected release success")
	}

	secondLease := acquireWithRetry(t, ctx, cli, key, "minio-lifecycle-worker-2", 30, 5)
	if secondLease.LeaseID == lease.LeaseID {
		t.Fatal("expected a new lease id")
	}
	if !releaseLease(t, ctx, cli, key, secondLease.LeaseID) {
		t.Fatalf("expected release success")
	}

	cleanupMinio(t, cfg, key)
}

func TestMinioLockConcurrency(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "minio-concurrency-" + uuid.NewString()
	workers := 4
	iterations := 3
	ttl := int64(45)

	var wg sync.WaitGroup
	wg.Add(workers)
	for id := 0; id < workers; id++ {
		go func(workerID int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", workerID)
			for iter := 0; iter < iterations; {
				lease := acquireWithRetry(t, ctx, cli, key, owner, ttl, 20)
				state, etag, version, err := getStateJSON(ctx, cli, key, lease.LeaseID)
				if err != nil {
					_ = releaseLease(t, ctx, cli, key, lease.LeaseID)
					continue
				}
				if version == "" {
					version = strconv.FormatInt(lease.Version, 10)
				}
				var counter float64
				if state != nil {
					if v, ok := state["counter"]; ok {
						counter, _ = v.(float64)
					}
				}
				counter++
				body, _ := json.Marshal(map[string]any{"counter": counter, "last": owner})
				if _, err := cli.UpdateState(ctx, key, lease.LeaseID, body, lockdclient.UpdateStateOptions{IfETag: etag, IfVersion: version}); err != nil {
					t.Fatalf("update state: %v", err)
				}
				_ = releaseLease(t, ctx, cli, key, lease.LeaseID)
				iter++
			}
		}(id)
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli, key, "verifier", ttl, 5)
	finalState, _, _, err := getStateJSON(ctx, cli, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, cli, key, verifier.LeaseID) {
		t.Fatalf("expected release success")
	}
	cleanupMinio(t, cfg, key)

	expected := float64(workers * iterations)
	if finalState == nil {
		t.Fatalf("expected final state, got nil")
	}
	if value, ok := finalState["counter"].(float64); !ok || value != expected {
		t.Fatalf("expected counter %.0f, got %v", expected, finalState["counter"])
	}
}

func loadMinioConfig(tb testing.TB) lockd.Config {
	ensureMinioCredentials(tb)
	store := os.Getenv("LOCKD_STORE")
	if store == "" {
		store = "minio://localhost:9000/lockd-integration?insecure=1"
	}
	if !strings.HasPrefix(store, "minio://") {
		tb.Skip("LOCKD_STORE must reference a minio:// URI for MinIO integration test")
	}

	cfg := lockd.Config{
		Store:           store,
		S3MaxPartSize:   8 << 20,
		SweeperInterval: time.Second,
	}

	if v := os.Getenv("LOCKD_MINIO_SECURE"); v != "" {
		secure, err := strconv.ParseBool(v)
		if err != nil {
			tb.Fatalf("parse LOCKD_MINIO_SECURE: %v", err)
		}
		cfg.S3DisableTLS = !secure
	}
	if v := os.Getenv("LOCKD_S3_DISABLE_TLS"); v != "" {
		disabled, err := strconv.ParseBool(v)
		if err != nil {
			tb.Fatalf("parse LOCKD_S3_DISABLE_TLS: %v", err)
		}
		cfg.S3DisableTLS = disabled
	}

	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config validation: %v", err)
	}
	return cfg
}

func ensureMinioCredentials(tb testing.TB) {
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
}

func ensureMinioBucket(tb testing.TB, cfg lockd.Config) {
	minioCfg, err := lockd.BuildMinioConfig(cfg)
	if err != nil {
		tb.Fatalf("build minio config: %v", err)
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
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		tb.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		tb.Fatalf("store verification failed: %+v", res)
	}
}

func startLockdServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	addr := pickPort(tb)
	cfg.Listen = addr
	cfg.MTLS = false
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

	srv, err := lockd.NewServer(cfg, lockd.WithLogger(port.NoopLogger()))
	if err != nil {
		tb.Fatalf("new server: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Start() }()
	tb.Cleanup(func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = srv.Shutdown(shutdownCtx)
		<-done
	})

	httpClient := &http.Client{Timeout: 15 * time.Second}
	baseURL := fmt.Sprintf("http://%s", addr)
	waitForReady(tb, httpClient, baseURL)

	cli, err := lockdclient.New(baseURL, lockdclient.WithHTTPClient(httpClient))
	if err != nil {
		tb.Fatalf("new client: %v", err)
	}
	return cli
}

func pickPort(tb testing.TB) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func waitForReady(tb testing.TB, client *http.Client, baseURL string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := client.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("server not ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func acquireWithRetry(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl, block int64) *lockdclient.AcquireResponse {
	for attempt := 0; attempt < 60; attempt++ {
		resp, err := cli.Acquire(ctx, lockdclient.AcquireRequest{Key: key, Owner: owner, TTLSeconds: ttl, BlockSecs: block})
		if err == nil {
			return resp
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
	data, etag, version, err := cli.GetState(ctx, key, leaseID)
	if err != nil {
		if apiErr := (*lockdclient.APIError)(nil); errors.As(err, &apiErr) && apiErr.Status == http.StatusNoContent {
			return nil, "", "", nil
		}
		return nil, "", "", err
	}
	if len(data) == 0 {
		return nil, etag, version, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, "", "", err
	}
	return payload, etag, version, nil
}

func releaseLease(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, leaseID string) bool {
	resp, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: leaseID})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !resp.Released {
		return false
	}
	return true
}

func cleanupMinio(tb testing.TB, cfg lockd.Config, key string) {
	minioCfg, err := lockd.BuildMinioConfig(cfg)
	if err != nil {
		tb.Fatalf("build minio config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new minio store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Logf("remove state failed: %v", err)
	}
	if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Logf("delete meta failed: %v", err)
	}
}
