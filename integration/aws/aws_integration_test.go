//go:build integration && aws

package awsintegration

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

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	port "pkt.systems/logport"
)

func TestAWSStoreVerification(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
}

func TestAWSLockLifecycle(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "aws-lifecycle-" + uuid.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "aws-lifecycle", 30, 1)

	state, etag, version := getStateJSON(t, ctx, cli, key, lease.LeaseID)
	if state != nil {
		t.Fatal("expected no initial state")
	}

	payload, _ := json.Marshal(map[string]any{"cursor": 42, "source": "aws"})
	opts := lockdclient.UpdateStateOptions{IfVersion: version}
	if opts.IfVersion == "" {
		opts.IfVersion = strconv.FormatInt(lease.Version, 10)
	}
	if etag != "" {
		opts.IfETag = etag
	}
	if _, err := cli.UpdateState(ctx, key, lease.LeaseID, payload, opts); err != nil {
		t.Fatalf("update state: %v", err)
	}

	state, _, _ = getStateJSON(t, ctx, cli, key, lease.LeaseID)
	if cursor, ok := state["cursor"].(float64); !ok || cursor != 42 {
		t.Fatalf("expected cursor 42, got %v", state["cursor"])
	}

	releaseLease(t, ctx, cli, key, lease.LeaseID)

	secondLease := acquireWithRetry(t, ctx, cli, key, "aws-lifecycle-2", 30, 1)
	if secondLease.LeaseID == lease.LeaseID {
		t.Fatal("expected new lease id")
	}
	releaseLease(t, ctx, cli, key, secondLease.LeaseID)

	cleanupS3(t, cfg, key)
}

func TestAWSLockConcurrency(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "aws-concurrency-" + uuid.NewString()
	workers := 5
	iterations := 3
	ttl := int64(45)

	var wg sync.WaitGroup
	wg.Add(workers)
	for id := 0; id < workers; id++ {
		go func(workerID int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", workerID)
			for iter := 0; iter < iterations; iter++ {
				lease := acquireWithRetry(t, ctx, cli, key, owner, ttl, 5)
				state, etag, version := getStateJSON(t, ctx, cli, key, lease.LeaseID)
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
				releaseLease(t, ctx, cli, key, lease.LeaseID)
			}
		}(id)
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli, key, "verifier", ttl, 5)
	finalState, _, _ := getStateJSON(t, ctx, cli, key, verifier.LeaseID)
	releaseLease(t, ctx, cli, key, verifier.LeaseID)
	cleanupS3(t, cfg, key)

	if finalState == nil {
		t.Fatal("expected final state")
	}
	expected := float64(workers * iterations)
	if value, ok := finalState["counter"].(float64); !ok || value != expected {
		t.Fatalf("expected counter %.0f, got %v", expected, finalState["counter"])
	}
}

func loadAWSConfig(t *testing.T) lockd.Config {
	store := os.Getenv("LOCKD_STORE")
	if store == "" || !strings.HasPrefix(store, "s3://") {
		t.Skip("LOCKD_STORE must reference an s3:// URI for AWS integration test")
	}
	cfg := lockd.Config{
		Store:         store,
		S3Region:      os.Getenv("LOCKD_S3_REGION"),
		S3Endpoint:    os.Getenv("LOCKD_S3_ENDPOINT"),
		S3SSE:         os.Getenv("LOCKD_S3_SSE"),
		S3KMSKeyID:    os.Getenv("LOCKD_S3_KMS_KEY_ID"),
		S3ForcePath:   os.Getenv("LOCKD_S3_PATH_STYLE") == "1",
		S3DisableTLS:  os.Getenv("LOCKD_S3_DISABLE_TLS") == "1",
		S3MaxPartSize: 16 << 20,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation: %v", err)
	}
	return cfg
}

func ensureStoreReady(t *testing.T, ctx context.Context, cfg lockd.Config) {
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		t.Fatalf("store verification failed: %+v", res)
	}
}

func startLockdServer(t *testing.T, cfg lockd.Config) *lockdclient.Client {
	addr := pickPort(t)
	cfg.Listen = addr
	cfg.MTLS = false
	cfg.JSONMaxBytes = 100 << 20
	cfg.DefaultTTL = 30 * time.Second
	cfg.MaxTTL = 2 * time.Minute
	cfg.AcquireBlock = 5 * time.Second
	cfg.SweeperInterval = 5 * time.Second

	srv, err := lockd.NewServer(cfg, lockd.WithLogger(port.NoopLogger()))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Start() }()
	t.Cleanup(func() {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = srv.Shutdown(shutdownCtx)
		<-done
	})

	httpClient := &http.Client{Timeout: 15 * time.Second}
	baseURL := fmt.Sprintf("http://%s", addr)
	waitForReady(t, httpClient, baseURL)

	cli, err := lockdclient.New(baseURL, lockdclient.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return cli
}

func pickPort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func waitForReady(t *testing.T, client *http.Client, baseURL string) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := client.Get(baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not ready: %v", err)
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
			if apiErr.Status >= 500 {
				t.Logf("acquire transient error %d: %s", apiErr.Status, apiErr.Error())
				time.Sleep(300 * time.Millisecond)
				continue
			}
		}
		t.Fatalf("acquire failed: %v", err)
	}
	t.Fatal("acquire retry limit exceeded")
	return nil
}

func getStateJSON(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, leaseID string) (map[string]any, string, string) {
	data, etag, version, err := cli.GetState(ctx, key, leaseID)
	if err != nil {
		if apiErr := (*lockdclient.APIError)(nil); errors.As(err, &apiErr) && apiErr.Status == http.StatusNoContent {
			return nil, "", ""
		}
		t.Fatalf("get_state: %v", err)
	}
	if len(data) == 0 {
		return nil, etag, version
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	return payload, etag, version
}

func releaseLease(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, leaseID string) {
	resp, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: leaseID})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !resp.Released {
		t.Fatalf("expected release success for lease %s", leaseID)
	}
}

func cleanupS3(t *testing.T, cfg lockd.Config, key string) {
	s3cfg, _, _, err := lockd.BuildS3Config(cfg)
	if err != nil {
		t.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Logf("remove state failed: %v", err)
	}
	if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Logf("delete meta failed: %v", err)
	}
}
