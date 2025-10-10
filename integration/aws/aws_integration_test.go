//go:build integration && aws

package awsintegration

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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/api"
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
	baseURL, httpClient := startLockdServer(t, cfg)

	key := "aws-integration-" + uuid.NewString()
	leaseTTL := int64(30)

	// Acquire initial lease
	acqResp := acquireWithRetry(t, httpClient, baseURL, key, "aws-integration", leaseTTL, 1)
	if acqResp.LeaseID == "" {
		t.Fatal("expected lease id")
	}

	// Initial state should be empty (204)
	if hasState := getState(t, httpClient, baseURL, key, acqResp.LeaseID); hasState {
		t.Fatal("expected no initial state")
	}

	// Update state
	statePayload := map[string]any{"cursor": 42, "source": "aws"}
	updateState(t, httpClient, baseURL, key, acqResp.LeaseID, statePayload, "", strconv.FormatInt(acqResp.Version, 10))

	// Fetch state again, expect JSON with cursor 42
	state, _, _ := readState(t, httpClient, baseURL, key, acqResp.LeaseID)
	if state["cursor"].(float64) != 42 {
		t.Fatalf("expected cursor 42, got %v", state["cursor"])
	}

	// Release lease
	releaseLease(t, httpClient, baseURL, key, acqResp.LeaseID)

	// Acquire again to ensure release worked
	secondAcquire := acquireWithRetry(t, httpClient, baseURL, key, "aws-integration-2", leaseTTL, 1)
	if secondAcquire.LeaseID == acqResp.LeaseID {
		t.Fatal("expected new lease id")
	}
	releaseLease(t, httpClient, baseURL, key, secondAcquire.LeaseID)

	cleanupS3(t, cfg, key)
}

func TestAWSLockConcurrency(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	baseURL, httpClient := startLockdServer(t, cfg)

	key := "aws-concurrency-" + uuid.NewString()
	workers := 5
	iterations := 3
	leaseTTL := int64(45)

	var wg sync.WaitGroup
	wg.Add(workers)
	for id := 0; id < workers; id++ {
		go func(workerID int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", workerID)
			for iter := 0; iter < iterations; iter++ {
				lease := acquireWithRetry(t, httpClient, baseURL, key, owner, leaseTTL, 5)
				payload, etag, _ := readState(t, httpClient, baseURL, key, lease.LeaseID)
				var counter float64
				if payload != nil {
					if v, ok := payload["counter"]; ok {
						counter, _ = v.(float64)
					}
				}
				newCounter := counter + 1
				newState := map[string]any{
					"counter": newCounter,
					"last":    owner,
				}
				updateState(t, httpClient, baseURL, key, lease.LeaseID, newState, etag, strconv.FormatInt(lease.Version, 10))
				releaseLease(t, httpClient, baseURL, key, lease.LeaseID)
			}
		}(id)
	}
	wg.Wait()

	verifier := acquireWithRetry(t, httpClient, baseURL, key, "verifier", leaseTTL, 5)
	finalState, _, _ := readState(t, httpClient, baseURL, key, verifier.LeaseID)
	releaseLease(t, httpClient, baseURL, key, verifier.LeaseID)
	cleanupS3(t, cfg, key)

	if finalState == nil {
		t.Fatal("expected final state")
	}
	expected := float64(workers * iterations)
	value, ok := finalState["counter"].(float64)
	if !ok || value != expected {
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

func startLockdServer(t *testing.T, cfg lockd.Config) (string, *http.Client) {
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

	client := &http.Client{Timeout: 15 * time.Second}
	baseURL := fmt.Sprintf("http://%s", addr)
	waitForReady(t, client, baseURL)
	return baseURL, client
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

func postJSON(t *testing.T, client *http.Client, url string, body any) (int, []byte) {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		t.Fatalf("encode body: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, url, buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, data
}

func request[T any](t *testing.T, client *http.Client, url string, body any, out *T) {
	status, data := postJSON(t, client, url, body)
	if status >= 300 {
		t.Fatalf("request to %s failed: %s", url, string(data))
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
}

func getState(t *testing.T, client *http.Client, baseURL, key, leaseID string) bool {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/get_state?key=%s", baseURL, key), http.NoBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Lease-ID", leaseID)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("get_state unexpected status %d: %s", resp.StatusCode, string(data))
	}
	return true
}

func readState(t *testing.T, client *http.Client, baseURL, key, leaseID string) (map[string]any, string, string) {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/get_state?key=%s", baseURL, key), http.NoBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Lease-ID", leaseID)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil, "", ""
	}
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("get_state unexpected status %d: %s", resp.StatusCode, string(data))
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	etag := resp.Header.Get("ETag")
	version := resp.Header.Get("X-Key-Version")
	return payload, etag, version
}

func updateState(t *testing.T, client *http.Client, baseURL, key, leaseID string, body any, etag, version string) {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		t.Fatalf("encode state: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/update_state?key=%s", baseURL, key), buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Lease-ID", leaseID)
	if etag != "" {
		req.Header.Set("X-If-State-ETag", etag)
	}
	if version != "" {
		req.Header.Set("X-If-Version", version)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("update state: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("update_state failed: %s", string(data))
	}
}

func releaseLease(t *testing.T, client *http.Client, baseURL, key, leaseID string) {
	releaseReq := api.ReleaseRequest{Key: key, LeaseID: leaseID}
	var releaseResp api.ReleaseResponse
	request(t, client, baseURL+"/v1/release", releaseReq, &releaseResp)
	if !releaseResp.Released {
		t.Fatalf("expected release success for lease %s", leaseID)
	}
}

func acquireWithRetry(t *testing.T, client *http.Client, baseURL, key, owner string, ttl, block int64) api.AcquireResponse {
	reqURL := baseURL + "/v1/acquire"
	for attempt := 0; attempt < 60; attempt++ {
		payload := api.AcquireRequest{Key: key, Owner: owner, TTLSeconds: ttl, BlockSecs: block}
		status, data := postJSON(t, client, reqURL, payload)
		if status == http.StatusOK {
			var resp api.AcquireResponse
			if err := json.Unmarshal(data, &resp); err != nil {
				t.Fatalf("decode acquire: %v", err)
			}
			return resp
		}
		if status == http.StatusConflict {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		t.Fatalf("unexpected acquire response %d: %s", status, string(data))
	}
	t.Fatal("acquire retry limit exceeded")
	return api.AcquireResponse{}
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
