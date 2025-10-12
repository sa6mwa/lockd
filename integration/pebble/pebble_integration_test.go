//go:build integration && pebble

package pebbleintegration

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	port "pkt.systems/logport"
)

func TestPebbleLockLifecycle(t *testing.T) {
	cfg, cleanup := newPebbleConfig(t)
	defer cleanup()

	cli := startPebbleServer(t, cfg)
	ctx := context.Background()

	key := "pebble-lifecycle-" + uuid.NewString()

	lease, err := cli.Acquire(ctx, lockdclient.AcquireRequest{
		Key:        key,
		Owner:      "lifecycle",
		TTLSeconds: 30,
		BlockSecs:  5,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	state, etag, version, err := getStateJSON(ctx, cli, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if state != nil {
		t.Fatal("expected empty state")
	}

	payload, _ := json.Marshal(map[string]any{"cursor": 99, "owner": "lifecycle"})
	opts := lockdclient.UpdateStateOptions{IfVersion: version}
	if opts.IfVersion == "" {
		opts.IfVersion = strconv.FormatInt(lease.Version, 10)
	}
	if etag != "" {
		opts.IfETag = etag
	}
	if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
		t.Fatalf("update state: %v", err)
	}

	state, _, _, err = getStateJSON(ctx, cli, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if got := state["cursor"]; got != float64(99) {
		t.Fatalf("expected cursor 99, got %v", got)
	}

	if !releaseLease(t, ctx, cli, key, lease.LeaseID) {
		t.Fatalf("expected release success")
	}

	nextLease, err := cli.Acquire(ctx, lockdclient.AcquireRequest{
		Key:        key,
		Owner:      "other",
		TTLSeconds: 30,
		BlockSecs:  5,
	})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if nextLease.LeaseID == lease.LeaseID {
		t.Fatal("expected new lease id")
	}
	if !releaseLease(t, ctx, cli, key, nextLease.LeaseID) {
		t.Fatalf("expected release success")
	}
}

func TestPebbleLockAggressiveConcurrency(t *testing.T) {
	cfg, cleanup := newPebbleConfig(t)
	defer cleanup()

	cli := startPebbleServer(t, cfg)
	ctx := context.Background()

	key := "pebble-concurrency-" + uuid.NewString()
	workers := 20
	iterations := 100
	ttl := int64(60)

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(worker int) {
			defer wg.Done()
			owner := "worker-" + strconv.Itoa(worker)
			for iter := 0; iter < iterations; {
				lease, err := cli.Acquire(ctx, lockdclient.AcquireRequest{
					Key:        key,
					Owner:      owner,
					TTLSeconds: ttl,
					BlockSecs:  20,
				})
				if err != nil {
					t.Fatalf("worker %d acquire: %v", worker, err)
				}

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
				body, _ := json.Marshal(map[string]any{
					"counter": counter,
					"last":    owner,
				})
				if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, body, lockdclient.UpdateStateOptions{
					IfETag:    etag,
					IfVersion: version,
				}); err != nil {
					t.Fatalf("worker %d update: %v", worker, err)
				}
				_ = releaseLease(t, ctx, cli, key, lease.LeaseID)
				iter++
			}
		}(i)
	}

	wg.Wait()

	finalLease, err := cli.Acquire(ctx, lockdclient.AcquireRequest{
		Key:        key,
		Owner:      "verifier",
		TTLSeconds: ttl,
		BlockSecs:  10,
	})
	if err != nil {
		t.Fatalf("verifier acquire: %v", err)
	}
	finalState, _, _, err := getStateJSON(ctx, cli, key, finalLease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, cli, key, finalLease.LeaseID) {
		t.Fatalf("expected release success")
	}

	if finalState == nil {
		t.Fatal("expected final state")
	}
	expected := float64(workers * iterations)
	if value, ok := finalState["counter"].(float64); !ok || value != expected {
		t.Fatalf("expected counter %.0f, got %v", expected, finalState["counter"])
	}
}

func newPebbleConfig(t *testing.T) (lockd.Config, func()) {
	dir := t.TempDir()
	cfg := lockd.Config{
		Store:                   "pebble:///" + dir,
		DefaultTTL:              30 * time.Second,
		MaxTTL:                  2 * time.Minute,
		AcquireBlock:            10 * time.Second,
		SweeperInterval:         2 * time.Second,
		StorageRetryMaxAttempts: 6,
		StorageRetryBaseDelay:   50 * time.Millisecond,
		StorageRetryMaxDelay:    500 * time.Millisecond,
		StorageRetryMultiplier:  1.5,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	cleanup := func() {}
	return cfg, cleanup
}

func startPebbleServer(t *testing.T, cfg lockd.Config) *lockdclient.Client {
	addr := pickPort(t)
	cfg.Listen = addr
	cfg.MTLS = false

	srv, err := lockd.NewServer(cfg, lockd.WithLogger(port.NoopLogger()))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- srv.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	})

	httpClient := &http.Client{Timeout: 15 * time.Second}
	baseURL := "http://" + addr
	waitForReady(t, httpClient, baseURL)

	cli, err := lockdclient.New(baseURL, lockdclient.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatalf("client: %v", err)
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

func getStateJSON(ctx context.Context, cli *lockdclient.Client, key, leaseID string) (map[string]any, string, string, error) {
	data, etag, version, err := cli.GetStateBytes(ctx, key, leaseID)
	if err != nil {
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
	return resp.Released
}
