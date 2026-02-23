//go:build integration && mem && !lq && !query && !crypto

package memintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/hatest"
	"pkt.systems/lockd/integration/internal/locktest"
	shutdowntest "pkt.systems/lockd/integration/internal/shutdowntest"
	testlog "pkt.systems/lockd/integration/internal/testlog"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

type failoverPhase int

type failoverLogResult struct {
	AlternateEndpoint string
	AlternateStatus   int
	ErrorEndpoint     string
	HadTransportError bool
}

const (
	failoverDuringHandlerStart failoverPhase = iota
	failoverBeforeSave
	failoverAfterSave
)

func (p failoverPhase) String() string {
	switch p {
	case failoverDuringHandlerStart:
		return "handler-start"
	case failoverBeforeSave:
		return "before-save"
	case failoverAfterSave:
		return "after-save"
	default:
		return fmt.Sprintf("phase-%d", int(p))
	}
}

// Tests mirror the disk integration suite, adapted for the in-memory backend.

func TestMemLockLifecycle(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	ctx := context.Background()
	key := "mem-lifecycle-" + uuidv7.NewString()
	runLifecycleTest(t, ctx, cli, key, "mem-worker")
}

func TestMemAcquireNoWaitReturnsWaiting(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	ctx := context.Background()
	key := "mem-nowait-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "holder", 30, lockdclient.BlockWaitForever)
	t.Cleanup(func() { _ = releaseLease(t, ctx, lease) })

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	start := time.Now()
	_, err := cli.Acquire(reqCtx, api.AcquireRequest{
		Key:        key,
		Owner:      "no-wait",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockNoWait,
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected no-wait acquire to fail")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("expected no-wait acquire within 5s, got %s", elapsed)
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "waiting" {
		t.Fatalf("expected waiting API error, got %v", err)
	}
}

func TestMemAcquireIfNotExistsReturnsAlreadyExists(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	locktest.RunAcquireIfNotExistsReturnsAlreadyExists(t, locktest.AcquireIfNotExistsConfig{
		Client:    cli,
		KeyPrefix: "mem-if-not-exists",
	})
}

func TestMemAttachmentsLifecycle(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	ctx := context.Background()
	key := "mem-attach-" + uuidv7.NewString()
	runAttachmentTest(t, ctx, cli, key)
}

func TestMemConcurrency(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	ctx := context.Background()
	key := "mem-concurrency-" + uuidv7.NewString()
	workers := 4
	iterations := 3
	ttl := int64(45)

	var updates atomic.Int64

	var wg sync.WaitGroup
	wg.Add(workers)
	for id := 0; id < workers; id++ {
		go func(workerID int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", workerID)
			for iter := 0; iter < iterations; {
				lease := acquireWithRetry(t, ctx, cli, key, owner, ttl, 20)
				state, _, _, err := getStateJSON(ctx, lease)
				if err != nil {
					_ = releaseLease(t, ctx, lease)
					continue
				}
				var counter float64
				if state != nil {
					if v, ok := state["counter"]; ok {
						counter, _ = v.(float64)
					}
				}
				counter++
				body, _ := json.Marshal(map[string]any{"counter": counter, "last": owner})
				_, err = lease.UpdateBytes(ctx, body)
				if err != nil {
					var apiErr *lockdclient.APIError
					if errors.As(err, &apiErr) {
						code := apiErr.Response.ErrorCode
						if code == "meta_conflict" || code == "version_conflict" || code == "etag_mismatch" {
							_ = releaseLease(t, ctx, lease)
							continue
						}
					}
					t.Fatalf("update state: %v", err)
				}
				updates.Add(1)
				_ = releaseLease(t, ctx, lease)
				iter++
			}
		}(id)
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli, key, "verifier", ttl, lockdclient.BlockWaitForever)
	finalState, _, _, err := getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, verifier) {
		t.Fatalf("expected release success")
	}

	expected := float64(workers * iterations)
	if got := float64(updates.Load()); got != expected {
		t.Fatalf("expected %f successful updates, got %f", expected, got)
	}
	if finalState == nil {
		t.Fatalf("expected final state, got nil")
	}
	if value, ok := finalState["counter"].(float64); !ok || value != expected {
		t.Fatalf("expected counter %.0f, got %v", expected, finalState["counter"])
	}
}

func TestMemAutoKeyAcquire(t *testing.T) {
	runMemAutoKeyAcquireScenario(t, "mem-auto")
}

func TestMemPublicGetAfterRelease(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "mem-public-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "public-reader", 30, lockdclient.BlockWaitForever)
	stateBody, _ := json.Marshal(map[string]any{"payload": "public", "count": 1})
	if _, err := lease.UpdateBytes(ctx, stateBody); err != nil {
		t.Fatalf("update state: %v", err)
	}

	// Release the lease so the key has no active holder.
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Attempt a normal leased GET using the old lease id/token – expect lease_required.
	cli.RegisterLeaseToken(lease.LeaseID, lease.FencingToken)
	staleCtx, staleCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer staleCancel()
	_, err := cli.Get(staleCtx, key,
		lockdclient.WithGetLeaseID(lease.LeaseID),
		lockdclient.WithGetPublicDisabled(true),
	)
	var apiErr *lockdclient.APIError
	if err == nil || !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "lease_required" {
		t.Fatalf("expected lease_required from stale lease get, got %v", err)
	}

	// Public GET should succeed without a lease.
	publicCtx, publicCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publicCancel()
	resp, err := cli.Get(publicCtx, key)
	if err != nil {
		t.Fatalf("public get failed: %v", err)
	}
	if resp == nil || !resp.HasState {
		t.Fatalf("expected public payload")
	}
	defer resp.Close()
	data, err := resp.Bytes()
	if err != nil {
		t.Fatalf("read public body: %v", err)
	}
	if len(resp.ETag) == 0 || len(resp.Version) == 0 {
		t.Fatalf("expected etag and version, got etag=%q version=%q", resp.ETag, resp.Version)
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("decode public payload: %v", err)
	}
	if payload["payload"] != "public" {
		t.Fatalf("unexpected public payload: %+v", payload)
	}
}

func TestMemShutdownDrainingBlocksAcquire(t *testing.T) {
	ctx := context.Background()
	cfg := buildMemConfig(t)
	ts := startMemTestServer(t, cfg)
	t.Cleanup(func() { _ = ts.Stop(context.Background()) })
	cli := ts.Client
	if cli == nil {
		t.Fatalf("nil test server client")
	}
	key := "mem-drain-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "holder", 30, lockdclient.BlockWaitForever)
	stopCh := make(chan error, 1)
	go func() {
		stopCh <- ts.Stop(context.Background(), lockd.WithDrainLeases(1500*time.Millisecond), lockd.WithShutdownTimeout(2*time.Second))
	}()
	acquirePayload, _ := json.Marshal(api.AcquireRequest{Key: "mem-drain-wait", Owner: "drain-tester", TTLSeconds: 5})
	url := ts.URL() + "/v1/acquire"
	httpClient, err := ts.NewHTTPClient()
	if err != nil {
		t.Fatalf("build http client: %v", err)
	}
	result := shutdowntest.WaitForShutdownDrainingAcquireWithClient(t, httpClient, url, acquirePayload)
	if result.Response.ErrorCode != "shutdown_draining" {
		t.Fatalf("expected shutdown_draining error, got %+v", result.Response)
	}
	if result.Header.Get("Shutdown-Imminent") == "" {
		t.Fatalf("expected Shutdown-Imminent header, got %v", result.Header)
	}
	select {
	case err := <-stopCh:
		if err != nil {
			t.Fatalf("server stop failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("server stop timed out")
	}
	_ = lease.Release(ctx)
}

func runMemAutoKeyAcquireScenario(t *testing.T, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Owner:      owner,
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.Key == "" {
		t.Fatal("expected generated key")
	}
	if err := lease.Save(ctx, map[string]any{"owner": owner}); err != nil {
		lease.Release(ctx)
		t.Fatalf("save: %v", err)
	}
	key := lease.Key
	if !releaseLease(t, ctx, lease) {
		t.Fatalf("release failed")
	}

	verify := acquireWithRetry(t, ctx, cli, key, owner+"-verify", 20, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verify)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if state["owner"] != owner {
		t.Fatalf("unexpected state: %+v", state)
	}
	if !releaseLease(t, ctx, verify) {
		t.Fatalf("verify release failed")
	}
}

// Additional tests mirroring the disk suite follow ...

func TestMemRemoveSingleServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	key := "mem-remove-single-" + uuidv7.NewString()

	writer := acquireWithRetry(t, ctx, cli, key, "writer", 30, lockdclient.BlockWaitForever)
	if err := writer.Save(ctx, map[string]any{"payload": "seed", "count": 1.0}); err != nil {
		t.Fatalf("writer save: %v", err)
	}
	releaseLease(t, ctx, writer)

	remover := acquireWithRetry(t, ctx, cli, key, "remover", 30, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, remover)
	if err != nil {
		t.Fatalf("remover get: %v", err)
	}
	if state["payload"] != "seed" {
		t.Fatalf("expected seed payload, got %+v", state)
	}
	res, err := remover.Remove(ctx)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !res.Removed {
		t.Fatalf("expected removal, got %+v", res)
	}
	releaseLease(t, ctx, remover)

	verify := acquireWithRetry(t, ctx, cli, key, "verifier", 30, lockdclient.BlockWaitForever)
	state, _, _, err = getStateJSON(ctx, verify)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state after remove, got %+v", state)
	}
	releaseLease(t, ctx, verify)
}

func TestMemRemoveAcquireForUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	key := "mem-remove-update-" + uuidv7.NewString()

	err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		return af.Save(handlerCtx, map[string]any{"payload": "from-update", "count": 1.0})
	})
	if err != nil {
		t.Fatalf("seed acquire-for-update: %v", err)
	}

	remover := acquireWithRetry(t, ctx, cli, key, "remover", 30, lockdclient.BlockWaitForever)
	res, err := remover.Remove(ctx)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !res.Removed {
		t.Fatalf("expected removal, got %+v", res)
	}
	releaseLease(t, ctx, remover)
}

func TestMemRejectsReservedNamespace(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  ".txns",
		Key:        "should-fail",
		Owner:      "tester",
		TTLSeconds: 5,
	})
	if err == nil {
		t.Fatalf("expected acquire to fail for reserved namespace")
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if apiErr.Status != http.StatusBadRequest || apiErr.Response.ErrorCode != "invalid_namespace" {
		t.Fatalf("unexpected error: status=%d code=%s detail=%s", apiErr.Status, apiErr.Response.ErrorCode, apiErr.Response.Detail)
	}
}

func TestMemAcquireForUpdateConcurrency(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "mem-for-update-concurrency-" + uuidv7.NewString()
	workers := 4
	iterations := 6
	var updates atomic.Int64

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(workerID int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", workerID)
			for iter := 0; iter < iterations; iter++ {
				err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
					Key:        key,
					Owner:      owner,
					TTLSeconds: 20,
					BlockSecs:  lockdclient.BlockWaitForever,
				}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
					var payload map[string]any
					if af.State != nil && af.State.HasState {
						if err := af.State.Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
							return err
						}
					}
					if payload == nil {
						payload = make(map[string]any)
					}
					payload["owner"] = owner
					var count float64
					if v, ok := payload["count"].(float64); ok {
						count = v
					}
					payload["count"] = count + 1
					updates.Add(1)
					return af.Save(handlerCtx, payload)
				})
				if err != nil {
					t.Fatalf("acquire-for-update: %v", err)
				}
			}
		}(worker)
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli, key, "verifier", 20, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	releaseLease(t, ctx, verifier)

	if state == nil {
		t.Fatalf("expected final state")
	}
	if _, ok := state["owner"].(string); !ok {
		t.Fatalf("expected owner field in state")
	}
	expected := float64(updates.Load())
	if got := state["count"].(float64); got != expected {
		t.Fatalf("expected count %f, got %f", expected, got)
	}
}

func TestMemRemoveMultiServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	backend := memorybackend.New()
	cfg := buildMemConfig(t)
	cfg.HAMode = "concurrent"

	primary := startMemTestServerWithBackend(t, cfg, backend)
	secondary := startMemTestServerWithBackend(t, cfg, backend)

	key := "mem-remove-multi-" + uuidv7.NewString()

	writer := acquireWithRetry(t, ctx, primary.Client, key, "writer", 30, lockdclient.BlockWaitForever)
	if err := writer.Save(ctx, map[string]any{"payload": "shared", "count": 1.0}); err != nil {
		t.Fatalf("writer save: %v", err)
	}
	releaseLease(t, ctx, writer)

	remover := acquireWithRetry(t, ctx, primary.Client, key, "remover", 30, lockdclient.BlockWaitForever)
	res, err := remover.Remove(ctx)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !res.Removed {
		t.Fatalf("expected removal, got %+v", res)
	}
	releaseLease(t, ctx, remover)

	verifier := acquireWithRetry(t, ctx, secondary.Client, key, "verifier", 30, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state on second server, got %+v", state)
	}
	releaseLease(t, ctx, verifier)
}

func TestMemRemoveCASMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	key := "mem-remove-cas-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "cas-owner", 30, lockdclient.BlockWaitForever)

	if err := lease.Save(ctx, map[string]any{"payload": "v1"}); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	staleETag := lease.StateETag
	if err := lease.Save(ctx, map[string]any{"payload": "v2"}); err != nil {
		t.Fatalf("second save: %v", err)
	}
	currentVersion := lease.Version

	staleOpts := lockdclient.RemoveOptions{
		IfETag:    staleETag,
		IfVersion: strconv.FormatInt(currentVersion, 10),
	}
	if _, err := lease.RemoveWithOptions(ctx, staleOpts); err == nil {
		t.Fatalf("expected stale remove to fail")
	}

	res, err := lease.Remove(ctx)
	if err != nil {
		t.Fatalf("remove after conflict: %v", err)
	}
	if !res.Removed {
		t.Fatalf("expected removal success, got %+v", res)
	}
	releaseLease(t, ctx, lease)

	verify := acquireWithRetry(t, ctx, cli, key, "cas-verify", 30, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verify)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state after remove, got %+v", state)
	}
	releaseLease(t, ctx, verify)
}

func TestMemRemoveKeepAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	key := "mem-remove-keepalive-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "keepalive-owner", 45, lockdclient.BlockWaitForever)

	if err := lease.Save(ctx, map[string]any{"payload": "seed", "count": 1.0}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	originalETag := lease.StateETag
	originalVersion := lease.Version

	res, err := lease.Remove(ctx)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !res.Removed {
		t.Fatalf("expected removal, got %+v", res)
	}

	if _, err := lease.KeepAlive(ctx, 30*time.Second); err != nil {
		t.Fatalf("keepalive after remove: %v", err)
	}

	staleUpdate := lockdclient.UpdateOptions{
		IfETag:    originalETag,
		IfVersion: strconv.FormatInt(originalVersion, 10),
	}
	if _, err := lease.UpdateWithOptions(ctx, bytes.NewReader([]byte(`{"payload":"stale"}`)), staleUpdate); err == nil {
		t.Fatalf("expected stale update to fail")
	}

	if err := lease.Save(ctx, map[string]any{"payload": "fresh", "count": 2.0}); err != nil {
		t.Fatalf("save after remove: %v", err)
	}
	finalVersion := lease.Version
	releaseLease(t, ctx, lease)

	verify := acquireWithRetry(t, ctx, cli, key, "keepalive-verify", 30, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verify)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state["payload"] != "fresh" {
		t.Fatalf("expected fresh payload, got %+v", state)
	}
	if verify.Version != finalVersion {
		t.Fatalf("expected version %d, got %d", finalVersion, verify.Version)
	}
	releaseLease(t, ctx, verify)
}

func TestMemRemoveFailoverMultiServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	backend := memorybackend.New()
	cfg := buildMemConfig(t)

	closeDefaults := lockd.WithTestCloseDefaults(
		lockd.WithDrainLeases(8*time.Second),
		lockd.WithShutdownTimeout(10*time.Second),
	)
	var sharedCreds lockd.TestMTLSCredentials
	if cryptotest.TestMTLSEnabled() {
		sharedCreds = cryptotest.SharedMTLSCredentials(t)
	}
	primary := startMemTestServerWithBackendOpts(t, cfg, backend, append([]lockd.TestServerOption{closeDefaults}, cryptotest.SharedMTLSOptions(t)...)...)
	secondary := startMemTestServerWithBackendOpts(t, cfg, backend, append([]lockd.TestServerOption{closeDefaults}, cryptotest.SharedMTLSOptions(t)...)...)

	key := "mem-remove-failover-" + uuidv7.NewString()

	activeServer, seed, err := hatest.FindActiveServer(ctx, primary, secondary)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	standbyServer := secondary
	if activeServer == secondary {
		standbyServer = primary
	}
	t.Cleanup(func() { _ = seed.Close() })

	seedLease, err := seed.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(ctx, map[string]any{"payload": "seed"}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(ctx); err != nil {
		t.Fatalf("seed release: %v", err)
	}

	clientLogger, recorder := testlog.NewRecorder(t, pslog.TraceLevel)
	clientOptions := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(2 * time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
	}
	if cryptotest.TestMTLSEnabled() {
		httpClient := cryptotest.RequireMTLSHTTPClient(t, sharedCreds)
		clientOptions = append(clientOptions, lockdclient.WithHTTPClient(httpClient))
	}
	failoverCli, err := lockdclient.NewWithEndpoints([]string{activeServer.URL(), standbyServer.URL()}, clientOptions...)
	if err != nil {
		t.Fatalf("failover client: %v", err)
	}
	t.Cleanup(func() { _ = failoverCli.Close() })

	if err := activeServer.Server.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown active server: %v", err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 12*time.Second)
	if err := hatest.WaitForActive(waitCtx, standbyServer); err != nil {
		waitCancel()
		t.Fatalf("standby activation: %v", err)
	}
	waitCancel()

	err = failoverCli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "remover",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		_, err := af.Remove(handlerCtx)
		return err
	})
	if err != nil {
		t.Fatalf("failover remove: %v", err)
	}

	assertRemoveFailoverLogs(t, recorder, activeServer.URL(), standbyServer.URL())
}

func TestMemAutoKeyAcquireForUpdate(t *testing.T) {
	runMemAutoKeyAcquireForUpdateScenario(t, "mem-auto")
}

func runMemAutoKeyAcquireForUpdateScenario(t *testing.T, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Owner:      owner,
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		if af.Session.Key == "" {
			return fmt.Errorf("expected generated key")
		}
		return af.Save(handlerCtx, map[string]any{"owner": owner})
	})
	if err != nil {
		t.Fatalf("auto acquire-for-update: %v", err)
	}
}

func TestMemAcquireForUpdateConnectionDrop(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "mem-for-update-drop-" + uuidv7.NewString()

	var firstLease atomic.Value
	acquired := make(chan struct{}, 1)
	handlerCtx, handlerCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- cli.AcquireForUpdate(handlerCtx, api.AcquireRequest{
			Key:        key,
			Owner:      "dropper",
			TTLSeconds: 15,
			BlockSecs:  lockdclient.BlockWaitForever,
		}, func(cbCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
			firstLease.Store(af.Session.LeaseID)
			select {
			case acquired <- struct{}{}:
			default:
			}
			<-cbCtx.Done()
			return cbCtx.Err()
		})
	}()

	select {
	case <-acquired:
	case err := <-errCh:
		t.Fatalf("acquire-for-update ended before handler started: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for acquire-for-update handler")
	}

	handlerCancel()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("acquire-for-update did not return after cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}

	firstLeaseID, _ := firstLease.Load().(string)
	if firstLeaseID == "" {
		t.Fatalf("first acquire-for-update did not produce a lease id")
	}
	second := acquireWithRetry(t, ctx, cli, key, "re-acquire", 15, lockdclient.BlockWaitForever)
	if second.LeaseID == firstLeaseID {
		t.Fatalf("expected a new lease after simulated drop, got %q", second.LeaseID)
	}
	releaseLease(t, ctx, second)
}

func TestMemAcquireForUpdateRandomPayloads(t *testing.T) {
	cfg := buildMemConfig(t)
	cli := startMemServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "mem-for-update-random-" + uuidv7.NewString()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const iterations = 40
	for i := 0; i < iterations; i++ {
		payload := map[string]any{
			"iteration": i,
			"value":     rng.Int63(),
			"text":      randomJSONSafeString(rng),
			"time":      time.Now().UTC().Format(time.RFC3339Nano),
		}
		err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      "random",
			TTLSeconds: 20,
			BlockSecs:  lockdclient.BlockWaitForever,
		}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
			return af.Save(handlerCtx, payload)
		})
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}
}

func TestMemAcquireForUpdateCallbackSingleServer(t *testing.T) {
	cfg := buildMemConfig(t)
	ts := startMemTestServer(t, cfg)
	cli := directMemClient(t, ts)
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := "mem-callback-single-" + uuidv7.NewString()

	browser := map[string]any{"payload": "seed", "count": 1.0}
	err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		return af.Save(handlerCtx, browser)
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}

	err = cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "reader",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		if af.State == nil || !af.State.HasState {
			return fmt.Errorf("expected snapshot")
		}
		var snapshot map[string]any
		if err := af.State.Decode(&snapshot); err != nil {
			return err
		}
		if snapshot["payload"] != "seed" {
			return fmt.Errorf("unexpected snapshot: %+v", snapshot)
		}
		delete(snapshot, "payload")
		snapshot["count"] = snapshot["count"].(float64) + 1
		return af.Save(handlerCtx, snapshot)
	})
	if err != nil {
		t.Fatalf("reader acquire: %v", err)
	}
}

func TestMemAcquireForUpdateCallbackFailoverMultiServer(t *testing.T) {
	phases := []failoverPhase{
		failoverDuringHandlerStart,
		failoverBeforeSave,
		failoverAfterSave,
	}

	for _, phase := range phases {
		phase := phase
		t.Run("mem/"+phase.String(), func(t *testing.T) {
			runMemAcquireForUpdateCallbackFailoverMultiServer(t, phase)
		})
	}
}

func TestMemMultiReplica(t *testing.T) {
	backend := memorybackend.New()
	cfg := buildMemConfig(t)
	cfg.HAMode = "concurrent"

	primary := startMemTestServerWithBackend(t, cfg, backend)
	secondary := startMemTestServerWithBackend(t, cfg, backend)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := "mem-multi-replica-" + uuidv7.NewString()

	cli := directMemClient(t, primary)
	t.Cleanup(func() { _ = cli.Close() })

	lease := acquireWithRetry(t, ctx, cli, key, "writer", 20, lockdclient.BlockWaitForever)
	if err := lease.Save(ctx, map[string]any{"payload": "shared"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	releaseLease(t, ctx, lease)

	verifier := acquireWithRetry(t, ctx, secondary.Client, key, "verifier", 20, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state["payload"] != "shared" {
		t.Fatalf("unexpected state: %+v", state)
	}
	releaseLease(t, ctx, verifier)
}
func runMemAcquireForUpdateCallbackFailoverMultiServer(t *testing.T, phase failoverPhase) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	watchdog := time.AfterFunc(30*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("runMemAcquireForUpdateCallbackFailoverMultiServer timeout after 30s:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	backend := memorybackend.New()
	cfg := buildMemConfig(t)

	const disconnectAfter = 1 * time.Second
	chaos := &lockd.ChaosConfig{
		Seed:            4242 + int64(phase),
		DisconnectAfter: disconnectAfter,
		MinDelay:        5 * time.Millisecond,
		MaxDelay:        20 * time.Millisecond,
		MaxDisconnects:  1,
	}

	var sharedCreds lockd.TestMTLSCredentials
	if cryptotest.TestMTLSEnabled() {
		sharedCreds = cryptotest.SharedMTLSCredentials(t)
	}
	primaryOptions := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestBackend(backend),
		lockd.WithTestChaos(chaos),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
			lockdclient.WithHTTPTimeout(time.Second),
		),
		lockd.WithTestCloseDefaults(
			lockd.WithDrainLeases(8*time.Second),
			lockd.WithShutdownTimeout(10*time.Second),
		),
	}
	primaryOptions = append(primaryOptions, cryptotest.SharedMTLSOptions(t)...)
	primary := lockd.StartTestServer(t, primaryOptions...)
	backupOptions := append([]lockd.TestServerOption(nil), cryptotest.SharedMTLSOptions(t)...)
	backupOptions = append(backupOptions,
		lockd.WithTestConfig(cfg),
		lockd.WithTestBackend(backend),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
			lockdclient.WithHTTPTimeout(time.Second),
		),
		lockd.WithTestCloseDefaults(
			lockd.WithDrainLeases(8*time.Second),
			lockd.WithShutdownTimeout(10*time.Second),
		),
	)
	backup := lockd.StartTestServer(t, backupOptions...)

	failoverBlob := strings.Repeat("mem-failover-", 32768)
	key := fmt.Sprintf("mem-multi-%s-%s", phase.String(), uuidv7.NewString())

	activeServer, seedCli, err := hatest.FindActiveServer(ctx, primary, backup)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	standbyServer := backup
	if activeServer == backup {
		standbyServer = primary
	}
	t.Cleanup(func() { _ = seedCli.Close() })

	seedLease, err := seedCli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(ctx, map[string]any{
		"payload": "mem-multi",
		"count":   2,
		"blob":    failoverBlob,
	}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(ctx); err != nil {
		t.Fatalf("seed release: %v", err)
	}

	clientLogger, clientLogs := testlog.NewRecorder(t, pslog.TraceLevel)
	failoverOptions := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(2 * time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
	}
	if cryptotest.TestMTLSEnabled() {
		httpClient := cryptotest.RequireMTLSHTTPClient(t, sharedCreds)
		failoverOptions = append(failoverOptions, lockdclient.WithHTTPClient(httpClient))
	}
	failoverClient, err := lockdclient.NewWithEndpoints(
		[]string{activeServer.URL(), standbyServer.URL()},
		failoverOptions...,
	)
	if err != nil {
		t.Fatalf("failover client: %v", err)
	}
	t.Cleanup(func() { _ = failoverClient.Close() })

	var (
		shutdownOnce   sync.Once
		shutdownErr    error
		shutdownCtx    context.Context
		shutdownCancel context.CancelFunc
	)
	triggerFailover := func() error {
		shutdownOnce.Do(func() {
			shutdownCtx, shutdownCancel = context.WithTimeout(context.Background(), 12*time.Second)
			shutdownErr = activeServer.Server.ShutdownWithOptions(shutdownCtx,
				lockd.WithDrainLeases(8*time.Second),
				lockd.WithShutdownTimeout(10*time.Second),
			)
			if shutdownErr != nil {
				return
			}
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer waitCancel()
			if err := hatest.WaitForActive(waitCtx, standbyServer); err != nil {
				shutdownErr = fmt.Errorf("standby activation: %w", err)
			}
		})
		return shutdownErr
	}
	t.Cleanup(func() {
		if shutdownCancel != nil {
			shutdownCancel()
		}
	})

	handlerCalled := false
	err = failoverClient.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "reader-multi",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		handlerCalled = true
		if phase == failoverDuringHandlerStart {
			if err := triggerFailover(); err != nil {
				return fmt.Errorf("shutdown primary: %w", err)
			}
		}
		if af.State == nil || !af.State.HasState {
			return fmt.Errorf("expected snapshot for %q", key)
		}
		var snapshot map[string]any
		if err := af.State.Decode(&snapshot); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}
		if snapshot["payload"] != "mem-multi" {
			return fmt.Errorf("unexpected snapshot: %+v", snapshot)
		}
		if phase == failoverBeforeSave {
			if err := triggerFailover(); err != nil {
				return fmt.Errorf("shutdown primary: %w", err)
			}
		}
		snapshot["count"] = snapshot["count"].(float64) + 1
		snapshot["blob"] = failoverBlob
		if err := af.Save(handlerCtx, snapshot); err != nil {
			return fmt.Errorf("save: %w", err)
		}
		if phase == failoverAfterSave {
			if err := triggerFailover(); err != nil {
				return fmt.Errorf("shutdown primary: %w", err)
			}
		}
		return nil
	})
	expectedConflict := phase == failoverAfterSave
	conflictObserved := false
	if err != nil {
		var apiErr *lockdclient.APIError
		if expectedConflict && errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "version_conflict" {
			conflictObserved = true
			t.Logf("phase %s observed expected version_conflict after failover: %v", phase, err)
		} else {
			t.Fatalf("acquire-for-update failover: %v\n%s", err, clientLogs.Summary())
		}
	}
	if !handlerCalled {
		t.Fatalf("handler never called")
	}

	result := recordFailoverLogs(t, clientLogs, activeServer.URL(), standbyServer.URL())
	if result.AlternateEndpoint != standbyServer.URL() {
		t.Fatalf("expected failover to standby %q, got %+v", standbyServer.URL(), result)
	}
	if conflictObserved {
		if result.AlternateStatus != http.StatusConflict {
			t.Fatalf("expected HTTP 409 on backup during conflict in phase %s; got %d\nlogs:\n%s", phase, result.AlternateStatus, clientLogs.Summary())
		}
	} else if result.AlternateStatus != http.StatusOK {
		t.Fatalf("expected HTTP 200 on backup during phase %s; got %d\nlogs:\n%s", phase, result.AlternateStatus, clientLogs.Summary())
	}
}

func buildMemConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	cfg := lockd.Config{
		Store:           "mem://",
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
		HAMode:          "failover",
	}
	cfg.DisableMemQueueWatch = false
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	return cfg
}

func startMemServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(60*time.Second),
			lockdclient.WithCloseTimeout(60*time.Second),
			lockdclient.WithKeepAliveTimeout(60*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
	}
	options = append(options, cryptotest.SharedMTLSOptions(tb)...)
	ts := lockd.StartTestServer(tb, options...)
	return ts.Client
}

func startMemTestServer(tb testing.TB, cfg lockd.Config) *lockd.TestServer {
	tb.Helper()
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(60*time.Second),
			lockdclient.WithCloseTimeout(60*time.Second),
			lockdclient.WithKeepAliveTimeout(60*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
	}
	options = append(options, cryptotest.SharedMTLSOptions(tb)...)
	return lockd.StartTestServer(tb, options...)
}

func startMemTestServerWithBackend(tb testing.TB, cfg lockd.Config, backend *memorybackend.Store) *lockd.TestServer {
	tb.Helper()
	return startMemTestServerWithBackendOpts(tb, cfg, backend)
}

func startMemTestServerWithBackendOpts(tb testing.TB, cfg lockd.Config, backend *memorybackend.Store, extra ...lockd.TestServerOption) *lockd.TestServer {
	tb.Helper()
	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestBackend(backend),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(60*time.Second),
			lockdclient.WithCloseTimeout(60*time.Second),
			lockdclient.WithKeepAliveTimeout(60*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
	}
	opts = append(opts, extra...)
	opts = append(opts, cryptotest.SharedMTLSOptions(tb)...)
	return lockd.StartTestServer(tb, opts...)
}

func directMemClient(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
	t.Helper()
	if ts == nil || ts.Server == nil {
		t.Fatalf("nil test server")
	}
	addr := ts.Server.ListenerAddr()
	if addr == nil {
		t.Fatalf("listener not initialized")
	}
	scheme := "http"
	if ts.Config.MTLSEnabled() {
		scheme = "https"
	}
	endpoint := fmt.Sprintf("%s://%s", scheme, addr.String())
	cli, err := ts.NewEndpointsClient([]string{endpoint},
		lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
		lockdclient.WithHTTPTimeout(time.Second),
		lockdclient.WithEndpointShuffle(false),
	)
	if err != nil {
		t.Fatalf("direct client: %v", err)
	}
	return cli
}

func runLifecycleTest(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, owner string) {
	lease := acquireWithRetry(t, ctx, cli, key, owner, 45, lockdclient.BlockWaitForever)

	state, _, _, err := getStateJSON(ctx, lease)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if state != nil {
		t.Fatalf("expected nil state, got %v", state)
	}

	payload, _ := json.Marshal(map[string]any{"cursor": 42, "backend": "mem"})
	if _, err := lease.UpdateBytes(ctx, payload); err != nil {
		t.Fatalf("update state: %v", err)
	}

	state, _, _, err = getStateJSON(ctx, lease)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if cursor, ok := state["cursor"].(float64); !ok || cursor != 42 {
		t.Fatalf("expected cursor 42, got %v", state["cursor"])
	}

	if !releaseLease(t, ctx, lease) {
		t.Fatalf("expected release success")
	}
}

func runAttachmentTest(t *testing.T, ctx context.Context, cli *lockdclient.Client, key string) {
	t.Helper()
	lease := acquireWithRetry(t, ctx, cli, key, "attach-worker", 30, lockdclient.BlockWaitForever)
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

	lease2 := acquireWithRetry(t, ctx, cli, key, "attach-delete", 30, lockdclient.BlockWaitForever)
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

	lease3 := acquireWithRetry(t, ctx, cli, key, "attach-clear", 30, lockdclient.BlockWaitForever)
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

func randomJSONSafeString(r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	length := 8 + r.Intn(16)
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

func acquireWithRetry(tb testing.TB, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl, block int64) *lockdclient.LeaseSession {
	tb.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		lease, err := cli.Acquire(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      owner,
			TTLSeconds: ttl,
			BlockSecs:  block,
		})
		if err == nil {
			return lease
		}
		if time.Now().After(deadline) {
			tb.Fatalf("acquire failed: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func getStateJSON(ctx context.Context, sess *lockdclient.LeaseSession) (map[string]any, string, string, error) {
	snap, err := sess.Get(ctx)
	if err != nil {
		return nil, "", "", err
	}
	if snap == nil {
		return nil, "", "", nil
	}
	defer snap.Close()
	var payload map[string]any
	if snap.Reader != nil {
		if err := json.NewDecoder(snap.Reader).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
			return nil, "", "", err
		}
	}
	versionStr := ""
	if snap.Version > 0 {
		versionStr = strconv.FormatInt(snap.Version, 10)
	}
	return payload, snap.ETag, versionStr, nil
}

func releaseLease(tb testing.TB, ctx context.Context, sess *lockdclient.LeaseSession) bool {
	tb.Helper()
	if err := sess.Release(ctx); err != nil {
		tb.Fatalf("release: %v", err)
	}
	return true
}

func distributeQuota(total, workers int) []int {
	if workers <= 0 {
		return nil
	}
	base := total / workers
	rem := total % workers
	out := make([]int, workers)
	for i := 0; i < workers; i++ {
		out[i] = base
		if i < rem {
			out[i]++
		}
	}
	return out
}

func shouldRetryQueueError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var apiErr *lockdclient.APIError
	if errors.As(err, &apiErr) {
		if apiErr.Response.ErrorCode == "waiting" {
			return true
		}
		if apiErr.Response.RetryAfterSeconds > 0 {
			return true
		}
	}
	if strings.Contains(err.Error(), "EOF") {
		return true
	}
	return false
}

func recordFailoverLogs(t testing.TB, rec *testlog.Recorder, primary, backup string) failoverLogResult {
	const (
		completeMsg   = "client.acquire_for_update.success"
		handlerErrMsg = "client.acquire_for_update.handler_error"
		errorMsg      = "client.http.error"
		successMsg    = "client.http.success"
	)

	if _, ok := rec.First(func(e testlog.Entry) bool {
		if e.Message == completeMsg {
			return true
		}
		if e.Message == handlerErrMsg {
			errText := testlog.GetStringField(e, "error")
			return strings.Contains(errText, "version_conflict")
		}
		return false
	}); !ok {
		t.Fatalf("expected %s or %s entry; logs:\n%s", completeMsg, handlerErrMsg, rec.Summary())
	}

	errorEntry, ok := rec.First(func(e testlog.Entry) bool {
		if e.Message != errorMsg {
			return false
		}
		return testlog.GetStringField(e, "endpoint") == primary
	})
	if !ok {
		t.Fatalf("expected HTTP error against primary %q; logs:\n%s", primary, rec.Summary())
	}

	var status int
	var seen []int
	found := false
	for _, entry := range rec.Events() {
		if entry.Timestamp.Before(errorEntry.Timestamp) {
			continue
		}
		if entry.Message != successMsg {
			continue
		}
		if testlog.GetStringField(entry, "endpoint") != backup {
			continue
		}
		entryStatus, ok := testlog.GetIntField(entry, "status")
		if !ok {
			continue
		}
		seen = append(seen, entryStatus)
		if entryStatus == http.StatusOK || entryStatus == http.StatusConflict {
			status = entryStatus
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected HTTP 200/409 against backup %q; observed %v\nlogs:\n%s", backup, seen, rec.Summary())
	}

	return failoverLogResult{
		AlternateEndpoint: backup,
		AlternateStatus:   status,
		HadTransportError: true,
		ErrorEndpoint:     primary,
	}
}

func assertRemoveFailoverLogs(t testing.TB, rec *testlog.Recorder, primary, backup string) {
	const (
		completeMsg = "client.remove.success"
		errorMsg    = "client.http.error"
		successMsg  = "client.http.success"
	)

	if _, ok := rec.First(func(e testlog.Entry) bool {
		return e.Message == completeMsg
	}); !ok {
		t.Fatalf("expected %s entry; logs:\n%s", completeMsg, rec.Summary())
	}

	errorEntry, ok := rec.First(func(e testlog.Entry) bool {
		if e.Message != errorMsg {
			return false
		}
		return testlog.GetStringField(e, "endpoint") == primary
	})
	if !ok {
		t.Fatalf("expected HTTP error against primary %q; logs:\n%s", primary, rec.Summary())
	}

	successEntry, ok := rec.FirstAfter(errorEntry.Timestamp, func(e testlog.Entry) bool {
		if e.Message != successMsg {
			return false
		}
		return testlog.GetStringField(e, "endpoint") == backup
	})
	if !ok {
		t.Fatalf("expected HTTP success against backup %q; logs:\n%s", backup, rec.Summary())
	}

	status, ok := testlog.GetIntField(successEntry, "status")
	if !ok {
		t.Fatalf("%s missing status; logs:\n%s", successMsg, rec.Summary())
	}
	if status != http.StatusOK && status != http.StatusConflict {
		t.Fatalf("unexpected status %d on backup %q; logs:\n%s", status, backup, rec.Summary())
	}
}
