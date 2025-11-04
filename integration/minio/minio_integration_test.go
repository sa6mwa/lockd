//go:build integration && minio && !lq

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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	cryptotest "pkt.systems/lockd/integration/internal/cryptotest"
	shutdowntest "pkt.systems/lockd/integration/internal/shutdowntest"
	testlog "pkt.systems/lockd/integration/internal/testlog"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

func retryableTransportError(err error) bool {
	if err == nil {
		return false
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

func TestMinioStoreVerification(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
}

func TestMinioShutdownDrainingBlocksAcquire(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	ts := startMinioTestServer(t, cfg)
	t.Cleanup(func() { _ = ts.Stop(context.Background()) })
	cli := ts.Client
	if cli == nil {
		t.Fatalf("nil test server client")
	}
	ctx := context.Background()
	key := "minio-drain-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "holder", 30, lockdclient.BlockWaitForever)
	stopCh := make(chan error, 1)
	go func() {
		stopCh <- ts.Stop(context.Background(), lockd.WithDrainLeases(3*time.Second), lockd.WithShutdownTimeout(4*time.Second))
	}()
	payload, _ := json.Marshal(api.AcquireRequest{Key: "minio-drain-wait", Owner: "drain-tester", TTLSeconds: 5})
	url := ts.URL() + "/v1/acquire"
	httpClient, err := ts.NewHTTPClient()
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	result := shutdowntest.WaitForShutdownDrainingAcquireWithClient(t, httpClient, url, payload)
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
	case <-time.After(10 * time.Second):
		t.Fatalf("server stop timed out")
	}
	_ = lease.Release(ctx)
}

func TestMinioLockLifecycle(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "minio-lifecycle-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "minio-lifecycle-worker", 30, 5)

	state, etag, version, err := getStateJSON(ctx, cli, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if state != nil {
		t.Fatal("expected no initial state")
	}

	payload, _ := json.Marshal(map[string]any{"cursor": 7, "source": "minio"})
	opts := lockdclient.UpdateOptions{IfVersion: version, IfETag: etag}
	if opts.IfVersion == "" {
		opts.IfVersion = strconv.FormatInt(lease.Version, 10)
	}
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
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
	key := "minio-concurrency-" + uuidv7.NewString()
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
				if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, body, lockdclient.UpdateOptions{IfETag: etag, IfVersion: version}); err != nil {
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

func TestMinioAcquireForUpdateCallbackSingleServer(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)

	chaos := &lockd.ChaosConfig{
		Seed:            2024,
		DisconnectAfter: 200 * time.Millisecond,
		MinDelay:        5 * time.Millisecond,
		MaxDelay:        20 * time.Millisecond,
		MaxDisconnects:  1,
	}

	ts := startMinioTestServer(t, cfg, lockd.WithTestChaos(chaos))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	watchdog := time.AfterFunc(10*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("TestMinioAcquireForUpdateCallbackSingleServer timeout after 10s:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	key := "minio-single-" + uuidv7.NewString()
	defer cleanupMinio(t, cfg, key)

	proxiedClient := ts.Client
	if proxiedClient == nil {
		var err error
		proxiedClient, err = ts.NewClient(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
		)
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}
	t.Cleanup(func() { _ = proxiedClient.Close() })

	seedCli := directClient(t, ts)
	t.Cleanup(func() { _ = seedCli.Close() })

	seedLease, err := seedCli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(ctx, map[string]any{"payload": "single-server", "count": 1}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(ctx); err != nil {
		t.Fatalf("seed release: %v", err)
	}

	handlerCalled := false
	err = proxiedClient.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "reader-single",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		handlerCalled = true
		if af.State == nil || !af.State.HasState {
			return fmt.Errorf("expected snapshot for %q", key)
		}
		var snapshot map[string]any
		if err := af.State.Decode(&snapshot); err != nil {
			if loadErr := af.Load(handlerCtx, &snapshot); loadErr != nil {
				return fmt.Errorf("decode snapshot: %w", errors.Join(err, loadErr))
			}
		}
		if snapshot["payload"] != "single-server" {
			return fmt.Errorf("unexpected payload: %+v", snapshot)
		}
		var count float64
		if v, ok := snapshot["count"].(float64); ok {
			count = v
		}
		updated := map[string]any{
			"payload": "single-server",
			"count":   count + 1,
			"owner":   "reader-single",
		}
		var saveErr error
		for attempt := 0; attempt < 3; attempt++ {
			if saveErr = af.Save(handlerCtx, updated); saveErr == nil {
				return nil
			}
			if !retryableTransportError(saveErr) {
				return fmt.Errorf("save snapshot: %w", saveErr)
			}
			time.Sleep(50 * time.Millisecond * time.Duration(attempt+1))
		}
		return fmt.Errorf("save snapshot: %w", saveErr)
	}, lockdclient.WithAcquireFailureRetries(3))
	watchdog.Stop()
	if err != nil {
		t.Fatalf("acquire-for-update callback: %v", err)
	}
	if !handlerCalled {
		t.Fatalf("callback was not invoked")
	}

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer verifyCancel()
	verifyLease := acquireWithRetry(t, verifyCtx, seedCli, key, "verifier", 30, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(verifyCtx, seedCli, key, verifyLease.LeaseID)
	if err != nil {
		t.Fatalf("verify get_state: %v", err)
	}
	if !releaseLease(t, verifyCtx, seedCli, key, verifyLease.LeaseID) {
		t.Fatalf("verify release failed")
	}
	if state["owner"] != "reader-single" {
		t.Fatalf("unexpected owner after callback: %+v", state)
	}
	if count, ok := state["count"].(float64); !ok || count != 2 {
		t.Fatalf("unexpected count after callback: %v", state["count"])
	}
}

func TestMinioRemoveAcquireForUpdate(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)

	ts := startMinioTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)))
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	seed := func(key string, payload map[string]any) {
		lease := acquireWithRetry(t, ctx, cli, key, "seed", 30, lockdclient.BlockWaitForever)
		if err := lease.Save(ctx, payload); err != nil {
			t.Fatalf("seed save: %v", err)
		}
		releaseLease(t, ctx, cli, key, lease.LeaseID)
	}

	t.Run("remove-in-handler", func(t *testing.T) {
		key := "minio-remove-afu-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupMinio(t, cfg, key) })
		seed(key, map[string]any{"payload": "seed-value", "count": 7.0})

		handlerCalled := false
		err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      "remove-handler",
			TTLSeconds: 30,
			BlockSecs:  lockdclient.BlockWaitForever,
		}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
			handlerCalled = true
			if af.State == nil || !af.State.HasState {
				return fmt.Errorf("expected snapshot for %q", key)
			}
			var snapshot map[string]any
			if err := af.State.Decode(&snapshot); err != nil {
				return fmt.Errorf("decode snapshot: %w", err)
			}
			if snapshot["payload"] != "seed-value" {
				return fmt.Errorf("unexpected snapshot: %+v", snapshot)
			}
			res, err := af.Remove(handlerCtx)
			if err != nil {
				return fmt.Errorf("remove in handler: %w", err)
			}
			if res == nil {
				return fmt.Errorf("expected removal result")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("acquire-for-update: %v", err)
		}
		if !handlerCalled {
			t.Fatalf("handler not called")
		}

		verify := acquireWithRetry(t, ctx, cli, key, "remove-handler-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err := getStateJSON(ctx, cli, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state after handler remove, got %+v", state)
		}
		releaseLease(t, ctx, cli, key, verify.LeaseID)
	})

	t.Run("remove-and-recreate", func(t *testing.T) {
		key := "minio-remove-recreate-afu-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupMinio(t, cfg, key) })
		seed(key, map[string]any{"payload": "initial", "count": 1.0})

		err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      "remove-recreate",
			TTLSeconds: 30,
			BlockSecs:  lockdclient.BlockWaitForever,
		}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
			if af.State == nil || !af.State.HasState {
				return fmt.Errorf("expected snapshot for %q", key)
			}
			if _, err := af.Remove(handlerCtx); err != nil {
				return fmt.Errorf("remove before recreate: %w", err)
			}
			payload := map[string]any{"payload": "recreated", "count": 2.0}
			if err := af.Save(handlerCtx, payload); err != nil {
				return fmt.Errorf("save after remove: %w", err)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("acquire-for-update: %v", err)
		}

		verify := acquireWithRetry(t, ctx, cli, key, "remove-recreate-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err := getStateJSON(ctx, cli, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state["payload"] != "recreated" {
			t.Fatalf("expected recreated payload, got %+v", state)
		}
		if count, ok := state["count"].(float64); !ok || count != 2.0 {
			t.Fatalf("expected count 2.0, got %+v", state["count"])
		}
		releaseLease(t, ctx, cli, key, verify.LeaseID)
	})
}
func TestMinioRemoveSingleServer(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	t.Run("remove-after-update", func(t *testing.T) {
		key := "minio-remove-update-" + uuidv7.NewString()
		lease := acquireWithRetry(t, ctx, cli, key, "remover-update", 30, lockdclient.BlockWaitForever)
		if err := lease.Save(ctx, map[string]any{"value": "present", "count": 1.0}); err != nil {
			t.Fatalf("save: %v", err)
		}
		prevVersion := lease.Version
		res, err := lease.Remove(ctx)
		if err != nil {
			t.Fatalf("remove: %v", err)
		}
		if !res.Removed {
			t.Fatalf("expected removal, got %+v", res)
		}
		if res.NewVersion <= prevVersion {
			t.Fatalf("expected version bump, got %+v", res)
		}
		state, _, _, err := getStateJSON(ctx, cli, key, lease.LeaseID)
		if err != nil {
			t.Fatalf("get after remove: %v", err)
		}
		if state != nil {
			t.Fatalf("expected nil state after remove, got %+v", state)
		}
		releaseLease(t, ctx, cli, key, lease.LeaseID)

		verify := acquireWithRetry(t, ctx, cli, key, "remover-update-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, cli, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state post-remove, got %+v", state)
		}
		releaseLease(t, ctx, cli, key, verify.LeaseID)
		cleanupMinio(t, cfg, key)
	})

	t.Run("remove-without-state", func(t *testing.T) {
		key := "minio-remove-empty-" + uuidv7.NewString()
		lease := acquireWithRetry(t, ctx, cli, key, "remover-empty", 30, lockdclient.BlockWaitForever)
		res, err := lease.Remove(ctx)
		if err != nil {
			t.Fatalf("remove empty: %v", err)
		}
		if res != nil && res.Removed {
			// MinIO returns success even for missing objects; allow removed=false but tolerate true.
		}
		state, _, _, err := getStateJSON(ctx, cli, key, lease.LeaseID)
		if err != nil {
			t.Fatalf("get after empty remove: %v", err)
		}
		if state != nil {
			t.Fatalf("expected nil state, got %+v", state)
		}
		releaseLease(t, ctx, cli, key, lease.LeaseID)
		cleanupMinio(t, cfg, key)
	})

	t.Run("remove-after-reacquire", func(t *testing.T) {
		key := "minio-remove-reacquire-" + uuidv7.NewString()
		writer := acquireWithRetry(t, ctx, cli, key, "writer", 30, lockdclient.BlockWaitForever)
		if err := writer.Save(ctx, map[string]any{"step": "written"}); err != nil {
			t.Fatalf("writer save: %v", err)
		}
		releaseLease(t, ctx, cli, key, writer.LeaseID)

		remover := acquireWithRetry(t, ctx, cli, key, "remover", 30, lockdclient.BlockWaitForever)
		state, _, _, err := getStateJSON(ctx, cli, key, remover.LeaseID)
		if err != nil {
			t.Fatalf("remover get: %v", err)
		}
		if state["step"] != "written" {
			t.Fatalf("expected written state, got %+v", state)
		}
		res, err := remover.Remove(ctx)
		if err != nil {
			t.Fatalf("remove: %v", err)
		}
		if !res.Removed {
			t.Fatalf("expected removal, got %+v", res)
		}
		releaseLease(t, ctx, cli, key, remover.LeaseID)

		verify := acquireWithRetry(t, ctx, cli, key, "remover-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, cli, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state after remove, got %+v", state)
		}
		releaseLease(t, ctx, cli, key, verify.LeaseID)
		cleanupMinio(t, cfg, key)
	})
}

func TestMinioAcquireForUpdateCallbackFailover(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)

	productionClose := lockd.WithTestCloseDefaults(
		lockd.WithDrainLeases(8*time.Second),
		lockd.WithShutdownTimeout(10*time.Second),
	)
	primary := startMinioTestServer(t, cfg, productionClose)
	backup := startMinioTestServer(t, cfg, productionClose)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	watchdog := time.AfterFunc(30*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("TestMinioAcquireForUpdateCallbackFailover timeout:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	key := "minio-forupdate-failover-" + uuidv7.NewString()
	defer cleanupMinio(t, cfg, key)

	seedClient := backup.Client
	if seedClient == nil {
		var err error
		seedClient, err = backup.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)))
		if err != nil {
			t.Fatalf("seed client: %v", err)
		}
	}
	t.Cleanup(func() { _ = seedClient.Close() })

	seedLease, err := seedClient.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 45,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(ctx, map[string]any{"payload": "minio-failover", "count": 2}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(ctx); err != nil {
		t.Fatalf("seed release: %v", err)
	}

	if err := primary.Stop(context.Background()); err != nil {
		t.Fatalf("stop primary: %v", err)
	}

	clientLogger, clientLogs := testlog.NewRecorder(t, pslog.TraceLevel)
	clientOptions := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(90 * time.Second),
		lockdclient.WithCloseTimeout(90 * time.Second),
		lockdclient.WithKeepAliveTimeout(90 * time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
	}
	if primary.Config.MTLSEnabled() {
		creds := primary.TestMTLSCredentials()
		if !creds.Valid() {
			t.Fatalf("minio failover: missing MTLS credentials")
		}
		httpClient := cryptotest.RequireMTLSHTTPClient(t, creds)
		clientOptions = append(clientOptions, lockdclient.WithHTTPClient(httpClient))
	}
	failoverClient, err := lockdclient.NewWithEndpoints(
		[]string{primary.URL(), backup.URL()},
		clientOptions...,
	)
	if err != nil {
		t.Fatalf("failover client: %v", err)
	}
	defer failoverClient.Close()

	handlerCalled := false
	err = failoverClient.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "reader-failover",
		TTLSeconds: 45,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		handlerCalled = true
		var snapshot map[string]any
		if err := af.Load(handlerCtx, &snapshot); err != nil {
			return fmt.Errorf("load snapshot: %w", err)
		}
		updated := map[string]any{
			"payload": "minio-failover",
			"count":   3,
			"owner":   "reader-failover",
		}
		return af.Save(handlerCtx, updated)
	})
	if err != nil {
		t.Fatalf("acquire-for-update: %v\n%s", err, clientLogs.Summary())
	}
	if !handlerCalled {
		t.Fatalf("callback was not invoked")
	}

	verifier := acquireWithRetry(t, ctx, seedClient, key, "verifier", 45, lockdclient.BlockWaitForever)
	finalState, _, _, err := getStateJSON(ctx, seedClient, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("verify get_state: %v", err)
	}
	if !releaseLease(t, ctx, seedClient, key, verifier.LeaseID) {
		t.Fatalf("verify release failed")
	}
	if finalState == nil {
		t.Fatalf("expected final state after failover")
	}
	if owner, ok := finalState["owner"].(string); !ok || owner != "reader-failover" {
		t.Fatalf("unexpected owner: %+v", finalState)
	}
	if count, ok := finalState["count"].(float64); !ok || count != 3 {
		t.Fatalf("unexpected count: %v", finalState["count"])
	}

	primaryURL := primary.URL()
	backupURL := backup.URL()
	var sawError bool
	var sawSuccess bool
	for _, entry := range clientLogs.Events() {
		endpoint := testlog.GetStringField(entry, "endpoint")
		switch entry.Message {
		case "client.http.error":
			if endpoint == primaryURL {
				sawError = true
			}
		case "client.http.success":
			if endpoint == primaryURL {
				t.Fatalf("unexpected success on primary endpoint %s\nlogs:\n%s", primaryURL, clientLogs.Summary())
			}
			if endpoint == backupURL {
				sawSuccess = true
			}
		}
	}
	if !sawError {
		t.Fatalf("expected connection error on primary endpoint; logs:\n%s", clientLogs.Summary())
	}
	if !sawSuccess {
		t.Fatalf("expected HTTP success on backup endpoint; logs:\n%s", clientLogs.Summary())
	}
	t.Logf("minio failover log summary: error_endpoint=%s success_endpoint=%s", primaryURL, backupURL)
}

func assertMinioRemoveFailoverLogs(t testing.TB, rec *testlog.Recorder, primary, backup string) {
	const (
		startMsg        = "client.remove.start"
		successMsg      = "client.remove.success"
		transportErrMsg = "client.remove.transport_error"
		httpErrorMsg    = "client.http.error"
	)

	startEntry, ok := rec.First(func(e testlog.Entry) bool {
		return e.Message == startMsg
	})
	if !ok {
		t.Fatalf("expected %s entry; logs:\n%s", startMsg, rec.Summary())
	}
	initialEndpoint := testlog.GetStringField(startEntry, "endpoint")

	var errorEntry *testlog.Entry
	if entry, ok := rec.FirstAfter(startEntry.Timestamp, func(e testlog.Entry) bool {
		return e.Message == transportErrMsg || e.Message == httpErrorMsg
	}); ok {
		errorEntry = &entry
	}
	if errorEntry == nil {
		t.Fatalf("expected error entry on primary endpoint; logs:\n%s", rec.Summary())
	}
	if errEndpoint := testlog.GetStringField(*errorEntry, "endpoint"); errEndpoint != primary {
		t.Fatalf("expected error on primary %s, got %s; logs:\n%s", primary, errEndpoint, rec.Summary())
	}

	successEntry, ok := rec.FirstAfter(startEntry.Timestamp, func(e testlog.Entry) bool {
		return e.Message == successMsg
	})
	if !ok {
		t.Fatalf("expected %s entry; logs:\n%s", successMsg, rec.Summary())
	}
	successEndpoint := testlog.GetStringField(successEntry, "endpoint")
	if successEndpoint == initialEndpoint {
		t.Fatalf("expected success on alternate endpoint; logs:\n%s", rec.Summary())
	}
	if successEndpoint != backup {
		t.Fatalf("expected success on backup %s, got %s; logs:\n%s", backup, successEndpoint, rec.Summary())
	}

	t.Logf("minio remove failover log summary: initial=%s success=%s\n%s", initialEndpoint, successEndpoint, rec.Summary())
}

func TestMinioRemoveFailover(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)

	productionClose := lockd.WithTestCloseDefaults(
		lockd.WithDrainLeases(8*time.Second),
		lockd.WithShutdownTimeout(10*time.Second),
	)
	primary := startMinioTestServer(t, cfg, productionClose)
	backup := startMinioTestServer(t, cfg, productionClose)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	key := "minio-remove-failover-" + uuidv7.NewString()
	defer cleanupMinio(t, cfg, key)

	seedClient := backup.Client
	if seedClient == nil {
		var err error
		seedClient, err = backup.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)))
		if err != nil {
			t.Fatalf("seed client: %v", err)
		}
	}
	defer seedClient.Close()

	seedLease, err := seedClient.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 45,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(ctx, map[string]any{"payload": "seed", "count": 1.0}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	releaseLease(t, ctx, seedClient, key, seedLease.LeaseID)

	clientLogger, clientLogs := testlog.NewRecorder(t, pslog.TraceLevel)
	clientOptions := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(90 * time.Second),
		lockdclient.WithCloseTimeout(90 * time.Second),
		lockdclient.WithKeepAliveTimeout(90 * time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
	}
	if primary.Config.MTLSEnabled() {
		creds := primary.TestMTLSCredentials()
		if !creds.Valid() {
			t.Fatalf("minio failover: missing MTLS credentials")
		}
		httpClient := cryptotest.RequireMTLSHTTPClient(t, creds)
		clientOptions = append(clientOptions, lockdclient.WithHTTPClient(httpClient))
	}
	failoverClient, err := lockdclient.NewWithEndpoints(
		[]string{primary.URL(), backup.URL()},
		clientOptions...,
	)
	if err != nil {
		t.Fatalf("failover client: %v", err)
	}
	defer failoverClient.Close()

	lease := acquireWithRetry(t, ctx, failoverClient, key, "failover-remover", 45, lockdclient.BlockWaitForever)

	if err := primary.Stop(context.Background()); err != nil {
		t.Fatalf("stop primary: %v", err)
	}

	res, err := lease.Remove(ctx)
	if err != nil {
		t.Fatalf("remove during failover: %v\n%s", err, clientLogs.Summary())
	}
	if !res.Removed {
		t.Fatalf("expected removal success, got %+v", res)
	}
	releaseLease(t, ctx, failoverClient, key, lease.LeaseID)

	verify := acquireWithRetry(t, ctx, seedClient, key, "failover-verify", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, seedClient, key, verify.LeaseID)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state after failover remove, got %+v", state)
	}
	releaseLease(t, ctx, seedClient, key, verify.LeaseID)

	assertMinioRemoveFailoverLogs(t, clientLogs, primary.URL(), backup.URL())
}

func TestMinioRemoveCASMismatch(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	key := "minio-remove-cas-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "cas-owner", 45, lockdclient.BlockWaitForever)
	if err := lease.Save(ctx, map[string]any{"payload": "v1"}); err != nil {
		t.Fatalf("seed save: %v", err)
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
	} else if apiErr := (*lockdclient.APIError)(nil); !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "etag_mismatch" {
		t.Fatalf("expected etag_mismatch, got %v", err)
	}

	res, err := lease.Remove(ctx)
	if err != nil {
		t.Fatalf("remove after conflict: %v", err)
	}
	if !res.Removed {
		t.Fatalf("expected removal, got %+v", res)
	}
	releaseLease(t, ctx, cli, key, lease.LeaseID)

	verify := acquireWithRetry(t, ctx, cli, key, "cas-verify", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, cli, key, verify.LeaseID)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state after remove, got %+v", state)
	}
	releaseLease(t, ctx, cli, key, verify.LeaseID)
	cleanupMinio(t, cfg, key)
}

func TestMinioRemoveKeepAlive(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	key := "minio-remove-keepalive-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "keepalive-owner", 60, lockdclient.BlockWaitForever)
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
	} else if apiErr := (*lockdclient.APIError)(nil); !errors.As(err, &apiErr) || (apiErr.Response.ErrorCode != "etag_mismatch" && apiErr.Response.ErrorCode != "version_conflict") {
		t.Fatalf("unexpected error from stale update: %v", err)
	}

	if err := lease.Save(ctx, map[string]any{"payload": "fresh", "count": 2.0}); err != nil {
		t.Fatalf("save after remove: %v", err)
	}
	finalVersion := lease.Version
	releaseLease(t, ctx, cli, key, lease.LeaseID)

	verify := acquireWithRetry(t, ctx, cli, key, "keepalive-verify", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, cli, key, verify.LeaseID)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state["payload"] != "fresh" {
		t.Fatalf("expected fresh payload, got %+v", state)
	}
	if verify.Version != finalVersion {
		t.Fatalf("expected version %d, got %d", finalVersion, verify.Version)
	}
	releaseLease(t, ctx, cli, key, verify.LeaseID)
	cleanupMinio(t, cfg, key)
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
	return lockd.StartTestServer(tb, options...)
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
	data, etag, version, err := cli.GetBytes(ctx, key, leaseID)
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
	if err := store.Remove(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Logf("remove state failed: %v", err)
	}
	if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Logf("delete meta failed: %v", err)
	}
}
