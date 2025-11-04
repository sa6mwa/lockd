//go:build integration && aws && !lq

package awsintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	shutdowntest "pkt.systems/lockd/integration/internal/shutdowntest"
	testlog "pkt.systems/lockd/integration/internal/testlog"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

var (
	awsStoreVerifyOnce sync.Once
	awsStoreVerifyErr  error
)

func TestAWSStoreVerification(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
}

func TestAWSShutdownDrainingBlocksAcquire(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	ts := startAWSTestServer(t, cfg)
	t.Cleanup(func() { _ = ts.Stop(context.Background()) })
	cli := ts.Client
	if cli == nil {
		t.Fatalf("nil test server client")
	}
	ctx := context.Background()
	key := "aws-drain-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "holder", 45, lockdclient.BlockWaitForever)
	stopCh := make(chan error, 1)
	go func() {
		stopCh <- ts.Stop(context.Background(), lockd.WithDrainLeases(3*time.Second), lockd.WithShutdownTimeout(4*time.Second))
	}()
	payload, _ := json.Marshal(api.AcquireRequest{Key: "aws-drain-wait", Owner: "drain-tester", TTLSeconds: 5})
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
	case <-time.After(15 * time.Second):
		t.Fatalf("server stop timed out")
	}
	_ = lease.Release(ctx)
}

func TestAWSLockLifecycle(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "aws-lifecycle-" + uuidv7.NewString()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{Key: key, Owner: "aws-lifecycle", TTLSeconds: 30, BlockSecs: 5})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	state, etag, version, err := getStateJSON(ctx, cli, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if state != nil {
		t.Fatal("expected no initial state")
	}

	payload, _ := json.Marshal(map[string]any{"cursor": 42, "source": "aws"})
	opts := lockdclient.UpdateOptions{IfVersion: version}
	if opts.IfVersion == "" {
		opts.IfVersion = strconv.FormatInt(lease.Version, 10)
	}
	if etag != "" {
		opts.IfETag = etag
	}
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
		t.Fatalf("update state: %v", err)
	}

	state, _, _, err = getStateJSON(ctx, cli, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if cursor, ok := state["cursor"].(float64); !ok || cursor != 42 {
		t.Fatalf("expected cursor 42, got %v", state["cursor"])
	}

	if !releaseLease(t, ctx, cli, key, lease.LeaseID) {
		t.Fatalf("expected release success")
	}

	secondLease, err := cli.Acquire(ctx, api.AcquireRequest{Key: key, Owner: "aws-lifecycle-2", TTLSeconds: 30, BlockSecs: 5})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if secondLease.LeaseID == lease.LeaseID {
		t.Fatal("expected new lease id")
	}
	if !releaseLease(t, ctx, cli, key, secondLease.LeaseID) {
		t.Fatalf("expected release success")
	}

	cleanupS3(t, cfg, key)
}

func TestAWSLockConcurrency(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "aws-concurrency-" + uuidv7.NewString()
	workers := 5
	iterations := 3
	ttl := int64(45)

	var wg sync.WaitGroup
	wg.Add(workers)
	for id := 0; id < workers; id++ {
		go func(workerID int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", workerID)
			for iter := 0; iter < iterations; {
				lease, err := cli.Acquire(ctx, api.AcquireRequest{Key: key, Owner: owner, TTLSeconds: ttl, BlockSecs: 20})
				if err != nil {
					t.Fatalf("worker %d acquire: %v", workerID, err)
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

	verifier, err := cli.Acquire(ctx, api.AcquireRequest{Key: key, Owner: "verifier", TTLSeconds: ttl, BlockSecs: 5})
	if err != nil {
		t.Fatalf("verifier acquire: %v", err)
	}
	finalState, _, _, err := getStateJSON(ctx, cli, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, cli, key, verifier.LeaseID) {
		t.Fatalf("expected release success")
	}
	cleanupS3(t, cfg, key)

	if finalState == nil {
		t.Fatal("expected final state")
	}
	expected := float64(workers * iterations)
	if value, ok := finalState["counter"].(float64); !ok || value != expected {
		t.Fatalf("expected counter %.0f, got %v", expected, finalState["counter"])
	}
}

func TestAWSAutoKeyAcquire(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "",
		Owner:      "aws-auto",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.Key == "" {
		t.Fatalf("expected generated key")
	}
	key := lease.Key
	if err := lease.Save(ctx, map[string]any{"owner": "aws-auto"}); err != nil {
		lease.Release(ctx)
		t.Fatalf("save: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	verifyLease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "aws-auto-verify",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("verify acquire: %v", err)
	}
	state, _, _, err := getStateJSON(ctx, cli, key, verifyLease.LeaseID)
	if err != nil {
		t.Fatalf("verify get_state: %v", err)
	}
	if !releaseLease(t, ctx, cli, key, verifyLease.LeaseID) {
		t.Fatalf("verify release failed")
	}
	if owner, ok := state["owner"].(string); !ok || owner != "aws-auto" {
		t.Fatalf("unexpected owner in generated key state: %+v", state)
	}
	cleanupS3(t, cfg, key)
}

func TestAWSRemoveSingleServer(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	t.Run("remove-after-update", func(t *testing.T) {
		key := "aws-remove-update-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupS3(t, cfg, key) })
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
			t.Fatalf("expected version bump (was %d, got %d)", prevVersion, res.NewVersion)
		}
		state, _, _, err := getStateJSON(ctx, cli, key, lease.LeaseID)
		if err != nil {
			t.Fatalf("get after remove: %v", err)
		}
		if state != nil {
			t.Fatalf("expected nil state after remove, got %+v", state)
		}
		if !releaseLease(t, ctx, cli, key, lease.LeaseID) {
			t.Fatalf("release failed")
		}

		verify := acquireWithRetry(t, ctx, cli, key, "remover-update-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, cli, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state post-remove, got %+v", state)
		}
		if verify.Version != res.NewVersion {
			t.Fatalf("expected version %d, got %d", res.NewVersion, verify.Version)
		}
		releaseLease(t, ctx, cli, key, verify.LeaseID)
	})

	t.Run("remove-without-state", func(t *testing.T) {
		key := "aws-remove-empty-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupS3(t, cfg, key) })
		lease := acquireWithRetry(t, ctx, cli, key, "remover-empty", 30, lockdclient.BlockWaitForever)
		res, err := lease.Remove(ctx)
		if err != nil {
			t.Fatalf("remove empty: %v", err)
		}
		if res.Removed && res.NewVersion == 0 {
			t.Fatalf("inconsistent remove result %+v", res)
		}
		state, _, _, err := getStateJSON(ctx, cli, key, lease.LeaseID)
		if err != nil {
			t.Fatalf("get after remove: %v", err)
		}
		if state != nil {
			t.Fatalf("expected nil state, got %+v", state)
		}
		releaseLease(t, ctx, cli, key, lease.LeaseID)
	})

	t.Run("remove-after-reacquire", func(t *testing.T) {
		key := "aws-remove-reacquire-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupS3(t, cfg, key) })
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
	})
}

func TestAWSRemoveAcquireForUpdate(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)

	ts := startAWSTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	seed := func(tb testing.TB, key string, payload map[string]any) {
		t.Helper()
		lease := acquireWithRetry(t, ctx, cli, key, "seed", 30, lockdclient.BlockWaitForever)
		if err := lease.Save(ctx, payload); err != nil {
			t.Fatalf("seed save: %v", err)
		}
		releaseLease(t, ctx, cli, key, lease.LeaseID)
	}

	t.Run("remove-in-handler", func(t *testing.T) {
		key := "aws-remove-afu-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupS3(t, cfg, key) })
		seed(t, key, map[string]any{"payload": "seed-value", "count": 7.0})

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
			if res == nil || !res.Removed {
				return fmt.Errorf("expected removal result, got %+v", res)
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
		key := "aws-remove-recreate-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupS3(t, cfg, key) })
		seed(t, key, map[string]any{"payload": "initial", "count": 1.0})

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

func TestAWSAcquireForUpdateCallbackSingleServer(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)

	ts := startAWSTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	key := "aws-forupdate-single-" + uuidv7.NewString()
	defer cleanupS3(t, cfg, key)

	seedLease, err := cli.Acquire(ctx, api.AcquireRequest{
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
	err = cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "reader-single",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		handlerCalled = true
		if af.State == nil || !af.State.HasState {
			return fmt.Errorf("expected snapshot state for %q", key)
		}
		var snapshot map[string]any
		if err := af.State.Decode(&snapshot); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}
		payload, ok := snapshot["payload"].(string)
		if !ok || payload != "single-server" {
			return fmt.Errorf("unexpected payload: %+v", snapshot)
		}
		var count float64
		if v, ok := snapshot["count"].(float64); ok {
			count = v
		}
		updated := map[string]any{
			"payload": payload,
			"count":   count + 1,
			"owner":   "reader-single",
		}
		return af.Save(handlerCtx, updated)
	})
	if err != nil {
		t.Fatalf("acquire-for-update callback: %v", err)
	}
	if !handlerCalled {
		t.Fatalf("callback was not invoked")
	}

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer verifyCancel()
	verifier := acquireWithRetry(t, verifyCtx, cli, key, "verifier", 30, lockdclient.BlockWaitForever)
	finalState, _, _, err := getStateJSON(verifyCtx, cli, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("verify get_state: %v", err)
	}
	if !releaseLease(t, verifyCtx, cli, key, verifier.LeaseID) {
		t.Fatalf("expected release success")
	}
	if finalState == nil {
		t.Fatalf("expected final state after callback")
	}
	if payload, ok := finalState["payload"].(string); !ok || payload != "single-server" {
		t.Fatalf("unexpected payload post-callback: %+v", finalState)
	}
	if owner, ok := finalState["owner"].(string); !ok || owner != "reader-single" {
		t.Fatalf("unexpected owner post-callback: %+v", finalState)
	}
	if count, ok := finalState["count"].(float64); !ok || count != 2 {
		t.Fatalf("unexpected count post-callback: %v", finalState["count"])
	}
}

func TestAWSAcquireForUpdateCallbackFailover(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)

	primary := startAWSTestServer(t, cfg)
	backup := startAWSTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	watchdog := time.AfterFunc(60*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("TestAWSAcquireForUpdateCallbackFailover timeout:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	key := "aws-forupdate-failover-" + uuidv7.NewString()
	t.Cleanup(func() { cleanupS3(t, cfg, key) })

	seedClient := backup.Client
	if seedClient == nil {
		var err error
		seedClient, err = backup.NewClient()
		if err != nil {
			t.Fatalf("seed client: %v", err)
		}
	}

	seedLease, err := seedClient.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 45,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(ctx, map[string]any{
		"payload": "aws-failover",
		"count":   2,
	}); err != nil {
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
	if cryptotest.TestMTLSEnabled() {
		httpClient := cryptotest.RequireMTLSHTTPClient(t, cryptotest.SharedMTLSCredentials(t))
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
			"payload": "aws-failover",
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

	verifyClient := backup.Client
	if verifyClient == nil {
		var cerr error
		verifyClient, cerr = backup.NewClient()
		if cerr != nil {
			t.Fatalf("verify client: %v", cerr)
		}
	}
	verifier := acquireWithRetry(t, ctx, verifyClient, key, "verifier", 45, lockdclient.BlockWaitForever)
	finalState, _, _, err := getStateJSON(ctx, verifyClient, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("verify get_state: %v", err)
	}
	if !releaseLease(t, ctx, verifyClient, key, verifier.LeaseID) {
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

	var sawError bool
	var sawSuccess bool
	var errorEndpoints []string
	var successEndpoints []string
	for _, entry := range clientLogs.Events() {
		switch entry.Message {
		case "client.http.error":
			if strings.Contains(testlog.GetStringField(entry, "error"), "connection refused") {
				sawError = true
				errorEndpoints = append(errorEndpoints, testlog.GetStringField(entry, "endpoint"))
			}
		case "client.http.success":
			sawSuccess = true
			successEndpoints = append(successEndpoints, testlog.GetStringField(entry, "endpoint"))
		}
	}
	if !sawError {
		t.Fatalf("expected connection error before failover; logs:\n%s", clientLogs.Summary())
	}
	if !sawSuccess {
		t.Fatalf("expected HTTP success after failover; logs:\n%s", clientLogs.Summary())
	}
	primaryURL := primary.URL()
	backupURL := backup.URL()
	hasPrimaryError := false
	for _, ep := range errorEndpoints {
		if ep == primaryURL {
			hasPrimaryError = true
		}
	}
	if !hasPrimaryError {
		t.Fatalf("expected error endpoint %s; got %v\nlogs:\n%s", primaryURL, errorEndpoints, clientLogs.Summary())
	}
	hasBackupSuccess := false
	for _, ep := range successEndpoints {
		if ep == backupURL {
			hasBackupSuccess = true
		}
		if ep == primaryURL {
			t.Fatalf("unexpected success on primary endpoint %s\nlogs:\n%s", primaryURL, clientLogs.Summary())
		}
	}
	if !hasBackupSuccess {
		t.Fatalf("expected success endpoint %s; got %v\nlogs:\n%s", backupURL, successEndpoints, clientLogs.Summary())
	}
	t.Logf("aws failover log summary: error_endpoints=%v success_endpoints=%v", errorEndpoints, successEndpoints)
}

func assertAWSRemoveFailoverLogs(t testing.TB, rec *testlog.Recorder, primary, backup string) {
	const (
		startMsg        = "client.remove.start"
		successMsg      = "client.remove.success"
		transportErrMsg = "client.remove.transport_error"
		httpErrorMsg    = "client.http.error"
		httpSuccessMsg  = "client.http.success"
	)

	if rec == nil {
		t.Fatalf("nil recorder")
	}
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

	successEntry, ok := rec.FirstAfter(startEntry.Timestamp, func(e testlog.Entry) bool {
		return e.Message == successMsg
	})
	if !ok {
		t.Fatalf("expected %s entry; logs:\n%s", successMsg, rec.Summary())
	}
	successEndpoint := testlog.GetStringField(successEntry, "endpoint")
	if successEndpoint == "" {
		t.Fatalf("success entry missing endpoint; logs:\n%s", rec.Summary())
	}
	if successEndpoint == initialEndpoint {
		t.Fatalf("expected success on alternate endpoint, got %s; logs:\n%s", successEndpoint, rec.Summary())
	}
	if successEndpoint != backup {
		t.Fatalf("expected success on backup %s, got %s; logs:\n%s", backup, successEndpoint, rec.Summary())
	}

	if errorEntry != nil {
		errorEndpoint := testlog.GetStringField(*errorEntry, "endpoint")
		if errorEndpoint == "" {
			t.Fatalf("error entry missing endpoint; logs:\n%s", rec.Summary())
		}
		if errorEndpoint != primary {
			t.Fatalf("expected error on primary %s, got %s; logs:\n%s", primary, errorEndpoint, rec.Summary())
		}
	}

	httpSuccess, ok := rec.First(func(e testlog.Entry) bool {
		if e.Message != httpSuccessMsg {
			return false
		}
		return testlog.GetStringField(e, "endpoint") == successEndpoint
	})
	if !ok {
		t.Fatalf("expected %s for endpoint %s; logs:\n%s", httpSuccessMsg, successEndpoint, rec.Summary())
	}
	if status, ok := testlog.GetIntField(httpSuccess, "status"); !ok || status != http.StatusOK {
		t.Fatalf("unexpected status %d for endpoint %s; logs:\n%s", status, successEndpoint, rec.Summary())
	}

	t.Logf("aws remove-state failover log summary: start=%s success=%s error_logged=%t\n%s",
		initialEndpoint, successEndpoint, errorEntry != nil, rec.Summary())
}

func TestAWSRemoveFailover(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)

	primary := startAWSTestServer(t, cfg)
	backup := startAWSTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	key := "aws-remove-failover-" + uuidv7.NewString()
	t.Cleanup(func() { cleanupS3(t, cfg, key) })

	seedClient := backup.Client
	if seedClient == nil {
		var err error
		seedClient, err = backup.NewClient()
		if err != nil {
			t.Fatalf("seed client: %v", err)
		}
	}

	seedLease := acquireWithRetry(t, ctx, seedClient, key, "seed", 45, lockdclient.BlockWaitForever)
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
	if cryptotest.TestMTLSEnabled() {
		creds := cryptotest.SharedMTLSCredentials(t)
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

	verifyClient := backup.Client
	if verifyClient == nil {
		var cerr error
		verifyClient, cerr = backup.NewClient()
		if cerr != nil {
			t.Fatalf("verify client: %v", cerr)
		}
	}
	verifyLease := acquireWithRetry(t, ctx, verifyClient, key, "verify", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verifyClient, key, verifyLease.LeaseID)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state after failover remove, got %+v", state)
	}
	releaseLease(t, ctx, verifyClient, key, verifyLease.LeaseID)

	assertAWSRemoveFailoverLogs(t, clientLogs, primary.URL(), backup.URL())
}

func TestAWSRemoveCASMismatch(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := "aws-remove-cas-" + uuidv7.NewString()
	t.Cleanup(func() { cleanupS3(t, cfg, key) })

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
}

func TestAWSRemoveKeepAlive(t *testing.T) {
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := "aws-remove-keepalive-" + uuidv7.NewString()
	t.Cleanup(func() { cleanupS3(t, cfg, key) })

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
	} else if apiErr := (*lockdclient.APIError)(nil); !errors.As(err, &apiErr) || (apiErr.Response.ErrorCode != "version_conflict" && apiErr.Response.ErrorCode != "etag_mismatch") {
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
}

func loadAWSConfig(t *testing.T) lockd.Config {
	store := os.Getenv("LOCKD_STORE")
	if store == "" {
		t.Fatalf("LOCKD_STORE must be set to an aws:// URI for AWS integration tests")
	}
	if !strings.HasPrefix(store, "aws://") {
		t.Fatalf("LOCKD_STORE must reference an aws:// URI, got %q", store)
	}
	cfg := lockd.Config{
		Store:         store,
		AWSRegion:     os.Getenv("LOCKD_AWS_REGION"),
		AWSKMSKeyID:   os.Getenv("LOCKD_AWS_KMS_KEY_ID"),
		S3SSE:         os.Getenv("LOCKD_S3_SSE"),
		S3KMSKeyID:    os.Getenv("LOCKD_S3_KMS_KEY_ID"),
		S3MaxPartSize: 16 << 20,
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = os.Getenv("AWS_REGION")
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = os.Getenv("AWS_DEFAULT_REGION")
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation: %v", err)
	}
	return cfg
}

func ensureStoreReady(t *testing.T, ctx context.Context, cfg lockd.Config) {
	ResetAWSBucketForCrypto(t, cfg)
	awsStoreVerifyOnce.Do(func() {
		res, err := storagecheck.VerifyStore(ctx, cfg)
		if err != nil {
			awsStoreVerifyErr = err
			return
		}
		if !res.Passed() {
			awsStoreVerifyErr = fmt.Errorf("store verification failed: %+v", res)
		}
	})
	if awsStoreVerifyErr != nil {
		t.Fatalf("store verification failed: %v", awsStoreVerifyErr)
	}
}

func startLockdServer(t *testing.T, cfg lockd.Config) *lockdclient.Client {
	ts := startAWSTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}
	return cli
}

func startAWSTestServer(t testing.TB, cfg lockd.Config, opts ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
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
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
		),
		lockd.WithTestCloseDefaults(
			lockd.WithDrainLeases(-1),
			lockd.WithShutdownTimeout(10*time.Second),
		),
	}
	options = append(options, opts...)
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	return lockd.StartTestServer(t, options...)
}

func acquireWithRetry(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl, block int64) *lockdclient.LeaseSession {
	for attempt := 0; attempt < 60; attempt++ {
		resp, err := cli.Acquire(ctx, api.AcquireRequest{Key: key, Owner: owner, TTLSeconds: ttl, BlockSecs: block})
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

func cleanupS3(t *testing.T, cfg lockd.Config, key string) {
	s3cfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		t.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := store.Remove(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Logf("remove state failed: %v", err)
	}
	if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Logf("delete meta failed: %v", err)
	}
}
