//go:build integration && azure && !lq

package azureintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	testlog "pkt.systems/lockd/integration/internal/testlog"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	azurestore "pkt.systems/lockd/internal/storage/azure"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/logport"
)

func TestAzureStoreVerification(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)
}

func TestAzureLockLifecycle(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)

	ctx := context.Background()
	key := "azure-lifecycle-" + uuidv7.NewString()
	defer cleanupAzure(t, cfg, key)

	lease := acquireWithRetry(t, ctx, ts.Client, key, "azure-lifecycle-worker", 30, lockdclient.BlockWaitForever)
	state, etag, version, err := getStateJSON(ctx, ts.Client, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if state != nil {
		t.Fatal("expected empty initial state")
	}

	payload, _ := json.Marshal(map[string]any{"cursor": 42, "source": "azure"})
	opts := lockdclient.UpdateStateOptions{IfVersion: version, IfETag: etag}
	if opts.IfVersion == "" {
		opts.IfVersion = strconv.FormatInt(lease.Version, 10)
	}
	if _, err := ts.Client.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
		t.Fatalf("update state: %v", err)
	}

	state, _, _, err = getStateJSON(ctx, ts.Client, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if cursor, ok := state["cursor"].(float64); !ok || cursor != 42 {
		t.Fatalf("expected cursor 42, got %v", state["cursor"])
	}

	if !releaseLease(t, ctx, ts.Client, key, lease.LeaseID) {
		t.Fatalf("expected release success")
	}

	second := acquireWithRetry(t, ctx, ts.Client, key, "azure-lifecycle-worker-2", 30, lockdclient.BlockWaitForever)
	if second.LeaseID == lease.LeaseID {
		t.Fatal("expected distinct lease id")
	}
	if !releaseLease(t, ctx, ts.Client, key, second.LeaseID) {
		t.Fatalf("expected release success")
	}
}

func TestAzureLockConcurrency(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	key := "azure-concurrency-" + uuidv7.NewString()
	defer cleanupAzure(t, cfg, key)

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
				lease := acquireWithRetry(t, ctx, ts.Client, key, owner, ttl, 10)
				state, etag, version, err := getStateJSON(ctx, ts.Client, key, lease.LeaseID)
				if err != nil {
					_ = releaseLease(t, ctx, ts.Client, key, lease.LeaseID)
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
				_, err = ts.Client.UpdateStateBytes(ctx, key, lease.LeaseID, body, lockdclient.UpdateStateOptions{IfETag: etag, IfVersion: version})
				if err != nil {
					var apiErr *lockdclient.APIError
					if errors.As(err, &apiErr) {
						code := apiErr.Response.ErrorCode
						if code == "meta_conflict" || code == "version_conflict" || code == "etag_mismatch" {
							_ = releaseLease(t, ctx, ts.Client, key, lease.LeaseID)
							continue
						}
					}
					t.Fatalf("update state: %v", err)
				}
				updates.Add(1)
				_ = releaseLease(t, ctx, ts.Client, key, lease.LeaseID)
				iter++
			}
		}(id)
	}
	wg.Wait()

	verify := acquireWithRetry(t, ctx, ts.Client, key, "azure-verifier", ttl, lockdclient.BlockWaitForever)
	finalState, _, _, err := getStateJSON(ctx, ts.Client, key, verify.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, ts.Client, key, verify.LeaseID) {
		t.Fatalf("expected release success")
	}

	expected := float64(workers * iterations)
	if finalState == nil {
		t.Fatalf("expected final state, got nil")
	}
	if value, ok := finalState["counter"].(float64); !ok || value != expected {
		t.Fatalf("expected counter %.0f, got %v", expected, finalState["counter"])
	}
	if updates.Load() != int64(expected) {
		t.Fatalf("expected %d updates, observed %d", int64(expected), updates.Load())
	}
}

func TestAzureAutoKeyAcquire(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Owner:      "azure-auto",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.Key == "" {
		t.Fatalf("expected generated key, got empty string")
	}
	key := lease.Key
	defer cleanupAzure(t, cfg, key)

	if err := lease.Save(ctx, map[string]any{"owner": "azure-auto"}); err != nil {
		lease.Release(ctx)
		t.Fatalf("save: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	verifyLease := acquireWithRetry(t, ctx, ts.Client, key, "azure-auto-verify", 30, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, ts.Client, key, verifyLease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, ts.Client, key, verifyLease.LeaseID) {
		t.Fatalf("verify release failed")
	}
	if owner, ok := state["owner"].(string); !ok || owner != "azure-auto" {
		t.Fatalf("unexpected owner in generated key state: %+v", state)
	}

}

func TestAzureRemoveStateSingleServer(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	t.Run("remove-after-update", func(t *testing.T) {
		key := "azure-remove-update-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupAzure(t, cfg, key) })

		lease := acquireWithRetry(t, ctx, ts.Client, key, "remover-update", 30, lockdclient.BlockWaitForever)
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
		state, _, _, err := getStateJSON(ctx, ts.Client, key, lease.LeaseID)
		if err != nil {
			t.Fatalf("get after remove: %v", err)
		}
		if state != nil {
			t.Fatalf("expected nil state after remove, got %+v", state)
		}
		releaseLease(t, ctx, ts.Client, key, lease.LeaseID)

		verify := acquireWithRetry(t, ctx, ts.Client, key, "remover-update-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, ts.Client, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state post-remove, got %+v", state)
		}
		if verify.Version != res.NewVersion {
			t.Fatalf("expected version %d, got %d", res.NewVersion, verify.Version)
		}
		releaseLease(t, ctx, ts.Client, key, verify.LeaseID)
	})

	t.Run("remove-without-state", func(t *testing.T) {
		key := "azure-remove-empty-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupAzure(t, cfg, key) })

		lease := acquireWithRetry(t, ctx, ts.Client, key, "remover-empty", 30, lockdclient.BlockWaitForever)
		res, err := lease.Remove(ctx)
		if err != nil {
			t.Fatalf("remove empty: %v", err)
		}
		if res.Removed {
			t.Fatalf("expected removed=false, got %+v", res)
		}
		if res.NewVersion != 0 {
			t.Fatalf("expected version 0, got %d", res.NewVersion)
		}
		state, _, _, err := getStateJSON(ctx, ts.Client, key, lease.LeaseID)
		if err != nil {
			t.Fatalf("get after remove: %v", err)
		}
		if state != nil {
			t.Fatalf("expected nil state, got %+v", state)
		}
		releaseLease(t, ctx, ts.Client, key, lease.LeaseID)
	})

	t.Run("remove-after-reacquire", func(t *testing.T) {
		key := "azure-remove-reacquire-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupAzure(t, cfg, key) })

		writer := acquireWithRetry(t, ctx, ts.Client, key, "writer", 30, lockdclient.BlockWaitForever)
		if err := writer.Save(ctx, map[string]any{"step": "written"}); err != nil {
			t.Fatalf("writer save: %v", err)
		}
		releaseLease(t, ctx, ts.Client, key, writer.LeaseID)

		remover := acquireWithRetry(t, ctx, ts.Client, key, "remover", 30, lockdclient.BlockWaitForever)
		state, _, _, err := getStateJSON(ctx, ts.Client, key, remover.LeaseID)
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
		releaseLease(t, ctx, ts.Client, key, remover.LeaseID)

		verify := acquireWithRetry(t, ctx, ts.Client, key, "remover-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, ts.Client, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state after remove, got %+v", state)
		}
		releaseLease(t, ctx, ts.Client, key, verify.LeaseID)
	})
}

func TestAzureRemoveStateAcquireForUpdate(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)))
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	seed := func(key string, payload map[string]any) {
		lease := acquireWithRetry(t, ctx, cli, key, "seed", 30, lockdclient.BlockWaitForever)
		if err := lease.Save(ctx, payload); err != nil {
			t.Fatalf("seed save: %v", err)
		}
		releaseLease(t, ctx, cli, key, lease.LeaseID)
	}

	t.Run("remove-in-handler", func(t *testing.T) {
		key := "azure-remove-afu-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupAzure(t, cfg, key) })
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
		key := "azure-remove-recreate-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupAzure(t, cfg, key) })
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

func TestAzureAcquireForUpdateCallbackSingleServer(t *testing.T) {
	watchdog := time.AfterFunc(20*time.Second, func() {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		panic(fmt.Sprintf("azure callback single-server hang\n%s", buf[:n]))
	})
	defer watchdog.Stop()

	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := "azure-single-" + uuidv7.NewString()
	defer cleanupAzure(t, cfg, key)

	seedCli := directClient(t, ts)
	t.Cleanup(func() { _ = seedCli.Close() })

	seedLease, err := seedCli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 15,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(ctx, map[string]any{"payload": "azure-single", "count": 1}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(ctx); err != nil {
		t.Fatalf("seed release: %v", err)
	}

	handlerCalled := false
	err = ts.Client.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "reader-single",
		TTLSeconds: 15,
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
		if snapshot["payload"] != "azure-single" {
			return fmt.Errorf("unexpected payload: %+v", snapshot)
		}
		var count float64
		if v, ok := snapshot["count"].(float64); ok {
			count = v
		}
		updated := map[string]any{
			"payload": "azure-single",
			"count":   count + 1,
			"owner":   "reader-single",
		}
		return af.Save(handlerCtx, updated)
	})
	watchdog.Stop()
	if err != nil {
		t.Fatalf("acquire-for-update callback: %v", err)
	}
	if !handlerCalled {
		t.Fatalf("callback was not invoked")
	}

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer verifyCancel()
	verifyLease := acquireWithRetry(t, verifyCtx, seedCli, key, "verifier", 15, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(verifyCtx, seedCli, key, verifyLease.LeaseID)
	if err != nil {
		t.Fatalf("verify get_state: %v", err)
	}
	if !releaseLease(t, verifyCtx, seedCli, key, verifyLease.LeaseID) {
		t.Fatalf("verify release failed")
	}
	if owner, ok := state["owner"].(string); !ok || owner != "reader-single" {
		t.Fatalf("unexpected owner after callback: %+v", state)
	}
	if count, ok := state["count"].(float64); !ok || count != 2 {
		t.Fatalf("unexpected count after callback: %v", state["count"])
	}
}

func TestAzureAcquireForUpdateCallbackFailover(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	primary := startAzureTestServer(t, cfg)
	backup := startAzureTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	watchdog := time.AfterFunc(60*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("TestAzureAcquireForUpdateCallbackFailover timeout:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	key := "azure-forupdate-failover-" + uuidv7.NewString()
	defer cleanupAzure(t, cfg, key)

	seedClient := backup.Client
	if seedClient == nil {
		var err error
		seedClient, err = backup.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)))
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
	if err := seedLease.Save(ctx, map[string]any{"payload": "azure-failover", "count": 2}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(ctx); err != nil {
		t.Fatalf("seed release: %v", err)
	}

	if err := primary.Stop(context.Background()); err != nil {
		t.Fatalf("stop primary: %v", err)
	}

	clientLogger, clientLogs := testlog.NewRecorder(t, logport.TraceLevel)
	failoverClient, err := lockdclient.NewWithEndpoints(
		[]string{primary.URL(), backup.URL()},
		lockdclient.WithMTLS(false),
		lockdclient.WithHTTPTimeout(90*time.Second),
		lockdclient.WithCloseTimeout(90*time.Second),
		lockdclient.WithKeepAliveTimeout(90*time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
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
			"payload": "azure-failover",
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

	var sawError bool
	var sawSuccess bool
	primaryURL := primary.URL()
	backupURL := backup.URL()
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
	t.Logf("azure failover log summary: error_endpoint=%s success_endpoint=%s", primaryURL, backupURL)
}

func assertAzureRemoveFailoverLogs(t testing.TB, rec *testlog.Recorder, primary, backup string) {
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	const (
		startMsg        = "client.remove_state.start"
		successMsg      = "client.remove_state.success"
		transportErrMsg = "client.remove_state.transport_error"
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

	t.Logf("azure remove-state failover log summary: initial=%s success=%s\n%s", initialEndpoint, successEndpoint, rec.Summary())
}

func TestAzureRemoveStateFailover(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	primary := startAzureTestServer(t, cfg)
	backup := startAzureTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	key := "azure-remove-failover-" + uuidv7.NewString()
	t.Cleanup(func() { cleanupAzure(t, cfg, key) })

	seedClient := backup.Client
	if seedClient == nil {
		var err error
		seedClient, err = backup.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)))
		if err != nil {
			t.Fatalf("seed client: %v", err)
		}
	}
	defer seedClient.Close()

	seedLease := acquireWithRetry(t, ctx, seedClient, key, "seed", 45, lockdclient.BlockWaitForever)
	if err := seedLease.Save(ctx, map[string]any{"payload": "seed", "count": 1.0}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	releaseLease(t, ctx, seedClient, key, seedLease.LeaseID)

	clientLogger, clientLogs := testlog.NewRecorder(t, logport.TraceLevel)
	failoverClient, err := lockdclient.NewWithEndpoints(
		[]string{primary.URL(), backup.URL()},
		lockdclient.WithMTLS(false),
		lockdclient.WithHTTPTimeout(90*time.Second),
		lockdclient.WithCloseTimeout(90*time.Second),
		lockdclient.WithKeepAliveTimeout(90*time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
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

	assertAzureRemoveFailoverLogs(t, clientLogs, primary.URL(), backup.URL())
}

func TestAzureRemoveStateCASMismatch(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)))
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	key := "azure-remove-cas-" + uuidv7.NewString()
	defer cleanupAzure(t, cfg, key)

	lease := acquireWithRetry(t, ctx, cli, key, "cas-owner", 45, lockdclient.BlockWaitForever)
	if err := lease.Save(ctx, map[string]any{"payload": "v1"}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	staleETag := lease.StateETag
	if err := lease.Save(ctx, map[string]any{"payload": "v2"}); err != nil {
		t.Fatalf("second save: %v", err)
	}
	currentVersion := lease.Version

	staleOpts := lockdclient.RemoveStateOptions{
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

func TestAzureRemoveStateKeepAlive(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)))
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	key := "azure-remove-keepalive-" + uuidv7.NewString()
	defer cleanupAzure(t, cfg, key)

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

	staleUpdate := lockdclient.UpdateStateOptions{
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
}

func acquireWithRetry(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl int64, block int64) *lockdclient.LeaseSession {
	for attempt := 0; attempt < 20; attempt++ {
		lease, err := cli.Acquire(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      owner,
			TTLSeconds: ttl,
			BlockSecs:  block,
		})
		if err == nil {
			return lease
		}
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) && apiErr.Status == 409 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		t.Fatalf("acquire: %v", err)
	}
	t.Fatalf("acquire retry limit exceeded for key %q", key)
	return nil
}

func getStateJSON(ctx context.Context, cli *lockdclient.Client, key, leaseID string) (map[string]any, string, string, error) {
	reader, etag, version, err := cli.GetState(ctx, key, leaseID)
	if err != nil {
		return nil, "", "", err
	}
	if reader == nil {
		return nil, etag, version, nil
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
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
	resp, err := cli.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: leaseID})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	return resp.Released
}

func loadAzureConfig(t *testing.T) lockd.Config {
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		t.Skip("LOCKD_STORE must reference an azure:// URI for Azure integration tests")
	}
	if !strings.HasPrefix(store, "azure://") {
		t.Fatalf("LOCKD_STORE must reference an azure:// URI, got %q", store)
	}
	cfg := lockd.Config{
		Store:         store,
		AzureEndpoint: os.Getenv("LOCKD_AZURE_ENDPOINT"),
		AzureSASToken: os.Getenv("LOCKD_AZURE_SAS_TOKEN"),
	}
	cfg.AzureAccountKey = os.Getenv("LOCKD_AZURE_ACCOUNT_KEY")
	return cfg
}

func ensureAzureStoreReady(t *testing.T, ctx context.Context, cfg lockd.Config) {
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		t.Fatalf("store verification failed: %+v", res)
	}
}

func startAzureTestServer(t testing.TB, cfg lockd.Config, opts ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
	cfgCopy := cfg
	cfgCopy.MTLS = false
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
		lockd.WithTestLoggerFromTB(t, logport.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithForUpdateTimeout(2*time.Minute),
			lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)),
		),
	}
	options = append(options, opts...)
	return lockd.StartTestServer(t, options...)
}

func directClient(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
	t.Helper()
	addr := ts.Server.ListenerAddr()
	if addr == nil {
		t.Fatalf("listener not initialised")
	}
	base := "http://" + addr.String()
	cli, err := lockdclient.New(base,
		lockdclient.WithMTLS(false),
		lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)),
		lockdclient.WithHTTPTimeout(2*time.Minute),
	)
	if err != nil {
		t.Fatalf("direct client: %v", err)
	}
	return cli
}

func cleanupAzure(t *testing.T, cfg lockd.Config, key string) {
	azureCfg, err := lockd.BuildAzureConfig(cfg)
	if err != nil {
		t.Fatalf("build azure config: %v", err)
	}
	store, err := azurestore.New(azureCfg)
	if err != nil {
		t.Fatalf("new azure store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Logf("remove state failed: %v", err)
	}
	if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Logf("delete meta failed: %v", err)
	}
}
