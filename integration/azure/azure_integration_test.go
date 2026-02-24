//go:build integration && azure && !lq && !query && !crypto

package azureintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	azuretest "pkt.systems/lockd/integration/azuretest"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/hatest"
	"pkt.systems/lockd/integration/internal/locktest"
	shutdowntest "pkt.systems/lockd/integration/internal/shutdowntest"
	testlog "pkt.systems/lockd/integration/internal/testlog"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func TestAzureStoreVerification(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)
}

func TestAzureShutdownDrainingBlocksAcquire(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)
	ts := startAzureTestServer(t, cfg)
	t.Cleanup(func() { _ = ts.Stop(context.Background()) })
	cli := ts.Client
	if cli == nil {
		t.Fatalf("nil test server client")
	}
	ctx := context.Background()
	key := "azure-drain-" + uuidv7.NewString()
	t.Cleanup(func() { azuretest.CleanupKey(t, cfg, namespaces.Default, key) })
	t.Cleanup(func() { azuretest.CleanupKey(t, cfg, namespaces.Default, "azure-drain-wait") })
	lease := acquireWithRetry(t, ctx, cli, key, "holder", 45, lockdclient.BlockWaitForever)
	stopCh := make(chan error, 1)
	go func() {
		stopCh <- ts.Stop(context.Background(), lockd.WithDrainLeases(3*time.Second), lockd.WithShutdownTimeout(4*time.Second))
	}()
	payload, _ := json.Marshal(api.AcquireRequest{Key: "azure-drain-wait", Owner: "drain-tester", TTLSeconds: 5})
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
	case <-time.After(60 * time.Second):
		t.Fatalf("server stop timed out")
	}
	_ = lease.Release(ctx)
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
	opts := lockdclient.UpdateOptions{IfVersion: version, IfETag: etag}
	if opts.IfVersion == "" {
		opts.IfVersion = strconv.FormatInt(lease.Version, 10)
	}
	if _, err := ts.Client.UpdateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
		t.Fatalf("update state: %v", err)
	}

	state, _, _, err = getStateJSON(ctx, ts.Client, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if cursor, ok := state["cursor"].(float64); !ok || cursor != 42 {
		t.Fatalf("expected cursor 42, got %v", state["cursor"])
	}

	if !releaseLease(t, ctx, lease) {
		t.Fatalf("expected release success")
	}

	second := acquireWithRetry(t, ctx, ts.Client, key, "azure-lifecycle-worker-2", 30, lockdclient.BlockWaitForever)
	if second.LeaseID == lease.LeaseID {
		t.Fatal("expected distinct lease id")
	}
	if !releaseLease(t, ctx, second) {
		t.Fatalf("expected release success")
	}
}

func TestAzureAcquireNoWaitReturnsWaiting(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)
	ts := startAzureTestServer(t, cfg)

	ctx := context.Background()
	key := "azure-nowait-" + uuidv7.NewString()
	t.Cleanup(func() { azuretest.CleanupKey(t, cfg, namespaces.Default, key) })
	lease := acquireWithRetry(t, ctx, ts.Client, key, "holder", 30, lockdclient.BlockWaitForever)
	t.Cleanup(func() { _ = lease.Release(ctx) })

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	start := time.Now()
	_, err := ts.Client.Acquire(reqCtx, api.AcquireRequest{
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

func TestAzureAcquireIfNotExistsReturnsAlreadyExists(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)
	ts := startAzureTestServer(t, cfg)

	locktest.RunAcquireIfNotExistsReturnsAlreadyExists(t, locktest.AcquireIfNotExistsConfig{
		Client:    ts.Client,
		KeyPrefix: "azure-if-not-exists",
		CleanupKey: func(key string) {
			cleanupAzure(t, cfg, key)
		},
	})
}

func TestAzureAttachmentsLifecycle(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)
	ts := startAzureTestServer(t, cfg)

	ctx := context.Background()
	key := "azure-attach-" + uuidv7.NewString()
	defer cleanupAzure(t, cfg, key)

	runAttachmentTest(t, ctx, ts.Client, key)
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
					_ = releaseLease(t, ctx, lease)
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
				_, err = ts.Client.UpdateBytes(ctx, key, lease.LeaseID, body, lockdclient.UpdateOptions{IfETag: etag, IfVersion: version})
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

	verify := acquireWithRetry(t, ctx, ts.Client, key, "azure-verifier", ttl, lockdclient.BlockWaitForever)
	finalState, _, _, err := getStateJSON(ctx, ts.Client, key, verify.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, verify) {
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

func TestAzureTxnCommitAcrossNodes(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	cliRM := startAzureTestServer(t, cfg) // acts as RM holding the lease
	cliTC := startAzureTestServer(t, cfg) // acts as TC issuing the decision

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "azure-txn-cross-commit-" + uuidv7.NewString()
	defer cleanupAzure(t, cfg, key)

	lease := acquireWithRetry(t, ctx, cliRM.Client, key, "rm-node", 45, lockdclient.BlockWaitForever)
	if err := lease.Save(ctx, map[string]any{"value": "from-rm"}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	resp, err := cliTC.Client.Release(ctx, api.ReleaseRequest{
		Key:     key,
		LeaseID: lease.LeaseID,
		TxnID:   lease.TxnID,
	})
	if err != nil {
		t.Fatalf("cross-node release (commit): %v", err)
	}
	if !resp.Released {
		t.Fatalf("expected release=true")
	}
	// Drop the lease tracker on the originating server to avoid lingering in-memory trackers.
	_, _ = cliRM.Client.Release(ctx, api.ReleaseRequest{
		Key:     key,
		LeaseID: lease.LeaseID,
		TxnID:   lease.TxnID,
	})

	verifier := acquireWithRetry(t, ctx, cliRM.Client, key, "verifier", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, cliRM.Client, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, verifier) {
		t.Fatalf("release verifier failed")
	}
	if state == nil || state["value"] != "from-rm" {
		t.Fatalf("expected committed state from RM, got %+v", state)
	}
}

func TestAzureTxnRollbackAcrossNodes(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	cliRM := startAzureTestServer(t, cfg)
	cliTC := startAzureTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "azure-txn-cross-rollback-" + uuidv7.NewString()
	defer cleanupAzure(t, cfg, key)

	lease := acquireWithRetry(t, ctx, cliRM.Client, key, "rm-node", 45, lockdclient.BlockWaitForever)
	if err := lease.Save(ctx, map[string]any{"value": "should-rollback"}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	resp, err := cliTC.Client.Release(ctx, api.ReleaseRequest{
		Key:      key,
		LeaseID:  lease.LeaseID,
		TxnID:    lease.TxnID,
		Rollback: true,
	})
	if err != nil {
		t.Fatalf("cross-node release (rollback): %v", err)
	}
	if !resp.Released {
		t.Fatalf("expected release=true")
	}
	// Clear the lease tracker on the original server.
	_, _ = cliRM.Client.Release(ctx, api.ReleaseRequest{
		Key:      key,
		LeaseID:  lease.LeaseID,
		TxnID:    lease.TxnID,
		Rollback: true,
	})

	verifier := acquireWithRetry(t, ctx, cliRM.Client, key, "verifier", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, cliRM.Client, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, verifier) {
		t.Fatalf("release verifier failed")
	}
	if state != nil {
		t.Fatalf("expected rollback to discard state, got %+v", state)
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
	if !releaseLease(t, ctx, verifyLease) {
		t.Fatalf("verify release failed")
	}
	if owner, ok := state["owner"].(string); !ok || owner != "azure-auto" {
		t.Fatalf("unexpected owner in generated key state: %+v", state)
	}

}

func TestAzureRemoveSingleServer(t *testing.T) {
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
		releaseLease(t, ctx, lease)

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
		releaseLease(t, ctx, verify)
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
		releaseLease(t, ctx, lease)
	})

	t.Run("remove-after-reacquire", func(t *testing.T) {
		key := "azure-remove-reacquire-" + uuidv7.NewString()
		t.Cleanup(func() { cleanupAzure(t, cfg, key) })

		writer := acquireWithRetry(t, ctx, ts.Client, key, "writer", 30, lockdclient.BlockWaitForever)
		if err := writer.Save(ctx, map[string]any{"step": "written"}); err != nil {
			t.Fatalf("writer save: %v", err)
		}
		releaseLease(t, ctx, writer)

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
		releaseLease(t, ctx, remover)

		verify := acquireWithRetry(t, ctx, ts.Client, key, "remover-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, ts.Client, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state after remove, got %+v", state)
		}
		releaseLease(t, ctx, verify)
	})
}

func TestAzureRemoveAcquireForUpdate(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)))
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
		releaseLease(t, ctx, lease)
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
		releaseLease(t, ctx, verify)
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
		releaseLease(t, ctx, verify)
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
	if !releaseLease(t, verifyCtx, verifyLease) {
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

	var sharedCreds lockd.TestMTLSCredentials
	if cryptotest.TestMTLSEnabled() {
		sharedCreds = cryptotest.SharedMTLSCredentials(t)
	}
	primary := startAzureTestServer(t, cfg, cryptotest.SharedMTLSOptions(t)...)
	backup := startAzureTestServer(t, cfg, cryptotest.SharedMTLSOptions(t)...)

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

	activeServer, seedClient, err := hatest.FindActiveServer(ctx, primary, backup)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	t.Cleanup(func() { _ = seedClient.Close() })
	standbyServer := backup
	if activeServer == backup {
		standbyServer = primary
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
	if err := seedLease.Save(ctx, map[string]any{"payload": "azure-failover", "count": 2}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(ctx); err != nil {
		t.Fatalf("seed release: %v", err)
	}

	if err := activeServer.Stop(context.Background()); err != nil {
		t.Fatalf("stop active server: %v", err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := hatest.WaitForActive(waitCtx, standbyServer); err != nil {
		waitCancel()
		t.Fatalf("standby activation: %v", err)
	}
	waitCancel()

	clientLogger, clientLogs := testlog.NewRecorder(t, pslog.TraceLevel)
	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(90 * time.Second),
		lockdclient.WithCloseTimeout(90 * time.Second),
		lockdclient.WithKeepAliveTimeout(90 * time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
	}
	if cryptotest.TestMTLSEnabled() {
		httpClient := cryptotest.RequireMTLSHTTPClient(t, sharedCreds)
		clientOpts = append(clientOpts, lockdclient.WithHTTPClient(httpClient))
	}
	failoverClient, err := lockdclient.NewWithEndpoints(
		[]string{activeServer.URL(), standbyServer.URL()},
		clientOpts...,
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

	verifyClient, err := standbyServer.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)))
	if err != nil {
		t.Fatalf("verify client: %v", err)
	}
	t.Cleanup(func() { _ = verifyClient.Close() })
	verifier := acquireWithRetry(t, ctx, verifyClient, key, "verifier", 45, lockdclient.BlockWaitForever)
	finalState, _, _, err := getStateJSON(ctx, verifyClient, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("verify get_state: %v", err)
	}
	if !releaseLease(t, ctx, verifier) {
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
	primaryURL := activeServer.URL()
	backupURL := standbyServer.URL()
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

	t.Logf("azure remove-state failover log summary: initial=%s success=%s\n%s", initialEndpoint, successEndpoint, rec.Summary())
}

func TestAzureRemoveFailover(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	primary := startAzureTestServer(t, cfg)
	backup := startAzureTestServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	key := "azure-remove-failover-" + uuidv7.NewString()
	t.Cleanup(func() { cleanupAzure(t, cfg, key) })

	activeServer, seedClient, err := hatest.FindActiveServer(ctx, primary, backup)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	t.Cleanup(func() { _ = seedClient.Close() })
	standby := backup
	if activeServer == backup {
		standby = primary
	}

	seedLease := acquireWithRetry(t, ctx, seedClient, key, "seed", 45, lockdclient.BlockWaitForever)
	if err := seedLease.Save(ctx, map[string]any{"payload": "seed", "count": 1.0}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	releaseLease(t, ctx, seedLease)

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
		[]string{activeServer.URL(), standby.URL()},
		clientOptions...,
	)
	if err != nil {
		t.Fatalf("failover client: %v", err)
	}
	defer failoverClient.Close()

	lease := acquireWithRetry(t, ctx, failoverClient, key, "failover-remover", 45, lockdclient.BlockWaitForever)

	if err := activeServer.Stop(context.Background()); err != nil {
		t.Fatalf("stop active server: %v", err)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := hatest.WaitForActive(waitCtx, standby); err != nil {
		waitCancel()
		t.Fatalf("standby activation: %v", err)
	}
	waitCancel()

	res, err := lease.Remove(ctx)
	if err != nil {
		t.Fatalf("remove during failover: %v\n%s", err, clientLogs.Summary())
	}
	if !res.Removed {
		t.Fatalf("expected removal success, got %+v", res)
	}
	releaseLease(t, ctx, lease)

	verifyClient, err := standby.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)))
	if err != nil {
		t.Fatalf("verify client: %v", err)
	}
	t.Cleanup(func() { _ = verifyClient.Close() })
	verify := acquireWithRetry(t, ctx, verifyClient, key, "failover-verify", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verifyClient, key, verify.LeaseID)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state after failover remove, got %+v", state)
	}
	releaseLease(t, ctx, verify)

	assertAzureRemoveFailoverLogs(t, clientLogs, activeServer.URL(), standby.URL())
}

func TestAzureRemoveCASMismatch(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)))
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

	staleOpts := lockdclient.RemoveOptions{
		IfETag:    staleETag,
		IfVersion: lockdclient.Int64(currentVersion),
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
	releaseLease(t, ctx, lease)

	verify := acquireWithRetry(t, ctx, cli, key, "cas-verify", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, cli, key, verify.LeaseID)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state after remove, got %+v", state)
	}
	releaseLease(t, ctx, verify)
}

func TestAzureRemoveKeepAlive(t *testing.T) {
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	ts := startAzureTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient(lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)))
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

	staleUpdate := lockdclient.UpdateOptions{
		IfETag:    originalETag,
		IfVersion: lockdclient.Int64(originalVersion),
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
	releaseLease(t, ctx, lease)

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
	releaseLease(t, ctx, verify)
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
	opts := []lockdclient.GetOption{}
	if leaseID != "" {
		opts = append(opts, lockdclient.WithGetLeaseID(leaseID))
	}
	resp, err := cli.Get(ctx, key, opts...)
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

func releaseLease(t *testing.T, ctx context.Context, lease *lockdclient.LeaseSession) bool {
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	return true
}
