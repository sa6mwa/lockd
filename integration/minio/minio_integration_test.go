//go:build integration && minio && !lq && !query && !crypto

package miniointegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	cryptotest "pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/hatest"
	"pkt.systems/lockd/integration/internal/locktest"
	shutdowntest "pkt.systems/lockd/integration/internal/shutdowntest"
	testlog "pkt.systems/lockd/integration/internal/testlog"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func TestMinioStoreVerification(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
}

func TestMinioShutdownDrainingBlocksAcquire(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	CleanupNamespaces(t, cfg, namespaces.Default)
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
	ifVersion := lockdclient.Int64(lease.Version)
	if version != "" {
		parsedVersion, parseErr := strconv.ParseInt(version, 10, 64)
		if parseErr != nil {
			t.Fatalf("parse version %q: %v", version, parseErr)
		}
		ifVersion = lockdclient.Int64(parsedVersion)
	}
	opts := lockdclient.UpdateOptions{IfVersion: ifVersion, IfETag: etag}
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

	if !releaseLease(t, ctx, lease) {
		t.Fatalf("expected release success")
	}

	secondLease := acquireWithRetry(t, ctx, cli, key, "minio-lifecycle-worker-2", 30, 5)
	if secondLease.LeaseID == lease.LeaseID {
		t.Fatal("expected a new lease id")
	}
	if !releaseLease(t, ctx, secondLease) {
		t.Fatalf("expected release success")
	}

	cleanupMinio(t, cfg, key)
}

func TestMinioAcquireNoWaitReturnsWaiting(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "minio-nowait-" + uuidv7.NewString()
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
	cleanupMinio(t, cfg, key)
}

func TestMinioAcquireIfNotExistsReturnsAlreadyExists(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	locktest.RunAcquireIfNotExistsReturnsAlreadyExists(t, locktest.AcquireIfNotExistsConfig{
		Client:    cli,
		KeyPrefix: "minio-if-not-exists",
		CleanupKey: func(key string) {
			cleanupMinio(t, cfg, key)
		},
	})
}

func TestMinioAttachmentsLifecycle(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "minio-attach-" + uuidv7.NewString()
	runAttachmentTest(t, ctx, cli, key)
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
					_ = releaseLease(t, ctx, lease)
					continue
				}
				ifVersion := lockdclient.Int64(lease.Version)
				if version != "" {
					parsedVersion, parseErr := strconv.ParseInt(version, 10, 64)
					if parseErr != nil {
						t.Fatalf("parse version %q: %v", version, parseErr)
					}
					ifVersion = lockdclient.Int64(parsedVersion)
				}
				var counter float64
				if state != nil {
					if v, ok := state["counter"]; ok {
						counter, _ = v.(float64)
					}
				}
				counter++
				body, _ := json.Marshal(map[string]any{"counter": counter, "last": owner})
				if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, body, lockdclient.UpdateOptions{IfETag: etag, IfVersion: ifVersion}); err != nil {
					t.Fatalf("update state: %v", err)
				}
				_ = releaseLease(t, ctx, lease)
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
	if !releaseLease(t, ctx, verifier) {
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

	watchdog := time.AfterFunc(25*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("TestMinioAcquireForUpdateCallbackSingleServer timeout after 25s:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	key := "minio-single-" + uuidv7.NewString()
	defer cleanupMinio(t, cfg, key)

	proxiedClient, err := ts.NewClient(
		lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
		lockdclient.WithHTTPTimeout(15*time.Second),
		lockdclient.WithKeepAliveTimeout(15*time.Second),
		lockdclient.WithCloseTimeout(15*time.Second),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
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
		targetCount := count + 1
		expectedOwner := "reader-single"
		updated := map[string]any{
			"payload": "single-server",
			"count":   targetCount,
			"owner":   expectedOwner,
		}
		refreshProgress := func() (float64, bool, error) {
			var latest map[string]any
			if err := af.Load(handlerCtx, &latest); err != nil {
				return 0, false, err
			}
			if latest == nil {
				return 0, false, nil
			}
			current := 0.0
			if v, ok := latest["count"].(float64); ok {
				current = v
			}
			owner := fmt.Sprint(latest["owner"])
			achieved := owner == expectedOwner && current >= targetCount
			return current, achieved, nil
		}
		var saveErr error
		for attempt := 0; attempt < 4; attempt++ {
			if saveErr = af.Save(handlerCtx, updated); saveErr == nil {
				return nil
			}

			var apiErr *lockdclient.APIError
			if errors.As(saveErr, &apiErr) && apiErr.Response.ErrorCode == "version_conflict" {
				current, achieved, refreshErr := refreshProgress()
				if refreshErr != nil {
					return fmt.Errorf("save snapshot: %w (refresh: %v)", saveErr, refreshErr)
				}
				if achieved {
					return nil
				}
				targetCount = current + 1
				updated["count"] = targetCount
				continue
			}

			if !retryableTransportError(saveErr) {
				return fmt.Errorf("save snapshot: %w", saveErr)
			}
			current, achieved, refreshErr := refreshProgress()
			if refreshErr != nil {
				return fmt.Errorf("save snapshot: %w (refresh: %v)", saveErr, refreshErr)
			}
			if achieved {
				return nil
			}
			targetCount = current + 1
			updated["count"] = targetCount
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
	if !releaseLease(t, verifyCtx, verifyLease) {
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
		releaseLease(t, ctx, lease)
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
		releaseLease(t, ctx, verify)
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
		releaseLease(t, ctx, verify)
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
		releaseLease(t, ctx, lease)

		verify := acquireWithRetry(t, ctx, cli, key, "remover-update-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, cli, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state post-remove, got %+v", state)
		}
		releaseLease(t, ctx, verify)
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
		releaseLease(t, ctx, lease)
		cleanupMinio(t, cfg, key)
	})

	t.Run("remove-after-reacquire", func(t *testing.T) {
		key := "minio-remove-reacquire-" + uuidv7.NewString()
		writer := acquireWithRetry(t, ctx, cli, key, "writer", 30, lockdclient.BlockWaitForever)
		if err := writer.Save(ctx, map[string]any{"step": "written"}); err != nil {
			t.Fatalf("writer save: %v", err)
		}
		releaseLease(t, ctx, writer)

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
		releaseLease(t, ctx, remover)

		verify := acquireWithRetry(t, ctx, cli, key, "remover-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, cli, key, verify.LeaseID)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state after remove, got %+v", state)
		}
		releaseLease(t, ctx, verify)
		cleanupMinio(t, cfg, key)
	})
}

func TestMinioTxnCommitAcrossNodes(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)

	cliRM := startLockdServer(t, cfg) // acts as RM holding the lease
	cliTC := startLockdServer(t, cfg) // acts as TC issuing the decision

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "minio-txn-cross-commit-" + uuidv7.NewString()

	lease := acquireWithRetry(t, ctx, cliRM, key, "rm-node", 45, lockdclient.BlockWaitForever)
	if err := lease.Save(ctx, map[string]any{"value": "from-rm"}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	resp, err := cliTC.Release(ctx, api.ReleaseRequest{
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
	// Clear lease tracker on originating server to avoid lingering active lease during shutdown.
	_, _ = cliRM.Release(ctx, api.ReleaseRequest{
		Key:     key,
		LeaseID: lease.LeaseID,
		TxnID:   lease.TxnID,
	})

	verifier := acquireWithRetry(t, ctx, cliRM, key, "verifier", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, cliRM, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, verifier) {
		t.Fatalf("release verifier failed")
	}
	if state == nil || state["value"] != "from-rm" {
		t.Fatalf("expected committed state from RM, got %+v", state)
	}
	cleanupMinio(t, cfg, key)
}

func TestMinioTxnRollbackAcrossNodes(t *testing.T) {
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)

	cliRM := startLockdServer(t, cfg)
	cliTC := startLockdServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "minio-txn-cross-rollback-" + uuidv7.NewString()

	lease := acquireWithRetry(t, ctx, cliRM, key, "rm-node", 45, lockdclient.BlockWaitForever)
	if err := lease.Save(ctx, map[string]any{"value": "should-rollback"}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	resp, err := cliTC.Release(ctx, api.ReleaseRequest{
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
	// Clear lease tracker on originating server.
	_, _ = cliRM.Release(ctx, api.ReleaseRequest{
		Key:      key,
		LeaseID:  lease.LeaseID,
		TxnID:    lease.TxnID,
		Rollback: true,
	})

	verifier := acquireWithRetry(t, ctx, cliRM, key, "verifier", 45, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, cliRM, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, verifier) {
		t.Fatalf("release verifier failed")
	}
	if state != nil {
		t.Fatalf("expected rollback to discard state, got %+v", state)
	}
	cleanupMinio(t, cfg, key)
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
	if err := seedLease.Save(ctx, map[string]any{"payload": "minio-failover", "count": 2}); err != nil {
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
		[]string{activeServer.URL(), standbyServer.URL()},
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

	primaryURL := activeServer.URL()
	backupURL := standbyServer.URL()
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

	activeServer, seedClient, err := hatest.FindActiveServer(ctx, primary, backup)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	t.Cleanup(func() { _ = seedClient.Close() })
	standby := backup
	if activeServer == backup {
		standby = primary
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
	if primary.Config.MTLSEnabled() {
		creds := primary.TestMTLSCredentials()
		if !creds.Valid() {
			t.Fatalf("minio failover: missing MTLS credentials")
		}
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

	assertMinioRemoveFailoverLogs(t, clientLogs, activeServer.URL(), standby.URL())
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
	cleanupMinio(t, cfg, key)
}
