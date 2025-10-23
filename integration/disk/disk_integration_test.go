//go:build integration && disk && !lq

package diskintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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
	testlog "pkt.systems/lockd/integration/internal/testlog"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/logport"
	"pkt.systems/logport/adapters/psl"
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

func ensureDiskRootEnv(tb testing.TB) {
	tb.Helper()
	if env := os.Getenv("LOCKD_DISK_ROOT"); env != "" {
		return
	}
	tb.Fatalf("LOCKD_DISK_ROOT must be set (source .env.disk before running disk integration tests)")
}

func TestDiskLockLifecycle(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx := context.Background()
	key := "disk-lifecycle-" + uuidv7.NewString()
	runLifecycleTest(t, ctx, cli, key, "disk-worker")
}

func TestDiskConcurrency(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx := context.Background()
	key := "disk-concurrency-" + uuidv7.NewString()
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

func TestDiskAutoKeyAcquire(t *testing.T) {
	ensureDiskRootEnv(t)
	runDiskAutoKeyAcquireScenario(t, "", "disk-auto")
}

func runDiskAutoKeyAcquireScenario(t *testing.T, base string, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	root := prepareDiskRoot(t, base)
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

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

func TestDiskRemoveStateSingleServer(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("remove-after-update", func(t *testing.T) {
		key := "disk-remove-update-" + uuidv7.NewString()
		lease := acquireWithRetry(t, ctx, cli, key, "remover-update", 30, lockdclient.BlockWaitForever)
		payload := map[string]any{"value": "present", "count": 1.0}
		if err := lease.Save(ctx, payload); err != nil {
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
		state, _, _, err := getStateJSON(ctx, lease)
		if err != nil {
			t.Fatalf("get after remove: %v", err)
		}
		if state != nil {
			t.Fatalf("expected nil state after remove, got %+v", state)
		}
		releaseLease(t, ctx, lease)

		verify := acquireWithRetry(t, ctx, cli, key, "remover-update-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, verify)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state post-remove, got %+v", state)
		}
		if verify.Version != res.NewVersion {
			t.Fatalf("expected acquire version %d, got %d", res.NewVersion, verify.Version)
		}
		releaseLease(t, ctx, verify)
	})

	t.Run("remove-without-state", func(t *testing.T) {
		key := "disk-remove-empty-" + uuidv7.NewString()
		lease := acquireWithRetry(t, ctx, cli, key, "remover-empty", 30, lockdclient.BlockWaitForever)
		res, err := lease.Remove(ctx)
		if err != nil {
			t.Fatalf("remove empty: %v", err)
		}
		if res.Removed {
			t.Fatalf("expected removed=false for empty key, got %+v", res)
		}
		if res.NewVersion != 0 {
			t.Fatalf("expected version 0 for empty remove, got %d", res.NewVersion)
		}
		state, _, _, err := getStateJSON(ctx, lease)
		if err != nil {
			t.Fatalf("get after empty remove: %v", err)
		}
		if state != nil {
			t.Fatalf("expected nil state, got %+v", state)
		}
		releaseLease(t, ctx, lease)
	})

	t.Run("remove-after-reacquire", func(t *testing.T) {
		key := "disk-remove-reacquire-" + uuidv7.NewString()
		writer := acquireWithRetry(t, ctx, cli, key, "writer", 30, lockdclient.BlockWaitForever)
		if err := writer.Save(ctx, map[string]any{"step": "written"}); err != nil {
			t.Fatalf("save: %v", err)
		}
		releaseLease(t, ctx, writer)

		remover := acquireWithRetry(t, ctx, cli, key, "remover", 30, lockdclient.BlockWaitForever)
		state, _, _, err := getStateJSON(ctx, remover)
		if err != nil {
			t.Fatalf("remover get: %v", err)
		}
		if state["step"] != "written" {
			t.Fatalf("expected state before remove, got %+v", state)
		}
		res, err := remover.Remove(ctx)
		if err != nil {
			t.Fatalf("remove: %v", err)
		}
		if !res.Removed {
			t.Fatalf("expected removal, got %+v", res)
		}
		releaseLease(t, ctx, remover)

		verifier := acquireWithRetry(t, ctx, cli, key, "remover-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err = getStateJSON(ctx, verifier)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state after remove, got %+v", state)
		}
		if verifier.Version != res.NewVersion {
			t.Fatalf("expected version %d, got %d", res.NewVersion, verifier.Version)
		}
		releaseLease(t, ctx, verifier)
	})
}

func TestDiskRemoveStateAcquireForUpdate(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seedState := func(tb testing.TB, key string, payload map[string]any) {
		tb.Helper()
		lease := acquireWithRetry(tb, ctx, cli, key, "seed", 30, lockdclient.BlockWaitForever)
		if err := lease.Save(ctx, payload); err != nil {
			tb.Fatalf("seed save: %v", err)
		}
		releaseLease(tb, ctx, lease)
	}

	t.Run("remove-in-handler", func(t *testing.T) {
		key := "disk-remove-afu-" + uuidv7.NewString()
		seedState(t, key, map[string]any{"payload": "seed-value", "count": 7.0})

		var handlerCalled bool
		watchdog := time.AfterFunc(10*time.Second, func() {
			buf := make([]byte, 1<<18)
			n := runtime.Stack(buf, true)
			panic("TestDiskRemoveStateAcquireForUpdate/remove-in-handler hung:\n" + string(buf[:n]))
		})
		defer watchdog.Stop()

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
			t.Fatalf("handler was not invoked")
		}

		verify := acquireWithRetry(t, ctx, cli, key, "remove-handler-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err := getStateJSON(ctx, verify)
		if err != nil {
			t.Fatalf("verify get: %v", err)
		}
		if state != nil {
			t.Fatalf("expected empty state after handler remove, got %+v", state)
		}
		releaseLease(t, ctx, verify)
	})

	t.Run("remove-and-recreate", func(t *testing.T) {
		key := "disk-remove-recreate-" + uuidv7.NewString()
		seedState(t, key, map[string]any{"payload": "initial", "count": 1.0})

		watchdog := time.AfterFunc(10*time.Second, func() {
			buf := make([]byte, 1<<18)
			n := runtime.Stack(buf, true)
			panic("TestDiskRemoveStateAcquireForUpdate/remove-and-recreate hung:\n" + string(buf[:n]))
		})
		defer watchdog.Stop()

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
			newPayload := map[string]any{
				"payload": "recreated",
				"count":   2.0,
			}
			if err := af.Save(handlerCtx, newPayload); err != nil {
				return fmt.Errorf("save after remove: %w", err)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("acquire-for-update: %v", err)
		}

		verify := acquireWithRetry(t, ctx, cli, key, "remove-recreate-verify", 30, lockdclient.BlockWaitForever)
		state, _, _, err := getStateJSON(ctx, verify)
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

func TestDiskAcquireForUpdateConcurrency(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key := "disk-for-update-concurrency-" + uuidv7.NewString()
	const workers = 5
	const iterations = 8
	ttl := int64(30)

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
			for iter := 0; iter < iterations; {
				select {
				case <-ctx.Done():
					return
				default:
				}
				err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
					Key:        key,
					Owner:      fmt.Sprintf("for-update-worker-%d", worker),
					TTLSeconds: ttl,
					BlockSecs:  lockdclient.BlockWaitForever,
				}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
					payload := map[string]any{
						"worker":    worker,
						"iteration": iter,
						"nonce":     uuidv7.NewString(),
						"random":    rng.Int63(),
					}
					body, err := json.Marshal(payload)
					if err != nil {
						return fmt.Errorf("marshal payload: %w", err)
					}
					if _, err := af.UpdateBytes(handlerCtx, body); err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					var apiErr *lockdclient.APIError
					if errors.As(err, &apiErr) {
						code := apiErr.Response.ErrorCode
						if code == "meta_conflict" || code == "version_conflict" || code == "etag_mismatch" {
							continue
						}
					}
					t.Errorf("worker %d acquire-for-update: %v", worker, err)
					return
				}
				iter++
			}
		}()
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli, key, "verify", ttl, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("verify get state: %v", err)
	}
	if state == nil {
		t.Fatalf("expected state after concurrency test")
	}
	_ = releaseLease(t, ctx, verifier)
}

func TestDiskRemoveStateMultiServer(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")

	cfg1 := buildDiskConfig(t, root, 0)
	cli1 := startDiskServer(t, cfg1)

	cfg2 := buildDiskConfig(t, root, 0)
	cli2 := startDiskServer(t, cfg2)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	key := "disk-remove-multi-" + uuidv7.NewString()

	writer := acquireWithRetry(t, ctx, cli1, key, "writer-1", 30, lockdclient.BlockWaitForever)
	if err := writer.Save(ctx, map[string]any{"payload": "shared", "count": 1.0}); err != nil {
		t.Fatalf("writer save: %v", err)
	}
	releaseLease(t, ctx, writer)

	remover := acquireWithRetry(t, ctx, cli1, key, "remover-1", 30, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, remover)
	if err != nil {
		t.Fatalf("remover get: %v", err)
	}
	if state["payload"] != "shared" {
		t.Fatalf("expected shared payload, got %+v", state)
	}
	removeRes, err := remover.Remove(ctx)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !removeRes.Removed {
		t.Fatalf("expected remove to succeed, got %+v", removeRes)
	}
	releaseLease(t, ctx, remover)

	verifier := acquireWithRetry(t, ctx, cli2, key, "verifier-2", 30, lockdclient.BlockWaitForever)
	state, _, _, err = getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("verifier get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state on second server, got %+v", state)
	}
	if verifier.Version != removeRes.NewVersion {
		t.Fatalf("expected version %d on verifier, got %d", removeRes.NewVersion, verifier.Version)
	}
	releaseLease(t, ctx, verifier)
}

func TestDiskRemoveStateCASMismatch(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := "disk-remove-cas-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "cas-owner", 30, lockdclient.BlockWaitForever)

	if err := lease.Save(ctx, map[string]any{"payload": "v1"}); err != nil {
		t.Fatalf("initial save: %v", err)
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
	} else {
		var apiErr *lockdclient.APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("expected API error, got %v", err)
		}
		if apiErr.Response.ErrorCode != "etag_mismatch" {
			t.Fatalf("expected etag_mismatch, got %s", apiErr.Response.ErrorCode)
		}
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

func TestDiskRemoveStateKeepAlive(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	key := "disk-remove-keepalive-" + uuidv7.NewString()
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

	staleUpdate := lockdclient.UpdateStateOptions{
		IfETag:    originalETag,
		IfVersion: strconv.FormatInt(originalVersion, 10),
	}
	if _, err := lease.UpdateWithOptions(ctx, bytes.NewReader([]byte(`{"payload":"stale"}`)), staleUpdate); err == nil {
		t.Fatalf("expected stale update to fail")
	} else {
		var apiErr *lockdclient.APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("expected API error, got %v", err)
		}
		if apiErr.Response.ErrorCode != "version_conflict" && apiErr.Response.ErrorCode != "etag_mismatch" {
			t.Fatalf("unexpected error code %s", apiErr.Response.ErrorCode)
		}
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

func TestDiskRemoveStateFailoverMultiServer(t *testing.T) {
	ensureDiskRootEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	root := prepareDiskRoot(t, "")
	store, err := disk.New(disk.Config{Root: root})
	if err != nil {
		t.Fatalf("disk backend: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	primary := lockd.StartTestServer(t,
		lockd.WithTestBackend(store),
		lockd.WithTestLoggerFromTB(t, logport.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)),
			lockdclient.WithHTTPTimeout(2*time.Second),
		),
	)
	backup := lockd.StartTestServer(t,
		lockd.WithTestBackend(store),
		lockd.WithTestLoggerFromTB(t, logport.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)),
			lockdclient.WithHTTPTimeout(2*time.Second),
		),
	)

	key := "disk-remove-failover-" + uuidv7.NewString()

	seedCli := directDiskClient(t, backup)
	defer seedCli.Close()
	seedLease, err := seedCli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(ctx, map[string]any{"payload": "seed", "count": 1.0}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	releaseLease(t, ctx, seedLease)

	clientLogger, clientLogs := testlog.NewRecorder(t, logport.TraceLevel)
	failoverClient, err := lockdclient.NewWithEndpoints(
		[]string{primary.URL(), backup.URL()},
		lockdclient.WithMTLS(false),
		lockdclient.WithHTTPTimeout(2*time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
	)
	if err != nil {
		t.Fatalf("failover client: %v", err)
	}
	defer failoverClient.Close()

	lease, err := failoverClient.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "failover-remover",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("failover acquire: %v", err)
	}

	var shutdownOnce sync.Once
	var shutdownErr error
	triggerFailover := func() error {
		shutdownOnce.Do(func() {
			shutdownErr = primary.Server.Shutdown(context.Background())
		})
		return shutdownErr
	}

	if err := triggerFailover(); err != nil {
		t.Fatalf("shutdown primary: %v", err)
	}

	res, err := lease.Remove(ctx)
	if err != nil {
		t.Fatalf("remove during failover: %v", err)
	}
	if !res.Removed {
		t.Fatalf("expected removal success, got %+v", res)
	}
	releaseLease(t, ctx, lease)

	verify := acquireWithRetry(t, ctx, failoverClient, key, "failover-verifier", 20, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verify)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	if state != nil {
		t.Fatalf("expected empty state after failover remove, got %+v", state)
	}
	releaseLease(t, ctx, verify)

	assertRemoveFailoverLogs(t, clientLogs, primary.URL(), backup.URL())
}

func TestDiskAutoKeyAcquireForUpdate(t *testing.T) {
	ensureDiskRootEnv(t)
	runDiskAutoKeyAcquireForUpdateScenario(t, "", "auto-handler")
}

func runDiskAutoKeyAcquireForUpdateScenario(t *testing.T, base, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	root := prepareDiskRoot(t, base)
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	var generated string
	err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Owner:      owner,
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		if af.Session == nil || af.Session.Key == "" {
			return fmt.Errorf("expected generated key")
		}
		generated = af.Session.Key
		if af.State != nil && af.State.HasState {
			return fmt.Errorf("expected empty initial state")
		}
		return af.Save(handlerCtx, map[string]any{"owner": owner, "count": 1})
	})
	if err != nil {
		t.Fatalf("acquire-for-update: %v", err)
	}
	if generated == "" {
		t.Fatal("missing generated key")
	}

	verify := acquireWithRetry(t, ctx, cli, generated, owner+"-verify", 20, lockdclient.BlockWaitForever)
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

func TestDiskAcquireForUpdateConnectionDrop(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "disk-for-update-drop-" + uuidv7.NewString()

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
			// Wait for cancellation to simulate an abrupt disconnect.
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
	if !releaseLease(t, ctx, second) {
		t.Fatalf("release of second lease failed")
	}
}

func TestDiskAcquireForUpdateRandomPayloads(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "disk-for-update-random-" + uuidv7.NewString()
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
			t.Fatalf("iteration %d acquire-for-update: %v", i, err)
		}
	}

	verifier := acquireWithRetry(t, ctx, cli, key, "verify-random", 20, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("verify state: %v", err)
	}
	if !releaseLease(t, ctx, verifier) {
		t.Fatalf("verify release failed")
	}
	if state == nil {
		t.Fatalf("expected persisted state after random payloads")
	}
	if iter, ok := state["iteration"].(float64); !ok || int(iter) != iterations-1 {
		t.Fatalf("unexpected iteration in final state: %+v", state)
	}
}

func TestDiskAcquireForUpdateCallbackSingleServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watchdog := time.AfterFunc(10*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("TestDiskAcquireForUpdateCallbackSingleServer timeout after 10s:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	root := filepath.Join(t.TempDir(), "disk-backend")
	store, err := disk.New(disk.Config{Root: root})
	if err != nil {
		t.Fatalf("disk backend: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	chaos := &lockd.ChaosConfig{
		Seed:            321,
		DisconnectAfter: 150 * time.Millisecond,
		MinDelay:        5 * time.Millisecond,
		MaxDelay:        15 * time.Millisecond,
		MaxDisconnects:  1,
	}

	ts := lockd.StartTestServer(t,
		lockd.WithTestBackend(store),
		lockd.WithTestChaos(chaos),
		lockd.WithTestLoggerFromTB(t, logport.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)),
			lockdclient.WithHTTPTimeout(500*time.Millisecond),
		),
	)

	proxiedClient := ts.Client
	if proxiedClient == nil {
		var newErr error
		proxiedClient, newErr = ts.NewClient()
		if newErr != nil {
			t.Fatalf("proxied client: %v", newErr)
		}
	}
	t.Cleanup(func() { _ = proxiedClient.Close() })

	key := "disk-single-" + uuidv7.NewString()
	seedPayload := map[string]any{"payload": "disk-single", "count": 1}

	seedCli := directDiskClient(t, ts)
	t.Cleanup(func() { _ = seedCli.Close() })

	seedCtx, seedCancel := context.WithTimeout(ctx, time.Second)
	seedLease, err := seedCli.Acquire(seedCtx, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 20,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		seedCancel()
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(seedCtx, seedPayload); err != nil {
		seedLease.Release(seedCtx)
		seedCancel()
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(seedCtx); err != nil {
		seedCancel()
		t.Fatalf("seed release: %v", err)
	}
	seedCancel()

	handlerCalled := false
	err = proxiedClient.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "reader",
		TTLSeconds: 20,
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
		if snapshot["payload"] != "disk-single" {
			return fmt.Errorf("unexpected payload: %+v", snapshot)
		}
		var count float64
		if v, ok := snapshot["count"].(float64); ok {
			count = v
		}
		updated := map[string]any{
			"payload": "disk-single",
			"count":   count + 1,
			"owner":   "reader",
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

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), time.Second)
	defer verifyCancel()
	verifyLease, err := seedCli.Acquire(verifyCtx, api.AcquireRequest{
		Key:        key,
		Owner:      "verify",
		TTLSeconds: 10,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("verify acquire: %v", err)
	}
	state, _, _, err := getStateJSON(verifyCtx, verifyLease)
	if err != nil {
		t.Fatalf("verify get state: %v", err)
	}
	if state["owner"] != "reader" {
		t.Fatalf("unexpected owner after callback: %+v", state)
	}
	if !releaseLease(t, verifyCtx, verifyLease) {
		t.Fatalf("verify release failed")
	}
}

func TestDiskAcquireForUpdateCallbackFailoverMultiServer(t *testing.T) {
	phases := []failoverPhase{
		failoverDuringHandlerStart,
		failoverBeforeSave,
		failoverAfterSave,
	}
	ensureDiskRootEnv(t)
	for _, phase := range phases {
		phase := phase
		t.Run("disk/"+phase.String(), func(t *testing.T) {
			runDiskAcquireForUpdateCallbackFailoverMultiServer(t, phase, "")
		})
	}
}

func runDiskAcquireForUpdateCallbackFailoverMultiServer(t *testing.T, phase failoverPhase, base string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watchdog := time.AfterFunc(10*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("runDiskAcquireForUpdateCallbackFailoverMultiServer timeout after 10s:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	root := prepareDiskRoot(t, base)
	store, err := disk.New(disk.Config{Root: root})
	if err != nil {
		t.Fatalf("disk backend: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	const disconnectAfter = 1 * time.Second
	chaos := &lockd.ChaosConfig{
		Seed:            4242 + int64(phase),
		DisconnectAfter: disconnectAfter,
		MinDelay:        5 * time.Millisecond,
		MaxDelay:        20 * time.Millisecond,
		MaxDisconnects:  1,
	}

	primary := lockd.StartTestServer(t,
		lockd.WithTestBackend(store),
		lockd.WithTestChaos(chaos),
		lockd.WithTestLoggerFromTB(t, logport.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)),
			lockdclient.WithHTTPTimeout(time.Second),
		),
	)
	backup := lockd.StartTestServer(t,
		lockd.WithTestBackend(store),
		lockd.WithTestLoggerFromTB(t, logport.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)),
			lockdclient.WithHTTPTimeout(time.Second),
		),
	)

	failoverBlob := strings.Repeat("disk-failover-", 32768)
	key := fmt.Sprintf("disk-multi-%s-%s", phase.String(), uuidv7.NewString())
	seedCli := directDiskClient(t, backup)
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
		"payload": "disk-multi",
		"count":   2,
		"blob":    failoverBlob,
	}); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(ctx); err != nil {
		t.Fatalf("seed release: %v", err)
	}

	clientLogger, clientLogs := testlog.NewRecorder(t, logport.TraceLevel)
	failoverClient, err := lockdclient.NewWithEndpoints(
		[]string{primary.URL(), backup.URL()},
		lockdclient.WithMTLS(false),
		lockdclient.WithHTTPTimeout(2*time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
	)
	if err != nil {
		t.Fatalf("failover client: %v", err)
	}
	t.Cleanup(func() { _ = failoverClient.Close() })
	var shutdownOnce sync.Once
	var shutdownErr error
	triggerFailover := func() error {
		shutdownOnce.Do(func() {
			shutdownErr = primary.Server.Shutdown(context.Background())
		})
		return shutdownErr
	}

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
		if snapshot["payload"] != "disk-multi" {
			return fmt.Errorf("unexpected payload: %+v", snapshot)
		}
		blob, ok := snapshot["blob"].(string)
		if !ok || len(blob) != len(failoverBlob) {
			return fmt.Errorf("unexpected blob in snapshot: len=%d", len(blob))
		}
		if phase == failoverBeforeSave {
			if err := triggerFailover(); err != nil {
				return fmt.Errorf("shutdown primary: %w", err)
			}
		}
		select {
		case <-handlerCtx.Done():
			return handlerCtx.Err()
		case <-time.After(disconnectAfter + 200*time.Millisecond):
		}
		var count float64
		if v, ok := snapshot["count"].(float64); ok {
			count = v
		}
		updated := map[string]any{
			"payload": "disk-multi",
			"count":   count + 1,
			"blob":    blob,
			"owner":   "reader-multi",
		}
		if err := af.Save(handlerCtx, updated); err != nil {
			return err
		}
		if phase == failoverAfterSave {
			if err := triggerFailover(); err != nil {
				return fmt.Errorf("shutdown primary: %w", err)
			}
		}
		return nil
	})
	watchdog.Stop()
	expectedConflict := phase == failoverAfterSave
	conflictObserved := false
	if err != nil {
		var apiErr *lockdclient.APIError
		if expectedConflict && errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "version_conflict" {
			conflictObserved = true
			t.Logf("phase %s observed expected version_conflict after failover: %v", phase, err)
		} else {
			t.Fatalf("acquire-for-update callback: %v\n%s", err, clientLogs.Summary())
		}
	}
	if !handlerCalled {
		t.Fatalf("callback was not invoked")
	}

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), time.Second)
	defer verifyCancel()
	verifyLease, err := seedCli.Acquire(verifyCtx, api.AcquireRequest{
		Key:        key,
		Owner:      "verify",
		TTLSeconds: 10,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("verify acquire: %v", err)
	}
	state, _, _, err := getStateJSON(verifyCtx, verifyLease)
	if err != nil {
		t.Fatalf("verify get state: %v", err)
	}
	if payload, ok := state["payload"].(string); !ok || payload != "disk-multi" {
		t.Fatalf("unexpected payload post-callback: %+v", state)
	}
	if owner, ok := state["owner"].(string); !ok || owner != "reader-multi" {
		t.Fatalf("unexpected owner post-callback: %+v", state)
	}
	if blob, ok := state["blob"].(string); !ok || len(blob) != len(failoverBlob) {
		t.Fatalf("unexpected blob size: got %d want %d", len(blob), len(failoverBlob))
	}
	if count, ok := state["count"].(float64); !ok || count != 3 {
		t.Fatalf("unexpected count after callback: %v", state["count"])
	}
	if !releaseLease(t, verifyCtx, verifyLease) {
		t.Fatalf("verify release failed")
	}

	result := assertFailoverLogs(t, clientLogs, primary.URL(), backup.URL())
	if conflictObserved {
		if result.AlternateStatus != http.StatusConflict {
			t.Fatalf("expected HTTP 409 on alternate endpoint during conflict; got %d\nlogs:\n%s", result.AlternateStatus, clientLogs.Summary())
		}
	} else if result.AlternateStatus != http.StatusOK {
		t.Fatalf("expected HTTP 200 on alternate endpoint; got %d\nlogs:\n%s", result.AlternateStatus, clientLogs.Summary())
	}
}

func assertFailoverLogs(t testing.TB, rec *testlog.Recorder, primary, backup string) failoverLogResult {
	const (
		successMsg = "client.http.success"
		errorMsg   = "client.http.error"
		acquireMsg = "client.acquire_for_update.acquired"
	)
	result := failoverLogResult{}
	acquiredEntry, ok := rec.First(func(e testlog.Entry) bool {
		return e.Message == acquireMsg
	})
	if !ok {
		t.Fatalf("expected %s in logs; %s", acquireMsg, rec.Summary())
	}
	initialEndpoint := testlog.GetStringField(acquiredEntry, "endpoint")
	if initialEndpoint == "" {
		t.Fatalf("%s missing endpoint; %s", acquireMsg, rec.Summary())
	}

	errorEntry, hasError := rec.FirstAfter(acquiredEntry.Timestamp, func(e testlog.Entry) bool {
		if e.Message != errorMsg {
			return false
		}
		endpoint := testlog.GetStringField(e, "endpoint")
		return endpoint == initialEndpoint
	})
	if hasError {
		result.HadTransportError = true
		result.ErrorEndpoint = testlog.GetStringField(errorEntry, "endpoint")
	}

	events := rec.Events()
	successFound := false
	startTime := acquiredEntry.Timestamp
	if hasError {
		startTime = errorEntry.Timestamp
	}
	for _, entry := range events {
		if entry.Timestamp.Before(startTime) {
			continue
		}
		if entry.Message != successMsg {
			continue
		}
		endpoint := testlog.GetStringField(entry, "endpoint")
		if endpoint == "" {
			continue
		}
		if endpoint != primary && endpoint != backup {
			continue
		}
		if endpoint == initialEndpoint {
			continue
		}
		status, ok := testlog.GetIntField(entry, "status")
		if !ok {
			t.Fatalf("%s missing status; logs:\n%s", successMsg, rec.Summary())
		}
		if status != http.StatusOK && status != http.StatusConflict {
			t.Fatalf("unexpected status %d on alternate endpoint %q; logs:\n%s", status, endpoint, rec.Summary())
		}
		successFound = true
		result.AlternateEndpoint = endpoint
		result.AlternateStatus = status
		break
	}
	if !successFound {
		t.Fatalf("expected failover success on alternate endpoint; logs:\n%s", rec.Summary())
	}

	if hasError {
		errorEndpoint := testlog.GetStringField(errorEntry, "endpoint")
		if errorEndpoint == "" {
			t.Fatalf("%s missing endpoint; %s", errorMsg, rec.Summary())
		}
		if errorEndpoint != primary && errorEndpoint != backup {
			t.Fatalf("unexpected error endpoint %q; logs:\n%s", errorEndpoint, rec.Summary())
		}
		if result.AlternateEndpoint == errorEndpoint {
			t.Fatalf("expected success on a different endpoint than the error; logs:\n%s", rec.Summary())
		}
	} else if result.AlternateEndpoint == "" {
		t.Fatalf("expected alternate endpoint in logs; logs:\n%s", rec.Summary())
	}

	t.Logf("failover log summary: alternate=%s status=%d transport_error=%t error_endpoint=%s\n%s",
		result.AlternateEndpoint, result.AlternateStatus, result.HadTransportError, result.ErrorEndpoint, rec.Summary())
	return result
}

func assertRemoveFailoverLogs(t testing.TB, rec *testlog.Recorder, primary, backup string) {
	const (
		startMsg        = "client.remove_state.start"
		successMsg      = "client.remove_state.success"
		transportErrMsg = "client.remove_state.transport_error"
		httpErrorMsg    = "client.http.error"
		httpSuccessMsg  = "client.http.success"
	)

	events := rec.Events()
	if len(events) == 0 {
		t.Fatalf("no logs captured for remove failover")
	}

	startEntry, ok := rec.First(func(e testlog.Entry) bool {
		return e.Message == startMsg
	})
	if !ok {
		t.Fatalf("expected %s entry; logs:\n%s", startMsg, rec.Summary())
	}
	initialEndpoint := testlog.GetStringField(startEntry, "endpoint")

	var errorEntry *testlog.Entry
	hasErrorEntry := false
	if entry, ok := rec.FirstAfter(startEntry.Timestamp, func(e testlog.Entry) bool {
		if e.Message == transportErrMsg {
			return true
		}
		if e.Message == httpErrorMsg {
			return true
		}
		return false
	}); ok {
		errorEntry = &entry
		hasErrorEntry = true
	}

	successEntry, ok := rec.FirstAfter(startEntry.Timestamp, func(e testlog.Entry) bool {
		return e.Message == successMsg
	})
	if !ok {
		t.Fatalf("expected %s entry; logs:\n%s", successMsg, rec.Summary())
	}
	successEndpoint := testlog.GetStringField(successEntry, "endpoint")
	if successEndpoint == "" {
		t.Fatalf("%s missing endpoint; logs:\n%s", successMsg, rec.Summary())
	}
	if successEndpoint == initialEndpoint {
		t.Fatalf("success occurred on initial endpoint %q; expected failover. Logs:\n%s", successEndpoint, rec.Summary())
	}
	if successEndpoint != backup {
		t.Fatalf("expected success on backup %q, got %q; logs:\n%s", backup, successEndpoint, rec.Summary())
	}

	if hasErrorEntry {
		errorEndpoint := testlog.GetStringField(*errorEntry, "endpoint")
		if errorEndpoint == "" {
			t.Fatalf("error entry missing endpoint; logs:\n%s", rec.Summary())
		}
		if errorEndpoint != primary {
			t.Fatalf("expected error on primary %q, got %q; logs:\n%s", primary, errorEndpoint, rec.Summary())
		}
	}

	httpSuccess, ok := rec.First(func(e testlog.Entry) bool {
		if e.Message != httpSuccessMsg {
			return false
		}
		return testlog.GetStringField(e, "endpoint") == successEndpoint
	})
	if !ok {
		t.Fatalf("expected %s entry for endpoint %s; logs:\n%s", httpSuccessMsg, successEndpoint, rec.Summary())
	}
	if status, ok := testlog.GetIntField(httpSuccess, "status"); !ok || status != http.StatusOK {
		t.Fatalf("unexpected HTTP success status for endpoint %s: %d (expected 200); logs:\n%s", successEndpoint, status, rec.Summary())
	}

	t.Logf("remove-state failover log summary: initial=%s success_endpoint=%s error_present=%t\n%s",
		initialEndpoint, successEndpoint, hasErrorEntry, rec.Summary())
}

func TestDiskRetentionSweep(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	store, err := disk.New(disk.Config{
		Root:            root,
		Retention:       time.Second,
		JanitorInterval: 0,
		Now:             func() time.Time { return time.Unix(10, 0) },
	})
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	key := "retention-" + uuidv7.NewString()

	meta := storage.Meta{Version: 1, UpdatedAtUnix: 1}
	if _, err := store.StoreMeta(ctx, key, &meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	if _, err := store.WriteState(ctx, key, strings.NewReader(`{"old":true}`), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write state: %v", err)
	}

	store.SweepOnceForTests()

	if _, _, err := store.LoadMeta(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected meta cleanup, got %v", err)
	}
	if _, _, err := store.ReadState(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected state cleanup, got %v", err)
	}
}

func TestDiskMultiReplica(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")

	cfg1 := buildDiskConfig(t, root, 0)
	cli1 := startDiskServer(t, cfg1)

	cfg2 := buildDiskConfig(t, root, 0)
	cli2 := startDiskServer(t, cfg2)

	clients := []*lockdclient.Client{cli1, cli2}
	owners := []string{"replica-1", "replica-2"}
	ctx := context.Background()
	key := "disk-multi-" + uuidv7.NewString()
	iterations := 5

	var wg sync.WaitGroup
	for i, cli := range clients {
		owner := owners[i]
		wg.Add(1)
		go func(cli *lockdclient.Client, owner string) {
			defer wg.Done()
			for n := 0; n < iterations; n++ {
				for {
					lease := acquireWithRetry(t, ctx, cli, key, owner, 45, lockdclient.BlockWaitForever)
					state, _, _, err := getStateJSON(ctx, lease)
					if err != nil {
						_ = releaseLease(t, ctx, lease)
						continue
					}
					var counter float64
					if state != nil {
						if v, ok := state["counter"].(float64); ok {
							counter = v
						}
					}
					counter++
					body, _ := json.Marshal(map[string]any{"counter": counter, "owner": owner})
					_, err = lease.UpdateBytes(ctx, body)
					if err != nil {
						var apiErr *lockdclient.APIError
						if errors.As(err, &apiErr) {
							code := apiErr.Response.ErrorCode
							if code == "meta_conflict" || code == "etag_mismatch" || code == "version_conflict" {
								_ = releaseLease(t, ctx, lease)
								continue
							}
						}
						t.Fatalf("update state: %v", err)
					}
					if !releaseLease(t, ctx, lease) {
						t.Fatalf("release failed for owner %s", owner)
					}
					break
				}
			}
		}(cli, owner)
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli1, key, "verifier", 45, lockdclient.BlockWaitForever)
	finalState, _, _, err := getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, verifier) {
		t.Fatalf("release verifier failed")
	}
	expected := float64(iterations * len(clients))
	if finalState == nil {
		t.Fatalf("expected final state, got nil")
	}
	if v, ok := finalState["counter"].(float64); !ok || v != expected {
		t.Fatalf("expected counter %.0f, got %v", expected, finalState["counter"])
	}
}

func directDiskClient(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
	t.Helper()
	if ts == nil || ts.Server == nil {
		t.Fatalf("nil test server")
	}
	addr := ts.Server.ListenerAddr()
	if addr == nil {
		t.Fatalf("listener not initialized")
	}
	baseURL := "http://" + addr.String()
	cli, err := lockdclient.New(baseURL,
		lockdclient.WithMTLS(false),
		lockdclient.WithLogger(lockd.NewTestingLogger(t, logport.TraceLevel)),
		lockdclient.WithHTTPTimeout(time.Second),
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

	payload, _ := json.Marshal(map[string]any{"cursor": 42, "backend": "disk"})
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

// Exported helpers for other backend suites (e.g. integration/nfs).

var DiskFailoverPhaseNames = []string{
	failoverDuringHandlerStart.String(),
	failoverBeforeSave.String(),
	failoverAfterSave.String(),
}

func RunDiskAutoKeyAcquireScenario(t *testing.T, base, owner string) {
	runDiskAutoKeyAcquireScenario(t, base, owner)
}

func RunDiskAutoKeyAcquireForUpdateScenario(t *testing.T, base, owner string) {
	runDiskAutoKeyAcquireForUpdateScenario(t, base, owner)
}

func RunDiskAcquireForUpdateCallbackFailoverPhase(t *testing.T, phaseName, base string) {
	var phase failoverPhase
	switch phaseName {
	case failoverDuringHandlerStart.String():
		phase = failoverDuringHandlerStart
	case failoverBeforeSave.String():
		phase = failoverBeforeSave
	case failoverAfterSave.String():
		phase = failoverAfterSave
	default:
		t.Fatalf("unknown failover phase %q", phaseName)
	}
	runDiskAcquireForUpdateCallbackFailoverMultiServer(t, phase, base)
}

func RunDiskLifecycleScenario(t *testing.T, base, owner string) {
	ctx := context.Background()
	root := prepareDiskRoot(t, base)
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)
	key := "disk-nfs-" + uuidv7.NewString()
	runLifecycleTest(t, ctx, cli, key, owner)
}

func prepareDiskRoot(tb testing.TB, base string) string {
	tb.Helper()
	var root string
	if base != "" {
		if info, err := os.Stat(base); err != nil || !info.IsDir() {
			tb.Fatalf("disk base %q unavailable: %v", base, err)
		}
		root = filepath.Join(base, "lockd-"+uuidv7.NewString())
	} else if env := os.Getenv("LOCKD_DISK_ROOT"); env != "" {
		root = filepath.Join(env, "lockd-"+uuidv7.NewString())
	} else {
		tb.Fatalf("LOCKD_DISK_ROOT must be set (source .env.disk before running disk integration/benchmarks)")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		tb.Fatalf("mkdir disk root: %v", err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func buildDiskConfig(tb testing.TB, root string, retention time.Duration) lockd.Config {
	storeURL := diskStoreURL(root)
	cfg := lockd.Config{
		Store:           storeURL,
		MTLS:            false,
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
		DiskRetention:   retention,
	}
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config validation failed: %v", err)
	}
	return cfg
}

func diskStoreURL(root string) string {
	if root == "" {
		root = "/tmp/lockd-disk"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return (&url.URL{Scheme: "disk", Path: root}).String()
}

func startDiskServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()

	cfg.MTLS = false

	loggerOption := diskTestLoggerOption(tb)

	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithLogger(lockd.NewTestingLogger(tb, logport.TraceLevel)),
	}

	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		loggerOption,
		lockd.WithTestClientOptions(clientOpts...),
	}

	ts := lockd.StartTestServer(tb, options...)
	return ts.Client
}

func diskTestLoggerOption(tb testing.TB) lockd.TestServerOption {
	tb.Helper()
	levelStr := os.Getenv("LOCKD_BENCH_LOG_LEVEL")
	if levelStr == "" {
		return lockd.WithTestLoggerFromTB(tb, logport.TraceLevel)
	}

	logPath := os.Getenv("LOCKD_BENCH_LOG_PATH")
	if logPath == "" {
		logPath = filepath.Join(os.TempDir(), fmt.Sprintf("lockd-bench-%d.log", time.Now().UnixNano()))
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		tb.Fatalf("prepare log directory: %v", err)
	}
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		tb.Fatalf("open bench log file: %v", err)
	}
	tb.Cleanup(func() { _ = file.Close() })

	logger := psl.NewStructured(file).With("svc", "bench")
	if level, ok := logport.ParseLevel(levelStr); ok {
		logger = logger.LogLevel(level)
	} else {
		tb.Fatalf("invalid LOCKD_BENCH_LOG_LEVEL %q", levelStr)
	}
	return lockd.WithTestLogger(logger.WithLogLevel())
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
