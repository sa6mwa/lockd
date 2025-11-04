//go:build integration && nfs && !lq

package nfsintegration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
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
	"pkt.systems/lockd/integration/internal/cryptotest"
	shutdowntest "pkt.systems/lockd/integration/internal/shutdowntest"
	testlog "pkt.systems/lockd/integration/internal/testlog"
	"pkt.systems/lockd/internal/storage/disk"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

type failoverPhase int

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

func ensureNFSRootEnv(tb testing.TB) string {
	tb.Helper()
	root := os.Getenv("LOCKD_NFS_ROOT")
	if root == "" {
		tb.Fatalf("LOCKD_NFS_ROOT must be set (source .env.nfs before running integration/nfs)")
	}
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		tb.Fatalf("LOCKD_NFS_ROOT %q unavailable: %v", root, err)
	}
	return root
}

func prepareNFSRoot(tb testing.TB, base string) string {
	tb.Helper()
	if base == "" {
		base = ensureNFSRootEnv(tb)
	}
	root := filepath.Join(base, "lockd-"+uuidv7.NewString())
	if err := os.MkdirAll(root, 0o755); err != nil {
		tb.Fatalf("mkdir nfs root: %v", err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func buildNFSConfig(tb testing.TB, root string, retention time.Duration) lockd.Config {
	tb.Helper()
	storeURL := diskStoreURL(root)
	cfg := lockd.Config{
		Store:           storeURL,
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
		DiskRetention:   retention,
	}
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	return cfg
}

func diskStoreURL(root string) string {
	if root == "" {
		root = "/tmp/lockd-nfs"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return (&url.URL{Scheme: "disk", Path: root}).String()
}

func startNFSServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()
	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
	}
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestClientOptions(clientOpts...),
		lockd.WithTestCloseDefaults(
			lockd.WithDrainLeases(-1),
			lockd.WithShutdownTimeout(10*time.Second),
		),
	}
	options = append(options, cryptotest.SharedMTLSOptions(tb)...)
	ts := lockd.StartTestServer(tb, options...)
	return ts.Client
}

func startNFSTestServer(tb testing.TB, cfg lockd.Config) *lockd.TestServer {
	tb.Helper()
	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
	}
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestClientOptions(clientOpts...),
	}
	options = append(options, cryptotest.SharedMTLSOptions(tb)...)
	return lockd.StartTestServer(tb, options...)
}

func TestNFSAutoKeyAcquire(t *testing.T) {
	base := ensureNFSRootEnv(t)
	runAutoKeyAcquireScenario(t, base, "nfs-auto")
}

func TestNFSShutdownDrainingBlocksAcquire(t *testing.T) {
	ctx := context.Background()
	base := ensureNFSRootEnv(t)
	root := prepareNFSRoot(t, base)
	cfg := buildNFSConfig(t, root, 0)
	ts := startNFSTestServer(t, cfg)
	t.Cleanup(func() { _ = ts.Stop(context.Background()) })
	cli := ts.Client
	if cli == nil {
		t.Fatalf("nil test server client")
	}
	key := "nfs-drain-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "holder", 30, lockdclient.BlockWaitForever)
	stopCh := make(chan error, 1)
	go func() {
		stopCh <- ts.Stop(context.Background(), lockd.WithDrainLeases(3*time.Second), lockd.WithShutdownTimeout(4*time.Second))
	}()
	payload, _ := json.Marshal(api.AcquireRequest{Key: "nfs-drain-wait", Owner: "drain-tester", TTLSeconds: 5})
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

func TestNFSConcurrency(t *testing.T) {
	base := ensureNFSRootEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	root := prepareNFSRoot(t, base)
	cfg := buildNFSConfig(t, root, 0)
	cli := startNFSServer(t, cfg)

	const (
		workers    = 5
		iterations = 20
		leaseTTL   = int64(60)
	)

	key := "nfs-concurrency-" + uuidv7.NewString()
	var updates atomic.Int64
	var wg sync.WaitGroup
	for id := 0; id < workers; id++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", workerID)
			for iter := 0; iter < iterations; {
				lease := acquireWithRetry(t, ctx, cli, key, owner, leaseTTL, 10)
				payload := map[string]any{"counter": 1, "worker": owner, "iteration": iter}
				if err := lease.Save(ctx, payload); err != nil {
					releaseLease(t, ctx, lease)
					var apiErr *lockdclient.APIError
					if errors.As(err, &apiErr) {
						code := apiErr.Response.ErrorCode
						if code == "meta_conflict" || code == "version_conflict" || code == "etag_mismatch" {
							continue
						}
					}
					t.Fatalf("save: %v", err)
				}
				updates.Add(1)
				releaseLease(t, ctx, lease)
				iter++
			}
		}(id)
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli, key, "verifier", leaseTTL, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	releaseLease(t, ctx, verifier)

	expected := float64(workers * iterations)
	if got := float64(updates.Load()); got != expected {
		t.Fatalf("expected %f successful updates, got %f", expected, got)
	}
	if state == nil {
		t.Fatalf("expected final state, got nil")
	}
	if counter, ok := state["counter"].(float64); !ok || counter != 1 {
		t.Fatalf("unexpected counter payload: %+v", state)
	}
}

func TestNFSAutoKeyAcquireForUpdate(t *testing.T) {
	base := ensureNFSRootEnv(t)
	runAutoKeyAcquireForUpdateScenario(t, base, "nfs-auto-handler")
}

func TestNFSRemoveStateSingleServer(t *testing.T) {
	base := ensureNFSRootEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	root := prepareNFSRoot(t, base)
	cfg := buildNFSConfig(t, root, 0)
	cli := startNFSServer(t, cfg)

	t.Run("remove-after-update", func(t *testing.T) {
		key := "nfs-remove-update-" + uuidv7.NewString()
		lease := acquireWithRetry(t, ctx, cli, key, "remover-update", 45, lockdclient.BlockWaitForever)
		payload := map[string]any{"value": "present", "count": 1.0}
		if err := lease.Save(ctx, payload); err != nil {
			t.Fatalf("save: %v", err)
		}
		if !releaseLease(t, ctx, lease) {
			t.Fatalf("release failed")
		}

		lease = acquireWithRetry(t, ctx, cli, key, "remover-update-2", 45, lockdclient.BlockWaitForever)
		res, err := lease.Remove(ctx)
		if err != nil {
			t.Fatalf("remove: %v", err)
		}
		if !res.Removed {
			t.Fatalf("expected removal success")
		}
		releaseLease(t, ctx, lease)

		verify := acquireWithRetry(t, ctx, cli, key, "verifier", 45, lockdclient.BlockWaitForever)
		state, _, _, err := getStateJSON(ctx, verify)
		if err != nil {
			t.Fatalf("verify state: %v", err)
		}
		if state != nil {
			t.Fatalf("expected nil state after remove, got %v", state)
		}
		releaseLease(t, ctx, verify)
	})
}

func TestNFSAcquireForUpdateCallbackFailover(t *testing.T) {
	base := ensureNFSRootEnv(t)
	phases := []failoverPhase{failoverDuringHandlerStart, failoverBeforeSave, failoverAfterSave}
	for _, phase := range phases {
		phase := phase
		t.Run(phase.String(), func(t *testing.T) {
			runAcquireForUpdateCallbackFailover(t, phase, base)
		})
	}
}

func TestNFSLockLifecycle(t *testing.T) {
	base := ensureNFSRootEnv(t)
	ctx := context.Background()
	root := prepareNFSRoot(t, base)
	cfg := buildNFSConfig(t, root, 0)
	cli := startNFSServer(t, cfg)
	key := "nfs-lifecycle-" + uuidv7.NewString()
	runLifecycleTest(t, ctx, cli, key, "nfs-worker")
}

func TestNFSAcquireForUpdateRandomPayloads(t *testing.T) {
	base := ensureNFSRootEnv(t)
	root := prepareNFSRoot(t, base)
	cfg := buildNFSConfig(t, root, 0)
	cli := startNFSServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	key := "nfs-for-update-random-" + uuidv7.NewString()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	const iterations = 30
	for i := 0; i < iterations; i++ {
		payload := map[string]any{
			"iteration": i,
			"value":     rng.Int63n(1 << 32),
			"text":      randomJSONSafeString(rng, 64),
			"time":      time.Now().UTC().Format(time.RFC3339Nano),
		}
		err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      "random",
			TTLSeconds: 30,
			BlockSecs:  lockdclient.BlockWaitForever,
		}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
			return af.Save(handlerCtx, payload)
		})
		if err != nil {
			t.Fatalf("iteration %d acquire-for-update: %v", i, err)
		}
	}

	verifier := acquireWithRetry(t, ctx, cli, key, "verify-random", 30, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verifier)
	if err != nil {
		t.Fatalf("verify state: %v", err)
	}
	if !releaseLease(t, ctx, verifier) {
		t.Fatalf("verify release failed")
	}
	if iter, ok := state["iteration"].(float64); !ok || int(iter) != iterations-1 {
		t.Fatalf("unexpected iteration in final state: %+v", state)
	}
}

func TestNFSAcquireForUpdateCallbackSingleServer(t *testing.T) {
	base := ensureNFSRootEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	watchdog := time.AfterFunc(15*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("TestNFSAcquireForUpdateCallbackSingleServer timeout:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	root := prepareNFSRoot(t, base)
	store, err := disk.New(disk.Config{Root: root})
	if err != nil {
		t.Fatalf("disk backend: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	chaos := &lockd.ChaosConfig{
		Seed:            2024,
		DisconnectAfter: 250 * time.Millisecond,
		MinDelay:        5 * time.Millisecond,
		MaxDelay:        40 * time.Millisecond,
		MaxDisconnects:  1,
	}

	serverOpts := []lockd.TestServerOption{
		lockd.WithTestBackend(store),
		lockd.WithTestChaos(chaos),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
			lockdclient.WithHTTPTimeout(750*time.Millisecond),
			lockdclient.WithFailureRetries(5),
		),
	}
	serverOpts = append(serverOpts, cryptotest.SharedMTLSOptions(t)...)
	ts := lockd.StartTestServer(t, serverOpts...)

	proxiedClient := ts.Client
	if proxiedClient == nil {
		var newErr error
		proxiedClient, newErr = ts.NewClient()
		if newErr != nil {
			t.Fatalf("client: %v", newErr)
		}
	}
	t.Cleanup(func() { _ = proxiedClient.Close() })

	key := "nfs-callback-" + uuidv7.NewString()
	seedPayload := map[string]any{"payload": "nfs-single", "count": 1}

	seedCli := directClient(t, ts)
	t.Cleanup(func() { _ = seedCli.Close() })
	ctxSeed, cancelSeed := context.WithTimeout(ctx, 2*time.Second)
	seedLease, err := seedCli.Acquire(ctxSeed, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		cancelSeed()
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedLease.Save(ctxSeed, seedPayload); err != nil {
		seedLease.Release(ctxSeed)
		cancelSeed()
		t.Fatalf("seed save: %v", err)
	}
	if err := seedLease.Release(ctxSeed); err != nil {
		cancelSeed()
		t.Fatalf("seed release: %v", err)
	}
	cancelSeed()

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
			if loadErr := af.Load(handlerCtx, &snapshot); loadErr != nil {
				return fmt.Errorf("decode snapshot: %w", errors.Join(err, loadErr))
			}
		}
		if snapshot["payload"] != "nfs-single" {
			return fmt.Errorf("unexpected payload: %+v", snapshot)
		}
		count := 0.0
		if v, ok := snapshot["count"].(float64); ok {
			count = v
		}
		targetOwner := "reader"
		targetCount := count + 1
		payload := map[string]any{"payload": "nfs-single", "count": targetCount, "owner": targetOwner}

		refreshProgress := func() (float64, bool, error) {
			var latest map[string]any
			if err := af.Load(handlerCtx, &latest); err != nil {
				return 0, false, err
			}
			snapshot = latest
			current := 0.0
			if v, ok := latest["count"].(float64); ok {
				current = v
			}
			owner := fmt.Sprint(latest["owner"])
			achieved := owner == targetOwner && current >= targetCount
			return current, achieved, nil
		}

		var saveErr error
		for attempt := 0; attempt < 4; attempt++ {
			if saveErr = af.Save(handlerCtx, payload); saveErr == nil {
				return nil
			}

			var apiErr *lockdclient.APIError
			if errors.As(saveErr, &apiErr) && apiErr.Response.ErrorCode == "version_conflict" {
				if current, achieved, loadErr := refreshProgress(); loadErr == nil {
					if achieved {
						return nil
					}
					payload["count"] = current + 1
					targetCount = current + 1
					continue
				} else {
					return fmt.Errorf("save state: %w (refresh: %v)", saveErr, loadErr)
				}
			}

			if !retryableTransportError(saveErr) {
				return fmt.Errorf("save state: %w", saveErr)
			}

			if current, achieved, loadErr := refreshProgress(); loadErr == nil {
				if achieved {
					return nil
				}
				payload["count"] = current + 1
				targetCount = current + 1
			}
			time.Sleep(50 * time.Millisecond * time.Duration(attempt+1))
		}
		return fmt.Errorf("save state: %w", saveErr)
	}, lockdclient.WithAcquireFailureRetries(3))
	if err != nil {
		t.Fatalf("acquire-for-update: %v", err)
	}
	if !handlerCalled {
		t.Fatalf("callback was not invoked")
	}

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer verifyCancel()
	verifyLease, err := proxiedClient.Acquire(verifyCtx, api.AcquireRequest{
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
	releaseLease(t, verifyCtx, verifyLease)
}

func runAutoKeyAcquireScenario(t *testing.T, base, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	root := prepareNFSRoot(t, base)
	cfg := buildNFSConfig(t, root, 0)
	cli := startNFSServer(t, cfg)

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

func runAutoKeyAcquireForUpdateScenario(t *testing.T, base, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	root := prepareNFSRoot(t, base)
	cfg := buildNFSConfig(t, root, 0)
	cli := startNFSServer(t, cfg)

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

func runAcquireForUpdateCallbackFailover(t *testing.T, phase failoverPhase, base string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	watchdog := time.AfterFunc(30*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("nfs failover timeout after 30s:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	root := prepareNFSRoot(t, base)
	store, err := disk.New(disk.Config{Root: root})
	if err != nil {
		t.Fatalf("disk backend: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	const disconnectAfter = 2 * time.Second
	chaos := &lockd.ChaosConfig{
		Seed:            1024 + int64(phase),
		DisconnectAfter: disconnectAfter,
		MinDelay:        5 * time.Millisecond,
		MaxDelay:        20 * time.Millisecond,
		MaxDisconnects:  1,
	}

	closeDefaults := lockd.WithTestCloseDefaults(
		lockd.WithDrainLeases(-1),
		lockd.WithShutdownTimeout(10*time.Second),
	)

	var sharedCreds lockd.TestMTLSCredentials
	if cryptotest.TestMTLSEnabled() {
		sharedCreds = cryptotest.SharedMTLSCredentials(t)
	}

	primaryOptions := []lockd.TestServerOption{
		lockd.WithTestBackend(store),
		lockd.WithTestChaos(chaos),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
			lockdclient.WithHTTPTimeout(time.Second),
		),
	}
	primaryOptions = append(primaryOptions, closeDefaults)
	primaryOptions = append(primaryOptions, cryptotest.SharedMTLSOptions(t, sharedCreds)...)
	primary := lockd.StartTestServer(t, primaryOptions...)
	backupOptions := append([]lockd.TestServerOption{closeDefaults}, cryptotest.SharedMTLSOptions(t, sharedCreds)...)
	backupOptions = append(backupOptions,
		lockd.WithTestBackend(store),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
			lockdclient.WithHTTPTimeout(time.Second),
		),
	)
	backup := lockd.StartTestServer(t, backupOptions...)

	failoverBlob := strings.Repeat("nfs-failover-", 32768)
	key := fmt.Sprintf("nfs-multi-%s-%s", phase.String(), uuidv7.NewString())
	seedCli := directClient(t, backup)
	t.Cleanup(func() { _ = seedCli.Close() })

	ctxSeed, cancelSeed := context.WithTimeout(context.Background(), time.Second)
	defer cancelSeed()
	lease, err := seedCli.Acquire(ctxSeed, api.AcquireRequest{
		Key:        key,
		Owner:      "seed",
		TTLSeconds: 25,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("seed acquire: %v", err)
	}
	seedPayload, _ := json.Marshal(map[string]any{"payload": failoverBlob, "count": 1})
	if _, err := lease.UpdateBytes(ctxSeed, seedPayload); err != nil {
		t.Fatalf("seed update: %v", err)
	}
	releaseLease(t, ctxSeed, lease)

	clientLogger, clientLogs := testlog.NewRecorder(t, pslog.TraceLevel)
	clientOptions := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(2 * time.Second),
		lockdclient.WithFailureRetries(5),
		lockdclient.WithDrainAwareShutdown(false),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientLogger),
	}
	if cryptotest.TestMTLSEnabled() {
		httpClient := cryptotest.RequireMTLSHTTPClient(t, sharedCreds)
		clientOptions = append(clientOptions, lockdclient.WithHTTPClient(httpClient))
	}
	failoverClient, err := lockdclient.NewWithEndpoints([]string{primary.URL(), backup.URL()}, clientOptions...)
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
			shutdownErr = primary.Server.ShutdownWithOptions(shutdownCtx,
				lockd.WithDrainLeases(8*time.Second),
				lockd.WithShutdownTimeout(10*time.Second),
			)
		})
		return shutdownErr
	}
	t.Cleanup(func() {
		if shutdownCancel != nil {
			shutdownCancel()
		}
	})

	var handlerCount atomic.Int64
	err = failoverClient.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "reader",
		TTLSeconds: 15,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		handlerCount.Add(1)
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
		snapshot["payload"] = failoverBlob
		snapshot["count"] = 2.0
		if err := af.Save(handlerCtx, snapshot); err != nil {
			var apiErr *lockdclient.APIError
			if phase == failoverAfterSave && errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "lease_required" {
				return err
			}
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
		if expectedConflict && errors.As(err, &apiErr) {
			switch apiErr.Response.ErrorCode {
			case "version_conflict", "lease_required":
				conflictObserved = true
				t.Logf("phase %s observed expected %s after failover: %v", phase, apiErr.Response.ErrorCode, err)
			default:
				t.Fatalf("acquire-for-update failover unexpected code %s: %v\n%s", apiErr.Response.ErrorCode, err, clientLogs.Summary())
			}
		} else if expectedConflict && errors.Is(err, context.DeadlineExceeded) {
			conflictObserved = true
			t.Logf("phase %s observed expected deadline after failover: %v", phase, err)
		} else {
			t.Fatalf("acquire-for-update failover: %v\n%s", err, clientLogs.Summary())
		}
	}
	if handlerCount.Load() == 0 {
		t.Fatalf("handler not called")
	}

	verify := acquireWithRetry(t, ctx, backup.Client, key, "failover-verify", 20, lockdclient.BlockWaitForever)
	state, _, _, err := getStateJSON(ctx, verify)
	if err != nil {
		t.Fatalf("verify get: %v", err)
	}
	releaseLease(t, ctx, verify)

	backupEndpoint := backup.URL()
	result := assertFailoverLogs(t, clientLogs, primary.URL(), backupEndpoint)
	if result.AlternateEndpoint != backupEndpoint {
		t.Fatalf("expected failover to backup %q, got %q\nlogs:\n%s", backupEndpoint, result.AlternateEndpoint, clientLogs.Summary())
	}
	if conflictObserved {
		if result.AlternateStatus != http.StatusConflict {
			t.Fatalf("expected HTTP 409 when phase %s conflicts, got %d\nlogs:\n%s", phase, result.AlternateStatus, clientLogs.Summary())
		}
	} else if result.AlternateStatus != http.StatusOK {
		t.Fatalf("expected HTTP 200 during phase %s failover, got %d\nlogs:\n%s", phase, result.AlternateStatus, clientLogs.Summary())
	}
	if !conflictObserved && state == nil {
		t.Fatalf("expected state after failover")
	}
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

	payload, _ := json.Marshal(map[string]any{"cursor": 42, "backend": "nfs"})
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

func randomJSONSafeString(rng *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func acquireWithRetry(tb testing.TB, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl, block int64) *lockdclient.LeaseSession {
	tb.Helper()
	var lastErr error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		sess, err := cli.Acquire(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      owner,
			TTLSeconds: ttl,
			BlockSecs:  block,
		})
		if err == nil {
			return sess
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	tb.Fatalf("acquire %s/%s failed: %v", key, owner, lastErr)
	return nil
}

func getStateJSON(ctx context.Context, sess *lockdclient.LeaseSession) (map[string]any, string, string, error) {
	if sess == nil {
		return nil, "", "", errors.New("nil session")
	}
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
	version := ""
	if snap.Version > 0 {
		version = strconv.FormatInt(snap.Version, 10)
	}
	return payload, snap.ETag, version, nil
}

func releaseLease(tb testing.TB, ctx context.Context, sess *lockdclient.LeaseSession) bool {
	tb.Helper()
	if sess == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := sess.Release(ctx); err != nil {
		tb.Fatalf("release: %v", err)
	}
	return true
}

func directClient(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
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

func assertFailoverLogs(t testing.TB, rec *testlog.Recorder, primary, backup string) failoverLogResult {
	const (
		successMsg = "client.http.success"
		errorMsg   = "client.http.error"
		aquireMsg  = "client.acquire_for_update.acquired"
	)
	result := failoverLogResult{}
	acquiredEntry, ok := rec.First(func(e testlog.Entry) bool {
		return e.Message == aquireMsg
	})
	if !ok {
		t.Fatalf("expected %s in logs; %s", aquireMsg, rec.Summary())
	}
	initialEndpoint := testlog.GetStringField(acquiredEntry, "endpoint")
	if initialEndpoint == "" {
		t.Fatalf("%s missing endpoint; %s", aquireMsg, rec.Summary())
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
		result.ErrorEndpoint = errorEndpoint
	}

	return result
}

type failoverLogResult struct {
	HadTransportError bool
	ErrorEndpoint     string
	AlternateEndpoint string
	AlternateStatus   int
}
