package lockd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func mustCreateMTLSTestBundles(t *testing.T) ([]byte, []byte) {
	t.Helper()
	caBundle, err := CreateCABundle(CreateCABundleRequest{
		CommonName: "lockd-test-ca",
	})
	if err != nil {
		t.Fatalf("create ca bundle: %v", err)
	}
	serverBundle, err := CreateServerBundle(CreateServerBundleRequest{
		CABundlePEM: caBundle,
		CommonName:  "lockd-test-server",
		Hosts:       []string{"127.0.0.1", "localhost"},
		NodeID:      "test-node",
	})
	if err != nil {
		t.Fatalf("create server bundle: %v", err)
	}
	clientBundle, err := CreateClientBundle(CreateClientBundleRequest{
		CABundlePEM: caBundle,
		CommonName:  "lockd-test-client",
	})
	if err != nil {
		t.Fatalf("create client bundle: %v", err)
	}
	return serverBundle, clientBundle
}

type sweeperClock struct {
	mu  sync.Mutex
	now time.Time
}

func newSweeperClock(start time.Time) clock.Clock {
	return &sweeperClock{now: start}
}

func (c *sweeperClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *sweeperClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- now
	return ch
}

func (c *sweeperClock) Sleep(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func waitFor(t *testing.T, timeout, interval time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(interval)
	}
}

type drainCapture struct {
	mu     sync.Mutex
	policy DrainLeasesPolicy
	calls  int
}

func (d *drainCapture) run(_ context.Context, policy DrainLeasesPolicy) drainSummary {
	d.mu.Lock()
	d.policy = policy
	d.calls++
	d.mu.Unlock()
	elapsed := max(policy.GracePeriod, 0)
	return drainSummary{ActiveAtStart: 1, Remaining: 0, Elapsed: elapsed}
}

func (d *drainCapture) Policy() DrainLeasesPolicy {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.policy
}

type httpShutdownCapture struct {
	mu  sync.Mutex
	ctx context.Context
}

func (h *httpShutdownCapture) fn(ctx context.Context) error {
	h.mu.Lock()
	h.ctx = ctx
	h.mu.Unlock()
	if ctx == nil {
		return http.ErrServerClosed
	}
	if err := ctx.Err(); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		return err
	}
	return http.ErrServerClosed
}

func (h *httpShutdownCapture) Deadline() (time.Time, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx == nil {
		return time.Time{}, false
	}
	return h.ctx.Deadline()
}

type listenerCloseCapture struct {
	closeErr   error
	closeCalls int
}

type backendCloseCapture struct {
	storage.Backend
	closeCalls int
}

func (l *listenerCloseCapture) Accept() (net.Conn, error) {
	return nil, errors.New("accept not supported in test listener")
}

func (l *listenerCloseCapture) Close() error {
	l.closeCalls++
	return l.closeErr
}

func (l *listenerCloseCapture) Addr() net.Addr {
	return &net.TCPAddr{}
}

func (b *backendCloseCapture) Close() error {
	b.closeCalls++
	if b.Backend != nil {
		return b.Backend.Close()
	}
	return nil
}

func newShutdownHarness(t *testing.T) (*Server, *drainCapture, *httpShutdownCapture) {
	t.Helper()
	srv := &Server{
		cfg:              Config{},
		logger:           pslog.NoopLogger(),
		backend:          memory.New(),
		httpSrv:          &http.Server{},
		clock:            clock.Real{},
		readyCh:          make(chan struct{}),
		defaultCloseOpts: defaultCloseOptions(),
	}
	close(srv.readyCh)
	drainCap := &drainCapture{}
	srv.drainFn = drainCap.run
	httpCap := &httpShutdownCapture{}
	srv.httpShutdown = httpCap.fn
	return srv, drainCap, httpCap
}

func TestAbortDoesNotCloseBackend(t *testing.T) {
	backend := &backendCloseCapture{Backend: memory.New()}
	t.Cleanup(func() { _ = backend.Backend.Close() })

	srv := &Server{
		logger:  pslog.NoopLogger(),
		backend: backend,
		readyCh: make(chan struct{}),
		clock:   clock.Real{},
	}
	close(srv.readyCh)

	if err := srv.Abort(context.Background()); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if backend.closeCalls != 0 {
		t.Fatalf("expected abort to avoid closing backend, got %d close calls", backend.closeCalls)
	}
}

func TestAbortPreservesCrashExpiryForDiskSingleToAuto(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")

	cfgSingle := Config{
		Store:                    (&url.URL{Scheme: "disk", Path: root}).String(),
		ListenProto:              "tcp",
		Listen:                   "127.0.0.1:0",
		DisableMTLS:              true,
		DisableStorageEncryption: true,
		HAMode:                   "single",
	}
	single := StartTestServer(t, WithTestConfig(cfgSingle), WithoutTestMTLS())

	cfgAuto := cfgSingle
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := StartTestServer(t, WithTestConfig(cfgAuto), WithoutTestMTLS())

	passiveDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(passiveDeadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		lease, err := auto.Client.Acquire(ctx, api.AcquireRequest{
			Key:        "abort-crash-passive",
			Owner:      "probe",
			TTLSeconds: 5,
			BlockSecs:  client.BlockNoWait,
		})
		cancel()
		if err != nil {
			var apiErr *client.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "node_passive" {
				break
			}
		} else {
			_ = lease.Release(context.Background())
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := single.Abort(context.Background()); err != nil {
		t.Fatalf("abort single server: %v", err)
	}

	activeDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(activeDeadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		lease, err := auto.Client.Acquire(ctx, api.AcquireRequest{
			Key:        "abort-crash-active",
			Owner:      "probe",
			TTLSeconds: 5,
			BlockSecs:  client.BlockNoWait,
		})
		cancel()
		if err == nil {
			_ = lease.Release(context.Background())
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("expected auto server to become active after abrupt single-server abort")
}

func durationAlmostEqual(t *testing.T, got, want, tolerance time.Duration) {
	t.Helper()
	delta := got - want
	if delta < 0 {
		delta = -delta
	}
	if delta > tolerance {
		t.Fatalf("duration mismatch: got %v want %v (±%v)", got, want, tolerance)
	}
}

func TestSweeperClearsExpiredLeases(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	start := time.Unix(1_700_000_000, 0)
	expired := start.Add(-2 * time.Hour)
	namespace := namespaces.Default
	key := "alpha"
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "L-1",
			Owner:         "worker",
			ExpiresAtUnix: expired.Unix(),
		},
		Version: 1,
	}
	if _, err := store.StoreMeta(ctx, namespace, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	bucket := expired.UTC().Format("2006010215")
	bucketList, err := json.Marshal([]string{bucket})
	if err != nil {
		t.Fatalf("marshal bucket list: %v", err)
	}
	if _, err := store.PutObject(ctx, namespace, path.Join(".lease-index", "buckets.json"), bytes.NewReader(bucketList), storage.PutObjectOptions{
		ContentType: storage.ContentTypeJSON,
	}); err != nil {
		t.Fatalf("store lease index buckets: %v", err)
	}
	if _, err := store.PutObject(ctx, namespace, path.Join(".lease-index", bucket, key), bytes.NewReader([]byte("{}")), storage.PutObjectOptions{
		ContentType: storage.ContentTypeJSON,
	}); err != nil {
		t.Fatalf("store lease index entry: %v", err)
	}
	cfg := Config{
		Store:                    "mem://",
		DisableMTLS:              true,
		DisableStorageEncryption: true,
		SweeperInterval:          time.Second,
		IdleSweepGrace:           time.Nanosecond,
		IdleSweepOpDelay:         time.Nanosecond,
		IdleSweepMaxOps:          10,
		IdleSweepMaxRuntime:      time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	srv, err := NewServer(cfg,
		WithBackend(store),
		WithClock(newSweeperClock(start)),
		WithLogger(pslog.NoopLogger()),
	)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.startSweeper()
	defer srv.stopSweeper()
	// Allow sweeper goroutine to run at least once.
	time.Sleep(10 * time.Millisecond)
	updatedRes, err := store.LoadMeta(ctx, namespace, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	updated := updatedRes.Meta
	if updated.Lease != nil {
		t.Fatalf("expected sweeper to clear lease, still present: %+v", updated.Lease)
	}
}

func TestShutdownBlocksAcquireDuringDrain(t *testing.T) {
	ctx := context.Background()
	serverBundle, clientBundle := mustCreateMTLSTestBundles(t)
	cfg := Config{Store: "mem://", Listen: "127.0.0.1:0", BundlePEM: serverBundle}
	handle, err := StartServer(ctx, cfg)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	srv := handle.Server
	stop := handle.Stop
	addr := srv.ListenerAddr()
	if addr == nil {
		t.Fatal("listener address not available")
	}
	cli, err := client.New("https://"+addr.String(), client.WithBundlePEM(clientBundle))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()

	lease, err := cli.Acquire(ctx, api.AcquireRequest{Key: "alpha", Owner: "worker", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	defer lease.Close()

	done := make(chan error, 1)
	go func() {
		done <- stop(context.Background(), WithDrainLeases(2*time.Second))
	}()

	waitFor(t, time.Second, 10*time.Millisecond, func() bool {
		draining, _, _ := srv.shutdownState()
		return draining
	})

	acqCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	_, err = cli.Acquire(acqCtx, api.AcquireRequest{Key: "beta", Owner: "worker", TTLSeconds: 5, BlockSecs: api.BlockNoWait})
	if err == nil {
		t.Fatalf("expected acquire to fail during drain")
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T", err)
	}
	if apiErr.Status != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", apiErr.Status)
	}
	if apiErr.Response.ErrorCode != "shutdown_draining" {
		t.Fatalf("expected error code shutdown_draining, got %q", apiErr.Response.ErrorCode)
	}

	_ = lease.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete in time")
	}
}

func TestShutdownAutoReleasesLeases(t *testing.T) {
	ctx := context.Background()
	serverBundle, clientBundle := mustCreateMTLSTestBundles(t)
	cfg := Config{Store: "mem://", Listen: "127.0.0.1:0", BundlePEM: serverBundle}
	handle, err := StartServer(ctx, cfg)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	srv := handle.Server
	stop := handle.Stop
	addr := srv.ListenerAddr()
	if addr == nil {
		t.Fatal("listener address not available")
	}
	cli, err := client.New("https://"+addr.String(), client.WithBundlePEM(clientBundle))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()

	lease, err := cli.Acquire(ctx, api.AcquireRequest{Key: "gamma", Owner: "worker", TTLSeconds: 60})
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	defer func() { _ = lease.Close() }()

	done := make(chan error, 1)
	go func() {
		done <- stop(context.Background(), WithDrainLeases(time.Second))
	}()

	waitFor(t, time.Second, 10*time.Millisecond, func() bool {
		draining, _, _ := srv.shutdownState()
		return draining
	})

	if _, err := lease.KeepAlive(ctx, 30*time.Second); err != nil {
		t.Fatalf("keepalive during shutdown: %v", err)
	}
	namespace := srv.cfg.DefaultNamespace

	waitFor(t, 2*time.Second, 20*time.Millisecond, func() bool {
		metaRes, err := srv.backend.LoadMeta(ctx, namespace, "gamma")
		if err != nil {
			return false
		}
		return metaRes.Meta.Lease == nil
	})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete in time")
	}

}

func TestShutdownDefaultSplit(t *testing.T) {
	srv, drainCap, httpCap := newShutdownHarness(t)
	if err := srv.ShutdownWithOptions(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	policy := drainCap.Policy()
	if policy.GracePeriod != 8*time.Second {
		t.Fatalf("expected default grace 8s, got %v", policy.GracePeriod)
	}
	deadline, ok := httpCap.Deadline()
	if !ok {
		t.Fatalf("expected http shutdown context to have deadline")
	}
	remaining := time.Until(deadline)
	durationAlmostEqual(t, remaining, 2*time.Second, 150*time.Millisecond)
}

func TestShutdownRespectsDrainOverride(t *testing.T) {
	srv, drainCap, httpCap := newShutdownHarness(t)
	if err := srv.ShutdownWithOptions(context.Background(), WithDrainLeases(5*time.Second)); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	policy := drainCap.Policy()
	if policy.GracePeriod != 5*time.Second {
		t.Fatalf("expected grace 5s, got %v", policy.GracePeriod)
	}
	deadline, ok := httpCap.Deadline()
	if !ok {
		t.Fatalf("expected deadline for http shutdown")
	}
	remaining := time.Until(deadline)
	durationAlmostEqual(t, remaining, 5*time.Second, 150*time.Millisecond)
}

func TestShutdownTimeoutOverride(t *testing.T) {
	srv, drainCap, httpCap := newShutdownHarness(t)
	if err := srv.ShutdownWithOptions(context.Background(), WithShutdownTimeout(4*time.Second)); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	policy := drainCap.Policy()
	expectedGrace := time.Duration(float64(4*time.Second) * drainShutdownSplit)
	if policy.GracePeriod != expectedGrace {
		t.Fatalf("expected grace %v, got %v", expectedGrace, policy.GracePeriod)
	}
	deadline, ok := httpCap.Deadline()
	if !ok {
		t.Fatalf("expected deadline for http shutdown")
	}
	remaining := time.Until(deadline)
	expectedHTTP := 4*time.Second - expectedGrace
	durationAlmostEqual(t, remaining, expectedHTTP, 150*time.Millisecond)
}

func TestShutdownTimeoutDisabled(t *testing.T) {
	srv, drainCap, httpCap := newShutdownHarness(t)
	if err := srv.ShutdownWithOptions(context.Background(), WithShutdownTimeout(0)); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	policy := drainCap.Policy()
	if policy.GracePeriod != 10*time.Second {
		t.Fatalf("expected grace 10s when timeout disabled, got %v", policy.GracePeriod)
	}
	if _, ok := httpCap.Deadline(); ok {
		t.Fatalf("did not expect http shutdown deadline when timeout disabled")
	}
}

func TestShutdownIgnoresClosedListenerError(t *testing.T) {
	srv, _, _ := newShutdownHarness(t)
	var logBuf bytes.Buffer
	srv.logger = pslog.NewWithOptions(context.Background(), &logBuf, pslog.Options{
		Mode:             pslog.ModeStructured,
		DisableTimestamp: true,
		NoColor:          true,
		MinLevel:         pslog.DebugLevel,
	})
	listener := &listenerCloseCapture{closeErr: net.ErrClosed}
	srv.listener = listener

	if err := srv.ShutdownWithOptions(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if listener.closeCalls != 1 {
		t.Fatalf("expected listener to be closed once, got %d", listener.closeCalls)
	}
	if strings.Contains(logBuf.String(), "shutdown.listener.close_error") {
		t.Fatalf("expected no warning for net.ErrClosed listener close: %s", logBuf.String())
	}
}
