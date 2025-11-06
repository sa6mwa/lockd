package lockd

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/tlsutil"
	"pkt.systems/pslog"
)

const testMTLSEnv = "LOCKD_TEST_WITH_MTLS"

type testMTLSMode int

const (
	mtlsModeAuto testMTLSMode = iota
	mtlsModeForceOn
	mtlsModeForceOff
)

// TestServer wraps a running lockd.Server with convenient handles for tests.
type TestServer struct {
	Server         *Server
	BaseURL        string
	Listener       net.Addr
	Client         *client.Client
	Config         Config
	baseClientOpts []client.Option
	mtlsMaterial   *testMTLSMaterial
	extraClients   []*client.Client
	extraHTTP      []*http.Client

	stop    func(context.Context, ...CloseOption) error
	backend storage.Backend
	proxy   *chaosProxy
}

type testingWriter struct {
	t  testing.TB
	mu sync.Mutex
	// closed guards against writes after the associated test has finished.
	closed bool
}

func (w *testingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return len(p), nil
	}
	lines := bytes.SplitSeq(p, []byte{'\n'})
	for line := range lines {
		if len(line) == 0 {
			continue
		}
		w.t.Helper()
		func(entry string) {
			defer func() {
				if r := recover(); r != nil {
					msg := fmt.Sprint(r)
					if strings.Contains(msg, "Log in goroutine after") {
						return
					}
					if strings.Contains(msg, "Log in goroutine during concurrent Cleanups") {
						return
					}
					panic(r)
				}
			}()
			w.t.Log(entry)
		}(string(line))
	}
	w.mu.Unlock()
	return len(p), nil
}

func (w *testingWriter) close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
}

// Stop shuts down the server using the provided context.
func (ts *TestServer) Stop(ctx context.Context, opts ...CloseOption) error {
	if ts == nil || ts.stop == nil {
		return nil
	}
	if ts.Client != nil {
		_ = ts.Client.Close()
	}
	for _, cli := range ts.extraClients {
		_ = cli.Close()
	}
	ts.extraClients = nil
	for _, httpCli := range ts.extraHTTP {
		if tr, ok := httpCli.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
	ts.extraHTTP = nil
	if ts.proxy != nil {
		_ = ts.proxy.Close()
		ts.proxy = nil
	}
	return ts.stop(ctx, opts...)
}

// NewTestingLogger creates a pslog logger that writes through testing.TB.
func NewTestingLogger(t testing.TB, level pslog.Level) pslog.Logger {
	writer := &testingWriter{t: t}
	t.Cleanup(writer.close)
	logger := pslog.NewStructured(writer)
	if level != pslog.NoLevel {
		logger = logger.LogLevel(level)
		logger = logger.With("loglevel", pslog.LevelString(level))
	}
	return logger
}

// URL returns the base URL clients should use to reach the server.
func (ts *TestServer) URL() string {
	if ts == nil {
		return ""
	}
	return ts.BaseURL
}

// Addr returns the listener address the server is bound to.
func (ts *TestServer) Addr() net.Addr {
	if ts == nil {
		return nil
	}
	if ts.Listener != nil {
		return ts.Listener
	}
	if ts.Server != nil {
		return ts.Server.ListenerAddr()
	}
	return nil
}

// Backend exposes the storage backend used by the server.
func (ts *TestServer) Backend() storage.Backend {
	if ts == nil {
		return nil
	}
	return ts.backend
}

// NewClient returns a new client configured against the test server.
func (ts *TestServer) NewClient(opts ...client.Option) (*client.Client, error) {
	if ts == nil {
		return nil, fmt.Errorf("nil test server")
	}
	base := append([]client.Option(nil), ts.baseClientOpts...)
	if ts.mtlsMaterial != nil {
		httpClient, err := ts.mtlsMaterial.NewHTTPClient()
		if err != nil {
			return nil, err
		}
		base = append(base, client.WithHTTPClient(httpClient))
	}
	base = append(base, opts...)
	cli, err := client.New(ts.BaseURL, base...)
	if err != nil {
		return nil, err
	}
	ts.extraClients = append(ts.extraClients, cli)
	return cli, nil
}

// NewEndpointsClient returns a client configured with explicit endpoints while
// inheriting the test server defaults (mTLS, timeouts, logging, etc.).
func (ts *TestServer) NewEndpointsClient(endpoints []string, opts ...client.Option) (*client.Client, error) {
	if ts == nil {
		return nil, fmt.Errorf("nil test server")
	}
	base := append([]client.Option(nil), ts.baseClientOpts...)
	if ts.mtlsMaterial != nil {
		httpClient, err := ts.mtlsMaterial.NewHTTPClient()
		if err != nil {
			return nil, err
		}
		base = append(base, client.WithHTTPClient(httpClient))
	}
	base = append(base, opts...)
	cli, err := client.NewWithEndpoints(endpoints, base...)
	if err != nil {
		return nil, err
	}
	ts.extraClients = append(ts.extraClients, cli)
	return cli, nil
}

// NewHTTPClient returns a raw HTTP client configured for the test server's MTLS settings.
func (ts *TestServer) NewHTTPClient() (*http.Client, error) {
	if ts == nil {
		return nil, fmt.Errorf("nil test server")
	}
	if ts.mtlsMaterial == nil {
		client := &http.Client{Timeout: 5 * time.Second}
		ts.extraHTTP = append(ts.extraHTTP, client)
		return client, nil
	}
	httpClient, err := ts.mtlsMaterial.NewHTTPClient()
	if err != nil {
		return nil, err
	}
	ts.extraHTTP = append(ts.extraHTTP, httpClient)
	return httpClient, nil
}

// TestMTLSCredentials returns a clone of the MTLS material backing the test server (when enabled).
func (ts *TestServer) TestMTLSCredentials() TestMTLSCredentials {
	if ts == nil || ts.mtlsMaterial == nil {
		return TestMTLSCredentials{}
	}
	return TestMTLSCredentials{material: ts.mtlsMaterial.clone()}
}

type testServerOptions struct {
	cfg              Config
	cfgSet           bool
	mutators         []func(*Config)
	backend          storage.Backend
	logger           pslog.Logger
	clientOpts       []client.Option
	mtlsMaterial     *testMTLSMaterial
	mtlsProvided     bool
	disableClient    bool
	startTimeout     time.Duration
	chaosConfig      *ChaosConfig
	testTB           testing.TB
	testLogLevel     pslog.Level
	closeDefaults    []CloseOption
	closeDefaultsSet bool
	mtlsMode         testMTLSMode
}

// TestServerOption customises NewTestServer/StartTestServer behaviour.
type TestServerOption func(*testServerOptions)

// WithTestConfig provides an explicit Config to use. Missing fields will be
// defaulted during validation.
func WithTestConfig(cfg Config) TestServerOption {
	return func(o *testServerOptions) {
		o.cfg = cfg
		o.cfgSet = true
	}
}

// WithTestConfigFunc applies a mutation to the server configuration before start.
func WithTestConfigFunc(fn func(*Config)) TestServerOption {
	return func(o *testServerOptions) {
		if fn != nil {
			o.mutators = append(o.mutators, fn)
		}
	}
}

// WithTestListener overrides the listen protocol and address.
func WithTestListener(proto, address string) TestServerOption {
	return WithTestConfigFunc(func(cfg *Config) {
		cfg.ListenProto = proto
		cfg.Listen = address
	})
}

// WithTestUnixSocket configures the server to listen on the provided unix socket path.
func WithTestUnixSocket(path string) TestServerOption {
	return func(o *testServerOptions) {
		o.mutators = append(o.mutators, func(cfg *Config) {
			cfg.ListenProto = "unix"
			cfg.Listen = path
		})
	}
}

// WithTestStore sets the storage URL while still defaulting other values.
func WithTestStore(store string) TestServerOption {
	return WithTestConfigFunc(func(cfg *Config) {
		cfg.Store = store
	})
}

// WithTestBackend injects a pre-built backend (shared between servers if desired).
func WithTestBackend(backend storage.Backend) TestServerOption {
	return func(o *testServerOptions) {
		o.backend = backend
	}
}

// WithTestLogger supplies a custom logger.
func WithTestLogger(logger pslog.Logger) TestServerOption {
	return func(o *testServerOptions) {
		o.logger = logger
	}
}

// WithTestClientOptions appends client options used when auto-constructing the helper client.
func WithTestClientOptions(opts ...client.Option) TestServerOption {
	return func(o *testServerOptions) {
		o.clientOpts = append(o.clientOpts, opts...)
	}
}

// WithTestMTLSCredentials reuses the provided MTLS material for the test server.
func WithTestMTLSCredentials(creds TestMTLSCredentials) TestServerOption {
	return func(o *testServerOptions) {
		if o.mtlsProvided {
			return
		}
		if !creds.Valid() {
			return
		}
		shared := creds.cloneMaterial()
		if shared == nil {
			return
		}
		o.mtlsMaterial = shared
		o.mtlsProvided = true
		o.mutators = append(o.mutators, func(cfg *Config) {
			cfg.BundlePEM = append([]byte(nil), shared.serverBundle...)
			cfg.BundlePath = ""
			cfg.DisableMTLS = false
		})
	}
}

// WithoutTestClient disables automatic client creation.
func WithoutTestClient() TestServerOption {
	return func(o *testServerOptions) {
		o.disableClient = true
	}
}

// WithTestStartTimeout overrides the wait timeout when starting the server.
func WithTestStartTimeout(d time.Duration) TestServerOption {
	return func(o *testServerOptions) {
		o.startTimeout = d
	}
}

// WithTestChaos enables an in-process chaos proxy in front of the listener.
// Passing nil disables chaos behaviour.
func WithTestChaos(cfg *ChaosConfig) TestServerOption {
	return func(o *testServerOptions) {
		if cfg != nil {
			copyCfg := *cfg
			o.chaosConfig = &copyCfg
		} else {
			o.chaosConfig = nil
		}
	}
}

// WithTestLoggerFromTB routes server logs to the provided testing logger at the supplied level.
func WithTestLoggerFromTB(t testing.TB, level pslog.Level) TestServerOption {
	return func(o *testServerOptions) {
		o.testTB = t
		o.testLogLevel = level
	}
}

// WithTestLoggerTB uses the testing logger with Debug level.
func WithTestLoggerTB(t testing.TB) TestServerOption {
	return WithTestLoggerFromTB(t, pslog.DebugLevel)
}

// WithTestCloseDefaults overrides the shutdown CloseOptions applied to StartTestServer instances.
// Passing no options restores the production defaults (currently 8s drain / 10s overall).
func WithTestCloseDefaults(opts ...CloseOption) TestServerOption {
	return func(o *testServerOptions) {
		o.closeDefaults = append([]CloseOption(nil), opts...)
		o.closeDefaultsSet = true
	}
}

// WithTestMTLS forces StartTestServer to configure mutual TLS, regardless of the environment toggle.
func WithTestMTLS() TestServerOption {
	return func(o *testServerOptions) {
		o.mtlsMode = mtlsModeForceOn
	}
}

// WithoutTestMTLS disables automatic mTLS configuration for this test server.
func WithoutTestMTLS() TestServerOption {
	return func(o *testServerOptions) {
		o.mtlsMode = mtlsModeForceOff
	}
}

func defaultTestCloseDefaults() []CloseOption {
	return []CloseOption{
		WithDrainLeases(4 * time.Second),
		WithShutdownTimeout(5 * time.Second),
	}
}

// NewTestServer starts a lockd server suitable for tests. Call Stop to clean up resources.
func NewTestServer(ctx context.Context, opts ...TestServerOption) (*TestServer, error) {
	options := testServerOptions{
		cfg: Config{
			Store:                    "mem://",
			ListenProto:              "tcp",
			Listen:                   "127.0.0.1:0",
			DisableMTLS:              true,
			DisableStorageEncryption: true,
		},
		logger:       nil,
		startTimeout: 5 * time.Second,
		testLogLevel: pslog.DebugLevel,
	}
	for _, opt := range opts {
		opt(&options)
	}
	if !options.closeDefaultsSet {
		options.closeDefaults = defaultTestCloseDefaults()
	}

	cfg := options.cfg
	if !options.cfgSet {
		// ensure defaults for unset values remain applied even after mutators
		cfg.Store = defaultIfEmpty(cfg.Store, "mem://")
		cfg.ListenProto = defaultIfEmpty(cfg.ListenProto, "tcp")
		if cfg.Listen == "" {
			if cfg.ListenProto == "unix" {
				cfg.Listen = ""
			} else {
				cfg.Listen = "127.0.0.1:0"
			}
		}
	}
	for _, mut := range options.mutators {
		mut(&cfg)
	}
	if cfg.Store == "" {
		cfg.Store = "mem://"
	}
	if cfg.ListenProto == "" {
		cfg.ListenProto = "tcp"
	}
	if cfg.ListenProto != "unix" && cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:0"
	}

	logger := options.logger
	if logger == nil {
		if options.testTB != nil {
			logger = NewTestingLogger(options.testTB, options.testLogLevel)
		} else {
			logger = loggingutil.NoopLogger()
		}
	}

	var err error
	var mtlsMaterial *testMTLSMaterial
	options.clientOpts, mtlsMaterial, err = prepareTestClientOptions(&cfg, options.clientOpts, options.mtlsMode, options.mtlsMaterial)
	if err != nil {
		return nil, err
	}
	options.mtlsMaterial = mtlsMaterial

	ctxServer, cancel := context.WithCancel(context.Background())
	type startResult struct {
		srv  *Server
		stop func(context.Context, ...CloseOption) error
		err  error
	}
	resultCh := make(chan startResult, 1)
	backend := options.backend
	go func() {
		startOpts := []Option{WithLogger(logger)}
		if len(options.closeDefaults) > 0 {
			startOpts = append(startOpts, WithDefaultCloseOptions(options.closeDefaults...))
		}
		if backend != nil {
			startOpts = append(startOpts, WithBackend(backend))
		}
		srv, stop, err := StartServer(ctxServer, cfg, startOpts...)
		resultCh <- startResult{srv: srv, stop: stop, err: err}
	}()

	var (
		res     startResult
		timeout <-chan time.Time
		ctxDone <-chan struct{}
	)
	if options.startTimeout > 0 {
		timeout = time.After(options.startTimeout)
	}
	if ctx != nil {
		ctxDone = ctx.Done()
	}

	select {
	case res = <-resultCh:
	case <-timeout:
		cancel()
		res = <-resultCh
		if res.err == nil {
			res.err = fmt.Errorf("test server start timeout after %s", options.startTimeout)
		}
	case <-ctxDone:
		cancel()
		res = <-resultCh
		if res.err == nil {
			res.err = ctx.Err()
		}
	}
	if res.err != nil {
		return nil, res.err
	}
	srv := res.srv
	originalStop := res.stop
	stop := func(stopCtx context.Context, closeOpts ...CloseOption) error {
		cancel()
		return originalStop(stopCtx, closeOpts...)
	}

	addr := srv.ListenerAddr()
	if addr == nil {
		_ = stop(context.Background())
		return nil, fmt.Errorf("test server: listener not initialised")
	}

	baseURL, err := computeBaseURL(cfg, addr)
	if err != nil {
		_ = stop(context.Background())
		return nil, err
	}

	baseClientOpts := make([]client.Option, 0, len(options.clientOpts)+1)
	if !cfg.MTLSEnabled() {
		baseClientOpts = append(baseClientOpts, client.WithDisableMTLS(true))
	}
	baseClientOpts = append(baseClientOpts, options.clientOpts...)

	clientOptsForAuto := append([]client.Option(nil), baseClientOpts...)
	if options.mtlsMaterial != nil {
		httpClient, err := options.mtlsMaterial.NewHTTPClient()
		if err != nil {
			_ = stop(context.Background())
			return nil, err
		}
		clientOptsForAuto = append(clientOptsForAuto, client.WithHTTPClient(httpClient))
	}

	var cli *client.Client
	if !options.disableClient {
		cli, err = client.New(baseURL, clientOptsForAuto...)
		if err != nil {
			_ = stop(context.Background())
			return nil, err
		}
	}
	cfg = srv.cfg

	ts := &TestServer{
		Server:         srv,
		BaseURL:        baseURL,
		Listener:       addr,
		Client:         cli,
		Config:         cfg,
		stop:           stop,
		backend:        backend,
		baseClientOpts: baseClientOpts,
		mtlsMaterial:   options.mtlsMaterial,
	}

	if options.chaosConfig != nil {
		if strings.ToLower(cfg.ListenProto) != "tcp" {
			_ = stop(context.Background())
			return nil, fmt.Errorf("chaos proxy only supported for tcp listeners")
		}
		proxy, err := newChaosProxy(baseURL, options.chaosConfig)
		if err != nil {
			_ = stop(context.Background())
			return nil, err
		}
		ts.proxy = proxy
		portURL, err := url.Parse(baseURL)
		if err != nil {
			proxy.Close()
			_ = stop(context.Background())
			return nil, err
		}
		portURL.Host = proxy.Addr().String()
		ts.BaseURL = portURL.String()
		ts.Listener = proxy.Addr()
		if !options.disableClient {
			chaosClientOpts := append([]client.Option(nil), baseClientOpts...)
			if ts.mtlsMaterial != nil {
				httpClient, err := ts.mtlsMaterial.NewHTTPClient()
				if err != nil {
					proxy.Close()
					_ = stop(context.Background())
					return nil, err
				}
				chaosClientOpts = append(chaosClientOpts, client.WithHTTPClient(httpClient))
			}
			cli, err = client.New(ts.BaseURL, chaosClientOpts...)
			if err != nil {
				proxy.Close()
				_ = stop(context.Background())
				return nil, err
			}
			ts.Client = cli
		}
	}

	return ts, nil
}

// StartTestServer is a convenience wrapper that fails the test on error and registers cleanup.
func StartTestServer(t testing.TB, opts ...TestServerOption) *TestServer {
	t.Helper()
	ts, err := NewTestServer(context.Background(), opts...)
	if err != nil {
		t.Fatalf("start test server: %v", err)
	}
	t.Cleanup(func() {
		if err := ts.Stop(context.Background()); err != nil {
			t.Fatalf("stop test server: %v", err)
		}
	})
	return ts
}

func computeBaseURL(cfg Config, addr net.Addr) (string, error) {
	switch strings.ToLower(cfg.ListenProto) {
	case "unix":
		if cfg.Listen == "" {
			return "", fmt.Errorf("unix listener requires a socket path")
		}
		return "unix://" + cfg.Listen, nil
	default:
		host := addr.String()
		scheme := "http"
		if cfg.MTLSEnabled() {
			scheme = "https"
		}
		return fmt.Sprintf("%s://%s", scheme, host), nil
	}
}

func defaultIfEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// ChaosConfig describes network perturbations applied by the chaos proxy.
type ChaosConfig struct {
	// Seed controls the pseudo-random source. When zero, time.Now is used.
	Seed int64

	// MinDelay and MaxDelay bound per-chunk latency. When both zero no delay is added.
	MinDelay time.Duration
	MaxDelay time.Duration

	// DropProbability skips forwarding a chunk (0.0-1.0).
	DropProbability float64

	// ResetProbability closes both connections abruptly (0.0-1.0) evaluated per chunk.
	ResetProbability float64

	// DisconnectAfter closes the downstream connection after the specified duration (0 disables).
	DisconnectAfter time.Duration

	// BandwidthBytesPerSecond throttles throughput when >0.
	BandwidthBytesPerSecond int64

	// ChunkSize controls read/write batch size. Defaults to 32k if <=0.
	ChunkSize int

	// MaxDisconnects limits how many times DisconnectAfter is applied across connections (0 = unlimited).
	MaxDisconnects int
}

func (c *ChaosConfig) normalize() chaosRuntimeConfig {
	cfg := chaosRuntimeConfig{
		minDelay:        c.MinDelay,
		maxDelay:        c.MaxDelay,
		dropProb:        clampProbability(c.DropProbability),
		resetProb:       clampProbability(c.ResetProbability),
		disconnectAfter: c.DisconnectAfter,
		bandwidth:       c.BandwidthBytesPerSecond,
		chunkSize:       c.ChunkSize,
	}
	if cfg.chunkSize <= 0 {
		cfg.chunkSize = 32 << 10
	}
	if cfg.minDelay < 0 {
		cfg.minDelay = 0
	}
	if cfg.maxDelay < cfg.minDelay {
		cfg.maxDelay = cfg.minDelay
	}
	if cfg.maxDelay < 0 {
		cfg.maxDelay = 0
	}
	if c.Seed != 0 {
		cfg.seed = c.Seed
	} else {
		cfg.seed = time.Now().UnixNano()
	}
	cfg.maxDisconnects = max(c.MaxDisconnects, 0)
	return cfg
}

type chaosRuntimeConfig struct {
	minDelay        time.Duration
	maxDelay        time.Duration
	dropProb        float64
	resetProb       float64
	disconnectAfter time.Duration
	bandwidth       int64
	chunkSize       int
	seed            int64
	maxDisconnects  int
}

func (cfg chaosRuntimeConfig) ioDeadline() time.Duration {
	deadline := 500 * time.Millisecond
	if cfg.maxDelay > 0 {
		deadline = cfg.maxDelay*2 + 50*time.Millisecond
	}
	if cfg.disconnectAfter > 0 && cfg.disconnectAfter < deadline {
		deadline = cfg.disconnectAfter
	}
	if deadline <= 0 {
		deadline = 500 * time.Millisecond
	}
	return deadline
}

func clampProbability(v float64) float64 {
	if math.IsNaN(v) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

type chaosProxy struct {
	listener net.Listener
	remote   string
	cfg      chaosRuntimeConfig

	mu          sync.Mutex
	disconnects int
	closeOnce   sync.Once
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

func (cp *chaosProxy) shouldDisconnect() bool {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.cfg.disconnectAfter <= 0 {
		return false
	}
	if cp.cfg.maxDisconnects > 0 && cp.disconnects >= cp.cfg.maxDisconnects {
		return false
	}
	cp.disconnects++
	return true
}

func newChaosProxy(remoteURL string, config *ChaosConfig) (*chaosProxy, error) {
	cfg := (&ChaosConfig{}).normalize()
	if config != nil {
		cfg = config.normalize()
	}
	u, err := url.Parse(remoteURL)
	if err != nil {
		return nil, err
	}
	if u.Host == "" {
		return nil, fmt.Errorf("chaos proxy: missing remote host in %s", remoteURL)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	cp := &chaosProxy{
		listener: ln,
		remote:   u.Host,
		cfg:      cfg,
		stopCh:   make(chan struct{}),
	}
	cp.wg.Add(1)
	go cp.acceptLoop(u.Scheme)
	return cp, nil
}

func (cp *chaosProxy) Addr() net.Addr {
	return cp.listener.Addr()
}

func (cp *chaosProxy) Close() error {
	var err error
	cp.closeOnce.Do(func() {
		close(cp.stopCh)
		err = cp.listener.Close()
	})
	cp.wg.Wait()
	return err
}

func (cp *chaosProxy) acceptLoop(scheme string) {
	defer cp.wg.Done()
	for {
		conn, err := cp.listener.Accept()
		if err != nil {
			select {
			case <-cp.stopCh:
				return
			default:
			}
			continue
		}
		cp.wg.Add(1)
		go func(c net.Conn) {
			defer cp.wg.Done()
			cp.handleConnection(c, scheme)
		}(conn)
	}
}

func (cp *chaosProxy) handleConnection(downstream net.Conn, scheme string) {
	defer downstream.Close()
	dialTimeout := 200 * time.Millisecond
	upstream, err := net.DialTimeout("tcp", cp.remote, dialTimeout)
	if err != nil {
		return
	}
	defer upstream.Close()

	rngSrc := rand.NewSource(cp.cfg.seed ^ time.Now().UnixNano())
	rng := rand.New(rngSrc)

	if cp.cfg.resetProb > 0 && rng.Float64() < cp.cfg.resetProb {
		return
	}

	var disconnectCh <-chan time.Time
	if cp.shouldDisconnect() {
		timer := time.NewTimer(cp.cfg.disconnectAfter)
		defer timer.Stop()
		disconnectCh = timer.C
	}

	errCh := make(chan error, 2)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = downstream.SetDeadline(time.Now().Add(10 * time.Millisecond))
			_ = upstream.SetDeadline(time.Now().Add(10 * time.Millisecond))
		})
	}
	go cp.pipe(errCh, upstream, downstream, rng, disconnectCh)
	go cp.pipe(errCh, downstream, upstream, rng, disconnectCh)

	select {
	case <-cp.stopCh:
		stop()
	case <-disconnectCh:
		stop()
	case <-errCh:
		stop()
		<-errCh
	case <-errCh:
		stop()
		<-errCh
	}
}

func prepareTestClientOptions(cfg *Config, base []client.Option, mode testMTLSMode, shared *testMTLSMaterial) ([]client.Option, *testMTLSMaterial, error) {
	enable := shouldEnableTestMTLS(mode)
	if strings.EqualFold(cfg.ListenProto, "unix") {
		enable = false
	}
	if enable {
		if shared != nil {
			cloned := shared.clone()
			if len(cfg.BundlePEM) == 0 {
				cfg.BundlePEM = cloned.serverBundle
			}
			cfg.BundlePath = ""
			cfg.DisableMTLS = false
			if cfg.ListenProto == "" {
				cfg.ListenProto = "tcp"
			}
			if cfg.Listen == "" && strings.ToLower(cfg.ListenProto) != "unix" {
				cfg.Listen = "127.0.0.1:0"
			}
			return base, cloned, nil
		}
		if len(cfg.BundlePEM) == 0 && cfg.BundlePath == "" {
			hosts := gatherTestMTLSHosts(*cfg)
			material, err := newTestMTLSMaterial(hosts)
			if err != nil {
				return nil, nil, err
			}
			cfg.BundlePEM = material.serverBundle
			cfg.BundlePath = ""
			cfg.DisableMTLS = false
			if cfg.ListenProto == "" {
				cfg.ListenProto = "tcp"
			}
			if cfg.Listen == "" && strings.ToLower(cfg.ListenProto) != "unix" {
				cfg.Listen = "127.0.0.1:0"
			}
			return base, material, nil
		}
		cfg.DisableMTLS = false
		if cfg.ListenProto == "" {
			cfg.ListenProto = "tcp"
		}
		if cfg.Listen == "" && strings.ToLower(cfg.ListenProto) != "unix" {
			cfg.Listen = "127.0.0.1:0"
		}
		return base, nil, nil
	}
	if cfg.ListenProto == "" {
		cfg.ListenProto = "tcp"
	}
	if cfg.Listen == "" && strings.ToLower(cfg.ListenProto) != "unix" {
		cfg.Listen = "127.0.0.1:0"
	}
	cfg.DisableMTLS = true
	cfg.BundlePEM = nil
	return base, nil, nil
}

func shouldEnableTestMTLS(mode testMTLSMode) bool {
	switch mode {
	case mtlsModeForceOn:
		return true
	case mtlsModeForceOff:
		return false
	default:
		v := strings.TrimSpace(os.Getenv(testMTLSEnv))
		if v == "" {
			return true
		}
		switch strings.ToLower(v) {
		case "0", "false", "off", "no":
			return false
		default:
			return true
		}
	}
}

type testMTLSMaterial struct {
	serverBundle []byte
	clientBundle []byte
	clientParsed *tlsutil.ClientBundle
}

func (m *testMTLSMaterial) clone() *testMTLSMaterial {
	if m == nil {
		return nil
	}
	server := append([]byte(nil), m.serverBundle...)
	client := append([]byte(nil), m.clientBundle...)
	parsed, err := tlsutil.LoadClientBundleFromBytes(client)
	if err != nil {
		return nil
	}
	return &testMTLSMaterial{
		serverBundle: server,
		clientBundle: client,
		clientParsed: parsed,
	}
}

// TestMTLSCredentials captures ephemeral test-only MTLS material (server bundle + client credentials).
type TestMTLSCredentials struct {
	material *testMTLSMaterial
}

// Valid reports whether the credentials contain MTLS material.
func (c TestMTLSCredentials) Valid() bool {
	return c.material != nil
}

// ServerBundle returns a copy of the PEM-encoded server bundle associated with the credentials.
func (c TestMTLSCredentials) ServerBundle() []byte {
	if c.material == nil {
		return nil
	}
	return append([]byte(nil), c.material.serverBundle...)
}

// NewHTTPClient constructs an HTTP client configured for MTLS using the embedded client bundle.
func (c TestMTLSCredentials) NewHTTPClient() (*http.Client, error) {
	if c.material == nil {
		return nil, fmt.Errorf("test mtls credentials: empty material")
	}
	return c.material.clone().NewHTTPClient()
}

func (c TestMTLSCredentials) cloneMaterial() *testMTLSMaterial {
	if c.material == nil {
		return nil
	}
	return c.material.clone()
}

// NewTestMTLSCredentialsFromBundles constructs test MTLS credentials using the provided
// server and client bundles.
func NewTestMTLSCredentialsFromBundles(serverBundle, clientBundle []byte) (TestMTLSCredentials, error) {
	if len(serverBundle) == 0 {
		return TestMTLSCredentials{}, fmt.Errorf("test mtls credentials: server bundle required")
	}
	if len(clientBundle) == 0 {
		return TestMTLSCredentials{}, fmt.Errorf("test mtls credentials: client bundle required")
	}
	parsed, err := tlsutil.LoadClientBundleFromBytes(clientBundle)
	if err != nil {
		return TestMTLSCredentials{}, fmt.Errorf("test mtls credentials: parse client bundle: %w", err)
	}
	material := &testMTLSMaterial{
		serverBundle: append([]byte(nil), serverBundle...),
		clientBundle: append([]byte(nil), clientBundle...),
		clientParsed: parsed,
	}
	return TestMTLSCredentials{material: material}, nil
}

func newTestMTLSMaterial(hosts []string) (*testMTLSMaterial, error) {
	ca, err := tlsutil.GenerateCA("lockd-test-ca", 365*24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("mtls: generate ca: %w", err)
	}
	caBundle, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("mtls: encode ca bundle: %w", err)
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		return nil, fmt.Errorf("mtls: derive metadata material: %w", err)
	}
	dedupedHosts := dedupeHosts(hosts)
	serverCert, serverKey, err := ca.IssueServer(dedupedHosts, "lockd-test-server", 365*24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("mtls: issue server cert: %w", err)
	}
	serverBundle, err := tlsutil.EncodeServerBundle(ca.CertPEM, nil, serverCert, serverKey, nil)
	if err != nil {
		return nil, fmt.Errorf("mtls: encode server bundle: %w", err)
	}
	augmentedServerBundle, err := cryptoutil.ApplyMetadataMaterial(serverBundle, material)
	if err != nil {
		return nil, fmt.Errorf("mtls: augment server bundle: %w", err)
	}
	clientCert, clientKey, err := ca.IssueClient("lockd-test-client", 365*24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("mtls: issue client cert: %w", err)
	}
	clientBundle, err := tlsutil.EncodeClientBundle(ca.CertPEM, clientCert, clientKey)
	if err != nil {
		return nil, fmt.Errorf("mtls: encode client bundle: %w", err)
	}
	clientParsed, err := tlsutil.LoadClientBundleFromBytes(clientBundle)
	if err != nil {
		return nil, fmt.Errorf("mtls: parse client bundle: %w", err)
	}
	return &testMTLSMaterial{
		serverBundle: augmentedServerBundle,
		clientBundle: clientBundle,
		clientParsed: clientParsed,
	}, nil
}

func (m *testMTLSMaterial) NewHTTPClient() (*http.Client, error) {
	if m == nil {
		return http.DefaultClient, nil
	}
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("mtls: default transport is %T, want *http.Transport", http.DefaultTransport)
	}
	transport := baseTransport.Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      m.clientParsed.CAPool,
		Certificates: []tls.Certificate{m.clientParsed.Certificate},
	}
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}, nil
}

func dedupeHosts(hosts []string) []string {
	seen := make(map[string]struct{}, len(hosts))
	out := make([]string, 0, len(hosts))
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		host = strings.ToLower(host)
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	return out
}

func gatherTestMTLSHosts(cfg Config) []string {
	hosts := []string{"*", "127.0.0.1", "localhost"}
	if cfg.Listen != "" {
		if h, _, err := net.SplitHostPort(cfg.Listen); err == nil && h != "" && h != "0.0.0.0" && h != "::" && h != "[::]" {
			hosts = append(hosts, h)
		}
	}
	return hosts
}

func (cp *chaosProxy) pipe(errCh chan<- error, dst net.Conn, src net.Conn, rng *rand.Rand, disconnectCh <-chan time.Time) {
	buf := make([]byte, cp.cfg.chunkSize)
	readDeadline := cp.cfg.ioDeadline()
	defer src.SetReadDeadline(time.Time{})
	defer dst.SetWriteDeadline(time.Time{})
	for {
		select {
		case <-cp.stopCh:
			errCh <- io.EOF
			return
		default:
		}
		now := time.Now()
		_ = src.SetReadDeadline(now.Add(readDeadline))
		_ = dst.SetWriteDeadline(now.Add(readDeadline))
		n, err := src.Read(buf)
		if n > 0 {
			if cp.cfg.dropProb > 0 && rng.Float64() < cp.cfg.dropProb {
				continue
			}
			if cp.cfg.maxDelay > 0 {
				delay := cp.cfg.minDelay
				if cp.cfg.maxDelay > cp.cfg.minDelay {
					delta := cp.cfg.maxDelay - cp.cfg.minDelay
					delay += time.Duration(rng.Int63n(int64(delta) + 1))
				}
				time.Sleep(delay)
			}
			if cp.cfg.bandwidth > 0 {
				expected := time.Duration(float64(n) / float64(cp.cfg.bandwidth) * float64(time.Second))
				if expected > 0 {
					time.Sleep(expected)
				}
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				errCh <- werr
				return
			}
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-cp.stopCh:
					errCh <- io.EOF
					return
				default:
				}
				continue
			}
			if err != io.EOF {
				errCh <- err
			} else {
				errCh <- nil
			}
			return
		}
		select {
		case <-cp.stopCh:
			errCh <- io.EOF
			return
		default:
		}
	}
}
