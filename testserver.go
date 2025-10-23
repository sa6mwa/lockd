package lockd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/logport"
	"pkt.systems/logport/adapters/psl"
)

// TestServer wraps a running lockd.Server with convenient handles for tests.
type TestServer struct {
	Server   *Server
	BaseURL  string
	Listener net.Addr
	Client   *client.Client
	Config   Config

	stop    func(context.Context) error
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
	lines := bytes.Split(p, []byte{'\n'})
	for _, line := range lines {
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
func (ts *TestServer) Stop(ctx context.Context) error {
	if ts == nil || ts.stop == nil {
		return nil
	}
	if ts.Client != nil {
		_ = ts.Client.Close()
	}
	if ts.proxy != nil {
		_ = ts.proxy.Close()
		ts.proxy = nil
	}
	return ts.stop(ctx)
}

// NewTestingLogger creates a logport logger that writes through testing.TB.
func NewTestingLogger(t testing.TB, level logport.Level) logport.ForLogging {
	writer := &testingWriter{t: t}
	t.Cleanup(writer.close)
	logger := psl.NewStructured(writer).WithLogLevel()
	if level != logport.NoLevel {
		logger = logger.LogLevel(level)
	}
	return logger.With("app", "testserver")
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
	options := make([]client.Option, 0, len(opts)+1)
	if !ts.Config.MTLS {
		options = append(options, client.WithMTLS(false))
	}
	options = append(options, opts...)
	return client.New(ts.BaseURL, options...)
}

type testServerOptions struct {
	cfg           Config
	cfgSet        bool
	mutators      []func(*Config)
	backend       storage.Backend
	logger        logport.ForLogging
	clientOpts    []client.Option
	disableClient bool
	startTimeout  time.Duration
	chaosConfig   *ChaosConfig
	testTB        testing.TB
	testLogLevel  logport.Level
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
func WithTestLogger(logger logport.ForLogging) TestServerOption {
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
func WithTestLoggerFromTB(t testing.TB, level logport.Level) TestServerOption {
	return func(o *testServerOptions) {
		o.testTB = t
		o.testLogLevel = level
	}
}

// WithTestLoggerTB uses the testing logger with Debug level.
func WithTestLoggerTB(t testing.TB) TestServerOption {
	return WithTestLoggerFromTB(t, logport.DebugLevel)
}

// NewTestServer starts a lockd server suitable for tests. Call Stop to clean up resources.
func NewTestServer(ctx context.Context, opts ...TestServerOption) (*TestServer, error) {
	options := testServerOptions{
		cfg: Config{
			Store:       "mem://",
			ListenProto: "tcp",
			Listen:      "127.0.0.1:0",
			MTLS:        false,
		},
		logger:       nil,
		startTimeout: 5 * time.Second,
		testLogLevel: logport.DebugLevel,
	}
	for _, opt := range opts {
		opt(&options)
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
			logger = logport.NoopLogger()
		}
	}

	ctxServer, cancel := context.WithCancel(context.Background())
	type startResult struct {
		srv  *Server
		stop func(context.Context) error
		err  error
	}
	resultCh := make(chan startResult, 1)
	backend := options.backend
	go func() {
		startOpts := []Option{WithLogger(logger)}
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
	stop := func(stopCtx context.Context) error {
		cancel()
		return originalStop(stopCtx)
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

	var cli *client.Client
	var baseClientOpts []client.Option
	if !options.disableClient {
		baseClientOpts = make([]client.Option, 0, len(options.clientOpts)+1)
		if !cfg.MTLS {
			baseClientOpts = append(baseClientOpts, client.WithMTLS(false))
		}
		baseClientOpts = append(baseClientOpts, options.clientOpts...)
		cli, err = client.New(baseURL, baseClientOpts...)
		if err != nil {
			_ = stop(context.Background())
			return nil, err
		}
	}

	ts := &TestServer{
		Server:   srv,
		BaseURL:  baseURL,
		Listener: addr,
		Client:   cli,
		Config:   cfg,
		stop:     stop,
		backend:  backend,
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
			cli, err = client.New(ts.BaseURL, baseClientOpts...)
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
		if cfg.MTLS {
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
	if c.MaxDisconnects < 0 {
		cfg.maxDisconnects = 0
	} else {
		cfg.maxDisconnects = c.MaxDisconnects
	}
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
