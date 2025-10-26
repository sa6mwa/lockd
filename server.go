package lockd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/kryptograf/keymgmt"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/httpapi"
	"pkt.systems/lockd/internal/lsf"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/storage"
	loggingbackend "pkt.systems/lockd/internal/storage/logging"
	"pkt.systems/lockd/internal/storage/retry"
	"pkt.systems/lockd/internal/tlsutil"
	"pkt.systems/logport"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Server wraps the HTTP server, storage backend, and supporting components.
type Server struct {
	cfg          Config
	logger       logport.ForLogging
	backend      storage.Backend
	handler      *httpapi.Handler
	httpSrv      *http.Server
	httpShutdown func(context.Context) error
	listener     net.Listener
	socketPath   string
	clock        clock.Clock
	telemetry    *telemetryBundle
	lastServeErr error

	mu          sync.Mutex
	shutdown    bool
	sweeperStop chan struct{}
	sweeperDone sync.WaitGroup
	readyOnce   sync.Once
	readyCh     chan struct{}

	qrfController *qrf.Controller
	lsfObserver   *lsf.Observer
	lsfCancel     context.CancelFunc

	draining      atomic.Bool
	drainDeadline atomic.Int64
	drainMetrics  struct {
		once      sync.Once
		active    metric.Int64Histogram
		remaining metric.Int64Histogram
		duration  metric.Float64Histogram
		err       error
	}
	drainNotifyClients atomic.Bool
	defaultCloseOpts   closeOptions
	drainFn            func(context.Context, DrainLeasesPolicy) drainSummary
}

type leaseSnapshot struct {
	Key       string
	Owner     string
	LeaseID   string
	ExpiresAt int64
}

type drainSummary struct {
	ActiveAtStart int
	Remaining     int
	Elapsed       time.Duration
}

// Option configures server instances.
type Option func(*options)

type options struct {
	Logger        logport.ForLogging
	Backend       storage.Backend
	Clock         clock.Clock
	OTLPEndpoint  string
	configHooks   []func(*Config)
	closeDefaults []CloseOption
}

// WithLogger supplies a custom logger.
func WithLogger(l logport.ForLogging) Option {
	return func(o *options) {
		o.Logger = l
	}
}

// WithBackend injects a pre-built backend (useful for tests).
func WithBackend(b storage.Backend) Option {
	return func(o *options) {
		o.Backend = b
	}
}

// WithClock injects a custom clock implementation.
func WithClock(c clock.Clock) Option {
	return func(o *options) {
		o.Clock = c
	}
}

// WithOTLPEndpoint overrides the OTLP collector endpoint used for telemetry.
func WithOTLPEndpoint(endpoint string) Option {
	return func(o *options) {
		o.OTLPEndpoint = endpoint
	}
}

// WithLSFLogInterval overrides the cadence for lockd.lsf.sample telemetry logs; use 0 to disable logging.
func WithLSFLogInterval(interval time.Duration) Option {
	return func(o *options) {
		o.configHooks = append(o.configHooks, func(cfg *Config) {
			cfg.LSFLogInterval = interval
			cfg.LSFLogIntervalSet = true
		})
	}
}

// WithDefaultCloseOptions sets the server-wide defaults applied to Close/Shutdown calls.
func WithDefaultCloseOptions(opts ...CloseOption) Option {
	return func(o *options) {
		o.closeDefaults = append(o.closeDefaults, opts...)
	}
}

// CloseOption customises server shutdown semantics.
type CloseOption func(*closeOptions)

type closeOptions struct {
	drain              DrainLeasesPolicy
	drainSet           bool
	shutdownTimeout    time.Duration
	shutdownTimeoutSet bool
}

const drainShutdownSplit = 0.8

// DrainLeasesPolicy describes how the server should attempt to let existing lease
// holders finish work before the HTTP server stops accepting new connections.
type DrainLeasesPolicy struct {
	// GracePeriod defines how long the server should keep serving requests
	// (while denying new leases) before beginning the HTTP shutdown. Zero skips
	// the grace window.
	GracePeriod time.Duration

	// ForceRelease toggles metadata rewrites that explicitly clear outstanding
	// leases when the grace period elapses. This is experimental and disabled by
	// default.
	ForceRelease bool

	// NotifyClients controls whether the server surfaces Shutdown-Imminent
	// headers while draining so clients can release proactively.
	NotifyClients bool
}

// WithDrainLeases configures the shutdown grace period used to let existing
// lease holders flush state. Passing a negative duration disables draining.
func WithDrainLeases(grace time.Duration) CloseOption {
	return WithDrainLeasesPolicy(DrainLeasesPolicy{
		GracePeriod:   grace,
		NotifyClients: true,
	})
}

// WithDrainLeasesPolicy applies a full drain policy to shutdown calls.
func WithDrainLeasesPolicy(policy DrainLeasesPolicy) CloseOption {
	return func(opts *closeOptions) {
		opts.drain = policy.normalized()
		opts.drainSet = true
	}
}

// WithShutdownTimeout caps the total time allowed for drain plus HTTP shutdown.
// Zero disables the explicit cap and relies solely on the provided context.
func WithShutdownTimeout(d time.Duration) CloseOption {
	return func(opts *closeOptions) {
		if d <= 0 {
			opts.shutdownTimeout = 0
			opts.shutdownTimeoutSet = true
			return
		}
		opts.shutdownTimeout = d
		opts.shutdownTimeoutSet = true
	}
}

func defaultCloseOptions() closeOptions {
	return closeOptions{
		drain: DrainLeasesPolicy{
			GracePeriod:   10 * time.Second,
			NotifyClients: true,
		},
		drainSet:           true,
		shutdownTimeout:    10 * time.Second,
		shutdownTimeoutSet: true,
	}
}

func resolveCloseOptions(base closeOptions, input []CloseOption) closeOptions {
	opts := base
	for _, apply := range input {
		if apply == nil {
			continue
		}
		apply(&opts)
	}
	if !opts.drainSet {
		def := defaultCloseOptions().drain
		opts.drain = def
		opts.drainSet = true
	}
	opts.drain = opts.drain.normalized()
	return opts
}

func (p DrainLeasesPolicy) normalized() DrainLeasesPolicy {
	if p.GracePeriod < 0 {
		p.GracePeriod = 0
	}
	return p
}

// NewServer constructs a lockd server according to cfg.
// Example:
//
//	cfg := lockd.Config{Store: "mem://", Listen: ":9341", ListenProto: "tcp"}
//	srv, err := lockd.NewServer(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	go srv.Start()
func NewServer(cfg Config, opts ...Option) (*Server, error) {
	cfgCopy := cfg
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	for _, hook := range o.configHooks {
		hook(&cfgCopy)
	}
	if err := cfgCopy.Validate(); err != nil {
		return nil, err
	}
	cfg = cfgCopy
	var err error
	var bundle *tlsutil.Bundle
	if cfg.MTLSEnabled() || cfg.StorageEncryptionEnabled() {
		bundle, err = tlsutil.LoadBundle(cfg.BundlePath, cfg.DenylistPath)
		if err != nil {
			return nil, err
		}
		if cfg.StorageEncryptionEnabled() {
			if bundle.MetadataRootKey == (keymgmt.RootKey{}) {
				return nil, fmt.Errorf("config: server bundle %q missing kryptograf root key (reissue with 'lockd auth new server')", cfg.BundlePath)
			}
			if bundle.MetadataDescriptor == (keymgmt.Descriptor{}) {
				return nil, fmt.Errorf("config: server bundle %q missing metadata descriptor (reissue with 'lockd auth new server')", cfg.BundlePath)
			}
			caID, err := cryptoutil.CACertificateID(bundle.CACertPEM)
			if err != nil {
				return nil, fmt.Errorf("config: derive ca id: %w", err)
			}
			cfg.MetadataRootKey = bundle.MetadataRootKey
			cfg.MetadataDescriptor = bundle.MetadataDescriptor
			cfg.MetadataContext = caID
		}
	}
	var crypto *storage.Crypto
	if cfg.StorageEncryptionEnabled() {
		crypto, err = storage.NewCrypto(storage.CryptoConfig{
			Enabled:            true,
			RootKey:            cfg.MetadataRootKey,
			MetadataDescriptor: cfg.MetadataDescriptor,
			MetadataContext:    []byte(cfg.MetadataContext),
			Snappy:             cfg.StorageEncryptionSnappy,
		})
		if err != nil {
			return nil, err
		}
	}
	logger := o.Logger
	if logger == nil {
		logger = logport.NoopLogger()
	}
	logger = logger.With("app", "lockd")
	if cfg.StorageEncryptionEnabled() {
		logger.Info("storage.crypto.envelope enabled", "sys", "storage.crypto.envelope", "enabled", true)
		if cfg.StorageEncryptionSnappy {
			logger.Info("storage.pipeline.snappy pre-encrypt enabled", "sys", "storage.pipeline.snappy.pre_encrypt", "enabled", true)
		} else {
			logger.Info("storage.pipeline.snappy pre-encrypt disabled", "sys", "storage.pipeline.snappy.pre_encrypt", "enabled", false)
		}
	} else {
		logger.Warn("storage.crypto.envelope disabled; falling back to plaintext at rest", "sys", "storage.crypto.envelope", "impact", "data at rest will be stored in plaintext", "enabled", false)
		logger.Info("storage.pipeline.snappy pre-encrypt disabled", "sys", "storage.pipeline.snappy.pre_encrypt", "enabled", false, "reason", "requires storage.crypto.envelope")
	}
	var telemetry *telemetryBundle
	otlpEndpoint := cfg.OTLPEndpoint
	if o.OTLPEndpoint != "" {
		otlpEndpoint = o.OTLPEndpoint
	}
	if otlpEndpoint != "" {
		telemetry, err = setupTelemetry(context.Background(), otlpEndpoint, logger.With("sys", "observability.telemetry.exporter"))
		if err != nil {
			return nil, err
		}
	}
	backend := o.Backend
	ownedBackend := false
	if backend == nil {
		backend, err = openBackend(cfg, crypto)
		if err != nil {
			if telemetry != nil {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = telemetry.Shutdown(shutdownCtx)
				cancel()
			}
			return nil, err
		}
		ownedBackend = true
	}
	serverClock := o.Clock
	if serverClock == nil {
		serverClock = clock.Real{}
	}
	retryCfg := retry.Config{
		MaxAttempts: cfg.StorageRetryMaxAttempts,
		BaseDelay:   cfg.StorageRetryBaseDelay,
		MaxDelay:    cfg.StorageRetryMaxDelay,
		Multiplier:  cfg.StorageRetryMultiplier,
	}
	storageLogger := logger.With("sys", "storage.backend.core")
	backend = loggingbackend.Wrap(backend, storageLogger.With("layer", "backend"), "storage.backend.core")
	backend = retry.Wrap(backend, storageLogger.With("layer", "retry"), serverClock, retryCfg)
	jsonUtil, err := selectJSONUtil(cfg.JSONUtil)
	if err != nil {
		if ownedBackend {
			_ = backend.Close()
		}
		if telemetry != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = telemetry.Shutdown(shutdownCtx)
			cancel()
		}
		return nil, err
	}
	qrfCtrl := qrf.NewController(qrf.Config{
		Enabled:                     cfg.QRFEnabled,
		QueueSoftLimit:              cfg.QRFQueueSoftLimit,
		QueueHardLimit:              cfg.QRFQueueHardLimit,
		QueueConsumerSoftLimit:      cfg.QRFQueueConsumerSoftLimit,
		QueueConsumerHardLimit:      cfg.QRFQueueConsumerHardLimit,
		LockSoftLimit:               cfg.QRFLockSoftLimit,
		LockHardLimit:               cfg.QRFLockHardLimit,
		MemorySoftLimitBytes:        cfg.QRFMemorySoftLimitBytes,
		MemoryHardLimitBytes:        cfg.QRFMemoryHardLimitBytes,
		MemorySoftLimitPercent:      cfg.QRFMemorySoftLimitPercent,
		MemoryHardLimitPercent:      cfg.QRFMemoryHardLimitPercent,
		MemoryStrictHeadroomPercent: cfg.QRFMemoryStrictHeadroomPercent,
		SwapSoftLimitBytes:          cfg.QRFSwapSoftLimitBytes,
		SwapHardLimitBytes:          cfg.QRFSwapHardLimitBytes,
		SwapSoftLimitPercent:        cfg.QRFSwapSoftLimitPercent,
		SwapHardLimitPercent:        cfg.QRFSwapHardLimitPercent,
		CPUPercentSoftLimit:         cfg.QRFCPUPercentSoftLimit,
		CPUPercentHardLimit:         cfg.QRFCPUPercentHardLimit,
		LoadSoftLimitMultiplier:     cfg.QRFLoadSoftLimitMultiplier,
		LoadHardLimitMultiplier:     cfg.QRFLoadHardLimitMultiplier,
		RecoverySamples:             cfg.QRFRecoverySamples,
		SoftRetryAfter:              cfg.QRFSoftRetryAfter,
		EngagedRetryAfter:           cfg.QRFEngagedRetryAfter,
		RecoveryRetryAfter:          cfg.QRFRecoveryRetryAfter,
		Logger:                      logger,
	})
	var lsfObserver *lsf.Observer
	if cfg.QRFEnabled {
		lsfObserver = lsf.NewObserver(lsf.Config{
			Enabled:        true,
			SampleInterval: cfg.LSFSampleInterval,
			LogInterval:    cfg.LSFLogInterval,
		}, qrfCtrl, logger)
	}
	var srvRef *Server
	handler := httpapi.New(httpapi.Config{
		Store:                      backend,
		Crypto:                     crypto,
		Logger:                     logger,
		Clock:                      serverClock,
		JSONMaxBytes:               cfg.JSONMaxBytes,
		CompactWriter:              jsonUtil.compactWriter,
		DefaultTTL:                 cfg.DefaultTTL,
		MaxTTL:                     cfg.MaxTTL,
		AcquireBlock:               cfg.AcquireBlock,
		SpoolMemoryThreshold:       cfg.SpoolMemoryThreshold,
		EnforceClientIdentity:      cfg.MTLSEnabled(),
		MetaWarmupAttempts:         -1,
		StateWarmupAttempts:        -1,
		QueueMaxConsumers:          cfg.QueueMaxConsumers,
		QueuePollInterval:          cfg.QueuePollInterval,
		QueuePollJitter:            cfg.QueuePollJitter,
		QueueResilientPollInterval: cfg.QueueResilientPollInterval,
		LSFObserver:                lsfObserver,
		QRFController:              qrfCtrl,
		ShutdownState: func() httpapi.ShutdownState {
			if srvRef == nil {
				return httpapi.ShutdownState{}
			}
			draining, remaining, notify := srvRef.shutdownState()
			return httpapi.ShutdownState{Draining: draining, Remaining: remaining, Notify: notify}
		},
	})
	logger.Info("json compaction configured", "impl", jsonUtil.name)
	mux := http.NewServeMux()
	handler.Register(mux)
	var lsfCancel context.CancelFunc
	if lsfObserver != nil {
		lsfCtx, cancel := context.WithCancel(context.Background())
		lsfCancel = cancel
		lsfObserver.Start(lsfCtx)
	}

	httpSrv := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
		BaseContext: func(net.Listener) context.Context {
			return context.Background()
		},
	}
	httpSrv.ErrorLog = logport.LogLoggerWithLevel(logger.With("sys", "api.http.server"), logport.ErrorLevel)

	if cfg.MTLSEnabled() {
		httpSrv.TLSConfig = buildServerTLS(bundle)
	}
	if cfg.MTLSEnabled() {
		httpSrv.TLSConfig = buildServerTLS(bundle)
	}

	srv := &Server{
		cfg:              cfg,
		logger:           logger.With("sys", "server.lifecycle.core"),
		backend:          backend,
		handler:          handler,
		httpSrv:          httpSrv,
		httpShutdown:     httpSrv.Shutdown,
		clock:            serverClock,
		telemetry:        telemetry,
		readyCh:          make(chan struct{}),
		qrfController:    qrfCtrl,
		lsfObserver:      lsfObserver,
		lsfCancel:        lsfCancel,
		defaultCloseOpts: defaultCloseOptions(),
	}
	configClose := make([]CloseOption, 0, 2)
	if cfg.DrainGraceSet || cfg.DrainGrace > 0 {
		configClose = append(configClose, WithDrainLeases(cfg.DrainGrace))
	}
	if cfg.ShutdownTimeoutSet || cfg.ShutdownTimeout > 0 {
		configClose = append(configClose, WithShutdownTimeout(cfg.ShutdownTimeout))
	}
	if len(configClose) > 0 {
		srv.defaultCloseOpts = resolveCloseOptions(srv.defaultCloseOpts, configClose)
	}
	srv.drainFn = srv.performDrain
	if len(o.closeDefaults) > 0 {
		srv.defaultCloseOpts = resolveCloseOptions(srv.defaultCloseOpts, o.closeDefaults)
	}
	srvRef = srv
	return srv, nil
}

// Handler returns the underlying HTTP handler so lockd can be mounted inside an
// existing mux when embedding the server into another program.
func (s *Server) Handler() http.Handler {
	return s.httpSrv.Handler
}

// Start begins serving requests and blocks until the server stops.
func (s *Server) Start() error {
	if s.cfg.ListenProto == "unix" {
		if err := os.Remove(s.cfg.Listen); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale unix socket: %w", err)
		}
	}
	ln, err := net.Listen(s.cfg.ListenProto, s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen (%s %s): %w", s.cfg.ListenProto, s.cfg.Listen, err)
	}
	s.listener = ln
	if s.cfg.ListenProto == "unix" {
		s.socketPath = s.cfg.Listen
	}
	s.signalReady()
	s.logger.Info("listening", "network", s.cfg.ListenProto, "address", ln.Addr().String(), "mtls", s.cfg.MTLSEnabled())
	s.startSweeper()
	defer s.stopSweeper()
	var serveErr error
	if s.httpSrv.TLSConfig != nil {
		serveErr = s.httpSrv.ServeTLS(ln, "", "")
	} else {
		serveErr = s.httpSrv.Serve(ln)
	}
	s.recordServeErr(serveErr)
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	if serveErr != nil {
		return fmt.Errorf("http serve: %w", serveErr)
	}
	return nil
}

// Shutdown gracefully stops the server and returns any fatal serve/shutdown
// error. The returned error will be nil for clean shutdowns.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.ShutdownWithOptions(ctx)
}

// ShutdownWithOptions gracefully stops the server while applying custom close
// behaviour.
func (s *Server) ShutdownWithOptions(ctx context.Context, opts ...CloseOption) error {
	if ctx == nil {
		ctx = context.Background()
	}
	resolved := resolveCloseOptions(s.defaultCloseOpts, opts)

	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return nil
	}
	s.shutdown = true
	s.mu.Unlock()

	overallTimeout := time.Duration(0)
	if resolved.shutdownTimeoutSet && resolved.shutdownTimeout > 0 {
		overallTimeout = resolved.shutdownTimeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		if overallTimeout == 0 || (remaining > 0 && remaining < overallTimeout) {
			overallTimeout = remaining
		}
	}

	effectivePolicy := resolved.drain
	if effectivePolicy.GracePeriod > 0 && overallTimeout > 0 {
		cap := time.Duration(float64(overallTimeout) * drainShutdownSplit)
		if cap <= 0 {
			cap = overallTimeout
		}
		if cap > 0 && cap < effectivePolicy.GracePeriod {
			effectivePolicy.GracePeriod = cap
		}
	}

	summary := s.drainFn(ctx, effectivePolicy)
	if summary.ActiveAtStart > 0 {
		s.logger.Debug("shutdown.drain.summary", "started", summary.ActiveAtStart, "remaining", summary.Remaining, "elapsed", summary.Elapsed)
	}

	httpCtx := ctx
	if overallTimeout > 0 {
		remaining := overallTimeout - summary.Elapsed
		if remaining < 0 {
			remaining = 0
		}
		if remaining == 0 {
			remaining = 10 * time.Millisecond
		}
		var cancel context.CancelFunc
		httpCtx, cancel = context.WithTimeout(ctx, remaining)
		defer cancel()
	}

	shutdownFn := s.httpShutdown
	if shutdownFn == nil && s.httpSrv != nil {
		shutdownFn = s.httpSrv.Shutdown
	}
	if shutdownFn == nil {
		shutdownFn = func(context.Context) error { return nil }
	}
	if err := shutdownFn(httpCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.logger.Error("shutdown.http.error", "error", err)
		return fmt.Errorf("http shutdown: %w", err)
	}
	if s.lsfCancel != nil {
		s.lsfCancel()
		s.lsfCancel = nil
	}
	if s.lsfObserver != nil {
		s.lsfObserver.Wait()
	}
	if l := s.listener; l != nil {
		if err := l.Close(); err != nil {
			s.logger.Warn("shutdown.listener.close_error", "error", err)
		}
		s.listener = nil
	}
	s.stopSweeper()
	if err := s.backend.Close(); err != nil {
		s.logger.Error("shutdown.backend.close_error", "error", err)
		return err
	}
	if s.telemetry != nil {
		telemetryCtx := ctx
		if telemetryCtx.Err() != nil {
			var cancel context.CancelFunc
			telemetryCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
		}
		if err := s.telemetry.Shutdown(telemetryCtx); err != nil {
			s.logger.Error("shutdown.telemetry.close_error", "error", err)
			return err
		}
		s.telemetry = nil
	}
	if s.cfg.ListenProto == "unix" && s.socketPath != "" {
		if err := os.Remove(s.socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.logger.Warn("shutdown.socket.remove_error", "error", err)
			return err
		}
	}
	if err := s.LastServeError(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.logger.Error("shutdown.serve_error", "error", err)
		return err
	}
	return nil
}

// Close gracefully shuts the server down using a background context.
func (s *Server) Close(opts ...CloseOption) error {
	return s.ShutdownWithOptions(context.Background(), opts...)
}

func (s *Server) signalReady() {
	s.readyOnce.Do(func() {
		close(s.readyCh)
	})
}

// WaitUntilReady blocks until the server listener is initialized or context ends.
func (s *Server) WaitUntilReady(ctx context.Context) error {
	select {
	case <-s.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ListenerAddr returns the bound listener address once available.
func (s *Server) ListenerAddr() net.Addr {
	if l := s.listener; l != nil {
		return l.Addr()
	}
	return nil
}

func (s *Server) shutdownState() (bool, time.Duration, bool) {
	if s == nil {
		return false, 0, false
	}
	if !s.draining.Load() {
		return false, 0, false
	}
	remaining := time.Duration(0)
	if deadlineNano := s.drainDeadline.Load(); deadlineNano > 0 {
		deadline := time.Unix(0, deadlineNano)
		now := s.clock.Now()
		remaining = deadline.Sub(now)
		if remaining < 0 {
			remaining = 0
		}
	}
	return true, remaining, s.drainNotifyClients.Load()
}

func (s *Server) ensureDrainMetrics() {
	s.drainMetrics.once.Do(func() {
		meter := otel.Meter("pkt.systems/lockd/server")
		if hist, err := meter.Int64Histogram("lockd.shutdown.drain.active_leases"); err == nil {
			s.drainMetrics.active = hist
		} else {
			s.logger.Warn("shutdown.telemetry.instrument_error", "metric", "lockd.shutdown.drain.active_leases", "error", err)
		}
		if hist, err := meter.Int64Histogram("lockd.shutdown.drain.remaining_leases"); err == nil {
			s.drainMetrics.remaining = hist
		} else {
			s.logger.Warn("shutdown.telemetry.instrument_error", "metric", "lockd.shutdown.drain.remaining_leases", "error", err)
		}
		if hist, err := meter.Float64Histogram("lockd.shutdown.drain.elapsed_seconds"); err == nil {
			s.drainMetrics.duration = hist
		} else {
			s.logger.Warn("shutdown.telemetry.instrument_error", "metric", "lockd.shutdown.drain.elapsed_seconds", "error", err)
		}
	})
}

func (s *Server) recordDrainMetrics(summary drainSummary) {
	s.ensureDrainMetrics()
	ctx := context.Background()
	if s.drainMetrics.active != nil && summary.ActiveAtStart >= 0 {
		s.drainMetrics.active.Record(ctx, int64(summary.ActiveAtStart))
	}
	if s.drainMetrics.remaining != nil && summary.Remaining >= 0 {
		s.drainMetrics.remaining.Record(ctx, int64(summary.Remaining))
	}
	if s.drainMetrics.duration != nil && summary.Elapsed >= 0 {
		s.drainMetrics.duration.Record(ctx, summary.Elapsed.Seconds())
	}
}

func (s *Server) snapshotActiveLeases(ctx context.Context) (leases []leaseSnapshot, err error) {
	if s.backend == nil {
		return nil, nil
	}
	start := s.clock.Now()
	logger := s.logger.With("sys", "server.shutdown.controller")
	var totalKeys int
	defer func() {
		elapsed := s.clock.Now().Sub(start)
		fields := []any{
			"keys", totalKeys,
			"active", len(leases),
			"elapsed", elapsed,
		}
		if err != nil {
			fields = append(fields, "error", err)
			logger.Debug("shutdown.drain.snapshot.scan_error", fields...)
		} else {
			logger.Debug("shutdown.drain.snapshot.scan", fields...)
		}
	}()

	var keys []string
	keys, err = s.backend.ListMetaKeys(ctx)
	if err != nil {
		return nil, err
	}
	totalKeys = len(keys)
	now := s.clock.Now().Unix()
	leases = make([]leaseSnapshot, 0, len(keys))
	for _, key := range keys {
		var meta *storage.Meta
		meta, _, err = s.backend.LoadMeta(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			logger.Warn("shutdown.drain.load_meta_failed", "key", key, "error", err)
			continue
		}
		if meta.Lease == nil {
			continue
		}
		if meta.Lease.ExpiresAtUnix <= now {
			continue
		}
		leases = append(leases, leaseSnapshot{
			Key:       key,
			Owner:     meta.Lease.Owner,
			LeaseID:   meta.Lease.ID,
			ExpiresAt: meta.Lease.ExpiresAtUnix,
		})
	}
	return leases, nil
}

func (s *Server) forceReleaseLeases(ctx context.Context, leases []leaseSnapshot) int {
	released := 0
	if len(leases) == 0 || s.backend == nil {
		return 0
	}
	for _, lease := range leases {
		meta, etag, err := s.backend.LoadMeta(ctx, lease.Key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			s.logger.Warn("shutdown.drain.force_release.load_failed", "key", lease.Key, "error", err)
			continue
		}
		if meta.Lease == nil || meta.Lease.ID != lease.LeaseID {
			continue
		}
		meta.Lease = nil
		meta.UpdatedAtUnix = s.clock.Now().Unix()
		if _, err := s.backend.StoreMeta(ctx, lease.Key, meta, etag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			s.logger.Warn("shutdown.drain.force_release.store_failed", "key", lease.Key, "error", err)
			continue
		}
		released++
	}
	return released
}

func (s *Server) performDrain(parentCtx context.Context, policy DrainLeasesPolicy) drainSummary {
	summary := drainSummary{ActiveAtStart: 0, Remaining: 0}
	if !s.draining.Load() {
		s.draining.Store(true)
	}
	s.drainNotifyClients.Store(policy.NotifyClients)
	if policy.GracePeriod <= 0 {
		s.drainDeadline.Store(0)
		s.logger.Info("shutdown.drain.skip", "reason", "grace_disabled", "force_release", policy.ForceRelease, "notify_clients", policy.NotifyClients)
		s.recordDrainMetrics(summary)
		return summary
	}

	startTime := s.clock.Now()
	deadline := startTime.Add(policy.GracePeriod)
	s.drainDeadline.Store(deadline.UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	go func() {
		select {
		case <-parentCtx.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	leases, err := s.snapshotActiveLeases(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			s.logger.Warn("shutdown.drain.unsupported", "error", err)
		} else {
			s.logger.Warn("shutdown.drain.snapshot_failed", "error", err)
		}
		summary.Elapsed = s.clock.Now().Sub(startTime)
		s.recordDrainMetrics(summary)
		return summary
	}

	startActive := len(leases)
	summary.ActiveAtStart = startActive
	remaining := startActive
	logger := s.logger.With("sys", "server.shutdown.controller")
	logger.Info("shutdown.drain.begin",
		"active_leases", startActive,
		"grace_period", policy.GracePeriod,
		"force_release", policy.ForceRelease,
		"notify_clients", policy.NotifyClients,
	)
	if startActive > 0 {
		sample := make([]string, 0, len(leases))
		for i, lease := range leases {
			if i >= 5 {
				break
			}
			sample = append(sample, lease.Key)
		}
		if len(sample) > 0 {
			logger.Debug("shutdown.drain.sample", "keys", sample)
		}
	}

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if len(leases) == 0 {
			remaining = 0
			break
		}
		if !deadline.IsZero() && s.clock.Now().After(deadline) {
			remaining = len(leases)
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			logger.Warn("shutdown.drain.context_cancelled", "error", ctx.Err())
			remaining = len(leases)
			goto FORCE
		}
		if !deadline.IsZero() && s.clock.Now().After(deadline) {
			remaining = len(leases)
			break
		}
		leases, err = s.snapshotActiveLeases(ctx)
		if err != nil {
			logger.Warn("shutdown.drain.snapshot_failed", "error", err)
			remaining = len(leases)
			break
		}
		remaining = len(leases)
		if remaining > 0 {
			logger.Debug("shutdown.drain.wait", "remaining", remaining, "deadline", deadline)
		}
	}

FORCE:
	forced := 0
	if remaining > 0 && policy.ForceRelease {
		forced = s.forceReleaseLeases(ctx, leases)
		remaining -= forced
		if remaining < 0 {
			remaining = 0
		}
		logger.Warn("shutdown.drain.force_release", "attempted", len(leases), "released", forced, "remaining", remaining)
	}

	summary.Remaining = remaining
	summary.Elapsed = s.clock.Now().Sub(startTime)
	logger.Info("shutdown.drain.complete",
		"active_leases", startActive,
		"released", startActive-remaining,
		"force_release", policy.ForceRelease,
		"forced_releases", forced,
		"remaining", remaining,
		"elapsed", summary.Elapsed,
	)
	s.recordDrainMetrics(summary)
	return summary
}

func (s *Server) startSweeper() {
	if s.cfg.SweeperInterval <= 0 {
		return
	}
	s.mu.Lock()
	if s.sweeperStop != nil {
		s.mu.Unlock()
		return
	}
	s.sweeperStop = make(chan struct{})
	s.sweeperDone.Add(1)
	stopCh := s.sweeperStop
	interval := s.cfg.SweeperInterval
	sweeperCtx := context.Background()
	s.mu.Unlock()
	go func() {
		defer s.sweeperDone.Done()
		for {
			select {
			case <-stopCh:
				return
			case <-s.clock.After(interval):
				if err := s.sweepExpired(sweeperCtx); err != nil && !errors.Is(err, storage.ErrNotImplemented) {
					s.logger.Warn("sweeper iteration failed", "error", err)
				}
			}
		}
	}()
}

func (s *Server) stopSweeper() {
	s.mu.Lock()
	stopCh := s.sweeperStop
	if stopCh != nil {
		close(stopCh)
		s.sweeperStop = nil
	}
	s.mu.Unlock()
	if stopCh != nil {
		s.sweeperDone.Wait()
	}
}

func (s *Server) sweepExpired(ctx context.Context) error {
	keys, err := s.backend.ListMetaKeys(ctx)
	if err != nil {
		return err
	}
	now := s.clock.Now().Unix()
	for _, key := range keys {
		meta, etag, err := s.backend.LoadMeta(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			s.logger.Warn("sweeper load meta failed", "key", key, "error", err)
			continue
		}
		if meta.Lease == nil {
			continue
		}
		if meta.Lease.ExpiresAtUnix > now {
			continue
		}
		meta.Lease = nil
		meta.UpdatedAtUnix = now
		if _, err := s.backend.StoreMeta(ctx, key, meta, etag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			s.logger.Warn("sweeper store meta failed", "key", key, "error", err)
		}
	}
	return nil
}

func (s *Server) recordServeErr(err error) {
	s.mu.Lock()
	s.lastServeErr = err
	s.mu.Unlock()
}

// LastServeError returns the most recent error reported by the underlying HTTP
// server. It is primarily useful for diagnostics; Shutdown already reports any
// fatal serve/shutdown errors to callers.
func (s *Server) LastServeError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastServeErr
}

// QRFState returns the current state of the perimeter defence controller.
func (s *Server) QRFState() qrf.State {
	if s == nil || s.qrfController == nil {
		return qrf.StateDisengaged
	}
	return s.qrfController.State()
}

// QRFStatus returns the current controller state, reason, and last snapshot.
func (s *Server) QRFStatus() (qrf.State, string, qrf.Snapshot) {
	if s == nil || s.qrfController == nil {
		return qrf.StateDisengaged, "", qrf.Snapshot{}
	}
	return s.qrfController.Status()
}

// ForceQRFObserve injects a metrics snapshot into the QRF controller. It is
// intended for tests that need to drive the perimeter-defence state machine
// deterministically.
func (s *Server) ForceQRFObserve(snapshot qrf.Snapshot) {
	if s == nil || s.qrfController == nil {
		return
	}
	s.qrfController.Observe(snapshot)
}

func buildServerTLS(bundle *tlsutil.Bundle) *tls.Config {
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{bundle.ServerCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    bundle.CAPool,
	}
	if len(bundle.ServerCertificate.Certificate) > 0 {
		tlsCfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
			cfg := tlsCfg.Clone()
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
			cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				return verifyClientCert(rawCerts, bundle)
			}
			return cfg, nil
		}
	}
	return tlsCfg
}

// StartServer starts a lockd server in a background goroutine and waits until it
// is ready to accept connections. It returns the running server alongside a
// stop function that gracefully shuts it down.
// Example:
//
//	cfg := lockd.Config{Store: "mem://", ListenProto: "unix", Listen: "/tmp/lockd.sock", DisableMTLS: true}
//	srv, stop, err := lockd.StartServer(ctx, cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer stop(context.Background())
func StartServer(ctx context.Context, cfg Config, opts ...Option) (*Server, func(context.Context, ...CloseOption) error, error) {
	srv, err := NewServer(cfg, opts...)
	if err != nil {
		return nil, nil, err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()
	waitCtx := ctx
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	if err := srv.WaitUntilReady(waitCtx); err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.ShutdownWithOptions(shutdownCtx)
		<-errCh
		return nil, nil, err
	}
	var (
		stopOnce sync.Once
		stopErr  error
	)
	stop := func(shutdownCtx context.Context, closeOpts ...CloseOption) error {
		stopOnce.Do(func() {
			if shutdownCtx == nil {
				shutdownCtx = context.Background()
			}
			if err := srv.ShutdownWithOptions(shutdownCtx, closeOpts...); err != nil {
				stopErr = err
				return
			}
			if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
				stopErr = err
			}
		})
		return stopErr
	}
	if ctx != nil {
		go func() {
			<-ctx.Done()
			_ = stop(context.Background())
		}()
	}
	return srv, stop, nil
}

func verifyClientCert(rawCerts [][]byte, bundle *tlsutil.Bundle) error {
	if len(rawCerts) == 0 {
		return errors.New("mtls: missing client certificate")
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("mtls: parse client certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	leaf := certs[0]
	if _, ok := bundle.Denylist[strings.ToLower(leaf.SerialNumber.Text(16))]; ok {
		return fmt.Errorf("mtls: certificate %s revoked", leaf.SerialNumber.Text(16))
	}
	opts := x509.VerifyOptions{
		Roots:         bundle.CAPool,
		CurrentTime:   time.Now(),
		Intermediates: x509.NewCertPool(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := leaf.Verify(opts); err != nil {
		return fmt.Errorf("mtls: verify client certificate: %w", err)
	}
	return nil
}
