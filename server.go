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
)

// Server wraps the HTTP server, storage backend, and supporting components.
type Server struct {
	cfg          Config
	logger       logport.ForLogging
	backend      storage.Backend
	handler      *httpapi.Handler
	httpSrv      *http.Server
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
}

// Option configures server instances.
type Option func(*options)

type options struct {
	Logger       logport.ForLogging
	Backend      storage.Backend
	Clock        clock.Clock
	OTLPEndpoint string
	configHooks  []func(*Config)
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
	if cfg.MTLS || cfg.StorageEncryptionEnabled {
		bundle, err = tlsutil.LoadBundle(cfg.BundlePath, cfg.DenylistPath)
		if err != nil {
			return nil, err
		}
		if cfg.StorageEncryptionEnabled {
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
	if cfg.StorageEncryptionEnabled {
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
	if cfg.StorageEncryptionEnabled {
		logger.Info("storage encryption enabled")
	} else {
		logger.Warn("storage encryption disabled", "impact", "data at rest will be stored in plaintext")
	}
	logger.Info("storage encryption snappy", "enabled", cfg.StorageEncryptionSnappy)
	var telemetry *telemetryBundle
	otlpEndpoint := cfg.OTLPEndpoint
	if o.OTLPEndpoint != "" {
		otlpEndpoint = o.OTLPEndpoint
	}
	if otlpEndpoint != "" {
		telemetry, err = setupTelemetry(context.Background(), otlpEndpoint, logger.With("svc", "telemetry"))
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
	storageLogger := logger.With("svc", "storage")
	backend = loggingbackend.Wrap(backend, storageLogger.With("layer", "backend"))
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
		EnforceClientIdentity:      cfg.MTLS,
		MetaWarmupAttempts:         -1,
		StateWarmupAttempts:        -1,
		QueueMaxConsumers:          cfg.QueueMaxConsumers,
		QueuePollInterval:          cfg.QueuePollInterval,
		QueuePollJitter:            cfg.QueuePollJitter,
		QueueResilientPollInterval: cfg.QueueResilientPollInterval,
		LSFObserver:                lsfObserver,
		QRFController:              qrfCtrl,
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
	httpSrv.ErrorLog = logport.LogLoggerWithLevel(logger.With("svc", "http"), logport.ErrorLevel)

	if cfg.MTLS {
		httpSrv.TLSConfig = buildServerTLS(bundle)
	}
	if cfg.MTLS {
		httpSrv.TLSConfig = buildServerTLS(bundle)
	}

	return &Server{
		cfg:           cfg,
		logger:        logger.With("svc", "server"),
		backend:       backend,
		handler:       handler,
		httpSrv:       httpSrv,
		clock:         serverClock,
		telemetry:     telemetry,
		readyCh:       make(chan struct{}),
		qrfController: qrfCtrl,
		lsfObserver:   lsfObserver,
		lsfCancel:     lsfCancel,
	}, nil
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
	s.logger.Info("listening", "network", s.cfg.ListenProto, "address", ln.Addr().String(), "mtls", s.cfg.MTLS)
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
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return nil
	}
	s.shutdown = true
	s.mu.Unlock()

	if err := s.httpSrv.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
		_ = l.Close()
		s.listener = nil
	}
	s.stopSweeper()
	if err := s.backend.Close(); err != nil {
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
			return err
		}
		s.telemetry = nil
	}
	if s.cfg.ListenProto == "unix" && s.socketPath != "" {
		if err := os.Remove(s.socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := s.LastServeError(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Close gracefully shuts the server down using a background context.
func (s *Server) Close() error {
	return s.Shutdown(context.Background())
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
//	cfg := lockd.Config{Store: "mem://", ListenProto: "unix", Listen: "/tmp/lockd.sock", MTLS: false}
//	srv, stop, err := lockd.StartServer(ctx, cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer stop(context.Background())
func StartServer(ctx context.Context, cfg Config, opts ...Option) (*Server, func(context.Context) error, error) {
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
		_ = srv.Shutdown(shutdownCtx)
		<-errCh
		return nil, nil, err
	}
	var (
		stopOnce sync.Once
		stopErr  error
	)
	stop := func(shutdownCtx context.Context) error {
		stopOnce.Do(func() {
			if shutdownCtx == nil {
				shutdownCtx = context.Background()
			}
			if err := srv.Shutdown(shutdownCtx); err != nil {
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
