package lockd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/httpapi"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/retry"
	"pkt.systems/lockd/internal/tlsutil"
	"pkt.systems/logport"
)

// Server wraps the HTTP server, storage backend, and supporting components.
type Server struct {
	cfg      Config
	logger   logport.ForLogging
	backend  storage.Backend
	handler  *httpapi.Handler
	httpSrv  *http.Server
	listener net.Listener
	clock    clock.Clock

	mu          sync.Mutex
	shutdown    bool
	sweeperStop chan struct{}
	sweeperDone sync.WaitGroup
}

// Option configures server instances.
type Option func(*options)

type options struct {
	Logger  logport.ForLogging
	Backend storage.Backend
	Clock   clock.Clock
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

// NewServer constructs a lockd server according to cfg.
func NewServer(cfg Config, opts ...Option) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	logger := o.Logger
	if logger == nil {
		logger = logport.NoopLogger()
	}
	backend := o.Backend
	var err error
	ownedBackend := false
	if backend == nil {
		backend, err = openBackend(cfg)
		if err != nil {
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
	backend = retry.Wrap(backend, logger.With("component", "storage"), serverClock, retryCfg)
	handler := httpapi.New(httpapi.Config{
		Store:        backend,
		Logger:       logger.With("component", "api"),
		Clock:        serverClock,
		JSONMaxBytes: cfg.JSONMaxBytes,
		DefaultTTL:   cfg.DefaultTTL,
		MaxTTL:       cfg.MaxTTL,
		AcquireBlock: cfg.AcquireBlock,
	})
	mux := http.NewServeMux()
	handler.Register(mux)

	httpSrv := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
		BaseContext: func(net.Listener) context.Context {
			return context.Background()
		},
	}
	httpSrv.ErrorLog = logport.LogLoggerWithLevel(logger.With("component", "http"), logport.ErrorLevel)

	if cfg.MTLS {
		bundle, err := tlsutil.LoadBundle(cfg.BundlePath, cfg.DenylistPath)
		if err != nil {
			if ownedBackend {
				_ = backend.Close()
			}
			return nil, err
		}
		httpSrv.TLSConfig = buildServerTLS(bundle)
	}

	return &Server{
		cfg:     cfg,
		logger:  logger.With("component", "server"),
		backend: backend,
		handler: handler,
		httpSrv: httpSrv,
		clock:   serverClock,
	}, nil
}

// Handler exposes the server's HTTP handler, useful when embedding into other servers.
func (s *Server) Handler() http.Handler {
	return s.httpSrv.Handler
}

// Start begins serving requests and blocks until the server stops.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.listener = ln
	s.logger.Info("listening", "address", ln.Addr().String(), "mtls", s.cfg.MTLS)
	s.startSweeper()
	defer s.stopSweeper()
	var serveErr error
	if s.httpSrv.TLSConfig != nil {
		serveErr = s.httpSrv.ServeTLS(ln, "", "")
	} else {
		serveErr = s.httpSrv.Serve(ln)
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	return serveErr
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return nil
	}
	s.shutdown = true
	s.mu.Unlock()

	if err := s.httpSrv.Shutdown(ctx); err != nil {
		return err
	}
	s.stopSweeper()
	return s.backend.Close()
}

func (s *Server) Close() error {
	return s.Shutdown(context.Background())
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
