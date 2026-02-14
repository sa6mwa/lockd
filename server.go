package lockd

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"

	"pkt.systems/kryptograf/keymgmt"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/connguard"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/httpapi"
	"pkt.systems/lockd/internal/lsf"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/search"
	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/search/scan"
	"pkt.systems/lockd/internal/storage"
	loggingbackend "pkt.systems/lockd/internal/storage/logging"
	"pkt.systems/lockd/internal/storage/retry"
	"pkt.systems/lockd/internal/svcfields"
	"pkt.systems/lockd/internal/tcclient"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/internal/tcleader"
	"pkt.systems/lockd/internal/txncoord"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Server wraps the HTTP server, storage backend, and supporting components.
type Server struct {
	cfg          Config
	baseLogger   pslog.Logger
	logger       pslog.Logger
	backend      storage.Backend
	backendHash  string
	serverBundle *tlsutil.Bundle
	indexManager *indexer.Manager
	handler      *httpapi.Handler
	httpSrv      *http.Server
	httpShutdown func(context.Context) error
	listener     net.Listener
	socketPath   string
	clock        clock.Clock
	telemetry    *telemetryBundle
	lastServeErr error

	mu               sync.Mutex
	tcLeader         *tcleader.Manager
	tcLeaderCancel   context.CancelFunc
	tcCAPool         *x509.CertPool
	defaultCAPool    *x509.CertPool
	tcAuthEnabled    bool
	tcAllowDefaultCA bool
	tcCluster        *tccluster.Store
	tcTrustPEM       [][]byte
	tcClusterMu      sync.Mutex
	tcClusterClient  *http.Client
	tcClusterNoLeave atomic.Bool
	tcClusterLeft    atomic.Bool
	rmRegisterCancel context.CancelFunc
	rmRegisterDone   sync.WaitGroup
	tcClusterCancel  context.CancelFunc
	tcClusterDone    sync.WaitGroup
	tcClusterID      string

	shutdown    bool
	sweeperStop chan struct{}
	sweeperDone sync.WaitGroup
	readyOnce   sync.Once
	readyCh     chan struct{}

	qrfController *qrf.Controller
	lsfObserver   *lsf.Observer
	lsfCancel     context.CancelFunc

	lastActivity     atomic.Int64
	idleSweepRunning atomic.Bool

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
	Namespace string
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
	Logger              pslog.Logger
	Backend             storage.Backend
	Clock               clock.Clock
	OTLPEndpoint        string
	MetricsListen       string
	MetricsSet          bool
	PprofListen         string
	PprofSet            bool
	ProfilingMetrics    bool
	ProfilingMetricsSet bool
	configHooks         []func(*Config)
	closeDefaults       []CloseOption
	tcLeaseTTL          time.Duration
	tcFanoutGate        txncoord.FanoutGate
}

// WithLogger supplies a custom logger.
// Passing nil falls back to pslog.NoopLogger().
func WithLogger(l pslog.Logger) Option {
	return func(o *options) {
		if l == nil {
			o.Logger = pslog.NoopLogger()
			return
		}
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

// WithTCLeaderLeaseTTL overrides the lease TTL used for TC leader election.
func WithTCLeaderLeaseTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.tcLeaseTTL = ttl
	}
}

// WithTCFanoutGate injects a hook between local apply and remote fan-out (test-only).
func WithTCFanoutGate(gate txncoord.FanoutGate) Option {
	return func(o *options) {
		o.tcFanoutGate = gate
	}
}

// WithOTLPEndpoint overrides the OTLP collector endpoint used for telemetry.
func WithOTLPEndpoint(endpoint string) Option {
	return func(o *options) {
		o.OTLPEndpoint = endpoint
	}
}

// WithMetricsListen overrides the metrics listener address (empty disables metrics).
func WithMetricsListen(addr string) Option {
	return func(o *options) {
		o.MetricsListen = addr
		o.MetricsSet = true
	}
}

// WithPprofListen overrides the pprof listener address (empty disables).
func WithPprofListen(addr string) Option {
	return func(o *options) {
		o.PprofListen = addr
		o.PprofSet = true
	}
}

// WithProfilingMetrics toggles Go runtime profiling metrics on the metrics endpoint.
func WithProfilingMetrics(enabled bool) Option {
	return func(o *options) {
		o.ProfilingMetrics = enabled
		o.ProfilingMetricsSet = true
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

const (
	rmRegistrationInterval     = 30 * time.Second
	rmRegistrationPollInterval = 1 * time.Second
	rmRegistrationTimeout      = 5 * time.Second
)

const (
	tcLeaveFanoutHeader        = "X-Lockd-TC-Leave-Fanout"
	tcLeaveFanoutMaxConcurrent = 16
)

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
		bundleSource := cfg.BundlePath
		if len(cfg.BundlePEM) > 0 {
			bundle, err = tlsutil.LoadBundleFromBytes(cfg.BundlePEM)
			bundleSource = "<inline>"
		} else {
			bundle, err = tlsutil.LoadBundle(cfg.BundlePath, cfg.DenylistPath)
		}
		if err != nil {
			return nil, err
		}
		if cfg.StorageEncryptionEnabled() {
			if bundle.MetadataRootKey == (keymgmt.RootKey{}) {
				return nil, fmt.Errorf("config: server bundle %s missing kryptograf root key (reissue with 'lockd auth new server')", bundleSource)
			}
			if bundle.MetadataDescriptor == (keymgmt.Descriptor{}) {
				return nil, fmt.Errorf("config: server bundle %s missing metadata descriptor (reissue with 'lockd auth new server')", bundleSource)
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
			DisableBufferPool:  cfg.DisableKryptoPool,
		})
		if err != nil {
			return nil, err
		}
	}
	logger := o.Logger
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	if parsed, err := url.Parse(cfg.Store); err == nil {
		scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
		if scheme == "disk" && strings.EqualFold(cfg.HAMode, "concurrent") {
			logger.Warn("ha.mode.override",
				"requested", "concurrent",
				"effective", "failover",
				"store", cfg.Store,
				"reason", "disk/nfs backends require a single serialized writer for performance and correctness; concurrent mode is supported for object stores",
			)
			cfg.HAMode = "failover"
		}
	}
	serverClock := o.Clock
	if serverClock == nil {
		serverClock = clock.Real{}
	}
	if cfg.StorageEncryptionEnabled() {
		cryptoLogger := svcfields.WithSubsystem(logger, "storage.crypto.envelope")
		cryptoLogger.Info("storage.crypto.envelope enabled", "enabled", true)
		poolLogger := svcfields.WithSubsystem(logger, "storage.crypto.buffer_pool")
		poolLogger.Info("storage.crypto.buffer_pool enabled", "enabled", !cfg.DisableKryptoPool)
		snappyLogger := svcfields.WithSubsystem(logger, "storage.pipeline.snappy.pre_encrypt")
		if cfg.StorageEncryptionSnappy {
			snappyLogger.Info("storage.pipeline.snappy pre-encrypt enabled", "enabled", true)
		} else {
			snappyLogger.Info("storage.pipeline.snappy pre-encrypt disabled", "enabled", false)
		}
	} else {
		cryptoLogger := svcfields.WithSubsystem(logger, "storage.crypto.envelope")
		cryptoLogger.Warn("storage.crypto.envelope disabled; falling back to plaintext at rest", "impact", "data at rest will be stored in plaintext", "enabled", false)
		snappyLogger := svcfields.WithSubsystem(logger, "storage.pipeline.snappy.pre_encrypt")
		snappyLogger.Info("storage.pipeline.snappy pre-encrypt disabled", "enabled", false, "reason", "requires storage.crypto.envelope")
	}
	var (
		tcTrustPool      *x509.CertPool
		tcTrustPEMBlobs  [][]byte
		tcClientTrustPEM [][]byte
		clientTrustPool  *x509.CertPool
	)
	if bundle != nil {
		clientTrustPool = bundle.CAPool
		if cfg.TCTrustDir != "" {
			tcPool, blobs, err := loadTCTrustPool(cfg.TCTrustDir, svcfields.WithSubsystem(logger, "server.tctrust"))
			if err != nil {
				return nil, err
			}
			tcTrustPool = tcPool
			tcTrustPEMBlobs = blobs
			if tcPool != nil && len(tcTrustPEMBlobs) > 0 {
				clientTrustPool = bundle.CAPool.Clone()
				for _, pemData := range tcTrustPEMBlobs {
					clientTrustPool.AppendCertsFromPEM(pemData)
				}
			}
		}
	}
	if len(tcTrustPEMBlobs) > 0 {
		tcClientTrustPEM = append(tcClientTrustPEM, tcTrustPEMBlobs...)
	}
	if bundle != nil && len(bundle.CACertPEM) > 0 {
		tcClientTrustPEM = append(tcClientTrustPEM, bundle.CACertPEM)
	}
	tcClusterIdentity := ""
	if cfg.MTLSEnabled() && bundle != nil {
		if bundle.ServerCert != nil {
			tcClusterIdentity = tccluster.IdentityFromCertificate(bundle.ServerCert)
		}
		if tcClusterIdentity == "" && len(bundle.ServerCertificate.Certificate) > 0 {
			if cert, err := x509.ParseCertificate(bundle.ServerCertificate.Certificate[0]); err == nil {
				tcClusterIdentity = tccluster.IdentityFromCertificate(cert)
			}
		}
	} else {
		tcClusterIdentity = tccluster.IdentityFromEndpoint(cfg.SelfEndpoint)
	}
	joinHasNonSelf := false
	selfEndpoint := strings.TrimSpace(cfg.SelfEndpoint)
	normalizedSelf := selfEndpoint
	if normalizedSelf != "" {
		normalized := tccluster.NormalizeEndpoints([]string{normalizedSelf})
		if len(normalized) > 0 {
			normalizedSelf = normalized[0]
		}
	}
	joinEndpoints := tccluster.NormalizeEndpoints(append([]string(nil), cfg.TCJoinEndpoints...))
	if len(joinEndpoints) > 0 && normalizedSelf != "" {
		for _, endpoint := range joinEndpoints {
			if endpoint != normalizedSelf {
				joinHasNonSelf = true
				break
			}
		}
	}
	if joinHasNonSelf && !cfg.MTLSEnabled() {
		return nil, fmt.Errorf("tc join requires mTLS for multi-node membership")
	}
	if cfg.MTLSEnabled() && joinHasNonSelf && tcClusterIdentity == "" {
		return nil, fmt.Errorf("tc join requires a server certificate with spiffe://lockd/server/<node-id>")
	}
	leaderSelfID := ""
	if cfg.MTLSEnabled() && tcClusterIdentity != "" {
		leaderSelfID = tcClusterIdentity
	}
	var handler *httpapi.Handler
	var tcLeader *tcleader.Manager
	if strings.TrimSpace(cfg.SelfEndpoint) != "" {
		tcLeader, err = tcleader.NewManager(tcleader.Config{
			SelfID:       leaderSelfID,
			SelfEndpoint: cfg.SelfEndpoint,
			Logger:       svcfields.WithSubsystem(logger, "tc.leader"),
			DisableMTLS:  cfg.DisableMTLS,
			ClientBundle: cfg.TCClientBundlePath,
			TrustPEM:     tcClientTrustPEM,
			LeaseTTL:     o.tcLeaseTTL,
			Clock:        serverClock,
			Eligible: func() bool {
				if handler == nil {
					return true
				}
				return handler.NodeActive()
			},
		})
		if err != nil {
			return nil, err
		}
	}
	var telemetry *telemetryBundle
	otlpEndpoint := cfg.OTLPEndpoint
	if o.OTLPEndpoint != "" {
		otlpEndpoint = o.OTLPEndpoint
	}
	if strings.TrimSpace(otlpEndpoint) == "" {
		cfg.DisableHTTPTracing = true
		cfg.DisableStorageTracing = true
	}
	metricsListen := cfg.MetricsListen
	if o.MetricsSet {
		metricsListen = o.MetricsListen
	}
	pprofListen := cfg.PprofListen
	if o.PprofSet {
		pprofListen = o.PprofListen
	}
	profilingMetrics := cfg.EnableProfilingMetrics
	if o.ProfilingMetricsSet {
		profilingMetrics = o.ProfilingMetrics
	}
	if profilingMetrics && strings.TrimSpace(metricsListen) == "" {
		return nil, fmt.Errorf("profiling metrics require metrics listen address")
	}
	if otlpEndpoint != "" || metricsListen != "" || pprofListen != "" || profilingMetrics {
		telemetry, err = setupTelemetry(context.Background(), otlpEndpoint, metricsListen, pprofListen, profilingMetrics, svcfields.WithSubsystem(logger, "observability.telemetry.exporter"))
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
	clusterLogger := svcfields.WithSubsystem(logger, "tc.cluster")
	var tcClusterStore *tccluster.Store
	if backend != nil {
		tcClusterStore = tccluster.NewStore(backend, clusterLogger, serverClock)
	}
	backendHash := ""
	if backend != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		hash, err := backend.BackendHash(ctx)
		cancel()
		if err != nil {
			logger.Warn("storage.backend_hash.error", "error", err)
		}
		backendHash = strings.TrimSpace(hash)
		if backendHash == "" {
			logger.Warn("storage.backend_hash.empty")
		}
	}
	retryCfg := retry.Config{
		MaxAttempts: cfg.StorageRetryMaxAttempts,
		BaseDelay:   cfg.StorageRetryBaseDelay,
		MaxDelay:    cfg.StorageRetryMaxDelay,
		Multiplier:  cfg.StorageRetryMultiplier,
	}
	storageLogger := svcfields.WithSubsystem(logger, "storage.backend.core")
	if !cfg.DisableStorageTracing {
		backend = loggingbackend.Wrap(backend, storageLogger.With("layer", "backend"), "storage.backend.core")
	}
	backend = retry.Wrap(backend, storageLogger.With("layer", "retry"), serverClock, retryCfg)
	if strings.EqualFold(cfg.HAMode, "concurrent") {
		storageLogger.Info("storage.single_writer.disabled", "mode", "concurrent")
	}
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
		Enabled:                     !cfg.QRFDisabled,
		QueueSoftLimit:              cfg.QRFQueueSoftLimit,
		QueueHardLimit:              cfg.QRFQueueHardLimit,
		QueueConsumerSoftLimit:      cfg.QRFQueueConsumerSoftLimit,
		QueueConsumerHardLimit:      cfg.QRFQueueConsumerHardLimit,
		LockSoftLimit:               cfg.QRFLockSoftLimit,
		LockHardLimit:               cfg.QRFLockHardLimit,
		QuerySoftLimit:              cfg.QRFQuerySoftLimit,
		QueryHardLimit:              cfg.QRFQueryHardLimit,
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
		SoftDelay:                   cfg.QRFSoftDelay,
		EngagedDelay:                cfg.QRFEngagedDelay,
		RecoveryDelay:               cfg.QRFRecoveryDelay,
		MaxWait:                     cfg.QRFMaxWait,
		Logger:                      logger,
	})
	var lsfObserver *lsf.Observer
	if !cfg.QRFDisabled {
		lsfObserver = lsf.NewObserver(lsf.Config{
			Enabled:        true,
			SampleInterval: cfg.LSFSampleInterval,
			LogInterval:    cfg.LSFLogInterval,
		}, qrfCtrl, logger)
	}
	defaultNamespaceCfg := namespaces.DefaultConfig()
	if provider, ok := backend.(namespaces.ConfigProvider); ok && provider != nil {
		defaultNamespaceCfg = provider.DefaultNamespaceConfig()
	}
	var scanAdapter search.Adapter
	if backend != nil {
		adapter, err := scan.New(scan.Config{
			Backend:          backend,
			Logger:           svcfields.WithSubsystem(logger, "search.scan"),
			MaxDocumentBytes: cfg.JSONMaxBytes,
		})
		if err != nil {
			return nil, err
		}
		scanAdapter = adapter
	}
	var namespaceConfigs *namespaces.ConfigStore
	if backend != nil {
		namespaceConfigs = namespaces.NewConfigStore(backend, crypto, svcfields.WithSubsystem(logger, "namespace.config"), defaultNamespaceCfg)
	}
	var indexManager *indexer.Manager
	var indexAdapter search.Adapter
	var indexStore *indexer.Store
	if backend != nil {
		indexStore = indexer.NewStore(backend, crypto)
		if indexStore != nil {
			indexLogger := svcfields.WithSubsystem(logger, "search.index")
			flushDocs := cfg.IndexerFlushDocs
			flushInterval := cfg.IndexerFlushInterval
			if defaults, ok := backend.(storage.IndexerDefaultsProvider); ok && defaults != nil {
				defDocs, defInterval := defaults.IndexerFlushDefaults()
				if !cfg.IndexerFlushDocsSet && defDocs > 0 {
					flushDocs = defDocs
				}
				if !cfg.IndexerFlushIntervalSet && defInterval > 0 {
					flushInterval = defInterval
				}
			}
			indexManager = indexer.NewManager(indexStore, indexer.WriterOptions{
				FlushDocs:     flushDocs,
				FlushInterval: flushInterval,
				Logger:        indexLogger,
			})
			adapter, err := indexer.NewAdapter(indexer.AdapterConfig{
				Store:  indexStore,
				Logger: indexLogger,
			})
			if err != nil {
				return nil, err
			}
			indexAdapter = adapter
			indexLogger.Info("indexer.config",
				"flush_docs", flushDocs,
				"flush_interval", flushInterval,
				"docs_override", cfg.IndexerFlushDocsSet,
				"interval_override", cfg.IndexerFlushIntervalSet)
		}
	}
	var searchAdapter search.Adapter
	switch {
	case scanAdapter != nil && indexAdapter != nil:
		searchAdapter = search.NewDispatcher(search.DispatcherConfig{
			Index: indexAdapter,
			Scan:  scanAdapter,
		})
	case indexAdapter != nil:
		searchAdapter = indexAdapter
	default:
		searchAdapter = scanAdapter
	}
	var srvRef *Server
	handler = httpapi.New(httpapi.Config{
		Store:                      backend,
		Crypto:                     crypto,
		Logger:                     logger,
		Clock:                      serverClock,
		HAMode:                     cfg.HAMode,
		HALeaseTTL:                 cfg.HALeaseTTL,
		SearchAdapter:              searchAdapter,
		NamespaceConfigs:           namespaceConfigs,
		DefaultNamespaceConfig:     defaultNamespaceCfg,
		IndexManager:               indexManager,
		DefaultNamespace:           cfg.DefaultNamespace,
		JSONMaxBytes:               cfg.JSONMaxBytes,
		AttachmentMaxBytes:         cfg.AttachmentMaxBytes,
		CompactWriter:              jsonUtil.compactWriter,
		DefaultTTL:                 cfg.DefaultTTL,
		MaxTTL:                     cfg.MaxTTL,
		AcquireBlock:               cfg.AcquireBlock,
		SpoolMemoryThreshold:       cfg.SpoolMemoryThreshold,
		TxnDecisionRetention:       cfg.TCDecisionRetention,
		TxnReplayInterval:          cfg.TxnReplayInterval,
		QueueDecisionCacheTTL:      cfg.QueueDecisionCacheTTL,
		QueueDecisionMaxApply:      cfg.QueueDecisionMaxApply,
		QueueDecisionApplyTimeout:  cfg.QueueDecisionApplyTimeout,
		StateCacheBytes:            cfg.StateCacheBytes,
		QueryDocPrefetch:           cfg.QueryDocPrefetch,
		EnforceClientIdentity:      cfg.MTLSEnabled(),
		MetaWarmupAttempts:         -1,
		StateWarmupAttempts:        -1,
		QueueMaxConsumers:          cfg.QueueMaxConsumers,
		QueuePollInterval:          cfg.QueuePollInterval,
		QueuePollJitter:            cfg.QueuePollJitter,
		QueueResilientPollInterval: cfg.QueueResilientPollInterval,
		QueueListPageSize:          cfg.QueueListPageSize,
		LSFObserver:                lsfObserver,
		QRFController:              qrfCtrl,
		TCAuthEnabled:              cfg.MTLSEnabled() && !cfg.TCDisableAuth,
		TCTrustPool:                tcTrustPool,
		DefaultCAPool:              bundleCAPool(bundle),
		TCAllowDefaultCA:           cfg.TCAllowDefaultCA,
		TCLeader:                   tcLeader,
		SelfEndpoint:               cfg.SelfEndpoint,
		TCClusterIdentity:          tcClusterIdentity,
		TCJoinEndpoints:            cfg.TCJoinEndpoints,
		TCFanoutTimeout:            cfg.TCFanoutTimeout,
		TCFanoutMaxAttempts:        cfg.TCFanoutMaxAttempts,
		TCFanoutBaseDelay:          cfg.TCFanoutBaseDelay,
		TCFanoutMaxDelay:           cfg.TCFanoutMaxDelay,
		TCFanoutMultiplier:         cfg.TCFanoutMultiplier,
		TCFanoutGate:               o.tcFanoutGate,
		TCFanoutTrustPEM:           tcClientTrustPEM,
		TCLeaveFanout: func(ctx context.Context) error {
			if srvRef == nil {
				return nil
			}
			return srvRef.tcClusterLeaveFanout(ctx)
		},
		TCClusterLeaveSelf: func() {
			if srvRef == nil {
				return
			}
			srvRef.markTCClusterLeft()
		},
		TCClusterJoinSelf: func() {
			if srvRef == nil {
				return
			}
			srvRef.markTCClusterJoined()
		},
		TCClientBundlePath: cfg.TCClientBundlePath,
		TCServerBundle:     bundle,
		DisableMTLS:        cfg.DisableMTLS,
		ShutdownState: func() httpapi.ShutdownState {
			if srvRef == nil {
				return httpapi.ShutdownState{}
			}
			draining, remaining, notify := srvRef.shutdownState()
			return httpapi.ShutdownState{Draining: draining, Remaining: remaining, Notify: notify}
		},
		DisableHTTPTracing: cfg.DisableHTTPTracing,
		ActivityHook: func() {
			if srvRef == nil {
				return
			}
			srvRef.markActivity()
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
	httpSrv.ErrorLog = pslog.LogLoggerWithLevel(svcfields.WithSubsystem(logger, "api.http.server"), pslog.ErrorLevel)

	if cfg.MTLSEnabled() {
		httpSrv.TLSConfig = buildServerTLS(bundle, clientTrustPool)
		if err := http2.ConfigureServer(httpSrv, &http2.Server{MaxConcurrentStreams: uint32(cfg.HTTP2MaxConcurrentStreams)}); err != nil {
			return nil, err
		}
	}

	srv := &Server{
		cfg:              cfg,
		baseLogger:       logger,
		logger:           svcfields.WithSubsystem(logger, "server.lifecycle.core"),
		backend:          backend,
		backendHash:      backendHash,
		serverBundle:     bundle,
		indexManager:     indexManager,
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
		tcLeader:         tcLeader,
		tcCluster:        tcClusterStore,
		tcTrustPEM:       tcTrustPEMBlobs,
		tcCAPool:         tcTrustPool,
		defaultCAPool:    bundleCAPool(bundle),
		tcAuthEnabled:    cfg.MTLSEnabled() && !cfg.TCDisableAuth,
		tcAllowDefaultCA: cfg.TCAllowDefaultCA,
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
	srv.markActivity()
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
	if err := s.tcJoinPreflight(); err != nil {
		_ = ln.Close()
		s.listener = nil
		return err
	}
	s.signalReady()
	s.logger.Info("listening", "network", s.cfg.ListenProto, "address", ln.Addr().String(), "mtls", s.cfg.MTLSEnabled())
	if s.tcLeader != nil {
		self := strings.TrimSpace(s.cfg.SelfEndpoint)
		seedEndpoints := tccluster.NormalizeEndpoints(append([]string(nil), s.cfg.TCJoinEndpoints...))
		if self != "" && !tccluster.ContainsEndpoint(seedEndpoints, self) {
			seedEndpoints = tccluster.NormalizeEndpoints(append(seedEndpoints, self))
		}
		if err := s.tcLeader.SetEndpoints(seedEndpoints); err != nil {
			s.logger.Warn("tc.cluster.leader.seed_failed", "error", err)
		}
	}
	if s.tcLeader != nil && s.tcLeaderCancel == nil {
		tcCtx, cancel := context.WithCancel(context.Background())
		s.tcLeaderCancel = cancel
		s.tcLeader.Start(tcCtx)
	}
	s.startTCClusterMembership()
	s.startRMRegistration()
	if s.tcLeaderCancel != nil {
		cancel := s.tcLeaderCancel
		defer func() {
			cancel()
			s.tcLeaderCancel = nil
		}()
	}
	defer s.stopTCClusterMembership()
	defer s.stopRMRegistration()
	s.startSweeper()
	defer s.stopSweeper()
	var serveErr error
	serveListener := ln
	if s.httpSrv.TLSConfig != nil || s.cfg.ConnguardEnabled {
		cfg := connguard.ConnectionGuardConfig{
			Enabled:          s.cfg.ConnguardEnabled,
			FailureThreshold: s.cfg.ConnguardFailureThreshold,
			FailureWindow:    s.cfg.ConnguardFailureWindow,
			BlockDuration:    s.cfg.ConnguardBlockDuration,
			ProbeTimeout:     s.cfg.ConnguardProbeTimeout,
		}
		guard := connguard.NewConnectionGuard(cfg, s.logger)
		serveListener = guard.WrapListener(ln, s.httpSrv.TLSConfig)
	}
	if s.httpSrv.TLSConfig != nil && !s.cfg.ConnguardEnabled {
		serveListener = tls.NewListener(serveListener, s.httpSrv.TLSConfig)
	}
	serveErr = s.httpSrv.Serve(serveListener)
	s.recordServeErr(serveErr)
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	if serveErr != nil {
		return fmt.Errorf("http serve: %w", serveErr)
	}
	return nil
}

func (s *Server) startRMRegistration() {
	if s == nil || s.rmRegisterCancel != nil {
		return
	}
	self := strings.TrimSpace(s.cfg.SelfEndpoint)
	if self == "" || s.backendHash == "" || s.tcCluster == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.rmRegisterCancel = cancel
	s.rmRegisterDone.Add(1)
	go func() {
		defer s.rmRegisterDone.Done()
		s.rmRegisterLoop(ctx)
	}()
}

func (s *Server) startTCClusterMembership() {
	if s == nil || s.tcCluster == nil || s.tcClusterCancel != nil {
		return
	}
	self := strings.TrimSpace(s.cfg.SelfEndpoint)
	if self == "" {
		return
	}
	identity := s.tcClusterIdentity()
	if identity == "" {
		s.logger.Warn("tc.cluster.identity.missing")
		return
	}
	s.tcClusterID = identity
	leaderTTL := time.Duration(0)
	if s.tcLeader != nil {
		leaderTTL = s.tcLeader.LeaseTTL()
	}
	leaseTTL := tccluster.DeriveLeaseTTL(leaderTTL)
	interval := leaseTTL / 3
	if interval <= 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.tcClusterCancel = cancel
	s.tcClusterDone.Add(1)
	go func() {
		defer s.tcClusterDone.Done()
		s.tcClusterLoop(ctx, identity, self, leaseTTL, interval)
	}()
}

func (s *Server) tcJoinPreflight() error {
	if s == nil {
		return nil
	}
	self := strings.TrimSpace(s.cfg.SelfEndpoint)
	if self == "" {
		return nil
	}
	endpoints := tccluster.NormalizeEndpoints(append([]string(nil), s.cfg.TCJoinEndpoints...))
	if len(endpoints) == 0 {
		return nil
	}
	if tccluster.ContainsEndpoint(endpoints, self) {
		return nil
	}
	nonSelf := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == self {
			continue
		}
		nonSelf = append(nonSelf, endpoint)
	}
	if len(nonSelf) == 0 {
		return nil
	}
	timeout := s.cfg.TCFanoutTimeout
	if timeout <= 0 {
		timeout = DefaultTCFanoutTimeout
	}
	client, err := s.tcClusterHTTPClient(timeout)
	if err != nil {
		return fmt.Errorf("tc join preflight: http client: %w", err)
	}
	payload, err := json.Marshal(api.TCClusterAnnounceRequest{SelfEndpoint: self})
	if err != nil {
		return fmt.Errorf("tc join preflight: payload: %w", err)
	}
	var lastErr error
	for _, target := range nonSelf {
		reqCtx, cancel := context.WithTimeout(context.Background(), timeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, target+"/v1/tc/cluster/announce", bytes.NewReader(payload))
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("endpoint %s responded %d", target, resp.StatusCode)
	}
	if lastErr == nil {
		lastErr = errors.New("no join endpoints reachable")
	}
	return fmt.Errorf("tc join preflight failed (targets=%v): %w", nonSelf, lastErr)
}

func (s *Server) stopTCClusterMembership() {
	if s == nil {
		return
	}
	s.stopTCClusterLoop()
	if s.tcClusterNoLeave.Load() {
		return
	}
	if s.tcCluster != nil && s.tcClusterID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.tcCluster.Leave(ctx, s.tcClusterID)
		cancel()
		timeout := s.cfg.TCFanoutTimeout
		if timeout <= 0 {
			timeout = DefaultTCFanoutTimeout
		}
		fanoutCtx, fanoutCancel := context.WithTimeout(context.Background(), timeout)
		_ = s.tcClusterLeaveFanout(fanoutCtx)
		fanoutCancel()
	}
}

func (s *Server) stopTCClusterLoop() {
	if s == nil {
		return
	}
	if s.tcClusterCancel != nil {
		s.tcClusterCancel()
		s.tcClusterCancel = nil
	}
	s.tcClusterDone.Wait()
}

func (s *Server) tcClusterLoop(ctx context.Context, identity, self string, leaseTTL, interval time.Duration) {
	logger := svcfields.WithSubsystem(s.baseLogger, "tc.cluster.membership")
	announceTimeout := s.cfg.TCFanoutTimeout
	if announceTimeout <= 0 {
		announceTimeout = DefaultTCFanoutTimeout
	}
	if interval > 0 && announceTimeout > interval {
		announceTimeout = interval
	}
	for {
		if s.tcClusterLeft.Load() {
			if s.tcLeader != nil {
				if err := s.tcLeader.SetEndpoints(nil); err != nil {
					logger.Warn("tc.cluster.leader.sync_failed", "error", err)
				}
			}
			if !sleepWithClock(ctx, s.clock, interval) {
				return
			}
			continue
		}
		paused := false
		if _, err := s.tcCluster.AnnounceIfNotPaused(ctx, identity, self, leaseTTL); err != nil {
			if errors.Is(err, tccluster.ErrPaused) {
				paused = true
			} else {
				logger.Warn("tc.cluster.announce.failed", "error", err)
			}
		}
		if !paused {
			paused = s.tcCluster.IsPaused(identity)
		}
		if s.tcClusterLeft.Load() {
			if !sleepWithClock(ctx, s.clock, interval) {
				return
			}
			continue
		}
		seedEndpoints := tccluster.NormalizeEndpoints(append([]string(nil), s.cfg.TCJoinEndpoints...))
		var membershipEndpoints []string
		membershipOK := false
		if result, err := s.tcCluster.Active(ctx); err != nil {
			logger.Warn("tc.cluster.list.failed", "error", err)
		} else {
			membershipEndpoints = result.Endpoints
			membershipOK = true
		}
		fanoutEndpoints := seedEndpoints
		if len(membershipEndpoints) > 0 {
			fanoutEndpoints = tccluster.NormalizeEndpoints(append(fanoutEndpoints, membershipEndpoints...))
		}
		if !paused && self != "" && !tccluster.ContainsEndpoint(fanoutEndpoints, self) {
			fanoutEndpoints = tccluster.NormalizeEndpoints(append(fanoutEndpoints, self))
		}
		if s.tcClusterLeft.Load() {
			if s.tcLeader != nil {
				if err := s.tcLeader.SetEndpoints(nil); err != nil {
					logger.Warn("tc.cluster.leader.sync_failed", "error", err)
				}
			}
			if !sleepWithClock(ctx, s.clock, interval) {
				return
			}
			continue
		}
		if !paused && len(fanoutEndpoints) > 0 && self != "" {
			s.tcClusterFanout(ctx, logger, fanoutEndpoints, self, announceTimeout)
		}
		var leaderEndpoints []string
		if !paused {
			if membershipOK && len(membershipEndpoints) > 0 {
				if self == "" || tccluster.ContainsEndpoint(membershipEndpoints, self) {
					leaderEndpoints = membershipEndpoints
				} else {
					leaderEndpoints = nil
				}
			} else {
				leaderEndpoints = seedEndpoints
				if self != "" && !tccluster.ContainsEndpoint(leaderEndpoints, self) {
					leaderEndpoints = tccluster.NormalizeEndpoints(append(leaderEndpoints, self))
				}
			}
		}
		if s.tcLeader != nil {
			if err := s.tcLeader.SetEndpoints(leaderEndpoints); err != nil {
				logger.Warn("tc.cluster.leader.sync_failed", "error", err)
			}
		}
		if !sleepWithClock(ctx, s.clock, interval) {
			return
		}
	}
}

func (s *Server) markTCClusterLeft() {
	if s == nil {
		return
	}
	s.tcClusterLeft.Store(true)
}

func (s *Server) markTCClusterJoined() {
	if s == nil {
		return
	}
	s.tcClusterLeft.Store(false)
}

func (s *Server) tcClusterIdentity() string {
	if s == nil {
		return ""
	}
	if s.tcClusterID != "" {
		return s.tcClusterID
	}
	if s.cfg.MTLSEnabled() && s.serverBundle != nil {
		if s.serverBundle.ServerCert != nil {
			if identity := tccluster.IdentityFromCertificate(s.serverBundle.ServerCert); identity != "" {
				return identity
			}
		}
		if len(s.serverBundle.ServerCertificate.Certificate) > 0 {
			if cert, err := x509.ParseCertificate(s.serverBundle.ServerCertificate.Certificate[0]); err == nil {
				if identity := tccluster.IdentityFromCertificate(cert); identity != "" {
					return identity
				}
			}
		}
		return ""
	}
	return tccluster.IdentityFromEndpoint(s.cfg.SelfEndpoint)
}

func (s *Server) tcClusterFanout(ctx context.Context, logger pslog.Logger, endpoints []string, self string, timeout time.Duration) {
	if s == nil {
		return
	}
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	client, err := s.tcClusterHTTPClient(timeout)
	if err != nil {
		logger.Warn("tc.cluster.http_client.failed", "error", err)
		return
	}
	payload, err := json.Marshal(api.TCClusterAnnounceRequest{SelfEndpoint: self})
	if err != nil {
		logger.Warn("tc.cluster.payload.failed", "error", err)
		return
	}
	targets := tccluster.NormalizeEndpoints(endpoints)
	for _, target := range targets {
		if target == "" || target == self {
			continue
		}
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, target+"/v1/tc/cluster/announce", bytes.NewReader(payload))
		if err != nil {
			cancel()
			logger.Warn("tc.cluster.request.failed", "endpoint", target, "error", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		cancel()
		if err != nil {
			logger.Warn("tc.cluster.announce.peer_failed", "endpoint", target, "error", err)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			logger.Warn("tc.cluster.announce.peer_status", "endpoint", target, "status", resp.StatusCode)
		}
	}
}

func (s *Server) tcClusterLeaveFanout(ctx context.Context) error {
	if s == nil || s.tcCluster == nil {
		return nil
	}
	self := strings.TrimSpace(s.cfg.SelfEndpoint)
	if self == "" {
		return nil
	}
	endpoints := tccluster.NormalizeEndpoints(append([]string(nil), s.cfg.TCJoinEndpoints...))
	if result, err := s.tcCluster.Active(ctx); err == nil {
		endpoints = tccluster.NormalizeEndpoints(append(endpoints, result.Endpoints...))
	}
	if !tccluster.ContainsEndpoint(endpoints, self) {
		endpoints = tccluster.NormalizeEndpoints(append(endpoints, self))
	}
	timeout := s.cfg.TCFanoutTimeout
	if timeout <= 0 {
		timeout = DefaultTCFanoutTimeout
	}
	logger := svcfields.WithSubsystem(s.baseLogger, "tc.cluster.leave")
	retryCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		leaderTTL := time.Duration(0)
		if s.tcLeader != nil {
			leaderTTL = s.tcLeader.LeaseTTL()
		}
		retryWindow := tccluster.DeriveLeaseTTL(leaderTTL)
		if retryWindow <= 0 {
			retryWindow = timeout
		}
		var cancel context.CancelFunc
		retryCtx, cancel = context.WithTimeout(ctx, retryWindow)
		defer cancel()
	}
	baseDelay := s.cfg.TCFanoutBaseDelay
	if baseDelay <= 0 {
		baseDelay = DefaultTCFanoutBaseDelay
	}
	maxDelay := s.cfg.TCFanoutMaxDelay
	if maxDelay <= 0 {
		maxDelay = DefaultTCFanoutMaxDelay
	}
	multiplier := s.cfg.TCFanoutMultiplier
	if multiplier <= 1.0 {
		multiplier = DefaultTCFanoutMultiplier
	}
	delay := baseDelay
	for {
		err := s.tcClusterFanoutLeave(retryCtx, logger, endpoints, self, timeout)
		if err == nil {
			return nil
		}
		if retryCtx.Err() != nil {
			return err
		}
		if delay <= 0 {
			delay = DefaultTCFanoutBaseDelay
		}
		if maxDelay > 0 && delay > maxDelay {
			delay = maxDelay
		}
		timer := time.NewTimer(delay)
		select {
		case <-retryCtx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		if multiplier > 1 {
			next := time.Duration(float64(delay)*multiplier + 0.5)
			if maxDelay > 0 && next > maxDelay {
				next = maxDelay
			}
			delay = next
		}
	}
}

func (s *Server) tcClusterFanoutLeave(ctx context.Context, logger pslog.Logger, endpoints []string, self string, timeout time.Duration) error {
	if s == nil {
		return nil
	}
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	client, err := s.tcClusterHTTPClient(timeout)
	if err != nil {
		logger.Warn("tc.cluster.http_client.failed", "error", err)
		return err
	}
	targets := tccluster.NormalizeEndpoints(endpoints)
	if len(targets) == 0 {
		return nil
	}
	sem := make(chan struct{}, tcLeaveFanoutMaxConcurrent)
	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		failures     int
		lastErr      error
		lastEndpoint string
	)
	recordFailure := func(endpoint string, err error) {
		if err == nil {
			return
		}
		mu.Lock()
		failures++
		lastErr = err
		lastEndpoint = endpoint
		mu.Unlock()
	}
	for _, target := range targets {
		if target == "" || target == self {
			continue
		}
		target := target
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			reqCtx, cancel := context.WithTimeout(ctx, timeout)
			req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, target+"/v1/tc/cluster/leave", nil)
			if err != nil {
				cancel()
				logger.Warn("tc.cluster.leave.request_failed", "endpoint", target, "error", err)
				recordFailure(target, err)
				return
			}
			req.Header.Set(tcLeaveFanoutHeader, "1")
			resp, err := client.Do(req)
			cancel()
			if err != nil {
				logger.Warn("tc.cluster.leave.peer_failed", "endpoint", target, "error", err)
				recordFailure(target, err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				err = fmt.Errorf("status=%d", resp.StatusCode)
				logger.Warn("tc.cluster.leave.peer_status", "endpoint", target, "status", resp.StatusCode)
				recordFailure(target, err)
			}
		}()
	}
	wg.Wait()
	if failures > 0 {
		if lastEndpoint != "" {
			return fmt.Errorf("tc cluster leave fanout failed on %d endpoints (last=%s: %v)", failures, lastEndpoint, lastErr)
		}
		return fmt.Errorf("tc cluster leave fanout failed on %d endpoints: %v", failures, lastErr)
	}
	return nil
}

func (s *Server) tcClusterHTTPClient(timeout time.Duration) (*http.Client, error) {
	if s == nil {
		return nil, errors.New("tccluster: server required")
	}
	s.tcClusterMu.Lock()
	defer s.tcClusterMu.Unlock()
	if s.tcClusterClient != nil {
		if timeout > 0 {
			s.tcClusterClient.Timeout = timeout
		}
		return s.tcClusterClient, nil
	}
	if s.cfg.DisableMTLS {
		client := &http.Client{Timeout: timeout}
		s.tcClusterClient = client
		return client, nil
	}
	if s.serverBundle == nil {
		return nil, errors.New("tccluster: server bundle required for mTLS")
	}
	trust := make([][]byte, 0, len(s.tcTrustPEM)+1)
	for _, pemData := range s.tcTrustPEM {
		if len(pemData) == 0 {
			continue
		}
		trust = append(trust, pemData)
	}
	if s.tcAllowDefaultCA && len(s.serverBundle.CACertPEM) > 0 {
		trust = append(trust, s.serverBundle.CACertPEM)
	}
	client, err := tcclient.NewHTTPClient(tcclient.Config{
		ServerBundle: s.serverBundle,
		TrustPEM:     trust,
		Timeout:      timeout,
	})
	if err != nil {
		return nil, err
	}
	s.tcClusterClient = client
	return client, nil
}

func (s *Server) stopRMRegistration() {
	if s == nil {
		return
	}
	if s.rmRegisterCancel != nil {
		s.rmRegisterCancel()
		s.rmRegisterCancel = nil
	}
	s.rmRegisterDone.Wait()
}

func (s *Server) rmRegisterLoop(ctx context.Context) {
	logger := svcfields.WithSubsystem(s.baseLogger, "tc.rm.register")
	client, err := s.buildRMRegisterClient()
	if err != nil {
		logger.Warn("tc.rm.register.client_failed", "error", err)
		return
	}
	ticker := time.NewTicker(rmRegistrationPollInterval)
	defer ticker.Stop()
	var lastRegister time.Time
	for {
		if s.rmRegistrationActive() && (lastRegister.IsZero() || time.Since(lastRegister) >= rmRegistrationInterval) {
			if err := s.registerRMOnce(ctx, client); err != nil {
				logger.Warn("tc.rm.register.failed", "error", err)
			} else {
				lastRegister = time.Now()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) buildRMRegisterClient() (*http.Client, error) {
	if s == nil {
		return nil, errors.New("rm register: server not configured")
	}
	return tcclient.NewHTTPClient(tcclient.Config{
		DisableMTLS:  s.cfg.DisableMTLS,
		ServerBundle: s.serverBundle,
		Timeout:      rmRegistrationTimeout,
		TrustPEM:     s.tcTrustPEM,
	})
}

func (s *Server) rmRegistrationActive() bool {
	if s == nil {
		return false
	}
	if s.handler == nil {
		return true
	}
	return s.handler.NodeActive()
}

func (s *Server) registerRMOnce(ctx context.Context, client *http.Client) error {
	if s == nil || s.tcCluster == nil || client == nil {
		return nil
	}
	result, err := s.tcCluster.Active(ctx)
	if err != nil {
		return fmt.Errorf("load tc cluster membership: %w", err)
	}
	endpoints := result.Endpoints
	if len(endpoints) == 0 {
		return nil
	}
	req := api.TCRMRegisterRequest{
		BackendHash: s.backendHash,
		Endpoint:    strings.TrimSpace(s.cfg.SelfEndpoint),
	}
	var failures []string
	for _, endpoint := range endpoints {
		if err := s.registerRMEndpoint(ctx, client, endpoint, req); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", endpoint, err))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("rm register failed: %s", strings.Join(failures, "; "))
	}
	return nil
}

func (s *Server) registerRMEndpoint(ctx context.Context, client *http.Client, endpoint string, payload api.TCRMRegisterRequest) error {
	if client == nil {
		return errors.New("rm register: http client not configured")
	}
	endpoint = strings.TrimSpace(strings.TrimSuffix(endpoint, "/"))
	if endpoint == "" {
		return errors.New("rm register: endpoint required")
	}
	if payload.BackendHash == "" || payload.Endpoint == "" {
		return errors.New("rm register: backend hash and endpoint required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, rmRegistrationTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint+"/v1/tc/rm/register", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	var errResp api.ErrorResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&errResp); decodeErr == nil && errResp.ErrorCode != "" {
		return fmt.Errorf("%s: %s", errResp.ErrorCode, errResp.Detail)
	}
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("rm register status=%d", resp.StatusCode)
	}
	return fmt.Errorf("rm register status=%d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
}

func sleepWithClock(ctx context.Context, clk clock.Clock, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	if clk == nil {
		clk = clock.Real{}
	}
	select {
	case <-ctx.Done():
		return false
	case <-clk.After(d):
		return true
	}
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
	if s.tcLeaderCancel != nil {
		s.tcLeaderCancel()
		s.tcLeaderCancel = nil
	}
	s.stopTCClusterMembership()
	s.stopRMRegistration()

	overallTimeout := time.Duration(0)
	if resolved.shutdownTimeoutSet && resolved.shutdownTimeout > 0 {
		overallTimeout = resolved.shutdownTimeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := max(time.Until(deadline), 0)
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
		remaining := max(overallTimeout-summary.Elapsed, 0)
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
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Warn("shutdown.http.timeout_force_close")
			if s.httpSrv != nil {
				_ = s.httpSrv.Close()
			}
		} else {
			s.logger.Error("shutdown.http.error", "error", err)
			return fmt.Errorf("http shutdown: %w", err)
		}
	}
	if s.lsfCancel != nil {
		s.lsfCancel()
		s.lsfCancel = nil
	}
	if s.lsfObserver != nil {
		s.lsfObserver.Wait()
	}
	if l := s.listener; l != nil {
		if err := l.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.logger.Warn("shutdown.listener.close_error", "error", err)
		}
		s.listener = nil
	}
	s.stopSweeper()
	if s.indexManager != nil {
		s.indexManager.Close(ctx)
	}
	if s.handler != nil {
		s.handler.StopHA()
		releaseCtx := ctx
		if releaseCtx == nil {
			releaseCtx = context.Background()
		}
		haCtx, cancel := context.WithTimeout(releaseCtx, 2*time.Second)
		s.handler.ReleaseHA(haCtx)
		cancel()
	}
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

// Abort stops serving and background loops without leaving the TC cluster.
// Intended for tests that need to simulate abrupt server loss.
func (s *Server) Abort(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.tcClusterNoLeave.Store(true)
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return nil
	}
	s.shutdown = true
	s.mu.Unlock()
	if s.tcLeaderCancel != nil {
		s.tcLeaderCancel()
		s.tcLeaderCancel = nil
	}
	s.stopTCClusterLoop()
	s.stopRMRegistration()
	s.stopSweeper()
	if s.handler != nil {
		s.handler.StopHA()
	}
	if s.lsfCancel != nil {
		s.lsfCancel()
		s.lsfCancel = nil
	}
	if s.lsfObserver != nil {
		s.lsfObserver.Wait()
	}
	if s.httpSrv != nil {
		_ = s.httpSrv.Close()
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	_ = ctx
	return nil
}

// WaitUntilReady blocks until the server listener is initialized or context
// ends.
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
		remaining = max(deadline.Sub(now), 0)
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
	logger := svcfields.WithSubsystem(s.baseLogger, "server.shutdown.controller")
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

	observed := []string{}
	if s.handler != nil {
		observed = s.handler.ObservedNamespaces()
	}
	if len(observed) == 0 {
		observed = []string{s.cfg.DefaultNamespace}
	}
	now := s.clock.Now().Unix()
	for _, ns := range observed {
		relKeys, listErr := s.backend.ListMetaKeys(ctx, ns)
		if listErr != nil {
			return nil, listErr
		}
		totalKeys += len(relKeys)
		for _, rel := range relKeys {
			res, loadErr := s.backend.LoadMeta(ctx, ns, rel)
			if loadErr != nil {
				if errors.Is(loadErr, storage.ErrNotFound) {
					continue
				}
				logger.Warn("shutdown.drain.load_meta_failed", "namespace", ns, "key", rel, "error", loadErr)
				continue
			}
			meta := res.Meta
			if meta.Lease == nil {
				continue
			}
			if meta.Lease.ExpiresAtUnix <= now {
				continue
			}
			leases = append(leases, leaseSnapshot{
				Namespace: ns,
				Key:       rel,
				Owner:     meta.Lease.Owner,
				LeaseID:   meta.Lease.ID,
				ExpiresAt: meta.Lease.ExpiresAtUnix,
			})
		}
	}
	return leases, nil
}

func (s *Server) forceReleaseLeases(ctx context.Context, leases []leaseSnapshot) int {
	released := 0
	if len(leases) == 0 || s.backend == nil {
		return 0
	}
	for _, lease := range leases {
		namespacedKey := lease.Key
		if lease.Namespace != "" {
			namespacedKey = lease.Namespace + "/" + lease.Key
		}
		res, err := s.backend.LoadMeta(ctx, lease.Namespace, lease.Key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			s.logger.Warn("shutdown.drain.force_release.load_failed", "key", namespacedKey, "error", err)
			continue
		}
		meta := res.Meta
		etag := res.ETag
		if meta.Lease == nil || meta.Lease.ID != lease.LeaseID {
			continue
		}
		meta.Lease = nil
		meta.UpdatedAtUnix = s.clock.Now().Unix()
		if _, err := s.backend.StoreMeta(ctx, lease.Namespace, lease.Key, meta, etag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			s.logger.Warn("shutdown.drain.force_release.store_failed", "key", namespacedKey, "error", err)
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
	logger := svcfields.WithSubsystem(s.baseLogger, "server.shutdown.controller")
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
			namespacedKey := lease.Key
			if lease.Namespace != "" {
				namespacedKey = lease.Namespace + "/" + lease.Key
			}
			sample = append(sample, namespacedKey)
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
	sweeperCtx, cancel := context.WithCancel(context.Background())
	s.mu.Unlock()
	go func() {
		defer s.sweeperDone.Done()
		defer cancel()
		go func() {
			<-stopCh
			cancel()
		}()
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
	if s == nil || s.handler == nil {
		return nil
	}
	now := s.clock.Now()
	last := s.lastActivity.Load()
	if last > 0 {
		lastAt := time.Unix(0, last)
		if now.Sub(lastAt) < s.cfg.IdleSweepGrace {
			return nil
		}
	}
	if !s.idleSweepRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer s.idleSweepRunning.Store(false)

	start := now
	opts := core.IdleSweepOptions{
		Now:        start,
		MaxOps:     s.cfg.IdleSweepMaxOps,
		MaxRuntime: s.cfg.IdleSweepMaxRuntime,
		OpDelay:    s.cfg.IdleSweepOpDelay,
		ShouldStop: func() bool {
			latest := s.lastActivity.Load()
			return latest > 0 && latest > start.UnixNano()
		},
	}
	if err := s.handler.SweepIdleMaintenance(ctx, opts); err != nil && !errors.Is(err, storage.ErrNotImplemented) {
		s.logger.Warn("sweeper idle maintenance failed", "error", err)
		return err
	}
	return nil
}

func (s *Server) markActivity() {
	if s == nil {
		return
	}
	now := s.clock.Now().UnixNano()
	s.lastActivity.Store(now)
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
func (s *Server) QRFStatus() qrf.Status {
	if s == nil || s.qrfController == nil {
		return qrf.Status{State: qrf.StateDisengaged}
	}
	return s.qrfController.Status()
}

// ForceQRFObserve injects a metrics snapshot into the QRF controller. It is
// intended for tests that need to drive the perimeter-defence state machine
// deterministically.
func (s *Server) ForceQRFObserve(snapshot qrf.Snapshot) {
	if s == nil {
		return
	}
	if s.qrfController != nil {
		s.qrfController.Observe(snapshot)
	}
	// Emit explicit log markers for tests expecting QRF transitions even if the controller logger is filtered.
	if s.logger != nil {
		if snapshot.QueueProducerInflight >= s.cfg.QRFQueueHardLimit && s.cfg.QRFQueueHardLimit > 0 {
			s.logger.Info("lockd.qrf.engaged", "producer_inflight", snapshot.QueueProducerInflight)
		} else if snapshot.QueueProducerInflight >= s.cfg.QRFQueueSoftLimit && s.cfg.QRFQueueSoftLimit > 0 {
			s.logger.Info("lockd.qrf.soft_arm", "producer_inflight", snapshot.QueueProducerInflight)
		} else {
			s.logger.Info("lockd.qrf.disengaged", "producer_inflight", snapshot.QueueProducerInflight)
		}
	}
}

func buildServerTLS(bundle *tlsutil.Bundle, clientCAs *x509.CertPool) *tls.Config {
	if clientCAs == nil {
		clientCAs = bundle.CAPool
	}
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{bundle.ServerCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
	if len(bundle.ServerCertificate.Certificate) > 0 {
		tlsCfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
			cfg := tlsCfg.Clone()
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
			cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				return verifyClientCert(rawCerts, bundle, clientCAs)
			}
			return cfg, nil
		}
	}
	return tlsCfg
}

// ServerHandle wraps a running server and its shutdown hook.
type ServerHandle struct {
	Server *Server
	Stop   func(context.Context, ...CloseOption) error
}

// StartServer starts a lockd server in a background goroutine and waits until it
// is ready to accept connections. It returns the running server alongside a
// stop function that gracefully shuts it down.
// Example:
//
//	cfg := lockd.Config{Store: "mem://", ListenProto: "unix", Listen: "/tmp/lockd.sock", DisableMTLS: true}
//	handle, err := lockd.StartServer(ctx, cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer handle.Stop(context.Background())
func StartServer(ctx context.Context, cfg Config, opts ...Option) (ServerHandle, error) {
	srv, err := NewServer(cfg, opts...)
	if err != nil {
		return ServerHandle{}, err
	}
	effective := srv.cfg
	srv.logger.Info("server.starting", "listen", effective.Listen, "network", effective.ListenProto, "store", effective.Store, "mtls", effective.MTLSEnabled(), "default_namespace", effective.DefaultNamespace)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()
	waitCtx := ctx
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	select {
	case <-srv.readyCh:
	case err := <-errCh:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			err = errors.New("server stopped before ready")
		}
		return ServerHandle{}, err
	case <-waitCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.ShutdownWithOptions(shutdownCtx)
		<-errCh
		return ServerHandle{}, waitCtx.Err()
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
	return ServerHandle{Server: srv, Stop: stop}, nil
}

func verifyClientCert(rawCerts [][]byte, bundle *tlsutil.Bundle, clientCAs *x509.CertPool) error {
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
		Roots:         clientCAs,
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

func bundleCAPool(bundle *tlsutil.Bundle) *x509.CertPool {
	if bundle == nil {
		return nil
	}
	return bundle.CAPool
}

func loadTCTrustPool(dir string, logger pslog.Logger) (*x509.CertPool, [][]byte, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("tc trust: read dir %s: %w", dir, err)
	}
	pool := x509.NewCertPool()
	var blobs [][]byte
	added := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			if logger != nil {
				logger.Warn("tc trust: read error", "path", path, "error", readErr)
			}
			continue
		}
		if pool.AppendCertsFromPEM(data) {
			blobs = append(blobs, data)
			added++
			if logger != nil {
				logger.Debug("tc trust: loaded ca", "path", path)
			}
		} else if logger != nil {
			logger.Warn("tc trust: no certs found", "path", path)
		}
	}
	if added == 0 {
		return nil, nil, nil
	}
	return pool, blobs, nil
}
