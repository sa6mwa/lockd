package core

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/lsf"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/search"
	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

const (
	defaultQueryDocPrefetch = 8
	maxQueryDocPrefetch     = 64
)

// Service aggregates transport-agnostic domain services.
type Service struct {
	store                     storage.Backend
	backendHash               string
	crypto                    *storage.Crypto
	queueProvider             QueueProvider
	queueDispatcher           QueueDispatcher
	searchAdapter             search.Adapter
	namespaceConfigs          *namespaces.ConfigStore
	defaultNamespace          string
	defaultTTL                TTLConfig
	acquireBlock              time.Duration
	jsonMaxBytes              int64
	attachmentMaxBytes        int64
	spoolThreshold            int64
	txnDecisionRetention      time.Duration
	txnReplayInterval         time.Duration
	queueDecisionCacheTTL     time.Duration
	queueDecisionMaxApply     int
	queueDecisionApplyTimeout time.Duration
	stateCache                *stateCache
	defaultNamespaceConfig    namespaces.Config
	logger                    pslog.Logger
	clock                     clock.Clock
	qrf                       *qrf.Controller
	lsf                       *lsf.Observer
	indexManager              *indexer.Manager
	namespaceTracker          *NamespaceTracker
	metaWarmup                WarmupConfig
	stateWarmup               WarmupConfig
	shutdownState             func() ShutdownState
	enforceIdentity           bool

	createLocks *sync.Map

	staging storage.StagingBackend

	txnDecisionApplied atomic.Int64
	txnDecisionFailed  atomic.Int64
	txnMetrics         *txnMetrics
	queueMetrics       *queueMetrics
	leaseMetrics       *leaseMetrics
	attachmentMetrics  *attachmentMetrics
	sweeperMetrics     *sweeperMetrics
	tcDecider          TCDecider

	haMode         string
	haLeaseTTL     time.Duration
	haNodeID       string
	haActive       atomic.Bool
	haLeaseExpires atomic.Int64
	haRefreshes    atomic.Int64
	haErrors       atomic.Int64
	haTransitions  atomic.Int64
	haStop         chan struct{}
	haDone         chan struct{}

	leaseBucketCache    sync.Map
	decisionBucketCache sync.Map
	leaseSweepCursors   sync.Map
	decisionSweepCursor sweepCursor
	txnSweepCursor      sweepCursor
	txnReplayRunning    atomic.Bool
	txnReplayLast       atomic.Int64
	queueDecisionCache  sync.Map

	queryDocPrefetch int

	indexRebuildMu sync.Mutex
	indexRebuilds  map[string]*indexRebuildState
}

// New constructs the core Service with sane defaults.
func New(cfg Config) *Service {
	logger := cfg.Logger
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	defaultNS := cfg.DefaultNamespace
	if defaultNS == "" {
		defaultNS = namespaces.Default
	}
	nsTracker := cfg.NamespaceTracker
	if nsTracker == nil {
		nsTracker = NewNamespaceTracker(defaultNS)
	} else {
		nsTracker.Observe(defaultNS)
	}
	defaultCfg := cfg.DefaultNamespaceConfig
	defaultCfg.Normalize()
	if err := defaultCfg.Validate(); err != nil {
		defaultCfg = namespaces.DefaultConfig()
	}
	queueDecisionCacheTTL := cfg.QueueDecisionCacheTTL
	if queueDecisionCacheTTL < 0 {
		queueDecisionCacheTTL = 0
	}
	if queueDecisionCacheTTL == 0 {
		queueDecisionCacheTTL = time.Minute
	}
	queueDecisionMaxApply := cfg.QueueDecisionMaxApply
	if queueDecisionMaxApply <= 0 {
		queueDecisionMaxApply = 50
	}
	queueDecisionApplyTimeout := cfg.QueueDecisionApplyTimeout
	if queueDecisionApplyTimeout <= 0 {
		queueDecisionApplyTimeout = 2 * time.Second
	}
	queryDocPrefetch := cfg.QueryDocPrefetch
	if queryDocPrefetch <= 0 {
		queryDocPrefetch = defaultQueryDocPrefetch
	}
	if queryDocPrefetch > maxQueryDocPrefetch {
		queryDocPrefetch = maxQueryDocPrefetch
	}
	stateCache := newStateCache(cfg.StateCacheBytes)
	haMode := strings.ToLower(strings.TrimSpace(cfg.HAMode))
	if haMode == "" {
		haMode = "failover"
	}
	haLeaseTTL := cfg.HALeaseTTL
	if haLeaseTTL == 0 {
		haLeaseTTL = 5 * time.Second
	} else if haLeaseTTL < 0 {
		haLeaseTTL = 0
	}

	if cfg.Store != nil {
		if support, ok := cfg.Store.(storage.ConcurrentWriteSupport); ok && !support.SupportsConcurrentWrites() {
			if haMode == "concurrent" {
				if logger != nil {
					logger.Warn("ha.mode.override",
						"requested", "concurrent",
						"effective", "failover",
						"reason", "single-writer storage backends require failover mode",
					)
				}
				haMode = "failover"
			}
		}
	}

	svc := &Service{
		store:            cfg.Store,
		backendHash:      strings.TrimSpace(cfg.BackendHash),
		crypto:           cfg.Crypto,
		haMode:           haMode,
		haLeaseTTL:       haLeaseTTL,
		queueProvider:    cfg.QueueService,
		queueDispatcher:  cfg.QueueDispatcher,
		searchAdapter:    cfg.SearchAdapter,
		namespaceConfigs: cfg.NamespaceConfigs,
		defaultNamespace: defaultNS,
		defaultTTL: TTLConfig{
			Default: cfg.DefaultTTL,
			Max:     cfg.MaxTTL,
		},
		acquireBlock:              cfg.AcquireBlock,
		jsonMaxBytes:              cfg.JSONMaxBytes,
		attachmentMaxBytes:        cfg.AttachmentMaxBytes,
		spoolThreshold:            cfg.SpoolThreshold,
		txnDecisionRetention:      cfg.TxnDecisionRetention,
		txnReplayInterval:         cfg.TxnReplayInterval,
		queueDecisionCacheTTL:     queueDecisionCacheTTL,
		queueDecisionMaxApply:     queueDecisionMaxApply,
		queueDecisionApplyTimeout: queueDecisionApplyTimeout,
		stateCache:                stateCache,
		queryDocPrefetch:          queryDocPrefetch,
		defaultNamespaceConfig:    defaultCfg,
		logger:                    logger,
		clock:                     clk,
		qrf:                       cfg.QRFController,
		lsf:                       cfg.LSFObserver,
		indexManager:              cfg.IndexManager,
		namespaceTracker:          nsTracker,
		metaWarmup:                cfg.MetaWarmup,
		stateWarmup:               cfg.StateWarmup,
		shutdownState:             cfg.ShutdownState,
		enforceIdentity:           cfg.EnforceIdentity,
		createLocks:               &sync.Map{},
		staging:                   storage.EnsureStaging(cfg.Store),
		txnMetrics:                newTxnMetrics(logger),
		queueMetrics:              newQueueMetrics(logger),
		leaseMetrics:              newLeaseMetrics(logger),
		attachmentMetrics:         newAttachmentMetrics(logger),
		sweeperMetrics:            newSweeperMetrics(logger),
		tcDecider:                 cfg.TCDecider,
	}
	if svc.haMode == "failover" && svc.haLeaseTTL > 0 {
		svc.haNodeID = uuidv7.NewString()
		svc.startHA()
	}
	return svc
}

// SetTCDecider installs the TC decider used for implicit XA flows.
func (s *Service) SetTCDecider(decider TCDecider) {
	if s == nil {
		return
	}
	s.tcDecider = decider
}

// TxnDecisionCounters returns the number of transaction participant applications
// that succeeded or failed since process start. Intended for lightweight perf
// telemetry and tests.
func (s *Service) TxnDecisionCounters() (applied, failed int64) {
	return s.txnDecisionApplied.Load(), s.txnDecisionFailed.Load()
}

// BackendHash returns the stable identity hash for the configured backend.
func (s *Service) BackendHash() string {
	return s.backendHash
}

// HAStats reports HA refresh activity (failover mode only).
type HAStats struct {
	Refreshes   int64
	Errors      int64
	Transitions int64
}

// HAStats returns the HA refresh counters since process start.
func (s *Service) HAStats() HAStats {
	if s == nil {
		return HAStats{}
	}
	return HAStats{
		Refreshes:   s.haRefreshes.Load(),
		Errors:      s.haErrors.Load(),
		Transitions: s.haTransitions.Load(),
	}
}

func (s *Service) maybeNoSync(ctx context.Context) context.Context {
	if s == nil {
		return ctx
	}
	if strings.EqualFold(s.haMode, "failover") {
		return storage.ContextWithNoSync(ctx)
	}
	return ctx
}
