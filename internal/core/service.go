package core

import (
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
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

// Service aggregates transport-agnostic domain services.
type Service struct {
	store                  storage.Backend
	backendHash            string
	crypto                 *storage.Crypto
	queueProvider          QueueProvider
	queueDispatcher        QueueDispatcher
	searchAdapter          search.Adapter
	namespaceConfigs       *namespaces.ConfigStore
	defaultNamespace       string
	defaultTTL             TTLConfig
	acquireBlock           time.Duration
	jsonMaxBytes           int64
	attachmentMaxBytes     int64
	spoolThreshold         int64
	txnDecisionRetention   time.Duration
	defaultNamespaceConfig namespaces.Config
	logger                 pslog.Logger
	clock                  clock.Clock
	qrf                    *qrf.Controller
	lsf                    *lsf.Observer
	indexManager           *indexer.Manager
	namespaceTracker       *NamespaceTracker
	metaWarmup             WarmupConfig
	stateWarmup            WarmupConfig
	shutdownState          func() ShutdownState
	enforceIdentity        bool

	createLocks *sync.Map

	staging storage.StagingBackend

	txnDecisionApplied atomic.Int64
	txnDecisionFailed  atomic.Int64
	txnMetrics         *txnMetrics
	queueMetrics       *queueMetrics
	leaseMetrics       *leaseMetrics
	attachmentMetrics  *attachmentMetrics
	tcDecider          TCDecider
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

	return &Service{
		store:            cfg.Store,
		backendHash:      strings.TrimSpace(cfg.BackendHash),
		crypto:           cfg.Crypto,
		queueProvider:    cfg.QueueService,
		queueDispatcher:  cfg.QueueDispatcher,
		searchAdapter:    cfg.SearchAdapter,
		namespaceConfigs: cfg.NamespaceConfigs,
		defaultNamespace: defaultNS,
		defaultTTL: TTLConfig{
			Default: cfg.DefaultTTL,
			Max:     cfg.MaxTTL,
		},
		acquireBlock:           cfg.AcquireBlock,
		jsonMaxBytes:           cfg.JSONMaxBytes,
		attachmentMaxBytes:     cfg.AttachmentMaxBytes,
		spoolThreshold:         cfg.SpoolThreshold,
		txnDecisionRetention:   cfg.TxnDecisionRetention,
		defaultNamespaceConfig: defaultCfg,
		logger:                 logger,
		clock:                  clk,
		qrf:                    cfg.QRFController,
		lsf:                    cfg.LSFObserver,
		indexManager:           cfg.IndexManager,
		namespaceTracker:       nsTracker,
		metaWarmup:             cfg.MetaWarmup,
		stateWarmup:            cfg.StateWarmup,
		shutdownState:          cfg.ShutdownState,
		enforceIdentity:        cfg.EnforceIdentity,
		createLocks:            &sync.Map{},
		staging:                storage.EnsureStaging(cfg.Store),
		txnMetrics:             newTxnMetrics(logger),
		queueMetrics:           newQueueMetrics(logger),
		leaseMetrics:           newLeaseMetrics(logger),
		attachmentMetrics:      newAttachmentMetrics(logger),
		tcDecider:              cfg.TCDecider,
	}
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
