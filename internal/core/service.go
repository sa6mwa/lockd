package core

import (
	"sync"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/loggingutil"
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
	crypto                 *storage.Crypto
	queueProvider          QueueProvider
	queueDispatcher        QueueDispatcher
	searchAdapter          search.Adapter
	namespaceConfigs       *namespaces.ConfigStore
	defaultNamespace       string
	defaultTTL             TTLConfig
	acquireBlock           time.Duration
	jsonMaxBytes           int64
	spoolThreshold         int64
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
}

// New constructs the core Service with sane defaults.
func New(cfg Config) *Service {
	logger := loggingutil.EnsureLogger(cfg.Logger)
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
		spoolThreshold:         cfg.SpoolThreshold,
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
	}
}
