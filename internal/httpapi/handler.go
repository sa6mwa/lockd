package httpapi

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/jsonutil"
	"pkt.systems/lockd/internal/lsf"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/search"
	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/svcfields"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/internal/tcleader"
	"pkt.systems/lockd/internal/tcrm"
	"pkt.systems/lockd/internal/txncoord"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

const defaultPayloadSpoolMemoryThreshold = 4 << 20 // 4 MiB in-memory, then spill to disk
const (
	headerFencingToken        = "X-Fencing-Token"
	headerMetadataQueryHidden = "X-Lockd-Meta-Query-Hidden"
	headerQueryCursor         = "X-Lockd-Query-Cursor"
	headerQueryIndexSeq       = "X-Lockd-Query-Index-Seq"
	headerQueryMetadata       = "X-Lockd-Query-Metadata"
	headerQueryReturn         = "X-Lockd-Query-Return"
	headerTCReplica           = "X-Lockd-TC-Replicate"
	headerTCLeaveFanout       = "X-Lockd-TC-Leave-Fanout"
	headerAttachmentID        = "X-Attachment-ID"
	headerAttachmentName      = "X-Attachment-Name"
	headerAttachmentSize      = "X-Attachment-Size"
	headerAttachmentCreatedAt = "X-Attachment-Created-At"
	headerAttachmentUpdatedAt = "X-Attachment-Updated-At"
	headerAttachmentSHA256    = "X-Attachment-SHA256"
	headerExpectedSHA256      = "X-Expected-SHA256"
	headerExpectedBytes       = "X-Expected-Bytes"
)
const headerCorrelationID = "X-Correlation-Id"
const headerShutdownImminent = "Shutdown-Imminent"
const contentTypeNDJSON = "application/x-ndjson"
const maxQueueDequeueBatch = 64
const queueEnsureTimeoutGrace = 2 * time.Second
const defaultQueryLimit = 100
const maxQueryLimit = 1000
const namespaceConfigBodyLimit = 32 << 10
const indexFlushBodyLimit = 8 << 10
const defaultIndexFlushAsyncLimit = 8

const (
	acquireBackoffStart      = 500 * time.Millisecond
	acquireBackoffMax        = 5 * time.Second
	acquireBackoffMin        = 250 * time.Millisecond
	acquireBackoffMultiplier = 1.3
	acquireBackoffJitter     = 100 * time.Millisecond
)

const (
	backendWarmupInitialDelay = 25 * time.Millisecond
	backendWarmupMaxDelay     = 250 * time.Millisecond
	backendWarmupAttempts     = 3
)

var (
	backoffRandMu  sync.Mutex
	backoffRandSrc = rand.New(rand.NewSource(time.Now().UnixNano()))
)

var debugQueueTiming = os.Getenv("LOCKD_DEBUG_QUEUE_TIMING") == "1"

// Handler wires HTTP endpoints to backend operations.
type Handler struct {
	core                   *core.Service
	store                  storage.Backend
	crypto                 *storage.Crypto
	queueSvc               *queue.Service
	queueDisp              *queue.Dispatcher
	searchAdapter          search.Adapter
	namespaceConfigs       *namespaces.ConfigStore
	defaultNamespaceConfig namespaces.Config
	indexManager           *indexer.Manager
	indexControl           indexFlushController
	logger                 pslog.Logger
	clock                  clock.Clock
	lsfObserver            *lsf.Observer
	qrf                    *qrf.Controller
	jsonMaxBytes           int64
	compactWriter          func(io.Writer, io.Reader, int64) error
	defaultTTL             time.Duration
	maxTTL                 time.Duration
	acquireBlock           time.Duration
	spoolThreshold         int64
	defaultNamespace       string
	namespaceTracker       *NamespaceTracker
	leaseCache             sync.Map
	observedKeys           sync.Map
	enforceClientIdentity  bool
	tracer                 trace.Tracer
	metaWarmupAttempts     int
	metaWarmupInitial      time.Duration
	metaWarmupMax          time.Duration
	stateWarmupAttempts    int
	stateWarmupInitial     time.Duration
	stateWarmupMax         time.Duration
	pendingDeliveries      *core.PendingDeliveries
	shutdownState          func() ShutdownState
	activityHook           func()
	httpTracingEnabled     bool
	tcAuthEnabled          bool
	tcTrustPool            *x509.CertPool
	tcAllowDefaultCA       bool
	defaultCAPool          *x509.CertPool
	tcLeader               *tcleader.Manager
	tcCluster              *tccluster.Store
	tcRM                   *tcrm.Store
	tcRMReplicator         *tcrm.Replicator
	selfEndpoint           string
	tcClusterIdentity      string
	tcJoinEndpoints        []string
	tcLeaveFanout          func(context.Context) error
	tcClusterLeaveSelf     func()
	tcClusterJoinSelf      func()
	indexFlushMu           sync.Mutex
	indexFlushInFlight     map[string]string
	indexFlushAsyncLimit   int
}

type indexFlushController interface {
	Pending(namespace string) bool
	FlushNamespace(ctx context.Context, namespace string) error
	ManifestSeq(ctx context.Context, namespace string) (uint64, error)
	WaitForReadable(ctx context.Context, namespace string) error
	WarmNamespace(ctx context.Context, namespace string) error
}

func (h *Handler) reserveAsyncIndexFlush(namespace string) (flushID string, deduped bool, err error) {
	if h == nil {
		return "", false, httpError{Status: http.StatusServiceUnavailable, Code: "index_unavailable", Detail: "indexing is disabled"}
	}
	h.indexFlushMu.Lock()
	defer h.indexFlushMu.Unlock()
	if h.indexFlushInFlight == nil {
		h.indexFlushInFlight = make(map[string]string)
	}
	if existing, ok := h.indexFlushInFlight[namespace]; ok && existing != "" {
		return existing, true, nil
	}
	limit := h.indexFlushAsyncLimit
	if limit <= 0 {
		limit = defaultIndexFlushAsyncLimit
	}
	if len(h.indexFlushInFlight) >= limit {
		return "", false, httpError{
			Status: http.StatusConflict,
			Code:   "index_flush_busy",
			Detail: fmt.Sprintf("too many async index flushes in flight (limit %d)", limit),
		}
	}
	flushID = uuidv7.NewString()
	h.indexFlushInFlight[namespace] = flushID
	return flushID, false, nil
}

func (h *Handler) releaseAsyncIndexFlush(namespace, flushID string) {
	if h == nil {
		return
	}
	h.indexFlushMu.Lock()
	defer h.indexFlushMu.Unlock()
	if h.indexFlushInFlight == nil {
		return
	}
	if current, ok := h.indexFlushInFlight[namespace]; ok && current == flushID {
		delete(h.indexFlushInFlight, namespace)
	}
}

// ShutdownState exposes the server's current shutdown posture.
type ShutdownState struct {
	Draining  bool
	Remaining time.Duration
	Notify    bool
}

func (h *Handler) beginQueueProducer() func() {
	if h.lsfObserver == nil {
		return func() {}
	}
	return h.lsfObserver.BeginQueueProducer()
}

func (h *Handler) beginQueueConsumer() func() {
	if h.lsfObserver == nil {
		return func() {}
	}
	return h.lsfObserver.BeginQueueConsumer()
}

func (h *Handler) trackPendingDelivery(namespace, queue, owner string, delivery *core.QueueDelivery) {
	if h == nil || h.pendingDeliveries == nil {
		return
	}
	h.pendingDeliveries.Track(namespace, queue, owner, delivery)
}

func (h *Handler) clearPendingDelivery(namespace, queue, owner, messageID string) {
	if h == nil || h.pendingDeliveries == nil {
		return
	}
	h.pendingDeliveries.Clear(namespace, queue, owner, messageID)
}

func (h *Handler) logQueueSubscribeError(queue pslog.Logger, queueName, owner string, err error) {
	if err == nil {
		return
	}
	if queue != nil {
		queue.Warn("queue.subscribe.error", "error", err)
	}
	if os.Getenv("MEM_LQ_BENCH_DEBUG") == "1" {
		fmt.Fprintf(os.Stderr, "[subscribe error] queue=%s owner=%s err=%v\n", queueName, owner, err)
	}
}

func (h *Handler) currentShutdownState() ShutdownState {
	if h == nil || h.shutdownState == nil {
		return ShutdownState{}
	}
	state := h.shutdownState()
	if state.Remaining < 0 {
		state.Remaining = 0
	}
	return state
}

// StopHA stops the HA lease refresh loop for the handler's core service.
func (h *Handler) StopHA() {
	if h == nil || h.core == nil {
		return
	}
	h.core.StopHA()
}

// ReleaseHA releases the HA lease via the handler's core service.
func (h *Handler) ReleaseHA(ctx context.Context) {
	if h == nil || h.core == nil {
		return
	}
	h.core.ReleaseHA(ctx)
}

func (h *Handler) releasePendingDeliveries(namespace, queue, owner string) {
	if h == nil || h.pendingDeliveries == nil {
		return
	}
	h.pendingDeliveries.Release(namespace, queue, owner)
}

func (h *Handler) beginQueueAck() func() {
	if h.lsfObserver == nil {
		return func() {}
	}
	return h.lsfObserver.BeginQueueAck()
}

func (h *Handler) maybeThrottleQueue(kind qrf.Kind) error {
	if h.qrf == nil {
		return nil
	}
	return h.qrfThrottleError(h.qrf.Wait(context.Background(), kind))
}

func (h *Handler) qrfThrottleError(err error) error {
	if err == nil {
		return nil
	}
	var waitErr *qrf.WaitError
	if errors.As(err, &waitErr) {
		retry := durationToSeconds(waitErr.Delay)
		if retry <= 0 {
			retry = 1
		}
		return httpError{
			Status:     http.StatusTooManyRequests,
			Code:       "throttled",
			Detail:     "perimeter defence engaged",
			RetryAfter: retry,
		}
	}
	return err
}

func (h *Handler) requireQueueService() (*queue.Service, error) {
	if h.queueSvc == nil || h.queueDisp == nil {
		return nil, httpError{
			Status: http.StatusNotImplemented,
			Code:   "queue_disabled",
			Detail: "queue service not configured",
		}
	}
	return h.queueSvc, nil
}

func (h *Handler) clientKeyFromRequest(r *http.Request) string {
	addr := strings.TrimSpace(r.RemoteAddr)
	if addr != "" {
		return addr
	}
	if id := clientIdentityFromContext(r.Context()); id != "" {
		return id
	}
	return ""
}

func (h *Handler) requireTCClient(r *http.Request) error {
	if !h.tcAuthEnabled {
		return nil
	}
	if r == nil || r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return httpError{Status: http.StatusForbidden, Code: "tc_client_required", Detail: "client certificate required for transaction coordinator operations"}
	}
	leaf := r.TLS.PeerCertificates[0]
	intermediates := x509.NewCertPool()
	for _, cert := range r.TLS.PeerCertificates[1:] {
		intermediates.AddCert(cert)
	}
	verify := func(pool *x509.CertPool) bool {
		if pool == nil {
			return false
		}
		_, err := leaf.Verify(x509.VerifyOptions{
			Roots:         pool,
			Intermediates: intermediates,
			CurrentTime:   time.Now(),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		})
		return err == nil
	}
	if h.tcTrustPool != nil && verify(h.tcTrustPool) {
		return nil
	}
	if h.tcAllowDefaultCA && verify(h.defaultCAPool) {
		return nil
	}
	return httpError{Status: http.StatusForbidden, Code: "tc_forbidden", Detail: "client certificate not trusted for transaction coordinator operations"}
}

func (h *Handler) requireTCServer(r *http.Request) error {
	if !h.tcAuthEnabled {
		return nil
	}
	if r == nil || r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return httpError{Status: http.StatusForbidden, Code: "tc_server_required", Detail: "server certificate required for RM registration"}
	}
	leaf := r.TLS.PeerCertificates[0]
	intermediates := x509.NewCertPool()
	for _, cert := range r.TLS.PeerCertificates[1:] {
		intermediates.AddCert(cert)
	}
	verify := func(pool *x509.CertPool) bool {
		if pool == nil {
			return false
		}
		_, err := leaf.Verify(x509.VerifyOptions{
			Roots:         pool,
			Intermediates: intermediates,
			CurrentTime:   time.Now(),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		})
		return err == nil
	}
	if h.tcTrustPool != nil && verify(h.tcTrustPool) {
		return nil
	}
	if h.tcAllowDefaultCA && verify(h.defaultCAPool) {
		return nil
	}
	return httpError{Status: http.StatusForbidden, Code: "tc_forbidden", Detail: "server certificate not trusted for RM registration"}
}

func (h *Handler) currentTCLeader(now time.Time) (tcleader.LeaderInfo, bool) {
	if h == nil || h.tcLeader == nil || !h.tcLeader.Enabled() {
		return tcleader.LeaderInfo{}, false
	}
	info := h.tcLeader.Leader(now)
	if info.LeaderEndpoint == "" || info.Term == 0 {
		return info, false
	}
	if !info.ExpiresAt.IsZero() && !info.ExpiresAt.After(now) {
		return info, false
	}
	return info, true
}

func (h *Handler) tcLeaseStore() (*tcleader.LeaseStore, error) {
	if h == nil || h.tcLeader == nil || !h.tcLeader.Enabled() {
		return nil, httpError{Status: http.StatusServiceUnavailable, Code: "tc_unavailable", Detail: "tc leader election not configured"}
	}
	store := h.tcLeader.LeaseStore()
	if store == nil {
		return nil, httpError{Status: http.StatusServiceUnavailable, Code: "tc_unavailable", Detail: "tc lease store unavailable"}
	}
	return store, nil
}

func (h *Handler) tcClusterStore() (*tccluster.Store, error) {
	if h == nil || h.tcCluster == nil {
		return nil, httpError{Status: http.StatusServiceUnavailable, Code: "tc_unavailable", Detail: "tc cluster store not configured"}
	}
	return h.tcCluster, nil
}

func (h *Handler) tcRMStore() (*tcrm.Store, error) {
	if h == nil || h.tcRM == nil {
		return nil, httpError{Status: http.StatusServiceUnavailable, Code: "tc_unavailable", Detail: "tc RM registry not configured"}
	}
	return h.tcRM, nil
}

func (h *Handler) syncTCLeader(endpoints []string) error {
	if h == nil || h.tcLeader == nil {
		return nil
	}
	normalized := tccluster.NormalizeEndpoints(endpoints)
	self := strings.TrimSpace(h.selfEndpoint)
	var leaderEndpoints []string
	useMembership := len(normalized) > 0 && (self == "" || tccluster.ContainsEndpoint(normalized, self))
	if useMembership {
		leaderEndpoints = normalized
	} else {
		if len(h.tcJoinEndpoints) > 0 {
			leaderEndpoints = tccluster.NormalizeEndpoints(append([]string(nil), h.tcJoinEndpoints...))
		}
		if self != "" && !tccluster.ContainsEndpoint(leaderEndpoints, self) {
			leaderEndpoints = tccluster.NormalizeEndpoints(append(leaderEndpoints, self))
		}
	}
	return h.tcLeader.SetEndpoints(leaderEndpoints)
}

// Config groups the dependencies required by Handler.
type Config struct {
	Store                      storage.Backend
	Crypto                     *storage.Crypto
	QueueService               *queue.Service
	SearchAdapter              search.Adapter
	NamespaceConfigs           *namespaces.ConfigStore
	DefaultNamespaceConfig     namespaces.Config
	IndexManager               *indexer.Manager
	Logger                     pslog.Logger
	Clock                      clock.Clock
	HAMode                     string
	HALeaseTTL                 time.Duration
	DefaultNamespace           string
	JSONMaxBytes               int64
	AttachmentMaxBytes         int64
	CompactWriter              func(io.Writer, io.Reader, int64) error
	DefaultTTL                 time.Duration
	MaxTTL                     time.Duration
	AcquireBlock               time.Duration
	SpoolMemoryThreshold       int64
	TxnDecisionRetention       time.Duration
	TxnReplayInterval          time.Duration
	QueueDecisionCacheTTL      time.Duration
	QueueDecisionMaxApply      int
	QueueDecisionApplyTimeout  time.Duration
	StateCacheBytes            int64
	QueryDocPrefetch           int
	EnforceClientIdentity      bool
	MetaWarmupAttempts         int
	MetaWarmupInitialDelay     time.Duration
	MetaWarmupMaxDelay         time.Duration
	StateWarmupAttempts        int
	StateWarmupInitialDelay    time.Duration
	StateWarmupMaxDelay        time.Duration
	QueueMaxConsumers          int
	QueuePollInterval          time.Duration
	QueuePollJitter            time.Duration
	QueueResilientPollInterval time.Duration
	QueueListPageSize          int
	LSFObserver                *lsf.Observer
	QRFController              *qrf.Controller
	ShutdownState              func() ShutdownState
	ActivityHook               func()
	NamespaceTracker           *NamespaceTracker
	DisableHTTPTracing         bool
	TCAuthEnabled              bool
	TCTrustPool                *x509.CertPool
	DefaultCAPool              *x509.CertPool
	TCAllowDefaultCA           bool
	TCLeader                   *tcleader.Manager
	SelfEndpoint               string
	TCClusterIdentity          string
	TCJoinEndpoints            []string
	TCFanoutTimeout            time.Duration
	TCFanoutMaxAttempts        int
	TCFanoutBaseDelay          time.Duration
	TCFanoutMaxDelay           time.Duration
	TCFanoutMultiplier         float64
	TCFanoutGate               txncoord.FanoutGate
	TCFanoutTrustPEM           [][]byte
	TCLeaveFanout              func(context.Context) error
	TCClusterLeaveSelf         func()
	TCClusterJoinSelf          func()
	TCClientBundlePath         string
	TCServerBundle             *tlsutil.Bundle
	DisableMTLS                bool
}

// New constructs a Handler using the supplied configuration.
func New(cfg Config) *Handler {
	baseLogger := cfg.Logger
	if baseLogger == nil {
		baseLogger = pslog.NoopLogger()
	}
	logger := baseLogger
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	cw := cfg.CompactWriter
	if cw == nil {
		cw = jsonutil.CompactWriter
	}
	threshold := cfg.SpoolMemoryThreshold
	if threshold <= 0 {
		threshold = defaultPayloadSpoolMemoryThreshold
	}
	metaWarmupAttempts := backendWarmupAttempts
	if cfg.MetaWarmupAttempts > 0 {
		metaWarmupAttempts = cfg.MetaWarmupAttempts
	} else if cfg.MetaWarmupAttempts == 0 {
		metaWarmupAttempts = 0
	}
	metaWarmupInitial := backendWarmupInitialDelay
	if cfg.MetaWarmupInitialDelay > 0 {
		metaWarmupInitial = cfg.MetaWarmupInitialDelay
	}
	metaWarmupMax := backendWarmupMaxDelay
	if cfg.MetaWarmupMaxDelay > 0 {
		metaWarmupMax = cfg.MetaWarmupMaxDelay
	}
	stateWarmupAttempts := backendWarmupAttempts
	if cfg.StateWarmupAttempts > 0 {
		stateWarmupAttempts = cfg.StateWarmupAttempts
	} else if cfg.StateWarmupAttempts == 0 {
		stateWarmupAttempts = 0
	}
	stateWarmupInitial := backendWarmupInitialDelay
	if cfg.StateWarmupInitialDelay > 0 {
		stateWarmupInitial = cfg.StateWarmupInitialDelay
	}
	stateWarmupMax := backendWarmupMaxDelay
	if cfg.StateWarmupMaxDelay > 0 {
		stateWarmupMax = cfg.StateWarmupMaxDelay
	}
	queueSvc := cfg.QueueService
	crypto := cfg.Crypto
	if queueSvc == nil && cfg.Store != nil {
		if svc, err := queue.New(cfg.Store, clk, queue.Config{
			Crypto:            crypto,
			QueuePollInterval: cfg.QueuePollInterval,
		}); err == nil {
			queueSvc = svc
		}
	}
	var queueDisp *queue.Dispatcher
	if queueSvc != nil {
		queueLogger := svcfields.WithSubsystem(baseLogger, "queue.dispatcher.core")
		opts := []queue.DispatcherOption{
			queue.WithLogger(queueLogger),
			queue.WithMaxConsumers(cfg.QueueMaxConsumers),
			queue.WithPollInterval(cfg.QueuePollInterval),
			queue.WithPollJitter(cfg.QueuePollJitter),
			queue.WithQueuePageSize(cfg.QueueListPageSize),
		}
		if cfg.QueueResilientPollInterval > 0 {
			opts = append(opts, queue.WithResilientPollInterval(cfg.QueueResilientPollInterval))
		}
		watchMode := "polling"
		watchReason := "backend_no_change_feed"
		if feed, ok := cfg.Store.(storage.QueueChangeFeed); ok {
			if factory := queue.WatchFactoryFromStorage(feed); factory != nil {
				opts = append(opts, queue.WithWatchFactory(factory))
				watchMode = "change_feed"
				watchReason = "backend_change_feed"
			} else {
				watchReason = "change_feed_factory_unavailable"
			}
		}
		queueDisp = queue.NewDispatcher(queueSvc, opts...)
		queueLogger.Info("queue.dispatcher.config",
			"max_consumers", cfg.QueueMaxConsumers,
			"poll_interval", cfg.QueuePollInterval,
			"poll_jitter", cfg.QueuePollJitter,
			"resilient_poll_interval", cfg.QueueResilientPollInterval,
			"page_size", cfg.QueueListPageSize,
			"watch_mode", watchMode,
			"watch_reason", watchReason,
		)
		if provider, ok := cfg.Store.(storage.QueueWatchStatusProvider); ok {
			status := provider.QueueWatchStatus()
			queueLogger.Info("queue.dispatcher.watch_status",
				"enabled", status.Enabled,
				"mode", status.Mode,
				"reason", status.Reason,
			)
		}
	}
	defaultNamespace := namespaces.Default
	if cfg.DefaultNamespace != "" {
		if ns, err := namespaces.Normalize(cfg.DefaultNamespace, namespaces.Default); err == nil {
			defaultNamespace = ns
		} else {
			logger.Warn("httpapi.handler.invalid_default_namespace", "namespace", cfg.DefaultNamespace, "error", err)
		}
	}
	defaultNamespaceConfig := cfg.DefaultNamespaceConfig
	defaultNamespaceConfig.Normalize()
	if err := defaultNamespaceConfig.Validate(); err != nil {
		defaultNamespaceConfig = namespaces.DefaultConfig()
	}
	tracker := cfg.NamespaceTracker
	if tracker == nil {
		tracker = NewNamespaceTracker(defaultNamespace)
	} else {
		tracker.Observe(defaultNamespace)
	}
	coreTracker := core.NewNamespaceTracker(defaultNamespace)
	backendHash := ""
	if cfg.Store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		hash, err := cfg.Store.BackendHash(ctx)
		cancel()
		if err != nil {
			logger.Warn("storage.backend_hash.error", "error", err)
		}
		backendHash = strings.TrimSpace(hash)
		if backendHash == "" {
			logger.Warn("storage.backend_hash.empty")
		}
	}
	coreSvc := core.New(core.Config{
		Store:                     cfg.Store,
		BackendHash:               backendHash,
		Crypto:                    crypto,
		HAMode:                    cfg.HAMode,
		HALeaseTTL:                cfg.HALeaseTTL,
		QueueService:              queueSvc,
		QueueDispatcher:           queueDisp,
		SearchAdapter:             cfg.SearchAdapter,
		NamespaceConfigs:          cfg.NamespaceConfigs,
		DefaultNamespaceConfig:    defaultNamespaceConfig,
		IndexManager:              cfg.IndexManager,
		DefaultNamespace:          defaultNamespace,
		Logger:                    logger,
		Clock:                     clk,
		DefaultTTL:                cfg.DefaultTTL,
		MaxTTL:                    cfg.MaxTTL,
		AcquireBlock:              cfg.AcquireBlock,
		JSONMaxBytes:              cfg.JSONMaxBytes,
		AttachmentMaxBytes:        cfg.AttachmentMaxBytes,
		SpoolThreshold:            threshold,
		TxnDecisionRetention:      cfg.TxnDecisionRetention,
		TxnReplayInterval:         cfg.TxnReplayInterval,
		QueueDecisionCacheTTL:     cfg.QueueDecisionCacheTTL,
		QueueDecisionMaxApply:     cfg.QueueDecisionMaxApply,
		QueueDecisionApplyTimeout: cfg.QueueDecisionApplyTimeout,
		StateCacheBytes:           cfg.StateCacheBytes,
		QueryDocPrefetch:          cfg.QueryDocPrefetch,
		EnforceIdentity:           cfg.EnforceClientIdentity,
		MetaWarmup: core.WarmupConfig{
			Attempts: metaWarmupAttempts,
			Initial:  metaWarmupInitial,
			Max:      metaWarmupMax,
		},
		StateWarmup: core.WarmupConfig{
			Attempts: stateWarmupAttempts,
			Initial:  stateWarmupInitial,
			Max:      stateWarmupMax,
		},
		LSFObserver:   cfg.LSFObserver,
		QRFController: cfg.QRFController,
		ShutdownState: func() core.ShutdownState {
			if cfg.ShutdownState == nil {
				return core.ShutdownState{}
			}
			s := cfg.ShutdownState()
			return core.ShutdownState{
				Draining:  s.Draining,
				Remaining: s.Remaining,
				Notify:    s.Notify,
			}
		},
		NamespaceTracker: coreTracker,
	})
	var rmStore *tcrm.Store
	if cfg.Store != nil {
		rmStore = tcrm.NewStore(cfg.Store, svcfields.WithSubsystem(logger, "tc.rm.store"))
	}
	coordLogger := svcfields.WithSubsystem(baseLogger, "txn.coordinator")
	coord, coordErr := txncoord.New(txncoord.Config{
		Core:              coreSvc,
		Logger:            coordLogger,
		DecisionRetention: cfg.TxnDecisionRetention,
		FanoutProvider:    rmStore,
		FanoutGate:        cfg.TCFanoutGate,
		FanoutTimeout:     cfg.TCFanoutTimeout,
		FanoutMaxAttempts: cfg.TCFanoutMaxAttempts,
		FanoutBaseDelay:   cfg.TCFanoutBaseDelay,
		FanoutMaxDelay:    cfg.TCFanoutMaxDelay,
		FanoutMultiplier:  cfg.TCFanoutMultiplier,
		DisableMTLS:       cfg.DisableMTLS,
		ClientBundlePath:  cfg.TCClientBundlePath,
		FanoutTrustPEM:    cfg.TCFanoutTrustPEM,
	})
	if coordErr != nil {
		logger.Warn("txn.coordinator.init_failed", "error", coordErr)
	}
	var tcDecider core.TCDecider
	if coordErr != nil {
		tcDecider = txncoord.NewErrorDecider(coordErr)
	} else if coord != nil {
		decider, err := txncoord.NewDecider(txncoord.DeciderConfig{
			Coordinator:      coord,
			Leader:           cfg.TCLeader,
			Logger:           svcfields.WithSubsystem(baseLogger, "txn.decider"),
			ForwardTimeout:   cfg.TCFanoutTimeout,
			DisableMTLS:      cfg.DisableMTLS,
			ClientBundlePath: cfg.TCClientBundlePath,
			ForwardTrustPEM:  cfg.TCFanoutTrustPEM,
		})
		if err != nil {
			logger.Warn("txn.decider.init_failed", "error", err)
			tcDecider = txncoord.NewErrorDecider(err)
		} else {
			tcDecider = decider
		}
	}
	if tcDecider != nil {
		coreSvc.SetTCDecider(tcDecider)
	}
	var clusterStore *tccluster.Store
	if cfg.Store != nil {
		clusterStore = tccluster.NewStore(cfg.Store, svcfields.WithSubsystem(logger, "tc.cluster.store"), clk)
	}
	var rmReplicator *tcrm.Replicator
	if rmStore != nil {
		replicator, err := tcrm.NewReplicator(tcrm.ReplicatorConfig{
			Store:            rmStore,
			Cluster:          clusterStore,
			SelfEndpoint:     strings.TrimSpace(cfg.SelfEndpoint),
			Logger:           svcfields.WithSubsystem(baseLogger, "tc.rm.replicator"),
			Timeout:          cfg.TCFanoutTimeout,
			DisableMTLS:      cfg.DisableMTLS,
			ClientBundlePath: cfg.TCClientBundlePath,
			ServerBundle:     cfg.TCServerBundle,
			TrustPEM:         cfg.TCFanoutTrustPEM,
		})
		if err != nil {
			logger.Warn("tc.rm.replicator.init_failed", "error", err)
		} else {
			rmReplicator = replicator
		}
	}
	return &Handler{
		core:                   coreSvc,
		store:                  cfg.Store,
		crypto:                 crypto,
		queueSvc:               queueSvc,
		queueDisp:              queueDisp,
		searchAdapter:          cfg.SearchAdapter,
		namespaceConfigs:       cfg.NamespaceConfigs,
		defaultNamespaceConfig: defaultNamespaceConfig,
		indexManager:           cfg.IndexManager,
		indexControl:           cfg.IndexManager,
		logger:                 logger,
		clock:                  clk,
		lsfObserver:            cfg.LSFObserver,
		qrf:                    cfg.QRFController,
		jsonMaxBytes:           cfg.JSONMaxBytes,
		compactWriter:          cw,
		defaultTTL:             cfg.DefaultTTL,
		maxTTL:                 cfg.MaxTTL,
		acquireBlock:           cfg.AcquireBlock,
		spoolThreshold:         threshold,
		defaultNamespace:       defaultNamespace,
		enforceClientIdentity:  cfg.EnforceClientIdentity,
		tracer:                 otel.Tracer("pkt.systems/lockd/httpapi"),
		metaWarmupAttempts:     metaWarmupAttempts,
		metaWarmupInitial:      metaWarmupInitial,
		metaWarmupMax:          metaWarmupMax,
		stateWarmupAttempts:    stateWarmupAttempts,
		stateWarmupInitial:     stateWarmupInitial,
		stateWarmupMax:         stateWarmupMax,
		shutdownState:          cfg.ShutdownState,
		activityHook:           cfg.ActivityHook,
		namespaceTracker:       tracker,
		pendingDeliveries:      core.NewPendingDeliveries(),
		httpTracingEnabled:     !cfg.DisableHTTPTracing,
		tcAuthEnabled:          cfg.TCAuthEnabled,
		tcTrustPool:            cfg.TCTrustPool,
		tcAllowDefaultCA:       cfg.TCAllowDefaultCA,
		defaultCAPool:          cfg.DefaultCAPool,
		tcLeader:               cfg.TCLeader,
		tcCluster:              clusterStore,
		tcRM:                   rmStore,
		tcRMReplicator:         rmReplicator,
		selfEndpoint:           strings.TrimSpace(cfg.SelfEndpoint),
		tcClusterIdentity:      strings.TrimSpace(cfg.TCClusterIdentity),
		tcJoinEndpoints:        tccluster.NormalizeEndpoints(append([]string(nil), cfg.TCJoinEndpoints...)),
		tcLeaveFanout:          cfg.TCLeaveFanout,
		tcClusterLeaveSelf:     cfg.TCClusterLeaveSelf,
		tcClusterJoinSelf:      cfg.TCClusterJoinSelf,
		indexFlushInFlight:     make(map[string]string),
		indexFlushAsyncLimit:   defaultIndexFlushAsyncLimit,
	}
}

// Register wires the routes under /v1 and health endpoints.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("/v1/acquire", h.wrap("acquire", h.handleAcquire))
	mux.Handle("/v1/keepalive", h.wrap("keepalive", h.handleKeepAlive))
	mux.Handle("/v1/release", h.wrap("release", h.handleRelease))
	mux.Handle("/v1/get", h.wrap("get", h.handleGet))
	mux.Handle("/v1/attachments", h.wrap("attachments", h.handleAttachments))
	mux.Handle("/v1/attachment", h.wrap("attachment", h.handleAttachment))
	mux.Handle("/v1/query", h.wrap("query", h.handleQuery))
	mux.Handle("/v1/update", h.wrap("update", h.handleUpdate))
	mux.Handle("/v1/metadata", h.wrap("update_metadata", h.handleMetadata))
	mux.Handle("/v1/remove", h.wrap("remove", h.handleRemove))
	mux.Handle("/v1/describe", h.wrap("describe", h.handleDescribe))
	mux.Handle("/v1/index/flush", h.wrap("index.flush", h.handleIndexFlush))
	mux.Handle("/v1/namespace", h.wrap("namespace", h.handleNamespaceConfig))
	mux.Handle("/v1/queue/enqueue", h.wrap("queue.enqueue", h.handleQueueEnqueue))
	mux.Handle("/v1/queue/stats", h.wrap("queue.stats", h.handleQueueStats))
	mux.Handle("/v1/queue/dequeue", h.wrap("queue.dequeue", h.handleQueueDequeue))
	mux.Handle("/v1/queue/dequeueWithState", h.wrap("queue.dequeue_with_state", h.handleQueueDequeueWithState))
	mux.Handle("/v1/queue/watch", h.wrap("queue.watch", h.handleQueueWatch))
	mux.Handle("/v1/queue/subscribe", h.wrap("queue.subscribe", h.handleQueueSubscribe))
	mux.Handle("/v1/queue/subscribeWithState", h.wrap("queue.subscribe_with_state", h.handleQueueSubscribeWithState))
	mux.Handle("/v1/queue/ack", h.wrap("queue.ack", h.handleQueueAck))
	mux.Handle("/v1/queue/nack", h.wrap("queue.nack", h.handleQueueNack))
	mux.Handle("/v1/queue/extend", h.wrap("queue.extend", h.handleQueueExtend))
	mux.Handle("/v1/txn/replay", h.wrap("txn.replay", h.handleTxnReplay))
	mux.Handle("/v1/txn/decide", h.wrap("txn.decide", h.handleTxnDecide))
	mux.Handle("/v1/txn/commit", h.wrap("txn.commit", h.handleTxnCommit))
	mux.Handle("/v1/txn/rollback", h.wrap("txn.rollback", h.handleTxnRollback))
	mux.Handle("/v1/tc/lease/acquire", h.wrap("tc.lease.acquire", h.handleTCLeaseAcquire))
	mux.Handle("/v1/tc/lease/renew", h.wrap("tc.lease.renew", h.handleTCLeaseRenew))
	mux.Handle("/v1/tc/lease/release", h.wrap("tc.lease.release", h.handleTCLeaseRelease))
	mux.Handle("/v1/tc/leader", h.wrap("tc.leader", h.handleTCLeader))
	mux.Handle("/v1/tc/cluster/announce", h.wrap("tc.cluster.announce", h.handleTCClusterAnnounce))
	mux.Handle("/v1/tc/cluster/leave", h.wrap("tc.cluster.leave", h.handleTCClusterLeave))
	mux.Handle("/v1/tc/cluster/list", h.wrap("tc.cluster.list", h.handleTCClusterList))
	mux.Handle("/v1/tc/rm/register", h.wrap("tc.rm.register", h.handleTCRMRegister))
	mux.Handle("/v1/tc/rm/unregister", h.wrap("tc.rm.unregister", h.handleTCRMUnregister))
	mux.Handle("/v1/tc/rm/list", h.wrap("tc.rm.list", h.handleTCRMList))
	mux.Handle("/healthz", h.wrap("healthz", h.handleHealth))
	mux.Handle("/readyz", h.wrap("readyz", h.handleReady))
}

type handlerFunc func(http.ResponseWriter, *http.Request) error

type acquireParams struct {
	Namespace     string
	Key           string
	Owner         string
	TTL           time.Duration
	Block         time.Duration
	WaitForever   bool
	GeneratedKey  bool
	CorrelationID string
}

type acquireOutcome struct {
	Response api.AcquireResponse
	Meta     storage.Meta
	MetaETag string
}

func (h *Handler) requireNodeActive(ctx context.Context) error {
	if h == nil || h.core == nil {
		return nil
	}
	if err := h.core.RequireNodeActive(); err != nil {
		return convertCoreError(err)
	}
	return nil
}

// NodeActive reports whether this server is currently active for failover mode.
func (h *Handler) NodeActive() bool {
	if h == nil || h.core == nil {
		return true
	}
	return h.core.NodeActive()
}

func requiresNodeActive(operation string) bool {
	switch operation {
	case "healthz", "readyz", "get", "describe", "query", "attachments", "attachment":
		return false
	}
	return !strings.HasPrefix(operation, "tc.")
}

func (h *Handler) wrap(operation string, fn handlerFunc) http.Handler {
	sys := routerSys(operation)
	httpSpanName := "lockd.http." + operation
	txSpanName := "lockd.tx." + operation

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := r.Context()
		if h.activityHook != nil {
			h.activityHook()
		}
		reqID := uuidv7.NewString()
		instrument := h.httpTracingEnabled
		var span trace.Span
		if instrument {
			ctx, span = h.tracer.Start(ctx, txSpanName,
				trace.WithSpanKind(trace.SpanKindInternal),
				trace.WithAttributes(attribute.String("lockd.sys", sys)),
			)
			span.SetAttributes(
				attribute.String("lockd.operation", operation),
				attribute.String("lockd.route", r.URL.Path),
				attribute.Bool("lockd.enforce_identity", h.enforceClientIdentity),
			)
			span.AddEvent("lockd.tx.begin")
			defer span.End()
		} else {
			span = trace.SpanFromContext(ctx)
		}

		ctx = correlation.Ensure(ctx)

		logger := svcfields.WithSubsystem(h.logger, sys).With(
			"req_id", reqID,
			"method", r.Method,
			"path", r.URL.Path,
		)
		ctx = pslog.ContextWithLogger(ctx, logger)

		if h.enforceClientIdentity {
			if id := clientIdentityFromRequest(r); id != "" {
				ctx = WithClientIdentity(ctx, id)
				logger = logger.With("client_identity", id)
				ctx = pslog.ContextWithLogger(ctx, logger)
				if instrument {
					span.SetAttributes(attribute.Bool("lockd.has_client_identity", true))
				}
			} else if instrument {
				span.SetAttributes(attribute.Bool("lockd.has_client_identity", false))
			}
		}

		if corr := strings.TrimSpace(r.Header.Get(headerCorrelationID)); corr != "" {
			if normalized, ok := correlation.Normalize(corr); ok {
				ctx = correlation.Set(ctx, normalized)
			}
		}
		if !correlation.Has(ctx) {
			ctx = correlation.Set(ctx, correlation.Generate())
		}
		ctx, logger = applyCorrelation(ctx, logger, span)
		verboseLogger := logger

		r = r.WithContext(ctx)

		verboseLogger.Trace("http.request.start", "remote_addr", r.RemoteAddr)

		state := h.currentShutdownState()
		if state.Draining && state.Notify {
			w.Header().Set(headerShutdownImminent, "true")
		}

		result := "ok"
		status := codes.Ok
		statusMsg := ""
		if requiresNodeActive(operation) {
			if err := h.requireNodeActive(ctx); err != nil {
				result = "error"
				status = codes.Error
				statusMsg = "node_passive"
				h.handleError(ctx, w, err)
				return
			}
		}
		defer func() {
			if instrument {
				duration := time.Since(start).Milliseconds()
				span.SetStatus(status, statusMsg)
				span.AddEvent("lockd.tx.end", trace.WithAttributes(
					attribute.String("lockd.result", result),
					attribute.Int64("lockd.duration_ms", duration),
				))
			}
		}()

		r = r.WithContext(ctx)
		if err := fn(w, r); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				result = "context"
				status = codes.Error
				statusMsg = "context_canceled"
				if instrument {
					span.SetAttributes(attribute.String("lockd.error_code", "context"))
				}
				verboseLogger.Trace("http.request.canceled", "elapsed", time.Since(start))
				// Always emit a structured response so clients don't see an empty 200.
				h.handleError(ctx, w, httpError{
					Status:     http.StatusConflict,
					Code:       "waiting",
					Detail:     "no messages available",
					RetryAfter: 1,
				})
				return
			}
			result = "error"
			status = codes.Error
			statusMsg = "handler_error"
			if instrument {
				span.RecordError(err)
			}
			var httpErr httpError
			if errors.As(err, &httpErr) {
				if instrument {
					span.SetAttributes(
						attribute.String("lockd.error_code", httpErr.Code),
						attribute.Int("lockd.error_status", httpErr.Status),
					)
				}
			} else if instrument {
				span.SetAttributes(attribute.String("lockd.error_code", "internal"))
			}
			ctx = r.Context()
			if corr := correlation.ID(ctx); corr != "" {
				w.Header().Set(headerCorrelationID, corr)
			}
			verboseLogger.Debug("http.request.error", "elapsed", time.Since(start), "error", err)
			h.handleError(ctx, w, err)
			return
		}
		ctx = r.Context()
		if corr := correlation.ID(ctx); corr != "" {
			w.Header().Set(headerCorrelationID, corr)
		}
		verboseLogger.Trace("http.request.complete", "elapsed", time.Since(start))
	})

	if !h.httpTracingEnabled {
		return handler
	}
	return otelhttp.NewHandler(handler, httpSpanName,
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents))
}

func (h *Handler) writeQueryKeysResponse(w http.ResponseWriter, resp api.QueryResponse, mode api.QueryReturn) error {
	headers := makeQueryResponseHeaders(resp.Cursor, resp.IndexSeq, resp.Metadata, mode)
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

func (h *Handler) writeQueryDocumentsCore(ctx context.Context, w http.ResponseWriter, cmd core.QueryCommand) error {
	result, err := h.core.QueryDocuments(ctx, cmd, nil)
	if err != nil {
		return convertCoreError(err)
	}

	headers := makeQueryResponseHeaders(result.Cursor, result.IndexSeq, result.Metadata, api.QueryReturnDocuments)
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", contentTypeNDJSON)
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	sink := &ndjsonSink{
		writer:     w,
		flusher:    flusher,
		logger:     logger,
		stream:     h.streamDocumentRow,
		ns:         cmd.Namespace,
		copyBuf:    make([]byte, queryStreamCopyBufSize),
		rowBuf:     make([]byte, 0, queryStreamRowBufSize),
		flushEvery: queryStreamFlushEvery,
	}
	_, err = h.core.QueryDocuments(ctx, cmd, sink)
	sink.Flush()
	return err
}

const (
	queryStreamCopyBufSize = 32 * 1024
	queryStreamRowBufSize  = 256
	queryStreamFlushEvery  = 16
)

func (h *Handler) streamDocumentRow(w io.Writer, namespace, key string, version int64, doc io.Reader, copyBuf []byte, rowBuf []byte) ([]byte, error) {
	buf := rowBuf[:0]
	buf = append(buf, '{')
	buf = append(buf, `"ns":`...)
	buf = strconv.AppendQuote(buf, namespace)
	buf = append(buf, `,"key":`...)
	buf = strconv.AppendQuote(buf, key)
	if version != 0 {
		buf = append(buf, `,"ver":`...)
		buf = strconv.AppendInt(buf, version, 10)
	}
	buf = append(buf, `,"doc":`...)
	if _, err := w.Write(buf); err != nil {
		return rowBuf, err
	}
	lw := &limitedWriter{Writer: w, limit: h.jsonMaxBytes}
	if len(copyBuf) == 0 {
		if _, err := io.Copy(lw, doc); err != nil {
			return rowBuf, err
		}
	} else if _, err := io.CopyBuffer(lw, doc, copyBuf); err != nil {
		return rowBuf, err
	}
	if _, err := io.WriteString(w, "}\n"); err != nil {
		return rowBuf, err
	}
	return buf[:0], nil
}

func (h *Handler) selectQueryEngine(ctx context.Context, namespace string, hint search.EngineHint) (search.EngineHint, error) {
	caps, err := h.searchAdapter.Capabilities(ctx, namespace)
	if err != nil {
		return "", err
	}
	cfg := h.defaultNamespaceConfig
	if h.namespaceConfigs != nil {
		if loaded, loadErr := h.namespaceConfigs.Load(ctx, namespace); loadErr == nil {
			cfg = loaded.Config
		} else {
			logger := pslog.LoggerFromContext(ctx)
			if logger != nil {
				logger.Warn("namespace.config.load_failed", "namespace", namespace, "error", loadErr)
			}
		}
	}
	engine, err := cfg.SelectEngine(hint, caps)
	if err != nil {
		return "", httpError{
			Status: http.StatusBadRequest,
			Code:   "query_engine_unavailable",
			Detail: err.Error(),
		}
	}
	return engine, nil
}

func (h *Handler) waitForIndexReadable(ctx context.Context, namespace string) error {
	if h == nil || h.indexControl == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return h.indexControl.WaitForReadable(ctx, namespace)
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, payload any, headers map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	enc := json.NewEncoder(w)
	_ = enc.Encode(payload)
}

func (h *Handler) acquireLeaseForKey(ctx context.Context, _ pslog.Logger, params acquireParams) (*acquireOutcome, error) {
	if params.Key == "" {
		return nil, httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key required"}
	}
	namespace := strings.TrimSpace(params.Namespace)
	if namespace == "" {
		return nil, httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: "namespace required for acquire"}
	}
	h.observeNamespace(namespace)
	relKey := relativeKey(namespace, params.Key)
	isQueueStateKey := queue.IsQueueStateKey(relKey)
	if params.Owner == "" {
		return nil, httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner required"}
	}
	if params.TTL <= 0 {
		params.TTL = h.defaultTTL
	}

	blockSeconds := int64(params.Block.Seconds())
	if params.WaitForever {
		blockSeconds = -1
	}

	targetKey := relKey
	cmd := core.AcquireCommand{
		Namespace:        namespace,
		Key:              targetKey,
		Owner:            params.Owner,
		TTLSeconds:       int64(params.TTL.Seconds()),
		BlockSeconds:     blockSeconds,
		ClientHint:       params.CorrelationID,
		ForceQueryHidden: isQueueStateKey,
	}

	res, err := h.core.Acquire(ctx, cmd)
	if err != nil {
		return nil, convertCoreError(err)
	}
	if res.Meta != nil {
		h.cacheLease(res.LeaseID, params.Key, *res.Meta, res.MetaETag)
	}

	resp := api.AcquireResponse{
		Namespace:     res.Namespace,
		LeaseID:       res.LeaseID,
		TxnID:         res.TxnID,
		Key:           relativeKey(res.Namespace, res.Key),
		Owner:         res.Owner,
		ExpiresAt:     res.ExpiresAt,
		Version:       res.Version,
		StateETag:     res.StateETag,
		FencingToken:  res.FencingToken,
		RetryAfter:    res.RetryAfter,
		CorrelationID: res.CorrelationID,
	}
	return &acquireOutcome{
		Response: resp,
		Meta:     *res.Meta,
		MetaETag: res.MetaETag,
	}, nil
}

func (h *Handler) releaseLeaseWithMeta(ctx context.Context, namespace, key string, leaseID string, meta *storage.Meta, metaETag string) error {
	if meta == nil {
		return nil
	}
	relKey := relativeKey(namespace, key)
	metaCopy := *meta
	metaCopy.Lease = nil
	metaCopy.UpdatedAtUnix = h.clock.Now().Unix()
	if _, err := h.store.StoreMeta(ctx, namespace, relKey, &metaCopy, metaETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	h.dropLease(leaseID)
	return nil
}

func (h *Handler) releaseLeaseOutcome(ctx context.Context, key string, outcome *acquireOutcome) error {
	if outcome == nil {
		return nil
	}
	return h.releaseLeaseWithMeta(ctx, outcome.Response.Namespace, key, outcome.Response.LeaseID, &outcome.Meta, outcome.MetaETag)
}

func (h *Handler) appendQueueOwner(ctx context.Context, owner string) string {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return owner
	}
	if id := clientIdentityFromContext(ctx); id != "" {
		return fmt.Sprintf("%s/%s", owner, id)
	}
	return owner
}

func (h *Handler) resolveTTL(requested int64) time.Duration {
	if requested <= 0 {
		return h.defaultTTL
	}
	ttl := time.Duration(requested) * time.Second
	if ttl > h.maxTTL {
		return h.maxTTL
	}
	return ttl
}

type leaseCacheEntry struct {
	key  string
	meta storage.Meta
	etag string
}

func (h *Handler) markKeyObserved(key string) {
	if key == "" {
		return
	}
	h.observedKeys.Store(key, struct{}{})
}

func (h *Handler) cacheLease(leaseID, key string, meta storage.Meta, etag string) {
	h.leaseCache.Store(leaseID, &leaseCacheEntry{key: key, meta: cloneMeta(meta), etag: etag})
	h.markKeyObserved(key)
}

func (h *Handler) leaseSnapshot(leaseID string) (storage.Meta, string, string, bool) {
	if v, ok := h.leaseCache.Load(leaseID); ok {
		entry := v.(*leaseCacheEntry)
		return cloneMeta(entry.meta), entry.etag, entry.key, true
	}
	return storage.Meta{}, "", "", false
}

func (h *Handler) resolveNamespace(ns string) (string, error) {
	if h == nil {
		norm, err := namespaces.Normalize(ns, namespaces.Default)
		if err != nil {
			return "", err
		}
		if strings.HasPrefix(norm, ".") {
			return "", fmt.Errorf("namespace %q is reserved", norm)
		}
		return norm, nil
	}
	if ns == "" {
		return h.defaultNamespace, nil
	}
	normalized, err := namespaces.Normalize(ns, h.defaultNamespace)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(normalized, ".") {
		return "", fmt.Errorf("namespace %q is reserved", normalized)
	}
	return normalized, nil
}

func (h *Handler) observeNamespace(ns string) {
	ns = strings.TrimSpace(ns)
	if ns == "" {
		ns = h.defaultNamespace
	}
	if h.namespaceTracker != nil && ns != "" {
		h.namespaceTracker.Observe(ns)
	}
}

// ObservedNamespaces returns the namespaces seen by this handler since startup.
func (h *Handler) ObservedNamespaces() []string {
	if h == nil || h.namespaceTracker == nil {
		return nil
	}
	return h.namespaceTracker.All()
}

func (h *Handler) namespacedKey(namespace, key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("key required")
	}
	key = strings.TrimPrefix(key, "/")
	normalized, err := namespaces.NormalizeKey(key)
	if err != nil {
		return "", err
	}
	if namespace == "" {
		return normalized, nil
	}
	return namespace + "/" + normalized, nil
}

func (h *Handler) dropLease(leaseID string) {
	h.leaseCache.Delete(leaseID)
}

// SweepTransactions replays committed/expired transaction records for recovery.
func (h *Handler) SweepTransactions(ctx context.Context, now time.Time) error {
	if h == nil || h.core == nil {
		return nil
	}
	return h.core.SweepTxnRecords(ctx, now)
}

// SweepIdleMaintenance runs low-impact maintenance sweeping when the server is idle.
func (h *Handler) SweepIdleMaintenance(ctx context.Context, opts core.IdleSweepOptions) error {
	if h == nil || h.core == nil {
		return nil
	}
	return h.core.SweepIdle(ctx, opts)
}

type httpError struct {
	Status         int
	Code           string
	Detail         string
	LeaderEndpoint string
	Version        int64
	ETag           string
	RetryAfter     int64
}

func (h httpError) Error() string {
	if h.Detail != "" {
		return fmt.Sprintf("%s: %s", h.Code, h.Detail)
	}
	return h.Code
}
