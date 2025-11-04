package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
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
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/jsonutil"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/lsf"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

const defaultPayloadSpoolMemoryThreshold = 4 << 20 // 4 MiB in-memory, then spill to disk
const headerFencingToken = "X-Fencing-Token"
const headerCorrelationID = "X-Correlation-Id"
const headerShutdownImminent = "Shutdown-Imminent"
const maxQueueDequeueBatch = 64
const queueEnsureTimeoutGrace = 2 * time.Second

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

var errQueueEmpty = errors.New("queue: no messages available")
var debugQueueTiming = os.Getenv("LOCKD_DEBUG_QUEUE_TIMING") == "1"
var errDeliveryRetry = errors.New("queue delivery retry")

func queueBlockModeLabel(blockSeconds int64) string {
	switch blockSeconds {
	case api.BlockNoWait:
		return "nowait"
	case 0:
		return "forever"
	default:
		if blockSeconds < 0 {
			return "custom"
		}
		return fmt.Sprintf("%ds", blockSeconds)
	}
}

// Handler wires HTTP endpoints to backend operations.
type Handler struct {
	store                 storage.Backend
	crypto                *storage.Crypto
	queueSvc              *queue.Service
	queueDisp             *queue.Dispatcher
	logger                pslog.Logger
	clock                 clock.Clock
	lsfObserver           *lsf.Observer
	qrf                   *qrf.Controller
	jsonMaxBytes          int64
	compactWriter         func(io.Writer, io.Reader, int64) error
	defaultTTL            time.Duration
	maxTTL                time.Duration
	acquireBlock          time.Duration
	spoolThreshold        int64
	leaseCache            sync.Map
	createLocks           sync.Map
	observedKeys          sync.Map
	enforceClientIdentity bool
	tracer                trace.Tracer
	metaWarmupAttempts    int
	metaWarmupInitial     time.Duration
	metaWarmupMax         time.Duration
	stateWarmupAttempts   int
	stateWarmupInitial    time.Duration
	stateWarmupMax        time.Duration
	pendingDeliveries     sync.Map // map[string]*pendingQueueDeliveries
	shutdownState         func() ShutdownState
}

// ShutdownState exposes the server's current shutdown posture.
type ShutdownState struct {
	Draining  bool
	Remaining time.Duration
	Notify    bool
}

type correlationAppliedKey struct{}

type pendingQueueDeliveries struct {
	mu     sync.Mutex
	owners map[string]map[string]*queueDelivery
}

func logQueueDeliveryInfo(logger pslog.Logger, delivery *queueDelivery) {
	if logger == nil || delivery == nil {
		return
	}
	msg := delivery.message
	if msg == nil {
		return
	}
	fields := []any{
		"mid", msg.MessageID,
		"attempts", msg.Attempts,
		"max_attempts", msg.MaxAttempts,
		"lease", msg.LeaseID,
		"fencing", msg.FencingToken,
		"visibility_timeout_seconds", msg.VisibilityTimeoutSeconds,
	}
	if msg.StateLeaseID != "" {
		fields = append(fields,
			"state_lease", msg.StateLeaseID,
			"state_fencing", msg.StateFencingToken,
		)
	}
	if delivery.nextCursor != "" {
		fields = append(fields, "cursor", delivery.nextCursor)
	}
	logger.Info("queue.delivery.sent", fields...)
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

func (h *Handler) trackPendingDelivery(queue, owner string, delivery *queueDelivery) {
	if h == nil || delivery == nil || queue == "" || owner == "" || delivery.message == nil {
		return
	}
	value, _ := h.pendingDeliveries.LoadOrStore(queue, &pendingQueueDeliveries{
		owners: make(map[string]map[string]*queueDelivery),
	})
	set := value.(*pendingQueueDeliveries)
	set.mu.Lock()
	defer set.mu.Unlock()
	ownerSet := set.owners[owner]
	if ownerSet == nil {
		ownerSet = make(map[string]*queueDelivery)
		set.owners[owner] = ownerSet
	}
	ownerSet[delivery.message.MessageID] = delivery
}

func (h *Handler) clearPendingDelivery(queue, owner, messageID string) {
	if h == nil || queue == "" || owner == "" || messageID == "" {
		return
	}
	value, ok := h.pendingDeliveries.Load(queue)
	if !ok {
		return
	}
	set := value.(*pendingQueueDeliveries)
	set.mu.Lock()
	defer set.mu.Unlock()
	if ownerSet, exists := set.owners[owner]; exists {
		delete(ownerSet, messageID)
		if len(ownerSet) == 0 {
			delete(set.owners, owner)
		}
	}
	if len(set.owners) == 0 {
		h.pendingDeliveries.Delete(queue)
	}
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

func (h *Handler) releasePendingDeliveries(queue, owner string) {
	if h == nil || queue == "" || owner == "" {
		return
	}
	value, ok := h.pendingDeliveries.Load(queue)
	if !ok {
		return
	}
	set := value.(*pendingQueueDeliveries)
	set.mu.Lock()
	ownerSet := set.owners[owner]
	delete(set.owners, owner)
	if len(set.owners) == 0 {
		h.pendingDeliveries.Delete(queue)
	}
	set.mu.Unlock()
	for _, delivery := range ownerSet {
		if delivery != nil {
			delivery.abort()
		}
	}
}

func (h *Handler) beginQueueAck() func() {
	if h.lsfObserver == nil {
		return func() {}
	}
	return h.lsfObserver.BeginQueueAck()
}

func (h *Handler) beginLockOp() func() {
	if h.lsfObserver == nil {
		return func() {}
	}
	return h.lsfObserver.BeginLockOp()
}

func (h *Handler) maybeThrottleQueue(kind qrf.Kind) error {
	if h.qrf == nil {
		return nil
	}
	decision := h.qrf.Decide(kind)
	if !decision.Throttle {
		return nil
	}
	retry := durationToSeconds(decision.RetryAfter)
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

func (h *Handler) maybeThrottleLock() error {
	if h.qrf == nil {
		return nil
	}
	decision := h.qrf.Decide(qrf.KindLock)
	if !decision.Throttle {
		return nil
	}
	retry := durationToSeconds(decision.RetryAfter)
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

// Config groups the dependencies required by Handler.
type Config struct {
	Store                      storage.Backend
	Crypto                     *storage.Crypto
	QueueService               *queue.Service
	Logger                     pslog.Logger
	Clock                      clock.Clock
	JSONMaxBytes               int64
	CompactWriter              func(io.Writer, io.Reader, int64) error
	DefaultTTL                 time.Duration
	MaxTTL                     time.Duration
	AcquireBlock               time.Duration
	SpoolMemoryThreshold       int64
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
	LSFObserver                *lsf.Observer
	QRFController              *qrf.Controller
	ShutdownState              func() ShutdownState
}

// New constructs a Handler using the supplied configuration.
func New(cfg Config) *Handler {
	baseLogger := loggingutil.EnsureLogger(cfg.Logger)
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
		if svc, err := queue.New(cfg.Store, clk, queue.Config{Crypto: crypto}); err == nil {
			queueSvc = svc
		}
	}
	var queueDisp *queue.Dispatcher
	if queueSvc != nil {
		queueLogger := loggingutil.WithSubsystem(baseLogger, "queue.dispatcher.core")
		opts := []queue.DispatcherOption{
			queue.WithLogger(queueLogger),
			queue.WithMaxConsumers(cfg.QueueMaxConsumers),
			queue.WithPollInterval(cfg.QueuePollInterval),
			queue.WithPollJitter(cfg.QueuePollJitter),
		}
		if cfg.QueueResilientPollInterval > 0 {
			opts = append(opts, queue.WithResilientPollInterval(cfg.QueueResilientPollInterval))
		}
		watchMode := "polling"
		watchReason := "backend_missing_change_feed"
		if feed, ok := cfg.Store.(storage.QueueChangeFeed); ok {
			if factory := queue.WatchFactoryFromStorage(feed); factory != nil {
				opts = append(opts, queue.WithWatchFactory(factory))
				watchMode = "change_feed"
				watchReason = "backend_change_feed"
			} else {
				watchReason = "change_feed_factory_unavailable"
			}
		} else {
			watchReason = "backend_no_change_feed"
		}
		queueDisp = queue.NewDispatcher(queueSvc, opts...)
		queueLogger.Info("queue.dispatcher.config",
			"max_consumers", cfg.QueueMaxConsumers,
			"poll_interval", cfg.QueuePollInterval,
			"poll_jitter", cfg.QueuePollJitter,
			"resilient_poll_interval", cfg.QueueResilientPollInterval,
			"watch_mode", watchMode,
			"watch_reason", watchReason,
		)
		if provider, ok := cfg.Store.(storage.QueueWatchStatusProvider); ok {
			enabled, mode, reason := provider.QueueWatchStatus()
			queueLogger.Info("queue.dispatcher.watch_status",
				"enabled", enabled,
				"mode", mode,
				"reason", reason,
			)
		}
	}
	return &Handler{
		store:                 cfg.Store,
		crypto:                crypto,
		queueSvc:              queueSvc,
		queueDisp:             queueDisp,
		logger:                logger,
		clock:                 clk,
		lsfObserver:           cfg.LSFObserver,
		qrf:                   cfg.QRFController,
		jsonMaxBytes:          cfg.JSONMaxBytes,
		compactWriter:         cw,
		defaultTTL:            cfg.DefaultTTL,
		maxTTL:                cfg.MaxTTL,
		acquireBlock:          cfg.AcquireBlock,
		spoolThreshold:        threshold,
		enforceClientIdentity: cfg.EnforceClientIdentity,
		tracer:                otel.Tracer("pkt.systems/lockd/httpapi"),
		metaWarmupAttempts:    metaWarmupAttempts,
		metaWarmupInitial:     metaWarmupInitial,
		metaWarmupMax:         metaWarmupMax,
		stateWarmupAttempts:   stateWarmupAttempts,
		stateWarmupInitial:    stateWarmupInitial,
		stateWarmupMax:        stateWarmupMax,
		shutdownState:         cfg.ShutdownState,
	}
}

// Register wires the routes under /v1 and health endpoints.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle("/v1/acquire", h.wrap("acquire", h.handleAcquire))
	mux.Handle("/v1/keepalive", h.wrap("keepalive", h.handleKeepAlive))
	mux.Handle("/v1/release", h.wrap("release", h.handleRelease))
	mux.Handle("/v1/get", h.wrap("get", h.handleGet))
	mux.Handle("/v1/update", h.wrap("update", h.handleUpdate))
	mux.Handle("/v1/remove", h.wrap("remove", h.handleRemove))
	mux.Handle("/v1/describe", h.wrap("describe", h.handleDescribe))
	mux.Handle("/v1/queue/enqueue", h.wrap("queue.enqueue", h.handleQueueEnqueue))
	mux.Handle("/v1/queue/dequeue", h.wrap("queue.dequeue", h.handleQueueDequeue))
	mux.Handle("/v1/queue/dequeueWithState", h.wrap("queue.dequeue_with_state", h.handleQueueDequeueWithState))
	mux.Handle("/v1/queue/subscribe", h.wrap("queue.subscribe", h.handleQueueSubscribe))
	mux.Handle("/v1/queue/subscribeWithState", h.wrap("queue.subscribe_with_state", h.handleQueueSubscribeWithState))
	mux.Handle("/v1/queue/ack", h.wrap("queue.ack", h.handleQueueAck))
	mux.Handle("/v1/queue/nack", h.wrap("queue.nack", h.handleQueueNack))
	mux.Handle("/v1/queue/extend", h.wrap("queue.extend", h.handleQueueExtend))
	mux.Handle("/healthz", h.wrap("healthz", h.handleHealth))
	mux.Handle("/readyz", h.wrap("readyz", h.handleReady))
}

type handlerFunc func(http.ResponseWriter, *http.Request) error

type acquireParams struct {
	Key           string
	Owner         string
	TTL           time.Duration
	Block         time.Duration
	WaitForever   bool
	Idempotency   string
	GeneratedKey  bool
	CorrelationID string
}

type acquireOutcome struct {
	Response api.AcquireResponse
	Meta     storage.Meta
	MetaETag string
}

func routerSys(operation string) string {
	parts := strings.FieldsFunc(operation, func(r rune) bool {
		switch r {
		case '.', '/', '-', '_':
			return true
		}
		return false
	})
	if len(parts) == 0 {
		return "api.http.router"
	}
	return "api.http.router." + strings.Join(parts, ".")
}

func (h *Handler) wrap(operation string, fn handlerFunc) http.Handler {
	sys := routerSys(operation)
	httpSpanName := "lockd.http." + operation
	txSpanName := "lockd.tx." + operation

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := uuidv7.NewString()
		ctx := r.Context()
		ctx, span := h.tracer.Start(ctx, txSpanName,
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(attribute.String("lockd.sys", sys)),
		)
		defer span.End()

		span.SetAttributes(
			attribute.String("lockd.operation", operation),
			attribute.String("lockd.route", r.URL.Path),
			attribute.Bool("lockd.enforce_identity", h.enforceClientIdentity),
		)
		span.AddEvent("lockd.tx.begin")

		ctx = correlation.Ensure(ctx)

		logger := loggingutil.WithSubsystem(h.logger, sys).With(
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
				span.SetAttributes(attribute.Bool("lockd.has_client_identity", true))
			} else {
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
		defer func() {
			duration := time.Since(start).Milliseconds()
			span.SetStatus(status, statusMsg)
			span.AddEvent("lockd.tx.end", trace.WithAttributes(
				attribute.String("lockd.result", result),
				attribute.Int64("lockd.duration_ms", duration),
			))
		}()

		r = r.WithContext(ctx)
		if err := fn(w, r); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				result = "context"
				status = codes.Error
				statusMsg = "context_canceled"
				span.SetAttributes(attribute.String("lockd.error_code", "context"))
				verboseLogger.Trace("http.request.canceled", "elapsed", time.Since(start))
				return
			}
			result = "error"
			status = codes.Error
			statusMsg = "handler_error"
			span.RecordError(err)
			var httpErr httpError
			if errors.As(err, &httpErr) {
				span.SetAttributes(
					attribute.String("lockd.error_code", httpErr.Code),
					attribute.Int("lockd.error_status", httpErr.Status),
				)
			} else {
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

	return otelhttp.NewHandler(handler, httpSpanName,
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents))
}

func applyCorrelation(ctx context.Context, logger pslog.Logger, span trace.Span) (context.Context, pslog.Logger) {
	if id := correlation.ID(ctx); id != "" {
		if ctx.Value(correlationAppliedKey{}) == nil {
			logger = logger.With("cid", id)
			ctx = context.WithValue(ctx, correlationAppliedKey{}, struct{}{})
		} else if existing := pslog.LoggerFromContext(ctx); existing != nil {
			logger = existing
		}
		ctx = pslog.ContextWithLogger(ctx, logger)
		if span != nil {
			span.SetAttributes(attribute.String("lockd.correlation_id", id))
		}
	}
	return ctx, logger
}

func clientIdentityFromRequest(r *http.Request) string {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return ""
	}
	cert := r.TLS.PeerCertificates[0]
	serial := ""
	if cert.SerialNumber != nil {
		serial = cert.SerialNumber.Text(16)
	}
	return fmt.Sprintf("%s#%s", cert.Subject.String(), serial)
}

// handleAcquire godoc
// @Summary      Acquire an exclusive lease
// @Description  Acquire or wait for an exclusive lease on a key. When block_seconds > 0 the request will long-poll until a lease becomes available or the timeout elapses.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        request  body      api.AcquireRequest  true  "Lease acquisition parameters"
// @Success      200      {object}  api.AcquireResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/acquire [post]
func (h *Handler) handleAcquire(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleLock(); err != nil {
		return err
	}
	finish := h.beginLockOp()
	defer finish()
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.AcquireRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		return httpError{
			Status: http.StatusBadRequest,
			Code:   "invalid_body",
			Detail: fmt.Sprintf("failed to parse request: %v", err),
		}
	}
	if id := clientIdentityFromContext(ctx); id != "" {
		payload.Owner = fmt.Sprintf("%s/%s", payload.Owner, id)
	}
	payload.Idempotency = strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
	if payload.Key == "" {
		payload.Key = r.URL.Query().Get("key")
	}
	autoKey := false
	if payload.Key == "" {
		generated, err := h.generateUniqueKey(ctx)
		if err != nil {
			return fmt.Errorf("generate key: %w", err)
		}
		payload.Key = generated
		autoKey = true
	}
	if payload.Owner == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner is required"}
	}
	ttl := h.resolveTTL(payload.TTLSeconds)
	if ttl <= 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_ttl", Detail: "ttl_seconds must be positive"}
	}
	block, waitForever := h.resolveBlock(payload.BlockSecs)
	if !correlation.Has(ctx) {
		ctx = correlation.Set(ctx, correlation.Generate())
	}
	span := trace.SpanFromContext(ctx)
	ctx, corrLogger := applyCorrelation(ctx, pslog.LoggerFromContext(ctx), span)
	r = r.WithContext(ctx)
	logger := corrLogger
	remoteAddr := h.clientKeyFromRequest(r)
	infoLogger := logger.With(
		"key", payload.Key,
		"owner", payload.Owner,
		"remote_addr", remoteAddr,
	)
	state := h.currentShutdownState()
	if state.Draining {
		retry := durationToSeconds(state.Remaining)
		infoLogger.Info("lease.acquire.reject_shutdown",
			"ttl_seconds", ttl.Seconds(),
			"block_seconds", payload.BlockSecs,
			"remaining_seconds", retry,
		)
		verbose := logger
		verbose.Debug("acquire.reject.shutdown",
			"key", payload.Key,
			"owner", payload.Owner,
			"remaining_seconds", retry,
		)
		return httpError{
			Status:     http.StatusServiceUnavailable,
			Code:       "shutdown_draining",
			Detail:     "server is draining existing leases",
			RetryAfter: retry,
		}
	}
	infoLogger.Info("lease.acquire.begin",
		"ttl_seconds", ttl.Seconds(),
		"block_seconds", payload.BlockSecs,
		"idempotent", payload.Idempotency != "",
		"generated_key", autoKey,
	)
	verbose := logger
	correlationID := correlation.ID(ctx)
	verbose.Debug("acquire.begin",
		"key", payload.Key,
		"owner", payload.Owner,
		"ttl_seconds", ttl.Seconds(),
		"block_seconds", payload.BlockSecs,
		"idempotent", payload.Idempotency != "",
		"generated_key", autoKey,
	)

	var deadline time.Time
	if !waitForever && block > 0 {
		deadline = h.clock.Now().Add(block)
	}
	leaseID := uuidv7.NewString()
	backoff := newAcquireBackoff()
	for {
		now := h.clock.Now()
		meta, metaETag, err := h.ensureMeta(ctx, payload.Key)
		if err != nil {
			return err
		}
		leaseOwner := ""
		leaseExpires := int64(0)
		if meta.Lease != nil {
			leaseOwner = meta.Lease.Owner
			leaseExpires = meta.Lease.ExpiresAtUnix
		}
		verbose.Trace("acquire.meta_snapshot",
			"key", payload.Key,
			"version", meta.Version,
			"meta_etag", metaETag,
			"lease_owner", leaseOwner,
			"lease_expires_at", leaseExpires,
			"state_etag", meta.StateETag,
			"fencing", meta.FencingToken,
		)
		var creationMu *sync.Mutex
		if metaETag == "" {
			creationMu = h.creationMutex(payload.Key)
			creationMu.Lock()
			verbose.Trace("acquire.creation_lock", "key", payload.Key)
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			meta.Lease = nil
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix > now.Unix() {
			verbose.Debug("acquire.lease_busy",
				"key", payload.Key,
				"current_owner", meta.Lease.Owner,
				"expires_at", meta.Lease.ExpiresAtUnix,
			)
			if creationMu != nil {
				creationMu.Unlock()
			}
			if block > 0 && (waitForever || now.Before(deadline)) {
				leaseExpiry := time.Unix(meta.Lease.ExpiresAtUnix, 0)
				limit := leaseExpiry.Sub(now)
				if limit <= 0 {
					limit = acquireBackoffStart
				}
				if !waitForever && !deadline.IsZero() {
					remaining := deadline.Sub(now)
					if remaining > 0 && (limit <= 0 || remaining < limit) {
						limit = remaining
					}
				}
				sleep := backoff.Next(limit)
				verbose.Trace("acquire.wait_loop", "key", payload.Key, "sleep", sleep)
				h.clock.Sleep(sleep)
				continue
			}
			retryDur := max(time.Until(time.Unix(meta.Lease.ExpiresAtUnix, 0)), 0)
			retry := max(int64(math.Ceil(retryDur.Seconds())), 1)
			return httpError{
				Status:     http.StatusConflict,
				Code:       "waiting",
				Detail:     "lease already held",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				RetryAfter: retry,
			}
		}
		expiresAt := now.Add(ttl).Unix()
		newFencing := meta.FencingToken + 1
		meta.FencingToken = newFencing
		meta.Lease = &storage.Lease{
			ID:            leaseID,
			Owner:         payload.Owner,
			ExpiresAtUnix: expiresAt,
			FencingToken:  newFencing,
		}
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := h.store.StoreMeta(ctx, payload.Key, meta, metaETag)
		if err != nil {
			if creationMu != nil {
				creationMu.Unlock()
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				verbose.Trace("acquire.meta_conflict", "key", payload.Key)
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		if creationMu != nil {
			creationMu.Unlock()
		}
		metaETag = newMetaETag
		h.cacheLease(leaseID, payload.Key, *meta, metaETag)
		verbose.Debug("acquire.success",
			"key", payload.Key,
			"lease_id", leaseID,
			"expires_at", expiresAt,
			"version", meta.Version,
			"fencing", newFencing,
			"meta_etag", metaETag,
		)
		infoLogger.Info("lease.acquire.granted",
			"lease_id", leaseID,
			"expires_at", expiresAt,
			"version", meta.Version,
			"fencing", newFencing,
		)

		resp := api.AcquireResponse{
			LeaseID:       leaseID,
			Key:           payload.Key,
			Owner:         payload.Owner,
			ExpiresAt:     expiresAt,
			Version:       meta.Version,
			StateETag:     meta.StateETag,
			FencingToken:  newFencing,
			CorrelationID: correlationID,
		}
		w.Header().Set(headerFencingToken, strconv.FormatInt(newFencing, 10))
		headers := map[string]string{
			"X-Key-Version":     strconv.FormatInt(meta.Version, 10),
			headerCorrelationID: correlationID,
		}
		h.writeJSON(w, http.StatusOK, resp, headers)
		return nil
	}
}

// handleKeepAlive godoc
// @Summary      Extend an active lease TTL
// @Description  Refresh an existing lease before it expires. Returns the new expiration timestamp.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        request  body      api.KeepAliveRequest  true  "Lease keepalive parameters"
// @Success      200      {object}  api.KeepAliveResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/keepalive [post]
func (h *Handler) handleKeepAlive(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleLock(); err != nil {
		return err
	}
	finish := h.beginLockOp()
	defer finish()
	token, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()

	var payload api.KeepAliveRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
	}
	if payload.Key == "" && r.URL.Query().Get("key") != "" {
		payload.Key = r.URL.Query().Get("key")
	}
	if payload.Key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key is required"}
	}
	if payload.LeaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "lease_id required"}
	}
	ttl := h.resolveTTL(payload.TTLSeconds)
	if ttl <= 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_ttl", Detail: "ttl_seconds must be positive"}
	}
	key := payload.Key
	if queue.IsQueueStateKey(key) {
		return httpError{Status: http.StatusForbidden, Code: "queue_state_keepalive_unsupported", Detail: "queue state leases must be extended via queue extend"}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	verbose.Debug("keepalive.begin",
		"key", key,
		"lease_id", payload.LeaseID,
		"ttl_seconds", ttl.Seconds(),
	)
	for {
		now := h.clock.Now()
		cachedMeta, cachedETag, cachedKey, cached := h.leaseSnapshot(payload.LeaseID)
		var meta storage.Meta
		var metaETag string
		if cached && cachedKey == key {
			meta = cachedMeta
			metaETag = cachedETag
		} else {
			loadedMeta, etag, err := h.ensureMeta(ctx, key)
			if err != nil {
				return err
			}
			meta = *loadedMeta
			metaETag = etag
		}
		if err := validateLease(&meta, payload.LeaseID, token, now); err != nil {
			verbose.Warn("keepalive.validate_failed", "key", key, "lease_id", payload.LeaseID, "error", err)
			return err
		}
		meta.Lease.ExpiresAtUnix = now.Add(ttl).Unix()
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := h.store.StoreMeta(ctx, key, &meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				verbose.Trace("keepalive.meta_conflict", "key", key, "lease_id", payload.LeaseID)
				h.dropLease(payload.LeaseID)
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		h.cacheLease(payload.LeaseID, key, meta, newMetaETag)
		resp := api.KeepAliveResponse{ExpiresAt: meta.Lease.ExpiresAtUnix}
		w.Header().Set(headerFencingToken, strconv.FormatInt(meta.Lease.FencingToken, 10))
		h.writeJSON(w, http.StatusOK, resp, nil)
		verbose.Debug("keepalive.success",
			"key", key,
			"lease_id", payload.LeaseID,
			"expires_at", meta.Lease.ExpiresAtUnix,
			"fencing", meta.Lease.FencingToken,
		)
		return nil
	}
}

// handleRelease godoc
// @Summary      Release a held lease
// @Description  Releases the lease associated with the provided key and lease identifier.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        request  body      api.ReleaseRequest  true  "Lease release parameters"
// @Success      200      {object}  api.ReleaseResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/release [post]
func (h *Handler) handleRelease(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleLock(); err != nil {
		return err
	}
	finish := h.beginLockOp()
	defer finish()
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.ReleaseRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
	}
	if payload.Key == "" && r.URL.Query().Get("key") != "" {
		payload.Key = r.URL.Query().Get("key")
	}
	if payload.Key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key is required"}
	}
	if payload.LeaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "lease_id required"}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	verbose.Debug("release.begin", "key", payload.Key, "lease_id", payload.LeaseID)
	for {
		cachedMeta, cachedETag, cachedKey, cached := h.leaseSnapshot(payload.LeaseID)
		var meta storage.Meta
		var metaETag string
		if cached && cachedKey == payload.Key {
			meta = cachedMeta
			metaETag = cachedETag
		} else {
			loadedMeta, etag, err := h.ensureMeta(ctx, payload.Key)
			if err != nil {
				return err
			}
			meta = *loadedMeta
			metaETag = etag
		}
		if err := validateLease(&meta, payload.LeaseID, fencingToken, h.clock.Now()); err != nil {
			verbose.Warn("release.validate_failed", "key", payload.Key, "lease_id", payload.LeaseID, "error", err)
			var httpErr httpError
			if errors.As(err, &httpErr) {
				switch httpErr.Code {
				case "lease_required", "lease_expired", "fencing_mismatch":
					h.dropLease(payload.LeaseID)
					verbose.Debug("release.idempotent", "key", payload.Key, "lease_id", payload.LeaseID, "code", httpErr.Code)
					h.writeJSON(w, http.StatusOK, api.ReleaseResponse{Released: true}, nil)
					return nil
				}
			}
			return err
		}
		meta.Lease = nil
		meta.UpdatedAtUnix = h.clock.Now().Unix()
		released := true
		_, err := h.store.StoreMeta(ctx, payload.Key, &meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				verbose.Trace("release.meta_conflict", "key", payload.Key, "lease_id", payload.LeaseID)
				h.dropLease(payload.LeaseID)
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		h.dropLease(payload.LeaseID)
		h.writeJSON(w, http.StatusOK, api.ReleaseResponse{Released: released}, nil)
		verbose.Debug("release.success", "key", payload.Key, "lease_id", payload.LeaseID)
		return nil
	}
}

// handleGet godoc
// @Summary      Read the JSON checkpoint for a key
// @Description  Streams the currently committed JSON state for the key owned by the caller's lease. Returns 204 when no state is present.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        key              query   string  true   "Lease key"
// @Param        X-Lease-ID       header  string  true   "Lease identifier"
// @Param        X-Fencing-Token  header  string  false  "Optional fencing token proof"
// @Success      200              {object}  map[string]interface{}  "Streamed JSON state"
// @Success      204              {string}  string          "No state stored for this key"
// @Failure      400              {object}  api.ErrorResponse
// @Failure      401              {object}  api.ErrorResponse
// @Failure      409              {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/get [post]
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleLock(); err != nil {
		return err
	}
	finish := h.beginLockOp()
	defer finish()
	key := r.URL.Query().Get("key")
	leaseID := r.Header.Get("X-Lease-ID")
	if key == "" || leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_params", Detail: "key query and X-Lease-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	meta, _, err := h.ensureMeta(ctx, key)
	if err != nil {
		return err
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	remoteAddr := h.clientKeyFromRequest(r)
	infoLogger := logger.With("key", key, "lease_id", leaseID, "remote_addr", remoteAddr)
	infoLogger.Debug("lease.acquire_for_update.load.begin", "fencing_token", fencingToken)
	verbose.Debug("get.begin", "key", key, "lease_id", leaseID)
	if err := validateLease(meta, leaseID, fencingToken, h.clock.Now()); err != nil {
		verbose.Warn("get.validate_failed", "key", key, "lease_id", leaseID, "error", err)
		return err
	}
	expectState := meta.StateETag != ""
	stateCtx := ctx
	if len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	reader, info, err := h.readStateWithWarmup(stateCtx, key, expectState, verbose)
	if errors.Is(err, storage.ErrNotFound) {
		w.WriteHeader(http.StatusNoContent)
		verbose.Debug("get.empty", "key", key, "lease_id", leaseID)
		infoLogger.Debug("lease.acquire_for_update.load.empty")
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	defer reader.Close()
	if info != nil {
		w.Header().Set("ETag", info.ETag)
		if info.Version > 0 {
			w.Header().Set("X-Key-Version", strconv.FormatInt(info.Version, 10))
		} else {
			w.Header().Set("X-Key-Version", strconv.FormatInt(meta.Version, 10))
		}
	}
	w.Header().Set(headerFencingToken, strconv.FormatInt(meta.Lease.FencingToken, 10))
	w.WriteHeader(http.StatusOK)
	size := int64(-1)
	if info != nil {
		size = info.Size
	}
	written, err := io.Copy(w, reader)
	verbose.Debug("get.success",
		"key", key,
		"lease_id", leaseID,
		"bytes", size,
	)
	if err == nil {
		infoLogger.Debug("lease.acquire_for_update.load.success",
			"bytes", written,
			"state_etag", func() string {
				if info != nil {
					return info.ETag
				}
				return ""
			}(),
			"version", w.Header().Get("X-Key-Version"),
		)
	}
	return err
}

// handleUpdate godoc
// @Summary      Atomically update the JSON state for a key
// @Description  Streams JSON from the request body, compacts it, and installs it if the caller holds the lease. Optional CAS headers guard against concurrent updates.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        key                query   string  true   "Lease key"
// @Param        X-Lease-ID         header  string  true   "Lease identifier"
// @Param        X-Fencing-Token    header  string  false  "Optional fencing token proof"
// @Param        X-If-Version       header  string  false  "Conditionally update when the current version matches"
// @Param        X-If-State-ETag    header  string  false  "Conditionally update when the state ETag matches"
// @Param        state              body    string  true   "New JSON state payload"
// @Success      200                {object}  api.UpdateResponse
// @Failure      400                {object}  api.ErrorResponse
// @Failure      404                {object}  api.ErrorResponse
// @Failure      409                {object}  api.ErrorResponse
// @Failure      503                {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/update [post]
func (h *Handler) handleUpdate(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleLock(); err != nil {
		return err
	}
	finish := h.beginLockOp()
	defer finish()
	key := r.URL.Query().Get("key")
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	leaseID := r.Header.Get("X-Lease-ID")
	if leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "X-Lease-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	ifMatch := strings.TrimSpace(r.Header.Get("X-If-State-ETag"))
	ifVersion := strings.TrimSpace(r.Header.Get("X-If-Version"))
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	remoteAddr := h.clientKeyFromRequest(r)
	infoLogger := logger.With("key", key, "lease_id", leaseID, "remote_addr", remoteAddr)
	infoLogger.Debug("lease.acquire_for_update.update.begin",
		"if_match", ifMatch,
		"if_version", ifVersion,
	)
	verbose.Debug("update.begin",
		"key", key,
		"lease_id", leaseID,
		"if_match", ifMatch,
		"if_version", ifVersion,
	)

	body := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer body.Close()

	now := h.clock.Now()
	cachedMeta, cachedETag, cachedKey, cached := h.leaseSnapshot(leaseID)
	var meta storage.Meta
	var metaETag string
	if cached && cachedKey == key {
		meta = cachedMeta
		metaETag = cachedETag
	} else {
		loadedMeta, etag, err := h.ensureMeta(ctx, key)
		if err != nil {
			return err
		}
		meta = *loadedMeta
		metaETag = etag
	}
	if err := validateLease(&meta, leaseID, fencingToken, now); err != nil {
		verbose.Warn("update.validate_failed", "key", key, "lease_id", leaseID, "error", err)
		return err
	}
	if ifVersion != "" {
		want, parseErr := strconv.ParseInt(ifVersion, 10, 64)
		if parseErr != nil {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_version", Detail: parseErr.Error()}
		}
		if meta.Version != want {
			return httpError{
				Status:  http.StatusConflict,
				Code:    "version_conflict",
				Detail:  "state version mismatch",
				Version: meta.Version,
				ETag:    meta.StateETag,
			}
		}
	}

	spool := newPayloadSpool(h.spoolThreshold)
	defer spool.Close()
	if err := h.compactWriter(spool, body, h.jsonMaxBytes); err != nil {
		return err
	}
	payloadReader, err := spool.Reader()
	if err != nil {
		return err
	}

	stateCtx := ctx
	if h.crypto != nil && h.crypto.Enabled() {
		mat, descBytes, err := h.crypto.MintMaterial(storage.StateObjectContext(key))
		if err != nil {
			return fmt.Errorf("mint state descriptor: %w", err)
		}
		mat.Zero()
		meta.StateDescriptor = append([]byte(nil), descBytes...)
		stateCtx = storage.ContextWithStateDescriptor(ctx, descBytes)
	} else {
		meta.StateDescriptor = nil
		meta.StatePlaintextBytes = 0
	}
	putRes, err := h.store.WriteState(stateCtx, key, payloadReader, storage.PutStateOptions{
		ExpectedETag: ifMatch,
	})
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			verbose.Warn("update.etag_conflict", "key", key, "lease_id", leaseID)
			return httpError{
				Status:  http.StatusConflict,
				Code:    "etag_mismatch",
				Detail:  "state etag mismatch",
				Version: meta.Version,
				ETag:    meta.StateETag,
			}
		}
		return fmt.Errorf("write state: %w", err)
	}
	verbose.Trace("update.payload_written", "key", key, "lease_id", leaseID, "bytes", putRes.BytesWritten, "new_etag", putRes.NewETag)
	if len(putRes.Descriptor) > 0 {
		meta.StateDescriptor = append([]byte(nil), putRes.Descriptor...)
	}
	meta.StatePlaintextBytes = putRes.BytesWritten

	meta.Version++
	meta.StateETag = putRes.NewETag
	meta.UpdatedAtUnix = h.clock.Now().Unix()
	newMetaETag, err := h.store.StoreMeta(ctx, key, &meta, metaETag)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			verbose.Trace("update.meta_conflict", "key", key, "lease_id", leaseID)
			return httpError{
				Status:  http.StatusConflict,
				Code:    "meta_conflict",
				Detail:  "state metadata changed concurrently",
				Version: meta.Version,
				ETag:    meta.StateETag,
			}
		}
		return fmt.Errorf("store meta: %w", err)
	}
	h.cacheLease(leaseID, key, meta, newMetaETag)
	verbose.Debug("update.success",
		"key", key,
		"lease_id", leaseID,
		"version", meta.Version,
		"bytes", putRes.BytesWritten,
	)
	infoLogger.Debug("lease.acquire_for_update.update.success",
		"new_version", meta.Version,
		"new_state_etag", meta.StateETag,
		"bytes", putRes.BytesWritten,
	)
	resp := api.UpdateResponse{
		NewVersion:   meta.Version,
		NewStateETag: meta.StateETag,
		Bytes:        putRes.BytesWritten,
	}
	headers := map[string]string{
		"X-Key-Version":    strconv.FormatInt(meta.Version, 10),
		"ETag":             meta.StateETag,
		headerFencingToken: strconv.FormatInt(meta.Lease.FencingToken, 10),
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

// handleRemove godoc
// @Summary      Delete the JSON state for a key
// @Description  Removes the stored state blob if the caller holds the lease. Optional CAS headers guard against concurrent updates.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        key              query   string  true   "Lease key"
// @Param        X-Lease-ID       header  string  true   "Lease identifier"
// @Param        X-Fencing-Token  header  string  false  "Optional fencing token proof"
// @Param        X-If-Version     header  string  false  "Conditionally remove when version matches"
// @Param        X-If-State-ETag  header  string  false  "Conditionally remove when state ETag matches"
// @Success      200              {object}  api.RemoveResponse
// @Failure      400              {object}  api.ErrorResponse
// @Failure      404              {object}  api.ErrorResponse
// @Failure      409              {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/remove [post]
func (h *Handler) handleRemove(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleLock(); err != nil {
		return err
	}
	finish := h.beginLockOp()
	defer finish()
	key := r.URL.Query().Get("key")
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	leaseID := r.Header.Get("X-Lease-ID")
	if leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "X-Lease-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	ifMatch := strings.TrimSpace(r.Header.Get("X-If-State-ETag"))
	ifVersion := strings.TrimSpace(r.Header.Get("X-If-Version"))
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	verbose.Debug("remove.begin",
		"key", key,
		"lease_id", leaseID,
		"if_match", ifMatch,
		"if_version", ifVersion,
	)

	now := h.clock.Now()
	cachedMeta, cachedETag, cachedKey, cached := h.leaseSnapshot(leaseID)
	var meta storage.Meta
	var metaETag string
	if cached && cachedKey == key {
		meta = cachedMeta
		metaETag = cachedETag
	} else {
		loadedMeta, etag, err := h.ensureMeta(ctx, key)
		if err != nil {
			return err
		}
		meta = *loadedMeta
		metaETag = etag
	}
	if err := validateLease(&meta, leaseID, fencingToken, now); err != nil {
		verbose.Warn("remove.validate_failed", "key", key, "lease_id", leaseID, "error", err)
		return err
	}
	if ifVersion != "" {
		want, parseErr := strconv.ParseInt(ifVersion, 10, 64)
		if parseErr != nil {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_version", Detail: parseErr.Error()}
		}
		if meta.Version != want {
			return httpError{
				Status:  http.StatusConflict,
				Code:    "version_conflict",
				Detail:  "state version mismatch",
				Version: meta.Version,
				ETag:    meta.StateETag,
			}
		}
	}

	hadMetaState := meta.StateETag != ""
	removeErr := h.store.Remove(ctx, key, ifMatch)
	removed := false
	switch {
	case removeErr == nil:
		removed = true
	case errors.Is(removeErr, storage.ErrNotFound):
		removed = hadMetaState
	case errors.Is(removeErr, storage.ErrCASMismatch):
		verbose.Debug("remove.etag_conflict", "key", key, "lease_id", leaseID)
		return httpError{
			Status:  http.StatusConflict,
			Code:    "etag_mismatch",
			Detail:  "state etag mismatch",
			Version: meta.Version,
			ETag:    meta.StateETag,
		}
	default:
		return fmt.Errorf("remove state: %w", removeErr)
	}

	changed := false
	if removed || hadMetaState {
		if meta.StateETag != "" {
			changed = true
		}
		meta.StateETag = ""
		meta.Version++
		changed = true
		meta.UpdatedAtUnix = now.Unix()
	}

	if changed {
		meta.StateDescriptor = nil
		meta.StatePlaintextBytes = 0
		newMetaETag, err := h.store.StoreMeta(ctx, key, &meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				verbose.Trace("remove.meta_conflict", "key", key, "lease_id", leaseID)
				return httpError{
					Status:  http.StatusConflict,
					Code:    "meta_conflict",
					Detail:  "state metadata changed concurrently",
					Version: meta.Version,
					ETag:    meta.StateETag,
				}
			}
			return fmt.Errorf("store meta: %w", err)
		}
		metaETag = newMetaETag
	}
	h.cacheLease(leaseID, key, meta, metaETag)
	resp := api.RemoveResponse{
		Removed:    removed,
		NewVersion: meta.Version,
	}
	headers := map[string]string{
		"X-Key-Version":    strconv.FormatInt(meta.Version, 10),
		headerFencingToken: strconv.FormatInt(meta.Lease.FencingToken, 10),
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	verbose.Debug("remove.success",
		"key", key,
		"lease_id", leaseID,
		"removed", removed,
		"version", meta.Version,
	)
	return nil
}

// handleDescribe godoc
// @Summary      Inspect metadata for a key
// @Description  Returns lease and state metadata without streaming the state payload.
// @Tags         lease
// @Produce      json
// @Param        key  query  string  true  "Lease key"
// @Success      200  {object}  api.DescribeResponse
// @Failure      400  {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/describe [get]
func (h *Handler) handleDescribe(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	key := r.URL.Query().Get("key")
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	verbose.Debug("describe.begin", "key", key)
	meta, _, err := h.ensureMeta(ctx, key)
	if err != nil {
		return err
	}
	resp := api.DescribeResponse{
		Key:       key,
		Version:   meta.Version,
		StateETag: meta.StateETag,
		UpdatedAt: meta.UpdatedAtUnix,
	}
	if meta.Lease != nil {
		resp.LeaseID = meta.Lease.ID
		resp.Owner = meta.Lease.Owner
		resp.ExpiresAt = meta.Lease.ExpiresAtUnix
	}
	h.writeJSON(w, http.StatusOK, resp, nil)
	verbose.Trace("describe.success", "key", key, "version", resp.Version, "state_etag", resp.StateETag, "lease_id", resp.LeaseID)
	return nil
}

// handleQueueEnqueue godoc
// @Summary      Enqueue a message
// @Description  Writes a message into the durable queue. The payload is streamed directly and may include optional attributes.
// @Tags         queue
// @Accept       multipart/form-data
// @Produce      json
// @Param        meta     formData  string  true   "JSON encoded api.EnqueueRequest metadata"
// @Param        payload  formData  file    false  "Optional payload stream"
// @Success      200      {object}  api.EnqueueResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/enqueue [post]
func (h *Handler) handleQueueEnqueue(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueProducer); err != nil {
		return err
	}
	finish := h.beginQueueProducer()
	defer finish()
	qsvc, err := h.requireQueueService()
	if err != nil {
		return err
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	contentType := r.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		return httpError{Status: http.StatusUnsupportedMediaType, Code: "invalid_content_type", Detail: "expected multipart content"}
	}
	boundary := params["boundary"]
	if boundary == "" {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_multipart", Detail: "missing multipart boundary"}
	}
	mr := multipart.NewReader(r.Body, boundary)

	var meta api.EnqueueRequest
	var haveMeta bool
	var payloadReader io.Reader
	var payloadCloser io.Closer
	var payloadContentType string

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("queue enqueue multipart: %w", err)
		}
		name := part.FormName()
		switch {
		case name == "meta" || strings.EqualFold(part.Header.Get("Content-Type"), "application/json"):
			if haveMeta {
				part.Close()
				return httpError{Status: http.StatusBadRequest, Code: "duplicate_meta", Detail: "duplicate meta part"}
			}
			if err := json.NewDecoder(part).Decode(&meta); err != nil {
				part.Close()
				return httpError{Status: http.StatusBadRequest, Code: "invalid_meta", Detail: fmt.Sprintf("failed to parse meta: %v", err)}
			}
			haveMeta = true
			part.Close()
		case name == "payload" || payloadReader == nil:
			payloadReader = part
			payloadCloser = part
			payloadContentType = part.Header.Get("Content-Type")
		default:
			part.Close()
		}
		if haveMeta && payloadReader != nil {
			break
		}
	}

	if !haveMeta {
		return httpError{Status: http.StatusBadRequest, Code: "missing_meta", Detail: "meta part required"}
	}

	queueName := strings.TrimSpace(meta.Queue)
	if queueName == "" {
		queueName = strings.TrimSpace(r.URL.Query().Get("queue"))
	}
	if queueName == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_queue", Detail: "queue is required"}
	}

	remoteAddr := h.clientKeyFromRequest(r)
	statsBefore := queue.Stats{}
	if h.queueDisp != nil {
		statsBefore = h.queueDisp.QueueStats(queueName)
	}
	enqueueLogger := logger.With("queue", queueName, "remote_addr", remoteAddr)
	enqueueLogger.Info("queue.enqueue.begin",
		"waiting_consumers", statsBefore.WaitingConsumers,
		"pending_candidates", statsBefore.PendingCandidates,
		"total_consumers", statsBefore.TotalConsumers,
		"content_length", r.ContentLength,
	)

	opts := queue.EnqueueOptions{
		Delay:       time.Duration(meta.DelaySeconds) * time.Second,
		Visibility:  time.Duration(meta.VisibilityTimeoutSeconds) * time.Second,
		TTL:         time.Duration(meta.TTLSeconds) * time.Second,
		MaxAttempts: meta.MaxAttempts,
		Attributes:  meta.Attributes,
		ContentType: meta.PayloadContentType,
	}
	if opts.ContentType == "" {
		opts.ContentType = payloadContentType
	}
	var reader io.Reader = payloadReader
	if reader == nil {
		reader = bytes.NewReader(nil)
	}

	msg, err := qsvc.Enqueue(ctx, queueName, reader, opts)
	if payloadCloser != nil {
		payloadCloser.Close()
	}
	if err != nil {
		enqueueLogger.Warn("queue.enqueue.fail",
			"waiting_consumers", statsBefore.WaitingConsumers,
			"pending_candidates", statsBefore.PendingCandidates,
			"total_consumers", statsBefore.TotalConsumers,
			"error", err,
		)
		return err
	}
	if h.queueDisp != nil {
		h.queueDisp.Notify(msg.Queue)
	}
	statsAfter := statsBefore
	if h.queueDisp != nil {
		statsAfter = h.queueDisp.QueueStats(queueName)
	}
	corr := msg.CorrelationID
	if corr == "" {
		corr = correlation.ID(ctx)
	}
	if corr != "" {
		enqueueLogger = enqueueLogger.With("cid", corr)
	}
	enqueueLogger.Info("queue.enqueue.success",
		"mid", msg.ID,
		"attempts", msg.Attempts,
		"max_attempts", msg.MaxAttempts,
		"payload_bytes", msg.PayloadBytes,
		"waiting_consumers_before", statsBefore.WaitingConsumers,
		"pending_candidates_before", statsBefore.PendingCandidates,
		"waiting_consumers_after", statsAfter.WaitingConsumers,
		"pending_candidates_after", statsAfter.PendingCandidates,
		"total_consumers_after", statsAfter.TotalConsumers,
	)

	resp := api.EnqueueResponse{
		Queue:                    msg.Queue,
		MessageID:                msg.ID,
		Attempts:                 msg.Attempts,
		MaxAttempts:              msg.MaxAttempts,
		NotVisibleUntilUnix:      msg.NotVisibleUntil.Unix(),
		VisibilityTimeoutSeconds: int64(msg.Visibility / time.Second),
		PayloadBytes:             msg.PayloadBytes,
		CorrelationID:            corr,
	}
	headers := map[string]string{}
	if corr != "" {
		headers[headerCorrelationID] = corr
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

// handleQueueDequeue godoc
// @Summary      Fetch one or more queue messages
// @Description  Performs a single dequeue attempt, optionally waiting for availability. Responses stream as multipart/related parts containing message metadata and payload.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        request  body      api.DequeueRequest  true  "Dequeue parameters"
// @Success      200      {string}  string  "Multipart response with message metadata and optional payload"
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/dequeue [post]
func (h *Handler) handleQueueDequeue(w http.ResponseWriter, r *http.Request) error {
	baseCtx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueConsumer); err != nil {
		return err
	}
	finish := h.beginQueueConsumer()
	defer finish()
	qsvc, err := h.requireQueueService()
	if err != nil {
		return err
	}

	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.DequeueRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}

	queueName := strings.TrimSpace(req.Queue)
	if queueName == "" {
		queueName = strings.TrimSpace(r.URL.Query().Get("queue"))
	}
	if queueName == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_queue", Detail: "queue is required"}
	}

	owner := strings.TrimSpace(req.Owner)
	if owner == "" {
		owner = strings.TrimSpace(r.URL.Query().Get("owner"))
	}

	blockSeconds := req.WaitSeconds
	waitDuration := time.Duration(blockSeconds) * time.Second
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 1
	}
	if pageSize > maxQueueDequeueBatch {
		pageSize = maxQueueDequeueBatch
	}
	ctx := baseCtx
	var cancel context.CancelFunc
	if blockSeconds > 0 {
		timeout := waitDuration + queueEnsureTimeoutGrace
		ctx, cancel = context.WithTimeout(baseCtx, timeout)
		defer cancel()
	}

	if !correlation.Has(ctx) {
		ctx = correlation.Set(ctx, correlation.Generate())
	}
	span := trace.SpanFromContext(ctx)
	ctx, logger := applyCorrelation(ctx, pslog.LoggerFromContext(ctx), span)
	r = r.WithContext(ctx)
	owner = h.appendQueueOwner(ctx, owner)
	if owner == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner is required"}
	}

	remoteAddr := h.clientKeyFromRequest(r)
	queueLogger := logger.With("queue", queueName, "owner", owner, "stateful", false)
	if state := h.currentShutdownState(); state.Draining {
		retry := durationToSeconds(state.Remaining)
		queueLogger.Info("queue.dequeue.reject_shutdown",
			"block_seconds", blockSeconds,
			"remaining_seconds", retry,
		)
		return httpError{
			Status:     http.StatusServiceUnavailable,
			Code:       "shutdown_draining",
			Detail:     "server is draining existing leases",
			RetryAfter: retry,
		}
	}
	queueLogger.Info("queue.consumer.connect",
		"block_seconds", blockSeconds,
		"block_mode", queueBlockModeLabel(blockSeconds),
		"remote_addr", remoteAddr,
		"page_size", pageSize,
	)
	disconnectStatus := "unknown"
	var deliveries []*queueDelivery
	var nextCursor string
	defer func() {
		fields := []any{
			"status", disconnectStatus,
			"block_seconds", blockSeconds,
			"block_mode", queueBlockModeLabel(blockSeconds),
			"remote_addr", remoteAddr,
			"delivered_count", len(deliveries),
		}
		if disconnectStatus == "error" {
			queueLogger.Warn("queue.consumer.disconnect", fields...)
		} else {
			queueLogger.Info("queue.consumer.disconnect", fields...)
		}
	}()

	visibility := h.resolveTTL(req.VisibilityTimeoutSeconds)
	const maxEnsureRetries = 8
	ensureRetries := 0
	for {
		deliveries, nextCursor, err = h.consumeQueueBatch(ctx, queueLogger, qsvc, queueName, owner, visibility, false, blockSeconds, pageSize)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, errQueueEmpty) {
				disconnectStatus = "empty"
				retryAfter := int64(1)
				switch {
				case blockSeconds == api.BlockNoWait:
					retryAfter = 0
				case blockSeconds > 0:
					retryAfter = max(int64(math.Ceil(waitDuration.Seconds())), 1)
				}
				return httpError{
					Status:     http.StatusConflict,
					Code:       "waiting",
					Detail:     "no messages available",
					RetryAfter: retryAfter,
				}
			}
			disconnectStatus = "error"
			h.logQueueSubscribeError(queueLogger, queueName, owner, err)
			return err
		}

		retryFetch := false
		for _, delivery := range deliveries {
			if delivery == nil {
				continue
			}
			if err := delivery.ensure(ctx); err != nil {
				if errors.Is(err, errDeliveryRetry) {
					delivery.abort()
					if ensureRetries < maxEnsureRetries {
						ensureRetries++
						retryFetch = true
						break
					}
					disconnectStatus = "empty"
					return httpError{
						Status:     http.StatusConflict,
						Code:       "waiting",
						Detail:     "no messages available",
						RetryAfter: 1,
					}
				}
				delivery.abort()
				disconnectStatus = "error"
				h.logQueueSubscribeError(queueLogger, queueName, owner, err)
				return err
			}
		}
		if retryFetch {
			continue
		}
		break
	}

	success := false
	defer func() {
		for _, delivery := range deliveries {
			if delivery == nil {
				continue
			}
			if success {
				delivery.complete()
			} else {
				delivery.abort()
			}
		}
	}()
	if err := writeQueueDeliveryBatch(w, deliveries, nextCursor); err != nil {
		disconnectStatus = "error"
		h.logQueueSubscribeError(queueLogger, queueName, owner, err)
		return err
	}
	success = true
	for _, delivery := range deliveries {
		if delivery != nil {
			logQueueDeliveryInfo(queueLogger, delivery)
		}
	}
	disconnectStatus = "delivered"
	return nil
}

// handleQueueDequeueWithState godoc
// @Summary      Fetch queue messages with state attachments
// @Description  Dequeues messages and includes their associated state blobs in the multipart response when available.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        request  body      api.DequeueRequest  true  "Dequeue parameters"
// @Success      200      {string}  string  "Multipart response with message metadata, payload, and state attachments"
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/dequeueWithState [post]
func (h *Handler) handleQueueDequeueWithState(w http.ResponseWriter, r *http.Request) error {
	baseCtx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueConsumer); err != nil {
		return err
	}
	finish := h.beginQueueConsumer()
	defer finish()
	qsvc, err := h.requireQueueService()
	if err != nil {
		return err
	}

	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.DequeueRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}

	queueName := strings.TrimSpace(req.Queue)
	if queueName == "" {
		queueName = strings.TrimSpace(r.URL.Query().Get("queue"))
	}
	if queueName == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_queue", Detail: "queue is required"}
	}

	owner := strings.TrimSpace(req.Owner)
	if owner == "" {
		owner = strings.TrimSpace(r.URL.Query().Get("owner"))
	}

	blockSeconds := req.WaitSeconds
	waitDuration := time.Duration(blockSeconds) * time.Second
	ctx := baseCtx
	var cancel context.CancelFunc
	if blockSeconds > 0 {
		timeout := waitDuration + queueEnsureTimeoutGrace
		ctx, cancel = context.WithTimeout(baseCtx, timeout)
		defer cancel()
	}

	if !correlation.Has(ctx) {
		ctx = correlation.Set(ctx, correlation.Generate())
	}
	span := trace.SpanFromContext(ctx)
	ctx, logger := applyCorrelation(ctx, pslog.LoggerFromContext(ctx), span)
	r = r.WithContext(ctx)
	owner = h.appendQueueOwner(ctx, owner)
	if owner == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner is required"}
	}

	remoteAddr := h.clientKeyFromRequest(r)
	queueLogger := logger.With("queue", queueName, "owner", owner, "stateful", true)
	if state := h.currentShutdownState(); state.Draining {
		retry := durationToSeconds(state.Remaining)
		queueLogger.Info("queue.dequeue_state.reject_shutdown",
			"block_seconds", blockSeconds,
			"remaining_seconds", retry,
		)
		return httpError{
			Status:     http.StatusServiceUnavailable,
			Code:       "shutdown_draining",
			Detail:     "server is draining existing leases",
			RetryAfter: retry,
		}
	}
	queueLogger.Info("queue.consumer.connect",
		"block_seconds", blockSeconds,
		"block_mode", queueBlockModeLabel(blockSeconds),
		"remote_addr", remoteAddr,
	)
	disconnectStatus := "unknown"
	defer func() {
		fields := []any{
			"status", disconnectStatus,
			"block_seconds", blockSeconds,
			"block_mode", queueBlockModeLabel(blockSeconds),
			"remote_addr", remoteAddr,
		}
		if disconnectStatus == "error" {
			queueLogger.Warn("queue.consumer.disconnect", fields...)
		} else {
			queueLogger.Info("queue.consumer.disconnect", fields...)
		}
	}()

	visibility := h.resolveTTL(req.VisibilityTimeoutSeconds)
	deliveries, nextCursor, err := h.consumeQueueBatch(ctx, queueLogger, qsvc, queueName, owner, visibility, true, blockSeconds, 1)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, errQueueEmpty) {
			disconnectStatus = "empty"
			retryAfter := int64(1)
			switch {
			case blockSeconds == api.BlockNoWait:
				retryAfter = 0
			case blockSeconds > 0:
				retryAfter = max(int64(math.Ceil(waitDuration.Seconds())), 1)
			}
			return httpError{
				Status:     http.StatusConflict,
				Code:       "waiting",
				Detail:     "no messages available",
				RetryAfter: retryAfter,
			}
		}
		disconnectStatus = "error"
		return err
	}

	success := false
	defer func() {
		for _, d := range deliveries {
			if d != nil {
				d.finalize(success)
			}
		}
	}()
	if err := writeQueueDeliveryBatch(w, deliveries, nextCursor); err != nil {
		disconnectStatus = "error"
		return err
	}
	success = true
	for _, delivery := range deliveries {
		if delivery != nil {
			logQueueDeliveryInfo(queueLogger, delivery)
		}
	}
	disconnectStatus = "delivered"
	return nil
}

// handleQueueSubscribe godoc
// @Summary      Stream queue deliveries
// @Description  Opens a long-lived multipart stream of deliveries for the specified queue owner. Each part contains message metadata and payload.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        request  body      api.DequeueRequest  true  "Subscription parameters (queue, owner, wait_seconds, visibility)"
// @Success      200      {string}  string  "Multipart stream of message deliveries"
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/subscribe [post]
func (h *Handler) handleQueueSubscribe(w http.ResponseWriter, r *http.Request) error {
	return h.handleQueueSubscribeInternal(w, r, false)
}

// handleQueueSubscribeWithState godoc
// @Summary      Stream queue deliveries with state
// @Description  Opens a long-lived multipart stream where each part contains message metadata, payload, and state snapshot when available.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        request  body      api.DequeueRequest  true  "Subscription parameters (queue, owner, wait_seconds, visibility)"
// @Success      200      {string}  string  "Multipart stream of message deliveries"
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/subscribeWithState [post]
func (h *Handler) handleQueueSubscribeWithState(w http.ResponseWriter, r *http.Request) error {
	return h.handleQueueSubscribeInternal(w, r, true)
}

func (h *Handler) handleQueueSubscribeInternal(w http.ResponseWriter, r *http.Request, stateful bool) error {
	baseCtx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueConsumer); err != nil {
		return err
	}
	finish := h.beginQueueConsumer()
	defer finish()
	qsvc, err := h.requireQueueService()
	if err != nil {
		return err
	}

	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()

	var req api.DequeueRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}

	queueName := strings.TrimSpace(req.Queue)
	if queueName == "" {
		queueName = strings.TrimSpace(r.URL.Query().Get("queue"))
	}
	if queueName == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_queue", Detail: "queue is required"}
	}

	owner := strings.TrimSpace(req.Owner)
	if owner == "" {
		owner = strings.TrimSpace(r.URL.Query().Get("owner"))
	}
	blockSeconds := req.WaitSeconds
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 1
	}

	ctx := baseCtx
	if !correlation.Has(ctx) {
		ctx = correlation.Set(ctx, correlation.Generate())
	}
	span := trace.SpanFromContext(ctx)
	ctx, logger := applyCorrelation(ctx, pslog.LoggerFromContext(ctx), span)
	r = r.WithContext(ctx)
	owner = h.appendQueueOwner(ctx, owner)
	if owner == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner is required"}
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		return httpError{Status: http.StatusInternalServerError, Code: "streaming_unsupported", Detail: "streaming not supported by response writer"}
	}

	visibility := h.resolveTTL(req.VisibilityTimeoutSeconds)
	remoteAddr := h.clientKeyFromRequest(r)
	queueLogger := logger.With("queue", queueName, "owner", owner, "stateful", stateful)
	if state := h.currentShutdownState(); state.Draining {
		retry := durationToSeconds(state.Remaining)
		queueLogger.Info("queue.subscribe.reject_shutdown",
			"block_seconds", blockSeconds,
			"remaining_seconds", retry,
			"prefetch", pageSize,
		)
		return httpError{
			Status:     http.StatusServiceUnavailable,
			Code:       "shutdown_draining",
			Detail:     "server is draining existing leases",
			RetryAfter: retry,
		}
	}
	queueLogger.Info("queue.subscribe.connect",
		"block_seconds", blockSeconds,
		"block_mode", queueBlockModeLabel(blockSeconds),
		"remote_addr", remoteAddr,
		"prefetch", pageSize,
	)
	defer h.releasePendingDeliveries(queueName, owner)

	headersWritten := false
	var writer *multipart.Writer
	writerCreated := false
	var deliveredCount int
	disconnectStatus := "unknown"
	defer func() {
		fields := []any{
			"status", disconnectStatus,
			"delivered_count", deliveredCount,
			"block_seconds", blockSeconds,
			"block_mode", queueBlockModeLabel(blockSeconds),
			"remote_addr", remoteAddr,
		}
		if disconnectStatus == "error" {
			queueLogger.Warn("queue.subscribe.disconnect", fields...)
		} else {
			queueLogger.Info("queue.subscribe.disconnect", fields...)
		}
		if writerCreated && writer != nil {
			_ = writer.Close()
		}
	}()

	writeDeliveries := func(deliveries []*queueDelivery, defaultCursor string) error {
		if writer == nil {
			writer = multipart.NewWriter(w)
			writerCreated = true
		}
		firstCID := ""
		for _, delivery := range deliveries {
			if delivery != nil && delivery.message != nil && delivery.message.CorrelationID != "" {
				firstCID = delivery.message.CorrelationID
				break
			}
		}
		if !headersWritten {
			contentType := fmt.Sprintf("multipart/related; boundary=%s", writer.Boundary())
			w.Header().Set("Content-Type", contentType)
			if firstCID != "" {
				w.Header().Set(headerCorrelationID, firstCID)
			}
			w.WriteHeader(http.StatusOK)
			headersWritten = true
		}
		if _, err := writeQueueDeliveriesToWriter(writer, deliveries, defaultCursor); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}

	const subscribeHotWindow = 2 * time.Second
	var hotUntil time.Time

	now := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			disconnectStatus = "context"
			break
		}

		if blockSeconds > 1 && h.queueDisp != nil && h.queueDisp.HasActiveWatcher(queueName) {
			blockSeconds = 1
		}

		loopBlock := blockSeconds
		if deliveredCount > 0 {
			now = time.Now()
			if hotUntil.After(now) {
				loopBlock = api.BlockNoWait
			} else if blockSeconds > 1 {
				loopBlock = 1
			}
		}
		if loopBlock > 1 && h.queueDisp != nil && h.queueDisp.HasActiveWatcher(queueName) {
			loopBlock = 1
		}
		iterCtx := ctx
		var cancel context.CancelFunc
		if loopBlock > 0 {
			waitDuration := time.Duration(loopBlock) * time.Second
			iterCtx, cancel = context.WithTimeout(ctx, waitDuration)
		}

		deliveries, nextCursor, err := h.consumeQueueBatch(iterCtx, queueLogger, qsvc, queueName, owner, visibility, stateful, loopBlock, pageSize)
		if err != nil {
			if cancel != nil {
				cancel()
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, errQueueEmpty) {
				hotUntil = time.Time{}
				if deliveredCount == 0 && !headersWritten {
					disconnectStatus = "empty"
					retryAfter := int64(1)
					switch {
					case blockSeconds == api.BlockNoWait:
						retryAfter = 0
					case blockSeconds > 0:
						waitDuration := time.Duration(blockSeconds) * time.Second
						retryAfter = max(int64(math.Ceil(waitDuration.Seconds())), 1)
					}
					return httpError{
						Status:     http.StatusConflict,
						Code:       "waiting",
						Detail:     "no messages available",
						RetryAfter: retryAfter,
					}
				}
				if blockSeconds == api.BlockNoWait {
					disconnectStatus = "empty"
					break
				}
				// wait for more messages
				continue
			}
			disconnectStatus = "error"
			for _, delivery := range deliveries {
				if delivery != nil {
					if delivery.message != nil {
						h.clearPendingDelivery(queueName, owner, delivery.message.MessageID)
					}
					delivery.abort()
				}
			}
			return err
		}

		for _, delivery := range deliveries {
			if delivery == nil {
				continue
			}
			candidateID := delivery.descriptor.Document.ID
			if err := delivery.ensure(iterCtx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					delivery.abort()
					disconnectStatus = "context"
					if ctxErr := ctx.Err(); ctxErr != nil {
						return ctxErr
					}
					return err
				}
				if queueLogger != nil {
					fields := []any{
						"queue", queueName,
						"owner", owner,
						"mid", candidateID,
						"error", err,
					}
					if delivery.message != nil {
						if delivery.message.MessageID != "" {
							fields[5] = delivery.message.MessageID
						}
						if lease := delivery.message.LeaseID; lease != "" {
							fields = append(fields, "lease", lease)
						}
						if fencing := delivery.message.FencingToken; fencing != 0 {
							fields = append(fields, "fencing", fencing)
						}
					}
					if errors.Is(err, errDeliveryRetry) {
						queueLogger.Trace("queue.subscribe.ensure_retry", fields...)
					} else {
						queueLogger.Warn("queue.subscribe.ensure_error", fields...)
					}
				}
				delivery.abort()
				if errors.Is(err, errDeliveryRetry) {
					hotUntil = time.Now().Add(subscribeHotWindow)
					continue
				}
				disconnectStatus = "error"
				return err
			}
			hotUntil = time.Now().Add(subscribeHotWindow)
			h.trackPendingDelivery(queueName, owner, delivery)
			writeErr := writeDeliveries([]*queueDelivery{delivery}, nextCursor)
			if writeErr != nil {
				if delivery.message != nil {
					h.clearPendingDelivery(queueName, owner, delivery.message.MessageID)
				}
				if debugQueueTiming {
					fmt.Fprintf(os.Stderr, "[%s] queue.subscribe.write_error queue=%s mid=%s err=%v\n",
						time.Now().Format(time.RFC3339Nano), queueName, delivery.message.MessageID, writeErr)
				}
				if queueLogger != nil {
					fields := []any{
						"queue", queueName,
						"owner", owner,
						"error", writeErr,
					}
					if delivery.message != nil {
						fields = append(fields,
							"mid", delivery.message.MessageID,
							"lease", delivery.message.LeaseID,
							"fencing", delivery.message.FencingToken,
						)
					} else if candidateID != "" {
						fields = append(fields, "mid", candidateID)
					}
					queueLogger.Warn("queue.subscribe.write_error", fields...)
				}
				delivery.abort()
				disconnectStatus = "error"
				return writeErr
			}
			if debugQueueTiming {
				fmt.Fprintf(os.Stderr, "[%s] queue.subscribe.delivered queue=%s mid=%s\n",
					time.Now().Format(time.RFC3339Nano), queueName, delivery.message.MessageID)
			}
			logQueueDeliveryInfo(queueLogger, delivery)
			delivery.complete()
			deliveredCount++
			queueLogger.Trace("queue.subscribe.delivered", "mid", delivery.message.MessageID, "cursor", delivery.nextCursor, "delivered", deliveredCount)
		}
		if cancel != nil {
			cancel()
		}
	}

	if disconnectStatus == "unknown" {
		disconnectStatus = "complete"
	}

	return nil
}

// handleQueueAck godoc
// @Summary      Acknowledge a delivered message
// @Description  Confirms processing of a delivery and deletes the message or its retry lease.
// @Tags         queue
// @Accept       json
// @Produce      json
// @Param        request  body      api.AckRequest  true  "Acknowledgement payload"
// @Success      200      {object}  api.AckResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/ack [post]
func (h *Handler) handleQueueAck(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueAck); err != nil {
		return err
	}
	finish := h.beginQueueAck()
	defer finish()
	qsvc, err := h.requireQueueService()
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.AckRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_fields", Detail: "queue, message_id, lease_id are required"}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	queueLogger := logger.With(
		"queue", req.Queue,
		"mid", req.MessageID,
		"lease", req.LeaseID,
	)
	if req.StateLeaseID != "" {
		queueLogger = queueLogger.With("state_lease", req.StateLeaseID)
	}
	queueLogger.Debug("queue.ack.begin")
	messageKey, err := queue.MessageLeaseKey(req.Queue, req.MessageID)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_queue_key", Detail: err.Error()}
	}
	meta, metaETag, err := h.ensureMeta(ctx, messageKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return httpError{Status: http.StatusNotFound, Code: "not_found", Detail: "message lease not found"}
		}
		return err
	}
	leaseOwner := ""
	if meta.Lease != nil {
		leaseOwner = meta.Lease.Owner
	}

	doc, docMetaETag, err := qsvc.GetMessage(ctx, req.Queue, req.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			queueLogger.Warn("queue.nack.message_missing", "error", err)
			return httpError{Status: http.StatusNotFound, Code: "not_found", Detail: "message not found"}
		}
		queueLogger.Error("queue.nack.load_message_error", "error", err)
		return fmt.Errorf("queue load message: %w", err)
	}
	now := h.clock.Now()
	if err := validateLease(meta, req.LeaseID, req.FencingToken, now); err != nil {
		var httpErr httpError
		if errors.As(err, &httpErr) && httpErr.Code == "lease_required" && meta.Lease != nil && meta.Lease.Owner != "" {
			if debugQueueTiming {
				fmt.Fprintf(os.Stderr, "[%s] queue.ack.lease_upgrade queue=%s mid=%s prior=%s new=%s fencing=%d\n",
					now.Format(time.RFC3339Nano), req.Queue, req.MessageID, req.LeaseID, meta.Lease.ID, meta.Lease.FencingToken)
			}
			queueLogger.Trace("queue.ack.lease_upgrade",
				"prior", req.LeaseID,
				"new", meta.Lease.ID,
				"fencing", meta.Lease.FencingToken,
			)
			req.LeaseID = meta.Lease.ID
			req.FencingToken = meta.Lease.FencingToken
			if docMetaETag != "" {
				req.MetaETag = docMetaETag
				metaETag = docMetaETag
			}
			if meta.Lease != nil {
				leaseOwner = meta.Lease.Owner
			}
		} else {
			if debugQueueTiming && meta != nil && meta.Lease != nil {
				fmt.Fprintf(os.Stderr, "[%s] queue.ack.lease_mismatch queue=%s mid=%s expected=%s actual=%s fencing=%d meta_version=%d\n",
					now.Format(time.RFC3339Nano), req.Queue, req.MessageID, req.LeaseID, meta.Lease.ID, meta.Lease.FencingToken, meta.Version)
			}
			queueLogger.Warn("queue.ack.validate_failed", "error", err)
			return err
		}
	}
	corr := doc.CorrelationID
	if corr == "" {
		corr = correlation.ID(ctx)
	}
	if corr == "" {
		corr = correlation.Generate()
	}
	ctx = correlation.Set(ctx, corr)
	if err := qsvc.Ack(ctx, req.Queue, req.MessageID, req.MetaETag, req.StateETag, req.StateLeaseID != ""); err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			queueLogger.Warn("queue.ack.cas_mismatch", "error", err)
			return httpError{Status: http.StatusConflict, Code: "cas_mismatch", Detail: "message metadata changed"}
		}
		if errors.Is(err, storage.ErrNotFound) {
			queueLogger.Warn("queue.ack.not_found", "error", err)
			return httpError{Status: http.StatusNotFound, Code: "not_found", Detail: "message already removed"}
		}
		queueLogger.Error("queue.ack.error", "error", err)
		return fmt.Errorf("queue ack: %w", err)
	}
	if leaseOwner != "" {
		h.clearPendingDelivery(req.Queue, leaseOwner, req.MessageID)
	}
	_ = h.releaseLeaseWithMeta(ctx, messageKey, req.LeaseID, meta, metaETag)

	if req.StateLeaseID != "" {
		stateKey, err := queue.StateLeaseKey(req.Queue, req.MessageID)
		if err == nil {
			stateMeta, stateMetaETag, loadErr := h.ensureMeta(ctx, stateKey)
			if loadErr == nil && stateMeta.Lease != nil {
				if err := validateLease(stateMeta, req.StateLeaseID, req.StateFencingToken, h.clock.Now()); err == nil {
					_ = h.releaseLeaseWithMeta(ctx, stateKey, req.StateLeaseID, stateMeta, stateMetaETag)
				}
			}
		}
	}

	if h.queueDisp != nil {
		queueName := req.Queue
		go h.queueDisp.Notify(queueName)
	}

	resp := api.AckResponse{Acked: true, CorrelationID: corr}
	queueLogger.Debug("queue.ack.success",
		"owner", leaseOwner,
		"correlation", corr,
	)
	headers := map[string]string{}
	if corr != "" {
		headers[headerCorrelationID] = corr
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

// handleQueueNack godoc
// @Summary      Return a message to the queue
// @Description  Requeues the delivery with optional delay and last error metadata.
// @Tags         queue
// @Accept       json
// @Produce      json
// @Param        request  body      api.NackRequest  true  "Negative acknowledgement payload"
// @Success      200      {object}  api.NackResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/nack [post]
func (h *Handler) handleQueueNack(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueAck); err != nil {
		return err
	}
	finish := h.beginQueueAck()
	defer finish()
	qsvc, err := h.requireQueueService()
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.NackRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" || req.MetaETag == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_fields", Detail: "queue, message_id, lease_id, meta_etag are required"}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	queueLogger := logger.With(
		"queue", req.Queue,
		"mid", req.MessageID,
		"lease", req.LeaseID,
	)
	if req.StateLeaseID != "" {
		queueLogger = queueLogger.With("state_lease", req.StateLeaseID)
	}
	if req.DelaySeconds > 0 {
		queueLogger = queueLogger.With("delay_seconds", req.DelaySeconds)
	}
	queueLogger.Debug("queue.nack.begin")
	messageKey, err := queue.MessageLeaseKey(req.Queue, req.MessageID)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_queue_key", Detail: err.Error()}
	}
	meta, metaETag, err := h.ensureMeta(ctx, messageKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return httpError{Status: http.StatusNotFound, Code: "not_found", Detail: "message lease not found"}
		}
		return err
	}
	if err := validateLease(meta, req.LeaseID, req.FencingToken, h.clock.Now()); err != nil {
		queueLogger.Warn("queue.nack.validate_failed", "error", err)
		return err
	}

	doc, _, err := qsvc.GetMessage(ctx, req.Queue, req.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			queueLogger.Warn("queue.extend.message_missing", "error", err)
			return httpError{Status: http.StatusNotFound, Code: "not_found", Detail: "message not found"}
		}
		queueLogger.Error("queue.extend.load_message_error", "error", err)
		return fmt.Errorf("queue load message: %w", err)
	}

	corr := doc.CorrelationID
	if corr == "" {
		corr = correlation.ID(ctx)
	}
	if corr == "" {
		corr = correlation.Generate()
	}
	if doc.CorrelationID != corr {
		doc.CorrelationID = corr
	}
	ctx = correlation.Set(ctx, corr)

	delay := time.Duration(req.DelaySeconds) * time.Second
	newMetaETag, err := qsvc.Nack(ctx, req.Queue, doc, req.MetaETag, delay, req.LastError)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			queueLogger.Warn("queue.nack.cas_mismatch", "error", err)
			return httpError{Status: http.StatusConflict, Code: "cas_mismatch", Detail: "message metadata changed"}
		}
		queueLogger.Error("queue.nack.error", "error", err)
		return fmt.Errorf("queue nack: %w", err)
	}

	_ = h.releaseLeaseWithMeta(ctx, messageKey, req.LeaseID, meta, metaETag)
	if req.StateLeaseID != "" {
		stateKey, err := queue.StateLeaseKey(req.Queue, req.MessageID)
		if err == nil {
			stateMeta, stateMetaETag, loadErr := h.ensureMeta(ctx, stateKey)
			if loadErr == nil && stateMeta.Lease != nil {
				if err := validateLease(stateMeta, req.StateLeaseID, req.StateFencingToken, h.clock.Now()); err == nil {
					_ = h.releaseLeaseWithMeta(ctx, stateKey, req.StateLeaseID, stateMeta, stateMetaETag)
				}
			}
		}
	}

	resp := api.NackResponse{
		Requeued:      true,
		MetaETag:      newMetaETag,
		CorrelationID: corr,
	}
	queueLogger.Debug("queue.nack.success",
		"correlation", corr,
		"requeued_at", doc.NotVisibleUntil.Unix(),
	)
	if h.queueDisp != nil && delay <= 0 {
		queueName := req.Queue
		go h.queueDisp.Notify(queueName)
	}
	headers := map[string]string{}
	if corr != "" {
		headers[headerCorrelationID] = corr
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

// handleQueueExtend godoc
// @Summary      Extend a delivery lease
// @Description  Extends the visibility timeout and lease window for an in-flight message.
// @Tags         queue
// @Accept       json
// @Produce      json
// @Param        request  body      api.ExtendRequest  true  "Extend payload"
// @Success      200      {object}  api.ExtendResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/extend [post]
func (h *Handler) handleQueueExtend(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueAck); err != nil {
		return err
	}
	finish := h.beginQueueAck()
	defer finish()
	qsvc, err := h.requireQueueService()
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.ExtendRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" || req.MetaETag == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_fields", Detail: "queue, message_id, lease_id, meta_etag are required"}
	}

	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	queueLogger := logger.With(
		"queue", req.Queue,
		"mid", req.MessageID,
		"lease", req.LeaseID,
		"extend_by_seconds", req.ExtendBySeconds,
	)
	if req.StateLeaseID != "" {
		queueLogger = queueLogger.With("state_lease", req.StateLeaseID)
	}
	queueLogger.Debug("queue.extend.begin")

	messageKey, err := queue.MessageLeaseKey(req.Queue, req.MessageID)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_queue_key", Detail: err.Error()}
	}
	meta, metaETag, err := h.ensureMeta(ctx, messageKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return httpError{Status: http.StatusNotFound, Code: "not_found", Detail: "message lease not found"}
		}
		return err
	}
	if err := validateLease(meta, req.LeaseID, req.FencingToken, h.clock.Now()); err != nil {
		queueLogger.Warn("queue.extend.validate_failed", "error", err)
		return err
	}

	doc, _, err := qsvc.GetMessage(ctx, req.Queue, req.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return httpError{Status: http.StatusNotFound, Code: "not_found", Detail: "message not found"}
		}
		return fmt.Errorf("queue load message: %w", err)
	}
	corr := doc.CorrelationID
	if corr == "" {
		corr = correlation.ID(ctx)
	}
	if corr == "" {
		corr = correlation.Generate()
	}
	if doc.CorrelationID != corr {
		doc.CorrelationID = corr
	}
	ctx = correlation.Set(ctx, corr)

	extension := time.Duration(req.ExtendBySeconds) * time.Second
	newMetaDocETag, err := qsvc.ExtendVisibility(ctx, req.Queue, doc, req.MetaETag, extension)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			queueLogger.Warn("queue.extend.cas_mismatch", "error", err)
			return httpError{Status: http.StatusConflict, Code: "cas_mismatch", Detail: "message metadata changed"}
		}
		queueLogger.Error("queue.extend.error", "error", err)
		return fmt.Errorf("queue extend: %w", err)
	}

	now := h.clock.Now()
	if meta.Lease == nil {
		return httpError{Status: http.StatusForbidden, Code: "lease_required", Detail: "lease missing"}
	}
	meta.Lease.ExpiresAtUnix = now.Add(extension).Unix()
	meta.UpdatedAtUnix = now.Unix()
	newMetaLeaseETag, err := h.store.StoreMeta(ctx, messageKey, meta, metaETag)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			queueLogger.Warn("queue.extend.lease_conflict", "error", err)
			return httpError{Status: http.StatusConflict, Code: "lease_conflict", Detail: "lease changed during extend"}
		}
		queueLogger.Error("queue.extend.store_meta_error", "error", err)
		return fmt.Errorf("queue extend lease: %w", err)
	}
	h.cacheLease(req.LeaseID, messageKey, *meta, newMetaLeaseETag)

	stateLeaseExpires := int64(0)
	if req.StateLeaseID != "" {
		stateKey, err := queue.StateLeaseKey(req.Queue, req.MessageID)
		if err != nil {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_queue_state_key", Detail: err.Error()}
		}
		stateMeta, stateMetaETag, err := h.ensureMeta(ctx, stateKey)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return httpError{Status: http.StatusNotFound, Code: "state_not_found", Detail: "state lease missing"}
			}
			return err
		}
		if err := validateLease(stateMeta, req.StateLeaseID, req.StateFencingToken, now); err != nil {
			queueLogger.Warn("queue.extend.state_validate_failed", "error", err)
			return err
		}
		if stateMeta.Lease == nil {
			return httpError{Status: http.StatusForbidden, Code: "lease_required", Detail: "state lease missing"}
		}
		stateMeta.Lease.ExpiresAtUnix = now.Add(extension).Unix()
		stateMeta.UpdatedAtUnix = now.Unix()
		newStateETag, err := h.store.StoreMeta(ctx, stateKey, stateMeta, stateMetaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				queueLogger.Warn("queue.extend.state_conflict", "error", err)
				return httpError{Status: http.StatusConflict, Code: "lease_conflict", Detail: "state lease changed during extend"}
			}
			queueLogger.Error("queue.extend.state_store_error", "error", err)
			return fmt.Errorf("queue extend state lease: %w", err)
		}
		h.cacheLease(req.StateLeaseID, stateKey, *stateMeta, newStateETag)
		stateLeaseExpires = stateMeta.Lease.ExpiresAtUnix
	}

	resp := api.ExtendResponse{
		LeaseExpiresAtUnix:       meta.Lease.ExpiresAtUnix,
		VisibilityTimeoutSeconds: doc.VisibilityTimeout,
		MetaETag:                 newMetaDocETag,
		StateLeaseExpiresAtUnix:  stateLeaseExpires,
		CorrelationID:            corr,
	}
	queueLogger.Debug("queue.extend.success",
		"lease_expires_at", resp.LeaseExpiresAtUnix,
		"state_lease_expires_at", resp.StateLeaseExpiresAtUnix,
		"correlation", corr,
	)
	headers := map[string]string{}
	if corr != "" {
		headers[headerCorrelationID] = corr
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

// handleHealth godoc
// @Summary      Liveness probe
// @Tags         system
// @Produce      plain
// @Success      200  {string}  string  "OK"
// @Router       /healthz [get]
func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) error {
	w.WriteHeader(http.StatusOK)
	return nil
}

// handleReady godoc
// @Summary      Readiness probe
// @Tags         system
// @Produce      plain
// @Success      200  {string}  string  "Ready"
// @Router       /readyz [get]
func (h *Handler) handleReady(w http.ResponseWriter, _ *http.Request) error {
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Handler) handleError(ctx context.Context, w http.ResponseWriter, err error) {
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	var httpErr httpError
	if errors.As(err, &httpErr) {
		verbose.Debug("http.request.failure",
			"status", httpErr.Status,
			"code", httpErr.Code,
			"detail", httpErr.Detail,
			"version", httpErr.Version,
			"etag", httpErr.ETag,
			"retry_after", httpErr.RetryAfter,
		)
		resp := api.ErrorResponse{
			ErrorCode:         httpErr.Code,
			Detail:            httpErr.Detail,
			CurrentVersion:    httpErr.Version,
			CurrentETag:       httpErr.ETag,
			RetryAfterSeconds: httpErr.RetryAfter,
		}
		headers := map[string]string{}
		if httpErr.RetryAfter > 0 {
			headers["Retry-After"] = strconv.FormatInt(httpErr.RetryAfter, 10)
		}
		if httpErr.Status == http.StatusTooManyRequests && h.qrf != nil {
			headers["X-Lockd-QRF-State"] = h.qrf.State().String()
		}
		h.writeJSON(w, httpErr.Status, resp, headers)
		return
	}
	logger.Error("http.request.panic", "error", err)
	resp := api.ErrorResponse{
		ErrorCode: "internal_error",
		Detail:    "internal server error",
	}
	h.writeJSON(w, http.StatusInternalServerError, resp, nil)
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

func (h *Handler) ensureMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger

	attemptLimit := h.metaWarmupAttempts
	delay := h.metaWarmupInitial
	maxDelay := h.metaWarmupMax
	attempts := 0
	observed := h.isKeyObserved(key)
	if !observed && strings.HasPrefix(key, "q/") {
		attemptLimit = 0
	}
	for {
		meta, etag, err := h.store.LoadMeta(ctx, key)
		if err == nil {
			h.markKeyObserved(key)
			return meta, etag, nil
		}
		if errors.Is(err, storage.ErrNotFound) {
			if attemptLimit <= 0 || attempts >= attemptLimit {
				if attempts > 0 {
					verbose.Trace("storage.load_meta.giveup_not_found", "key", key, "attempts", attempts, "observed", observed)
				}
				return &storage.Meta{}, "", nil
			}
			attempts++
			if delay > 0 {
				verbose.Trace("storage.load_meta.retry_not_found", "key", key, "attempt", attempts, "delay", delay, "observed", observed)
				if waitErr := h.waitWithContext(ctx, delay); waitErr != nil {
					return nil, "", waitErr
				}
				delay = nextWarmupDelay(delay, maxDelay)
			} else {
				verbose.Trace("storage.load_meta.retry_not_found", "key", key, "attempt", attempts, "observed", observed)
			}
			continue
		}
		return nil, "", fmt.Errorf("load meta: %w", err)
	}
}

func (h *Handler) readStateWithWarmup(ctx context.Context, key string, expectState bool, verbose pslog.Logger) (io.ReadCloser, *storage.StateInfo, error) {
	reader, info, err := h.store.ReadState(ctx, key)
	if err == nil || !expectState || !errors.Is(err, storage.ErrNotFound) {
		return reader, info, err
	}

	attemptLimit := h.stateWarmupAttempts
	delay := h.stateWarmupInitial
	maxDelay := h.stateWarmupMax
	for attempt := 1; attempt <= attemptLimit; attempt++ {
		if delay > 0 {
			verbose.Trace("storage.read_state.retry_not_found", "key", key, "attempt", attempt, "delay", delay)
			if waitErr := h.waitWithContext(ctx, delay); waitErr != nil {
				return nil, nil, waitErr
			}
			delay = nextWarmupDelay(delay, maxDelay)
		} else {
			verbose.Trace("storage.read_state.retry_not_found", "key", key, "attempt", attempt)
		}
		reader, info, err = h.store.ReadState(ctx, key)
		if err == nil {
			return reader, info, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, nil, err
		}
	}
	return nil, nil, storage.ErrNotFound
}

func (h *Handler) waitWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := h.clock.After(delay)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer:
		return nil
	}
}

func nextWarmupDelay(current, max time.Duration) time.Duration {
	if current <= 0 {
		return 0
	}
	next := current * 2
	if max > 0 && next > max {
		next = max
	}
	return next
}

func (h *Handler) acquireLeaseForKey(ctx context.Context, logger pslog.Logger, params acquireParams) (*acquireOutcome, error) {
	if params.Key == "" {
		return nil, httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key required"}
	}
	if params.Owner == "" {
		return nil, httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner required"}
	}
	if params.TTL <= 0 {
		params.TTL = h.defaultTTL
	}
	verbose := logger
	beginFields := []any{
		"key", params.Key,
		"owner", params.Owner,
		"ttl_seconds", params.TTL.Seconds(),
		"block_seconds", params.Block.Seconds(),
		"wait_forever", params.WaitForever,
	}
	if params.CorrelationID != "" {
		beginFields = append(beginFields, "cid", params.CorrelationID)
	}
	verbose.Debug("acquire.begin", beginFields...)

	var deadline time.Time
	if !params.WaitForever && params.Block > 0 {
		deadline = h.clock.Now().Add(params.Block)
	}
	leaseID := uuidv7.NewString()
	backoff := newAcquireBackoff()

	for {
		now := h.clock.Now()
		meta, metaETag, err := h.ensureMeta(ctx, params.Key)
		if err != nil {
			return nil, err
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			meta.Lease = nil
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix > now.Unix() {
			if params.Block > 0 && (params.WaitForever || (!deadline.IsZero() && now.Before(deadline))) {
				leaseExpiry := time.Unix(meta.Lease.ExpiresAtUnix, 0)
				limit := leaseExpiry.Sub(now)
				if limit <= 0 {
					limit = acquireBackoffStart
				}
				if !params.WaitForever && !deadline.IsZero() {
					remaining := deadline.Sub(now)
					if remaining > 0 && (limit <= 0 || remaining < limit) {
						limit = remaining
					}
				}
				sleep := backoff.Next(limit)
				waitFields := []any{"key", params.Key, "sleep", sleep}
				if params.CorrelationID != "" {
					waitFields = append(waitFields, "cid", params.CorrelationID)
				}
				verbose.Trace("acquire.wait_loop", waitFields...)
				h.clock.Sleep(sleep)
				continue
			}
			retryDur := max(time.Until(time.Unix(meta.Lease.ExpiresAtUnix, 0)), 0)
			retry := max(int64(math.Ceil(retryDur.Seconds())), 1)
			if params.CorrelationID != "" {
				verbose.Trace("acquire.wait_conflict", "key", params.Key, "cid", params.CorrelationID, "retry_after", retry)
			}
			return nil, httpError{
				Status:     http.StatusConflict,
				Code:       "waiting",
				Detail:     "lease already held",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				RetryAfter: retry,
			}
		}

		expiresAt := now.Add(params.TTL).Unix()
		newFencing := meta.FencingToken + 1
		meta.FencingToken = newFencing
		meta.Lease = &storage.Lease{
			ID:            leaseID,
			Owner:         params.Owner,
			ExpiresAtUnix: expiresAt,
			FencingToken:  newFencing,
		}
		meta.UpdatedAtUnix = now.Unix()

		var creationMu *sync.Mutex
		if metaETag == "" {
			creationMu = h.creationMutex(params.Key)
			creationMu.Lock()
		}
		newMetaETag, err := h.store.StoreMeta(ctx, params.Key, meta, metaETag)
		if creationMu != nil {
			creationMu.Unlock()
		}
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				fields := []any{"key", params.Key}
				if params.CorrelationID != "" {
					fields = append(fields, "cid", params.CorrelationID)
				}
				verbose.Trace("acquire.meta_conflict", fields...)
				continue
			}
			return nil, fmt.Errorf("store meta: %w", err)
		}

		metaCopy := *meta
		h.cacheLease(leaseID, params.Key, metaCopy, newMetaETag)
		successFields := []any{
			"key", params.Key,
			"lease_id", leaseID,
			"expires_at", expiresAt,
			"fencing", newFencing,
		}
		if params.CorrelationID != "" {
			successFields = append(successFields, "cid", params.CorrelationID)
		}
		verbose.Debug("acquire.success", successFields...)
		if debugQueueTiming && strings.HasPrefix(params.Key, "q/") {
			fmt.Fprintf(os.Stderr, "[%s] queue.acquire.success key=%s lease=%s fencing=%d expires=%d\n", time.Now().Format(time.RFC3339Nano), params.Key, leaseID, newFencing, expiresAt)
		}

		resp := api.AcquireResponse{
			LeaseID:       leaseID,
			Key:           params.Key,
			Owner:         params.Owner,
			ExpiresAt:     expiresAt,
			Version:       meta.Version,
			StateETag:     meta.StateETag,
			FencingToken:  newFencing,
			CorrelationID: params.CorrelationID,
		}
		return &acquireOutcome{
			Response: resp,
			Meta:     metaCopy,
			MetaETag: newMetaETag,
		}, nil
	}
}

func (h *Handler) releaseLeaseOutcome(ctx context.Context, key string, outcome *acquireOutcome) error {
	if outcome == nil {
		return nil
	}
	metaCopy := outcome.Meta
	metaCopy.Lease = nil
	metaCopy.UpdatedAtUnix = h.clock.Now().Unix()
	_, err := h.store.StoreMeta(ctx, key, &metaCopy, outcome.MetaETag)
	if err == nil || errors.Is(err, storage.ErrNotFound) {
		h.dropLease(outcome.Response.LeaseID)
	}
	return err
}

func (h *Handler) releaseLeaseWithMeta(ctx context.Context, key, leaseID string, meta *storage.Meta, metaETag string) error {
	if meta == nil {
		return nil
	}
	metaCopy := *meta
	metaCopy.Lease = nil
	metaCopy.UpdatedAtUnix = h.clock.Now().Unix()
	_, err := h.store.StoreMeta(ctx, key, &metaCopy, metaETag)
	if err == nil || errors.Is(err, storage.ErrNotFound) {
		h.dropLease(leaseID)
	}
	return err
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

type queueDelivery struct {
	handler    *Handler
	qsvc       *queue.Service
	queueName  string
	owner      string
	visibility time.Duration
	stateful   bool
	logger     pslog.Logger
	candidate  *queue.Candidate
	descriptor queue.MessageDescriptor

	message            *api.Message
	payload            io.ReadCloser
	payloadContentType string
	payloadBytes       int64
	nextCursor         string

	materializeOnce sync.Once
	materializeErr  error

	finalizeMu sync.Mutex
	finalize   func(success bool)
}

func (h *Handler) consumeQueue(ctx context.Context, logger pslog.Logger, qsvc *queue.Service, queueName, owner string, visibility time.Duration, stateful bool, blockSeconds int64) (delivery *queueDelivery, nextCursor string, err error) {
	if h.queueDisp == nil {
		err = httpError{Status: http.StatusNotImplemented, Code: "queue_disabled", Detail: "queue dispatcher not configured"}
		return
	}
	type consumeTiming struct {
		wait    time.Duration
		try     time.Duration
		prepare time.Duration
	}
	var timing consumeTiming
	mode := "wait"
	if blockSeconds == api.BlockNoWait {
		mode = "try"
	}
	start := time.Now()
	attempts := 0
	retries := 0
	defer func() {
		if logger == nil {
			return
		}
		status := "ok"
		switch {
		case err == nil && delivery == nil:
			status = "none"
		case errors.Is(err, errQueueEmpty):
			status = "empty"
		case err != nil:
			status = "error"
		}
		logger.Trace("queue.consume.summary",
			"mode", mode,
			"attempts", attempts,
			"retries", retries,
			"duration", time.Since(start),
			"status", status,
		)
	}()
	logTiming := debugQueueTiming && logger != nil
	emitTiming := func(status string) {
		if !logTiming {
			return
		}
		fields := []any{
			"mode", mode,
			"status", status,
			"attempts", attempts,
			"retries", retries,
		}
		if timing.wait > 0 {
			fields = append(fields, "wait_elapsed", timing.wait)
		}
		if timing.try > 0 {
			fields = append(fields, "try_elapsed", timing.try)
		}
		if timing.prepare > 0 {
			fields = append(fields, "prepare_elapsed", timing.prepare)
		}
		logger.Trace("queue.consume.timing", fields...)
	}
	if blockSeconds == api.BlockNoWait {
		const maxImmediateAttempts = 5
		for range maxImmediateAttempts {
			attempts++
			tryStart := time.Now()
			cand, tryErr := h.queueDisp.Try(ctx, queueName)
			timing.try += time.Since(tryStart)
			if tryErr != nil {
				if errors.Is(tryErr, queue.ErrTooManyConsumers) {
					err = httpError{Status: http.StatusServiceUnavailable, Code: "queue_busy", Detail: "too many consumers"}
					emitTiming("queue_busy")
					return
				}
				err = tryErr
				emitTiming("error")
				return
			}
			if cand == nil {
				err = errQueueEmpty
				emitTiming("empty")
				return
			}
			var retry bool
			prepStart := time.Now()
			delivery, retry, err = h.prepareQueueDelivery(ctx, logger, qsvc, queueName, owner, visibility, stateful, cand)
			timing.prepare += time.Since(prepStart)
			if retry {
				retries++
				h.queueDisp.Notify(queueName)
				continue
			}
			if err != nil {
				emitTiming("error")
				return
			}
			if delivery == nil {
				err = fmt.Errorf("queue delivery missing for %s", queueName)
				emitTiming("error")
				return
			}
			nextCursor = cand.NextCursor
			delivery.nextCursor = nextCursor
			emitTiming("ok")
			return
		}
		err = errQueueEmpty
		emitTiming("empty")
		return
	}
	for {
		attempts++
		waitStart := time.Now()
		cand, waitErr := h.queueDisp.Wait(ctx, queueName)
		timing.wait += time.Since(waitStart)
		if waitErr != nil {
			if errors.Is(waitErr, queue.ErrTooManyConsumers) {
				err = httpError{Status: http.StatusServiceUnavailable, Code: "queue_busy", Detail: "too many consumers"}
				emitTiming("queue_busy")
				return
			}
			err = waitErr
			emitTiming("error")
			return
		}
		var retry bool
		prepStart := time.Now()
		delivery, retry, err = h.prepareQueueDelivery(ctx, logger, qsvc, queueName, owner, visibility, stateful, cand)
		timing.prepare += time.Since(prepStart)
		if retry {
			retries++
			h.queueDisp.Notify(queueName)
			continue
		}
		if err != nil {
			emitTiming("error")
			return
		}
		if delivery == nil {
			err = fmt.Errorf("queue delivery missing for %s", queueName)
			emitTiming("error")
			return
		}
		nextCursor = cand.NextCursor
		emitTiming("ok")
		return
	}
}

func (h *Handler) queueHasActiveWatcher(queue string) bool {
	if h.queueDisp == nil {
		return false
	}
	return h.queueDisp.HasActiveWatcher(queue)
}

func (h *Handler) queueBatchFillConfig(queue string, hasWatcher bool, blockSeconds int64, pageSize int) (time.Duration, time.Duration) {
	if pageSize <= 1 {
		return 0, 0
	}
	if blockSeconds == api.BlockNoWait {
		return 0, 0
	}
	if pageSize < 1 {
		pageSize = 1
	}
	const (
		watcherStep = 5 * time.Millisecond
		watcherCap  = 50 * time.Millisecond
		pollStep    = 50 * time.Millisecond
		pollCap     = 250 * time.Millisecond
	)
	var budget, step time.Duration
	if hasWatcher {
		step = watcherStep
		budget = min(watcherStep*time.Duration(minInt(pageSize, 8)), watcherCap)
	} else {
		step = pollStep
		budget = min(pollStep*time.Duration(pageSize), pollCap)
	}
	if blockSeconds > 0 {
		requested := time.Duration(blockSeconds) * time.Second
		if budget == 0 || requested < budget {
			budget = requested
		}
	} else if blockSeconds == 0 {
		if budget == 0 {
			budget = step
		}
	}
	if budget < step {
		step = budget
	}
	if budget <= 0 {
		return 0, 0
	}
	if step <= 0 {
		step = budget
	}
	return budget, step
}

func (h *Handler) rescheduleAfterPrepareRetry(queueLogger pslog.Logger, qsvc *queue.Service, queueName string, doc *queue.MessageDescriptor, metaETag string) {
	if doc == nil || metaETag == "" {
		return
	}
	docCopy := doc.Document
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := qsvc.Reschedule(ctx, queueName, &docCopy, metaETag, 0); err != nil {
		if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
			if h.queueDisp != nil {
				h.queueDisp.Notify(queueName)
			}
			return
		}
		if debugQueueTiming && queueLogger != nil {
			queueLogger.Trace("queue.prepare_delivery.reschedule_failed",
				"queue", queueName,
				"mid", docCopy.ID,
				"error", err,
			)
		}
		return
	}
	if h.queueDisp != nil {
		h.queueDisp.Notify(queueName)
	}
}

func (h *Handler) consumeQueueBatch(ctx context.Context, logger pslog.Logger, qsvc *queue.Service, queueName, owner string, visibility time.Duration, stateful bool, blockSeconds int64, pageSize int) ([]*queueDelivery, string, error) {
	if pageSize <= 1 {
		delivery, nextCursor, err := h.consumeQueue(ctx, logger, qsvc, queueName, owner, visibility, stateful, blockSeconds)
		if err != nil {
			return nil, "", err
		}
		if delivery == nil {
			return nil, "", errQueueEmpty
		}
		return []*queueDelivery{delivery}, nextCursor, nil
	}

	if pageSize > 1 {
		pageSize = 1
	}

	hasWatcher := h.queueHasActiveWatcher(queueName)
	fillBudget, retryInterval := h.queueBatchFillConfig(queueName, hasWatcher, blockSeconds, pageSize)
	var fillDeadline time.Time
	if fillBudget > 0 {
		fillDeadline = time.Now().Add(fillBudget)
	}

	deliveries := make([]*queueDelivery, 0, pageSize)
	var nextCursor string
	currentBlock := blockSeconds
	start := time.Now()
	var (
		tryAttempts   int
		waitAttempts  int
		emptyFastPath int
	)

	for len(deliveries) < pageSize {
		delivery, cursor, err := h.consumeQueue(ctx, logger, qsvc, queueName, owner, visibility, stateful, currentBlock)
		if err != nil {
			if errors.Is(err, errQueueEmpty) {
				emptyFastPath++
				if len(deliveries) == 0 {
					return nil, "", err
				}
				if fillDeadline.IsZero() || time.Now().After(fillDeadline) || retryInterval <= 0 {
					break
				}
				waitAttempts++
				wait := time.Until(fillDeadline)
				if wait <= 0 {
					break
				}
				if retryInterval > 0 && wait > retryInterval {
					wait = retryInterval
				}
				waitCtx, cancel := context.WithTimeout(ctx, wait)
				waitDelivery, waitCursor, waitErr := h.consumeQueue(waitCtx, logger, qsvc, queueName, owner, visibility, stateful, 1)
				cancel()
				if waitErr != nil {
					if errors.Is(waitErr, errQueueEmpty) || errors.Is(waitErr, context.DeadlineExceeded) || errors.Is(waitErr, context.Canceled) {
						continue
					}
					return nil, "", waitErr
				}
				if waitDelivery == nil {
					continue
				}
				delivery = waitDelivery
				cursor = waitCursor
			} else {
				return nil, "", err
			}
		}

		if delivery == nil {
			if len(deliveries) == 0 {
				return nil, "", errQueueEmpty
			}
			break
		}
		deliveries = append(deliveries, delivery)
		nextCursor = cursor
		currentBlock = api.BlockNoWait
		tryAttempts++
	}
	if len(deliveries) == 0 {
		return nil, "", errQueueEmpty
	}
	if logger != nil {
		logger.Trace("queue.consume_batch.summary",
			"page_size", pageSize,
			"deliveries", len(deliveries),
			"try_attempts", tryAttempts,
			"wait_attempts", waitAttempts,
			"empty_fast_path", emptyFastPath,
			"fill_budget", fillBudget,
			"duration", time.Since(start),
		)
	}
	return deliveries, nextCursor, nil
}

func (h *Handler) prepareQueueDelivery(ctx context.Context, logger pslog.Logger, qsvc *queue.Service, queueName, owner string, visibility time.Duration, stateful bool, cand *queue.Candidate) (*queueDelivery, bool, error) {
	delivery := &queueDelivery{
		handler:    h,
		qsvc:       qsvc,
		queueName:  queueName,
		owner:      owner,
		visibility: visibility,
		stateful:   stateful,
		logger:     logger,
		candidate:  cand,
		descriptor: cand.Descriptor,
		nextCursor: cand.NextCursor,
	}
	return delivery, false, nil
}

func (d *queueDelivery) ensure(ctx context.Context) error {
	d.materializeOnce.Do(func() {
		d.materializeErr = d.materialize(ctx)
	})
	return d.materializeErr
}

func (d *queueDelivery) materialize(ctx context.Context) error {
	if d == nil {
		return nil
	}
	h := d.handler
	qsvc := d.qsvc
	if h == nil || qsvc == nil {
		return fmt.Errorf("queue delivery not initialised")
	}
	logger := d.logger
	desc := d.descriptor
	doc := desc.Document

	messageKey, err := queue.MessageLeaseKey(d.queueName, doc.ID)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_queue_key", Detail: err.Error()}
	}

	ttl := d.visibility
	if ttl <= 0 {
		ttl = time.Duration(doc.VisibilityTimeout) * time.Second
		if ttl <= 0 {
			ttl = h.defaultTTL
		}
	}

	corr := strings.TrimSpace(doc.CorrelationID)
	if corr != "" {
		if normalized, ok := correlation.Normalize(corr); ok {
			corr = normalized
			if doc.CorrelationID != corr {
				doc.CorrelationID = corr
			}
		} else {
			corr = correlation.Generate()
			doc.CorrelationID = corr
		}
	}
	if corr == "" {
		corr = correlation.ID(ctx)
		if corr == "" {
			corr = correlation.Generate()
		}
		doc.CorrelationID = corr
	}
	ctx = correlation.Set(ctx, corr)
	if logger != nil {
		logger = logger.With("cid", corr)
	}

	type prepareTiming struct {
		acquire      time.Duration
		increment    time.Duration
		getPayload   time.Duration
		stateEnsure  time.Duration
		stateAcquire time.Duration
		stateLoad    time.Duration
	}
	var timing prepareTiming
	logPrep := debugQueueTiming && logger != nil
	emitPrepare := func(status string) {
		if !logPrep {
			return
		}
		fields := []any{
			"queue.prepare_delivery.timing",
			"queue", d.queueName,
			"mid", doc.ID,
			"status", status,
		}
		if timing.acquire > 0 {
			fields = append(fields, "acquire_elapsed", timing.acquire)
		}
		if timing.increment > 0 {
			fields = append(fields, "increment_elapsed", timing.increment)
		}
		if timing.getPayload > 0 {
			fields = append(fields, "payload_elapsed", timing.getPayload)
		}
		if timing.stateEnsure > 0 {
			fields = append(fields, "state_ensure_elapsed", timing.stateEnsure)
		}
		if timing.stateAcquire > 0 {
			fields = append(fields, "state_acquire_elapsed", timing.stateAcquire)
		}
		if timing.stateLoad > 0 {
			fields = append(fields, "state_load_elapsed", timing.stateLoad)
		}
		logger.Trace(fields[0].(string), fields[1:]...)
	}

	acquireStart := time.Now()
	acq, err := h.acquireLeaseForKey(ctx, logger.With("queue_msg", doc.ID), acquireParams{
		Key:           messageKey,
		Owner:         d.owner,
		TTL:           ttl,
		Block:         0,
		WaitForever:   false,
		CorrelationID: corr,
	})
	timing.acquire += time.Since(acquireStart)
	if err != nil {
		if httpErr, ok := err.(httpError); ok && httpErr.Code == "waiting" {
			emitPrepare("retry")
			if debugQueueTiming {
				fmt.Fprintf(os.Stderr, "[%s] queue.prepare.retry.acquire queue=%s mid=%s\n",
					time.Now().Format(time.RFC3339Nano), d.queueName, doc.ID)
			}
			return errDeliveryRetry
		}
		emitPrepare("error")
		return err
	}
	releaseMessage := func() {
		_ = h.releaseLeaseOutcome(ctx, messageKey, acq)
	}

	incrementStart := time.Now()
	newMetaETag, err := qsvc.IncrementAttempts(ctx, d.queueName, &doc, desc.MetadataETag, ttl)
	timing.increment += time.Since(incrementStart)
	if err != nil {
		releaseMessage()
		if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
			emitPrepare("retry")
			if debugQueueTiming {
				fmt.Fprintf(os.Stderr, "[%s] queue.prepare.retry.increment queue=%s mid=%s\n",
					time.Now().Format(time.RFC3339Nano), d.queueName, doc.ID)
			}
			return errDeliveryRetry
		}
		emitPrepare("error")
		return fmt.Errorf("queue increment attempts: %w", err)
	}

	payloadStart := time.Now()
	payloadCtx := storage.ContextWithObjectPlaintextSize(ctx, doc.PayloadBytes)
	if len(doc.PayloadDescriptor) > 0 {
		payloadCtx = storage.ContextWithObjectDescriptor(payloadCtx, doc.PayloadDescriptor)
	}
	reader, info, err := qsvc.GetPayload(payloadCtx, d.queueName, doc.ID)
	timing.getPayload += time.Since(payloadStart)
	if err != nil {
		releaseMessage()
		if errors.Is(err, storage.ErrNotFound) {
			emitPrepare("retry")
			h.rescheduleAfterPrepareRetry(logger, qsvc, d.queueName, &desc, newMetaETag)
			if debugQueueTiming {
				fmt.Fprintf(os.Stderr, "[%s] queue.prepare.retry.payload queue=%s mid=%s\n",
					time.Now().Format(time.RFC3339Nano), d.queueName, doc.ID)
			}
			return errDeliveryRetry
		}
		emitPrepare("error")
		return fmt.Errorf("queue payload: %w", err)
	}

	var payloadSize int64 = doc.PayloadBytes
	if info != nil && info.Size >= 0 {
		payloadSize = info.Size
	}
	if payloadSize < 0 {
		payloadSize = 0
	}
	contentType := doc.PayloadContentType
	if info != nil && info.ContentType != "" {
		contentType = info.ContentType
	}

	message := &api.Message{
		Queue:                    d.queueName,
		MessageID:                doc.ID,
		Attempts:                 doc.Attempts,
		MaxAttempts:              doc.MaxAttempts,
		NotVisibleUntilUnix:      doc.NotVisibleUntil.Unix(),
		VisibilityTimeoutSeconds: doc.VisibilityTimeout,
		Attributes:               doc.Attributes,
		PayloadContentType:       contentType,
		PayloadBytes:             payloadSize,
		CorrelationID:            corr,
		LeaseID:                  acq.Response.LeaseID,
		LeaseExpiresAtUnix:       acq.Response.ExpiresAt,
		FencingToken:             acq.Response.FencingToken,
		MetaETag:                 newMetaETag,
	}

	var stateOutcome *acquireOutcome
	var stateKey string
	var releaseState func()
	if d.stateful {
		stateEnsureStart := time.Now()
		stateETag, err := qsvc.EnsureStateExists(ctx, d.queueName, doc.ID)
		timing.stateEnsure += time.Since(stateEnsureStart)
		if err != nil {
			reader.Close()
			releaseMessage()
			if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
				emitPrepare("retry")
				if debugQueueTiming {
					fmt.Fprintf(os.Stderr, "[%s] queue.prepare.retry.ensure_state queue=%s mid=%s\n",
						time.Now().Format(time.RFC3339Nano), d.queueName, doc.ID)
				}
				return errDeliveryRetry
			}
			emitPrepare("error")
			return fmt.Errorf("queue ensure state: %w", err)
		}
		stateKey, err = queue.StateLeaseKey(d.queueName, doc.ID)
		if err != nil {
			reader.Close()
			releaseMessage()
			emitPrepare("error")
			return httpError{Status: http.StatusBadRequest, Code: "invalid_queue_state_key", Detail: err.Error()}
		}
		stateAcquireStart := time.Now()
		stateOutcome, err = h.acquireLeaseForKey(ctx, logger.With("queue_state", doc.ID), acquireParams{
			Key:           stateKey,
			Owner:         d.owner,
			TTL:           ttl,
			Block:         0,
			WaitForever:   false,
			CorrelationID: corr,
		})
		timing.stateAcquire += time.Since(stateAcquireStart)
		if err != nil {
			reader.Close()
			releaseMessage()
			if httpErr, ok := err.(httpError); ok && httpErr.Code == "waiting" {
				emitPrepare("retry")
				h.rescheduleAfterPrepareRetry(logger, qsvc, d.queueName, &desc, newMetaETag)
				if debugQueueTiming {
					fmt.Fprintf(os.Stderr, "[%s] queue.prepare.retry.state queue=%s mid=%s\n",
						time.Now().Format(time.RFC3339Nano), d.queueName, doc.ID)
				}
				return errDeliveryRetry
			}
			emitPrepare("error")
			return err
		}
		releaseState = func() {
			_ = h.releaseLeaseOutcome(ctx, stateKey, stateOutcome)
		}
		stateLoadStart := time.Now()
		_, stateDocETag, err := qsvc.LoadState(ctx, d.queueName, doc.ID)
		timing.stateLoad += time.Since(stateLoadStart)
		if err != nil {
			releaseState()
			reader.Close()
			releaseMessage()
			if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
				emitPrepare("retry")
				if debugQueueTiming {
					fmt.Fprintf(os.Stderr, "[%s] queue.prepare.retry.load_state queue=%s mid=%s\n",
						time.Now().Format(time.RFC3339Nano), d.queueName, doc.ID)
				}
				return errDeliveryRetry
			}
			emitPrepare("error")
			return fmt.Errorf("queue load state: %w", err)
		}
		if stateDocETag != "" {
			stateETag = stateDocETag
		}
		message.StateLeaseID = stateOutcome.Response.LeaseID
		message.StateLeaseExpiresAtUnix = stateOutcome.Response.ExpiresAt
		message.StateFencingToken = stateOutcome.Response.FencingToken
		message.StateETag = stateETag
	}

	docForReschedule := doc
	d.message = message
	d.payload = reader
	d.payloadContentType = contentType
	d.payloadBytes = payloadSize

	d.finalizeMu.Lock()
	d.finalize = func(success bool) {
		if d.payload != nil {
			d.payload.Close()
		}
		if !success {
			ctx := context.Background()
			if newMetaETag != "" {
				if _, err := qsvc.Reschedule(ctx, d.queueName, &docForReschedule, newMetaETag, 0); err != nil {
					if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
						if h.queueDisp != nil {
							h.queueDisp.Notify(d.queueName)
						}
						goto release
					}
					if debugQueueTiming {
						fmt.Fprintf(os.Stderr, "[%s] queue.delivery.reschedule_failed queue=%s mid=%s error=%v\n",
							time.Now().Format(time.RFC3339Nano), d.queueName, doc.ID, err)
					}
				} else if h.queueDisp != nil {
					h.queueDisp.Notify(d.queueName)
				}
			}
		release:
			releaseMessage()
			if releaseState != nil {
				releaseState()
			}
			return
		}
	}
	d.finalizeMu.Unlock()

	emitPrepare("ok")
	if debugQueueTiming {
		fmt.Fprintf(os.Stderr, "[%s] queue.prepare.success queue=%s mid=%s lease=%s\n",
			time.Now().Format(time.RFC3339Nano), d.queueName, doc.ID, message.LeaseID)
	}
	return nil
}

func (d *queueDelivery) abort() {
	d.finalizeMu.Lock()
	finalize := d.finalize
	d.finalizeMu.Unlock()
	if finalize != nil {
		finalize(false)
		return
	}
	if d.handler != nil && d.handler.queueDisp != nil {
		d.handler.queueDisp.Notify(d.queueName)
	}
}

func (d *queueDelivery) complete() {
	d.finalizeMu.Lock()
	finalize := d.finalize
	d.finalizeMu.Unlock()
	if finalize != nil {
		finalize(true)
	}
}

func writeQueueDeliveryBatch(w http.ResponseWriter, deliveries []*queueDelivery, defaultCursor string) error {
	mw := multipart.NewWriter(w)
	firstCID := ""
	if len(deliveries) > 0 && deliveries[0] != nil && deliveries[0].message != nil {
		firstCID = deliveries[0].message.CorrelationID
	}
	contentType := fmt.Sprintf("multipart/related; boundary=%s", mw.Boundary())
	w.Header().Set("Content-Type", contentType)
	if firstCID != "" {
		w.Header().Set(headerCorrelationID, firstCID)
	}
	w.WriteHeader(http.StatusOK)

	if _, err := writeQueueDeliveriesToWriter(mw, deliveries, defaultCursor); err != nil {
		_ = mw.Close()
		return err
	}
	return mw.Close()
}

func writeQueueDeliveriesToWriter(mw *multipart.Writer, deliveries []*queueDelivery, defaultCursor string) (string, error) {
	total := len(deliveries)
	var firstCID string
	for i, delivery := range deliveries {
		if delivery == nil {
			continue
		}
		if err := delivery.ensure(context.Background()); err != nil {
			return "", err
		}
		metaHeader := textproto.MIMEHeader{}
		metaHeader.Set("Content-Type", "application/json")
		if total > 1 {
			metaHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="meta"; filename="meta-%d.json"`, i))
		} else {
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
		}
		metaPart, err := mw.CreatePart(metaHeader)
		if err != nil {
			return "", err
		}
		cursor := delivery.nextCursor
		if cursor == "" {
			cursor = defaultCursor
		}
		meta := api.DequeueResponse{Message: delivery.message, NextCursor: cursor}
		if err := json.NewEncoder(metaPart).Encode(meta); err != nil {
			return "", err
		}
		if delivery.message != nil && firstCID == "" {
			firstCID = delivery.message.CorrelationID
		}

		if delivery.payload != nil {
			payloadHeader := textproto.MIMEHeader{}
			ctype := strings.TrimSpace(delivery.payloadContentType)
			if ctype == "" {
				ctype = "application/octet-stream"
			}
			payloadHeader.Set("Content-Type", ctype)
			if delivery.payloadBytes >= 0 {
				payloadHeader.Set("Content-Length", strconv.FormatInt(delivery.payloadBytes, 10))
			}
			if total > 1 {
				payloadHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="payload"; filename="payload-%d"`, i))
			} else {
				payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			}
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				return "", err
			}
			if _, err := io.Copy(payloadPart, delivery.payload); err != nil {
				return "", err
			}
		}
	}
	return firstCID, nil
}

func writeQueueDeliveryResponse(w http.ResponseWriter, delivery *queueDelivery, nextCursor string) error {
	if delivery == nil {
		return writeQueueDeliveryBatch(w, nil, nextCursor)
	}
	if err := delivery.ensure(context.Background()); err != nil {
		return err
	}
	if delivery.nextCursor == "" {
		delivery.nextCursor = nextCursor
	}
	return writeQueueDeliveryBatch(w, []*queueDelivery{delivery}, nextCursor)
}

type acquireBackoff struct {
	next time.Duration
	rand func(int64) int64
}

func newAcquireBackoff() *acquireBackoff {
	return &acquireBackoff{
		next: acquireBackoffStart,
		rand: backoffRandInt63n,
	}
}

func (b *acquireBackoff) Next(limit time.Duration) time.Duration {
	if b.next <= 0 {
		b.next = acquireBackoffStart
	}
	sleep := b.next
	if limit > 0 && limit < sleep {
		sleep = limit
	}
	sleep = applyBackoffJitter(sleep, limit, b.rand)
	if limit > 0 && sleep > limit {
		sleep = limit
	}
	if sleep < acquireBackoffMin {
		if limit > 0 && limit < acquireBackoffMin {
			sleep = limit
		} else {
			sleep = acquireBackoffMin
		}
	}
	if sleep < 0 {
		sleep = 0
	}
	next := time.Duration(float64(b.next)*acquireBackoffMultiplier + 0.5)
	if next <= 0 {
		next = acquireBackoffStart
	}
	if next > acquireBackoffMax {
		next = acquireBackoffMax
	}
	b.next = next
	return sleep
}

func applyBackoffJitter(base, limit time.Duration, randFn func(int64) int64) time.Duration {
	if acquireBackoffJitter <= 0 || randFn == nil {
		return base
	}
	jitter := acquireBackoffJitter
	if base > 0 && base < jitter {
		jitter = base / 2
	}
	if limit > 0 && limit < jitter {
		jitter = limit / 2
	}
	if jitter <= 0 {
		return base
	}
	max := int64(jitter)*2 + 1
	if max <= 0 {
		return base
	}
	val := randFn(max)
	offset := time.Duration(val) - jitter
	return base + offset
}

func backoffRandInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	backoffRandMu.Lock()
	v := backoffRandSrc.Int63n(n)
	backoffRandMu.Unlock()
	return v
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

func (h *Handler) resolveBlock(requested int64) (time.Duration, bool) {
	if requested == api.BlockNoWait {
		return 0, false
	}
	if requested == 0 {
		if h.acquireBlock <= 0 {
			return 0, true
		}
		return h.acquireBlock, true
	}
	block := min(time.Duration(requested)*time.Second, h.acquireBlock)
	return block, false
}

func validateLease(meta *storage.Meta, leaseID string, fencingToken int64, now time.Time) error {
	if meta.Lease == nil || meta.Lease.ID != leaseID {
		// Provide optional diagnostics to help track lease mismatches during queue tuning.
		if debugQueueTiming {
			actual := ""
			if meta.Lease != nil {
				actual = meta.Lease.ID
			}
			fmt.Fprintf(os.Stderr, "[%s] queue.validate_lease.mismatch expected=%s actual=%s has_meta=%v\n", time.Now().Format(time.RFC3339Nano), leaseID, actual, meta != nil)
		}
		return httpError{
			Status:  http.StatusForbidden,
			Code:    "lease_required",
			Detail:  "active lease required",
			Version: meta.Version,
			ETag:    meta.StateETag,
		}
	}
	if meta.Lease.ExpiresAtUnix < now.Unix() {
		return httpError{
			Status:  http.StatusForbidden,
			Code:    "lease_expired",
			Detail:  "lease expired",
			Version: meta.Version,
			ETag:    meta.StateETag,
		}
	}
	if meta.Lease.FencingToken != fencingToken {
		return httpError{
			Status:  http.StatusForbidden,
			Code:    "fencing_mismatch",
			Detail:  "fencing token mismatch",
			Version: meta.Version,
			ETag:    meta.StateETag,
		}
	}
	return nil
}

func parseFencingToken(r *http.Request) (int64, error) {
	value := strings.TrimSpace(r.Header.Get(headerFencingToken))
	if value == "" {
		return 0, httpError{Status: http.StatusBadRequest, Code: "missing_fencing_token", Detail: "X-Fencing-Token header required"}
	}
	token, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, httpError{Status: http.StatusBadRequest, Code: "invalid_fencing_token", Detail: "invalid fencing token"}
	}
	return token, nil
}

type payloadSpool struct {
	threshold int64
	buf       []byte
	file      *os.File
	pooled    bool
}

var payloadBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, defaultPayloadSpoolMemoryThreshold)
	},
}

func newPayloadSpool(threshold int64) *payloadSpool {
	ps := &payloadSpool{threshold: threshold}
	if threshold <= 0 {
		return ps
	}
	maxInt := int64(^uint(0) >> 1)
	if threshold > maxInt {
		threshold = maxInt
	}
	bufCap := int(threshold)
	if threshold == defaultPayloadSpoolMemoryThreshold {
		if buf, ok := payloadBufferPool.Get().([]byte); ok {
			if cap(buf) < bufCap {
				buf = make([]byte, 0, bufCap)
			} else {
				buf = buf[:0]
			}
			ps.buf = buf
			ps.pooled = true
			return ps
		}
	}
	if bufCap > 0 {
		ps.buf = make([]byte, 0, bufCap)
	}
	return ps
}

func (p *payloadSpool) Write(data []byte) (int, error) {
	if p.file != nil {
		return p.file.Write(data)
	}
	if int64(len(p.buf))+int64(len(data)) <= p.threshold {
		p.buf = append(p.buf, data...)
		return len(data), nil
	}
	f, err := os.CreateTemp("", "lockd-json-")
	if err != nil {
		return 0, err
	}
	if len(p.buf) > 0 {
		if _, err := f.Write(p.buf); err != nil {
			f.Close()
			os.Remove(f.Name())
			return 0, err
		}
	}
	if p.pooled && p.buf != nil {
		payloadBufferPool.Put(p.buf[:0])
		p.pooled = false
	}
	n, err := f.Write(data)
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		return n, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		os.Remove(f.Name())
		return n, err
	}
	// ensure subsequent writes append to file
	p.file = f
	p.buf = nil
	return n, nil
}

func (p *payloadSpool) Reader() (io.ReadSeeker, error) {
	if p.file != nil {
		if _, err := p.file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return p.file, nil
	}
	return bytes.NewReader(p.buf), nil
}

func (p *payloadSpool) Close() error {
	if p.file != nil {
		name := p.file.Name()
		err := p.file.Close()
		_ = os.Remove(name)
		p.file = nil
		return err
	}
	if p.pooled && p.buf != nil {
		payloadBufferPool.Put(p.buf[:0])
		p.pooled = false
	}
	p.buf = nil
	return nil
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

func (h *Handler) isKeyObserved(key string) bool {
	if key == "" {
		return false
	}
	_, ok := h.observedKeys.Load(key)
	return ok
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

func cloneMeta(meta storage.Meta) storage.Meta {
	clone := meta
	if meta.Lease != nil {
		leaseCopy := *meta.Lease
		clone.Lease = &leaseCopy
	}
	if len(meta.StateDescriptor) > 0 {
		clone.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
	}
	return clone
}

func (h *Handler) creationMutex(key string) *sync.Mutex {
	mu, _ := h.createLocks.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (h *Handler) generateUniqueKey(ctx context.Context) (string, error) {
	const maxAttempts = 5
	for range maxAttempts {
		candidate := uuidv7.NewString()
		_, _, err := h.store.LoadMeta(ctx, candidate)
		if errors.Is(err, storage.ErrNotFound) {
			return candidate, nil
		}
		if err != nil {
			return "", fmt.Errorf("load meta: %w", err)
		}
	}
	return "", fmt.Errorf("unable to allocate unique key after %d attempts", maxAttempts)
}

func (h *Handler) dropLease(leaseID string) {
	h.leaseCache.Delete(leaseID)
}

type httpError struct {
	Status     int
	Code       string
	Detail     string
	Version    int64
	ETag       string
	RetryAfter int64
}

func (h httpError) Error() string {
	if h.Detail != "" {
		return fmt.Sprintf("%s: %s", h.Code, h.Detail)
	}
	return h.Code
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func durationToSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}
