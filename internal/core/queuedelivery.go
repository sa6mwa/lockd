package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

var (
	errQueueEmpty    = errors.New("queue: no messages available")
	errDeliveryRetry = errors.New("queue delivery retry")

	// ErrQueueEmpty is returned when no messages are available and waiting has expired.
	ErrQueueEmpty = errQueueEmpty
)

type payloadOutcome struct {
	res queue.PayloadResult
	err error
}

// consumeQueue obtains a single delivery respecting block/wait semantics.
func (s *Service) consumeQueue(ctx context.Context, qsvc *queue.Service, disp *queue.Dispatcher, namespace, queueName, owner string, txnID string, visibility time.Duration, stateful bool, blockSeconds int64) (delivery *QueueDelivery, nextCursor string, err error) {
	if disp == nil {
		return nil, "", Failure{Code: "queue_disabled", Detail: "queue dispatcher not configured", HTTPStatus: 501}
	}
	if err := s.maybeThrottleQueue(ctx, qrf.KindQueueConsumer); err != nil {
		return nil, "", err
	}
	if err := s.applyShutdownGuard("queue"); err != nil {
		return nil, "", err
	}
	decisionCtx, decisionCancel := s.queueDecisionContext(ctx)
	if decisionCancel != nil {
		defer decisionCancel()
	}
	if applied, err := s.maybeApplyQueueDecisionWorklist(decisionCtx, namespace, queueName); err != nil {
		if s.logger != nil {
			s.logger.Warn("queue.decision.apply_failed", "namespace", namespace, "queue", queueName, "error", err)
		}
	} else if applied > 0 && disp != nil {
		disp.Notify(namespace, queueName)
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
	start := s.clock.Now()
	attempts := 0
	retries := 0
	transientFailures := 0
	defer func() {
		_ = timing
		_ = mode
		_ = attempts
		_ = retries
		_ = transientFailures
		_ = start
	}()

	if blockSeconds == api.BlockNoWait {
		const maxImmediateAttempts = 5
		for range maxImmediateAttempts {
			attempts++
			tryStart := s.clock.Now()
			cand, tryErr := disp.Try(ctx, namespace, queueName)
			timing.try += s.clock.Now().Sub(tryStart)
			if tryErr != nil {
				if errors.Is(tryErr, queue.ErrTooManyConsumers) {
					return nil, "", Failure{Code: "queue_busy", Detail: "too many consumers", HTTPStatus: 503}
				}
				return nil, "", tryErr
			}
			if cand == nil {
				return nil, "", errQueueEmpty
			}
			var retry bool
			prepStart := s.clock.Now()
			delivery, retry, err = s.prepareQueueDelivery(ctx, qsvc, namespace, queueName, owner, txnID, visibility, stateful, cand)
			timing.prepare += s.clock.Now().Sub(prepStart)
			if retry {
				retries++
				disp.Notify(namespace, queueName)
				continue
			}
			if err != nil {
				if failure := (Failure{}); errors.As(err, &failure) {
					return nil, "", err
				}
				transientFailures++
				if transientFailures >= 5 {
					return nil, "", errQueueEmpty
				}
				if s.logger != nil {
					s.logger.Warn("queue.consume.retry", "namespace", namespace, "queue", queueName, "owner", owner, "err", err, "attempt", attempts, "transient_failures", transientFailures)
				}
				disp.Notify(namespace, queueName)
				continue
			}
			if delivery == nil {
				return nil, "", fmt.Errorf("queue delivery missing for %s", queueName)
			}
			nextCursor = cand.NextCursor
			delivery.NextCursor = nextCursor
			if s.queueMetrics != nil {
				duration := s.clock.Now().Sub(start)
				s.queueMetrics.recordDequeue(ctx, namespace, queueName, delivery.PayloadBytes, duration, stateful)
			}
			return delivery, nextCursor, nil
		}
		return nil, "", errQueueEmpty
	}

	for {
		attempts++
		waitCtx := ctx
		var waitCancel context.CancelFunc
		if blockSeconds > 0 {
			// Keep the wait bounded to the caller's block window so the remaining
			// context deadline can be used for the prepare/storage phase.
			waitCtx, waitCancel = context.WithTimeout(ctx, time.Duration(blockSeconds)*time.Second)
		}
		waitStart := s.clock.Now()
		cand, waitErr := disp.Wait(waitCtx, namespace, queueName)
		timing.wait += s.clock.Now().Sub(waitStart)
		if waitCancel != nil {
			waitCancel()
		}
		if waitErr != nil {
			if errors.Is(waitErr, queue.ErrTooManyConsumers) {
				return nil, "", Failure{Code: "queue_busy", Detail: "too many consumers", HTTPStatus: 503}
			}
			return nil, "", waitErr
		}
		var retry bool
		prepStart := s.clock.Now()
		delivery, retry, err = s.prepareQueueDelivery(ctx, qsvc, namespace, queueName, owner, txnID, visibility, stateful, cand)
		timing.prepare += s.clock.Now().Sub(prepStart)
		if retry {
			retries++
			disp.Notify(namespace, queueName)
			continue
		}
		if err != nil {
			if failure := (Failure{}); errors.As(err, &failure) {
				return nil, "", err
			}
			transientFailures++
			if transientFailures >= 5 {
				return nil, "", errQueueEmpty
			}
			if s.logger != nil {
				s.logger.Warn("queue.consume.retry", "namespace", namespace, "queue", queueName, "owner", owner, "err", err, "attempt", attempts, "transient_failures", transientFailures)
			}
			disp.Notify(namespace, queueName)
			continue
		}
		if delivery == nil {
			return nil, "", fmt.Errorf("queue delivery missing for %s", queueName)
		}
		nextCursor = cand.NextCursor
		delivery.NextCursor = nextCursor
		if s.queueMetrics != nil {
			duration := s.clock.Now().Sub(start)
			s.queueMetrics.recordDequeue(ctx, namespace, queueName, delivery.PayloadBytes, duration, stateful)
		}
		return delivery, nextCursor, nil
	}
}

func (s *Service) queueDecisionContext(parent context.Context) (context.Context, context.CancelFunc) {
	if s == nil || s.queueDecisionApplyTimeout <= 0 {
		return parent, nil
	}
	base := context.Background()
	if parent != nil {
		base = context.WithoutCancel(parent)
	}
	return context.WithTimeout(base, s.queueDecisionApplyTimeout)
}

// consumeQueueBatch aggregates deliveries up to pageSize with the same fill heuristics used by the HTTP handler.
func (s *Service) consumeQueueBatch(ctx context.Context, qsvc *queue.Service, disp *queue.Dispatcher, namespace, queueName, owner string, txnID string, visibility time.Duration, stateful bool, blockSeconds int64, pageSize int) ([]*QueueDelivery, string, error) {
	if pageSize <= 1 {
		delivery, nextCursor, err := s.consumeQueue(ctx, qsvc, disp, namespace, queueName, owner, txnID, visibility, stateful, blockSeconds)
		if err != nil {
			return nil, "", err
		}
		if delivery == nil {
			return nil, "", errQueueEmpty
		}
		return []*QueueDelivery{delivery}, nextCursor, nil
	}

	hasWatcher := s.queueHasActiveWatcher(disp, namespace, queueName)
	fillBudget, retryInterval := queueBatchFillConfig(queueName, hasWatcher, blockSeconds, pageSize)
	var fillDeadline time.Time
	if fillBudget > 0 {
		fillDeadline = s.clock.Now().Add(fillBudget)
	}

	deliveries := make([]*QueueDelivery, 0, pageSize)
	var nextCursor string
	currentBlock := blockSeconds
	start := s.clock.Now()
	var (
		tryAttempts   int
		waitAttempts  int
		emptyFastPath int
	)

	for len(deliveries) < pageSize {
		delivery, cursor, err := s.consumeQueue(ctx, qsvc, disp, namespace, queueName, owner, txnID, visibility, stateful, currentBlock)
		if err != nil {
			if errors.Is(err, errQueueEmpty) {
				emptyFastPath++
				if len(deliveries) == 0 {
					return nil, "", err
				}
				if fillDeadline.IsZero() || s.clock.Now().After(fillDeadline) || retryInterval <= 0 {
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
				waitDelivery, waitCursor, waitErr := s.consumeQueue(waitCtx, qsvc, disp, namespace, queueName, owner, txnID, visibility, stateful, 1)
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
	_ = tryAttempts
	_ = waitAttempts
	_ = emptyFastPath
	_ = start

	if len(deliveries) == 0 {
		return nil, "", errQueueEmpty
	}
	return deliveries, nextCursor, nil
}

func (s *Service) queueHasActiveWatcher(disp *queue.Dispatcher, namespace, queue string) bool {
	if disp == nil {
		return false
	}
	return disp.HasActiveWatcher(namespace, queue)
}

func queueBatchFillConfig(queue string, hasWatcher bool, blockSeconds int64, pageSize int) (time.Duration, time.Duration) {
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
		budget = minDuration(watcherStep*time.Duration(minInt(pageSize, 8)), watcherCap)
	} else {
		step = pollStep
		budget = minDuration(pollStep*time.Duration(pageSize), pollCap)
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (s *Service) rescheduleAfterPrepareRetry(qsvc *queue.Service, namespace, queueName string, doc *queue.MessageDescriptor, metaETag string) {
	if doc == nil || metaETag == "" {
		return
	}
	docCopy := doc.Document
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := qsvc.Reschedule(ctx, namespace, queueName, &docCopy, metaETag, 0); err != nil {
		if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
			if s.queueDispatcher != nil {
				s.queueDispatcher.Notify(namespace, queueName)
			}
			return
		}
		return
	}
	if s.queueDispatcher != nil {
		s.queueDispatcher.Notify(namespace, queueName)
	}
}

func (s *Service) prepareQueueDelivery(ctx context.Context, qsvc *queue.Service, namespace, queueName, owner string, txnID string, visibility time.Duration, stateful bool, cand *queue.Candidate) (*QueueDelivery, bool, error) {
	if cand == nil {
		return nil, false, fmt.Errorf("nil candidate")
	}
	desc := cand.Descriptor
	doc := desc.Document

	messageKey, err := queue.MessageLeaseKey(namespace, queueName, doc.ID)
	if err != nil {
		return nil, false, Failure{Code: "invalid_queue_key", Detail: err.Error(), HTTPStatus: 400}
	}
	freshRes, err := qsvc.GetMessage(ctx, namespace, queueName, doc.ID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if s.queueDispatcher != nil {
				s.queueDispatcher.Notify(namespace, queueName)
			}
			return nil, true, errDeliveryRetry
		}
		return nil, false, err
	}
	if freshRes.Document == nil {
		if s.queueDispatcher != nil {
			s.queueDispatcher.Notify(namespace, queueName)
		}
		return nil, true, errDeliveryRetry
	}
	if freshRes.Document.Queue != queueName || freshRes.Document.ID != doc.ID {
		if s.queueDispatcher != nil {
			s.queueDispatcher.Notify(namespace, queueName)
		}
		return nil, true, errDeliveryRetry
	}
	doc = *freshRes.Document
	desc.Document = doc
	if freshRes.ETag != "" {
		desc.MetadataETag = freshRes.ETag
	}
	now := s.clock.Now().UTC()
	if doc.NotVisibleUntil.After(now) {
		qsvc.UpdateReadyCache(namespace, queueName, &doc, desc.MetadataETag)
		traceQueueLease(s.logger, "queue.delivery.not_visible_retry",
			"namespace", namespace,
			"queue", queueName,
			"message_id", doc.ID,
			"not_visible_until", doc.NotVisibleUntil.Unix(),
			"now_unix", now.Unix(),
		)
		if s.queueDispatcher != nil {
			s.queueDispatcher.Notify(namespace, queueName)
		}
		return nil, true, errDeliveryRetry
	}

	ttl := visibility
	if ttl <= 0 {
		ttl = time.Duration(doc.VisibilityTimeout) * time.Second
		if ttl <= 0 {
			ttl = s.defaultTTL.Default
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

	var acquireStart time.Time
	if queueDeliveryStatsEnabled() {
		acquireStart = time.Now()
	}
	acq, err := s.acquireLeaseForKey(ctx, acquireParams{
		Namespace:     namespace,
		Key:           messageKey,
		Owner:         owner,
		TxnID:         txnID,
		TTL:           ttl,
		Block:         0,
		WaitForever:   false,
		CorrelationID: corr,
	})
	if !acquireStart.IsZero() {
		recordQueueDeliveryAcquire(time.Since(acquireStart))
	}
	if err != nil {
		if failure, ok := err.(Failure); ok && failure.Code == "waiting" {
			return nil, true, errDeliveryRetry
		}
		return nil, false, err
	}
	releaseLease := func(key string, outcome *acquireOutcome) {
		if outcome == nil {
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.releaseLeaseOutcome(releaseCtx, key, outcome)
	}
	releaseMessage := func() {
		releaseLease(messageKey, acq)
	}

	if txnID != "" && txnExplicit(&acq.Meta) {
		relParticipant := relativeKey(namespace, messageKey)
		if _, _, err := s.enlistTxnParticipant(ctx, txnID, namespace, relParticipant, acq.Response.ExpiresAt); err != nil {
			releaseMessage()
			return nil, false, fmt.Errorf("register txn participant: %w", err)
		}
	}

	doc.LeaseID = acq.Response.LeaseID
	doc.LeaseFencingToken = acq.Response.FencingToken
	doc.LeaseTxnID = acq.Response.TxnID
	var incrementStart time.Time
	if queueDeliveryStatsEnabled() {
		incrementStart = time.Now()
	}
	payloadSize := doc.PayloadBytes
	contentType := doc.PayloadContentType
	var (
		payloadCh     chan payloadOutcome
		payloadCancel context.CancelFunc
	)
	if payloadSize > 0 {
		payloadCtx, cancel := context.WithCancel(ctx)
		payloadCancel = cancel
		payloadCh = make(chan payloadOutcome, 1)
		go s.fetchQueuePayload(payloadCtx, payloadCh, qsvc, namespace, queueName, doc.ID, payloadSize, doc.PayloadDescriptor)
	}

	newMetaETag, err := qsvc.IncrementAttempts(ctx, namespace, queueName, &doc, desc.MetadataETag, ttl)
	if !incrementStart.IsZero() {
		recordQueueDeliveryIncrement(time.Since(incrementStart))
	}
	if err != nil {
		if payloadCancel != nil {
			payloadCancel()
		}
		releaseMessage()
		if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
			return nil, true, errDeliveryRetry
		}
		if s.logger != nil {
			s.logger.Warn("queue.delivery.increment_attempts.retry", "namespace", namespace, "queue", queueName, "message_id", doc.ID, "err", err)
		}
		return nil, true, errDeliveryRetry
	}
	if s.queueDispatcher != nil {
		s.queueDispatcher.NotifyAt(namespace, queueName, doc.ID, doc.NotVisibleUntil)
	}
	now = s.clock.Now()
	redelivered, previous := recordQueueLeaseDelivery(namespace, queueName, doc.ID, acq.Response.LeaseID, acq.Response.FencingToken, acq.Response.TxnID, newMetaETag, doc.NotVisibleUntil, now, &acq.Meta, acq.MetaETag)
	if redelivered {
		traceQueueLease(s.logger, "queue.lease.redelivered_inflight",
			"namespace", namespace,
			"queue", queueName,
			"message_id", doc.ID,
			"prev_lease_id", previous.leaseID,
			"prev_fencing_token", previous.fencingToken,
			"prev_txn_id", previous.txnID,
			"prev_meta_etag", previous.metaETag,
			"prev_not_visible_until", previous.notVisibleUntil.Unix(),
			"prev_updated_at", previous.updatedAt.Unix(),
			"lease_id", acq.Response.LeaseID,
			"fencing_token", acq.Response.FencingToken,
			"txn_id", acq.Response.TxnID,
			"meta_etag", newMetaETag,
			"not_visible_until", doc.NotVisibleUntil.Unix(),
			"now_unix", now.Unix(),
		)
	}

	var (
		reader io.ReadCloser
		info   *storage.ObjectInfo
	)
	if payloadSize <= 0 {
		reader = io.NopCloser(bytes.NewReader(nil))
		payloadSize = 0
	} else if payloadCh != nil {
		outcome := <-payloadCh
		if outcome.err != nil {
			releaseMessage()
			if errors.Is(outcome.err, storage.ErrNotFound) {
				s.rescheduleAfterPrepareRetry(qsvc, namespace, queueName, &desc, newMetaETag)
				return nil, true, errDeliveryRetry
			}
			if s.logger != nil {
				s.logger.Warn("queue.delivery.payload.retry", "namespace", namespace, "queue", queueName, "message_id", doc.ID, "err", outcome.err)
			}
			return nil, true, errDeliveryRetry
		}
		reader = outcome.res.Reader
		info = outcome.res.Info
		if info != nil && info.Size >= 0 {
			payloadSize = info.Size
		}
		if payloadSize < 0 {
			payloadSize = 0
		}
		if info != nil && info.ContentType != "" {
			contentType = info.ContentType
		}
	}

	message := &QueueMessage{
		Namespace:                namespace,
		Queue:                    queueName,
		MessageID:                doc.ID,
		Attempts:                 doc.Attempts,
		MaxAttempts:              doc.MaxAttempts,
		FailureAttempts:          doc.FailureAttempts,
		NotVisibleUntilUnix:      doc.NotVisibleUntil.Unix(),
		VisibilityTimeoutSeconds: doc.VisibilityTimeout,
		Attributes:               doc.Attributes,
		PayloadContentType:       contentType,
		PayloadBytes:             payloadSize,
		CorrelationID:            corr,
		LeaseID:                  acq.Response.LeaseID,
		LeaseExpiresAtUnix:       acq.Response.ExpiresAt,
		FencingToken:             acq.Response.FencingToken,
		TxnID:                    acq.Response.TxnID,
		MetaETag:                 newMetaETag,
	}

	stateTxnID := txnID
	if stateTxnID == "" {
		stateTxnID = acq.Response.TxnID
	}

	var stateOutcome *acquireOutcome
	var stateKey string
	var releaseState func()
	if stateful {
		stateETag, err := qsvc.EnsureStateExists(ctx, namespace, queueName, doc.ID)
		if err != nil {
			reader.Close()
			releaseMessage()
			if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
				return nil, true, errDeliveryRetry
			}
			return nil, false, fmt.Errorf("queue ensure state: %w", err)
		}
		stateKey, err = queue.StateLeaseKey(namespace, queueName, doc.ID)
		if err != nil {
			reader.Close()
			releaseMessage()
			return nil, false, Failure{Code: "invalid_queue_state_key", Detail: err.Error(), HTTPStatus: 400}
		}
		stateOutcome, err = s.acquireLeaseForKey(ctx, acquireParams{
			Namespace:     namespace,
			Key:           stateKey,
			Owner:         owner,
			TxnID:         stateTxnID,
			TTL:           ttl,
			Block:         0,
			WaitForever:   false,
			CorrelationID: corr,
		})
		if err != nil {
			reader.Close()
			releaseMessage()
			if failure, ok := err.(Failure); ok && failure.Code == "waiting" {
				s.rescheduleAfterPrepareRetry(qsvc, namespace, queueName, &desc, newMetaETag)
				return nil, true, errDeliveryRetry
			}
			return nil, false, err
		}
		releaseState = func() {
			releaseLease(stateKey, stateOutcome)
		}
		if txnID != "" && stateOutcome != nil && txnExplicit(&stateOutcome.Meta) {
			relParticipant := relativeKey(namespace, stateKey)
			if _, _, err := s.enlistTxnParticipant(ctx, txnID, namespace, relParticipant, stateOutcome.Response.ExpiresAt); err != nil {
				releaseState()
				reader.Close()
				releaseMessage()
				return nil, false, fmt.Errorf("register txn participant: %w", err)
			}
		}
		stateRes, err := qsvc.LoadState(ctx, namespace, queueName, doc.ID)
		if err != nil {
			releaseState()
			reader.Close()
			releaseMessage()
			if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
				return nil, true, errDeliveryRetry
			}
			if s.logger != nil {
				s.logger.Warn("queue.delivery.state.retry", "namespace", namespace, "queue", queueName, "message_id", doc.ID, "err", err)
			}
			return nil, true, errDeliveryRetry
		}
		stateDocETag := stateRes.ETag
		if stateDocETag != "" {
			stateETag = stateDocETag
		}
		message.StateLeaseID = stateOutcome.Response.LeaseID
		message.StateLeaseExpiresAtUnix = stateOutcome.Response.ExpiresAt
		message.StateFencingToken = stateOutcome.Response.FencingToken
		message.StateTxnID = stateOutcome.Response.TxnID
		message.StateETag = stateETag
	}

	docForReschedule := doc
	finalize := func(success bool) {
		if reader != nil {
			reader.Close()
		}
		if success {
			return
		}
		if newMetaETag != "" {
			traceQueueLease(s.logger, "queue.delivery.finalize_keep_lease",
				"namespace", namespace,
				"queue", queueName,
				"message_id", docForReschedule.ID,
				"meta_etag", newMetaETag,
				"not_visible_until", docForReschedule.NotVisibleUntil.Unix(),
			)
		}
	}

	return &QueueDelivery{
		Message:            message,
		Payload:            reader,
		PayloadContentType: contentType,
		PayloadBytes:       payloadSize,
		NextCursor:         cand.NextCursor,
		Finalize:           finalize,
	}, false, nil
}

// internal compatibility helpers lifted from the HTTP handler. They should be migrated
// to a transport-neutral location as decoupling progresses.
type acquireOutcome struct {
	Response api.AcquireResponse
	Meta     storage.Meta
	MetaETag string
}

type acquireParams struct {
	Namespace     string
	Key           string
	Owner         string
	TxnID         string
	TTL           time.Duration
	Block         time.Duration
	WaitForever   bool
	CorrelationID string
}

// acquireLeaseForKey reuses the core Acquire flow but preserves queue-specific defaults.
func (s *Service) acquireLeaseForKey(ctx context.Context, params acquireParams) (*acquireOutcome, error) {
	if params.Key == "" {
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: 400}
	}
	namespace := strings.TrimSpace(params.Namespace)
	if namespace == "" {
		return nil, Failure{Code: "invalid_namespace", Detail: "namespace required for acquire", HTTPStatus: 400}
	}
	s.observeNamespace(namespace)
	relKey := relativeKey(namespace, params.Key)
	isQueueStateKey := queue.IsQueueStateKey(relKey)
	if params.Owner == "" {
		return nil, Failure{Code: "missing_owner", Detail: "owner required", HTTPStatus: 400}
	}
	if params.TTL <= 0 {
		params.TTL = s.defaultTTL.Default
	}

	blockSeconds := int64(params.Block.Seconds())
	if params.WaitForever {
		blockSeconds = -1
	}

	cmd := AcquireCommand{
		Namespace:        namespace,
		Key:              relKey,
		Owner:            params.Owner,
		TTLSeconds:       int64(params.TTL.Seconds()),
		BlockSeconds:     blockSeconds,
		TxnID:            params.TxnID,
		ClientHint:       params.CorrelationID,
		ForceQueryHidden: isQueueStateKey,
	}

	if queue.IsQueueMessageKey(relKey) || queue.IsQueueStateKey(relKey) {
		if parts, ok := queue.ParseMessageLeaseKey(relKey); ok {
			if rec, ok := queueLeaseRecordSnapshot(namespace, parts.Queue, parts.ID); ok && rec.leaseMetaSet && rec.leaseMetaETag != "" {
				now := s.clock.Now()
				meta := cloneMeta(rec.leaseMeta)
				if meta.Lease == nil || meta.Lease.ExpiresAtUnix <= now.Unix() {
					if cmd.ForceQueryHidden && !meta.HasQueryHiddenPreference() {
						meta.SetQueryHidden(true)
					}
					if meta.Lease == nil && meta.StagedTxnID != "" {
						s.discardStagedArtifacts(ctx, namespace, relKey, &meta)
						clearStagingFields(&meta)
					}
					oldExpires := int64(0)
					if meta.Lease != nil {
						oldExpires = meta.Lease.ExpiresAtUnix
					}
					leaseID := xid.New().String()
					txnID := strings.TrimSpace(params.TxnID)
					txnExplicit := txnID != ""
					if txnID == "" {
						txnID = xid.New().String()
					}
					newFencing := meta.FencingToken + 1
					meta.FencingToken = newFencing
					meta.Lease = &storage.Lease{
						ID:            leaseID,
						Owner:         params.Owner,
						ExpiresAtUnix: now.Add(params.TTL).Unix(),
						FencingToken:  newFencing,
						TxnID:         txnID,
						TxnExplicit:   txnExplicit,
					}
					meta.UpdatedAtUnix = now.Unix()
					newMetaETag, err := s.store.StoreMeta(ctx, namespace, relKey, &meta, rec.leaseMetaETag)
					if err == nil {
						if err := s.updateLeaseIndex(ctx, namespace, relKey, oldExpires, meta.Lease.ExpiresAtUnix); err != nil && s.logger != nil {
							s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", relKey, "error", err)
						}
						return &acquireOutcome{
							Response: api.AcquireResponse{
								Namespace:     namespace,
								LeaseID:       leaseID,
								TxnID:         txnID,
								Key:           relKey,
								Owner:         params.Owner,
								ExpiresAt:     meta.Lease.ExpiresAtUnix,
								Version:       meta.Version,
								StateETag:     meta.StateETag,
								FencingToken:  newFencing,
								CorrelationID: params.CorrelationID,
							},
							Meta:     meta,
							MetaETag: newMetaETag,
						}, nil
					}
				}
			}
		}
	}

	res, err := s.Acquire(ctx, cmd)
	if err != nil {
		var failure Failure
		if errors.As(err, &failure) && failure.Code == "waiting" {
			if queue.IsQueueMessageKey(relKey) || queue.IsQueueStateKey(relKey) {
				applied, applyErr := s.maybeApplyDecisionMarkerForLease(ctx, namespace, relKey)
				if applyErr != nil {
					return nil, applyErr
				}
				if applied {
					res, err = s.Acquire(ctx, cmd)
				}
			}
		}
		if err != nil {
			return nil, err
		}
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

func (s *Service) releaseLeaseOutcome(ctx context.Context, key string, outcome *acquireOutcome) error {
	if outcome == nil {
		return nil
	}
	return s.releaseLeaseWithMeta(ctx, outcome.Response.Namespace, key, outcome.Response.LeaseID, &outcome.Meta, outcome.MetaETag)
}

func (s *Service) releaseLeaseWithMeta(ctx context.Context, namespace, key string, leaseID string, meta *storage.Meta, metaETag string) error {
	if meta == nil {
		return nil
	}
	relKey := relativeKey(namespace, key)
	metaCopy := *meta
	oldExpires := int64(0)
	if metaCopy.Lease != nil {
		oldExpires = metaCopy.Lease.ExpiresAtUnix
	}
	metaCopy.Lease = nil
	metaCopy.UpdatedAtUnix = s.clock.Now().Unix()
	if _, err := s.store.StoreMeta(ctx, namespace, relKey, &metaCopy, metaETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if err := s.updateLeaseIndex(ctx, namespace, relKey, oldExpires, 0); err != nil && s.logger != nil {
		s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", relKey, "error", err)
	}
	return nil
}

func (s *Service) fetchQueuePayload(ctx context.Context, ch chan<- payloadOutcome, qsvc *queue.Service, namespace, queueName, id string, payloadSize int64, descriptor []byte) {
	payloadCtx := storage.ContextWithObjectPlaintextSize(ctx, payloadSize)
	if len(descriptor) > 0 {
		payloadCtx = storage.ContextWithObjectDescriptor(payloadCtx, descriptor)
	}
	var payloadStart time.Time
	if queueDeliveryStatsEnabled() {
		payloadStart = time.Now()
	}
	res, err := qsvc.GetPayload(payloadCtx, namespace, queueName, id)
	if !payloadStart.IsZero() {
		recordQueueDeliveryPayload(time.Since(payloadStart))
	}
	outcome := payloadOutcome{res: res, err: err}
	select {
	case ch <- outcome:
	default:
	}
}
