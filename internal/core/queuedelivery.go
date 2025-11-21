package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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

// consumeQueue obtains a single delivery respecting block/wait semantics.
func (s *Service) consumeQueue(ctx context.Context, qsvc *queue.Service, disp *queue.Dispatcher, namespace, queueName, owner string, visibility time.Duration, stateful bool, blockSeconds int64) (delivery *QueueDelivery, nextCursor string, err error) {
	if disp == nil {
		return nil, "", Failure{Code: "queue_disabled", Detail: "queue dispatcher not configured", HTTPStatus: 501}
	}
	if err := s.maybeThrottleQueue(qrf.KindQueueConsumer); err != nil {
		return nil, "", err
	}
	if err := s.applyShutdownGuard("queue"); err != nil {
		return nil, "", err
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
			delivery, retry, err = s.prepareQueueDelivery(ctx, qsvc, namespace, queueName, owner, visibility, stateful, cand)
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
			return delivery, nextCursor, nil
		}
		return nil, "", errQueueEmpty
	}

	for {
		attempts++
		waitStart := s.clock.Now()
		cand, waitErr := disp.Wait(ctx, namespace, queueName)
		timing.wait += s.clock.Now().Sub(waitStart)
		if waitErr != nil {
			if errors.Is(waitErr, queue.ErrTooManyConsumers) {
				return nil, "", Failure{Code: "queue_busy", Detail: "too many consumers", HTTPStatus: 503}
			}
			return nil, "", waitErr
		}
		var retry bool
		prepStart := s.clock.Now()
		delivery, retry, err = s.prepareQueueDelivery(ctx, qsvc, namespace, queueName, owner, visibility, stateful, cand)
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
		return delivery, nextCursor, nil
	}
}

// consumeQueueBatch aggregates deliveries up to pageSize with the same fill heuristics used by the HTTP handler.
func (s *Service) consumeQueueBatch(ctx context.Context, qsvc *queue.Service, disp *queue.Dispatcher, namespace, queueName, owner string, visibility time.Duration, stateful bool, blockSeconds int64, pageSize int) ([]*QueueDelivery, string, error) {
	if pageSize <= 1 {
		delivery, nextCursor, err := s.consumeQueue(ctx, qsvc, disp, namespace, queueName, owner, visibility, stateful, blockSeconds)
		if err != nil {
			return nil, "", err
		}
		if delivery == nil {
			return nil, "", errQueueEmpty
		}
		return []*QueueDelivery{delivery}, nextCursor, nil
	}

	if pageSize > 1 {
		pageSize = 1
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
		delivery, cursor, err := s.consumeQueue(ctx, qsvc, disp, namespace, queueName, owner, visibility, stateful, currentBlock)
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
				waitDelivery, waitCursor, waitErr := s.consumeQueue(waitCtx, qsvc, disp, namespace, queueName, owner, visibility, stateful, 1)
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

func (s *Service) prepareQueueDelivery(ctx context.Context, qsvc *queue.Service, namespace, queueName, owner string, visibility time.Duration, stateful bool, cand *queue.Candidate) (*QueueDelivery, bool, error) {
	if cand == nil {
		return nil, false, fmt.Errorf("nil candidate")
	}
	desc := cand.Descriptor
	doc := desc.Document

	messageKey, err := queue.MessageLeaseKey(namespace, queueName, doc.ID)
	if err != nil {
		return nil, false, Failure{Code: "invalid_queue_key", Detail: err.Error(), HTTPStatus: 400}
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

	acq, err := s.acquireLeaseForKey(ctx, acquireParams{
		Namespace:     namespace,
		Key:           messageKey,
		Owner:         owner,
		TTL:           ttl,
		Block:         0,
		WaitForever:   false,
		CorrelationID: corr,
	})
	if err != nil {
		if failure, ok := err.(Failure); ok && failure.Code == "waiting" {
			return nil, true, errDeliveryRetry
		}
		return nil, false, err
	}
	releaseMessage := func() {
		_ = s.releaseLeaseOutcome(ctx, messageKey, acq)
	}

	newMetaETag, err := qsvc.IncrementAttempts(ctx, namespace, queueName, &doc, desc.MetadataETag, ttl)
	if err != nil {
		releaseMessage()
		if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
			return nil, true, errDeliveryRetry
		}
		if s.logger != nil {
			s.logger.Warn("queue.delivery.increment_attempts.retry", "namespace", namespace, "queue", queueName, "message_id", doc.ID, "err", err)
		}
		return nil, true, errDeliveryRetry
	}

	payloadCtx := storage.ContextWithObjectPlaintextSize(ctx, doc.PayloadBytes)
	if len(doc.PayloadDescriptor) > 0 {
		payloadCtx = storage.ContextWithObjectDescriptor(payloadCtx, doc.PayloadDescriptor)
	}
	reader, info, err := qsvc.GetPayload(payloadCtx, namespace, queueName, doc.ID)
	if err != nil {
		releaseMessage()
		if errors.Is(err, storage.ErrNotFound) {
			s.rescheduleAfterPrepareRetry(qsvc, namespace, queueName, &desc, newMetaETag)
			return nil, true, errDeliveryRetry
		}
		if s.logger != nil {
			s.logger.Warn("queue.delivery.payload.retry", "namespace", namespace, "queue", queueName, "message_id", doc.ID, "err", err)
		}
		return nil, true, errDeliveryRetry
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

	message := &QueueMessage{
		Namespace:                namespace,
		Queue:                    queueName,
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
			_ = s.releaseLeaseOutcome(ctx, stateKey, stateOutcome)
		}
		_, stateDocETag, err := qsvc.LoadState(ctx, namespace, queueName, doc.ID)
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
		if stateDocETag != "" {
			stateETag = stateDocETag
		}
		message.StateLeaseID = stateOutcome.Response.LeaseID
		message.StateLeaseExpiresAtUnix = stateOutcome.Response.ExpiresAt
		message.StateFencingToken = stateOutcome.Response.FencingToken
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
			if _, err := qsvc.Reschedule(context.Background(), namespace, queueName, &docForReschedule, newMetaETag, 0); err == nil && s.queueDispatcher != nil {
				s.queueDispatcher.Notify(namespace, queueName)
			}
		}
		releaseMessage()
		if releaseState != nil {
			releaseState()
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
	TTL           time.Duration
	Block         time.Duration
	WaitForever   bool
	Idempotency   string
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
		Idempotency:      params.Idempotency,
		ClientHint:       params.CorrelationID,
		ForceQueryHidden: isQueueStateKey,
	}

	res, err := s.Acquire(ctx, cmd)
	if err != nil {
		return nil, err
	}

	resp := api.AcquireResponse{
		Namespace:     res.Namespace,
		LeaseID:       res.LeaseID,
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
	metaCopy.Lease = nil
	metaCopy.UpdatedAtUnix = s.clock.Now().Unix()
	if _, err := s.store.StoreMeta(ctx, namespace, relKey, &metaCopy, metaETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return nil
}
