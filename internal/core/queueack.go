package core

import (
	"context"
	"errors"

	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

// QueueAckResult mirrors the HTTP response but is transport-neutral.
type QueueAckResult struct {
	Acked         bool
	CorrelationID string
	LeaseOwner    string
}

// QueueNackResult describes a negative-ack outcome.
type QueueNackResult struct {
	Requeued      bool
	MetaETag      string
	CorrelationID string
}

// QueueExtendResult describes an extend outcome.
type QueueExtendResult struct {
	LeaseExpiresAtUnix       int64
	VisibilityTimeoutSeconds int64
	MetaETag                 string
	StateLeaseExpiresAtUnix  int64
	CorrelationID            string
}

// Ack confirms processing of a delivery and releases leases.
func (s *Service) Ack(ctx context.Context, cmd QueueAckCommand) (*QueueAckResult, error) {
	if err := s.maybeThrottleQueue(qrf.KindQueueAck); err != nil {
		return nil, err
	}
	qsvc, ok := s.queueProvider.(*queue.Service)
	if !ok || qsvc == nil {
		return nil, Failure{Code: "queue_disabled", Detail: "queue service not configured", HTTPStatus: 501}
	}
	if err := s.applyShutdownGuard("queue_ack"); err != nil {
		return nil, err
	}
	if cmd.Queue == "" || cmd.MessageID == "" || cmd.LeaseID == "" {
		return nil, Failure{Code: "missing_fields", Detail: "queue, message_id, lease_id are required", HTTPStatus: 400}
	}
	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: 400}
	}
	s.observeNamespace(namespace)

	messageKey, err := queue.MessageLeaseKey(namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		return nil, Failure{Code: "invalid_queue_key", Detail: err.Error(), HTTPStatus: 400}
	}
	messageRel := relativeKey(namespace, messageKey)
	meta, metaETag, err := s.ensureMeta(ctx, namespace, messageKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message lease not found", HTTPStatus: 404}
		}
		return nil, err
	}
	leaseOwner := ""
	if meta.Lease != nil {
		leaseOwner = meta.Lease.Owner
	}

	doc, docMetaETag, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
		}
		return nil, err
	}

	now := s.clock.Now()
	if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, now); err != nil {
		// Allow transparent lease upgrade when a fresher lease exists.
		var fail Failure
		if errors.As(err, &fail) && fail.Code == "lease_required" && meta.Lease != nil && meta.Lease.Owner != "" {
			cmd.LeaseID = meta.Lease.ID
			cmd.FencingToken = meta.Lease.FencingToken
			if docMetaETag != "" {
				cmd.MetaETag = docMetaETag
				metaETag = docMetaETag
			}
			leaseOwner = meta.Lease.Owner
		} else {
			return nil, err
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

	if err := qsvc.Ack(ctx, namespace, cmd.Queue, cmd.MessageID, cmd.MetaETag, cmd.StateETag, cmd.Stateful); err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return nil, Failure{Code: "cas_mismatch", Detail: "message metadata changed", HTTPStatus: 409}
		}
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message already removed", HTTPStatus: 404}
		}
		return nil, err
	}

	_ = s.releaseLeaseWithMeta(ctx, namespace, messageRel, cmd.LeaseID, meta, metaETag)

	if cmd.StateLeaseID != "" {
		stateKey, err := queue.StateLeaseKey(namespace, cmd.Queue, cmd.MessageID)
		if err == nil {
			stateRel := relativeKey(namespace, stateKey)
			stateMeta, stateMetaETag, loadErr := s.ensureMeta(ctx, namespace, stateKey)
			if loadErr == nil && stateMeta.Lease != nil {
				if err := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, s.clock.Now()); err == nil {
					_ = s.releaseLeaseWithMeta(ctx, namespace, stateRel, cmd.StateLeaseID, stateMeta, stateMetaETag)
				}
			}
		}
	}

	if s.queueDispatcher != nil {
		s.queueDispatcher.Notify(namespace, cmd.Queue)
	}

	return &QueueAckResult{Acked: true, CorrelationID: corr, LeaseOwner: leaseOwner}, nil
}

// Nack requeues a delivery with optional delay and last error.
func (s *Service) Nack(ctx context.Context, cmd QueueNackCommand) (*QueueNackResult, error) {
	if err := s.maybeThrottleQueue(qrf.KindQueueAck); err != nil {
		return nil, err
	}
	qsvc, ok := s.queueProvider.(*queue.Service)
	if !ok || qsvc == nil {
		return nil, Failure{Code: "queue_disabled", Detail: "queue service not configured", HTTPStatus: 501}
	}
	if err := s.applyShutdownGuard("queue_nack"); err != nil {
		return nil, err
	}
	if cmd.Queue == "" || cmd.MessageID == "" || cmd.LeaseID == "" || cmd.MetaETag == "" {
		return nil, Failure{Code: "missing_fields", Detail: "queue, message_id, lease_id, meta_etag are required", HTTPStatus: 400}
	}
	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: 400}
	}
	s.observeNamespace(namespace)

	messageKey, err := queue.MessageLeaseKey(namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		return nil, Failure{Code: "invalid_queue_key", Detail: err.Error(), HTTPStatus: 400}
	}
	messageRel := relativeKey(namespace, messageKey)
	meta, metaETag, err := s.ensureMeta(ctx, namespace, messageKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message lease not found", HTTPStatus: 404}
		}
		return nil, err
	}
	if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, s.clock.Now()); err != nil {
		return nil, err
	}

	doc, _, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
		}
		return nil, err
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

	delay := cmd.Delay
	newMetaETag, err := qsvc.Nack(ctx, namespace, cmd.Queue, doc, cmd.MetaETag, delay, cmd.LastError)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return nil, Failure{Code: "cas_mismatch", Detail: "message metadata changed", HTTPStatus: 409}
		}
		return nil, err
	}

	_ = s.releaseLeaseWithMeta(ctx, namespace, messageRel, cmd.LeaseID, meta, metaETag)
	if cmd.StateLeaseID != "" {
		stateKey, err := queue.StateLeaseKey(namespace, cmd.Queue, cmd.MessageID)
		if err == nil {
			stateRel := relativeKey(namespace, stateKey)
			stateMeta, stateMetaETag, loadErr := s.ensureMeta(ctx, namespace, stateKey)
			if loadErr == nil && stateMeta.Lease != nil {
				if err := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, s.clock.Now()); err == nil {
					_ = s.releaseLeaseWithMeta(ctx, namespace, stateRel, cmd.StateLeaseID, stateMeta, stateMetaETag)
				}
			}
		}
	}

	if s.queueDispatcher != nil && delay <= 0 {
		s.queueDispatcher.Notify(namespace, cmd.Queue)
	}

	return &QueueNackResult{
		Requeued:      true,
		MetaETag:      newMetaETag,
		CorrelationID: corr,
	}, nil
}

// Extend extends the visibility/lease for an in-flight message (and optional state lease).
func (s *Service) Extend(ctx context.Context, cmd QueueExtendCommand) (*QueueExtendResult, error) {
	if err := s.maybeThrottleQueue(qrf.KindQueueAck); err != nil {
		return nil, err
	}
	qsvc, ok := s.queueProvider.(*queue.Service)
	if !ok || qsvc == nil {
		return nil, Failure{Code: "queue_disabled", Detail: "queue service not configured", HTTPStatus: 501}
	}
	if err := s.applyShutdownGuard("queue_extend"); err != nil {
		return nil, err
	}
	if cmd.Queue == "" || cmd.MessageID == "" || cmd.LeaseID == "" || cmd.MetaETag == "" {
		return nil, Failure{Code: "missing_fields", Detail: "queue, message_id, lease_id, meta_etag are required", HTTPStatus: 400}
	}
	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: 400}
	}
	s.observeNamespace(namespace)

	messageKey, err := queue.MessageLeaseKey(namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		return nil, Failure{Code: "invalid_queue_key", Detail: err.Error(), HTTPStatus: 400}
	}
	messageRel := relativeKey(namespace, messageKey)
	meta, metaETag, err := s.ensureMeta(ctx, namespace, messageKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message lease not found", HTTPStatus: 404}
		}
		return nil, err
	}
	if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, s.clock.Now()); err != nil {
		return nil, err
	}

	doc, _, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
		}
		return nil, err
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

	extension := cmd.Visibility
	newMetaDocETag, err := qsvc.ExtendVisibility(ctx, namespace, cmd.Queue, doc, cmd.MetaETag, extension)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return nil, Failure{Code: "cas_mismatch", Detail: "message metadata changed", HTTPStatus: 409}
		}
		return nil, err
	}

	now := s.clock.Now()
	if meta.Lease == nil {
		return nil, Failure{Code: "lease_required", Detail: "lease missing", HTTPStatus: 403}
	}
	meta.Lease.ExpiresAtUnix = now.Add(extension).Unix()
	meta.UpdatedAtUnix = now.Unix()
	_, err = s.store.StoreMeta(ctx, namespace, messageRel, meta, metaETag)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return nil, Failure{Code: "lease_conflict", Detail: "lease changed during extend", HTTPStatus: 409}
		}
		return nil, err
	}

	stateLeaseExpires := int64(0)
	if cmd.StateLeaseID != "" {
		stateKey, err := queue.StateLeaseKey(namespace, cmd.Queue, cmd.MessageID)
		if err != nil {
			return nil, Failure{Code: "invalid_queue_state_key", Detail: err.Error(), HTTPStatus: 400}
		}
		stateRel := relativeKey(namespace, stateKey)
		stateMeta, stateMetaETag, err := s.ensureMeta(ctx, namespace, stateKey)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, Failure{Code: "state_not_found", Detail: "state lease missing", HTTPStatus: 404}
			}
			return nil, err
		}
		if err := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, now); err != nil {
			return nil, err
		}
		if stateMeta.Lease == nil {
			return nil, Failure{Code: "lease_required", Detail: "state lease missing", HTTPStatus: 403}
		}
		stateMeta.Lease.ExpiresAtUnix = now.Add(extension).Unix()
		stateMeta.UpdatedAtUnix = now.Unix()
		newStateETag, err := s.store.StoreMeta(ctx, namespace, stateRel, stateMeta, stateMetaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				return nil, Failure{Code: "lease_conflict", Detail: "state lease changed during extend", HTTPStatus: 409}
			}
			return nil, err
		}
		stateLeaseExpires = stateMeta.Lease.ExpiresAtUnix
		_ = newStateETag
	}

	return &QueueExtendResult{
		LeaseExpiresAtUnix:       meta.Lease.ExpiresAtUnix,
		VisibilityTimeoutSeconds: doc.VisibilityTimeout,
		MetaETag:                 newMetaDocETag,
		StateLeaseExpiresAtUnix:  stateLeaseExpires,
		CorrelationID:            corr,
	}, nil
}
