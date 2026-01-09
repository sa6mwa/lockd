package core

import (
	"context"
	"errors"
	"fmt"

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
	TxnID         string
}

// QueueNackResult describes a negative-ack outcome.
type QueueNackResult struct {
	Requeued      bool
	MetaETag      string
	CorrelationID string
	TxnID         string
}

// QueueExtendResult describes an extend outcome.
type QueueExtendResult struct {
	LeaseExpiresAtUnix       int64
	VisibilityTimeoutSeconds int64
	MetaETag                 string
	StateLeaseExpiresAtUnix  int64
	CorrelationID            string
	TxnID                    string
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
	now := s.clock.Now()
	leaseTxn := ""
	if meta.Lease != nil {
		leaseTxn = meta.Lease.TxnID
	}
	checkTxn := cmd.TxnID
	if checkTxn == "" {
		checkTxn = leaseTxn
	}
	if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
		leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now)
		if _, _, err := s.clearExpiredLease(ctx, namespace, messageRel, meta, metaETag, now, false); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				return nil, leaseErr
			}
			return nil, err
		}
		return nil, leaseErr
	}

	docRes, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
		}
		return nil, err
	}
	doc := docRes.Document

	if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now); err != nil {
		return nil, err
	}
	if err := validateQueueMessageLease(doc, cmd.LeaseID, cmd.FencingToken, checkTxn); err != nil {
		return nil, err
	}

	corr := doc.CorrelationID
	if corr == "" {
		corr = correlation.ID(ctx)
	}
	if corr == "" {
		corr = correlation.Generate()
	}
	ctx = correlation.Set(ctx, corr)

	if checkTxn != "" && s.tcDecider != nil {
		rec := TxnRecord{
			TxnID:         checkTxn,
			State:         TxnStateCommit,
			ExpiresAtUnix: meta.Lease.ExpiresAtUnix,
			Participants: []TxnParticipant{{
				Namespace:   namespace,
				Key:         messageRel,
				BackendHash: s.backendHash,
			}},
		}
		if cmd.StateLeaseID != "" {
			stateKey, err := queue.StateLeaseKey(namespace, cmd.Queue, cmd.MessageID)
			if err != nil {
				return nil, Failure{Code: "invalid_queue_state_key", Detail: err.Error(), HTTPStatus: 400}
			}
			rec.Participants = append(rec.Participants, TxnParticipant{
				Namespace:   namespace,
				Key:         relativeKey(namespace, stateKey),
				BackendHash: s.backendHash,
			})
		}
		state, err := s.tcDecider.Decide(ctx, rec)
		if err != nil {
			return nil, err
		}
		if state == TxnStatePending {
			return nil, Failure{Code: "txn_pending", Detail: "transaction decision not recorded", HTTPStatus: 409}
		}
		return &QueueAckResult{
			Acked:         true,
			CorrelationID: corr,
			LeaseOwner:    leaseOwner,
			TxnID:         leaseTxn,
		}, nil
	}

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
				stateTxn := stateMeta.Lease.TxnID
				now := s.clock.Now()
				if stateMeta.Lease.ExpiresAtUnix <= now.Unix() {
					_, _, _ = s.clearExpiredLease(ctx, namespace, stateRel, stateMeta, stateMetaETag, now, false)
				} else if err := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, stateTxn, now); err == nil {
					_ = s.releaseLeaseWithMeta(ctx, namespace, stateRel, cmd.StateLeaseID, stateMeta, stateMetaETag)
				}
			}
		}
	}

	if s.queueDispatcher != nil {
		s.queueDispatcher.Notify(namespace, cmd.Queue)
	}

	return &QueueAckResult{
		Acked:         true,
		CorrelationID: corr,
		LeaseOwner:    leaseOwner,
		TxnID:         leaseTxn,
	}, nil
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
	leaseTxn := ""
	if meta.Lease != nil {
		leaseTxn = meta.Lease.TxnID
	}
	checkTxn := cmd.TxnID
	if checkTxn == "" {
		checkTxn = leaseTxn
	}
	now := s.clock.Now()
	if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
		leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now)
		if _, _, err := s.clearExpiredLease(ctx, namespace, messageRel, meta, metaETag, now, false); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				return nil, leaseErr
			}
			return nil, err
		}
		return nil, leaseErr
	}
	if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now); err != nil {
		return nil, err
	}

	docRes, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
		}
		return nil, err
	}
	doc := docRes.Document
	if err := validateQueueMessageLease(doc, cmd.LeaseID, cmd.FencingToken, checkTxn); err != nil {
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

	if checkTxn != "" && s.tcDecider != nil {
		rec := TxnRecord{
			TxnID:         checkTxn,
			State:         TxnStateRollback,
			ExpiresAtUnix: meta.Lease.ExpiresAtUnix,
			Participants: []TxnParticipant{{
				Namespace:   namespace,
				Key:         messageRel,
				BackendHash: s.backendHash,
			}},
		}
		if cmd.StateLeaseID != "" {
			stateKey, err := queue.StateLeaseKey(namespace, cmd.Queue, cmd.MessageID)
			if err != nil {
				return nil, Failure{Code: "invalid_queue_state_key", Detail: err.Error(), HTTPStatus: 400}
			}
			rec.Participants = append(rec.Participants, TxnParticipant{
				Namespace:   namespace,
				Key:         relativeKey(namespace, stateKey),
				BackendHash: s.backendHash,
			})
		}
		state, err := s.tcDecider.Decide(ctx, rec)
		if err != nil {
			return nil, err
		}
		if state == TxnStatePending {
			return nil, Failure{Code: "txn_pending", Detail: "transaction decision not recorded", HTTPStatus: 409}
		}
		metaETag := cmd.MetaETag
		if cmd.Delay > 0 {
			docRes, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
				}
				return nil, err
			}
			newETag, err := qsvc.Reschedule(ctx, namespace, cmd.Queue, docRes.Document, docRes.ETag, cmd.Delay)
			if err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					return nil, Failure{Code: "cas_mismatch", Detail: "message metadata changed", HTTPStatus: 409}
				}
				return nil, err
			}
			metaETag = newETag
		}
		return &QueueNackResult{
			Requeued:      true,
			MetaETag:      metaETag,
			CorrelationID: corr,
			TxnID:         leaseTxn,
		}, nil
	}

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
				stateTxn := stateMeta.Lease.TxnID
				now := s.clock.Now()
				if stateMeta.Lease.ExpiresAtUnix <= now.Unix() {
					_, _, _ = s.clearExpiredLease(ctx, namespace, stateRel, stateMeta, stateMetaETag, now, false)
				} else if err := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, stateTxn, now); err == nil {
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
		TxnID:         leaseTxn,
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
	leaseTxn := ""
	if meta.Lease != nil {
		leaseTxn = meta.Lease.TxnID
	}
	checkTxn := cmd.TxnID
	if checkTxn == "" {
		checkTxn = leaseTxn
	}
	now := s.clock.Now()
	if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
		leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now)
		if _, _, err := s.clearExpiredLease(ctx, namespace, messageRel, meta, metaETag, now, false); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				return nil, leaseErr
			}
			return nil, err
		}
		return nil, leaseErr
	}
	if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now); err != nil {
		return nil, err
	}

	docRes, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
		}
		return nil, err
	}
	doc := docRes.Document
	if err := validateQueueMessageLease(doc, cmd.LeaseID, cmd.FencingToken, checkTxn); err != nil {
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

	now = s.clock.Now()
	if meta.Lease == nil {
		return nil, Failure{Code: "lease_required", Detail: "lease missing", HTTPStatus: 403}
	}
	oldExpires := meta.Lease.ExpiresAtUnix
	meta.Lease.ExpiresAtUnix = now.Add(extension).Unix()
	meta.UpdatedAtUnix = now.Unix()
	_, err = s.store.StoreMeta(ctx, namespace, messageRel, meta, metaETag)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return nil, Failure{Code: "lease_conflict", Detail: "lease changed during extend", HTTPStatus: 409}
		}
		return nil, err
	}
	if err := s.updateLeaseIndex(ctx, namespace, messageRel, oldExpires, meta.Lease.ExpiresAtUnix); err != nil && s.logger != nil {
		s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", messageRel, "error", err)
	}
	if checkTxn != "" {
		if _, _, err := s.registerTxnParticipant(ctx, checkTxn, namespace, messageRel, meta.Lease.ExpiresAtUnix); err != nil {
			return nil, fmt.Errorf("register txn participant: %w", err)
		}
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
		stateTxn := ""
		if stateMeta.Lease != nil {
			stateTxn = stateMeta.Lease.TxnID
		}
		if stateMeta.Lease != nil && stateMeta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, stateTxn, now)
			if _, _, err := s.clearExpiredLease(ctx, namespace, stateRel, stateMeta, stateMetaETag, now, false); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					return nil, leaseErr
				}
				return nil, err
			}
			return nil, leaseErr
		}
		if err := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, stateTxn, now); err != nil {
			return nil, err
		}
		if stateMeta.Lease == nil {
			return nil, Failure{Code: "lease_required", Detail: "state lease missing", HTTPStatus: 403}
		}
		stateOldExpires := stateMeta.Lease.ExpiresAtUnix
		stateMeta.Lease.ExpiresAtUnix = now.Add(extension).Unix()
		stateMeta.UpdatedAtUnix = now.Unix()
		newStateETag, err := s.store.StoreMeta(ctx, namespace, stateRel, stateMeta, stateMetaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				return nil, Failure{Code: "lease_conflict", Detail: "state lease changed during extend", HTTPStatus: 409}
			}
			return nil, err
		}
		if err := s.updateLeaseIndex(ctx, namespace, stateRel, stateOldExpires, stateMeta.Lease.ExpiresAtUnix); err != nil && s.logger != nil {
			s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", stateRel, "error", err)
		}
		stateLeaseExpires = stateMeta.Lease.ExpiresAtUnix
		if checkTxn != "" {
			if _, _, err := s.registerTxnParticipant(ctx, checkTxn, namespace, stateRel, stateMeta.Lease.ExpiresAtUnix); err != nil {
				return nil, fmt.Errorf("register txn participant: %w", err)
			}
		}
		_ = newStateETag
	}

	return &QueueExtendResult{
		LeaseExpiresAtUnix:       meta.Lease.ExpiresAtUnix,
		VisibilityTimeoutSeconds: doc.VisibilityTimeout,
		MetaETag:                 newMetaDocETag,
		StateLeaseExpiresAtUnix:  stateLeaseExpires,
		CorrelationID:            corr,
		TxnID:                    leaseTxn,
	}, nil
}
