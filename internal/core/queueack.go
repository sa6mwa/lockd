package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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
	var ackStart time.Time
	if queueDeliveryStatsEnabled() {
		ackStart = time.Now()
		defer func() {
			if !ackStart.IsZero() {
				recordQueueAck(time.Since(ackStart))
			}
		}()
	}
	if err := s.maybeThrottleQueue(ctx, qrf.KindQueueAck); err != nil {
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
	checkTxn := strings.TrimSpace(cmd.TxnID)
	var (
		doc            *queue.MessageDocument
		docETag        string
		useRecord      bool
		cachedMeta     *storage.Meta
		cachedMetaETag string
	)
	if rec, ok := queueLeaseRecordSnapshot(namespace, cmd.Queue, cmd.MessageID); ok {
		ackTxn := checkTxn
		if ackTxn == "" {
			ackTxn = rec.txnID
		}
		if rec.leaseID == cmd.LeaseID && rec.fencingToken == cmd.FencingToken && rec.txnID == ackTxn {
			useRecord = true
			if checkTxn == "" {
				checkTxn = rec.txnID
			}
			if cmd.MetaETag == "" && rec.metaETag != "" {
				cmd.MetaETag = rec.metaETag
			}
			doc = queueLeaseRecordDocument(namespace, cmd.Queue, cmd.MessageID, rec)
			docETag = rec.metaETag
			if rec.leaseMetaSet && rec.leaseMetaETag != "" {
				metaCopy := cloneMeta(rec.leaseMeta)
				cachedMeta = &metaCopy
				cachedMetaETag = rec.leaseMetaETag
			}
		}
	}
	if !useRecord {
		docRes, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
			}
			return nil, err
		}
		doc = docRes.Document
		docETag = docRes.ETag
		if checkTxn == "" {
			checkTxn = doc.LeaseTxnID
		}
		if err := validateQueueMessageLeaseIfPresent(doc, cmd.LeaseID, cmd.FencingToken, checkTxn); err != nil {
			if isQueueMessageLeaseMismatch(err) {
				noteQueueAckLeaseMismatch(namespace, cmd.Queue, cmd.MessageID, cmd.LeaseID, cmd.FencingToken, checkTxn, cmd.MetaETag)
			}
			traceQueueLeaseDocMismatch(s.logger, "queue.lease.ack.doc_mismatch", namespace, cmd.Queue, cmd.MessageID, cmd.LeaseID, cmd.FencingToken, checkTxn, cmd.MetaETag, doc, docETag)
			return nil, err
		}
	}

	corr := ""
	if doc != nil {
		corr = doc.CorrelationID
	}
	if corr == "" {
		corr = correlation.ID(ctx)
	}
	if corr == "" {
		corr = correlation.Generate()
	}
	ctx = correlation.Set(ctx, corr)

	for attempt := 0; attempt < 2; attempt++ {
		plan := s.newWritePlan(ctx)
		commitCtx := plan.Context()

		meta, metaETag, err := s.loadMetaMaybeCached(commitCtx, namespace, messageKey, cachedMeta, cachedMetaETag)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, plan.Wait(Failure{Code: "not_found", Detail: "message lease not found", HTTPStatus: 404})
			}
			return nil, plan.Wait(err)
		}
		now := s.clock.Now()
		leaseTxn := doc.LeaseTxnID
		leaseOwner := ""
		if meta.Lease != nil {
			leaseOwner = meta.Lease.Owner
			if leaseTxn == "" {
				leaseTxn = meta.Lease.TxnID
			}
		}
		if checkTxn == "" {
			checkTxn = leaseTxn
		}
		docLeaseMatch := queueMessageLeaseMatches(doc, cmd.LeaseID, cmd.FencingToken, checkTxn, now)
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now)
			if _, _, err := s.clearExpiredLease(commitCtx, namespace, messageRel, meta, metaETag, now, sweepModeTransparent, false); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					return nil, plan.Wait(leaseErr)
				}
				return nil, plan.Wait(err)
			}
			return nil, plan.Wait(leaseErr)
		}
		if meta.Lease != nil {
			if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now); err != nil {
				if attempt == 0 && docLeaseMatch && isRetryableQueueLeaseMismatch(err) {
					if waitErr := plan.Wait(nil); waitErr != nil {
						return nil, waitErr
					}
					time.Sleep(5 * time.Millisecond)
					continue
				}
				traceQueueLeaseMetaMismatch(s.logger, "queue.lease.ack.meta_mismatch", namespace, cmd.Queue, cmd.MessageID, cmd.LeaseID, cmd.FencingToken, checkTxn, metaETag, meta, doc, docETag, now, docLeaseMatch)
				return nil, plan.Wait(err)
			}
		}

		if checkTxn != "" && s.tcDecider != nil && txnExplicit(meta) {
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
			state, err := s.tcDecider.Decide(commitCtx, rec)
			if err != nil {
				return nil, plan.Wait(err)
			}
			if state == TxnStatePending {
				return nil, plan.Wait(Failure{Code: "txn_pending", Detail: "transaction decision not recorded", HTTPStatus: 409})
			}
			if waitErr := plan.Wait(nil); waitErr != nil {
				return nil, waitErr
			}
			return &QueueAckResult{
				Acked:         true,
				CorrelationID: corr,
				LeaseOwner:    leaseOwner,
				TxnID:         leaseTxn,
			}, nil
		}

		if err := qsvc.Ack(commitCtx, namespace, cmd.Queue, cmd.MessageID, cmd.MetaETag, cmd.StateETag, cmd.Stateful); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				if attempt == 0 {
					docRes, reloadErr := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
					if reloadErr != nil {
						if errors.Is(reloadErr, storage.ErrNotFound) {
							return nil, plan.Wait(Failure{Code: "not_found", Detail: "message already removed", HTTPStatus: 404})
						}
						return nil, plan.Wait(reloadErr)
					}
					doc = docRes.Document
					docETag = docRes.ETag
					if queueMessageLeaseMatches(doc, cmd.LeaseID, cmd.FencingToken, checkTxn, s.clock.Now()) {
						cmd.MetaETag = docRes.ETag
						if waitErr := plan.Wait(nil); waitErr != nil {
							return nil, waitErr
						}
						continue
					}
				}
				return nil, plan.Wait(Failure{Code: "cas_mismatch", Detail: "message metadata changed", HTTPStatus: 409})
			}
			if errors.Is(err, storage.ErrNotFound) {
				return nil, plan.Wait(Failure{Code: "not_found", Detail: "message already removed", HTTPStatus: 404})
			}
			return nil, plan.Wait(err)
		}

		if metaETag != "" {
			_ = s.releaseLeaseWithMeta(commitCtx, namespace, messageRel, cmd.LeaseID, meta, metaETag)
		}

		if cmd.StateLeaseID != "" {
			stateKey, err := queue.StateLeaseKey(namespace, cmd.Queue, cmd.MessageID)
			if err == nil {
				stateRel := relativeKey(namespace, stateKey)
				stateMeta, stateMetaETag, loadErr := s.ensureMeta(commitCtx, namespace, stateKey)
				if loadErr == nil && stateMeta.Lease != nil {
					stateTxn := stateMeta.Lease.TxnID
					now := s.clock.Now()
					if stateMeta.Lease.ExpiresAtUnix <= now.Unix() {
						_, _, _ = s.clearExpiredLease(commitCtx, namespace, stateRel, stateMeta, stateMetaETag, now, sweepModeTransparent, false)
					} else if err := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, stateTxn, now); err == nil {
						// Ack finalizes processing, so any staged queue-state changes are discarded.
						if stateTxn != "" {
							_ = s.applyTxnDecisionForMeta(commitCtx, namespace, stateRel, stateTxn, false, stateMeta, stateMetaETag)
						} else {
							_ = s.releaseLeaseWithMeta(commitCtx, namespace, stateRel, cmd.StateLeaseID, stateMeta, stateMetaETag)
						}
					}
				}
			}
		}

		if waitErr := plan.Wait(nil); waitErr != nil {
			return nil, waitErr
		}

		if s.queueDispatcher != nil {
			s.queueDispatcher.CancelNotify(namespace, cmd.Queue, cmd.MessageID)
			s.queueDispatcher.Notify(namespace, cmd.Queue)
		}

		return &QueueAckResult{
			Acked:         true,
			CorrelationID: corr,
			LeaseOwner:    leaseOwner,
			TxnID:         leaseTxn,
		}, nil
	}

	return nil, Failure{Code: "queue_ack_retry_exceeded", Detail: "queue ack retry attempts exhausted", HTTPStatus: 500}
}

func isRetryableQueueLeaseMismatch(err error) bool {
	var failure Failure
	if !errors.As(err, &failure) {
		return false
	}
	switch failure.Code {
	case "lease_required", "fencing_mismatch", "txn_mismatch":
		return true
	default:
		return false
	}
}

// Nack requeues a delivery with optional delay and last error.
func (s *Service) Nack(ctx context.Context, cmd QueueNackCommand) (*QueueNackResult, error) {
	if err := s.maybeThrottleQueue(ctx, qrf.KindQueueAck); err != nil {
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
	docRes, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
		}
		return nil, err
	}
	doc := docRes.Document
	docETag := docRes.ETag
	checkTxn := strings.TrimSpace(cmd.TxnID)
	if checkTxn == "" {
		checkTxn = doc.LeaseTxnID
	}
	if err := validateQueueMessageLeaseIfPresent(doc, cmd.LeaseID, cmd.FencingToken, checkTxn); err != nil {
		traceQueueLeaseDocMismatch(s.logger, "queue.lease.nack.doc_mismatch", namespace, cmd.Queue, cmd.MessageID, cmd.LeaseID, cmd.FencingToken, checkTxn, cmd.MetaETag, doc, docETag)
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

	plan := s.newWritePlan(ctx)
	commitCtx := plan.Context()

	meta, metaETag, err := s.ensureMeta(commitCtx, namespace, messageKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, plan.Wait(Failure{Code: "not_found", Detail: "message lease not found", HTTPStatus: 404})
		}
		return nil, plan.Wait(err)
	}
	leaseTxn := doc.LeaseTxnID
	if meta.Lease != nil && leaseTxn == "" {
		leaseTxn = meta.Lease.TxnID
	}
	if checkTxn == "" {
		checkTxn = leaseTxn
	}
	now := s.clock.Now()
	docLeaseMatch := queueMessageLeaseMatches(doc, cmd.LeaseID, cmd.FencingToken, checkTxn, now)
	if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
		leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now)
		if _, _, err := s.clearExpiredLease(commitCtx, namespace, messageRel, meta, metaETag, now, sweepModeTransparent, false); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				return nil, plan.Wait(leaseErr)
			}
			return nil, plan.Wait(err)
		}
		return nil, plan.Wait(leaseErr)
	}
	if meta.Lease != nil {
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now); err != nil {
			traceQueueLeaseMetaMismatch(s.logger, "queue.lease.nack.meta_mismatch", namespace, cmd.Queue, cmd.MessageID, cmd.LeaseID, cmd.FencingToken, checkTxn, metaETag, meta, doc, docETag, now, docLeaseMatch)
			return nil, plan.Wait(err)
		}
	}

	if checkTxn != "" && s.tcDecider != nil && txnExplicit(meta) {
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
		state, err := s.tcDecider.Decide(commitCtx, rec)
		if err != nil {
			return nil, plan.Wait(err)
		}
		if state == TxnStatePending {
			return nil, plan.Wait(Failure{Code: "txn_pending", Detail: "transaction decision not recorded", HTTPStatus: 409})
		}
		metaETag := cmd.MetaETag
		if cmd.Delay > 0 {
			docRes, err := qsvc.GetMessage(commitCtx, namespace, cmd.Queue, cmd.MessageID)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return nil, plan.Wait(Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404})
				}
				return nil, plan.Wait(err)
			}
			newETag, err := qsvc.Reschedule(commitCtx, namespace, cmd.Queue, docRes.Document, docRes.ETag, cmd.Delay)
			if err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					return nil, plan.Wait(Failure{Code: "cas_mismatch", Detail: "message metadata changed", HTTPStatus: 409})
				}
				return nil, plan.Wait(err)
			}
			metaETag = newETag
			if s.queueDispatcher != nil && docRes.Document != nil {
				s.queueDispatcher.NotifyAt(namespace, cmd.Queue, docRes.Document.ID, docRes.Document.NotVisibleUntil)
			}
		}
		if waitErr := plan.Wait(nil); waitErr != nil {
			return nil, waitErr
		}
		return &QueueNackResult{
			Requeued:      true,
			MetaETag:      metaETag,
			CorrelationID: corr,
			TxnID:         leaseTxn,
		}, nil
	}

	delay := cmd.Delay
	newMetaETag, err := qsvc.Nack(commitCtx, namespace, cmd.Queue, doc, cmd.MetaETag, delay, cmd.LastError)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return nil, plan.Wait(Failure{Code: "cas_mismatch", Detail: "message metadata changed", HTTPStatus: 409})
		}
		return nil, plan.Wait(err)
	}

	if metaETag != "" {
		_ = s.releaseLeaseWithMeta(commitCtx, namespace, messageRel, cmd.LeaseID, meta, metaETag)
	}
	if cmd.StateLeaseID != "" {
		stateKey, err := queue.StateLeaseKey(namespace, cmd.Queue, cmd.MessageID)
		if err == nil {
			stateRel := relativeKey(namespace, stateKey)
			stateMeta, stateMetaETag, loadErr := s.ensureMeta(commitCtx, namespace, stateKey)
			if loadErr == nil && stateMeta.Lease != nil {
				stateTxn := stateMeta.Lease.TxnID
				now := s.clock.Now()
				if stateMeta.Lease.ExpiresAtUnix <= now.Unix() {
					_, _, _ = s.clearExpiredLease(commitCtx, namespace, stateRel, stateMeta, stateMetaETag, now, sweepModeTransparent, false)
				} else if err := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, stateTxn, now); err == nil {
					// Nack should preserve queue-state progress across retries.
					if stateTxn != "" {
						_ = s.applyTxnDecisionForMeta(commitCtx, namespace, stateRel, stateTxn, true, stateMeta, stateMetaETag)
					} else {
						_ = s.releaseLeaseWithMeta(commitCtx, namespace, stateRel, cmd.StateLeaseID, stateMeta, stateMetaETag)
					}
				}
			}
		}
	}

	if waitErr := plan.Wait(nil); waitErr != nil {
		return nil, waitErr
	}

	if s.queueDispatcher != nil {
		s.queueDispatcher.NotifyAt(namespace, cmd.Queue, doc.ID, doc.NotVisibleUntil)
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
	if err := s.maybeThrottleQueue(ctx, qrf.KindQueueAck); err != nil {
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
	docRes, err := qsvc.GetMessage(ctx, namespace, cmd.Queue, cmd.MessageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "not_found", Detail: "message not found", HTTPStatus: 404}
		}
		return nil, err
	}
	doc := docRes.Document
	docETag := docRes.ETag
	checkTxn := strings.TrimSpace(cmd.TxnID)
	if checkTxn == "" {
		checkTxn = doc.LeaseTxnID
	}
	if err := validateQueueMessageLeaseIfPresent(doc, cmd.LeaseID, cmd.FencingToken, checkTxn); err != nil {
		traceQueueLeaseDocMismatch(s.logger, "queue.lease.extend.doc_mismatch", namespace, cmd.Queue, cmd.MessageID, cmd.LeaseID, cmd.FencingToken, checkTxn, cmd.MetaETag, doc, docETag)
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

	plan := s.newWritePlan(ctx)
	commitCtx := plan.Context()

	meta, metaETag, err := s.ensureMeta(commitCtx, namespace, messageKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, plan.Wait(Failure{Code: "not_found", Detail: "message lease not found", HTTPStatus: 404})
		}
		return nil, plan.Wait(err)
	}
	leaseTxn := doc.LeaseTxnID
	if meta.Lease != nil && leaseTxn == "" {
		leaseTxn = meta.Lease.TxnID
	}
	if checkTxn == "" {
		checkTxn = leaseTxn
	}
	now := s.clock.Now()
	docLeaseMatch := queueMessageLeaseMatches(doc, cmd.LeaseID, cmd.FencingToken, checkTxn, now)
	if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
		leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now)
		if _, _, err := s.clearExpiredLease(commitCtx, namespace, messageRel, meta, metaETag, now, sweepModeTransparent, false); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				return nil, plan.Wait(leaseErr)
			}
			return nil, plan.Wait(err)
		}
		return nil, plan.Wait(leaseErr)
	}
	if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, checkTxn, now); err != nil {
		traceQueueLeaseMetaMismatch(s.logger, "queue.lease.extend.meta_mismatch", namespace, cmd.Queue, cmd.MessageID, cmd.LeaseID, cmd.FencingToken, checkTxn, metaETag, meta, doc, docETag, now, docLeaseMatch)
		return nil, plan.Wait(err)
	}

	extension := cmd.Visibility
	newMetaDocETag, err := qsvc.ExtendVisibility(commitCtx, namespace, cmd.Queue, doc, cmd.MetaETag, extension)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return nil, plan.Wait(Failure{Code: "cas_mismatch", Detail: "message metadata changed", HTTPStatus: 409})
		}
		return nil, plan.Wait(err)
	}

	now = s.clock.Now()
	if meta.Lease == nil {
		return nil, Failure{Code: "lease_required", Detail: "lease missing", HTTPStatus: 403}
	}
	oldExpires := meta.Lease.ExpiresAtUnix
	meta.Lease.ExpiresAtUnix = now.Add(extension).Unix()
	meta.UpdatedAtUnix = now.Unix()
	_, err = s.store.StoreMeta(commitCtx, namespace, messageRel, meta, metaETag)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return nil, plan.Wait(Failure{Code: "lease_conflict", Detail: "lease changed during extend", HTTPStatus: 409})
		}
		return nil, plan.Wait(err)
	}
	if err := s.updateLeaseIndex(ctx, namespace, messageRel, oldExpires, meta.Lease.ExpiresAtUnix); err != nil && s.logger != nil {
		s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", messageRel, "error", err)
	}
	if checkTxn != "" && txnExplicit(meta) {
		if _, _, err := s.registerTxnParticipant(commitCtx, checkTxn, namespace, messageRel, meta.Lease.ExpiresAtUnix); err != nil {
			return nil, plan.Wait(fmt.Errorf("register txn participant: %w", err))
		}
	}

	stateLeaseExpires := int64(0)
	if cmd.StateLeaseID != "" {
		stateKey, err := queue.StateLeaseKey(namespace, cmd.Queue, cmd.MessageID)
		if err != nil {
			return nil, plan.Wait(Failure{Code: "invalid_queue_state_key", Detail: err.Error(), HTTPStatus: 400})
		}
		stateRel := relativeKey(namespace, stateKey)
		stateMeta, stateMetaETag, err := s.ensureMeta(commitCtx, namespace, stateKey)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, plan.Wait(Failure{Code: "state_not_found", Detail: "state lease missing", HTTPStatus: 404})
			}
			return nil, plan.Wait(err)
		}
		stateTxn := ""
		if stateMeta.Lease != nil {
			stateTxn = stateMeta.Lease.TxnID
		}
		if stateMeta.Lease != nil && stateMeta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, stateTxn, now)
			if _, _, err := s.clearExpiredLease(commitCtx, namespace, stateRel, stateMeta, stateMetaETag, now, sweepModeTransparent, false); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					return nil, plan.Wait(leaseErr)
				}
				return nil, plan.Wait(err)
			}
			return nil, plan.Wait(leaseErr)
		}
		if err := validateLease(stateMeta, cmd.StateLeaseID, cmd.StateFencingToken, stateTxn, now); err != nil {
			return nil, plan.Wait(err)
		}
		if stateMeta.Lease == nil {
			return nil, plan.Wait(Failure{Code: "lease_required", Detail: "state lease missing", HTTPStatus: 403})
		}
		stateOldExpires := stateMeta.Lease.ExpiresAtUnix
		stateMeta.Lease.ExpiresAtUnix = now.Add(extension).Unix()
		stateMeta.UpdatedAtUnix = now.Unix()
		newStateETag, err := s.store.StoreMeta(commitCtx, namespace, stateRel, stateMeta, stateMetaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				return nil, plan.Wait(Failure{Code: "lease_conflict", Detail: "state lease changed during extend", HTTPStatus: 409})
			}
			return nil, plan.Wait(err)
		}
		if err := s.updateLeaseIndex(ctx, namespace, stateRel, stateOldExpires, stateMeta.Lease.ExpiresAtUnix); err != nil && s.logger != nil {
			s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", stateRel, "error", err)
		}
		stateLeaseExpires = stateMeta.Lease.ExpiresAtUnix
		if checkTxn != "" && txnExplicit(stateMeta) {
			if _, _, err := s.registerTxnParticipant(commitCtx, checkTxn, namespace, stateRel, stateMeta.Lease.ExpiresAtUnix); err != nil {
				return nil, plan.Wait(fmt.Errorf("register txn participant: %w", err))
			}
		}
		_ = newStateETag
	}

	if waitErr := plan.Wait(nil); waitErr != nil {
		return nil, waitErr
	}

	if s.queueDispatcher != nil {
		s.queueDispatcher.NotifyAt(namespace, cmd.Queue, doc.ID, doc.NotVisibleUntil)
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
