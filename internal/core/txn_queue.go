package core

import (
	"context"
	"errors"
	"fmt"

	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

// applyTxnDecisionQueueMessage applies a commit or rollback to a queue message participant.
// Commit => Ack (delete); Rollback => Nack (make visible again).
func (s *Service) applyTxnDecisionQueueMessage(ctx context.Context, namespace, queueName, messageID, txnID string, commit bool) error {
	qsvc, ok := s.queueProvider.(*queue.Service)
	if !ok || qsvc == nil {
		return nil
	}

	metaKey, err := queue.MessageLeaseKey(namespace, queueName, messageID)
	if err != nil {
		return err
	}
	relKey := relativeKey(namespace, metaKey)

	metaRes, err := s.store.LoadMeta(ctx, namespace, relKey)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	meta := metaRes.Meta
	metaETag := metaRes.ETag

	docRes, err := qsvc.GetMessage(ctx, namespace, queueName, messageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return err
	}
	doc := docRes.Document
	docETag := docRes.ETag

	leaseMissing := doc.LeaseID == "" && doc.LeaseTxnID == "" && doc.LeaseFencingToken == 0
	if leaseMissing {
		if !commit && (meta == nil || meta.Lease == nil) {
			return nil
		}
		return queueMessageLeaseFailure("queue message lease missing")
	}
	if meta == nil || meta.Lease == nil {
		return queueMessageLeaseFailure("queue message lease missing")
	}
	if meta.Lease.TxnID != txnID {
		return queueMessageLeaseFailure("queue message lease mismatch")
	}
	if err := validateQueueMessageLease(doc, meta.Lease.ID, meta.Lease.FencingToken, txnID); err != nil {
		return err
	}

	if commit {
		if err := qsvc.Ack(ctx, namespace, queueName, messageID, docETag, "", false); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				return queueMessageLeaseFailure("queue message lease mismatch")
			}
			return err
		}
		if s.queueDispatcher != nil {
			s.queueDispatcher.Notify(namespace, queueName)
		}
	} else {
		if _, err := qsvc.Nack(ctx, namespace, queueName, doc, docETag, 0, map[string]any{"reason": "txn_rollback"}); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				return queueMessageLeaseFailure("queue message lease mismatch")
			}
			return err
		}
		if s.queueDispatcher != nil {
			s.queueDispatcher.Notify(namespace, queueName)
		}
	}

	// Clear the lease metadata to avoid stranding.
	if meta != nil && meta.Lease != nil {
		meta.Lease = nil
		meta.UpdatedAtUnix = s.clock.Now().Unix()
		if _, err := s.store.StoreMeta(ctx, namespace, relKey, meta, metaETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("clear queue lease meta: %w", err)
		}
	}
	return nil
}

// applyTxnDecisionQueueState applies a decision to a queue state participant.
// Commit => delete state document; Rollback => keep state. Both paths clear the state lease meta.
func (s *Service) applyTxnDecisionQueueState(ctx context.Context, namespace, queueName, messageID, txnID string, commit bool) error {
	qsvc, ok := s.queueProvider.(*queue.Service)
	if !ok || qsvc == nil {
		return nil
	}

	stateKey, err := queue.StateLeaseKey(namespace, queueName, messageID)
	if err != nil {
		return err
	}
	relKey := relativeKey(namespace, stateKey)

	metaRes, err := s.store.LoadMeta(ctx, namespace, relKey)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	meta := metaRes.Meta
	metaETag := metaRes.ETag
	// Skip if the lease belongs to another transaction (including non-XA leases).
	if meta != nil && meta.Lease != nil && meta.Lease.TxnID != txnID {
		return nil
	}

	if commit {
		if err := qsvc.DeleteState(ctx, namespace, queueName, messageID, ""); err != nil {
			if !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		}
	}

	if meta != nil && meta.Lease != nil {
		meta.Lease = nil
		meta.UpdatedAtUnix = s.clock.Now().Unix()
		if _, err := s.store.StoreMeta(ctx, namespace, relKey, meta, metaETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("clear queue state lease meta: %w", err)
		}
	}
	return nil
}
