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
	now := s.clock.Now().Unix()

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
	if commit && meta != nil && meta.Lease != nil && meta.Lease.ExpiresAtUnix > 0 && meta.Lease.ExpiresAtUnix <= now {
		return queueMessageLeaseFailure("queue message lease expired")
	}

	docRes, err := qsvc.GetMessage(ctx, namespace, queueName, messageID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return err
	}
	doc := docRes.Document
	docETag := docRes.ETag
	if commit && doc.LeaseTxnID != txnID {
		return queueMessageLeaseFailure("queue message lease mismatch")
	}
	if commit && !doc.NotVisibleUntil.IsZero() && s.clock.Now().After(doc.NotVisibleUntil) && doc.LeaseTxnID == txnID {
		return queueMessageLeaseFailure("queue message lease expired")
	}

	leaseMissing := doc.LeaseID == "" && doc.LeaseTxnID == "" && doc.LeaseFencingToken == 0
	if leaseMissing {
		if commit {
			return queueMessageLeaseFailure("queue message lease missing")
		}
		if meta != nil && meta.Lease != nil && meta.Lease.TxnID != txnID {
			return queueMessageLeaseFailure("queue message lease mismatch")
		}
	} else if meta == nil || meta.Lease == nil {
		if commit {
			return queueMessageLeaseFailure("queue message lease missing")
		}
		if doc.LeaseTxnID == "" {
			return nil
		}
		if doc.LeaseTxnID != txnID {
			return queueMessageLeaseFailure("queue message lease mismatch")
		}
	} else {
		if meta.Lease.TxnID != txnID {
			return queueMessageLeaseFailure("queue message lease mismatch")
		}
		if err := validateQueueMessageLease(doc, meta.Lease.ID, meta.Lease.FencingToken, txnID); err != nil {
			return err
		}
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
	if !commit {
		if meta != nil && meta.Lease != nil {
			if meta.Lease.TxnID != txnID {
				return queueMessageLeaseFailure("queue message lease mismatch")
			}
			oldExpires := meta.Lease.ExpiresAtUnix
			meta.Lease = nil
			meta.UpdatedAtUnix = s.clock.Now().Unix()
			if _, err := s.store.StoreMeta(ctx, namespace, relKey, meta, metaETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("clear queue lease meta: %w", err)
			}
			if err := s.updateLeaseIndex(ctx, namespace, relKey, oldExpires, 0); err != nil && s.logger != nil {
				s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", relKey, "error", err)
			}
		}
		return nil
	}

	// Clear the lease metadata to avoid stranding.
	if meta != nil && meta.Lease != nil {
		oldExpires := meta.Lease.ExpiresAtUnix
		meta.Lease = nil
		meta.UpdatedAtUnix = s.clock.Now().Unix()
		if _, err := s.store.StoreMeta(ctx, namespace, relKey, meta, metaETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("clear queue lease meta: %w", err)
		}
		if err := s.updateLeaseIndex(ctx, namespace, relKey, oldExpires, 0); err != nil && s.logger != nil {
			s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", relKey, "error", err)
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
		return queueMessageLeaseFailure("queue state lease mismatch")
	}

	if commit {
		if err := qsvc.DeleteState(ctx, namespace, queueName, messageID, ""); err != nil {
			if !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		}
	}

	if meta != nil && meta.Lease != nil {
		oldExpires := meta.Lease.ExpiresAtUnix
		meta.Lease = nil
		meta.UpdatedAtUnix = s.clock.Now().Unix()
		if _, err := s.store.StoreMeta(ctx, namespace, relKey, meta, metaETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("clear queue state lease meta: %w", err)
		}
		if err := s.updateLeaseIndex(ctx, namespace, relKey, oldExpires, 0); err != nil && s.logger != nil {
			s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", relKey, "error", err)
		}
	}
	return nil
}
