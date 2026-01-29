package core

import (
	"context"
	"errors"
	"path"

	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

// maybeApplyDecisionMarkerForLease applies a recorded txn decision marker to the
// current lease holder for the provided key. It returns true when an apply attempt
// was made.
func (s *Service) maybeApplyDecisionMarkerForLease(ctx context.Context, namespace, key string) (bool, error) {
	if s == nil {
		return false, nil
	}
	applied, err := s.applyDecisionMarkerForLeaseKey(ctx, namespace, key)
	if err != nil || !applied {
		return applied, err
	}
	if parts, ok := queue.ParseMessageLeaseKey(key); ok {
		stateKey := path.Join("q", parts.Queue, "state", parts.ID)
		stateApplied, err := s.applyDecisionMarkerForLeaseKey(ctx, namespace, stateKey)
		if err != nil {
			return applied, err
		}
		return applied || stateApplied, nil
	}
	if parts, ok := queue.ParseStateLeaseKey(key); ok {
		msgKey := path.Join("q", parts.Queue, "msg", parts.ID)
		msgApplied, err := s.applyDecisionMarkerForLeaseKey(ctx, namespace, msgKey)
		if err != nil {
			return applied, err
		}
		return applied || msgApplied, nil
	}
	return applied, nil
}

func (s *Service) applyDecisionMarkerForLeaseKey(ctx context.Context, namespace, key string) (bool, error) {
	res, err := s.store.LoadMeta(ctx, namespace, key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
			return false, nil
		}
		return false, err
	}
	meta := res.Meta
	if meta == nil || meta.Lease == nil || meta.Lease.TxnID == "" {
		return false, nil
	}
	if !txnExplicit(meta) {
		return false, nil
	}
	marker, _, err := s.loadTxnDecisionMarkerWithMode(ctx, meta.Lease.TxnID, sweepModeTransparent)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
			marker = nil
		}
		if err != nil {
			return false, err
		}
	}
	if marker == nil || marker.State == "" || marker.State == TxnStatePending {
		rec, _, recErr := s.loadTxnRecord(ctx, meta.Lease.TxnID)
		if recErr != nil {
			if errors.Is(recErr, storage.ErrNotFound) || errors.Is(recErr, storage.ErrNotImplemented) {
				return false, nil
			}
			return false, recErr
		}
		if rec == nil {
			return false, nil
		}
		if rec.TxnID == "" {
			rec.TxnID = meta.Lease.TxnID
		}
		if rec.State == "" {
			rec.State = TxnStatePending
		}
		if rec.State == TxnStatePending {
			return false, nil
		}
		commit := rec.State == TxnStateCommit
		if err := s.applyTxnDecisionToKey(ctx, namespace, key, meta.Lease.TxnID, commit, true); err != nil {
			if isQueueMessageLeaseMismatch(err) {
				return false, nil
			}
			return false, err
		}
		_ = s.MarkTxnParticipantsApplied(ctx, meta.Lease.TxnID, []TxnParticipant{{Namespace: namespace, Key: key}})
		return true, nil
	}
	commit := marker.State == TxnStateCommit
	if err := s.applyTxnDecisionToKey(ctx, namespace, key, meta.Lease.TxnID, commit, true); err != nil {
		if isQueueMessageLeaseMismatch(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
