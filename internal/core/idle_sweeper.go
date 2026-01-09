package core

import (
	"context"
	"errors"

	"pkt.systems/lockd/internal/storage"
)

// SweepIdle runs low-impact maintenance sweeping for leases, txn records, and decision markers.
func (s *Service) SweepIdle(ctx context.Context, opts IdleSweepOptions) error {
	if s == nil {
		return nil
	}
	budget := newSweepBudget(s.clock, opts)
	if !budget.allowed() {
		return nil
	}
	if err := s.sweepLeaseIndex(ctx, budget); err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			return nil
		}
		return err
	}
	if !budget.allowed() {
		return nil
	}
	if err := s.sweepTxnRecordsPartial(ctx, budget); err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			return nil
		}
		return err
	}
	if !budget.allowed() {
		return nil
	}
	if err := s.sweepDecisionIndex(ctx, budget); err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			return nil
		}
		return err
	}
	return nil
}
