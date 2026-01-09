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
	start := s.clock.Now()
	result := ""
	defer func() {
		if s.sweeperMetrics == nil {
			return
		}
		if result == "" {
			result = budget.stopResult()
		}
		s.sweeperMetrics.recordIdle(ctx, s.clock.Now().Sub(start), budget.opsCount(), result)
	}()
	if err := s.sweepLeaseIndex(ctx, budget); err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			return nil
		}
		result = "error"
		return err
	}
	if !budget.allowed() {
		return nil
	}
	if err := s.sweepTxnRecordsPartial(ctx, budget, sweepModeIdle); err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			return nil
		}
		result = "error"
		return err
	}
	if !budget.allowed() {
		return nil
	}
	if _, _, err := s.sweepDecisionIndex(ctx, budget, sweepModeIdle); err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			return nil
		}
		result = "error"
		return err
	}
	return nil
}
