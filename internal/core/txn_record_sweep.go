package core

import (
	"context"
	"errors"
	"strings"
	"time"

	"pkt.systems/lockd/internal/storage"
)

const (
	txnReplayImmediateThreshold = 5 * time.Second
	txnReplayMaxOps             = 128
	txnReplayAsyncMaxRuntime    = 2 * time.Second
	txnReplayInlineMaxRuntime   = 250 * time.Millisecond
)

func (s *Service) sweepTxnRecordsPartial(ctx context.Context, budget *sweepBudget, mode sweepMode) error {
	if s == nil || budget == nil {
		return nil
	}
	start := budget.clock.Now()
	limit := remainingOps(budget)
	if limit <= 0 {
		return nil
	}
	cursor := &s.txnSweepCursor
	cursor.mu.Lock()
	startAfter := cursor.startAfter
	cursor.mu.Unlock()

	list, err := s.store.ListObjects(ctx, txnNamespace, storage.ListOptions{StartAfter: startAfter, Limit: limit})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
			return nil
		}
		return err
	}

	var (
		totalCount        int
		pendingSkipped    int
		pendingExpired    int
		applySuccess      int
		applyFailed       int
		loadErrors        int
		cleanupDeleted    int
		cleanupFailed     int
		rollbackCASFailed int
	)
	now := budget.clock.Now()
	nowUnix := now.Unix()
	idleMode := mode == sweepModeIdle
	applyOpts := txnApplyOptions{
		tolerateQueueLeaseMismatch: true,
		deferFinalize:              idleMode,
	}

	for _, obj := range list.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !budget.allowed() {
			break
		}
		totalCount++
		rec, etag, err := s.loadTxnRecord(ctx, obj.Key)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if errors.Is(err, storage.ErrNotFound) {
				goto next
			}
			if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			loadErrors++
			goto next
		}
		if rec.TxnID == "" {
			rec.TxnID = strings.TrimSpace(obj.Key)
		}
		if rec.State == "" {
			rec.State = TxnStatePending
		}
		if rec.State == TxnStatePending && rec.ExpiresAtUnix > 0 && rec.ExpiresAtUnix <= nowUnix {
			rec.State = TxnStateRollback
			pendingExpired++
			if s.txnDecisionRetention > 0 {
				rec.ExpiresAtUnix = nowUnix + int64(s.txnDecisionRetention/time.Second)
			}
			rec.UpdatedAtUnix = nowUnix
			if _, err := s.putTxnRecord(ctx, rec, etag); err != nil {
				if errors.Is(err, storage.ErrNotImplemented) {
					// proceed without persisting updated state
				} else if errors.Is(err, storage.ErrCASMismatch) {
					rollbackCASFailed++
					continue
				} else {
					return err
				}
			}
		}
		if rec.State == TxnStatePending {
			pendingSkipped++
			goto next
		}
		if allParticipantsApplied(rec.Participants) {
			if idleMode {
				pendingSkipped++
				goto next
			}
			if err := s.writeDecisionMarker(ctx, rec); err != nil {
				applyFailed++
				goto next
			}
			if err := s.store.DeleteObject(ctx, txnNamespace, rec.TxnID, storage.DeleteObjectOptions{}); err != nil {
				if !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, storage.ErrNotImplemented) {
					cleanupFailed++
				}
			} else {
				cleanupDeleted++
			}
			goto next
		}
		if err := s.applyTxnDecisionWithOptions(ctx, rec, applyOpts); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			applyFailed++
			goto next
		}
		applySuccess++
		if !idleMode && s.txnDecisionRetention > 0 && rec.ExpiresAtUnix > 0 && rec.ExpiresAtUnix <= nowUnix {
			if err := s.store.DeleteObject(ctx, txnNamespace, rec.TxnID, storage.DeleteObjectOptions{}); err != nil {
				if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
					continue
				}
				cleanupFailed++
			} else {
				cleanupDeleted++
			}
		}
	next:
		if !budget.consume() {
			break
		}
	}

	if list.Truncated {
		cursor.mu.Lock()
		cursor.startAfter = list.NextStartAfter
		cursor.mu.Unlock()
	} else {
		cursor.mu.Lock()
		cursor.startAfter = ""
		cursor.mu.Unlock()
	}
	if s.txnMetrics != nil {
		duration := budget.clock.Now().Sub(start)
		s.txnMetrics.recordSweep(ctx, mode, duration, applySuccess, applyFailed, pendingExpired, cleanupDeleted, cleanupFailed, loadErrors, rollbackCASFailed)
	}
	return nil
}

func (s *Service) maybeReplayTxnRecords(ctx context.Context) {
	if s == nil {
		return
	}
	interval := s.txnReplayInterval
	if interval <= 0 {
		return
	}
	now := s.clock.Now()
	last := s.txnReplayLast.Load()
	if last == 0 && interval > txnReplayImmediateThreshold {
		s.txnReplayLast.Store(now.UnixNano())
		return
	}
	if last > 0 && now.Sub(time.Unix(0, last)) < interval {
		return
	}
	if !s.txnReplayRunning.CompareAndSwap(false, true) {
		return
	}
	previousLast := last
	s.txnReplayLast.Store(now.UnixNano())
	go func(start time.Time) {
		defer s.txnReplayRunning.Store(false)
		maxRuntime := txnReplayAsyncMaxRuntime
		sweepCtx, cancel := context.WithTimeout(context.Background(), maxRuntime)
		defer cancel()
		if err := s.replayTxnRecords(sweepCtx, start, maxRuntime); err != nil {
			if errors.Is(err, storage.ErrNotImplemented) {
				return
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				s.txnReplayLast.Store(previousLast)
				return
			}
			if s.logger != nil {
				s.logger.Warn("txn.replay.sweep_failed", "error", err)
			}
		}
	}(now)
}

func (s *Service) maybeReplayTxnRecordsInline(ctx context.Context) bool {
	if s == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return false
	}
	interval := s.txnReplayInterval
	if interval <= 0 {
		return false
	}
	now := s.clock.Now()
	last := s.txnReplayLast.Load()
	if last == 0 && interval > txnReplayImmediateThreshold {
		s.txnReplayLast.Store(now.UnixNano())
		return false
	}
	if last > 0 && now.Sub(time.Unix(0, last)) < interval {
		return false
	}
	if !s.txnReplayRunning.CompareAndSwap(false, true) {
		return false
	}
	previousLast := last
	s.txnReplayLast.Store(now.UnixNano())
	defer s.txnReplayRunning.Store(false)

	maxRuntime := txnReplayInlineMaxRuntime
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		if remaining < maxRuntime {
			maxRuntime = remaining
		}
	}
	if maxRuntime <= 0 {
		return false
	}
	sweepCtx, cancel := context.WithTimeout(ctx, maxRuntime)
	defer cancel()
	if err := s.replayTxnRecords(sweepCtx, now, maxRuntime); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Allow immediate async replay when inline budget is exhausted.
			s.txnReplayLast.Store(previousLast)
			return false
		}
		if errors.Is(err, storage.ErrNotImplemented) {
			return false
		}
		if s.logger != nil {
			s.logger.Warn("txn.replay.sweep_failed", "error", err)
		}
	}
	return true
}

func (s *Service) replayTxnRecords(ctx context.Context, start time.Time, maxRuntime time.Duration) error {
	budget := newSweepBudget(s.clock, IdleSweepOptions{
		Now:        start,
		MaxOps:     txnReplayMaxOps,
		MaxRuntime: maxRuntime,
	})
	return s.sweepTxnRecordsPartial(ctx, budget, sweepModeReplay)
}
