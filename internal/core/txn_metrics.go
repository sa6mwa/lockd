package core

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/pslog"
)

type txnMetrics struct {
	decisionsRecorded     metric.Int64Counter
	decisionDuration      metric.Int64Histogram
	applyApplied          metric.Int64Counter
	applyFailed           metric.Int64Counter
	applyRetries          metric.Int64Counter
	applyDuration         metric.Int64Histogram
	replayDuration        metric.Int64Histogram
	sweepDuration         metric.Int64Histogram
	sweepApplied          metric.Int64Counter
	sweepFailed           metric.Int64Counter
	sweepPendingExpired   metric.Int64Counter
	sweepCleanupDeleted   metric.Int64Counter
	sweepCleanupFailed    metric.Int64Counter
	sweepLoadErrors       metric.Int64Counter
	sweepRollbackCASFail  metric.Int64Counter
	decisionMarkerDeleted metric.Int64Counter
	decisionMarkerFailed  metric.Int64Counter
}

func newTxnMetrics(logger pslog.Logger) *txnMetrics {
	meter := otel.Meter("pkt.systems/lockd/txn")
	m := &txnMetrics{}
	var err error

	m.decisionsRecorded, err = meter.Int64Counter(
		"lockd.txn.decisions.recorded",
		metric.WithDescription("Transaction decisions recorded"),
	)
	logMetricInitError(logger, "lockd.txn.decisions.recorded", err)

	m.decisionDuration, err = meter.Int64Histogram(
		"lockd.txn.decide.duration_ms",
		metric.WithDescription("Time spent recording a transaction decision"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.txn.decide.duration_ms", err)

	m.applyApplied, err = meter.Int64Counter(
		"lockd.txn.apply.applied",
		metric.WithDescription("Transaction participants applied successfully"),
	)
	logMetricInitError(logger, "lockd.txn.apply.applied", err)

	m.applyFailed, err = meter.Int64Counter(
		"lockd.txn.apply.failed",
		metric.WithDescription("Transaction participants that failed to apply"),
	)
	logMetricInitError(logger, "lockd.txn.apply.failed", err)

	m.applyRetries, err = meter.Int64Counter(
		"lockd.txn.apply.retries",
		metric.WithDescription("Retry attempts while applying transaction participants"),
	)
	logMetricInitError(logger, "lockd.txn.apply.retries", err)

	m.applyDuration, err = meter.Int64Histogram(
		"lockd.txn.apply.duration_ms",
		metric.WithDescription("Time spent applying a transaction decision"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.txn.apply.duration_ms", err)

	m.replayDuration, err = meter.Int64Histogram(
		"lockd.txn.replay.duration_ms",
		metric.WithDescription("Time spent replaying a transaction decision"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.txn.replay.duration_ms", err)

	m.sweepDuration, err = meter.Int64Histogram(
		"lockd.txn.sweep.duration_ms",
		metric.WithDescription("Time spent sweeping transaction records"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.txn.sweep.duration_ms", err)

	m.sweepApplied, err = meter.Int64Counter(
		"lockd.txn.sweep.applied",
		metric.WithDescription("Sweeper-applied transaction records"),
	)
	logMetricInitError(logger, "lockd.txn.sweep.applied", err)

	m.sweepFailed, err = meter.Int64Counter(
		"lockd.txn.sweep.failed",
		metric.WithDescription("Sweeper-applied transaction records that failed"),
	)
	logMetricInitError(logger, "lockd.txn.sweep.failed", err)

	m.sweepPendingExpired, err = meter.Int64Counter(
		"lockd.txn.sweep.pending_expired",
		metric.WithDescription("Pending transaction records expired and rolled back"),
	)
	logMetricInitError(logger, "lockd.txn.sweep.pending_expired", err)

	m.sweepCleanupDeleted, err = meter.Int64Counter(
		"lockd.txn.sweep.cleanup_deleted",
		metric.WithDescription("Expired transaction records cleaned up"),
	)
	logMetricInitError(logger, "lockd.txn.sweep.cleanup_deleted", err)

	m.sweepCleanupFailed, err = meter.Int64Counter(
		"lockd.txn.sweep.cleanup_failed",
		metric.WithDescription("Failed cleanup attempts during transaction sweeping"),
	)
	logMetricInitError(logger, "lockd.txn.sweep.cleanup_failed", err)

	m.sweepLoadErrors, err = meter.Int64Counter(
		"lockd.txn.sweep.load_errors",
		metric.WithDescription("Transaction record load errors during sweeping"),
	)
	logMetricInitError(logger, "lockd.txn.sweep.load_errors", err)

	m.sweepRollbackCASFail, err = meter.Int64Counter(
		"lockd.txn.sweep.rollback_cas_failed",
		metric.WithDescription("CAS failures while updating expired pending transactions"),
	)
	logMetricInitError(logger, "lockd.txn.sweep.rollback_cas_failed", err)

	m.decisionMarkerDeleted, err = meter.Int64Counter(
		"lockd.txn.decision.marker.deleted",
		metric.WithDescription("Transaction decision markers deleted"),
	)
	logMetricInitError(logger, "lockd.txn.decision.marker.deleted", err)

	m.decisionMarkerFailed, err = meter.Int64Counter(
		"lockd.txn.decision.marker.failed",
		metric.WithDescription("Failed transaction decision marker deletions"),
	)
	logMetricInitError(logger, "lockd.txn.decision.marker.failed", err)

	return m
}

func (m *txnMetrics) recordDecision(ctx context.Context, state TxnState, duration time.Duration) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{attribute.String("lockd.txn.state", txnStateLabel(state))}
	if m.decisionsRecorded != nil {
		m.decisionsRecorded.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.decisionDuration != nil {
		m.decisionDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *txnMetrics) recordApply(ctx context.Context, state TxnState, appliedCount, failedCount, retryCount int, duration time.Duration) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{attribute.String("lockd.txn.state", txnStateLabel(state))}
	if appliedCount > 0 && m.applyApplied != nil {
		m.applyApplied.Add(ctx, int64(appliedCount), metric.WithAttributes(attrs...))
	}
	if failedCount > 0 && m.applyFailed != nil {
		m.applyFailed.Add(ctx, int64(failedCount), metric.WithAttributes(attrs...))
	}
	if retryCount > 0 && m.applyRetries != nil {
		m.applyRetries.Add(ctx, int64(retryCount), metric.WithAttributes(attrs...))
	}
	if m.applyDuration != nil {
		m.applyDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *txnMetrics) recordReplay(ctx context.Context, state TxnState, duration time.Duration) {
	if m == nil || m.replayDuration == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{attribute.String("lockd.txn.state", txnStateLabel(state))}
	m.replayDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
}

func (m *txnMetrics) recordSweep(ctx context.Context, mode sweepMode, duration time.Duration, applied, failed, pendingExpired, cleanupDeleted, cleanupFailed, loadErrors, rollbackCASFailed int) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{attribute.String("lockd.txn.sweep_mode", sweepModeLabel(mode))}
	if applied > 0 && m.sweepApplied != nil {
		m.sweepApplied.Add(ctx, int64(applied), metric.WithAttributes(attrs...))
	}
	if failed > 0 && m.sweepFailed != nil {
		m.sweepFailed.Add(ctx, int64(failed), metric.WithAttributes(attrs...))
	}
	if pendingExpired > 0 && m.sweepPendingExpired != nil {
		m.sweepPendingExpired.Add(ctx, int64(pendingExpired), metric.WithAttributes(attrs...))
	}
	if cleanupDeleted > 0 && m.sweepCleanupDeleted != nil {
		m.sweepCleanupDeleted.Add(ctx, int64(cleanupDeleted), metric.WithAttributes(attrs...))
	}
	if cleanupFailed > 0 && m.sweepCleanupFailed != nil {
		m.sweepCleanupFailed.Add(ctx, int64(cleanupFailed), metric.WithAttributes(attrs...))
	}
	if loadErrors > 0 && m.sweepLoadErrors != nil {
		m.sweepLoadErrors.Add(ctx, int64(loadErrors), metric.WithAttributes(attrs...))
	}
	if rollbackCASFailed > 0 && m.sweepRollbackCASFail != nil {
		m.sweepRollbackCASFail.Add(ctx, int64(rollbackCASFailed), metric.WithAttributes(attrs...))
	}
	if m.sweepDuration != nil {
		m.sweepDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *txnMetrics) recordDecisionMarkerSweep(ctx context.Context, mode sweepMode, deleted, failed int) {
	if m == nil {
		return
	}
	if deleted <= 0 && failed <= 0 {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{attribute.String("lockd.txn.sweep_mode", sweepModeLabel(mode))}
	if deleted > 0 && m.decisionMarkerDeleted != nil {
		m.decisionMarkerDeleted.Add(ctx, int64(deleted), metric.WithAttributes(attrs...))
	}
	if failed > 0 && m.decisionMarkerFailed != nil {
		m.decisionMarkerFailed.Add(ctx, int64(failed), metric.WithAttributes(attrs...))
	}
}

func metricContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func txnStateLabel(state TxnState) string {
	if state == "" {
		return "unknown"
	}
	return string(state)
}

func sweepModeLabel(mode sweepMode) string {
	if mode == "" {
		return "unknown"
	}
	return string(mode)
}

func logMetricInitError(logger pslog.Logger, name string, err error) {
	if err == nil || logger == nil {
		return
	}
	logger.Warn("telemetry.metric.init_failed", "name", name, "error", err)
}
