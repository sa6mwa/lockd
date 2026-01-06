package txncoord

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/lockd/internal/core"
	"pkt.systems/pslog"
)

type txncoordMetrics struct {
	decideDuration metric.Int64Histogram
	fanoutDuration metric.Int64Histogram
	fanoutAttempts metric.Int64Counter
	fanoutFailed   metric.Int64Counter
}

func newTxncoordMetrics(logger pslog.Logger) *txncoordMetrics {
	meter := otel.Meter("pkt.systems/lockd/txncoord")
	m := &txncoordMetrics{}
	var err error

	m.decideDuration, err = meter.Int64Histogram(
		"lockd.txn.tc.decide.duration_ms",
		metric.WithDescription("Time spent recording a TC decision"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.txn.tc.decide.duration_ms", err)

	m.fanoutDuration, err = meter.Int64Histogram(
		"lockd.txn.fanout.duration_ms",
		metric.WithDescription("Time spent fanning out transaction decisions"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.txn.fanout.duration_ms", err)

	m.fanoutAttempts, err = meter.Int64Counter(
		"lockd.txn.fanout.attempts",
		metric.WithDescription("Fan-out apply attempts"),
	)
	logMetricInitError(logger, "lockd.txn.fanout.attempts", err)

	m.fanoutFailed, err = meter.Int64Counter(
		"lockd.txn.fanout.failed",
		metric.WithDescription("Fan-out apply failures"),
	)
	logMetricInitError(logger, "lockd.txn.fanout.failed", err)

	return m
}

func (m *txncoordMetrics) recordDecide(ctx context.Context, state core.TxnState, duration time.Duration) {
	if m == nil || m.decideDuration == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{attribute.String("lockd.txn.state", txnStateLabel(state))}
	m.decideDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
}

func (m *txncoordMetrics) recordFanout(ctx context.Context, state core.TxnState, duration time.Duration, result string) {
	if m == nil || m.fanoutDuration == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.txn.state", txnStateLabel(state)),
		attribute.String("lockd.txn.result", result),
	}
	m.fanoutDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
}

func (m *txncoordMetrics) recordFanoutAttempt(ctx context.Context, state core.TxnState, backendHash string) {
	if m == nil || m.fanoutAttempts == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{attribute.String("lockd.txn.state", txnStateLabel(state))}
	if backendHash != "" {
		attrs = append(attrs, attribute.String("lockd.txn.backend_hash", backendHash))
	}
	m.fanoutAttempts.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (m *txncoordMetrics) recordFanoutFailure(ctx context.Context, state core.TxnState, backendHash, reason string) {
	if m == nil || m.fanoutFailed == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.txn.state", txnStateLabel(state)),
		attribute.String("lockd.txn.fanout_reason", reason),
	}
	if backendHash != "" {
		attrs = append(attrs, attribute.String("lockd.txn.backend_hash", backendHash))
	}
	m.fanoutFailed.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func metricContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func txnStateLabel(state core.TxnState) string {
	if state == "" {
		return "unknown"
	}
	return string(state)
}

func logMetricInitError(logger pslog.Logger, name string, err error) {
	if err == nil || logger == nil {
		return
	}
	logger.Warn("telemetry.metric.init_failed", "name", name, "error", err)
}
