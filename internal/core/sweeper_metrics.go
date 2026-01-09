package core

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/pslog"
)

type sweeperMetrics struct {
	idleRuns     metric.Int64Counter
	idleDuration metric.Int64Histogram
	idleOps      metric.Int64Histogram
	leaseCleared metric.Int64Counter
}

func newSweeperMetrics(logger pslog.Logger) *sweeperMetrics {
	meter := otel.Meter("pkt.systems/lockd/sweeper")
	m := &sweeperMetrics{}
	var err error

	m.idleRuns, err = meter.Int64Counter(
		"lockd.sweeper.idle.run",
		metric.WithDescription("Idle maintenance sweeper runs"),
	)
	logMetricInitError(logger, "lockd.sweeper.idle.run", err)

	m.idleDuration, err = meter.Int64Histogram(
		"lockd.sweeper.idle.duration_ms",
		metric.WithDescription("Idle maintenance sweeper duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.sweeper.idle.duration_ms", err)

	m.idleOps, err = meter.Int64Histogram(
		"lockd.sweeper.idle.ops",
		metric.WithDescription("Idle maintenance sweeper operations"),
	)
	logMetricInitError(logger, "lockd.sweeper.idle.ops", err)

	m.leaseCleared, err = meter.Int64Counter(
		"lockd.sweep.lease.cleared",
		metric.WithDescription("Expired leases cleared during sweeps"),
	)
	logMetricInitError(logger, "lockd.sweep.lease.cleared", err)

	return m
}

func (m *sweeperMetrics) recordIdle(ctx context.Context, duration time.Duration, ops int, result string) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{attribute.String("lockd.sweeper.result", result)}
	if m.idleRuns != nil {
		m.idleRuns.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.idleDuration != nil {
		m.idleDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
	if m.idleOps != nil {
		m.idleOps.Record(ctx, int64(ops), metric.WithAttributes(attrs...))
	}
}

func (m *sweeperMetrics) recordLeaseCleared(ctx context.Context, mode sweepMode, kind string) {
	if m == nil || m.leaseCleared == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.sweep.mode", string(mode)),
		attribute.String("lockd.lease.kind", kind),
	}
	m.leaseCleared.Add(ctx, 1, metric.WithAttributes(attrs...))
}
