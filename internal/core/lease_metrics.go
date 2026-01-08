package core

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/pslog"
)

type leaseMetrics struct {
	acquireCount    metric.Int64Counter
	acquireDuration metric.Int64Histogram
	keepaliveCount  metric.Int64Counter
	keepaliveDur    metric.Int64Histogram
	releaseCount    metric.Int64Counter
	releaseDur      metric.Int64Histogram
	activeGauge     metric.Int64ObservableGauge
	activeLock      atomic.Int64
	activeMsg       atomic.Int64
	activeState     atomic.Int64
}

func newLeaseMetrics(logger pslog.Logger) *leaseMetrics {
	meter := otel.Meter("pkt.systems/lockd/lease")
	m := &leaseMetrics{}
	var err error

	m.acquireCount, err = meter.Int64Counter(
		"lockd.lease.acquire",
		metric.WithDescription("Lease acquisitions"),
	)
	logMetricInitError(logger, "lockd.lease.acquire", err)

	m.acquireDuration, err = meter.Int64Histogram(
		"lockd.lease.acquire.duration_ms",
		metric.WithDescription("Lease acquire duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.lease.acquire.duration_ms", err)

	m.keepaliveCount, err = meter.Int64Counter(
		"lockd.lease.keepalive",
		metric.WithDescription("Lease keepalive operations"),
	)
	logMetricInitError(logger, "lockd.lease.keepalive", err)

	m.keepaliveDur, err = meter.Int64Histogram(
		"lockd.lease.keepalive.duration_ms",
		metric.WithDescription("Lease keepalive duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.lease.keepalive.duration_ms", err)

	m.releaseCount, err = meter.Int64Counter(
		"lockd.lease.release",
		metric.WithDescription("Lease release operations"),
	)
	logMetricInitError(logger, "lockd.lease.release", err)

	m.releaseDur, err = meter.Int64Histogram(
		"lockd.lease.release.duration_ms",
		metric.WithDescription("Lease release duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.lease.release.duration_ms", err)

	m.activeGauge, err = meter.Int64ObservableGauge(
		"lockd.lease.active",
		metric.WithDescription("Active leases (best-effort)"),
	)
	logMetricInitError(logger, "lockd.lease.active", err)

	if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		m.observeActive(ctx, o)
		return nil
	}, m.activeGauge); err != nil && logger != nil {
		logger.Warn("telemetry.metric.callback_failed", "name", "lockd.lease.active", "error", err)
	}

	return m
}

func (m *leaseMetrics) observeActive(ctx context.Context, o metric.Observer) {
	if m == nil || m.activeGauge == nil {
		return
	}
	attrsLock := []attribute.KeyValue{attribute.String("lockd.lease.kind", "lock")}
	attrsMsg := []attribute.KeyValue{attribute.String("lockd.lease.kind", "queue_message")}
	attrsState := []attribute.KeyValue{attribute.String("lockd.lease.kind", "queue_state")}

	o.ObserveInt64(m.activeGauge, m.activeLock.Load(), metric.WithAttributes(attrsLock...))
	o.ObserveInt64(m.activeGauge, m.activeMsg.Load(), metric.WithAttributes(attrsMsg...))
	o.ObserveInt64(m.activeGauge, m.activeState.Load(), metric.WithAttributes(attrsState...))
}

func (m *leaseMetrics) addActive(kind string, delta int64) {
	if m == nil {
		return
	}
	switch kind {
	case "queue_message":
		m.activeMsg.Add(delta)
	case "queue_state":
		m.activeState.Add(delta)
	default:
		m.activeLock.Add(delta)
	}
}

func (m *leaseMetrics) recordAcquire(ctx context.Context, namespace, kind string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.lease.result", metricResultLabel(err)),
		attribute.String("lockd.lease.kind", kind),
	}
	if m.acquireCount != nil {
		m.acquireCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.acquireDuration != nil {
		m.acquireDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *leaseMetrics) recordKeepAlive(ctx context.Context, namespace, kind string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.lease.result", metricResultLabel(err)),
		attribute.String("lockd.lease.kind", kind),
	}
	if m.keepaliveCount != nil {
		m.keepaliveCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.keepaliveDur != nil {
		m.keepaliveDur.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *leaseMetrics) recordRelease(ctx context.Context, namespace, kind string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.lease.result", metricResultLabel(err)),
		attribute.String("lockd.lease.kind", kind),
	}
	if m.releaseCount != nil {
		m.releaseCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.releaseDur != nil {
		m.releaseDur.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}
