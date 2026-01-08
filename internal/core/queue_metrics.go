package core

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/pslog"
)

type queueMetrics struct {
	enqueueCount    metric.Int64Counter
	enqueueBytes    metric.Int64Counter
	enqueueDuration metric.Int64Histogram
	dequeueCount    metric.Int64Counter
	dequeueBytes    metric.Int64Counter
	dequeueDuration metric.Int64Histogram
}

func newQueueMetrics(logger pslog.Logger) *queueMetrics {
	meter := otel.Meter("pkt.systems/lockd/queue")
	m := &queueMetrics{}
	var err error

	m.enqueueCount, err = meter.Int64Counter(
		"lockd.queue.enqueue",
		metric.WithDescription("Queue enqueue operations"),
	)
	logMetricInitError(logger, "lockd.queue.enqueue", err)

	m.enqueueBytes, err = meter.Int64Counter(
		"lockd.queue.enqueue.bytes",
		metric.WithDescription("Queue enqueue bytes"),
		metric.WithUnit("By"),
	)
	logMetricInitError(logger, "lockd.queue.enqueue.bytes", err)

	m.enqueueDuration, err = meter.Int64Histogram(
		"lockd.queue.enqueue.duration_ms",
		metric.WithDescription("Queue enqueue duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.queue.enqueue.duration_ms", err)

	m.dequeueCount, err = meter.Int64Counter(
		"lockd.queue.dequeue",
		metric.WithDescription("Queue dequeue operations"),
	)
	logMetricInitError(logger, "lockd.queue.dequeue", err)

	m.dequeueBytes, err = meter.Int64Counter(
		"lockd.queue.dequeue.bytes",
		metric.WithDescription("Queue dequeue bytes"),
		metric.WithUnit("By"),
	)
	logMetricInitError(logger, "lockd.queue.dequeue.bytes", err)

	m.dequeueDuration, err = meter.Int64Histogram(
		"lockd.queue.dequeue.duration_ms",
		metric.WithDescription("Queue dequeue duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.queue.dequeue.duration_ms", err)

	return m
}

func (m *queueMetrics) recordEnqueue(ctx context.Context, namespace, queue string, bytes int64, duration time.Duration) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.queue", queue),
	}
	if m.enqueueCount != nil {
		m.enqueueCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.enqueueBytes != nil && bytes > 0 {
		m.enqueueBytes.Add(ctx, bytes, metric.WithAttributes(attrs...))
	}
	if m.enqueueDuration != nil {
		m.enqueueDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *queueMetrics) recordDequeue(ctx context.Context, namespace, queue string, bytes int64, duration time.Duration, stateful bool) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.queue", queue),
		attribute.String("lockd.queue.stateful", boolLabel(stateful)),
	}
	if m.dequeueCount != nil {
		m.dequeueCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.dequeueBytes != nil && bytes > 0 {
		m.dequeueBytes.Add(ctx, bytes, metric.WithAttributes(attrs...))
	}
	if m.dequeueDuration != nil {
		m.dequeueDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}
