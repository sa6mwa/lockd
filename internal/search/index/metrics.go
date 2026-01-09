package index

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/pslog"
)

type indexMetrics struct {
	flushDuration  metric.Int64Histogram
	flushErrors    metric.Int64Counter
	segmentWritten metric.Int64Counter
	segmentDocs    metric.Int64Histogram
	segmentSize    metric.Int64Histogram
	queueDepth     metric.Int64ObservableGauge
	queueLag       metric.Int64ObservableGauge
}

func newIndexMetrics(logger pslog.Logger) *indexMetrics {
	meter := otel.Meter("pkt.systems/lockd/index")
	m := &indexMetrics{}
	var err error

	m.flushDuration, err = meter.Int64Histogram(
		"lockd.index.flush.duration_ms",
		metric.WithDescription("Index segment flush duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.index.flush.duration_ms", err)

	m.flushErrors, err = meter.Int64Counter(
		"lockd.index.flush.errors",
		metric.WithDescription("Index segment flush errors"),
	)
	logMetricInitError(logger, "lockd.index.flush.errors", err)

	m.segmentWritten, err = meter.Int64Counter(
		"lockd.index.segment.written",
		metric.WithDescription("Index segments persisted"),
	)
	logMetricInitError(logger, "lockd.index.segment.written", err)

	m.segmentDocs, err = meter.Int64Histogram(
		"lockd.index.segment.docs",
		metric.WithDescription("Document count per index segment"),
	)
	logMetricInitError(logger, "lockd.index.segment.docs", err)

	m.segmentSize, err = meter.Int64Histogram(
		"lockd.index.segment.size",
		metric.WithDescription("Index segment payload size"),
		metric.WithUnit("By"),
	)
	logMetricInitError(logger, "lockd.index.segment.size", err)

	m.queueDepth, err = meter.Int64ObservableGauge(
		"lockd.index.queue.depth",
		metric.WithDescription("Pending documents queued for indexing"),
	)
	logMetricInitError(logger, "lockd.index.queue.depth", err)

	m.queueLag, err = meter.Int64ObservableGauge(
		"lockd.index.queue.lag_ms",
		metric.WithDescription("Age of the oldest pending indexed document"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.index.queue.lag_ms", err)

	return m
}

func (m *indexMetrics) registerWriter(w *Writer) {
	if m == nil || w == nil {
		return
	}
	if m.queueDepth == nil && m.queueLag == nil {
		return
	}
	meter := otel.Meter("pkt.systems/lockd/index")
	if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		m.observeQueue(ctx, o, w)
		return nil
	}, m.queueDepth, m.queueLag); err != nil && w.logger != nil {
		w.logger.Warn("telemetry.metric.callback_failed", "name", "lockd.index.queue", "error", err)
	}
}

func (m *indexMetrics) observeQueue(ctx context.Context, o metric.Observer, w *Writer) {
	if m == nil || w == nil {
		return
	}
	depth, lagMs := w.metricsSnapshot(w.clock.Now())
	attrs := []attribute.KeyValue{attribute.String("lockd.namespace", w.cfg.Namespace)}
	if m.queueDepth != nil {
		o.ObserveInt64(m.queueDepth, depth, metric.WithAttributes(attrs...))
	}
	if m.queueLag != nil {
		o.ObserveInt64(m.queueLag, lagMs, metric.WithAttributes(attrs...))
	}
}

func (m *indexMetrics) recordFlush(ctx context.Context, namespace string, duration time.Duration, err error, docs int64, bytes int64) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.index.flush.result", metricResultLabel(err)),
	}
	if m.flushDuration != nil {
		m.flushDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
	if err != nil {
		if m.flushErrors != nil {
			m.flushErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
		}
		return
	}
	if m.segmentWritten != nil {
		m.segmentWritten.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.segmentDocs != nil && docs > 0 {
		m.segmentDocs.Record(ctx, docs, metric.WithAttributes(attrs...))
	}
	if m.segmentSize != nil && bytes > 0 {
		m.segmentSize.Record(ctx, bytes, metric.WithAttributes(attrs...))
	}
}

func metricResultLabel(err error) string {
	if err == nil {
		return "success"
	}
	return "error"
}

func metricContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func logMetricInitError(logger pslog.Logger, name string, err error) {
	if err == nil || logger == nil {
		return
	}
	logger.Warn("telemetry.metric.init_failed", "name", name, "error", err)
}
