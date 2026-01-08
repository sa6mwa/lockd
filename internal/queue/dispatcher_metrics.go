package queue

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/pslog"
)

type dispatcherMetrics struct {
	waiters   metric.Int64ObservableGauge
	pending   metric.Int64ObservableGauge
	consumers metric.Int64ObservableGauge
}

func newDispatcherMetrics(logger pslog.Logger, dispatcher *Dispatcher) *dispatcherMetrics {
	meter := otel.Meter("pkt.systems/lockd/queue")
	m := &dispatcherMetrics{}
	var err error

	m.waiters, err = meter.Int64ObservableGauge(
		"lockd.queue.waiters",
		metric.WithDescription("Queue waiters (per queue)"),
	)
	logMetricInitError(logger, "lockd.queue.waiters", err)

	m.pending, err = meter.Int64ObservableGauge(
		"lockd.queue.pending",
		metric.WithDescription("Pending queue candidates (per queue)"),
	)
	logMetricInitError(logger, "lockd.queue.pending", err)

	m.consumers, err = meter.Int64ObservableGauge(
		"lockd.queue.consumers",
		metric.WithDescription("Total active queue consumers"),
	)
	logMetricInitError(logger, "lockd.queue.consumers", err)

	if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if dispatcher == nil {
			return nil
		}
		dispatcher.observeMetrics(ctx, o, m)
		return nil
	}, m.waiters, m.pending, m.consumers); err != nil && logger != nil {
		logger.Warn("telemetry.metric.callback_failed", "name", "lockd.queue.dispatcher", "error", err)
	}

	return m
}

func (d *Dispatcher) observeMetrics(ctx context.Context, o metric.Observer, m *dispatcherMetrics) {
	if d == nil || m == nil {
		return
	}
	d.mu.Lock()
	totalConsumers := d.totalConsumers
	queues := make([]*queueState, 0, len(d.queues))
	for _, qs := range d.queues {
		queues = append(queues, qs)
	}
	d.mu.Unlock()

	if m.consumers != nil {
		o.ObserveInt64(m.consumers, int64(totalConsumers))
	}

	for _, qs := range queues {
		snap := qs.snapshot()
		attrs := []attribute.KeyValue{
			attribute.String("lockd.namespace", qs.namespace),
			attribute.String("lockd.queue", qs.name),
		}
		if m.waiters != nil {
			o.ObserveInt64(m.waiters, int64(snap.waiters), metric.WithAttributes(attrs...))
		}
		if m.pending != nil {
			o.ObserveInt64(m.pending, int64(snap.pending), metric.WithAttributes(attrs...))
		}
	}
}

func logMetricInitError(logger pslog.Logger, name string, err error) {
	if err == nil || logger == nil {
		return
	}
	logger.Warn("telemetry.metric.init_failed", "name", name, "error", err)
}
