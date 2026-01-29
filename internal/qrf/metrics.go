package qrf

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/pslog"
)

type qrfMetrics struct {
	state       metric.Int64ObservableGauge
	decisions   metric.Int64Counter
	transitions metric.Int64Counter
}

func newQRFMetrics(logger pslog.Logger, controller *Controller) *qrfMetrics {
	meter := otel.Meter("pkt.systems/lockd/qrf")
	m := &qrfMetrics{}
	var err error

	m.state, err = meter.Int64ObservableGauge(
		"lockd.qrf.state",
		metric.WithDescription("Current QRF state"),
	)
	logMetricInitError(logger, "lockd.qrf.state", err)

	m.decisions, err = meter.Int64Counter(
		"lockd.qrf.decision",
		metric.WithDescription("QRF throttle decisions"),
	)
	logMetricInitError(logger, "lockd.qrf.decision", err)

	m.transitions, err = meter.Int64Counter(
		"lockd.qrf.transition",
		metric.WithDescription("QRF state transitions"),
	)
	logMetricInitError(logger, "lockd.qrf.transition", err)

	if m.state != nil {
		if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			if controller == nil {
				return nil
			}
			o.ObserveInt64(m.state, int64(controller.State()))
			return nil
		}, m.state); err != nil && logger != nil {
			logger.Warn("telemetry.metric.callback_failed", "name", "lockd.qrf.state", "error", err)
		}
	}

	return m
}

func (m *qrfMetrics) recordDecision(ctx context.Context, kind Kind, decision Decision) {
	if m == nil || m.decisions == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.qrf.kind", kindLabel(kind)),
		attribute.String("lockd.qrf.state", decision.State.String()),
		attribute.String("lockd.qrf.throttle", boolLabel(decision.Throttle)),
	}
	m.decisions.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (m *qrfMetrics) recordTransition(ctx context.Context, from, to State, reason string) {
	if m == nil || m.transitions == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.qrf.from", from.String()),
		attribute.String("lockd.qrf.to", to.String()),
		attribute.String("lockd.qrf.reason", reasonLabel(reason)),
	}
	m.transitions.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func kindLabel(kind Kind) string {
	switch kind {
	case KindQueueProducer:
		return "queue_producer"
	case KindQueueConsumer:
		return "queue_consumer"
	case KindQueueAck:
		return "queue_ack"
	case KindLock:
		return "lock"
	case KindQuery:
		return "query"
	default:
		return "unknown"
	}
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func reasonLabel(reason string) string {
	if reason == "" {
		return "unknown"
	}
	return reason
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
