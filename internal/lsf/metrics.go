package lsf

import (
	"context"
	"math"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/pslog"
)

type lsfMetrics struct {
	sample         metric.Int64Counter
	queueInflight  metric.Int64ObservableGauge
	lockInflight   metric.Int64ObservableGauge
	queryInflight  metric.Int64ObservableGauge
	rssBytes       metric.Int64ObservableGauge
	swapBytes      metric.Int64ObservableGauge
	memoryPercent  metric.Float64ObservableGauge
	swapPercent    metric.Float64ObservableGauge
	cpuPercent     metric.Float64ObservableGauge
	load           metric.Float64ObservableGauge
	loadBaseline   metric.Float64ObservableGauge
	loadMultiplier metric.Float64ObservableGauge
	goroutines     metric.Int64ObservableGauge

	snapshot atomic.Value
}

func newLSFMetrics(logger pslog.Logger) *lsfMetrics {
	meter := otel.Meter("pkt.systems/lockd/lsf")
	m := &lsfMetrics{}
	var err error

	m.sample, err = meter.Int64Counter(
		"lockd.lsf.sample",
		metric.WithDescription("LSF samples collected"),
	)
	logMetricInitError(logger, "lockd.lsf.sample", err)

	m.queueInflight, err = meter.Int64ObservableGauge(
		"lockd.lsf.queue.inflight",
		metric.WithDescription("LSF queue inflight counts"),
	)
	logMetricInitError(logger, "lockd.lsf.queue.inflight", err)

	m.lockInflight, err = meter.Int64ObservableGauge(
		"lockd.lsf.lock.inflight",
		metric.WithDescription("LSF lock inflight count"),
	)
	logMetricInitError(logger, "lockd.lsf.lock.inflight", err)

	m.queryInflight, err = meter.Int64ObservableGauge(
		"lockd.lsf.query.inflight",
		metric.WithDescription("LSF query inflight count"),
	)
	logMetricInitError(logger, "lockd.lsf.query.inflight", err)

	m.rssBytes, err = meter.Int64ObservableGauge(
		"lockd.lsf.rss.bytes",
		metric.WithDescription("LSF RSS bytes"),
		metric.WithUnit("By"),
	)
	logMetricInitError(logger, "lockd.lsf.rss.bytes", err)

	m.swapBytes, err = meter.Int64ObservableGauge(
		"lockd.lsf.swap.bytes",
		metric.WithDescription("LSF swap bytes"),
		metric.WithUnit("By"),
	)
	logMetricInitError(logger, "lockd.lsf.swap.bytes", err)

	m.memoryPercent, err = meter.Float64ObservableGauge(
		"lockd.lsf.memory.percent",
		metric.WithDescription("LSF system memory used percent"),
	)
	logMetricInitError(logger, "lockd.lsf.memory.percent", err)

	m.swapPercent, err = meter.Float64ObservableGauge(
		"lockd.lsf.swap.percent",
		metric.WithDescription("LSF system swap used percent"),
	)
	logMetricInitError(logger, "lockd.lsf.swap.percent", err)

	m.cpuPercent, err = meter.Float64ObservableGauge(
		"lockd.lsf.cpu.percent",
		metric.WithDescription("LSF system CPU percent"),
	)
	logMetricInitError(logger, "lockd.lsf.cpu.percent", err)

	m.load, err = meter.Float64ObservableGauge(
		"lockd.lsf.load",
		metric.WithDescription("LSF system load average"),
	)
	logMetricInitError(logger, "lockd.lsf.load", err)

	m.loadBaseline, err = meter.Float64ObservableGauge(
		"lockd.lsf.load.baseline",
		metric.WithDescription("LSF load baseline"),
	)
	logMetricInitError(logger, "lockd.lsf.load.baseline", err)

	m.loadMultiplier, err = meter.Float64ObservableGauge(
		"lockd.lsf.load.multiplier",
		metric.WithDescription("LSF load multiplier"),
	)
	logMetricInitError(logger, "lockd.lsf.load.multiplier", err)

	m.goroutines, err = meter.Int64ObservableGauge(
		"lockd.lsf.goroutines",
		metric.WithDescription("LSF goroutine count"),
	)
	logMetricInitError(logger, "lockd.lsf.goroutines", err)

	if _, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		m.observe(ctx, o)
		return nil
	}, m.queueInflight, m.lockInflight, m.queryInflight, m.rssBytes, m.swapBytes, m.memoryPercent, m.swapPercent, m.cpuPercent, m.load, m.loadBaseline, m.loadMultiplier, m.goroutines); err != nil && logger != nil {
		logger.Warn("telemetry.metric.callback_failed", "name", "lockd.lsf.metrics", "error", err)
	}

	return m
}

func (m *lsfMetrics) recordSample(ctx context.Context, snapshot qrf.Snapshot) {
	if m == nil {
		return
	}
	m.snapshot.Store(snapshot)
	if m.sample != nil {
		m.sample.Add(metricContext(ctx), 1)
	}
}

func (m *lsfMetrics) observe(ctx context.Context, o metric.Observer) {
	if m == nil {
		return
	}
	raw := m.snapshot.Load()
	if raw == nil {
		return
	}
	snapshot, ok := raw.(qrf.Snapshot)
	if !ok {
		return
	}
	if m.queueInflight != nil {
		o.ObserveInt64(m.queueInflight, snapshot.QueueProducerInflight, metric.WithAttributes(attribute.String("lockd.queue.kind", "producer")))
		o.ObserveInt64(m.queueInflight, snapshot.QueueConsumerInflight, metric.WithAttributes(attribute.String("lockd.queue.kind", "consumer")))
		o.ObserveInt64(m.queueInflight, snapshot.QueueAckInflight, metric.WithAttributes(attribute.String("lockd.queue.kind", "ack")))
	}
	if m.lockInflight != nil {
		o.ObserveInt64(m.lockInflight, snapshot.LockInflight)
	}
	if m.queryInflight != nil {
		o.ObserveInt64(m.queryInflight, snapshot.QueryInflight)
	}
	if m.rssBytes != nil {
		o.ObserveInt64(m.rssBytes, clampUint64(snapshot.RSSBytes))
	}
	if m.swapBytes != nil {
		o.ObserveInt64(m.swapBytes, clampUint64(snapshot.SwapBytes))
	}
	if m.memoryPercent != nil {
		o.ObserveFloat64(m.memoryPercent, snapshot.SystemMemoryUsedPercent)
	}
	if m.swapPercent != nil {
		o.ObserveFloat64(m.swapPercent, snapshot.SystemSwapUsedPercent)
	}
	if m.cpuPercent != nil {
		o.ObserveFloat64(m.cpuPercent, snapshot.SystemCPUPercent)
	}
	if m.load != nil {
		observeLoad(o, m.load, snapshot.SystemLoad1, snapshot.SystemLoad5, snapshot.SystemLoad15)
	}
	if m.loadBaseline != nil {
		observeLoad(o, m.loadBaseline, snapshot.Load1Baseline, snapshot.Load5Baseline, snapshot.Load15Baseline)
	}
	if m.loadMultiplier != nil {
		observeLoad(o, m.loadMultiplier, snapshot.Load1Multiplier, snapshot.Load5Multiplier, snapshot.Load15Multiplier)
	}
	if m.goroutines != nil {
		o.ObserveInt64(m.goroutines, int64(snapshot.Goroutines))
	}
}

func observeLoad(o metric.Observer, instrument metric.Float64ObservableGauge, load1, load5, load15 float64) {
	if instrument == nil {
		return
	}
	o.ObserveFloat64(instrument, load1, metric.WithAttributes(attribute.String("lockd.load.window", "1")))
	o.ObserveFloat64(instrument, load5, metric.WithAttributes(attribute.String("lockd.load.window", "5")))
	o.ObserveFloat64(instrument, load15, metric.WithAttributes(attribute.String("lockd.load.window", "15")))
}

func clampUint64(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
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
