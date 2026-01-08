package core

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pkt.systems/pslog"
)

type attachmentMetrics struct {
	attachCount    metric.Int64Counter
	attachBytes    metric.Int64Counter
	attachDuration metric.Int64Histogram
	listCount      metric.Int64Counter
	listDuration   metric.Int64Histogram
	listItems      metric.Int64Histogram
	retrieveCount  metric.Int64Counter
	retrieveBytes  metric.Int64Counter
	retrieveDur    metric.Int64Histogram
	deleteCount    metric.Int64Counter
	deleteDur      metric.Int64Histogram
	deleteAllCount metric.Int64Counter
	deleteAllDur   metric.Int64Histogram
	deleteAllItems metric.Int64Histogram
}

func newAttachmentMetrics(logger pslog.Logger) *attachmentMetrics {
	meter := otel.Meter("pkt.systems/lockd/attachments")
	m := &attachmentMetrics{}
	var err error

	m.attachCount, err = meter.Int64Counter(
		"lockd.attachments.attach",
		metric.WithDescription("Attachment uploads"),
	)
	logMetricInitError(logger, "lockd.attachments.attach", err)

	m.attachBytes, err = meter.Int64Counter(
		"lockd.attachments.attach.bytes",
		metric.WithDescription("Attachment upload bytes"),
		metric.WithUnit("By"),
	)
	logMetricInitError(logger, "lockd.attachments.attach.bytes", err)

	m.attachDuration, err = meter.Int64Histogram(
		"lockd.attachments.attach.duration_ms",
		metric.WithDescription("Attachment upload duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.attachments.attach.duration_ms", err)

	m.listCount, err = meter.Int64Counter(
		"lockd.attachments.list",
		metric.WithDescription("Attachment list operations"),
	)
	logMetricInitError(logger, "lockd.attachments.list", err)

	m.listDuration, err = meter.Int64Histogram(
		"lockd.attachments.list.duration_ms",
		metric.WithDescription("Attachment list duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.attachments.list.duration_ms", err)

	m.listItems, err = meter.Int64Histogram(
		"lockd.attachments.list.items",
		metric.WithDescription("Attachment list item counts"),
	)
	logMetricInitError(logger, "lockd.attachments.list.items", err)

	m.retrieveCount, err = meter.Int64Counter(
		"lockd.attachments.retrieve",
		metric.WithDescription("Attachment retrieve operations"),
	)
	logMetricInitError(logger, "lockd.attachments.retrieve", err)

	m.retrieveBytes, err = meter.Int64Counter(
		"lockd.attachments.retrieve.bytes",
		metric.WithDescription("Attachment retrieve bytes"),
		metric.WithUnit("By"),
	)
	logMetricInitError(logger, "lockd.attachments.retrieve.bytes", err)

	m.retrieveDur, err = meter.Int64Histogram(
		"lockd.attachments.retrieve.duration_ms",
		metric.WithDescription("Attachment retrieve duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.attachments.retrieve.duration_ms", err)

	m.deleteCount, err = meter.Int64Counter(
		"lockd.attachments.delete",
		metric.WithDescription("Attachment delete operations"),
	)
	logMetricInitError(logger, "lockd.attachments.delete", err)

	m.deleteDur, err = meter.Int64Histogram(
		"lockd.attachments.delete.duration_ms",
		metric.WithDescription("Attachment delete duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.attachments.delete.duration_ms", err)

	m.deleteAllCount, err = meter.Int64Counter(
		"lockd.attachments.delete_all",
		metric.WithDescription("Attachment delete-all operations"),
	)
	logMetricInitError(logger, "lockd.attachments.delete_all", err)

	m.deleteAllDur, err = meter.Int64Histogram(
		"lockd.attachments.delete_all.duration_ms",
		metric.WithDescription("Attachment delete-all duration"),
		metric.WithUnit("ms"),
	)
	logMetricInitError(logger, "lockd.attachments.delete_all.duration_ms", err)

	m.deleteAllItems, err = meter.Int64Histogram(
		"lockd.attachments.delete_all.items",
		metric.WithDescription("Attachment delete-all item counts"),
	)
	logMetricInitError(logger, "lockd.attachments.delete_all.items", err)

	return m
}

func (m *attachmentMetrics) recordAttach(ctx context.Context, namespace string, bytes int64, duration time.Duration, err error) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.attachment.result", metricResultLabel(err)),
	}
	if m.attachCount != nil {
		m.attachCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.attachBytes != nil && err == nil && bytes > 0 {
		m.attachBytes.Add(ctx, bytes, metric.WithAttributes(attrs...))
	}
	if m.attachDuration != nil {
		m.attachDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *attachmentMetrics) recordList(ctx context.Context, namespace string, count int, duration time.Duration, err error) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.attachment.result", metricResultLabel(err)),
	}
	if m.listCount != nil {
		m.listCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.listItems != nil && err == nil {
		m.listItems.Record(ctx, int64(count), metric.WithAttributes(attrs...))
	}
	if m.listDuration != nil {
		m.listDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *attachmentMetrics) recordRetrieve(ctx context.Context, namespace string, bytes int64, duration time.Duration, err error) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.attachment.result", metricResultLabel(err)),
	}
	if m.retrieveCount != nil {
		m.retrieveCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.retrieveBytes != nil && err == nil && bytes > 0 {
		m.retrieveBytes.Add(ctx, bytes, metric.WithAttributes(attrs...))
	}
	if m.retrieveDur != nil {
		m.retrieveDur.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *attachmentMetrics) recordDelete(ctx context.Context, namespace string, duration time.Duration, err error) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.attachment.result", metricResultLabel(err)),
	}
	if m.deleteCount != nil {
		m.deleteCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.deleteDur != nil {
		m.deleteDur.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}

func (m *attachmentMetrics) recordDeleteAll(ctx context.Context, namespace string, deleted int, duration time.Duration, err error) {
	if m == nil {
		return
	}
	ctx = metricContext(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("lockd.namespace", namespace),
		attribute.String("lockd.attachment.result", metricResultLabel(err)),
	}
	if m.deleteAllCount != nil {
		m.deleteAllCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.deleteAllItems != nil && err == nil {
		m.deleteAllItems.Record(ctx, int64(deleted), metric.WithAttributes(attrs...))
	}
	if m.deleteAllDur != nil {
		m.deleteAllDur.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
	}
}
