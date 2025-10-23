package logging

import (
	"context"
	"io"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/logport"

	"pkt.systems/lockd/internal/storage"
)

type backend struct {
	inner  storage.Backend
	logger logport.ForLogging
	tracer trace.Tracer
}

// Wrap decorates inner with trace/debug logging.
func Wrap(inner storage.Backend, logger logport.ForLogging) storage.Backend {
	if logger == nil {
		logger = logport.NoopLogger()
	}
	return &backend{
		inner:  inner,
		logger: logger,
		tracer: otel.Tracer("pkt.systems/lockd/storage"),
	}
}

func (b *backend) start(ctx context.Context, op string) (context.Context, trace.Span, logport.ForLogging, logport.ForLogging, time.Time, func(string, error)) {
	begin := time.Now()
	ctx, span := b.tracer.Start(ctx, "lockd.storage."+op, trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(attribute.String("lockd.storage.operation", op))
	span.AddEvent("lockd.storage.begin")

	logger := b.logger
	if ctxLogger := logport.LoggerFromContext(ctx); ctxLogger != nil {
		logger = ctxLogger
	} else if corr := correlation.ID(ctx); corr != "" {
		logger = logger.With("cid", corr)
	}
	logger = logger.With("storage_op", op)
	verbose := logger.WithTrace(ctx)
	if corr := correlation.ID(ctx); corr != "" {
		span.SetAttributes(attribute.String("lockd.correlation_id", corr))
	}

	ctx = logport.ContextWithLogger(ctx, logger)
	return ctx, span, logger, verbose, begin, func(result string, err error) {
		duration := time.Since(begin).Milliseconds()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "storage_error")
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.AddEvent("lockd.storage.end", trace.WithAttributes(
			attribute.String("lockd.storage.result", result),
			attribute.Int64("lockd.storage.duration_ms", duration),
		))
	}
}

func (b *backend) LoadMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "load_meta")
	defer span.End()

	verbose.Trace("storage.load_meta.begin", "key", key)
	span.SetAttributes(attribute.Bool("lockd.storage.has_key", key != ""))

	meta, etag, err := b.inner.LoadMeta(ctx, key)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.load_meta.error", "key", key, "error", err, "elapsed", time.Since(begin))
		return nil, etag, err
	}
	owner := ""
	expires := int64(0)
	fencing := int64(0)
	if meta != nil && meta.Lease != nil {
		owner = meta.Lease.Owner
		expires = meta.Lease.ExpiresAtUnix
		fencing = meta.Lease.FencingToken
	}
	version := int64(0)
	stateETag := ""
	if meta != nil {
		version = meta.Version
		stateETag = meta.StateETag
	}
	if meta != nil {
		span.SetAttributes(
			attribute.Bool("lockd.storage.lease_active", meta.Lease != nil),
			attribute.Int64("lockd.storage.meta_version", meta.Version),
			attribute.Bool("lockd.storage.has_state_etag", meta.StateETag != ""),
		)
	}
	finish("ok", nil)
	verbose.Debug("storage.load_meta.success",
		"key", key,
		"version", version,
		"state_etag", stateETag,
		"lease_owner", owner,
		"lease_expires_at", expires,
		"fencing", fencing,
		"meta_etag", etag,
		"elapsed", time.Since(begin),
	)
	return meta, etag, nil
}

func (b *backend) StoreMeta(ctx context.Context, key string, meta *storage.Meta, expectedETag string) (string, error) {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "store_meta")
	defer span.End()

	version := int64(0)
	stateETag := ""
	owner := ""
	expires := int64(0)
	fencing := int64(0)
	if meta != nil {
		version = meta.Version
		stateETag = meta.StateETag
		if meta.Lease != nil {
			owner = meta.Lease.Owner
			expires = meta.Lease.ExpiresAtUnix
			fencing = meta.Lease.FencingToken
		}
	}
	span.SetAttributes(
		attribute.Bool("lockd.storage.has_key", key != ""),
		attribute.Bool("lockd.storage.expected_etag", expectedETag != ""),
		attribute.Bool("lockd.storage.has_meta", meta != nil),
	)
	verbose.Trace("storage.store_meta.begin",
		"key", key,
		"expected_etag", expectedETag,
		"version", version,
		"state_etag", stateETag,
		"lease_owner", owner,
		"lease_expires_at", expires,
		"fencing", fencing,
	)
	newETag, err := b.inner.StoreMeta(ctx, key, meta, expectedETag)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.store_meta.error", "key", key, "error", err, "elapsed", time.Since(begin))
		return newETag, err
	}
	if meta != nil {
		span.SetAttributes(
			attribute.Int64("lockd.storage.meta_version", meta.Version),
			attribute.Bool("lockd.storage.lease_active", meta.Lease != nil),
		)
	}
	finish("ok", nil)
	verbose.Debug("storage.store_meta.success", "key", key, "new_etag", newETag, "elapsed", time.Since(begin))
	return newETag, nil
}

func (b *backend) DeleteMeta(ctx context.Context, key string, expectedETag string) error {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "delete_meta")
	defer span.End()

	span.SetAttributes(
		attribute.Bool("lockd.storage.has_key", key != ""),
		attribute.Bool("lockd.storage.expected_etag", expectedETag != ""),
	)
	verbose.Trace("storage.delete_meta.begin", "key", key, "expected_etag", expectedETag)
	err := b.inner.DeleteMeta(ctx, key, expectedETag)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.delete_meta.error", "key", key, "error", err, "elapsed", time.Since(begin))
		return err
	}
	finish("ok", nil)
	verbose.Debug("storage.delete_meta.success", "key", key, "elapsed", time.Since(begin))
	return nil
}

func (b *backend) ListMetaKeys(ctx context.Context) ([]string, error) {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "list_meta_keys")
	defer span.End()

	verbose.Trace("storage.list_meta_keys.begin")
	keys, err := b.inner.ListMetaKeys(ctx)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.list_meta_keys.error", "error", err, "elapsed", time.Since(begin))
		return keys, err
	}
	span.SetAttributes(attribute.Int("lockd.storage.key_count", len(keys)))
	finish("ok", nil)
	verbose.Debug("storage.list_meta_keys.success", "count", len(keys), "elapsed", time.Since(begin))
	return keys, nil
}

func (b *backend) ReadState(ctx context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "read_state")
	defer span.End()

	span.SetAttributes(attribute.Bool("lockd.storage.has_key", key != ""))
	verbose.Trace("storage.read_state.begin", "key", key)
	reader, info, err := b.inner.ReadState(ctx, key)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.read_state.error", "key", key, "error", err, "elapsed", time.Since(begin))
		return nil, info, err
	}
	size := int64(-1)
	stateETag := ""
	version := int64(0)
	if info != nil {
		size = info.Size
		stateETag = info.ETag
		version = info.Version
	}
	if info != nil {
		span.SetAttributes(
			attribute.Int64("lockd.storage.state_size_bytes", info.Size),
			attribute.Bool("lockd.storage.has_state_etag", info.ETag != ""),
			attribute.Int64("lockd.storage.state_version", info.Version),
		)
	}
	finish("ok", nil)
	verbose.Debug("storage.read_state.success",
		"key", key,
		"size", size,
		"state_etag", stateETag,
		"version", version,
		"elapsed", time.Since(begin),
	)
	return reader, info, nil
}

func (b *backend) WriteState(ctx context.Context, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "write_state")
	defer span.End()

	span.SetAttributes(
		attribute.Bool("lockd.storage.has_key", key != ""),
		attribute.Bool("lockd.storage.expected_etag", opts.ExpectedETag != ""),
	)
	verbose.Trace("storage.write_state.begin", "key", key, "expected_etag", opts.ExpectedETag)
	res, err := b.inner.WriteState(ctx, key, body, opts)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.write_state.error", "key", key, "error", err, "elapsed", time.Since(begin))
		return res, err
	}
	bytesWritten := int64(-1)
	newETag := ""
	if res != nil {
		bytesWritten = res.BytesWritten
		newETag = res.NewETag
	}
	if res != nil {
		span.SetAttributes(
			attribute.Int64("lockd.storage.bytes_written", res.BytesWritten),
			attribute.Bool("lockd.storage.has_state_etag", res.NewETag != ""),
		)
	}
	finish("ok", nil)
	verbose.Debug("storage.write_state.success",
		"key", key,
		"bytes_written", bytesWritten,
		"new_etag", newETag,
		"elapsed", time.Since(begin),
	)
	return res, nil
}

func (b *backend) RemoveState(ctx context.Context, key string, expectedETag string) error {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "remove_state")
	defer span.End()

	span.SetAttributes(
		attribute.Bool("lockd.storage.has_key", key != ""),
		attribute.Bool("lockd.storage.expected_etag", expectedETag != ""),
	)
	verbose.Trace("storage.remove_state.begin", "key", key, "expected_etag", expectedETag)
	err := b.inner.RemoveState(ctx, key, expectedETag)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.remove_state.error", "key", key, "error", err, "elapsed", time.Since(begin))
		return err
	}
	finish("ok", nil)
	verbose.Debug("storage.remove_state.success", "key", key, "elapsed", time.Since(begin))
	return nil
}

func (b *backend) ListObjects(ctx context.Context, opts storage.ListOptions) (*storage.ListResult, error) {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "list_objects")
	defer span.End()

	span.SetAttributes(
		attribute.String("lockd.storage.prefix", opts.Prefix),
		attribute.String("lockd.storage.start_after", opts.StartAfter),
		attribute.Int("lockd.storage.limit", opts.Limit),
	)
	verbose.Trace("storage.list_objects.begin",
		"prefix", opts.Prefix,
		"start_after", opts.StartAfter,
		"limit", opts.Limit,
	)
	result, err := b.inner.ListObjects(ctx, opts)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.list_objects.error", "error", err, "elapsed", time.Since(begin))
		return result, err
	}
	count := 0
	if result != nil {
		count = len(result.Objects)
	}
	span.SetAttributes(attribute.Int("lockd.storage.object_count", count))
	finish("ok", nil)
	verbose.Debug("storage.list_objects.success",
		"prefix", opts.Prefix,
		"start_after", opts.StartAfter,
		"limit", opts.Limit,
		"count", count,
		"truncated", result != nil && result.Truncated,
		"elapsed", time.Since(begin),
	)
	return result, nil
}

func (b *backend) GetObject(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "get_object")
	defer span.End()

	span.SetAttributes(attribute.Bool("lockd.storage.has_key", key != ""))
	verbose.Trace("storage.get_object.begin", "key", key)
	body, info, err := b.inner.GetObject(ctx, key)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.get_object.error", "key", key, "error", err, "elapsed", time.Since(begin))
		return body, info, err
	}
	etag := ""
	size := int64(0)
	if info != nil {
		etag = info.ETag
		size = info.Size
	}
	span.SetAttributes(
		attribute.Bool("lockd.storage.found", info != nil),
		attribute.Bool("lockd.storage.has_etag", etag != ""),
		attribute.Int64("lockd.storage.object_size", size),
	)
	finish("ok", nil)
	verbose.Debug("storage.get_object.success", "key", key, "etag", etag, "size", size, "elapsed", time.Since(begin))
	return body, info, nil
}

func (b *backend) PutObject(ctx context.Context, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "put_object")
	defer span.End()

	span.SetAttributes(
		attribute.Bool("lockd.storage.has_key", key != ""),
		attribute.Bool("lockd.storage.expected_etag", opts.ExpectedETag != ""),
		attribute.Bool("lockd.storage.if_not_exists", opts.IfNotExists),
	)
	verbose.Trace("storage.put_object.begin",
		"key", key,
		"expected_etag", opts.ExpectedETag,
		"if_not_exists", opts.IfNotExists,
		"content_type", opts.ContentType,
	)
	info, err := b.inner.PutObject(ctx, key, body, opts)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.put_object.error", "key", key, "error", err, "elapsed", time.Since(begin))
		return info, err
	}
	etag := ""
	size := int64(0)
	if info != nil {
		etag = info.ETag
		size = info.Size
	}
	span.SetAttributes(attribute.Bool("lockd.storage.has_etag", etag != ""))
	finish("ok", nil)
	verbose.Debug("storage.put_object.success", "key", key, "etag", etag, "size", size, "elapsed", time.Since(begin))
	return info, nil
}

func (b *backend) DeleteObject(ctx context.Context, key string, opts storage.DeleteObjectOptions) error {
	ctx, span, _, verbose, begin, finish := b.start(ctx, "delete_object")
	defer span.End()

	span.SetAttributes(
		attribute.Bool("lockd.storage.has_key", key != ""),
		attribute.Bool("lockd.storage.expected_etag", opts.ExpectedETag != ""),
		attribute.Bool("lockd.storage.ignore_not_found", opts.IgnoreNotFound),
	)
	verbose.Trace("storage.delete_object.begin",
		"key", key,
		"expected_etag", opts.ExpectedETag,
		"ignore_not_found", opts.IgnoreNotFound,
	)
	err := b.inner.DeleteObject(ctx, key, opts)
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.delete_object.error", "key", key, "error", err, "elapsed", time.Since(begin))
		return err
	}
	finish("ok", nil)
	verbose.Debug("storage.delete_object.success", "key", key, "elapsed", time.Since(begin))
	return nil
}

func (b *backend) Close() error {
	_, span, _, verbose, begin, finish := b.start(context.Background(), "close")
	defer span.End()

	verbose.Trace("storage.close.begin")
	err := b.inner.Close()
	if err != nil {
		finish("error", err)
		verbose.Debug("storage.close.error", "error", err, "elapsed", time.Since(begin))
		return err
	}
	finish("ok", nil)
	verbose.Debug("storage.close.success", "elapsed", time.Since(begin))
	return nil
}

func (b *backend) SubscribeQueueChanges(queue string) (storage.QueueChangeSubscription, error) {
	if feed, ok := b.inner.(storage.QueueChangeFeed); ok {
		return feed.SubscribeQueueChanges(queue)
	}
	return nil, storage.ErrNotImplemented
}

func (b *backend) QueueWatchStatus() (bool, string, string) {
	if provider, ok := b.inner.(storage.QueueWatchStatusProvider); ok {
		return provider.QueueWatchStatus()
	}
	return false, "unknown", "backend_does_not_report"
}
