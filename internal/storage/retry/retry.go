package retry

import (
	"context"
	"io"
	"time"

	"pkt.systems/logport"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
)

// Config controls retry behaviour.
type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
}

// Wrap returns a backend that retries transient errors according to cfg.
func Wrap(inner storage.Backend, logger logport.ForLogging, clk clock.Clock, cfg Config) storage.Backend {
	if inner == nil {
		return nil
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 50 * time.Millisecond
	}
	if cfg.Multiplier <= 0 {
		cfg.Multiplier = 2.0
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 2 * time.Second
	}
	return &backend{
		inner:  inner,
		logger: logger,
		clock:  clk,
		cfg:    cfg,
	}
}

type backend struct {
	inner  storage.Backend
	logger logport.ForLogging
	clock  clock.Clock
	cfg    Config
}

func (b *backend) LoadMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	var meta *storage.Meta
	var etag string
	err := b.withRetry(ctx, "load_meta", key, func(ctx context.Context) error {
		var err error
		meta, etag, err = b.inner.LoadMeta(ctx, key)
		return err
	})
	return meta, etag, err
}

func (b *backend) StoreMeta(ctx context.Context, key string, meta *storage.Meta, expectedETag string) (string, error) {
	var newETag string
	err := b.withRetry(ctx, "store_meta", key, func(ctx context.Context) error {
		var err error
		newETag, err = b.inner.StoreMeta(ctx, key, meta, expectedETag)
		return err
	})
	return newETag, err
}

func (b *backend) DeleteMeta(ctx context.Context, key string, expectedETag string) error {
	return b.withRetry(ctx, "delete_meta", key, func(ctx context.Context) error {
		return b.inner.DeleteMeta(ctx, key, expectedETag)
	})
}

func (b *backend) ListMetaKeys(ctx context.Context) ([]string, error) {
	var keys []string
	err := b.withRetry(ctx, "list_meta_keys", "", func(ctx context.Context) error {
		var err error
		keys, err = b.inner.ListMetaKeys(ctx)
		return err
	})
	return keys, err
}

func (b *backend) ReadState(ctx context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	var (
		r    io.ReadCloser
		info *storage.StateInfo
	)
	err := b.withRetry(ctx, "read_state", key, func(ctx context.Context) error {
		var err error
		r, info, err = b.inner.ReadState(ctx, key)
		return err
	})
	return r, info, err
}

func (b *backend) WriteState(ctx context.Context, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	var res *storage.PutStateResult
	err := b.withRetry(ctx, "write_state", key, func(ctx context.Context) error {
		var err error
		res, err = b.inner.WriteState(ctx, key, body, opts)
		return err
	})
	return res, err
}

func (b *backend) RemoveState(ctx context.Context, key string, expectedETag string) error {
	return b.withRetry(ctx, "remove_state", key, func(ctx context.Context) error {
		return b.inner.RemoveState(ctx, key, expectedETag)
	})
}

func (b *backend) ListObjects(ctx context.Context, opts storage.ListOptions) (*storage.ListResult, error) {
	var res *storage.ListResult
	err := b.withRetry(ctx, "list_objects", opts.Prefix, func(ctx context.Context) error {
		var err error
		res, err = b.inner.ListObjects(ctx, opts)
		return err
	})
	return res, err
}

func (b *backend) GetObject(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	var (
		r    io.ReadCloser
		info *storage.ObjectInfo
	)
	err := b.withRetry(ctx, "get_object", key, func(ctx context.Context) error {
		var err error
		r, info, err = b.inner.GetObject(ctx, key)
		return err
	})
	return r, info, err
}

func (b *backend) PutObject(ctx context.Context, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	var info *storage.ObjectInfo
	err := b.withRetry(ctx, "put_object", key, func(ctx context.Context) error {
		var err error
		info, err = b.inner.PutObject(ctx, key, body, opts)
		return err
	})
	return info, err
}

func (b *backend) DeleteObject(ctx context.Context, key string, opts storage.DeleteObjectOptions) error {
	return b.withRetry(ctx, "delete_object", key, func(ctx context.Context) error {
		return b.inner.DeleteObject(ctx, key, opts)
	})
}

func (b *backend) Close() error {
	return b.inner.Close()
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

func (b *backend) withRetry(ctx context.Context, op, key string, fn func(context.Context) error) error {
	attempts := b.cfg.MaxAttempts
	delay := b.cfg.BaseDelay
	if attempts <= 1 {
		return fn(ctx)
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if !storage.IsTransient(err) || attempt == attempts {
			return err
		}
		b.logger.Warn("storage transient error",
			"operation", op,
			"key", key,
			"attempt", attempt,
			"max_attempts", attempts,
			"error", err,
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			b.clock.Sleep(delay)
			next := time.Duration(float64(delay) * b.cfg.Multiplier)
			if b.cfg.MaxDelay > 0 && next > b.cfg.MaxDelay {
				next = b.cfg.MaxDelay
			}
			delay = next
		}
	}
	return lastErr
}
