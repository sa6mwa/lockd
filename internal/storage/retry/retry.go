package retry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

// ErrNonReplayableBody indicates a write retry could not be attempted because
// the request body cannot be safely rewound.
var ErrNonReplayableBody = errors.New("storage retry: non-replayable write body")

// Config controls retry behaviour.
type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
}

// Wrap returns a backend that retries transient errors according to cfg.
func Wrap(inner storage.Backend, logger pslog.Logger, clk clock.Clock, cfg Config) storage.Backend {
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
	if logger == nil {
		logger = pslog.NoopLogger()
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
	logger pslog.Logger
	clock  clock.Clock
	cfg    Config
}

func (b *backend) LoadMeta(ctx context.Context, namespace, key string) (storage.LoadMetaResult, error) {
	var result storage.LoadMetaResult
	err := b.withRetry(ctx, "load_meta", namespace, key, func(ctx context.Context) error {
		var err error
		result, err = b.inner.LoadMeta(ctx, namespace, key)
		return err
	})
	return result, err
}

func (b *backend) LoadMetaSummary(ctx context.Context, namespace, key string) (storage.LoadMetaSummaryResult, error) {
	var result storage.LoadMetaSummaryResult
	err := b.withRetry(ctx, "load_meta_summary", namespace, key, func(ctx context.Context) error {
		var err error
		result, err = storage.LoadMetaSummary(ctx, b.inner, namespace, key)
		return err
	})
	return result, err
}

func (b *backend) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	var newETag string
	err := b.withRetry(ctx, "store_meta", namespace, key, func(ctx context.Context) error {
		var err error
		newETag, err = b.inner.StoreMeta(ctx, namespace, key, meta, expectedETag)
		return err
	})
	return newETag, err
}

func (b *backend) DeleteMeta(ctx context.Context, namespace, key string, expectedETag string) error {
	return b.withRetry(ctx, "delete_meta", namespace, key, func(ctx context.Context) error {
		return b.inner.DeleteMeta(ctx, namespace, key, expectedETag)
	})
}

func (b *backend) ListMetaKeys(ctx context.Context, namespace string) ([]string, error) {
	var keys []string
	err := b.withRetry(ctx, "list_meta_keys", namespace, "", func(ctx context.Context) error {
		var err error
		keys, err = b.inner.ListMetaKeys(ctx, namespace)
		return err
	})
	return keys, err
}

func (b *backend) ReadState(ctx context.Context, namespace, key string) (storage.ReadStateResult, error) {
	attempts := b.cfg.MaxAttempts
	delay := b.cfg.BaseDelay
	if attempts <= 1 {
		return b.inner.ReadState(ctx, namespace, key)
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		result, err := b.inner.ReadState(ctx, namespace, key)
		if err == nil {
			return result, nil
		}
		if !storage.IsTransient(err) || attempt == attempts {
			return storage.ReadStateResult{}, err
		}
		b.logger.Warn("storage transient error",
			"operation", "read_state",
			"namespace", namespace,
			"key", key,
			"attempt", attempt,
			"max_attempts", attempts,
			"error", err,
		)
		select {
		case <-ctx.Done():
			return storage.ReadStateResult{}, ctx.Err()
		default:
			b.clock.Sleep(delay)
			next := time.Duration(float64(delay) * b.cfg.Multiplier)
			if b.cfg.MaxDelay > 0 && next > b.cfg.MaxDelay {
				next = b.cfg.MaxDelay
			}
			delay = next
		}
	}
	return storage.ReadStateResult{}, nil
}

func (b *backend) WriteState(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	var res *storage.PutStateResult
	err := b.withRetryBody(ctx, "write_state", namespace, key, body, func(ctx context.Context, reader io.Reader) error {
		var err error
		res, err = b.inner.WriteState(ctx, namespace, key, reader, opts)
		return err
	})
	return res, err
}

func (b *backend) Remove(ctx context.Context, namespace, key string, expectedETag string) error {
	return b.withRetry(ctx, "remove_state", namespace, key, func(ctx context.Context) error {
		return b.inner.Remove(ctx, namespace, key, expectedETag)
	})
}

func (b *backend) ListObjects(ctx context.Context, namespace string, opts storage.ListOptions) (*storage.ListResult, error) {
	var res *storage.ListResult
	err := b.withRetry(ctx, "list_objects", namespace, opts.Prefix, func(ctx context.Context) error {
		var err error
		res, err = b.inner.ListObjects(ctx, namespace, opts)
		return err
	})
	return res, err
}

func (b *backend) GetObject(ctx context.Context, namespace, key string) (storage.GetObjectResult, error) {
	var result storage.GetObjectResult
	err := b.withRetry(ctx, "get_object", namespace, key, func(ctx context.Context) error {
		var err error
		result, err = b.inner.GetObject(ctx, namespace, key)
		return err
	})
	return result, err
}

func (b *backend) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	var info *storage.ObjectInfo
	err := b.withRetryBody(ctx, "put_object", namespace, key, body, func(ctx context.Context, reader io.Reader) error {
		var err error
		info, err = b.inner.PutObject(ctx, namespace, key, reader, opts)
		return err
	})
	return info, err
}

func (b *backend) DeleteObject(ctx context.Context, namespace, key string, opts storage.DeleteObjectOptions) error {
	return b.withRetry(ctx, "delete_object", namespace, key, func(ctx context.Context) error {
		return b.inner.DeleteObject(ctx, namespace, key, opts)
	})
}

func (b *backend) BackendHash(ctx context.Context) (string, error) {
	return b.inner.BackendHash(ctx)
}

func (b *backend) SetSingleWriter(enabled bool) {
	if inner, ok := b.inner.(storage.SingleWriterControl); ok {
		inner.SetSingleWriter(enabled)
	}
}

func (b *backend) SupportsConcurrentWrites() bool {
	if inner, ok := b.inner.(storage.ConcurrentWriteSupport); ok {
		return inner.SupportsConcurrentWrites()
	}
	return true
}

func (b *backend) Close() error {
	return b.inner.Close()
}

func (b *backend) DefaultNamespaceConfig() namespaces.Config {
	if provider, ok := b.inner.(namespaces.ConfigProvider); ok && provider != nil {
		return provider.DefaultNamespaceConfig()
	}
	return namespaces.DefaultConfig()
}

func (b *backend) ListNamespaces(ctx context.Context) ([]string, error) {
	if lister, ok := b.inner.(storage.NamespaceLister); ok {
		return lister.ListNamespaces(ctx)
	}
	return nil, storage.ErrNotImplemented
}

func (b *backend) SubscribeQueueChanges(namespace, queue string) (storage.QueueChangeSubscription, error) {
	if feed, ok := b.inner.(storage.QueueChangeFeed); ok {
		return feed.SubscribeQueueChanges(namespace, queue)
	}
	return nil, storage.ErrNotImplemented
}

func (b *backend) QueueWatchStatus() storage.QueueWatchStatus {
	if provider, ok := b.inner.(storage.QueueWatchStatusProvider); ok {
		return provider.QueueWatchStatus()
	}
	return storage.QueueWatchStatus{Enabled: false, Mode: "unknown", Reason: "backend_does_not_report"}
}

func (b *backend) withRetry(ctx context.Context, op, namespace, key string, fn func(context.Context) error) error {
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
			"namespace", namespace,
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

func (b *backend) withRetryBody(ctx context.Context, op, namespace, key string, body io.Reader, fn func(context.Context, io.Reader) error) error {
	attempts := b.cfg.MaxAttempts
	delay := b.cfg.BaseDelay
	if attempts <= 1 {
		return fn(ctx, body)
	}
	var (
		lastErr     error
		startOffset int64
		seeker      io.Seeker
		replayable  bool
	)
	if body == nil {
		replayable = true
	}
	if s, ok := body.(io.Seeker); ok {
		offset, err := s.Seek(0, io.SeekCurrent)
		if err == nil {
			startOffset = offset
			seeker = s
			replayable = true
		}
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if !replayable {
				return fmt.Errorf("%w: operation=%s namespace=%s key=%s: transient write cannot be retried safely; body must implement io.Seeker or retries must be disabled: %v",
					ErrNonReplayableBody, op, namespace, key, lastErr)
			}
			if seeker != nil {
				if _, err := seeker.Seek(startOffset, io.SeekStart); err != nil {
					return fmt.Errorf("%w: operation=%s namespace=%s key=%s: seek failed before retry: %w",
						ErrNonReplayableBody, op, namespace, key, err)
				}
			}
		}
		err := fn(ctx, body)
		if err == nil {
			return nil
		}
		lastErr = err
		if !storage.IsTransient(err) || attempt == attempts {
			return err
		}
		if !replayable {
			return fmt.Errorf("%w: operation=%s namespace=%s key=%s: transient write cannot be retried safely; body must implement io.Seeker or retries must be disabled: %v",
				ErrNonReplayableBody, op, namespace, key, lastErr)
		}
		b.logger.Warn("storage transient error",
			"operation", op,
			"namespace", namespace,
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
