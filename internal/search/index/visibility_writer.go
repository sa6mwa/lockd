package index

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

// VisibilityWriterConfig configures a visibility writer.
type VisibilityWriterConfig struct {
	Namespace     string
	Store         *Store
	FlushEntries  int
	FlushInterval time.Duration
	Clock         clock.Clock
	Logger        pslog.Logger
}

// VisibilityWriter buffers visibility updates into segments.
type VisibilityWriter struct {
	cfg           VisibilityWriterConfig
	store         *Store
	memtable      *visibilityMemTable
	mu            sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	clock         clock.Clock
	logger        pslog.Logger
	pending       atomic.Bool
	lastFlush     atomic.Int64
	oldestPending atomic.Int64
	readableCh    chan struct{}
}

// NewVisibilityWriter constructs a visibility writer.
func NewVisibilityWriter(cfg VisibilityWriterConfig) *VisibilityWriter {
	if cfg.FlushEntries <= 0 {
		cfg.FlushEntries = DefaultFlushDocs
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &VisibilityWriter{
		cfg:      cfg,
		store:    cfg.Store,
		memtable: newVisibilityMemTable(),
		ctx:      ctx,
		cancel:   cancel,
		clock:    cfg.Clock,
		logger:   cfg.Logger,
	}
	w.lastFlush.Store(w.clock.Now().UnixNano())
	go w.flushLoop()
	return w
}

// Close stops the writer and flushes pending data.
func (w *VisibilityWriter) Close(ctx context.Context) error {
	w.cancel()
	return w.Flush(ctx)
}

// Update records a visibility change.
func (w *VisibilityWriter) Update(key string, visible bool) error {
	if key == "" {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	before := w.memtable.Len()
	w.memtable.Set(key, visible)
	after := w.memtable.Len()
	if before == 0 && after > 0 {
		w.oldestPending.Store(w.clock.Now().UnixNano())
	}
	w.pending.Store(true)
	if w.memtable.Len() >= w.cfg.FlushEntries {
		return w.flushLocked(context.Background())
	}
	return nil
}

// Flush forces a flush of pending visibility entries.
func (w *VisibilityWriter) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked(ctx)
}

func (w *VisibilityWriter) flushLoop() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.clock.After(w.cfg.FlushInterval):
			_ = w.Flush(context.Background())
		}
	}
}

func (w *VisibilityWriter) flushLocked(ctx context.Context) error {
	if w.memtable.Len() == 0 {
		w.pending.Store(false)
		w.oldestPending.Store(0)
		return nil
	}
	segment := w.memtable.Flush(w.clock.Now())
	if len(segment.Entries) == 0 {
		w.pending.Store(false)
		w.oldestPending.Store(0)
		return nil
	}
	if err := w.persistSegment(ctx, segment); err != nil {
		if w.logger != nil {
			w.logger.Warn("visibility.flush.error", "namespace", w.cfg.Namespace, "error", err)
		}
		return err
	}
	w.pending.Store(false)
	w.oldestPending.Store(0)
	w.lastFlush.Store(w.clock.Now().UnixNano())
	w.signalReadableLocked()
	if w.logger != nil {
		w.logger.Debug("visibility.flush.success", "namespace", w.cfg.Namespace, "segment", segment.ID, "entries", len(segment.Entries))
	}
	return nil
}

func (w *VisibilityWriter) persistSegment(ctx context.Context, segment *VisibilitySegment) error {
	if w.store == nil {
		return nil
	}
	if _, _, err := w.store.WriteVisibilitySegment(ctx, w.cfg.Namespace, segment); err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt < manifestSaveMaxRetries; attempt++ {
		manifestRes, err := w.store.LoadVisibilityManifest(ctx, w.cfg.Namespace)
		if err != nil {
			return err
		}
		manifest := manifestRes.Manifest
		etag := manifestRes.ETag
		manifest.Segments = append([]VisibilitySegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			Entries:   uint64(len(segment.Entries)),
		}}, manifest.Segments...)
		manifest.Seq++
		manifest.UpdatedAt = segment.CreatedAt
		if _, err = w.store.SaveVisibilityManifest(ctx, w.cfg.Namespace, manifest, etag); err == nil {
			lastErr = nil
			w.store.applyVisibilitySegment(w.cfg.Namespace, segment, manifest.Seq, manifest.UpdatedAt)
			break
		}
		lastErr = err
		if !errors.Is(err, storage.ErrCASMismatch) {
			return err
		}
		if attempt+1 < manifestSaveMaxRetries {
			w.clock.Sleep(manifestSaveRetryDelay)
		}
	}
	return lastErr
}

// WaitForReadable waits (best-effort) for pending visibility updates to flush.
func (w *VisibilityWriter) WaitForReadable(ctx context.Context) error {
	if w == nil || !w.pending.Load() {
		return nil
	}
	wait := w.cfg.FlushInterval
	if wait <= 0 {
		wait = DefaultFlushInterval
	}
	deadline := w.clock.After(wait)
	for {
		if !w.pending.Load() {
			return nil
		}
		ch := w.readableChan()
		if ch == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return nil
		case <-ch:
		}
	}
}

// HasPending reports whether buffered visibility updates are pending.
func (w *VisibilityWriter) HasPending() bool {
	if w == nil {
		return false
	}
	return w.pending.Load()
}

func (w *VisibilityWriter) readableChan() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.memtable.Len() == 0 {
		w.pending.Store(false)
		w.oldestPending.Store(0)
		return nil
	}
	if w.readableCh == nil {
		w.readableCh = make(chan struct{})
	}
	return w.readableCh
}

func (w *VisibilityWriter) signalReadableLocked() {
	if w.readableCh != nil {
		close(w.readableCh)
		w.readableCh = nil
	}
}
