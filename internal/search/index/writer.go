package index

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

// WriterConfig controls a single namespace writer.
type WriterConfig struct {
	Namespace     string
	Store         *Store
	FlushDocs     int
	FlushInterval time.Duration
	Clock         clock.Clock
	Logger        pslog.Logger
}

// Writer ingests documents and flushes them into immutable segments.
type Writer struct {
	cfg        WriterConfig
	store      *Store
	memtable   *MemTable
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	clock      clock.Clock
	logger     pslog.Logger
	pending    atomic.Bool
	lastFlush  atomic.Int64
	readableCh chan struct{}
}

// NewWriter constructs a namespace writer.
func NewWriter(cfg WriterConfig) *Writer {
	if cfg.FlushDocs <= 0 {
		cfg.FlushDocs = DefaultFlushDocs
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		cfg:      cfg,
		store:    cfg.Store,
		memtable: NewMemTable(),
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
func (w *Writer) Close(ctx context.Context) error {
	w.cancel()
	return w.Flush(ctx)
}

// Insert ingests a document.
func (w *Writer) Insert(doc Document) error {
	if doc.Key == "" {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for field, terms := range doc.Fields {
		for _, term := range terms {
			w.memtable.Add(field, term, doc.Key)
		}
	}
	w.pending.Store(true)
	if int(w.memtable.DocCount()) >= w.cfg.FlushDocs {
		return w.flushLocked(context.Background())
	}
	return nil
}

// Flush forces a flush of the memtable.
func (w *Writer) Flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked(ctx)
}

func (w *Writer) flushLoop() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.clock.After(w.cfg.FlushInterval):
			_ = w.Flush(context.Background())
		}
	}
}

func (w *Writer) flushLocked(ctx context.Context) error {
	if w.memtable.DocCount() == 0 {
		w.pending.Store(false)
		return nil
	}
	segment := w.memtable.Flush(w.clock.Now())
	if segment.DocCount() == 0 {
		w.pending.Store(false)
		return nil
	}
	if err := w.persistSegment(ctx, segment); err != nil {
		if w.logger != nil {
			w.logger.Warn("index.flush.error", "namespace", w.cfg.Namespace, "error", err)
		}
		return err
	}
	w.pending.Store(false)
	w.lastFlush.Store(w.clock.Now().UnixNano())
	w.signalReadableLocked()
	if w.logger != nil {
		w.logger.Debug("index.flush.success", "namespace", w.cfg.Namespace, "segment", segment.ID, "docs", segment.DocCount())
	}
	return nil
}

func (w *Writer) persistSegment(ctx context.Context, segment *Segment) error {
	if w.store == nil {
		return storage.ErrNotImplemented
	}
	if _, err := w.store.WriteSegment(ctx, w.cfg.Namespace, segment); err != nil {
		return err
	}
	manifestRes, err := w.store.LoadManifest(ctx, w.cfg.Namespace)
	if err != nil {
		return err
	}
	manifest := manifestRes.Manifest
	etag := manifestRes.ETag
	shard := manifest.Shards[0]
	if shard == nil {
		shard = &Shard{ID: 0}
		manifest.Shards[0] = shard
	}
	shard.Segments = append([]SegmentRef{{
		ID:        segment.ID,
		CreatedAt: segment.CreatedAt,
		DocCount:  segment.DocCount(),
	}}, shard.Segments...)
	manifest.Seq++
	manifest.UpdatedAt = segment.CreatedAt
	_, err = w.store.SaveManifest(ctx, w.cfg.Namespace, manifest, etag)
	return err
}

// WaitForReadable waits (best-effort) until pending documents are flushed or
// the writer's flush interval elapses.
func (w *Writer) WaitForReadable(ctx context.Context) error {
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

// HasPending reports whether buffered documents still need flushing.
func (w *Writer) HasPending() bool {
	if w == nil {
		return false
	}
	return w.pending.Load()
}

func (w *Writer) readableChan() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.memtable.DocCount() == 0 {
		w.pending.Store(false)
		return nil
	}
	if w.readableCh == nil {
		w.readableCh = make(chan struct{})
	}
	return w.readableCh
}

func (w *Writer) signalReadableLocked() {
	if w.readableCh != nil {
		close(w.readableCh)
		w.readableCh = nil
	}
}
