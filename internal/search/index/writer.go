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
	cfg           WriterConfig
	store         *Store
	memtable      *MemTable
	mu            sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	clock         clock.Clock
	logger        pslog.Logger
	pending       atomic.Bool
	lastFlush     atomic.Int64
	oldestPending atomic.Int64
	readableCh    chan struct{}
	metrics       *indexMetrics
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
	w.metrics = newIndexMetrics(cfg.Logger)
	if w.metrics != nil {
		w.metrics.registerWriter(w)
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
	before := w.memtable.DocCount()
	for field, terms := range doc.Fields {
		for _, term := range terms {
			w.memtable.Add(field, term, doc.Key)
		}
	}
	if doc.Meta != nil {
		meta := DocumentMetadata{
			StateETag:           doc.Meta.StateETag,
			StatePlaintextBytes: doc.Meta.StatePlaintextBytes,
			PublishedVersion:    doc.Meta.PublishedVersion,
		}
		if len(doc.Meta.StateDescriptor) > 0 {
			meta.StateDescriptor = append([]byte(nil), doc.Meta.StateDescriptor...)
		}
		w.memtable.SetMeta(doc.Key, meta)
	}
	after := w.memtable.DocCount()
	if before == 0 && after > 0 {
		w.oldestPending.Store(w.clock.Now().UnixNano())
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
		w.oldestPending.Store(0)
		return nil
	}
	segment := w.memtable.Flush(w.clock.Now())
	if segment.DocCount() == 0 {
		w.pending.Store(false)
		w.oldestPending.Store(0)
		return nil
	}
	start := w.clock.Now()
	segmentBytes, err := w.persistSegment(ctx, segment)
	if w.metrics != nil {
		w.metrics.recordFlush(ctx, w.cfg.Namespace, w.clock.Now().Sub(start), err, int64(segment.DocCount()), segmentBytes)
	}
	if err != nil {
		if w.logger != nil {
			w.logger.Warn("index.flush.error", "namespace", w.cfg.Namespace, "error", err)
		}
		return err
	}
	w.pending.Store(false)
	w.oldestPending.Store(0)
	w.lastFlush.Store(w.clock.Now().UnixNano())
	w.signalReadableLocked()
	if w.logger != nil {
		w.logger.Debug("index.flush.success", "namespace", w.cfg.Namespace, "segment", segment.ID, "docs", segment.DocCount())
	}
	return nil
}

func (w *Writer) persistSegment(ctx context.Context, segment *Segment) (int64, error) {
	if w.store == nil {
		return 0, storage.ErrNotImplemented
	}
	manifestRes, err := w.store.LoadManifest(ctx, w.cfg.Namespace)
	if err != nil {
		return 0, err
	}
	targetFormat := uint32(IndexFormatVersionV4)
	if manifest := manifestRes.Manifest; manifest != nil && manifest.Format >= IndexFormatVersionV4 {
		targetFormat = manifest.Format
	}
	segment.Format = targetFormat

	_, segmentBytes, err := w.store.WriteSegment(ctx, w.cfg.Namespace, segment)
	if err != nil {
		return 0, err
	}
	var lastErr error
	for attempt := 0; attempt < manifestSaveMaxRetries; attempt++ {
		manifestRes, err := w.store.LoadManifest(ctx, w.cfg.Namespace)
		if err != nil {
			return 0, err
		}
		manifest := manifestRes.Manifest
		etag := manifestRes.ETag
		if manifest != nil && manifest.Format == 0 {
			manifest.Format = targetFormat
		}
		if manifest == nil || len(manifest.Shards) == 0 {
			manifest = NewManifest()
			manifest.Format = targetFormat
		}
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
		if _, err = w.store.SaveManifest(ctx, w.cfg.Namespace, manifest, etag); err == nil {
			lastErr = nil
			break
		}
		lastErr = err
		if !errors.Is(err, storage.ErrCASMismatch) {
			return 0, err
		}
		if attempt+1 < manifestSaveMaxRetries {
			w.clock.Sleep(manifestSaveRetryDelay)
		}
	}
	if lastErr != nil {
		return 0, lastErr
	}
	return segmentBytes, nil
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
		w.oldestPending.Store(0)
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

func (w *Writer) metricsSnapshot(now time.Time) (depth int64, lagMs int64) {
	if w == nil {
		return 0, 0
	}
	w.mu.Lock()
	depth = int64(w.memtable.DocCount())
	w.mu.Unlock()
	if depth == 0 {
		return 0, 0
	}
	oldest := w.oldestPending.Load()
	if oldest == 0 {
		return depth, 0
	}
	lag := now.Sub(time.Unix(0, oldest)).Milliseconds()
	if lag < 0 {
		lag = 0
	}
	return depth, lag
}
