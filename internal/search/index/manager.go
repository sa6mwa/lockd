package index

import (
	"context"
	"fmt"
	"sync"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/pslog"
)

// Manager orchestrates namespace writers and provides shared configuration.
type Manager struct {
	store     *Store
	options   WriterOptions
	writersMu sync.Mutex
	writers   map[string]*Writer
	visMu     sync.Mutex
	vis       map[string]*VisibilityWriter
}

// WriterOptions tunes all namespace writers.
type WriterOptions struct {
	FlushDocs     int
	FlushInterval time.Duration
	Clock         clock.Clock
	Logger        pslog.Logger
}

// NewManager builds an index manager.
func NewManager(store *Store, opts WriterOptions) *Manager {
	if opts.FlushDocs <= 0 {
		opts.FlushDocs = DefaultFlushDocs
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultFlushInterval
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	return &Manager{
		store:   store,
		options: opts,
		writers: make(map[string]*Writer),
		vis:     make(map[string]*VisibilityWriter),
	}
}

// WriterFor returns (or creates) the writer for namespace.
func (m *Manager) WriterFor(namespace string) *Writer {
	if m == nil {
		return nil
	}
	m.writersMu.Lock()
	defer m.writersMu.Unlock()
	if writer, ok := m.writers[namespace]; ok {
		return writer
	}
	writer := NewWriter(WriterConfig{
		Namespace:     namespace,
		Store:         m.store,
		FlushDocs:     m.options.FlushDocs,
		FlushInterval: m.options.FlushInterval,
		Clock:         m.options.Clock,
		Logger:        m.options.Logger,
	})
	m.writers[namespace] = writer
	return writer
}

func (m *Manager) writer(namespace string) *Writer {
	if m == nil {
		return nil
	}
	m.writersMu.Lock()
	defer m.writersMu.Unlock()
	return m.writers[namespace]
}

// visibilityWriter returns (or creates) the visibility writer for namespace.
func (m *Manager) visibilityWriter(namespace string) *VisibilityWriter {
	if m == nil {
		return nil
	}
	m.visMu.Lock()
	defer m.visMu.Unlock()
	if writer, ok := m.vis[namespace]; ok {
		return writer
	}
	writer := NewVisibilityWriter(VisibilityWriterConfig{
		Namespace:     namespace,
		Store:         m.store,
		FlushEntries:  m.options.FlushDocs,
		FlushInterval: m.options.FlushInterval,
		Clock:         m.options.Clock,
		Logger:        m.options.Logger,
	})
	m.vis[namespace] = writer
	return writer
}

// Insert inserts a document into the namespace writer.
func (m *Manager) Insert(namespace string, doc Document) error {
	writer := m.WriterFor(namespace)
	if writer == nil {
		return nil
	}
	return writer.Insert(doc)
}

// UpdateVisibility records a visibility decision for a key.
func (m *Manager) UpdateVisibility(namespace, key string, visible bool) error {
	writer := m.visibilityWriter(namespace)
	if writer == nil {
		return nil
	}
	return writer.Update(key, visible)
}

// WaitForReadable waits (best-effort) for pending documents in the namespace
// to flush so queries observe the latest index state.
func (m *Manager) WaitForReadable(ctx context.Context, namespace string) error {
	writer := m.writer(namespace)
	if writer == nil {
		return nil
	}
	if err := writer.WaitForReadable(ctx); err != nil {
		return err
	}
	if vis := m.visibilityWriter(namespace); vis != nil {
		return vis.WaitForReadable(ctx)
	}
	return nil
}

// Pending reports whether the namespace has unflushed documents.
func (m *Manager) Pending(namespace string) bool {
	writer := m.writer(namespace)
	pending := false
	if writer != nil {
		pending = pending || writer.HasPending()
	}
	if vis := m.visibilityWriter(namespace); vis != nil {
		pending = pending || vis.HasPending()
	}
	return pending
}

// ManifestSeq loads the manifest and reports its sequence number.
func (m *Manager) ManifestSeq(ctx context.Context, namespace string) (uint64, error) {
	if m == nil || m.store == nil {
		return 0, fmt.Errorf("index store unavailable")
	}
	manifestRes, err := m.store.LoadManifest(ctx, namespace)
	if err != nil {
		return 0, err
	}
	manifest := manifestRes.Manifest
	if manifest == nil {
		return 0, nil
	}
	return manifest.Seq, nil
}

// LoadManifest exposes the namespace manifest.
func (m *Manager) LoadManifest(ctx context.Context, namespace string) (ManifestLoadResult, error) {
	if m == nil || m.store == nil {
		return ManifestLoadResult{}, fmt.Errorf("index store unavailable")
	}
	return m.store.LoadManifest(ctx, namespace)
}

// SaveManifest persists the manifest with CAS semantics.
func (m *Manager) SaveManifest(ctx context.Context, namespace string, manifest *Manifest, expectedETag string) (string, error) {
	if m == nil || m.store == nil {
		return "", fmt.Errorf("index store unavailable")
	}
	return m.store.SaveManifest(ctx, namespace, manifest, expectedETag)
}

// LoadSegment loads a segment by ID.
func (m *Manager) LoadSegment(ctx context.Context, namespace, segmentID string) (*Segment, error) {
	if m == nil || m.store == nil {
		return nil, fmt.Errorf("index store unavailable")
	}
	return m.store.LoadSegment(ctx, namespace, segmentID)
}

// DeleteSegment removes a segment by ID.
func (m *Manager) DeleteSegment(ctx context.Context, namespace, segmentID string) error {
	if m == nil || m.store == nil {
		return fmt.Errorf("index store unavailable")
	}
	return m.store.DeleteSegment(ctx, namespace, segmentID)
}

// WarmNamespace preloads index and visibility artifacts for a namespace.
func (m *Manager) WarmNamespace(ctx context.Context, namespace string) error {
	if m == nil || m.store == nil {
		return nil
	}
	return m.store.WarmNamespace(ctx, namespace)
}

// FlushNamespace forces a flush for the namespace.
func (m *Manager) FlushNamespace(ctx context.Context, namespace string) error {
	m.writersMu.Lock()
	writer := m.writers[namespace]
	m.writersMu.Unlock()
	if writer != nil {
		if err := writer.Flush(ctx); err != nil {
			return err
		}
	}
	if vis := m.visibilityWriter(namespace); vis != nil {
		return vis.Flush(ctx)
	}
	return nil
}

// Close stops all writers.
func (m *Manager) Close(ctx context.Context) {
	m.writersMu.Lock()
	writers := make([]*Writer, 0, len(m.writers))
	for _, w := range m.writers {
		writers = append(writers, w)
	}
	m.writersMu.Unlock()
	m.visMu.Lock()
	visWriters := make([]*VisibilityWriter, 0, len(m.vis))
	for _, v := range m.vis {
		visWriters = append(visWriters, v)
	}
	m.visMu.Unlock()
	for _, w := range writers {
		_ = w.Close(ctx)
	}
	for _, v := range visWriters {
		_ = v.Close(ctx)
	}
}
