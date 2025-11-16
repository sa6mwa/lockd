package index

import (
	"context"
	"fmt"
	"sync"
	"time"

	"pkt.systems/pslog"
)

// Manager orchestrates namespace writers and provides shared configuration.
type Manager struct {
	store     *Store
	options   WriterOptions
	writersMu sync.Mutex
	writers   map[string]*Writer
}

// WriterOptions tunes all namespace writers.
type WriterOptions struct {
	FlushDocs     int
	FlushInterval time.Duration
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
	return &Manager{
		store:   store,
		options: opts,
		writers: make(map[string]*Writer),
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

// Insert inserts a document into the namespace writer.
func (m *Manager) Insert(namespace string, doc Document) error {
	writer := m.WriterFor(namespace)
	if writer == nil {
		return nil
	}
	return writer.Insert(doc)
}

// WaitForReadable waits (best-effort) for pending documents in the namespace
// to flush so queries observe the latest index state.
func (m *Manager) WaitForReadable(ctx context.Context, namespace string) error {
	writer := m.writer(namespace)
	if writer == nil {
		return nil
	}
	return writer.WaitForReadable(ctx)
}

// Pending reports whether the namespace has unflushed documents.
func (m *Manager) Pending(namespace string) bool {
	writer := m.writer(namespace)
	if writer == nil {
		return false
	}
	return writer.HasPending()
}

// ManifestSeq loads the manifest and reports its sequence number.
func (m *Manager) ManifestSeq(ctx context.Context, namespace string) (uint64, error) {
	if m == nil || m.store == nil {
		return 0, fmt.Errorf("index store unavailable")
	}
	manifest, _, err := m.store.LoadManifest(ctx, namespace)
	if err != nil {
		return 0, err
	}
	if manifest == nil {
		return 0, nil
	}
	return manifest.Seq, nil
}

// FlushNamespace forces a flush for the namespace.
func (m *Manager) FlushNamespace(ctx context.Context, namespace string) error {
	m.writersMu.Lock()
	writer := m.writers[namespace]
	m.writersMu.Unlock()
	if writer == nil {
		return nil
	}
	return writer.Flush(ctx)
}

// Close stops all writers.
func (m *Manager) Close(ctx context.Context) {
	m.writersMu.Lock()
	writers := make([]*Writer, 0, len(m.writers))
	for _, w := range m.writers {
		writers = append(writers, w)
	}
	m.writersMu.Unlock()
	for _, w := range writers {
		_ = w.Close(ctx)
	}
}
