package index

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

type putNoSyncRecord struct {
	key    string
	noSync bool
}

type noSyncRecordingBackend struct {
	*memory.Store
	mu      sync.Mutex
	records []putNoSyncRecord
}

func (b *noSyncRecordingBackend) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	b.mu.Lock()
	b.records = append(b.records, putNoSyncRecord{
		key:    key,
		noSync: storage.NoSyncFromContext(ctx),
	})
	b.mu.Unlock()
	return b.Store.PutObject(ctx, namespace, key, body, opts)
}

func (b *noSyncRecordingBackend) assertPrefixNoSync(t *testing.T, prefix string, want bool) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	seen := false
	for _, record := range b.records {
		if !strings.HasPrefix(record.key, prefix) {
			continue
		}
		seen = true
		if record.noSync != want {
			t.Fatalf("expected %q no_sync=%v, got %v", record.key, want, record.noSync)
		}
	}
	if !seen {
		t.Fatalf("expected at least one put with prefix %q", prefix)
	}
}

func TestWriterNoSyncPropagation(t *testing.T) {
	tests := []struct {
		name   string
		noSync bool
	}{
		{name: "enabled", noSync: true},
		{name: "disabled", noSync: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &noSyncRecordingBackend{Store: memory.New()}
			idxStore := NewStore(backend, nil)
			writer := NewWriter(WriterConfig{
				Namespace:     "writer-nosync",
				Store:         idxStore,
				FlushDocs:     1,
				FlushInterval: time.Minute,
				NoSync:        tc.noSync,
				Logger:        pslog.NewStructured(context.Background(), io.Discard),
			})
			t.Cleanup(func() { _ = writer.Close(context.Background()) })
			writer.cancel()

			if err := writer.Insert(Document{
				Key:    "doc-1",
				Fields: map[string][]string{"/status": {"open"}},
			}); err != nil {
				t.Fatalf("insert: %v", err)
			}

			backend.assertPrefixNoSync(t, segmentPrefix+"/", tc.noSync)
			backend.assertPrefixNoSync(t, manifestObject, tc.noSync)
		})
	}
}

func TestVisibilityWriterNoSyncPropagation(t *testing.T) {
	tests := []struct {
		name   string
		noSync bool
	}{
		{name: "enabled", noSync: true},
		{name: "disabled", noSync: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &noSyncRecordingBackend{Store: memory.New()}
			idxStore := NewStore(backend, nil)
			writer := NewVisibilityWriter(VisibilityWriterConfig{
				Namespace:     "visibility-nosync",
				Store:         idxStore,
				FlushEntries:  1,
				FlushInterval: time.Minute,
				NoSync:        tc.noSync,
				Logger:        pslog.NewStructured(context.Background(), io.Discard),
			})
			t.Cleanup(func() { _ = writer.Close(context.Background()) })
			writer.cancel()

			if err := writer.Update("doc-1", true); err != nil {
				t.Fatalf("update visibility: %v", err)
			}

			backend.assertPrefixNoSync(t, visibilitySegmentDir+"/", tc.noSync)
			backend.assertPrefixNoSync(t, visibilityManifest, tc.noSync)
		})
	}
}
