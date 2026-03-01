package index

import (
	"context"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

type blockingSegmentBackend struct {
	*memory.Store
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type trackingSegmentBackend struct {
	*memory.Store
	delay         time.Duration
	activeWrites  atomic.Int64
	maxConcurrent atomic.Int64
}

func (b *blockingSegmentBackend) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	if strings.HasPrefix(key, "index/segments/") {
		b.once.Do(func() {
			close(b.started)
		})
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.Store.PutObject(ctx, namespace, key, body, opts)
}

func (b *trackingSegmentBackend) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	if strings.HasPrefix(key, "index/segments/") {
		cur := b.activeWrites.Add(1)
		for {
			prev := b.maxConcurrent.Load()
			if cur <= prev || b.maxConcurrent.CompareAndSwap(prev, cur) {
				break
			}
		}
		if b.delay > 0 {
			timer := time.NewTimer(b.delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				b.activeWrites.Add(-1)
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		b.activeWrites.Add(-1)
	}
	return b.Store.PutObject(ctx, namespace, key, body, opts)
}

func TestWriterFlushByCount(t *testing.T) {
	memStore := memory.New()
	idxStore := NewStore(memStore, nil)
	writer := NewWriter(WriterConfig{
		Namespace:     "default",
		Store:         idxStore,
		FlushDocs:     2,
		FlushInterval: time.Minute,
		Logger:        pslog.NewStructured(context.Background(), io.Discard),
	})
	defer writer.Close(context.Background())
	doc := Document{Key: "doc-1", Fields: map[string][]string{"status": {"open"}}}
	if err := writer.Insert(doc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := writer.Insert(Document{Key: "doc-2", Fields: map[string][]string{"status": {"open"}}}); err != nil {
		t.Fatalf("insert2: %v", err)
	}
	waitForManifestSegments(t, idxStore, "default", 1, time.Second)
}

func TestWriterFlushUsesManifestFormat(t *testing.T) {
	ctx := context.Background()
	memStore := memory.New()
	idxStore := NewStore(memStore, nil)

	manifest := NewManifest()
	manifest.Format = IndexFormatVersionV5
	manifest.Seq = 1
	manifest.UpdatedAt = time.Unix(1_700_000_100, 0).UTC()
	manifest.Shards[0] = &Shard{ID: 0}
	if _, err := idxStore.SaveManifest(ctx, "default", manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	writer := NewWriter(WriterConfig{
		Namespace:     "default",
		Store:         idxStore,
		FlushDocs:     1,
		FlushInterval: time.Minute,
		Logger:        pslog.NewStructured(context.Background(), io.Discard),
	})
	defer writer.Close(context.Background())

	if err := writer.Insert(Document{
		Key:    "doc-v5",
		Fields: map[string][]string{"/status": {"open"}},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	manifestRes := waitForManifestSegments(t, idxStore, "default", 1, time.Second)
	if manifestRes.Manifest == nil {
		t.Fatalf("expected manifest")
	}
	if manifestRes.Manifest.Format != IndexFormatVersionV5 {
		t.Fatalf("expected manifest format v5, got %d", manifestRes.Manifest.Format)
	}
	shard := manifestRes.Manifest.Shards[0]
	loaded, err := idxStore.LoadSegment(ctx, "default", shard.Segments[0].ID)
	if err != nil {
		t.Fatalf("load segment: %v", err)
	}
	if loaded.Format != IndexFormatVersionV5 {
		t.Fatalf("expected written segment format v5, got %d", loaded.Format)
	}
}

func TestWriterWaitForReadableReturnsAfterFlush(t *testing.T) {
	clk := clock.NewManual(time.Now().UTC())
	memStore := memory.New()
	idxStore := NewStore(memStore, nil)
	writer := NewWriter(WriterConfig{
		Namespace:     "waitable",
		Store:         idxStore,
		FlushDocs:     100,
		FlushInterval: 200 * time.Millisecond,
		Clock:         clk,
		Logger:        pslog.NewStructured(context.Background(), io.Discard),
	})
	defer writer.Close(context.Background())
	writer.cancel() // disable background flush loop to control timing

	doc := Document{Key: "pending", Fields: map[string][]string{"status": {"open"}}}
	if err := writer.Insert(doc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- writer.WaitForReadable(ctx)
	}()
	select {
	case err := <-done:
		t.Fatalf("wait returned before flush: %v", err)
	default:
	}
	if err := writer.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("wait did not return after flush")
	}
}

func TestWriterWaitForReadableTimesOut(t *testing.T) {
	clk := clock.NewManual(time.Now().UTC())
	memStore := memory.New()
	idxStore := NewStore(memStore, nil)
	writer := NewWriter(WriterConfig{
		Namespace:     "wait-timeout",
		Store:         idxStore,
		FlushDocs:     100,
		FlushInterval: 40 * time.Millisecond,
		Clock:         clk,
		Logger:        pslog.NewStructured(context.Background(), io.Discard),
	})
	defer writer.Close(context.Background())
	writer.cancel()

	if err := writer.Insert(Document{Key: "pending-timeout", Fields: map[string][]string{"kind": {"test"}}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- writer.WaitForReadable(ctx)
	}()
	select {
	case err := <-done:
		t.Fatalf("wait returned before timeout: %v", err)
	default:
	}
	if err := advanceManualUntilDone(t, clk, done, 5*time.Millisecond, 200*time.Millisecond); err != nil {
		t.Fatalf("wait: %v", err)
	}
}

func TestWriterInsertNotBlockedByFlushPersistence(t *testing.T) {
	backend := &blockingSegmentBackend{
		Store:   memory.New(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	idxStore := NewStore(backend, nil)
	writer := NewWriter(WriterConfig{
		Namespace:     "flush-handoff",
		Store:         idxStore,
		FlushDocs:     100,
		FlushInterval: time.Minute,
		Logger:        pslog.NewStructured(context.Background(), io.Discard),
	})
	defer writer.Close(context.Background())
	writer.cancel()

	if err := writer.Insert(Document{Key: "doc-1", Fields: map[string][]string{"status": {"open"}}}); err != nil {
		t.Fatalf("insert doc-1: %v", err)
	}

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- writer.Flush(context.Background())
	}()

	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatalf("flush did not start segment persistence")
	}

	insertDone := make(chan error, 1)
	go func() {
		insertDone <- writer.Insert(Document{Key: "doc-2", Fields: map[string][]string{"status": {"open"}}})
	}()

	select {
	case err := <-insertDone:
		if err != nil {
			t.Fatalf("insert doc-2: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("insert doc-2 blocked while flush persistence in progress")
	}

	close(backend.release)
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("flush did not complete after releasing backend")
	}
}

func TestWriterSerializesSegmentPersistence(t *testing.T) {
	backend := &trackingSegmentBackend{
		Store: memory.New(),
		delay: 50 * time.Millisecond,
	}
	idxStore := NewStore(backend, nil)
	writer := NewWriter(WriterConfig{
		Namespace:     "flush-serialize",
		Store:         idxStore,
		FlushDocs:     1,
		FlushInterval: time.Minute,
		Logger:        pslog.NewStructured(context.Background(), io.Discard),
	})
	defer writer.Close(context.Background())
	writer.cancel()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := writer.Insert(Document{Key: "doc-" + string(rune('a'+i)), Fields: map[string][]string{"status": {"open"}}}); err != nil {
				t.Errorf("insert doc-%d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	waitForManifestSegments(t, idxStore, "flush-serialize", 1, 2*time.Second)

	if got := backend.maxConcurrent.Load(); got > 1 {
		t.Fatalf("expected segment persistence to be serialized, max concurrent=%d", got)
	}
}

func waitForManifestSegments(t *testing.T, idxStore *Store, namespace string, minSegments int, timeout time.Duration) ManifestLoadResult {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		manifestRes, err := idxStore.LoadManifest(context.Background(), namespace)
		if err != nil {
			t.Fatalf("load manifest: %v", err)
		}
		shard := manifestRes.Manifest.Shards[0]
		if shard != nil && len(shard.Segments) >= minSegments {
			return manifestRes
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected at least %d segments in namespace %q within %s", minSegments, namespace, timeout)
	return ManifestLoadResult{}
}

func advanceManualUntilDone(t *testing.T, clk *clock.Manual, done <-chan error, step, limit time.Duration) error {
	t.Helper()
	if clk == nil {
		t.Fatalf("manual clock required")
	}
	if step <= 0 {
		step = 5 * time.Millisecond
	}
	if limit <= 0 {
		limit = 200 * time.Millisecond
	}
	deadline := clk.Now().Add(limit)
	for {
		select {
		case err := <-done:
			return err
		default:
		}
		if clk.Now().After(deadline) {
			t.Fatalf("wait did not return after manual timeout")
		}
		clk.Advance(step)
		runtime.Gosched()
	}
}
