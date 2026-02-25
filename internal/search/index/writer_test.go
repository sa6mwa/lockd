package index

import (
	"context"
	"io"
	"runtime"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

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
	manifestRes, err := idxStore.LoadManifest(context.Background(), "default")
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	shard := manifestRes.Manifest.Shards[0]
	if shard == nil || len(shard.Segments) == 0 {
		t.Fatalf("expected segments")
	}
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

	manifestRes, err := idxStore.LoadManifest(ctx, "default")
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifestRes.Manifest.Format != IndexFormatVersionV5 {
		t.Fatalf("expected manifest format v5, got %d", manifestRes.Manifest.Format)
	}
	shard := manifestRes.Manifest.Shards[0]
	if shard == nil || len(shard.Segments) == 0 {
		t.Fatalf("expected flushed segment in manifest")
	}
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
