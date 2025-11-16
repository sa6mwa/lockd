package index

import (
	"context"
	"io"
	"testing"
	"time"

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
		Logger:        pslog.NewStructured(io.Discard),
	})
	defer writer.Close(context.Background())
	doc := Document{Key: "doc-1", Fields: map[string][]string{"status": {"open"}}}
	if err := writer.Insert(doc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := writer.Insert(Document{Key: "doc-2", Fields: map[string][]string{"status": {"open"}}}); err != nil {
		t.Fatalf("insert2: %v", err)
	}
	manifest, _, err := idxStore.LoadManifest(context.Background(), "default")
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	shard := manifest.Shards[0]
	if shard == nil || len(shard.Segments) == 0 {
		t.Fatalf("expected segments")
	}
}

func TestWriterWaitForReadableReturnsAfterFlush(t *testing.T) {
	memStore := memory.New()
	idxStore := NewStore(memStore, nil)
	writer := NewWriter(WriterConfig{
		Namespace:     "waitable",
		Store:         idxStore,
		FlushDocs:     100,
		FlushInterval: 200 * time.Millisecond,
		Logger:        pslog.NewStructured(io.Discard),
	})
	defer writer.Close(context.Background())
	writer.cancel() // disable background flush loop to control timing
	writer.ticker.Stop()

	doc := Document{Key: "pending", Fields: map[string][]string{"status": {"open"}}}
	if err := writer.Insert(doc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	done := make(chan struct{})
	go func() {
		time.Sleep(25 * time.Millisecond)
		_ = writer.Flush(context.Background())
		close(done)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := writer.WaitForReadable(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatalf("wait returned too early, pending documents likely still buffered")
	}
	select {
	case <-done:
	default:
		t.Fatalf("flush did not complete")
	}
}

func TestWriterWaitForReadableTimesOut(t *testing.T) {
	memStore := memory.New()
	idxStore := NewStore(memStore, nil)
	writer := NewWriter(WriterConfig{
		Namespace:     "wait-timeout",
		Store:         idxStore,
		FlushDocs:     100,
		FlushInterval: 40 * time.Millisecond,
		Logger:        pslog.NewStructured(io.Discard),
	})
	defer writer.Close(context.Background())
	writer.cancel()
	writer.ticker.Stop()

	if err := writer.Insert(Document{Key: "pending-timeout", Fields: map[string][]string{"kind": {"test"}}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := writer.WaitForReadable(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Fatalf("expected wait to last at least flush interval, got %v", elapsed)
	}
}
