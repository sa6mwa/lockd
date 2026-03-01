package index

import (
	"context"
	"io"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

func TestManagerWaitForReadableFlushesPendingWriter(t *testing.T) {
	ctx := context.Background()
	store := NewStore(memory.New(), nil)
	manager := NewManager(store, WriterOptions{
		FlushDocs:     1000,
		FlushInterval: 30 * time.Second,
		Logger:        pslog.NewStructured(context.Background(), io.Discard),
	})
	t.Cleanup(func() { manager.Close(context.Background()) })

	if err := manager.Insert("wait-ns", Document{
		Key:    "doc-1",
		Fields: map[string][]string{"/kind": {"order"}},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := manager.WaitForReadable(waitCtx, "wait-ns"); err != nil {
		t.Fatalf("wait for readable: %v", err)
	}
	if manager.Pending("wait-ns") {
		t.Fatalf("expected no pending entries after wait_for flush")
	}

	manifestRes, err := store.LoadManifest(ctx, "wait-ns")
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	shard := manifestRes.Manifest.Shards[0]
	if shard == nil || len(shard.Segments) == 0 {
		t.Fatalf("expected flushed segment after wait_for")
	}
}

func TestManagerWaitForReadableFlushesPendingVisibility(t *testing.T) {
	ctx := context.Background()
	store := NewStore(memory.New(), nil)
	manager := NewManager(store, WriterOptions{
		FlushDocs:     1000,
		FlushInterval: 30 * time.Second,
		Logger:        pslog.NewStructured(context.Background(), io.Discard),
	})
	t.Cleanup(func() { manager.Close(context.Background()) })

	if err := manager.UpdateVisibility("vis-wait-ns", "doc-1", false); err != nil {
		t.Fatalf("update visibility: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := manager.WaitForReadable(waitCtx, "vis-wait-ns"); err != nil {
		t.Fatalf("wait for readable: %v", err)
	}
	if manager.Pending("vis-wait-ns") {
		t.Fatalf("expected no pending visibility entries after wait_for")
	}

	manifestRes, err := store.LoadVisibilityManifest(ctx, "vis-wait-ns")
	if err != nil {
		t.Fatalf("load visibility manifest: %v", err)
	}
	if len(manifestRes.Manifest.Segments) == 0 {
		t.Fatalf("expected visibility segment after wait_for")
	}
}
