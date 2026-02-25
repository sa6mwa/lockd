package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

type rebuildHarness struct {
	svc      *Service
	store    storage.Backend
	idxMgr   *indexer.Manager
	idxStore *indexer.Store
}

func newRebuildHarness(t testing.TB) rebuildHarness {
	t.Helper()
	mem := memory.New()
	idxStore := indexer.NewStore(mem, nil)
	idxMgr := indexer.NewManager(idxStore, indexer.WriterOptions{
		FlushDocs:     1000,
		FlushInterval: time.Minute,
		Logger:        pslog.NoopLogger(),
	})
	svc := New(Config{
		Store:            mem,
		IndexManager:     idxMgr,
		DefaultNamespace: namespaces.Default,
		BackendHash:      "test-backend",
	})
	t.Cleanup(func() { idxMgr.Close(context.Background()) })
	return rebuildHarness{svc: svc, store: mem, idxMgr: idxMgr, idxStore: idxStore}
}

func seedRebuildDoc(t testing.TB, h rebuildHarness, key string, jsonBody string) {
	t.Helper()
	ctx := context.Background()
	writeRes, err := h.store.WriteState(ctx, namespaces.Default, key, strings.NewReader(jsonBody), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state %s: %v", key, err)
	}
	meta := &storage.Meta{
		Version:             1,
		PublishedVersion:    1,
		StateETag:           writeRes.NewETag,
		StateDescriptor:     append([]byte(nil), writeRes.Descriptor...),
		StatePlaintextBytes: writeRes.BytesWritten,
	}
	if _, err := h.store.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
		t.Fatalf("store meta %s: %v", key, err)
	}
}

func TestRebuildIndexResetPreservesV5Format(t *testing.T) {
	ctx := context.Background()
	h := newRebuildHarness(t)
	seedRebuildDoc(t, h, "doc-1", `{"status":"open"}`)

	manifest := indexer.NewManifest()
	manifest.Format = indexer.IndexFormatVersionV5
	manifest.Seq = 1
	manifest.UpdatedAt = time.Unix(1_700_000_300, 0).UTC()
	manifest.Shards[0] = &indexer.Shard{ID: 0}
	if _, err := h.idxMgr.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("seed manifest: %v", err)
	}

	res, err := h.svc.RebuildIndex(ctx, namespaces.Default, IndexRebuildOptions{
		Mode:         "wait",
		Reset:        true,
		Cleanup:      true,
		CleanupDelay: 0,
	})
	if err != nil {
		t.Fatalf("rebuild index: %v", err)
	}
	if !res.Rebuilt {
		t.Fatalf("expected rebuilt result, got %+v", res)
	}

	manifestRes, err := h.idxMgr.LoadManifest(ctx, namespaces.Default)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifestRes.Manifest.Format != indexer.IndexFormatVersionV5 {
		t.Fatalf("expected manifest format v5, got %d", manifestRes.Manifest.Format)
	}
	shard := manifestRes.Manifest.Shards[0]
	if shard == nil || len(shard.Segments) == 0 {
		t.Fatalf("expected rebuilt segments")
	}
	for _, ref := range shard.Segments {
		segment, err := h.idxMgr.LoadSegment(ctx, namespaces.Default, ref.ID)
		if err != nil {
			t.Fatalf("load segment %s: %v", ref.ID, err)
		}
		if segment.Format != indexer.IndexFormatVersionV5 {
			t.Fatalf("expected segment format v5, got %d", segment.Format)
		}
	}
}

func TestRebuildIndexCleanupDropsLowerFormatSegments(t *testing.T) {
	ctx := context.Background()
	h := newRebuildHarness(t)
	seedRebuildDoc(t, h, "doc-1", `{"status":"open"}`)
	seedRebuildDoc(t, h, "doc-2", `{"status":"open"}`)

	legacy := indexer.NewSegment("legacy-v4", time.Unix(1_700_000_320, 0))
	legacy.Format = indexer.IndexFormatVersionV4
	legacy.Fields["/status"] = indexer.FieldBlock{Postings: map[string][]string{
		"open": {"doc-1"},
	}}
	if _, _, err := h.idxStore.WriteSegment(ctx, namespaces.Default, legacy); err != nil {
		t.Fatalf("write legacy segment: %v", err)
	}
	manifest := indexer.NewManifest()
	manifest.Format = indexer.IndexFormatVersionV5
	manifest.Seq = 9
	manifest.UpdatedAt = legacy.CreatedAt
	manifest.Shards[0] = &indexer.Shard{
		ID: 0,
		Segments: []indexer.SegmentRef{
			{ID: legacy.ID, CreatedAt: legacy.CreatedAt, DocCount: legacy.DocCount()},
		},
	}
	if _, err := h.idxMgr.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("save seed manifest: %v", err)
	}

	res, err := h.svc.RebuildIndex(ctx, namespaces.Default, IndexRebuildOptions{
		Mode:         "wait",
		Cleanup:      true,
		CleanupDelay: 0,
	})
	if err != nil {
		t.Fatalf("rebuild index: %v", err)
	}
	if !res.Rebuilt {
		t.Fatalf("expected rebuilt result, got %+v", res)
	}

	manifestRes, err := h.idxMgr.LoadManifest(ctx, namespaces.Default)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifestRes.Manifest.Format != indexer.IndexFormatVersionV5 {
		t.Fatalf("expected manifest format v5, got %d", manifestRes.Manifest.Format)
	}
	shard := manifestRes.Manifest.Shards[0]
	if shard == nil || len(shard.Segments) == 0 {
		t.Fatalf("expected rebuilt segments")
	}
	for _, ref := range shard.Segments {
		if ref.ID == legacy.ID {
			t.Fatalf("legacy v4 segment should have been cleaned from manifest")
		}
	}
	if _, err := h.idxMgr.LoadSegment(ctx, namespaces.Default, legacy.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected legacy segment deleted, got err=%v", err)
	}
}
