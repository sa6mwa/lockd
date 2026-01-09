package index

import (
	"context"
	"slices"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
)

func TestManifestRoundTrip(t *testing.T) {
	m := NewManifest()
	m.Seq = 7
	m.UpdatedAt = time.Unix(1_700_000_000, 0)
	m.Shards[0] = &Shard{ID: 0, Segments: []SegmentRef{{ID: "seg-1", CreatedAt: m.UpdatedAt, DocCount: 5}}}
	msg := m.ToProto()
	clone := ManifestFromProto(msg)
	if clone.Seq != m.Seq || len(clone.Shards) != len(m.Shards) {
		t.Fatalf("manifest mismatch: %#v != %#v", clone, m)
	}
}

func TestSegmentRoundTrip(t *testing.T) {
	seg := NewSegment("seg-1", time.Unix(1_700_000_010, 0))
	seg.Fields["status"] = FieldBlock{Postings: map[string][]string{"open": {"a", "b"}}}
	if err := seg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	msg := seg.ToProto()
	loaded := SegmentFromProto(msg)
	if loaded.ID != seg.ID || loaded.DocCount() != seg.DocCount() {
		t.Fatalf("segment mismatch: %+v vs %+v", loaded, seg)
	}
}

func TestStoreManifestLifecycle(t *testing.T) {
	store := memory.New()
	idxStore := NewStore(store, nil)
	ctx := context.Background()
	manifest := NewManifest()
	manifest.Seq = 1
	manifest.UpdatedAt = time.Now().UTC()
	manifest.Shards[0] = &Shard{ID: 0}
	if _, err := idxStore.SaveManifest(ctx, "default", manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	manifestRes, err := idxStore.LoadManifest(ctx, "default")
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	loaded := manifestRes.Manifest
	if manifestRes.ETag == "" || loaded.Seq != manifest.Seq {
		t.Fatalf("unexpected manifest: %+v etag=%q", loaded, manifestRes.ETag)
	}
}

func TestStoreSegmentLifecycle(t *testing.T) {
	store := memory.New()
	idxStore := NewStore(store, nil)
	ctx := context.Background()
	seg := NewSegment("seg-seed", time.Now())
	seg.Fields["status"] = FieldBlock{Postings: map[string][]string{"open": {"doc-1"}}}
	if _, _, err := idxStore.WriteSegment(ctx, "default", seg); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	loaded, err := idxStore.LoadSegment(ctx, "default", seg.ID)
	if err != nil {
		t.Fatalf("load segment: %v", err)
	}
	if loaded.DocCount() != seg.DocCount() {
		t.Fatalf("doc count mismatch")
	}
	if err := idxStore.DeleteSegment(ctx, "default", seg.ID); err != nil {
		t.Fatalf("delete segment: %v", err)
	}
}

func TestIndexAdapterQuery(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)
	segment := NewSegment("seg-query", time.Unix(1_700_000_020, 0))
	segment.Fields["status"] = FieldBlock{Postings: map[string][]string{
		"open":   {"orders-open-1", "orders-open-2", "orders-open-3"},
		"closed": {"orders-closed"},
	}}
	segment.Fields["device.telemetry.battery_mv"] = FieldBlock{Postings: map[string][]string{
		"4200": {"device-gw-42"},
		"3300": {"device-gw-7"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 2
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	seedMeta := func(key string, published bool, hidden bool) {
		meta := &storage.Meta{
			Version:          1,
			PublishedVersion: 0,
			StateETag:        "etag-" + key,
		}
		if published {
			meta.PublishedVersion = 1
		}
		if hidden {
			meta.SetQueryHidden(true)
		}
		if _, err := mem.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
			t.Fatalf("store meta %s: %v", key, err)
		}
	}
	seedMeta("orders-open-1", true, false)
	seedMeta("orders-open-2", true, true) // hidden
	seedMeta("orders-open-3", true, false)
	seedMeta("orders-closed", true, false)
	seedMeta("device-gw-42", true, false)
	seedMeta("device-gw-7", false, false) // unpublished

	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}

	// Eq selector should skip hidden keys.
	resp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "status", Value: "open"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query eq: %v", err)
	}
	want := []string{"orders-open-1", "orders-open-3"}
	if !slices.Equal(resp.Keys, want) {
		t.Fatalf("unexpected eq keys %v", resp.Keys)
	}

	// Range selector should only include published matches.
	rangeResp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Range: &api.RangeTerm{Field: "device.telemetry.battery_mv", GTE: floatPtr(4000)},
		},
		Limit:  5,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query range: %v", err)
	}
	if len(rangeResp.Keys) != 1 || rangeResp.Keys[0] != "device-gw-42" {
		t.Fatalf("unexpected range keys %v", rangeResp.Keys)
	}

	// Pagination should advance past the last returned key even with hidden entries.
	page1, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "status", Value: "open"},
		},
		Limit:  1,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query page1: %v", err)
	}
	if len(page1.Keys) != 1 || page1.Keys[0] != "orders-open-1" || page1.Cursor == "" {
		t.Fatalf("unexpected page1 %+v", page1)
	}
	page2, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "status", Value: "open"},
		},
		Limit:  2,
		Cursor: page1.Cursor,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query page2: %v", err)
	}
	if len(page2.Keys) != 1 || page2.Keys[0] != "orders-open-3" || page2.Cursor != "" {
		t.Fatalf("unexpected page2 %+v", page2)
	}
}

func floatPtr(val float64) *float64 {
	return &val
}
