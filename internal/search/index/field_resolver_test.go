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

func TestSelectorSupportsLegacyIndexFilterAllowsWildcard(t *testing.T) {
	if !selectorSupportsLegacyIndexFilter(api.Selector{
		Contains: &api.Term{Field: "/logs/*/message", Value: "timeout"},
	}) {
		t.Fatalf("expected wildcard selector to be index-supported")
	}
}

func TestSelectorSupportsLegacyIndexFilterRejectsRecursive(t *testing.T) {
	if selectorSupportsLegacyIndexFilter(api.Selector{
		Contains: &api.Term{Field: "/logs/**/message", Value: "timeout"},
	}) {
		t.Fatalf("expected recursive selector to be scan/postfilter-only")
	}
}

func TestIndexAdapterQueryWildcardContains(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)

	segment := NewSegment("seg-wildcard-contains", time.Unix(1_700_000_120, 0))
	segment.Fields["/logs/a/message"] = FieldBlock{Postings: map[string][]string{
		"timeout at edge": {"doc-a"},
	}}
	segment.Fields["/logs/b/message"] = FieldBlock{Postings: map[string][]string{
		"timeout at core": {"doc-b"},
	}}
	segment.Fields["/logs/c/message"] = FieldBlock{Postings: map[string][]string{
		"success": {"doc-c"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 9
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Format = IndexFormatVersionV4
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
	for _, key := range []string{"doc-a", "doc-b", "doc-c"} {
		meta := &storage.Meta{Version: 1, PublishedVersion: 1, StateETag: "etag-" + key}
		if _, err := mem.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
			t.Fatalf("store meta %s: %v", key, err)
		}
	}

	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	resp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			IContains: &api.Term{Field: "/logs/*/message", Value: "TIMEOUT"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query wildcard contains: %v", err)
	}
	if !slices.Equal(resp.Keys, []string{"doc-a", "doc-b"}) {
		t.Fatalf("unexpected wildcard contains keys %v", resp.Keys)
	}
}

func TestIndexAdapterQueryWildcardArraySugar(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)

	segment := NewSegment("seg-wildcard-array", time.Unix(1_700_000_121, 0))
	segment.Fields["/records/status"] = FieldBlock{Postings: map[string][]string{
		"open":   {"doc-open"},
		"closed": {"doc-closed"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 10
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Format = IndexFormatVersionV4
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
	for _, key := range []string{"doc-open", "doc-closed"} {
		meta := &storage.Meta{Version: 1, PublishedVersion: 1, StateETag: "etag-" + key}
		if _, err := mem.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
			t.Fatalf("store meta %s: %v", key, err)
		}
	}

	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	resp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "/records[]/status", Value: "open"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query wildcard []: %v", err)
	}
	if !slices.Equal(resp.Keys, []string{"doc-open"}) {
		t.Fatalf("unexpected wildcard [] keys %v", resp.Keys)
	}
}

