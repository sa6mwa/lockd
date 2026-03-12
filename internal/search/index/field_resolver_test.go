package index

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
)

func TestSelectorSupportsLegacyIndexFilterAllowsWildcard(t *testing.T) {
	if !selectorSupportsLegacyIndexFilter(mustParseSelector(t, `contains{field=/logs/*/message,value=timeout}`)) {
		t.Fatalf("expected wildcard selector to be index-supported")
	}
}

func TestSelectorSupportsLegacyIndexFilterAllowsRecursive(t *testing.T) {
	if !selectorSupportsLegacyIndexFilter(mustParseSelector(t, `contains{field=/logs/**/message,value=timeout}`)) {
		t.Fatalf("expected recursive selector to be index-supported")
	}
}

func TestSelectorSupportsLegacyIndexFilterAllowsTemporalFamilies(t *testing.T) {
	rangeSel := mustParseSelector(t, `/timestamp>=2026-03-05T10:28:21Z`)
	if !selectorSupportsLegacyIndexFilter(rangeSel) {
		t.Fatalf("expected datetime range selector to be index-supported")
	}

	dateSel := mustParseSelector(t, `date{f=/timestamp,a=2026-03-05,b=2026-03-07}`)
	if !selectorSupportsLegacyIndexFilter(dateSel) {
		t.Fatalf("expected date selector to be index-supported")
	}
}

func TestSelectorSupportsLegacyIndexFilterRejectsExplicitEmptyStringTerms(t *testing.T) {
	t.Parallel()
	cases := []string{
		`contains{f=/profile,v=""}`,
		`icontains{f=/profile,v=""}`,
		`prefix{f=/profile,v=""}`,
		`iprefix{f=/profile,v=""}`,
	}
	for _, expr := range cases {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			t.Parallel()
			sel := mustParseSelector(t, expr)
			if selectorSupportsLegacyIndexFilter(sel) {
				t.Fatalf("expected explicit-empty selector %q to require LQL post-eval", expr)
			}
		})
	}
}

func TestSelectorSupportsLegacyIndexFilterAllowsOmittedStringTermAssertions(t *testing.T) {
	t.Parallel()
	cases := []string{
		`contains{f=/profile}`,
		`icontains{f=/profile}`,
		`prefix{f=/profile}`,
		`iprefix{f=/profile}`,
	}
	for _, expr := range cases {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			t.Parallel()
			sel := mustParseSelector(t, expr)
			if !selectorSupportsLegacyIndexFilter(sel) {
				t.Fatalf("expected omitted-value selector %q to stay index-native", expr)
			}
		})
	}
}

func TestPrepareSelectorExecutionPlanTreatsTextSelectorsAsIndexNative(t *testing.T) {
	t.Parallel()
	adapter := &Adapter{}
	tests := []struct {
		name string
		expr string
	}{
		{name: "contains", expr: `contains{field=/message,value=timeout}`},
		{name: "icontains", expr: `icontains{field=/message,value=TIMEOUT}`},
		{name: "iprefix", expr: `iprefix{field=/owner,value=TEAM-}`},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			plan, err := adapter.prepareSelectorExecutionPlan(mustParseSelector(t, tt.expr))
			if err != nil {
				t.Fatalf("prepare selector execution plan: %v", err)
			}
			if !plan.useLegacyFilter {
				t.Fatalf("expected native index execution for %s selector", tt.name)
			}
			if plan.requirePostEval {
				t.Fatalf("unexpected post-eval fallback for %s selector", tt.name)
			}
		})
	}
}

func TestPrepareSelectorExecutionPlanTreatsTemporalSelectorsAsIndexNative(t *testing.T) {
	t.Parallel()
	adapter := &Adapter{}
	tests := []struct {
		name string
		expr string
	}{
		{name: "range_datetime", expr: `/timestamp>=2026-03-05T10:28:21Z,/timestamp<2026-03-05T10:30:00Z`},
		{name: "date", expr: `date{f=/timestamp,a=2026-03-05,b=2026-03-07}`},
		{name: "date_since_macro", expr: `date{f=/timestamp,since=today}`},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			plan, err := adapter.prepareSelectorExecutionPlan(mustParseSelector(t, tt.expr))
			if err != nil {
				t.Fatalf("prepare selector execution plan: %v", err)
			}
			if !plan.useLegacyFilter {
				t.Fatalf("expected native index execution for %s selector", tt.name)
			}
			if plan.requirePostEval {
				t.Fatalf("unexpected post-eval fallback for %s selector", tt.name)
			}
		})
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
		Selector:  mustParseSelector(t, `icontains{field=/logs/*/message,value=TIMEOUT}`),
		Limit:     10,
		Engine:    search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query wildcard contains: %v", err)
	}
	if !slices.Equal(resp.Keys, []string{"doc-a", "doc-b"}) {
		t.Fatalf("unexpected wildcard contains keys %v", resp.Keys)
	}
}

func TestIndexAdapterQueryRecursiveSingleStep(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)

	segment := NewSegment("seg-recursive-single-step", time.Unix(1_700_000_122, 0))
	segment.Fields["/logs/message"] = FieldBlock{Postings: map[string][]string{
		"timeout direct": {"doc-zero"},
	}}
	segment.Fields["/logs/a/message"] = FieldBlock{Postings: map[string][]string{
		"timeout one-level": {"doc-one"},
	}}
	segment.Fields["/logs/a/nested/message"] = FieldBlock{Postings: map[string][]string{
		"timeout deep": {"doc-deep"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 11
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
	for _, key := range []string{"doc-zero", "doc-one", "doc-deep"} {
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
		Selector:  mustParseSelector(t, `contains{field=/logs/**/message,value=timeout}`),
		Limit:     10,
		Engine:    search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query recursive **: %v", err)
	}
	if !slices.Equal(resp.Keys, []string{"doc-one", "doc-zero"}) {
		t.Fatalf("unexpected recursive ** keys %v", resp.Keys)
	}
}

func TestIndexAdapterQueryRecursiveDescendant(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)

	segment := NewSegment("seg-recursive-descendant", time.Unix(1_700_000_123, 0))
	segment.Fields["/logs/message"] = FieldBlock{Postings: map[string][]string{
		"timeout direct": {"doc-zero"},
	}}
	segment.Fields["/logs/a/message"] = FieldBlock{Postings: map[string][]string{
		"timeout one-level": {"doc-one"},
	}}
	segment.Fields["/logs/a/nested/message"] = FieldBlock{Postings: map[string][]string{
		"timeout deep": {"doc-deep"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 12
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
	for _, key := range []string{"doc-zero", "doc-one", "doc-deep"} {
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
		Selector:  mustParseSelector(t, `contains{field=/logs/.../message,value=timeout}`),
		Limit:     10,
		Engine:    search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query recursive ...: %v", err)
	}
	if !slices.Equal(resp.Keys, []string{"doc-deep", "doc-one", "doc-zero"}) {
		t.Fatalf("unexpected recursive ... keys %v", resp.Keys)
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
		Selector:  mustParseSelector(t, `eq{field=/records[]/status,value=open}`),
		Limit:     10,
		Engine:    search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query wildcard []: %v", err)
	}
	if !slices.Equal(resp.Keys, []string{"doc-open"}) {
		t.Fatalf("unexpected wildcard [] keys %v", resp.Keys)
	}
}

func TestShouldUseTriePatternPlanner(t *testing.T) {
	if !shouldUseTriePatternPlanner([]string{"logs", "*", "message"}, 512) {
		t.Fatal("expected trie planner for moderate wildcard pattern")
	}
	if shouldUseTriePatternPlanner([]string{"logs", "...", "message"}, 8_192) {
		t.Fatal("expected fallback planner for recursive pattern at large field count")
	}
	if shouldUseTriePatternPlanner([]string{"a", "*", "*", "*", "*", "*", "*", "z"}, 11_000) {
		t.Fatal("expected fallback planner for high estimated state count")
	}
}

func TestResolveSelectorFieldsFallbackPlannerMatches(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)

	segment := NewSegment("seg-planner-fallback", time.Unix(1_700_000_140, 0))
	for i := 0; i < 5_000; i++ {
		field := fmt.Sprintf("/logs/site/%04d/message", i)
		segment.Fields[field] = FieldBlock{Postings: map[string][]string{
			"timeout": {fmt.Sprintf("doc-%04d", i)},
		}}
	}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		}},
	}
	reader := newSegmentReader(namespaces.Default, manifest, store, nil)
	fields, err := reader.resolveSelectorFields(ctx, "/logs/.../message")
	if err != nil {
		t.Fatalf("resolve selector fields: %v", err)
	}
	if len(fields) != 5_000 {
		t.Fatalf("expected 5000 resolved fields, got %d", len(fields))
	}
}

func TestMatchPatternWithTrieStateCap(t *testing.T) {
	const nodes = 30_000
	fields := make([]string, 0, nodes)
	segments := make(map[string][]string, nodes)
	for i := 0; i < nodes; i++ {
		field := fmt.Sprintf("/cap/%05d", i)
		parts, err := indexFieldSegments(field)
		if err != nil {
			t.Fatalf("indexFieldSegments(%q): %v", field, err)
		}
		fields = append(fields, field)
		segments[field] = parts
	}
	reader := &segmentReader{
		fieldTrie: buildFieldPathTrie(fields, segments),
	}
	_, err := reader.matchPatternWithTrie([]string{"...", "...", "...", "...", "...", "...", "...", "...", "..."})
	if err == nil {
		t.Fatal("expected state-cap error for pathological recursive pattern")
	}
	if err != errFieldPatternStateLimit {
		t.Fatalf("expected errFieldPatternStateLimit, got %v", err)
	}
}
