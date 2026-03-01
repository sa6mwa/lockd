package index

import (
	"context"
	"slices"
	"testing"
	"time"

	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lql"
)

func TestQueryRelativeDateSinceBypassesSortedKeysCache(t *testing.T) {
	now := time.Date(2026, time.March, 7, 10, 0, 0, 0, time.UTC)
	h := newParityHarness(t, map[string]map[string]any{
		"doc-relative": {"timestamp": "2026-03-07T10:00:30Z"},
	})
	h.index.now = func() time.Time { return now }

	sel, err := lql.ParseSelectorString(`date{f=/timestamp,since=now}`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	req := search.Request{
		Namespace: namespaces.Default,
		Selector:  sel,
		Limit:     10,
		Engine:    search.EngineIndex,
	}
	ctx := context.Background()

	first, err := h.index.Query(ctx, req)
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	if !slices.Equal(first.Keys, []string{"doc-relative"}) {
		t.Fatalf("unexpected first keys: %v", first.Keys)
	}
	if got := h.index.sorted.len(); got != 0 {
		t.Fatalf("expected sorted-key cache bypass for relative date selector, got len=%d", got)
	}

	now = now.Add(2 * time.Minute)
	second, err := h.index.Query(ctx, req)
	if err != nil {
		t.Fatalf("second query: %v", err)
	}
	if len(second.Keys) != 0 {
		t.Fatalf("expected no keys after now advanced, got %v", second.Keys)
	}
	if got := h.index.sorted.len(); got != 0 {
		t.Fatalf("expected sorted-key cache bypass to remain uncached, got len=%d", got)
	}
}

func TestQueryAbsoluteDateSinceStillUsesSortedKeysCache(t *testing.T) {
	h := newParityHarness(t, map[string]map[string]any{
		"doc-a": {"timestamp": "2026-03-07T10:00:30Z"},
	})

	sel, err := lql.ParseSelectorString(`date{f=/timestamp,since=2026-03-07T10:00:00Z}`)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}
	req := search.Request{
		Namespace: namespaces.Default,
		Selector:  sel,
		Limit:     10,
		Engine:    search.EngineIndex,
	}
	ctx := context.Background()

	if _, err := h.index.Query(ctx, req); err != nil {
		t.Fatalf("first query: %v", err)
	}
	if got := h.index.sorted.len(); got != 1 {
		t.Fatalf("expected absolute date selector to populate sorted-key cache, got len=%d", got)
	}
	if _, err := h.index.Query(ctx, req); err != nil {
		t.Fatalf("second query: %v", err)
	}
	if got := h.index.sorted.len(); got != 1 {
		t.Fatalf("expected cache hit to keep one sorted-key entry, got len=%d", got)
	}
}
