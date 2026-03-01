package index

import (
	"testing"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/storage/memory"
)

func TestPrepareSelectorExecutionPlanCachesNormalizedAST(t *testing.T) {
	store := NewStore(memory.New(), nil)
	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}

	first, err := adapter.prepareSelectorExecutionPlan(api.Selector{
		Eq: &api.Term{Field: " /status ", Value: "open"},
	})
	if err != nil {
		t.Fatalf("prepare selector plan (first): %v", err)
	}
	if !first.useLegacyFilter || first.requirePostEval {
		t.Fatalf("unexpected plan flags %+v", first)
	}
	if got := adapter.plans.len(); got != 1 {
		t.Fatalf("expected cache size=1 after first plan, got %d", got)
	}

	second, err := adapter.prepareSelectorExecutionPlan(api.Selector{
		Eq: &api.Term{Field: "/status", Value: "open"},
	})
	if err != nil {
		t.Fatalf("prepare selector plan (second): %v", err)
	}
	if !second.useLegacyFilter || second.requirePostEval {
		t.Fatalf("unexpected cached plan flags %+v", second)
	}
	if got := adapter.plans.len(); got != 1 {
		t.Fatalf("expected normalized selector to hit cache size=1, got %d", got)
	}
	if second.selector.Eq == nil || second.selector.Eq.Field != "/status" {
		t.Fatalf("expected normalized field '/status', got %+v", second.selector)
	}
}

func TestSelectorPlanCacheEviction(t *testing.T) {
	cache := newSelectorPlanCache(2)
	cache.put("a", selectorExecutionPlan{})
	cache.put("b", selectorExecutionPlan{})
	cache.put("c", selectorExecutionPlan{})

	if cache.len() != 2 {
		t.Fatalf("expected bounded cache len=2, got %d", cache.len())
	}
	if _, ok := cache.get("a"); ok {
		t.Fatal("expected oldest key to be evicted")
	}
	if _, ok := cache.get("b"); !ok {
		t.Fatal("expected key b to remain in cache")
	}
	if _, ok := cache.get("c"); !ok {
		t.Fatal("expected key c to remain in cache")
	}
}

func TestPrepareSelectorExecutionPlanForEmptySelector(t *testing.T) {
	store := NewStore(memory.New(), nil)
	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	plan, err := adapter.prepareSelectorExecutionPlan(api.Selector{})
	if err != nil {
		t.Fatalf("prepare empty selector: %v", err)
	}
	if !plan.useLegacyFilter || plan.requirePostEval {
		t.Fatalf("unexpected empty-selector plan flags %+v", plan)
	}
	if !plan.selector.IsEmpty() {
		t.Fatalf("expected empty selector in plan, got %+v", plan.selector)
	}
}

func TestPrepareSelectorExecutionPlanCachesNormalizedDateSelectorAST(t *testing.T) {
	store := NewStore(memory.New(), nil)
	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	first, err := adapter.prepareSelectorExecutionPlan(api.Selector{
		Date: &api.DateTerm{Field: " /timestamp ", After: "2026-03-05"},
	})
	if err != nil {
		t.Fatalf("prepare date selector plan (first): %v", err)
	}
	if !first.useLegacyFilter || first.requirePostEval {
		t.Fatalf("unexpected date plan flags %+v", first)
	}
	if got := adapter.plans.len(); got != 1 {
		t.Fatalf("expected cache size=1 after first date plan, got %d", got)
	}

	second, err := adapter.prepareSelectorExecutionPlan(api.Selector{
		Date: &api.DateTerm{Field: "/timestamp", After: "2026-03-05"},
	})
	if err != nil {
		t.Fatalf("prepare date selector plan (second): %v", err)
	}
	if !second.useLegacyFilter || second.requirePostEval {
		t.Fatalf("unexpected cached date plan flags %+v", second)
	}
	if got := adapter.plans.len(); got != 1 {
		t.Fatalf("expected normalized date selector to hit cache size=1, got %d", got)
	}
	if second.selector.Date == nil || second.selector.Date.Field != "/timestamp" {
		t.Fatalf("expected normalized date field '/timestamp', got %+v", second.selector.Date)
	}
}

func TestPrepareSelectorExecutionPlanMarksRelativeDateSinceSelectors(t *testing.T) {
	store := NewStore(memory.New(), nil)
	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}

	relative, err := adapter.prepareSelectorExecutionPlan(api.Selector{
		And: []api.Selector{
			{Eq: &api.Term{Field: "/status", Value: "open"}},
			{Date: &api.DateTerm{Field: "/timestamp", Since: "today"}},
		},
	})
	if err != nil {
		t.Fatalf("prepare relative selector plan: %v", err)
	}
	if !relative.hasRelativeDateSinceTerm {
		t.Fatalf("expected relative date selector to disable sorted-key cache, got %+v", relative)
	}

	absolute, err := adapter.prepareSelectorExecutionPlan(api.Selector{
		Date: &api.DateTerm{Field: "/timestamp", Since: "2026-03-07T00:00:00Z"},
	})
	if err != nil {
		t.Fatalf("prepare absolute selector plan: %v", err)
	}
	if absolute.hasRelativeDateSinceTerm {
		t.Fatalf("expected absolute date selector to remain cacheable, got %+v", absolute)
	}
}
