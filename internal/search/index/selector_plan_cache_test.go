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
