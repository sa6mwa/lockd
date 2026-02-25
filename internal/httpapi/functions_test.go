package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
)

func TestParseEngineHint(t *testing.T) {
	hint, err := parseEngineHint("index")
	if err != nil || hint != search.EngineIndex {
		t.Fatalf("parseEngineHint index got %v, %v", hint, err)
	}
	if _, err := parseEngineHint("nope"); err == nil {
		t.Fatalf("expected error for invalid engine")
	}
}

func TestParseRefreshMode(t *testing.T) {
	mode, err := parseRefreshMode("wait_for")
	if err != nil || mode != refreshWaitFor {
		t.Fatalf("parseRefreshMode wait_for got %v %v", mode, err)
	}
	if _, err := parseRefreshMode("later"); err == nil {
		t.Fatalf("expected error for invalid refresh mode")
	}
}

func TestParseQueryReturnMode(t *testing.T) {
	mode, err := parseQueryReturnMode("documents")
	if err != nil || mode != api.QueryReturnDocuments {
		t.Fatalf("parseQueryReturnMode documents got %v %v", mode, err)
	}
	if _, err := parseQueryReturnMode("bogus"); err == nil {
		t.Fatalf("expected error for invalid return mode")
	}
}

func TestNormalizeSelectorFields(t *testing.T) {
	sel := api.Selector{
		Eq:        &api.Term{Field: "/foo"},
		Contains:  &api.Term{Field: "/msg"},
		IContains: &api.Term{Field: "/desc"},
		IPrefix:   &api.Term{Field: "/service"},
		Exists:    "/bar",
		And:       []api.Selector{{In: &api.InTerm{Field: "/baz"}}},
	}
	if err := normalizeSelectorFields(&sel); err != nil {
		t.Fatalf("normalizeSelectorFields error: %v", err)
	}
	if sel.Eq.Field != "/foo" || sel.Contains.Field != "/msg" || sel.IContains.Field != "/desc" || sel.IPrefix.Field != "/service" || sel.Exists != "/bar" || sel.And[0].In.Field != "/baz" {
		t.Fatalf("fields not normalized: %+v", sel)
	}
}

func TestParseFencingToken(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(headerFencingToken, "42")
	token, err := parseFencingToken(r)
	if err != nil || token != 42 {
		t.Fatalf("parseFencingToken got %v %v", token, err)
	}
}

func TestParseBoolQuery(t *testing.T) {
	for _, val := range []string{"1", "true", "YES", "on"} {
		if !parseBoolQuery(val) {
			t.Fatalf("expected true for %q", val)
		}
	}
	if parseBoolQuery("false") {
		t.Fatalf("expected false")
	}
}

func TestZeroSelector(t *testing.T) {
	if !zeroSelector(api.Selector{}) {
		t.Fatalf("empty selector should be zero")
	}
	if zeroSelector(api.Selector{Exists: "/foo"}) {
		t.Fatalf("non-empty selector should not be zero")
	}
}

func TestRelativeKey(t *testing.T) {
	if got := relativeKey("ns", "ns/key"); got != "key" {
		t.Fatalf("relativeKey unexpected: %s", got)
	}
	if got := relativeKey("", "key"); got != "key" {
		t.Fatalf("relativeKey empty namespace: %s", got)
	}
}

func TestCloneMeta(t *testing.T) {
	meta := storage.Meta{}
	meta.StateDescriptor = []byte("state")
	meta.Lease = &storage.Lease{ID: "abc"}
	cloned := cloneMeta(meta)
	cloned.StateDescriptor[0] = 'S'
	cloned.Lease.ID = "xyz"
	if meta.StateDescriptor[0] == 'S' || meta.Lease.ID == "xyz" {
		t.Fatalf("cloneMeta did not deep copy")
	}
}

func TestDurationToSeconds(t *testing.T) {
	if got := durationToSeconds(1500 * time.Millisecond); got != 2 {
		t.Fatalf("durationToSeconds round up failed: %d", got)
	}
	if got := durationToSeconds(-1); got != 0 {
		t.Fatalf("durationToSeconds negative should be 0, got %d", got)
	}
}

func TestNewAcquireBackoff(t *testing.T) {
	b := newAcquireBackoff()
	first := b.Next(0)
	second := b.Next(0)
	if first <= 0 {
		t.Fatalf("first backoff should be >0, got %v", first)
	}
	if second <= 0 {
		t.Fatalf("second backoff should be >0, got %v", second)
	}
}

func TestApplyBackoffJitterBounds(t *testing.T) {
	randFn := func(n int64) int64 { return n / 2 } // deterministic mid-point
	base := 500 * time.Millisecond
	limit := time.Second
	got := applyBackoffJitter(base, limit, randFn)
	if got < base-acquireBackoffJitter || got > base+acquireBackoffJitter {
		t.Fatalf("jitter out of bounds: %v", got)
	}
}

func TestDurationToSecondsCap(t *testing.T) {
	if got := durationToSeconds(999 * time.Millisecond); got != 1 {
		t.Fatalf("expected ceil to 1 second, got %d", got)
	}
}
