package index

import (
	"testing"

	"pkt.systems/lockd/api"
	"pkt.systems/lql"
)

func mustParseSelector(t testing.TB, expr string) api.Selector {
	t.Helper()
	sel, err := lql.ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse selector %q: %v", expr, err)
	}
	if sel.IsEmpty() {
		t.Fatalf("selector %q parsed empty", expr)
	}
	return sel
}
