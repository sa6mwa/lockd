package index

import (
	"testing"
	"time"

	"pkt.systems/lockd/api"
)

func TestParseTemporalLiteralAcceptsLowerRFC3339(t *testing.T) {
	value, ok := parseTemporalLiteral("2026-03-05t10:28:21z")
	if !ok {
		t.Fatalf("expected lowercase RFC3339 literal to parse")
	}
	if value.dateOnly {
		t.Fatalf("expected datetime literal, got date-only")
	}
}

func TestDetermineRangeMode(t *testing.T) {
	numeric := &api.RangeTerm{Field: "/n", GTE: api.NewNumericRangeBound(10)}
	if got := determineRangeMode(numeric); got != rangeModeNumeric {
		t.Fatalf("expected numeric range mode, got %v", got)
	}

	temporal := &api.RangeTerm{Field: "/ts", LT: api.NewDatetimeRangeBound("2026-03-06T00:00:00Z")}
	if got := determineRangeMode(temporal); got != rangeModeTemporal {
		t.Fatalf("expected temporal range mode, got %v", got)
	}

	mixed := &api.RangeTerm{
		Field: "/mixed",
		GTE:   api.NewNumericRangeBound(1),
		LT:    api.NewDatetimeRangeBound("2026-03-06T00:00:00Z"),
	}
	if got := determineRangeMode(mixed); got != rangeModeInvalid {
		t.Fatalf("expected invalid mixed range mode, got %v", got)
	}
}

func TestCompileDateTermSinceMacrosUTC(t *testing.T) {
	now := time.Date(2026, time.March, 7, 15, 4, 5, 0, time.FixedZone("utc+3", 3*3600))
	compiled, ok := compileDateTerm(&api.DateTerm{Field: "/timestamp", Since: "today"}, now)
	if !ok || compiled.gte == nil {
		t.Fatalf("expected compiled date since=today")
	}
	wantToday := time.Date(2026, time.March, 7, 0, 0, 0, 0, time.UTC)
	if !compiled.gte.orderingInstant().Equal(wantToday) {
		t.Fatalf("unexpected today macro value: got=%s want=%s", compiled.gte.orderingInstant(), wantToday)
	}

	compiled, ok = compileDateTerm(&api.DateTerm{Field: "/timestamp", Since: "yesterday"}, now)
	if !ok || compiled.gte == nil {
		t.Fatalf("expected compiled date since=yesterday")
	}
	wantYesterday := time.Date(2026, time.March, 6, 0, 0, 0, 0, time.UTC)
	if !compiled.gte.orderingInstant().Equal(wantYesterday) {
		t.Fatalf("unexpected yesterday macro value: got=%s want=%s", compiled.gte.orderingInstant(), wantYesterday)
	}
}
