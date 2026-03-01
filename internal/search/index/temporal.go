package index

import (
	"strings"
	"time"

	"pkt.systems/lockd/api"
)

type temporalValue struct {
	instant  time.Time
	year     int
	month    time.Month
	day      int
	dateOnly bool
}

type rangeMode uint8

const (
	rangeModeInvalid rangeMode = iota
	rangeModeNumeric
	rangeModeTemporal
)

type compiledNumericRange struct {
	hasGTE bool
	hasGT  bool
	hasLTE bool
	hasLT  bool
	gte    float64
	gt     float64
	lte    float64
	lt     float64
}

type compiledTemporalRange struct {
	hasGTE bool
	hasGT  bool
	hasLTE bool
	hasLT  bool
	gte    temporalValue
	gt     temporalValue
	lte    temporalValue
	lt     temporalValue
}

type compiledDateTerm struct {
	eq     *temporalValue
	gte    *temporalValue
	gt     *temporalValue
	lte    *temporalValue
	lt     *temporalValue
	hasAny bool
}

func parseTemporalLiteral(raw string) (temporalValue, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return temporalValue{}, false
	}
	if t, ok := parseDateOnlyLiteral(trimmed); ok {
		year, month, day := t.Date()
		return temporalValue{
			instant:  t,
			year:     year,
			month:    month,
			day:      day,
			dateOnly: true,
		}, true
	}
	if t, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		year, month, day := t.Date()
		return temporalValue{
			instant: t,
			year:    year,
			month:   month,
			day:     day,
		}, true
	}
	// Index terms are lower-cased for case-insensitive string matching.
	// Accept canonical RFC3339 values after restoring case-sensitive markers.
	if t, err := time.Parse(time.RFC3339Nano, strings.ToUpper(trimmed)); err == nil {
		year, month, day := t.Date()
		return temporalValue{
			instant: t,
			year:    year,
			month:   month,
			day:     day,
		}, true
	}
	return temporalValue{}, false
}

func parseDateOnlyLiteral(raw string) (time.Time, bool) {
	if len(raw) != len("2006-01-02") {
		return time.Time{}, false
	}
	for i := 0; i < len(raw); i++ {
		switch i {
		case 4, 7:
			if raw[i] != '-' {
				return time.Time{}, false
			}
		default:
			if raw[i] < '0' || raw[i] > '9' {
				return time.Time{}, false
			}
		}
	}
	t, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func temporalEqual(left, right temporalValue) bool {
	if left.dateOnly && right.dateOnly {
		return left.year == right.year && left.month == right.month && left.day == right.day
	}
	if !left.dateOnly && !right.dateOnly {
		return left.instant.Equal(right.instant)
	}
	if left.dateOnly {
		y, m, d := right.instant.Date()
		return left.year == y && left.month == m && left.day == d
	}
	y, m, d := left.instant.Date()
	return right.year == y && right.month == m && right.day == d
}

func temporalCompare(left, right temporalValue) int {
	leftTime := left.orderingInstant()
	rightTime := right.orderingInstant()
	if leftTime.Before(rightTime) {
		return -1
	}
	if leftTime.After(rightTime) {
		return 1
	}
	return 0
}

func (v temporalValue) orderingInstant() time.Time {
	if v.dateOnly {
		return time.Date(v.year, v.month, v.day, 0, 0, 0, 0, time.UTC)
	}
	return v.instant.UTC()
}

func determineRangeMode(term *api.RangeTerm) rangeMode {
	if term == nil {
		return rangeModeInvalid
	}
	mode := rangeModeInvalid
	for _, bound := range []*api.RangeBound{term.GTE, term.GT, term.LTE, term.LT} {
		if bound == nil {
			continue
		}
		if _, ok := bound.Number(); ok {
			if mode == rangeModeTemporal {
				return rangeModeInvalid
			}
			mode = rangeModeNumeric
			continue
		}
		if _, ok := bound.DateTime(); ok {
			if mode == rangeModeNumeric {
				return rangeModeInvalid
			}
			mode = rangeModeTemporal
			continue
		}
		return rangeModeInvalid
	}
	return mode
}

func compileNumericRangeBounds(term *api.RangeTerm) (compiledNumericRange, bool) {
	if determineRangeMode(term) != rangeModeNumeric {
		return compiledNumericRange{}, false
	}
	var out compiledNumericRange
	if value, ok := term.GTE.Number(); ok {
		out.hasGTE = true
		out.gte = value
	}
	if value, ok := term.GT.Number(); ok {
		out.hasGT = true
		out.gt = value
	}
	if value, ok := term.LTE.Number(); ok {
		out.hasLTE = true
		out.lte = value
	}
	if value, ok := term.LT.Number(); ok {
		out.hasLT = true
		out.lt = value
	}
	return out, true
}

func compileTemporalRangeBounds(term *api.RangeTerm) (compiledTemporalRange, bool) {
	if determineRangeMode(term) != rangeModeTemporal {
		return compiledTemporalRange{}, false
	}
	var out compiledTemporalRange
	if value, ok := temporalRangeBoundValue(term.GTE); ok {
		out.hasGTE = true
		out.gte = value
	}
	if value, ok := temporalRangeBoundValue(term.GT); ok {
		out.hasGT = true
		out.gt = value
	}
	if value, ok := temporalRangeBoundValue(term.LTE); ok {
		out.hasLTE = true
		out.lte = value
	}
	if value, ok := temporalRangeBoundValue(term.LT); ok {
		out.hasLT = true
		out.lt = value
	}
	return out, true
}

func temporalRangeBoundValue(bound *api.RangeBound) (temporalValue, bool) {
	if bound == nil {
		return temporalValue{}, false
	}
	text, ok := bound.DateTime()
	if !ok {
		return temporalValue{}, false
	}
	return parseTemporalLiteral(text)
}

func compileDateTerm(term *api.DateTerm, now time.Time) (compiledDateTerm, bool) {
	if term == nil {
		return compiledDateTerm{}, false
	}
	var out compiledDateTerm
	if term.Value != "" {
		value, ok := parseTemporalLiteral(term.Value)
		if !ok {
			return compiledDateTerm{}, false
		}
		out.eq = &value
		out.hasAny = true
	}
	if term.Since != "" {
		value, ok := compileDateSince(term.Since, now)
		if !ok {
			return compiledDateTerm{}, false
		}
		out.gte = &value
		out.hasAny = true
	}
	if term.After != "" {
		value, ok := parseTemporalLiteral(term.After)
		if !ok {
			return compiledDateTerm{}, false
		}
		out.gt = &value
		out.hasAny = true
	}
	if term.Before != "" {
		value, ok := parseTemporalLiteral(term.Before)
		if !ok {
			return compiledDateTerm{}, false
		}
		out.lt = &value
		out.hasAny = true
	}
	if term.GTE != "" {
		value, ok := parseTemporalLiteral(term.GTE)
		if !ok {
			return compiledDateTerm{}, false
		}
		out.gte = &value
		out.hasAny = true
	}
	if term.GT != "" {
		value, ok := parseTemporalLiteral(term.GT)
		if !ok {
			return compiledDateTerm{}, false
		}
		out.gt = &value
		out.hasAny = true
	}
	if term.LTE != "" {
		value, ok := parseTemporalLiteral(term.LTE)
		if !ok {
			return compiledDateTerm{}, false
		}
		out.lte = &value
		out.hasAny = true
	}
	if term.LT != "" {
		value, ok := parseTemporalLiteral(term.LT)
		if !ok {
			return compiledDateTerm{}, false
		}
		out.lt = &value
		out.hasAny = true
	}
	return out, out.hasAny
}

func compileDateSince(raw string, now time.Time) (temporalValue, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return temporalValue{}, false
	case "now":
		return temporalMacroNow(now), true
	case "today":
		return temporalMacroTodayUTC(now), true
	case "yesterday":
		return temporalMacroYesterdayUTC(now), true
	default:
		return parseTemporalLiteral(raw)
	}
}

func temporalMacroNow(now time.Time) temporalValue {
	utcNow := now.UTC()
	year, month, day := utcNow.Date()
	return temporalValue{
		instant: utcNow,
		year:    year,
		month:   month,
		day:     day,
	}
}

func temporalMacroTodayUTC(now time.Time) temporalValue {
	utcNow := now.UTC()
	y, m, d := utcNow.Date()
	return temporalValue{
		instant:  time.Date(y, m, d, 0, 0, 0, 0, time.UTC),
		year:     y,
		month:    m,
		day:      d,
		dateOnly: true,
	}
}

func temporalMacroYesterdayUTC(now time.Time) temporalValue {
	utcYesterday := now.UTC().AddDate(0, 0, -1)
	y, m, d := utcYesterday.Date()
	return temporalValue{
		instant:  time.Date(y, m, d, 0, 0, 0, 0, time.UTC),
		year:     y,
		month:    m,
		day:      d,
		dateOnly: true,
	}
}

func dateTermMatches(compiled compiledDateTerm, candidate temporalValue) bool {
	if !compiled.hasAny {
		return false
	}
	if compiled.eq != nil && !temporalEqual(candidate, *compiled.eq) {
		return false
	}
	if compiled.gte != nil && temporalCompare(candidate, *compiled.gte) < 0 {
		return false
	}
	if compiled.gt != nil && temporalCompare(candidate, *compiled.gt) <= 0 {
		return false
	}
	if compiled.lte != nil && temporalCompare(candidate, *compiled.lte) > 0 {
		return false
	}
	if compiled.lt != nil && temporalCompare(candidate, *compiled.lt) >= 0 {
		return false
	}
	return true
}
