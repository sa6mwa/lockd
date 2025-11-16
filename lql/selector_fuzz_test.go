package lql

import (
	"encoding/json"
	"strings"
	"testing"
)

func FuzzParseSelectorString(f *testing.F) {
	seed := []string{
		"",
		"eq{field=/status,value=open}",
		"and.eq{field=/status,value=open},and.eq{field=/owner,value=alice}",
		"or.eq{field=/status,value=open},or.eq{field=/status,value=processing}",
		"and.eq{field=/status,value=open},or.eq{field=/owner,value=\"alice\"}",
	}
	for _, s := range seed {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		_, _ = ParseSelectorString(expr)
	})
}

func FuzzParseSelectorShorthand(f *testing.F) {
	seeds := []struct {
		field string
		value string
		op    uint8
	}{
		{"status", "open", 0},
		{"progress/count", "42", 2},
		{"flags/urgent", "true", 1},
	}
	for _, seed := range seeds {
		f.Add(seed.field, seed.value, seed.op)
	}
	ops := []string{"=", "!=", ">", ">=", "<", "<="}
	f.Fuzz(func(t *testing.T, field, value string, opIdx uint8) {
		field = "/" + sanitizeFuzzField(field)
		if field == "/" {
			field = "/fuzz"
		}
		op := ops[int(opIdx)%len(ops)]
		literal := buildFuzzLiteral(op, value)
		expr := field + op + literal
		_, _ = ParseSelectorString(expr)
	})
}

func sanitizeFuzzField(in string) string {
	trimmed := strings.TrimSpace(in)
	if trimmed == "" {
		return "field"
	}
	replacer := func(r rune) rune {
		switch r {
		case '\n', '\r', '\t', '=', '>', '<', '!', '{', '}', ',', '\\':
			return '_'
		default:
			return r
		}
	}
	return strings.Map(replacer, trimmed)
}

func buildFuzzLiteral(op string, value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		trimmed = "seed"
	}
	switch op {
	case ">", ">=", "<", "<=":
		// ensure numeric literal for range operators
		size := len(trimmed)
		if size <= 0 {
			size = 1
		}
		return strings.TrimSpace(strings.Repeat("1", size%5+1))
	default:
		lower := strings.ToLower(trimmed)
		if lower == "true" || lower == "false" {
			return lower
		}
		if isNumeric(trimmed) {
			return trimmed
		}
		bytes, err := json.Marshal(trimmed)
		if err != nil {
			return "\"seed\""
		}
		return string(bytes)
	}
}

func isNumeric(in string) bool {
	if in == "" {
		return false
	}
	for _, r := range in {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
