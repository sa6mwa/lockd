package httpapi

import (
	"strconv"
	"testing"
)

func TestAppendJSONQuotedMatchesStrconv(t *testing.T) {
	t.Parallel()

	cases := []string{
		"default",
		"doc-1",
		"with space",
		`quote"inside`,
		`slash\inside`,
		"control-\n",
		"unicode-åäö",
	}
	for _, input := range cases {
		input := input
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got := appendJSONQuoted(nil, input)
			want := strconv.AppendQuote(nil, input)
			if string(got) != string(want) {
				t.Fatalf("appendJSONQuoted(%q) = %q, want %q", input, string(got), string(want))
			}
		})
	}
}
