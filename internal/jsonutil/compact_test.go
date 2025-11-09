package jsonutil

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func compactReference(input string) (string, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(input)); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func TestCompactWriterBasic(t *testing.T) {
	cases := []string{
		` { "foo" : [ 1 , 2 , 3 ] } `,
		"\n\t{\"nested\": {\"a\": 1, \"b\":true}}",
		`{"empty": [   ] , "obj" : {   }}`,
		`{"string":"\"quoted\"","escape":"\\tab\n"}`,
		`{"unicode":"åäö"}`,
		` [ 0 , -1 , 3.1415 , 10e-3 ] `,
	}
	for _, tc := range cases {
		var out bytes.Buffer
		if err := CompactWriter(&out, strings.NewReader(tc), 0); err != nil {
			t.Fatalf("compact failed: %v", err)
		}
		want, err := compactReference(tc)
		if err != nil {
			t.Fatalf("reference failed: %v", err)
		}
		if out.String() != want {
			t.Fatalf("unexpected output\n got: %q\nwant:%q", out.String(), want)
		}
	}
}

func TestCompactWriterErrors(t *testing.T) {
	tests := []string{
		`{`,              // unterminated object
		`{"a":}`,         // missing value
		`{"a"  "b"}`,     // missing colon
		`{"a":00}`,       // leading zero
		`{"a":"\u12x4"}`, // invalid unicode
		`{"a":"\x"}`,     // invalid escape
		`0 1`,            // multiple top-level values
		`\u1234`,         // invalid start
	}
	for _, tc := range tests {
		if err := CompactWriter(io.Discard, strings.NewReader(tc), 0); err == nil {
			t.Fatalf("expected error for input %q", tc)
		}
	}
}

func TestCompactWriterMaxBytes(t *testing.T) {
	input := `{"foo":` + strings.Repeat(" ", 10) + `"bar"}`
	if err := CompactWriter(io.Discard, strings.NewReader(input), 5); err == nil {
		t.Fatal("expected max bytes error")
	}
}

func TestCompactWriterFuzzSeeds(t *testing.T) {
	seeds := []string{
		`{"a": [1,2,{"b":true}]}`,
		`{"multi": [ {"nested": []} , {"x":null} ] }`,
		`"plain string"`,
		`1234567890`,
	}
	for _, tc := range seeds {
		var out bytes.Buffer
		if err := CompactWriter(&out, strings.NewReader(tc), 0); err != nil {
			t.Fatalf("compact failed: %v", err)
		}
		want, err := compactReference(tc)
		if err != nil {
			t.Fatalf("reference failed: %v", err)
		}
		if out.String() != want {
			t.Fatalf("unexpected output\n got: %q\nwant:%q", out.String(), want)
		}
	}
}

func FuzzCompactWriter(f *testing.F) {
	corpus := []string{
		`{ "foo": [1, 2, 3], "bar": {"baz": true} }`,
		`"string with \"quotes\""`,
		`[null, false, true, 0, 1e10, -3.14]`,
		`{"emoji":"😀","music":"\uD834\uDD1E","newline":"line1\nline2"}`,
		`{"big":"` + strings.Repeat("x", smallJSONThreshold+50) + `"}`,
	}
	for _, seed := range corpus {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		var out bytes.Buffer
		err := CompactWriter(&out, strings.NewReader(input), 0)
		ref, refErr := compactReference(input)
		if refErr != nil {
			if err == nil {
				t.Fatalf("expected error but got none for %q", input)
			}
			return
		}
		if err != nil {
			t.Fatalf("compact writer unexpected error: %v", err)
		}
		if out.String() != ref {
			t.Fatalf("mismatch for %q\n got: %q\nwant:%q", input, out.String(), ref)
		}
	})
}
