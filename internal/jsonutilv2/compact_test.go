package jsonutilv2

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func compactReference(input []byte) (string, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, input); err != nil {
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
		want, err := compactReference([]byte(tc))
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
		want, err := compactReference([]byte(tc))
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
		f.Add(seed, uint8(0), uint8(0), uint8(0))
	}
	f.Fuzz(func(t *testing.T, input string, sizeSel, chunkSel, maxSel uint8) {
		payload := fuzzResizedBytes([]byte(input), fuzzBoundaryInt(sizeSel, []int{
			0, 1, 2, 15, 63, 127, 255, 256, 511, 512, 1023, 1024, 2047, 2048, 2049, 4096,
		}))
		chunk := fuzzBoundaryInt(chunkSel, []int{1, 2, 3, 7, 16, 31, 32, 64, 255, 256})
		maxBytes := fuzzBoundaryMax(maxSel, int64(len(payload)), smallJSONThreshold)

		var out bytes.Buffer
		err := CompactWriter(&out, &fuzzChunkedReader{data: payload, chunk: chunk}, maxBytes)
		if maxBytes > 0 && int64(len(payload)) > maxBytes {
			if err == nil {
				t.Fatalf("expected max-bytes error payload=%d max=%d", len(payload), maxBytes)
			}
			return
		}
		ref, refErr := compactReference(payload)
		if refErr != nil {
			if err == nil {
				t.Fatalf("expected error but got none for %q", payload)
			}
			return
		}
		if err != nil {
			t.Fatalf("compact writer unexpected error: %v", err)
		}
		if out.String() != ref {
			t.Fatalf("mismatch for %q\n got: %q\nwant:%q", payload, out.String(), ref)
		}
	})
}

func fuzzBoundaryInt(sel uint8, values []int) int {
	if len(values) == 0 {
		return 0
	}
	return values[int(sel)%len(values)]
}

func fuzzBoundaryMax(sel uint8, payloadLen int64, threshold int) int64 {
	options := []int64{
		0,
		int64(threshold - 1),
		int64(threshold),
		int64(threshold + 1),
		payloadLen - 1,
		payloadLen,
		payloadLen + 1,
	}
	max := options[int(sel)%len(options)]
	if max < 0 {
		return 0
	}
	return max
}

func fuzzResizedBytes(in []byte, target int) []byte {
	if target <= 0 {
		return []byte{}
	}
	if len(in) == 0 {
		in = []byte{'{', '}'}
	}
	out := make([]byte, target)
	for i := 0; i < target; i++ {
		out[i] = in[i%len(in)]
	}
	return out
}

type fuzzChunkedReader struct {
	data  []byte
	chunk int
	off   int
}

func (r *fuzzChunkedReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := len(p)
	if r.chunk > 0 && n > r.chunk {
		n = r.chunk
	}
	remaining := len(r.data) - r.off
	if n > remaining {
		n = remaining
	}
	copy(p[:n], r.data[r.off:r.off+n])
	r.off += n
	return n, nil
}
