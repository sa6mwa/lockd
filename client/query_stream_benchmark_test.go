package client

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

func BenchmarkReadJSONStringSimple(b *testing.B) {
	input := []byte(`"default-namespace"`)
	reader := bufio.NewReaderSize(bytes.NewReader(input), 256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(bytes.NewReader(input))
		v, err := readJSONString(reader)
		if err != nil {
			b.Fatalf("read string: %v", err)
		}
		if v == "" {
			b.Fatalf("expected non-empty string")
		}
	}
}

func BenchmarkQueryStreamNextSingleRow(b *testing.B) {
	body := `{"ns":"default","key":"doc-1","ver":7,"doc":{"status":"ok","count":5}}
`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qs := newQueryStream(io.NopCloser(strings.NewReader(body)))
		row, err := qs.Next()
		if err != nil {
			b.Fatalf("next row: %v", err)
		}
		reader, err := row.doc.acquireReader()
		if err != nil {
			b.Fatalf("doc reader: %v", err)
		}
		if _, err := io.Copy(io.Discard, reader); err != nil {
			b.Fatalf("drain doc: %v", err)
		}
		if err := reader.Close(); err != nil {
			b.Fatalf("close doc: %v", err)
		}
		if _, err := qs.Next(); err != io.EOF {
			b.Fatalf("expected eof, got %v", err)
		}
	}
}
