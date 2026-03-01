package httpapi

import (
	"bytes"
	"testing"
)

func BenchmarkStreamDocumentRow(b *testing.B) {
	handler := &Handler{jsonMaxBytes: 10 * 1024 * 1024}
	copyBuf := make([]byte, queryStreamCopyBufSize)
	rowBuf := make([]byte, 0, queryStreamRowBufSize)
	out := bytes.NewBuffer(make([]byte, 0, 64*1024))

	payloads := []struct {
		name string
		doc  []byte
	}{
		{name: "1KiB", doc: bytes.Repeat([]byte("x"), 1024)},
		{name: "16KiB", doc: bytes.Repeat([]byte("x"), 16*1024)},
	}

	for _, payload := range payloads {
		payload := payload
		b.Run(payload.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out.Reset()
				doc := bytes.NewReader(payload.doc)
				nextRowBuf, err := handler.streamDocumentRow(out, "default", "doc-1", 7, doc, copyBuf, rowBuf)
				if err != nil {
					b.Fatalf("stream document row: %v", err)
				}
				rowBuf = nextRowBuf
			}
		})
	}
}
