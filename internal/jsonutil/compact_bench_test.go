package jsonutil

import (
	"bytes"
	"encoding/json"
	"testing"
)

var (
	benchSmallInput = []byte(`{"foo": [1, 2, 3], "bar": {"baz": true}, "str": "hello world"}`)
	benchLargeInput = buildLargeFixture()
)

type benchRecord struct {
	ID      int    `json:"id"`
	Payload string `json:"payload"`
}

type benchEnvelope struct {
	Records []benchRecord `json:"records"`
}

func buildLargeFixture() []byte {
	payload := make([]byte, 0, 4096)
	for len(payload) < 4096 {
		payload = append(payload, "abcdefghijklmnopqrstuvwxyz0123456789"...)
	}
	records := make([]benchRecord, 256)
	for i := range records {
		records[i] = benchRecord{ID: i, Payload: string(payload)}
	}
	data, _ := json.Marshal(benchEnvelope{Records: records})
	return data
}

func benchmarkCompact(b *testing.B, name string, data []byte) {
	b.Run(name+"/encoding_json", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			if err := json.Compact(&buf, data); err != nil {
				b.Fatalf("json.Compact failed: %v", err)
			}
		}
	})
	b.Run(name+"/compact_writer", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			if err := CompactWriter(&buf, bytes.NewReader(data), int64(len(data))); err != nil {
				b.Fatalf("CompactWriter failed: %v", err)
			}
		}
	})
}

func BenchmarkCompactSmall(b *testing.B) {
	benchmarkCompact(b, "small", benchSmallInput)
}

func BenchmarkCompactLarge(b *testing.B) {
	benchmarkCompact(b, "large", benchLargeInput)
}
