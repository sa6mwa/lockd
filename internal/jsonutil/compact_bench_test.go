package jsonutil

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	jsonutilv2 "pkt.systems/lockd/internal/jsonutilv2"
)

var (
	benchSmallInput  = []byte(`{"foo": [1, 2, 3], "bar": {"baz": true}, "str": "hello world"}`)
	benchLargeInput  = buildLargeFixture()
	benchMediumInput = buildMediumFixture()
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

func buildMediumFixture() []byte {
	var sb strings.Builder
	sb.Grow(60_000)
	sb.WriteString(`{"list":[`)
	for i := 0; i < 128; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.Itoa(i))
		sb.WriteByte(',')
		sb.WriteString(strconv.FormatFloat(float64(i)/3.14, 'f', 4, 64))
		sb.WriteString(`,{"msg":"`)
		sb.WriteString(strings.Repeat("x", 100))
		sb.WriteString(`","flag":`)
		sb.WriteString(strconv.FormatBool(i%2 == 0))
		sb.WriteString(`}`)
	}
	sb.WriteString(`],"meta":{"count":128}}`)
	return []byte(sb.String())
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
	b.Run(name+"/compact_lockd", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			if err := CompactWriter(&buf, bytes.NewReader(data), int64(len(data))); err != nil {
				b.Fatalf("CompactWriter failed: %v", err)
			}
		}
	})
	b.Run(name+"/compact_jsonv2", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			if err := jsonutilv2.CompactWriter(&buf, bytes.NewReader(data), int64(len(data))); err != nil {
				b.Fatalf("CompactWriter v2 failed: %v", err)
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

func BenchmarkCompactMedium(b *testing.B) {
	benchmarkCompact(b, "medium", benchMediumInput)
}
