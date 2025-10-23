package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"pkt.systems/logport"

	"pkt.systems/lockd/internal/storage/memory"
)

func FuzzCompactJSON(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"a":1}`),
		[]byte(`{"nested":{"array":[1,2,3],"bool":true}}`),
		[]byte(`[]`),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		h := New(Config{
			Store:                memory.New(),
			Logger:               logport.NoopLogger(),
			JSONMaxBytes:         1 << 20,
			DefaultTTL:           time.Second,
			MaxTTL:               time.Minute,
			AcquireBlock:         time.Second,
			SpoolMemoryThreshold: defaultPayloadSpoolMemoryThreshold,
		})
		spool := newPayloadSpool(defaultPayloadSpoolMemoryThreshold)
		defer spool.Close()
		if err := h.compactWriter(spool, bytes.NewReader(input), h.jsonMaxBytes); err != nil {
			return
		}
		reader, err := spool.Reader()
		if err != nil {
			return
		}
		payload, err := io.ReadAll(reader)
		if err != nil {
			return
		}
		if !json.Valid(payload) {
			t.Fatalf("compact json produced invalid output: %q", payload)
		}
		if bytes.Contains(payload, []byte("  ")) {
			t.Fatalf("compact json contains double space: %q", payload)
		}
	})
}
