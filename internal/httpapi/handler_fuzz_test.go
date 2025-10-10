package httpapi

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	port "pkt.systems/logport"

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
			Store:        memory.New(),
			Logger:       port.NoopLogger(),
			JSONMaxBytes: 1 << 20,
			DefaultTTL:   time.Second,
			MaxTTL:       time.Minute,
			AcquireBlock: time.Second,
		})
		payload, err := h.compactJSON(bytes.NewReader(input))
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
