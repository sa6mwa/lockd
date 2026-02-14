package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

const (
	fuzzCompactJSONMaxBytes   int64 = 2048
	fuzzCompactSpoolThreshold int64 = 256
)

func FuzzCompactJSON(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"a":1}`),
		[]byte(`{"nested":{"array":[1,2,3],"bool":true}}`),
		[]byte(`[]`),
	}
	for _, seed := range seeds {
		f.Add(seed, uint8(0), uint8(0), uint8(0))
	}
	f.Fuzz(func(t *testing.T, raw []byte, sizeSel, chunkSel, maxSel uint8) {
		targetSize := fuzzBoundaryInt(sizeSel, []int{
			0, 1, 2, 15, 63, 127, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025, 2047, 2048, 2049, 3072,
		})
		input := fuzzResizedBytes(raw, targetSize)
		maxBytes := fuzzBoundaryMax(maxSel, int64(len(input)), fuzzCompactJSONMaxBytes)
		readChunk := fuzzBoundaryInt(chunkSel, []int{1, 2, 3, 7, 16, 31, 32, 64, 127, 255, 256})

		h := New(Config{
			Store:                memory.New(),
			Logger:               pslog.NoopLogger(),
			JSONMaxBytes:         fuzzCompactJSONMaxBytes,
			DefaultTTL:           time.Second,
			MaxTTL:               time.Minute,
			AcquireBlock:         time.Second,
			SpoolMemoryThreshold: fuzzCompactSpoolThreshold,
		})
		spool := newPayloadSpool(fuzzCompactSpoolThreshold)
		defer spool.Close()
		err := h.compactWriter(spool, &fuzzChunkedReader{data: input, chunk: readChunk}, maxBytes)
		if maxBytes > 0 && int64(len(input)) > maxBytes {
			if err == nil {
				t.Fatalf("expected size error for payload=%d max=%d", len(input), maxBytes)
			}
			return
		}

		var ref bytes.Buffer
		refErr := json.Compact(&ref, input)
		if refErr != nil {
			if err == nil {
				t.Fatalf("expected compact error for invalid json input %q", input)
			}
			return
		}
		if err != nil {
			t.Fatalf("compact writer unexpected error: %v", err)
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
		if !bytes.Equal(payload, ref.Bytes()) {
			t.Fatalf("compact json mismatch\n got: %q\nwant:%q", payload, ref.Bytes())
		}
	})
}

func fuzzBoundaryInt(sel uint8, values []int) int {
	if len(values) == 0 {
		return 0
	}
	return values[int(sel)%len(values)]
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

func fuzzBoundaryMax(sel uint8, payloadLen, defaultMax int64) int64 {
	options := []int64{
		0,
		defaultMax - 1,
		defaultMax,
		defaultMax + 1,
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
