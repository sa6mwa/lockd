package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	benchKey     = "bench-key"
	benchLease   = "bench-lease"
	benchETag    = `"etag-1"`
	benchVersion = "1"
	benchFence   = "1"
	benchFenceV  = int64(1)
)

func newClientBenchmarkServer(payload []byte) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/get_state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("X-Lease-ID") != benchLease {
			http.Error(w, "missing lease", http.StatusForbidden)
			return
		}
		if r.Header.Get("X-Fencing-Token") != benchFence {
			http.Error(w, "missing fencing", http.StatusForbidden)
			return
		}
		if len(payload) == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", strings.Trim(benchETag, `"`))
		w.Header().Set("X-Key-Version", benchVersion)
		w.Header().Set("X-Fencing-Token", benchFence)
		if _, err := w.Write(payload); err != nil {
			panic(err)
		}
	})
	mux.HandleFunc("/v1/update_state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("X-Lease-ID") != benchLease {
			http.Error(w, "missing lease", http.StatusForbidden)
			return
		}
		if r.Header.Get("X-Fencing-Token") != benchFence {
			http.Error(w, "missing fencing", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Fencing-Token", benchFence)
		n, err := io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		if err != nil {
			http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusInternalServerError)
			return
		}
		resp := UpdateResult{
			NewVersion:   2,
			NewStateETag: strings.Trim(benchETag, `"`),
			BytesWritten: n,
		}
		if err := json.NewEncoder(w).Encode(&resp); err != nil {
			panic(err)
		}
	})
	return httptest.NewServer(mux)
}

func makeBenchmarkJSON(size int) []byte {
	doc := map[string]string{
		"payload": strings.Repeat("x", size),
	}
	data, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return data
}

type jsonPayloadStream struct {
	prefix    []byte
	remaining int64
	suffix    []byte
	stage     int
}

func newJSONPayloadStream(bodyLen int64) *jsonPayloadStream {
	return &jsonPayloadStream{
		prefix:    []byte(`{"payload":"`),
		remaining: bodyLen,
		suffix:    []byte(`"}`),
	}
}

func (s *jsonPayloadStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var total int
	for len(p) > 0 {
		switch s.stage {
		case 0:
			if len(s.prefix) == 0 {
				s.stage = 1
				continue
			}
			n := copy(p, s.prefix)
			s.prefix = s.prefix[n:]
			total += n
			p = p[n:]
		case 1:
			if s.remaining == 0 {
				s.stage = 2
				continue
			}
			chunk := min(int64(len(p)), s.remaining)
			for i := 0; i < int(chunk); i++ {
				p[i] = 'x'
			}
			s.remaining -= chunk
			total += int(chunk)
			p = p[chunk:]
		case 2:
			if len(s.suffix) == 0 {
				s.stage = 3
				continue
			}
			n := copy(p, s.suffix)
			s.suffix = s.suffix[n:]
			total += n
			p = p[n:]
		case 3:
			if total == 0 {
				return 0, io.EOF
			}
			return total, nil
		}
	}
	return total, nil
}

func BenchmarkClientGetBytes(b *testing.B) {
	payload := makeBenchmarkJSON(256 * 1024)
	server := newClientBenchmarkServer(payload)
	defer server.Close()
	cli, err := New(server.URL)
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	cli.RegisterLeaseToken(benchLease, benchFenceV)
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for b.Loop() {
		resp, err := cli.Get(ctx, benchKey,
			WithGetLeaseID(benchLease),
			WithGetPublicDisabled(true),
		)
		if err != nil {
			b.Fatalf("get state bytes: %v", err)
		}
		if resp != nil {
			data, err := resp.Bytes()
			resp.Close()
			if err != nil {
				b.Fatalf("read state bytes: %v", err)
			}
			if len(data) != len(payload) {
				b.Fatalf("unexpected payload length: got %d want %d", len(data), len(payload))
			}
		}
	}
}

func BenchmarkClientGetStream(b *testing.B) {
	payload := makeBenchmarkJSON(256 * 1024)
	server := newClientBenchmarkServer(payload)
	defer server.Close()
	cli, err := New(server.URL)
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	cli.RegisterLeaseToken(benchLease, benchFenceV)
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for b.Loop() {
		resp, err := cli.Get(ctx, benchKey,
			WithGetLeaseID(benchLease),
			WithGetPublicDisabled(true),
		)
		if err != nil {
			b.Fatalf("get state stream: %v", err)
		}
		if resp == nil {
			b.Fatal("expected response")
		}
		reader := resp.Reader()
		if reader == nil {
			b.Fatal("expected reader")
		}
		written, err := io.Copy(io.Discard, reader)
		resp.Close()
		if err != nil {
			b.Fatalf("stream copy: %v", err)
		}
		if int(written) != len(payload) {
			b.Fatalf("unexpected payload length: got %d want %d", written, len(payload))
		}
	}
}

func BenchmarkClientUpdateBytes(b *testing.B) {
	payload := makeBenchmarkJSON(256 * 1024)
	server := newClientBenchmarkServer(nil)
	defer server.Close()
	cli, err := New(server.URL)
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for b.Loop() {
		if _, err := cli.UpdateBytes(ctx, benchKey, benchLease, payload, UpdateOptions{}); err != nil {
			b.Fatalf("update state bytes: %v", err)
		}
	}
}

func BenchmarkClientUpdateStream(b *testing.B) {
	bodyLen := int64(256 * 1024)
	server := newClientBenchmarkServer(nil)
	defer server.Close()
	cli, err := New(server.URL)
	if err != nil {
		b.Fatalf("new client: %v", err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(bodyLen)

	for b.Loop() {
		reader := newJSONPayloadStream(bodyLen)
		if _, err := cli.Update(ctx, benchKey, benchLease, reader, UpdateOptions{}); err != nil {
			b.Fatalf("update state stream: %v", err)
		}
	}
}
