package lockd

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/pslog"
)

func FuzzAuthenticatedAPISurfaceNoServerError(f *testing.F) {
	ts := startTestServerFast(
		f,
		WithTestMTLS(),
		WithTestLogger(pslog.NoopLogger()),
		WithTestConfigFunc(func(cfg *Config) {
			cfg.ConnguardEnabled = false
			cfg.ConnguardEnabledSet = true
			cfg.JSONMaxBytes = 2048
			cfg.AttachmentMaxBytes = 2048
		}),
	)

	httpClient, err := ts.NewHTTPClient()
	if err != nil {
		f.Fatalf("new http client: %v", err)
	}

	f.Add(uint8(0), uint8(0), "default", "orders/1", []byte(`{}`), uint8(0), uint8(0))
	f.Add(uint8(1), uint8(1), "default", "../logstore/segment", []byte(`{"namespace":"default"}`), uint8(1), uint8(1))
	f.Add(uint8(2), uint8(2), "testing", "..", []byte(`{"selector":{"foo":{"$eq":"bar"}}}`), uint8(2), uint8(2))
	f.Add(uint8(3), uint8(0), "default", "%2e%2e/%2e%2e", []byte(`{"bogus":true}`), uint8(3), uint8(3))

	f.Fuzz(func(t *testing.T, routeSel, methodSel uint8, namespace, key string, body []byte, bodySizeSel, bodyChunkSel uint8) {
		if len(namespace) > 128 {
			namespace = namespace[:128]
		}
		if len(key) > 256 {
			key = key[:256]
		}
		body = fuzzResizedBytes(body, fuzzBoundaryInt(bodySizeSel, []int{
			0, 1, 2, 15, 63, 127, 255, 256, 511, 512, 1023, 1024, 2047, 2048, 2049, 4096,
		}))
		chunkSize := fuzzBoundaryInt(bodyChunkSel, []int{1, 2, 3, 7, 16, 31, 32, 64, 255, 256})

		routes := []string{
			"/v1/get",
			"/v1/query",
			"/v1/describe",
			"/v1/update",
			"/v1/metadata",
			"/v1/remove",
			"/v1/namespace",
			"/v1/attachments",
			"/v1/attachment",
		}
		methods := []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodDelete,
			http.MethodPut,
		}

		route := routes[int(routeSel)%len(routes)]
		method := methods[int(methodSel)%len(methods)]
		values := url.Values{}
		values.Set("namespace", namespace)
		values.Set("key", key)
		values.Set("name", key)
		target := ts.URL() + route + "?" + values.Encode()

		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		bodyReader := io.Reader(bytes.NewReader(body))
		if len(body) > 0 {
			bodyReader = &fuzzChunkedReader{data: body, chunk: chunkSize}
		}
		req, err := http.NewRequestWithContext(ctx, method, target, bodyReader)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("api request failed %s %s: %v", method, route, err)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32*1024))
		_ = resp.Body.Close()
		if resp.StatusCode >= http.StatusInternalServerError {
			t.Fatalf("unexpected server error status=%d route=%s method=%s", resp.StatusCode, route, method)
		}

		healthReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/healthz", http.NoBody)
		if err != nil {
			t.Fatalf("health request: %v", err)
		}
		healthResp, err := httpClient.Do(healthReq)
		if err != nil {
			t.Fatalf("health check failed after fuzzed request: %v", err)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(healthResp.Body, 16*1024))
		_ = healthResp.Body.Close()
		if healthResp.StatusCode != http.StatusOK {
			t.Fatalf("health check status=%d after route=%s method=%s", healthResp.StatusCode, route, method)
		}
	})
}

func FuzzDiskNamespaceAndPathContainment(f *testing.F) {
	parent := f.TempDir()
	storeRoot := filepath.Join(parent, "store")
	canaryPath := filepath.Join(parent, "outside-canary.txt")
	canaryContent := []byte("lockd-fuzz-canary")
	if err := os.WriteFile(canaryPath, canaryContent, 0o600); err != nil {
		f.Fatalf("write canary: %v", err)
	}

	ts := startTestServerFast(
		f,
		WithTestMTLS(),
		WithTestLogger(pslog.NoopLogger()),
		WithTestConfig(Config{
			Listen:                     "127.0.0.1:0",
			ListenProto:                "tcp",
			Store:                      "disk://" + storeRoot,
			DisableMTLS:                false,
			DisableStorageEncryption:   true,
			ConnguardEnabled:           false,
			ConnguardEnabledSet:        true,
			SweeperInterval:            time.Second,
			QueueResilientPollInterval: 100 * time.Millisecond,
		}),
	)

	cli, err := ts.NewClient()
	if err != nil {
		f.Fatalf("new client: %v", err)
	}

	allowedSiblings, err := dirEntryNames(parent)
	if err != nil {
		f.Fatalf("read parent dir: %v", err)
	}

	f.Add("default", "orders/1", []byte("state-a"), uint8(0))
	f.Add("default", "../logstore/segment", []byte("state-b"), uint8(1))
	f.Add("..", "state/../../meta/test", []byte("state-c"), uint8(2))
	f.Add("testing", "%2e%2e/%2e%2e", []byte("state-d"), uint8(3))

	f.Fuzz(func(t *testing.T, namespace, key string, payload []byte, payloadSizeSel uint8) {
		if len(namespace) > 128 {
			namespace = namespace[:128]
		}
		if len(key) > 128 {
			key = key[:128]
		}
		payload = fuzzResizedBytes(payload, fuzzBoundaryInt(payloadSizeSel, []int{
			0, 1, 2, 15, 63, 127, 255, 256, 511, 512, 1023, 1024, 2047, 2048,
		}))

		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		req := api.AcquireRequest{
			Namespace:  namespace,
			Key:        key,
			Owner:      "fuzz-attacker",
			TTLSeconds: 2,
			BlockSecs:  api.BlockNoWait,
		}
		acq, err := cli.Acquire(ctx, req)
		if err == nil {
			state := map[string]any{"payload_hex": hex.EncodeToString(payload)}
			if err := acq.Save(ctx, state); err != nil {
				t.Fatalf("save acquired state: %v", err)
			}
			otherNS := "fuzz-other"
			if strings.EqualFold(otherNS, acq.Namespace) {
				otherNS = "fuzz-other-2"
			}
			otherRes, otherErr := cli.Get(ctx, acq.Key, lockdclient.WithGetNamespace(otherNS))
			if otherErr == nil && otherRes.HasState {
				t.Fatalf("cross-namespace read succeeded key=%q src_ns=%q dst_ns=%q", acq.Key, acq.Namespace, otherNS)
			}
			if err := acq.Release(ctx); err != nil {
				t.Fatalf("release acquired lease: %v", err)
			}
		}

		currentSiblings, err := dirEntryNames(parent)
		if err != nil {
			t.Fatalf("read parent dir: %v", err)
		}
		for name := range currentSiblings {
			if _, ok := allowedSiblings[name]; !ok {
				t.Fatalf("unexpected sibling path created outside store root: %q", name)
			}
		}

		data, err := os.ReadFile(canaryPath)
		if err != nil {
			t.Fatalf("read canary: %v", err)
		}
		if !bytes.Equal(data, canaryContent) {
			t.Fatalf("outside canary changed: %q", string(data))
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
		in = []byte("x")
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

func dirEntryNames(dir string) (map[string]struct{}, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		out[entry.Name()] = struct{}{}
	}
	return out, nil
}
