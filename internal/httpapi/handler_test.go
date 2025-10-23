package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/logport"
	"pkt.systems/logport/adapters/zerologger"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/storage/memory"
)

func TestAcquireLifecycle(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	logger := zerologger.NewStructured(io.Discard)

	handler := New(Config{
		Store:        store,
		Logger:       logger,
		Clock:        clk,
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   15 * time.Second,
		MaxTTL:       1 * time.Minute,
		AcquireBlock: 10 * time.Second,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	acquireReq := api.AcquireRequest{
		Key:        "orders",
		Owner:      "worker-1",
		TTLSeconds: 10,
	}
	var acquireResp api.AcquireResponse
	status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, acquireReq, &acquireResp)
	if status != http.StatusOK {
		t.Fatalf("expected acquire 200, got %d", status)
	}
	if acquireResp.LeaseID == "" {
		t.Fatal("expected lease id")
	}
	if acquireResp.Version != 0 {
		t.Fatalf("expected version 0, got %d", acquireResp.Version)
	}
	fencingToken := strconv.FormatInt(acquireResp.FencingToken, 10)

	keepReq := api.KeepAliveRequest{
		Key:        "orders",
		LeaseID:    acquireResp.LeaseID,
		TTLSeconds: 20,
	}
	var keepResp api.KeepAliveResponse
	status = doJSON(t, server, http.MethodPost, "/v1/keepalive", map[string]string{"X-Fencing-Token": fencingToken}, keepReq, &keepResp)
	if status != http.StatusOK {
		t.Fatalf("expected keepalive 200, got %d", status)
	}
	if keepResp.ExpiresAt <= acquireResp.ExpiresAt {
		t.Fatal("expected keepalive to extend expiry")
	}

	stateHeaders := map[string]string{
		"X-Lease-ID":      acquireResp.LeaseID,
		"X-Fencing-Token": fencingToken,
	}
	updateBody := map[string]any{"cursor": 42}
	status = doJSON(t, server, http.MethodPost, "/v1/update-state?key=orders", stateHeaders, updateBody, nil)
	if status != http.StatusOK {
		t.Fatalf("expected update state 200, got %d", status)
	}

	getReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/get-state?key=orders", http.NoBody)
	getReq.Header.Set("X-Lease-ID", acquireResp.LeaseID)
	getReq.Header.Set("X-Fencing-Token", fencingToken)
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("get_state request failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected get_state 200, got %d", getResp.StatusCode)
	}
	var state map[string]any
	if err := json.NewDecoder(getResp.Body).Decode(&state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if state["cursor"].(float64) != 42 {
		t.Fatalf("expected cursor 42, got %v", state["cursor"])
	}

	releaseReq := api.ReleaseRequest{
		Key:     "orders",
		LeaseID: acquireResp.LeaseID,
	}
	var releaseResp api.ReleaseResponse
	status = doJSON(t, server, http.MethodPost, "/v1/release", map[string]string{"X-Fencing-Token": fencingToken}, releaseReq, &releaseResp)
	if status != http.StatusOK {
		t.Fatalf("expected release 200, got %d", status)
	}
	if !releaseResp.Released {
		t.Fatal("expected release to succeed")
	}
}

func TestAcquireAutoGeneratesKey(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	logger := zerologger.NewStructured(io.Discard)

	handler := New(Config{
		Store:        store,
		Logger:       logger,
		Clock:        clk,
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   15 * time.Second,
		MaxTTL:       1 * time.Minute,
		AcquireBlock: 10 * time.Second,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	autoReq := api.AcquireRequest{
		Owner:      "auto-worker",
		TTLSeconds: 12,
	}
	var autoResp api.AcquireResponse
	status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, autoReq, &autoResp)
	if status != http.StatusOK {
		t.Fatalf("expected acquire 200, got %d", status)
	}
	if autoResp.Key == "" {
		t.Fatal("expected server to generate key")
	}
	ctx := context.Background()
	meta, _, err := store.LoadMeta(ctx, autoResp.Key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if meta.Lease == nil || meta.Lease.Owner != autoReq.Owner {
		t.Fatalf("unexpected lease stored: %+v", meta.Lease)
	}

	stateHeaders := map[string]string{
		"X-Lease-ID":      autoResp.LeaseID,
		"X-Fencing-Token": strconv.FormatInt(autoResp.FencingToken, 10),
	}
	updateBody := map[string]any{"auto": true}
	status = doJSON(t, server, http.MethodPost, "/v1/update-state?key="+autoResp.Key, stateHeaders, updateBody, nil)
	if status != http.StatusOK {
		t.Fatalf("expected update state 200, got %d", status)
	}
	var releaseResp api.ReleaseResponse
	status = doJSON(t, server, http.MethodPost, "/v1/release", stateHeaders, api.ReleaseRequest{Key: autoResp.Key, LeaseID: autoResp.LeaseID}, &releaseResp)
	if status != http.StatusOK || !releaseResp.Released {
		t.Fatalf("release failed: status=%d resp=%+v", status, releaseResp)
	}

	secondReq := api.AcquireRequest{
		Owner:      "auto-worker-2",
		TTLSeconds: 8,
	}
	var secondResp api.AcquireResponse
	status = doJSON(t, server, http.MethodPost, "/v1/acquire", nil, secondReq, &secondResp)
	if status != http.StatusOK {
		t.Fatalf("second acquire expected 200, got %d", status)
	}
	if secondResp.Key == "" {
		t.Fatal("expected generated key on second acquire")
	}
	if secondResp.Key == autoResp.Key {
		t.Fatalf("expected distinct keys, got %q twice", autoResp.Key)
	}
}

func newTestHTTPServer(t *testing.T) *httptest.Server {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	logger := logport.NoopLogger()
	handler := New(Config{
		Store:        store,
		Logger:       logger,
		Clock:        clk,
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   15 * time.Second,
		MaxTTL:       1 * time.Minute,
		AcquireBlock: 10 * time.Second,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func TestAcquireCorrelationEcho(t *testing.T) {
	server := newTestHTTPServer(t)
	corr := "corr-123"
	body := api.AcquireRequest{Key: "corr", Owner: "tester", TTLSeconds: 5}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/acquire", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerCorrelationID, corr)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out api.AcquireResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.CorrelationID != corr {
		t.Fatalf("expected correlation id %q, got %q", corr, out.CorrelationID)
	}
	if hdr := resp.Header.Get(headerCorrelationID); hdr != corr {
		t.Fatalf("expected header correlation id %q, got %q", corr, hdr)
	}
}

func TestAcquireCorrelationInvalid(t *testing.T) {
	server := newTestHTTPServer(t)
	invalid := strings.Repeat("a", correlation.MaxIDLength+1)
	body := api.AcquireRequest{Key: "corr", Owner: "tester", TTLSeconds: 5}
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/acquire", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerCorrelationID, invalid)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out api.AcquireResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.CorrelationID == "" {
		t.Fatal("expected generated correlation id")
	}
	if len(out.CorrelationID) > correlation.MaxIDLength {
		t.Fatalf("correlation id too long: %d", len(out.CorrelationID))
	}
	if hdr := resp.Header.Get(headerCorrelationID); hdr != out.CorrelationID {
		t.Fatalf("header mismatch: %q vs %q", hdr, out.CorrelationID)
	}
}

func TestAcquireConflictAndWaiting(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	logger := logport.NoopLogger()
	handler := New(Config{
		Store:        store,
		Logger:       logger,
		Clock:        clk,
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   10 * time.Second,
		MaxTTL:       1 * time.Minute,
		AcquireBlock: 5 * time.Second,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	first := api.AcquireRequest{Key: "stream", Owner: "worker-a", TTLSeconds: 5}
	var resp api.AcquireResponse
	if status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, first, &resp); status != http.StatusOK {
		t.Fatalf("expected first acquire 200, got %d", status)
	}

	conflict := api.AcquireRequest{Key: "stream", Owner: "worker-b", TTLSeconds: 5, BlockSecs: api.BlockNoWait}
	var errResp api.ErrorResponse
	status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, conflict, &errResp)
	if status != http.StatusConflict {
		t.Fatalf("expected conflict 409, got %d", status)
	}
	if errResp.ErrorCode != "waiting" {
		t.Fatalf("expected waiting error code, got %s", errResp.ErrorCode)
	}
	if errResp.RetryAfterSeconds == 0 {
		t.Fatal("expected retry hint")
	}

	blocking := api.AcquireRequest{Key: "stream", Owner: "worker-b", TTLSeconds: 5, BlockSecs: 5}
	status = doJSON(t, server, http.MethodPost, "/v1/acquire", nil, blocking, &resp)
	if status != http.StatusOK {
		t.Fatalf("expected acquire to eventually succeed, got %d", status)
	}
	if resp.Owner != "worker-b" {
		t.Fatalf("expected new owner worker-b, got %s", resp.Owner)
	}
}

func TestUpdateStateVersionMismatch(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	handler := New(Config{
		Store:        store,
		Logger:       logport.NoopLogger(),
		Clock:        clk,
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   10 * time.Second,
		MaxTTL:       1 * time.Minute,
		AcquireBlock: 2 * time.Second,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	req := api.AcquireRequest{Key: "stream", Owner: "worker-a", TTLSeconds: 5}
	var acquire api.AcquireResponse
	doJSON(t, server, http.MethodPost, "/v1/acquire", nil, req, &acquire)

	token := strconv.FormatInt(acquire.FencingToken, 10)
	headers := map[string]string{"X-Lease-ID": acquire.LeaseID, "X-Fencing-Token": token}
	doJSON(t, server, http.MethodPost, "/v1/update-state?key=stream", headers, map[string]int{"pos": 1}, nil)

	headers["X-If-Version"] = "0"
	var errResp api.ErrorResponse
	status := doJSON(t, server, http.MethodPost, "/v1/update-state?key=stream", headers, map[string]int{"pos": 2}, &errResp)
	if status != http.StatusConflict {
		t.Fatalf("expected 409 conflict, got %d", status)
	}
	if errResp.ErrorCode != "version_conflict" {
		t.Fatalf("expected version_conflict, got %s", errResp.ErrorCode)
	}
}

func TestGetStateRequiresLease(t *testing.T) {
	store := memory.New()
	handler := New(Config{
		Store:        store,
		Logger:       logport.NoopLogger(),
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   5 * time.Second,
		MaxTTL:       30 * time.Second,
		AcquireBlock: 1 * time.Second,
		Clock:        newStubClock(time.Unix(1_700_000_000, 0)),
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	acq := api.AcquireRequest{Key: "alpha", Owner: "worker", TTLSeconds: 5}
	var acquire api.AcquireResponse
	doJSON(t, server, http.MethodPost, "/v1/acquire", nil, acq, &acquire)
	token := strconv.FormatInt(acquire.FencingToken, 10)
	headers := map[string]string{"X-Lease-ID": acquire.LeaseID, "X-Fencing-Token": token}
	doJSON(t, server, http.MethodPost, "/v1/update-state?key=alpha", headers, map[string]int{"pos": 1}, nil)

	getReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/get-state?key=alpha", http.NoBody)
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 missing lease, got %d", resp.StatusCode)
	}
}

func doJSON(t *testing.T, server *httptest.Server, method, path string, headers map[string]string, body any, out any) int {
	t.Helper()
	var payload io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		payload = bytes.NewReader(buf)
	} else {
		payload = http.NoBody
	}
	req, err := http.NewRequest(method, server.URL+path, payload)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if out != nil {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if len(data) > 0 {
			if err := json.Unmarshal(data, out); err != nil {
				t.Fatalf("decode response: %v (body=%s)", err, string(data))
			}
		}
	}
	return resp.StatusCode
}

type stubClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func newStubClock(start time.Time) *stubClock {
	return &stubClock{now: start}
}

func (c *stubClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stubClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	next := c.now
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- next
	return ch
}

func (c *stubClock) Sleep(d time.Duration) {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *stubClock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

func TestWaitBackoffGrowth(t *testing.T) {
	b := newAcquireBackoff()
	b.rand = func(n int64) int64 { return n / 2 }
	var got []time.Duration
	for i := 0; i < 6; i++ {
		got = append(got, b.Next(0))
	}
	cur := acquireBackoffStart
	for i, sleep := range got {
		if sleep != cur {
			t.Fatalf("sleep[%d]=%s, want %s", i, sleep, cur)
		}
		next := time.Duration(float64(cur)*acquireBackoffMultiplier + 0.5)
		if next <= 0 {
			next = acquireBackoffStart
		}
		if next > acquireBackoffMax {
			next = acquireBackoffMax
		}
		cur = next
	}
}

func TestWaitBackoffJitterBounds(t *testing.T) {
	b := newAcquireBackoff()
	b.rand = func(int64) int64 { return 0 }
	sleep := b.Next(0)
	if sleep < 0 {
		t.Fatalf("sleep negative: %s", sleep)
	}
	if sleep > acquireBackoffStart+acquireBackoffJitter {
		t.Fatalf("sleep %s exceeded jitter bound", sleep)
	}
}

func TestWaitBackoffRespectsLimit(t *testing.T) {
	b := newAcquireBackoff()
	b.rand = func(n int64) int64 { return n - 1 }
	limit := 80 * time.Millisecond
	sleep := b.Next(limit)
	if sleep > limit {
		t.Fatalf("sleep %s exceeded limit %s", sleep, limit)
	}
	if sleep < 0 {
		t.Fatalf("sleep negative: %s", sleep)
	}
}
