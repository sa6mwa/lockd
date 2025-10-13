package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	port "pkt.systems/logport"
	"pkt.systems/logport/adapters/zerologger"

	"pkt.systems/lockd/internal/api"
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

func TestAcquireConflictAndWaiting(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	logger := port.NoopLogger()
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

	conflict := api.AcquireRequest{Key: "stream", Owner: "worker-b", TTLSeconds: 5}
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
		Logger:       port.NoopLogger(),
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
		Logger:       port.NoopLogger(),
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
	now time.Time
}

func newStubClock(start time.Time) *stubClock {
	return &stubClock{now: start}
}

func (c *stubClock) Now() time.Time {
	return c.now
}

func (c *stubClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.now = c.now.Add(d)
	ch <- c.now
	return ch
}

func (c *stubClock) Sleep(d time.Duration) {
	c.now = c.now.Add(d)
}
