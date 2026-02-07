package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/pslog"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/storage/memory"
)

func doJSONRaw(t *testing.T, serverURL, path string, headers map[string]string, raw string) (int, api.ErrorResponse) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, serverURL+path, bytes.NewBufferString(raw))
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
	var errResp api.ErrorResponse
	_ = json.NewDecoder(resp.Body).Decode(&errResp)
	return resp.StatusCode, errResp
}

func TestMutationUnknownRejectsAcquire(t *testing.T) {
	server := newTestHTTPServer(t)
	status, errResp := doJSONRaw(t, server.URL, "/v1/acquire", nil, `{"key":"k","owner":"o","ttl_seconds":5,"unknown":1}`)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", status)
	}
	if errResp.ErrorCode != "invalid_body" {
		t.Fatalf("expected invalid_body, got %q", errResp.ErrorCode)
	}
}

func TestMutationUnknownRejectsKeepAliveAndRelease(t *testing.T) {
	server := newTestHTTPServer(t)

	var acq api.AcquireResponse
	if status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, api.AcquireRequest{
		Key:        "orders",
		Owner:      "worker",
		TTLSeconds: 5,
	}, &acq); status != http.StatusOK {
		t.Fatalf("acquire status=%d", status)
	}

	headers := map[string]string{
		"X-Fencing-Token": strconv.FormatInt(acq.FencingToken, 10),
	}
	keepRaw := `{"namespace":"default","key":"orders","lease_id":"` + acq.LeaseID + `","ttl_seconds":5,"unknown":1}`
	status, errResp := doJSONRaw(t, server.URL, "/v1/keepalive", headers, keepRaw)
	if status != http.StatusBadRequest {
		t.Fatalf("expected keepalive 400, got %d", status)
	}
	if errResp.ErrorCode != "invalid_body" {
		t.Fatalf("expected keepalive invalid_body, got %q", errResp.ErrorCode)
	}

	releaseHeaders := map[string]string{
		"X-Fencing-Token": strconv.FormatInt(acq.FencingToken, 10),
	}
	releaseRaw := `{"namespace":"default","key":"orders","lease_id":"` + acq.LeaseID + `","txn_id":"` + acq.TxnID + `","unknown":1}`
	status, errResp = doJSONRaw(t, server.URL, "/v1/release", releaseHeaders, releaseRaw)
	if status != http.StatusBadRequest {
		t.Fatalf("expected release 400, got %d", status)
	}
	if errResp.ErrorCode != "invalid_body" {
		t.Fatalf("expected release invalid_body, got %q", errResp.ErrorCode)
	}
}

func TestMutationUnknownRejectsQueueDequeue(t *testing.T) {
	server := newTestHTTPServer(t)
	status, errResp := doJSONRaw(t, server.URL, "/v1/queue/dequeue", nil, `{"namespace":"default","queue":"jobs","owner":"w1","unknown":1}`)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", status)
	}
	if errResp.ErrorCode != "invalid_body" {
		t.Fatalf("expected invalid_body, got %q", errResp.ErrorCode)
	}
}

func TestMutationTrailingJSONRejected(t *testing.T) {
	server := newTestHTTPServer(t)
	status, errResp := doJSONRaw(t, server.URL, "/v1/acquire", nil, `{"key":"k","owner":"o","ttl_seconds":5}{"extra":1}`)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", status)
	}
	if errResp.ErrorCode != "invalid_body" {
		t.Fatalf("expected invalid_body, got %q", errResp.ErrorCode)
	}
}

func TestMutationQueueDequeueAllowsEmptyBodyWithQuery(t *testing.T) {
	handler := New(Config{
		Store:             memory.New(),
		Logger:            pslog.NoopLogger(),
		Clock:             newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes:      1 << 20,
		DefaultTTL:        15 * time.Second,
		MaxTTL:            1 * time.Minute,
		AcquireBlock:      10 * time.Second,
		QueueMaxConsumers: 4,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/queue/dequeue?namespace=default&queue=jobs&owner=worker", http.NoBody)
	ctx, cancel := context.WithTimeout(req.Context(), 20*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	err := handler.handleQueueDequeue(rec, req)
	if err == nil {
		t.Fatal("expected waiting error")
	}
	var httpErr httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected httpError, got %T", err)
	}
	if httpErr.Code != "waiting" {
		t.Fatalf("expected waiting, got %q", httpErr.Code)
	}
}

func TestMutationCompatibilityTxnDecideAllowsUnknownField(t *testing.T) {
	server := newTestHTTPServer(t)
	txnID := xid.New().String()
	status, errResp := doJSONRaw(t, server.URL, "/v1/txn/decide", nil, `{"txn_id":"`+txnID+`","state":"pending","unknown":1}`)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d err=%+v", status, errResp)
	}
}

func TestMutationCompatibilityTCRMRegisterAllowsUnknownField(t *testing.T) {
	server := newTestHTTPServer(t)
	status, errResp := doJSONRaw(t, server.URL, "/v1/tc/rm/register", nil, `{"backend_hash":"hash-1","endpoint":"https://rm-1.local/api","unknown":1}`)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d err=%+v", status, errResp)
	}
}
