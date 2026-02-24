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

func TestMutationUnknownRejectsStrictQueueEndpointsTable(t *testing.T) {
	server := newTestHTTPServer(t)
	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "stats",
			path: "/v1/queue/stats",
			body: `{"namespace":"default","queue":"jobs","unknown":1}`,
		},
		{
			name: "dequeue",
			path: "/v1/queue/dequeue",
			body: `{"namespace":"default","queue":"jobs","owner":"w1","unknown":1}`,
		},
		{
			name: "dequeue_with_state",
			path: "/v1/queue/dequeueWithState",
			body: `{"namespace":"default","queue":"jobs","owner":"w1","unknown":1}`,
		},
		{
			name: "subscribe",
			path: "/v1/queue/subscribe",
			body: `{"namespace":"default","queue":"jobs","owner":"w1","unknown":1}`,
		},
		{
			name: "subscribe_with_state",
			path: "/v1/queue/subscribeWithState",
			body: `{"namespace":"default","queue":"jobs","owner":"w1","unknown":1}`,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			status, errResp := doJSONRaw(t, server.URL, tc.path, nil, tc.body)
			if status != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", status)
			}
			if errResp.ErrorCode != "invalid_body" {
				t.Fatalf("expected invalid_body, got %q", errResp.ErrorCode)
			}
		})
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

func TestMutationCompatibilityAllowsUnknownFieldTable(t *testing.T) {
	server := newTestHTTPServer(t)
	txnID := xid.New().String()
	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "txn_replay",
			path: "/v1/txn/replay",
			body: `{"txn_id":"` + txnID + `","unknown":1}`,
		},
		{
			name: "txn_decide",
			path: "/v1/txn/decide",
			body: `{"txn_id":"` + txnID + `","state":"pending","unknown":1}`,
		},
		{
			name: "txn_commit",
			path: "/v1/txn/commit",
			body: `{"txn_id":"` + txnID + `","unknown":1}`,
		},
		{
			name: "txn_rollback",
			path: "/v1/txn/rollback",
			body: `{"txn_id":"` + txnID + `","unknown":1}`,
		},
		{
			name: "tcrm_register",
			path: "/v1/tc/rm/register",
			body: `{"backend_hash":"hash-1","endpoint":"https://rm-1.local/api","unknown":1}`,
		},
		{
			name: "tcrm_unregister",
			path: "/v1/tc/rm/unregister",
			body: `{"backend_hash":"hash-1","endpoint":"https://rm-1.local/api","unknown":1}`,
		},
		{
			name: "tc_cluster_announce",
			path: "/v1/tc/cluster/announce",
			body: `{"self_endpoint":"https://node-1.local/api","unknown":1}`,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			status, errResp := doJSONRaw(t, server.URL, tc.path, nil, tc.body)
			if status == http.StatusBadRequest && errResp.ErrorCode == "invalid_body" {
				t.Fatalf("unexpected strict decode rejection for compatibility endpoint")
			}
		})
	}
}

func TestMutationCompatibilityTCRMRegisterAllowsUnknownField(t *testing.T) {
	server := newTestHTTPServer(t)
	status, errResp := doJSONRaw(t, server.URL, "/v1/tc/rm/register", nil, `{"backend_hash":"hash-1","endpoint":"https://rm-1.local/api","unknown":1}`)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d err=%+v", status, errResp)
	}
}
