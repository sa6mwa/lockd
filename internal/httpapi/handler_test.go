package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/pslog"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/search"
	scanadapter "pkt.systems/lockd/internal/search/scan"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
)

func scanNamespaceConfig() namespaces.Config {
	return namespaces.Config{
		Query: namespaces.QueryEngines{
			Preferred: search.EngineScan,
			Fallback:  namespaces.FallbackNone,
		},
	}
}

func TestAcquireLifecycle(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	logger := pslog.NewStructured(context.Background(), io.Discard)

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
		TxnID:      acquireResp.TxnID,
	}
	var keepResp api.KeepAliveResponse
	status = doJSON(t, server, http.MethodPost, "/v1/keepalive", map[string]string{"X-Fencing-Token": fencingToken, "X-Txn-ID": acquireResp.TxnID}, keepReq, &keepResp)
	if status != http.StatusOK {
		t.Fatalf("expected keepalive 200, got %d", status)
	}
	if keepResp.ExpiresAt <= acquireResp.ExpiresAt {
		t.Fatal("expected keepalive to extend expiry")
	}

	stateHeaders := map[string]string{
		"X-Lease-ID":      acquireResp.LeaseID,
		"X-Fencing-Token": fencingToken,
		"X-Txn-ID":        acquireResp.TxnID,
	}
	updateBody := map[string]any{"cursor": 42}
	status = doJSON(t, server, http.MethodPost, "/v1/update?key=orders", stateHeaders, updateBody, nil)
	if status != http.StatusOK {
		t.Fatalf("expected update state 200, got %d", status)
	}

	getReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/get?key=orders", http.NoBody)
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
		TxnID:   acquireResp.TxnID,
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

func TestAttachmentContentLengthHeader(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	logger := pslog.NewStructured(context.Background(), io.Discard)

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
	token := strconv.FormatInt(acquireResp.FencingToken, 10)

	payload := []byte("payload-bytes")
	sum := sha256.Sum256(payload)
	expectedSHA := hex.EncodeToString(sum[:])
	attachReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/attachments?key=orders&name=payload.bin", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new attach request: %v", err)
	}
	attachReq.Header.Set("X-Lease-ID", acquireResp.LeaseID)
	attachReq.Header.Set("X-Fencing-Token", token)
	attachReq.Header.Set("X-Txn-ID", acquireResp.TxnID)
	attachReq.Header.Set("Content-Type", "application/octet-stream")
	attachResp, err := http.DefaultClient.Do(attachReq)
	if err != nil {
		t.Fatalf("attach request: %v", err)
	}
	defer attachResp.Body.Close()
	if attachResp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(attachResp.Body)
		t.Fatalf("attach status=%d body=%s", attachResp.StatusCode, strings.TrimSpace(string(data)))
	}

	releaseReq := api.ReleaseRequest{
		Key:     "orders",
		LeaseID: acquireResp.LeaseID,
		TxnID:   acquireResp.TxnID,
	}
	status = doJSON(t, server, http.MethodPost, "/v1/release", map[string]string{"X-Fencing-Token": token}, releaseReq, nil)
	if status != http.StatusOK {
		t.Fatalf("expected release 200, got %d", status)
	}

	getReq, err := http.NewRequest(http.MethodGet, server.URL+"/v1/attachment?key=orders&name=payload.bin&public=1", http.NoBody)
	if err != nil {
		t.Fatalf("new get request: %v", err)
	}
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("get attachment: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(getResp.Body)
		t.Fatalf("get attachment status=%d body=%s", getResp.StatusCode, strings.TrimSpace(string(data)))
	}
	size, err := strconv.ParseInt(getResp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		t.Fatalf("parse content-length: %v", err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("content-length %d want %d", size, len(payload))
	}
	if headerSize := getResp.Header.Get("X-Attachment-Size"); headerSize != "" {
		declared, err := strconv.ParseInt(headerSize, 10, 64)
		if err != nil {
			t.Fatalf("parse x-attachment-size: %v", err)
		}
		if declared != int64(len(payload)) {
			t.Fatalf("x-attachment-size %d want %d", declared, len(payload))
		}
	}
	if got := strings.TrimSpace(getResp.Header.Get("X-Attachment-SHA256")); got != expectedSHA {
		t.Fatalf("x-attachment-sha256 %q want %q", got, expectedSHA)
	}
	data, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("read attachment body: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("unexpected attachment payload %q", data)
	}
}

func TestQueryDisabled(t *testing.T) {
	store := memory.New()
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NewStructured(context.Background(), io.Discard),
		Clock:        newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes: 1 << 20,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/query?namespace=default", http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("query request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d", resp.StatusCode)
	}
	var errResp api.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if errResp.ErrorCode != "query_disabled" {
		t.Fatalf("expected query_disabled, got %s", errResp.ErrorCode)
	}
}

func TestQuerySuccess(t *testing.T) {
	store := memory.New()
	adapter := &stubSearchAdapter{
		result: search.Result{
			Keys:     []string{"orders/1", "orders/2"},
			Cursor:   "next-cursor",
			IndexSeq: 42,
			Metadata: map[string]string{"hits": "2"},
		},
	}
	handler := New(Config{
		Store:                  store,
		Logger:                 pslog.NewStructured(context.Background(), io.Discard),
		Clock:                  newStubClock(time.Unix(1_700_000_000, 0)),
		SearchAdapter:          adapter,
		DefaultNamespaceConfig: scanNamespaceConfig(),
		JSONMaxBytes:           1 << 20,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	reqBody := api.QueryRequest{
		Namespace: "custom",
		Selector: api.Selector{
			Eq: &api.Term{Field: "/status", Value: "open"},
		},
		Limit: 5,
	}
	payload, _ := json.Marshal(reqBody)
	resp, err := http.Post(server.URL+"/v1/query", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("query request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, data)
	}
	var out api.QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Cursor != "next-cursor" || out.IndexSeq != 42 {
		t.Fatalf("unexpected response %+v", out)
	}
	if len(out.Keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(out.Keys))
	}
	if adapter.last.Namespace != "custom" {
		t.Fatalf("adapter namespace mismatch: %s", adapter.last.Namespace)
	}
	if adapter.last.Limit != 5 {
		t.Fatalf("expected limit 5, got %d", adapter.last.Limit)
	}
	if adapter.last.Selector.Eq == nil || adapter.last.Selector.Eq.Field != "/status" {
		t.Fatalf("selector not forwarded: %+v", adapter.last.Selector)
	}
	if adapter.last.Engine != search.EngineScan {
		t.Fatalf("expected scan engine, got %s", adapter.last.Engine)
	}
}

func TestTxnEndpointsRequireTCAuth(t *testing.T) {
	store := memory.New()
	handler := New(Config{
		Store:         store,
		Logger:        pslog.NewStructured(context.Background(), io.Discard),
		Clock:         newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes:  1 << 20,
		TCAuthEnabled: true,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	type tcCase struct {
		name string
		path string
		body any
	}
	cases := []tcCase{
		{name: "decide", path: "/v1/txn/decide", body: api.TxnDecisionRequest{}},
		{name: "commit", path: "/v1/txn/commit", body: api.TxnDecisionRequest{}},
		{name: "rollback", path: "/v1/txn/rollback", body: api.TxnDecisionRequest{}},
		{name: "replay", path: "/v1/txn/replay", body: api.TxnReplayRequest{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var errResp api.ErrorResponse
			status := doJSON(t, server, http.MethodPost, tc.path, nil, tc.body, &errResp)
			if status != http.StatusForbidden {
				t.Fatalf("expected 403, got %d", status)
			}
			if errResp.ErrorCode != "tc_client_required" {
				t.Fatalf("expected tc_client_required, got %s", errResp.ErrorCode)
			}
		})
	}
}

func TestTxnDecideNormalizesParticipantNamespace(t *testing.T) {
	store := memory.New()
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NoopLogger(),
		Clock:        newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   15 * time.Second,
		MaxTTL:       1 * time.Minute,
		AcquireBlock: 10 * time.Second,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	txnID := xid.New().String()
	var resp api.TxnDecisionResponse
	status := doJSON(t, server, http.MethodPost, "/v1/txn/decide", nil, api.TxnDecisionRequest{
		TxnID: txnID,
		State: string(core.TxnStatePending),
		Participants: []api.TxnParticipant{
			{Namespace: "MiXeD_Ns", Key: "orders"},
		},
	}, &resp)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}

	obj, err := store.GetObject(context.Background(), ".txns", txnID)
	if err != nil {
		t.Fatalf("load txn record: %v", err)
	}
	defer obj.Reader.Close()

	var rec core.TxnRecord
	if err := json.NewDecoder(obj.Reader).Decode(&rec); err != nil {
		t.Fatalf("decode txn record: %v", err)
	}
	if len(rec.Participants) != 1 {
		t.Fatalf("expected 1 participant, got %d", len(rec.Participants))
	}
	if got := rec.Participants[0].Namespace; got != "mixed_ns" {
		t.Fatalf("expected normalized namespace mixed_ns, got %q", got)
	}
}

type captureLoadMetaStore struct {
	*memory.Store
	mu         sync.Mutex
	namespaces []string
}

func (s *captureLoadMetaStore) LoadMeta(ctx context.Context, namespace, key string) (storage.LoadMetaResult, error) {
	s.mu.Lock()
	s.namespaces = append(s.namespaces, namespace)
	s.mu.Unlock()
	return s.Store.LoadMeta(ctx, namespace, key)
}

func (s *captureLoadMetaStore) sawNamespace(namespace string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seen := range s.namespaces {
		if seen == namespace {
			return true
		}
	}
	return false
}

func TestTxnCommitNormalizesParticipantNamespaceForApply(t *testing.T) {
	store := &captureLoadMetaStore{Store: memory.New()}
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NoopLogger(),
		Clock:        newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   15 * time.Second,
		MaxTTL:       1 * time.Minute,
		AcquireBlock: 10 * time.Second,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	txnID := xid.New().String()
	status := doJSON(t, server, http.MethodPost, "/v1/txn/commit", nil, api.TxnDecisionRequest{
		TxnID: txnID,
		Participants: []api.TxnParticipant{
			{Namespace: "MiXeD_Ns", Key: "orders"},
		},
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("expected 200, got %d", status)
	}
	if !store.sawNamespace("mixed_ns") {
		t.Fatalf("expected apply path to read normalized namespace mixed_ns, got %+v", store.namespaces)
	}
	if store.sawNamespace("MiXeD_Ns") {
		t.Fatalf("expected apply path to avoid raw namespace MiXeD_Ns, got %+v", store.namespaces)
	}
}

func TestQueryEngineSelection(t *testing.T) {
	t.Run("scan", func(t *testing.T) {
		adapter := &stubSearchAdapter{}
		server := newQueryTestServer(t, adapter)
		defer server.Close()
		resp, err := http.Post(server.URL+"/v1/query?engine=scan", "application/json", strings.NewReader(`{"namespace":"default"}`))
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		if adapter.last.Engine != search.EngineScan {
			t.Fatalf("expected scan engine, got %s", adapter.last.Engine)
		}
	})

	t.Run("index unavailable", func(t *testing.T) {
		adapter := &stubSearchAdapter{}
		server := newQueryTestServer(t, adapter)
		defer server.Close()
		resp, err := http.Post(server.URL+"/v1/query?engine=index", "application/json", strings.NewReader(`{"namespace":"default"}`))
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", resp.StatusCode)
		}
		var errResp api.ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if errResp.ErrorCode != "query_engine_unavailable" {
			t.Fatalf("expected query_engine_unavailable, got %s", errResp.ErrorCode)
		}
	})

	t.Run("index available", func(t *testing.T) {
		adapter := &stubSearchAdapter{caps: search.Capabilities{Index: true}}
		server := newQueryTestServer(t, adapter)
		defer server.Close()
		cfgPayload := `{"namespace":"default","query":{"preferred_engine":"index","fallback_engine":"scan"}}`
		respCfg, err := http.Post(server.URL+"/v1/namespace", "application/json", strings.NewReader(cfgPayload))
		if err != nil {
			t.Fatalf("namespace config request failed: %v", err)
		}
		respCfg.Body.Close()
		if respCfg.StatusCode != http.StatusOK {
			t.Fatalf("expected namespace config 200, got %d", respCfg.StatusCode)
		}
		resp, err := http.Post(server.URL+"/v1/query?engine=index", "application/json", strings.NewReader(`{"namespace":"default"}`))
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		if adapter.last.Engine != search.EngineIndex {
			t.Fatalf("expected index engine, got %s", adapter.last.Engine)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		adapter := &stubSearchAdapter{}
		server := newQueryTestServer(t, adapter)
		defer server.Close()
		resp, err := http.Post(server.URL+"/v1/query?engine=bogus", "application/json", strings.NewReader(`{"namespace":"default"}`))
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", resp.StatusCode)
		}
		var errResp api.ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if errResp.ErrorCode != "invalid_engine" {
			t.Fatalf("expected invalid_engine, got %s", errResp.ErrorCode)
		}
	})
}

func TestQueryInvalidCursor(t *testing.T) {
	store := memory.New()
	adapter := &stubSearchAdapter{
		err: fmt.Errorf("%w: bad cursor", search.ErrInvalidCursor),
	}
	handler := New(Config{
		Store:                  store,
		Logger:                 pslog.NewStructured(context.Background(), io.Discard),
		Clock:                  newStubClock(time.Unix(1_700_000_000, 0)),
		SearchAdapter:          adapter,
		DefaultNamespaceConfig: scanNamespaceConfig(),
		JSONMaxBytes:           1 << 20,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Post(server.URL+"/v1/query", "application/json", bytes.NewReader([]byte(`{"namespace":"default","cursor":"opaque"}`)))
	if err != nil {
		t.Fatalf("query request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, data)
	}
	var errResp api.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if errResp.ErrorCode != "invalid_cursor" {
		t.Fatalf("expected invalid_cursor, got %s", errResp.ErrorCode)
	}
}

func TestQueryReturnDocuments(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	adapter := &stubSearchAdapter{
		result: search.Result{
			Keys:     []string{"stream-1", "stream-2"},
			Cursor:   "next-cursor",
			IndexSeq: 99,
			Metadata: map[string]string{"hint": "scan"},
		},
	}
	defaultCfg := scanNamespaceConfig()
	configStore := namespaces.NewConfigStore(store, nil, nil, defaultCfg)
	handler := New(Config{
		Store:                  store,
		Logger:                 pslog.NoopLogger(),
		Clock:                  newStubClock(time.Unix(1_700_000_500, 0)),
		SearchAdapter:          adapter,
		NamespaceConfigs:       configStore,
		DefaultNamespaceConfig: defaultCfg,
		JSONMaxBytes:           1 << 20,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()
	seed := func(key string, body string) {
		res, err := store.WriteState(ctx, namespaces.Default, key, strings.NewReader(body), storage.PutStateOptions{})
		if err != nil {
			t.Fatalf("write state %s: %v", key, err)
		}
		meta := &storage.Meta{
			Version:             1,
			PublishedVersion:    1,
			StateETag:           res.NewETag,
			StatePlaintextBytes: res.BytesWritten,
			UpdatedAtUnix:       time.Now().Unix(),
		}
		if _, err := store.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
			t.Fatalf("store meta %s: %v", key, err)
		}
	}
	seed("stream-1", `{"status":"ready","rank":1}`)
	seed("stream-2", `{"status":"waiting","rank":2}`)
	resp, err := http.Post(server.URL+"/v1/query?namespace=default&return=documents", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("query request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, data)
	}
	if ct := resp.Header.Get("Content-Type"); ct != contentTypeNDJSON {
		t.Fatalf("expected %s content type, got %s", contentTypeNDJSON, ct)
	}
	if mode := resp.Header.Get(headerQueryReturn); mode != string(api.QueryReturnDocuments) {
		t.Fatalf("expected return header documents, got %s", mode)
	}
	if cursor := resp.Header.Get(headerQueryCursor); cursor != "" {
		t.Fatalf("expected no cursor header in documents mode, got %s", cursor)
	}
	if seq := resp.Header.Get(headerQueryIndexSeq); seq != "" {
		t.Fatalf("expected no index seq header in documents mode, got %s", seq)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if cursor := resp.Trailer.Get(headerQueryCursor); cursor != "next-cursor" {
		t.Fatalf("expected cursor trailer, got %s", cursor)
	}
	if seq := resp.Trailer.Get(headerQueryIndexSeq); seq != "99" {
		t.Fatalf("expected index seq trailer, got %s", seq)
	}
	if metadata := resp.Trailer.Get(headerQueryMetadata); metadata == "" {
		t.Fatalf("expected metadata trailer")
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 rows, got %d (%q)", len(lines), lines)
	}
	var rows []map[string]any
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("decode row: %v", err)
		}
		rows = append(rows, row)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 decoded rows, got %d", len(rows))
	}
	if rows[0]["ns"].(string) != namespaces.Default || rows[0]["key"].(string) != "stream-1" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	firstDoc, ok := rows[0]["doc"].(map[string]any)
	if !ok || firstDoc["status"] != "ready" {
		t.Fatalf("unexpected first document: %+v", rows[0])
	}
	if rows[1]["key"].(string) != "stream-2" {
		t.Fatalf("unexpected second row: %+v", rows[1])
	}
	secondDoc, ok := rows[1]["doc"].(map[string]any)
	if !ok || secondDoc["status"] != "waiting" {
		t.Fatalf("unexpected second document: %+v", rows[1])
	}
	if adapter.calls != 1 {
		t.Fatalf("expected a single query execution for documents mode, got %d", adapter.calls)
	}
}

func TestQueryReturnDocumentsIncludesStreamCounters(t *testing.T) {
	store := memory.New()
	scanAdapter, err := scanadapter.New(scanadapter.Config{Backend: store, MaxDocumentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("new scan adapter: %v", err)
	}
	defaultCfg := scanNamespaceConfig()
	configStore := namespaces.NewConfigStore(store, nil, nil, defaultCfg)
	handler := New(Config{
		Store:                  store,
		Logger:                 pslog.NoopLogger(),
		Clock:                  newStubClock(time.Unix(1_700_000_000, 0)),
		SearchAdapter:          scanAdapter,
		NamespaceConfigs:       configStore,
		DefaultNamespaceConfig: defaultCfg,
		JSONMaxBytes:           1 << 20,
		DefaultTTL:             15 * time.Second,
		MaxTTL:                 1 * time.Minute,
		AcquireBlock:           10 * time.Second,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	seedPublished := func(key, status string) {
		var acquire api.AcquireResponse
		if statusCode := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, api.AcquireRequest{
			Key: key, Owner: "stream-counters", TTLSeconds: 10,
		}, &acquire); statusCode != http.StatusOK {
			t.Fatalf("acquire %s: expected 200, got %d", key, statusCode)
		}
		headers := map[string]string{
			"X-Lease-ID":      acquire.LeaseID,
			"X-Txn-ID":        acquire.TxnID,
			"X-Fencing-Token": strconv.FormatInt(acquire.FencingToken, 10),
		}
		if statusCode := doJSON(t, server, http.MethodPost, "/v1/update?key="+key, headers, map[string]any{
			"status": status,
		}, nil); statusCode != http.StatusOK {
			t.Fatalf("update %s: expected 200, got %d", key, statusCode)
		}
		var release api.ReleaseResponse
		if statusCode := doJSON(t, server, http.MethodPost, "/v1/release", headers, api.ReleaseRequest{
			Key: key, LeaseID: acquire.LeaseID, TxnID: acquire.TxnID,
		}, &release); statusCode != http.StatusOK || !release.Released {
			t.Fatalf("release %s failed: status=%d released=%v", key, statusCode, release.Released)
		}
	}
	seedPublished("stream-counter-open", "open")
	seedPublished("stream-counter-closed", "closed")

	body, err := json.Marshal(api.QueryRequest{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "/status", Value: "open"},
		},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("marshal query request: %v", err)
	}
	resp, err := http.Post(server.URL+"/v1/query?namespace=default&return=documents", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("query request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, data)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("drain body: %v", err)
	}
	trailerMetadata := strings.TrimSpace(resp.Trailer.Get(headerQueryMetadata))
	if trailerMetadata == "" {
		t.Fatalf("expected %s trailer", headerQueryMetadata)
	}
	var metadata map[string]string
	if err := json.Unmarshal([]byte(trailerMetadata), &metadata); err != nil {
		t.Fatalf("decode metadata trailer: %v", err)
	}
	if got := metadata[search.MetadataQueryCandidatesSeen]; got != "2" {
		t.Fatalf("expected %s=2, got %q", search.MetadataQueryCandidatesSeen, got)
	}
	if got := metadata[search.MetadataQueryCandidatesMatched]; got != "1" {
		t.Fatalf("expected %s=1, got %q", search.MetadataQueryCandidatesMatched, got)
	}
}

func TestGetPublicWithoutLease(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	logger := pslog.NewStructured(context.Background(), io.Discard)

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
		Owner:      "worker-public",
		TTLSeconds: 10,
	}
	var acquireResp api.AcquireResponse
	status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, acquireReq, &acquireResp)
	if status != http.StatusOK {
		t.Fatalf("expected acquire 200, got %d", status)
	}
	stateHeaders := map[string]string{
		"X-Lease-ID":      acquireResp.LeaseID,
		"X-Fencing-Token": strconv.FormatInt(acquireResp.FencingToken, 10),
		"X-Txn-ID":        acquireResp.TxnID,
	}
	updateBody := map[string]any{"cursor": 99}
	status = doJSON(t, server, http.MethodPost, "/v1/update?key=orders", stateHeaders, updateBody, nil)
	if status != http.StatusOK {
		t.Fatalf("expected update state 200, got %d", status)
	}

	releaseReq := api.ReleaseRequest{
		Namespace: acquireResp.Namespace,
		Key:       "orders",
		LeaseID:   acquireResp.LeaseID,
		TxnID:     acquireResp.TxnID,
	}
	status = doJSON(t, server, http.MethodPost, "/v1/release", map[string]string{"X-Fencing-Token": strconv.FormatInt(acquireResp.FencingToken, 10)}, releaseReq, nil)
	if status != http.StatusOK {
		t.Fatalf("expected release 200, got %d", status)
	}

	publicReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/get?key=orders&public=1", http.NoBody)
	publicResp, err := http.DefaultClient.Do(publicReq)
	if err != nil {
		t.Fatalf("public get request failed: %v", err)
	}
	defer publicResp.Body.Close()
	if publicResp.StatusCode != http.StatusOK {
		t.Fatalf("expected public get 200, got %d", publicResp.StatusCode)
	}
	var state map[string]any
	if err := json.NewDecoder(publicResp.Body).Decode(&state); err != nil {
		t.Fatalf("decode public state: %v", err)
	}
	if v := state["cursor"].(float64); v != 99 {
		t.Fatalf("expected cursor 99, got %v", v)
	}

	emptyReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/get?key=missing&public=1", http.NoBody)
	emptyResp, err := http.DefaultClient.Do(emptyReq)
	if err != nil {
		t.Fatalf("empty public get request failed: %v", err)
	}
	defer emptyResp.Body.Close()
	if emptyResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected missing public get 204, got %d", emptyResp.StatusCode)
	}
}

func TestAcquireAutoGeneratesKey(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	logger := pslog.NewStructured(context.Background(), io.Discard)

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
	metaRes, err := store.LoadMeta(ctx, handler.defaultNamespace, autoResp.Key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta := metaRes.Meta
	if meta.Lease == nil || meta.Lease.Owner != autoReq.Owner {
		t.Fatalf("unexpected lease stored: %+v", meta.Lease)
	}

	stateHeaders := map[string]string{
		"X-Lease-ID":      autoResp.LeaseID,
		"X-Fencing-Token": strconv.FormatInt(autoResp.FencingToken, 10),
		"X-Txn-ID":        autoResp.TxnID,
	}
	updateBody := map[string]any{"auto": true}
	status = doJSON(t, server, http.MethodPost, "/v1/update?key="+autoResp.Key, stateHeaders, updateBody, nil)
	if status != http.StatusOK {
		t.Fatalf("expected update state 200, got %d", status)
	}
	var releaseResp api.ReleaseResponse
	status = doJSON(t, server, http.MethodPost, "/v1/release", stateHeaders, api.ReleaseRequest{Key: autoResp.Key, LeaseID: autoResp.LeaseID, TxnID: autoResp.TxnID}, &releaseResp)
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
	logger := pslog.NoopLogger()
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
	logger := pslog.NoopLogger()
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

func TestAcquireIfNotExistsReturnsAlreadyExists(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NoopLogger(),
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
	var firstResp api.AcquireResponse
	if status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, first, &firstResp); status != http.StatusOK {
		t.Fatalf("expected first acquire 200, got %d", status)
	}

	var releaseResp api.ReleaseResponse
	releaseReq := api.ReleaseRequest{
		Key:     firstResp.Key,
		LeaseID: firstResp.LeaseID,
		TxnID:   firstResp.TxnID,
	}
	releaseHeaders := map[string]string{"X-Fencing-Token": strconv.FormatInt(firstResp.FencingToken, 10)}
	if status := doJSON(t, server, http.MethodPost, "/v1/release", releaseHeaders, releaseReq, &releaseResp); status != http.StatusOK {
		t.Fatalf("expected release 200, got %d", status)
	}

	conflict := api.AcquireRequest{
		Key:         "stream",
		Owner:       "worker-b",
		TTLSeconds:  5,
		BlockSecs:   5,
		IfNotExists: true,
	}
	var errResp api.ErrorResponse
	status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, conflict, &errResp)
	if status != http.StatusConflict {
		t.Fatalf("expected conflict 409, got %d", status)
	}
	if errResp.ErrorCode != "already_exists" {
		t.Fatalf("expected already_exists error code, got %s", errResp.ErrorCode)
	}
}

func TestUpdateVersionMismatch(t *testing.T) {
	store := memory.New()
	clk := newStubClock(time.Unix(1_700_000_000, 0))
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NoopLogger(),
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
	headers := map[string]string{"X-Lease-ID": acquire.LeaseID, "X-Fencing-Token": token, "X-Txn-ID": acquire.TxnID}
	doJSON(t, server, http.MethodPost, "/v1/update?key=stream", headers, map[string]int{"pos": 1}, nil)

	// Commit the staged update to bump the version.
	releaseReq := api.ReleaseRequest{Key: "stream", LeaseID: acquire.LeaseID, TxnID: acquire.TxnID}
	status := doJSON(t, server, http.MethodPost, "/v1/release", map[string]string{"X-Fencing-Token": token}, releaseReq, nil)
	if status != http.StatusOK {
		t.Fatalf("release: expected 200, got %d", status)
	}

	// Reacquire and attempt an update with a stale version to trigger conflict.
	var reacquire api.AcquireResponse
	doJSON(t, server, http.MethodPost, "/v1/acquire", nil, req, &reacquire)
	token = strconv.FormatInt(reacquire.FencingToken, 10)
	headers = map[string]string{"X-Lease-ID": reacquire.LeaseID, "X-Fencing-Token": token, "X-Txn-ID": reacquire.TxnID, "X-If-Version": "0"}
	var errResp api.ErrorResponse
	status = doJSON(t, server, http.MethodPost, "/v1/update?key=stream", headers, map[string]int{"pos": 2}, &errResp)
	if status != http.StatusConflict {
		t.Fatalf("expected 409 conflict, got %d", status)
	}
	if errResp.ErrorCode != "version_conflict" {
		t.Fatalf("expected version_conflict, got %s", errResp.ErrorCode)
	}
}

func TestUpdateExpectedPayloadHeaders(t *testing.T) {
	server := newTestHTTPServer(t)

	req := api.AcquireRequest{Key: "stream-expected", Owner: "worker-a", TTLSeconds: 5}
	var acquire api.AcquireResponse
	if status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, req, &acquire); status != http.StatusOK {
		t.Fatalf("acquire: expected 200, got %d", status)
	}

	payload := []byte("{\n  \"pos\": 1\n}\n")
	sum := sha256.Sum256(payload)
	expectedSHA := hex.EncodeToString(sum[:])

	updateReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/update?key=stream-expected", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	updateReq.Header.Set("Content-Type", "application/json")
	updateReq.Header.Set("X-Lease-ID", acquire.LeaseID)
	updateReq.Header.Set("X-Txn-ID", acquire.TxnID)
	updateReq.Header.Set("X-Fencing-Token", strconv.FormatInt(acquire.FencingToken, 10))
	updateReq.Header.Set(headerExpectedSHA256, expectedSHA)
	updateReq.Header.Set(headerExpectedBytes, strconv.FormatInt(int64(len(payload)), 10))

	updateResp, err := http.DefaultClient.Do(updateReq)
	if err != nil {
		t.Fatalf("do update request: %v", err)
	}
	defer updateResp.Body.Close()
	if updateResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(updateResp.Body)
		t.Fatalf("expected 200, got %d: %s", updateResp.StatusCode, strings.TrimSpace(string(body)))
	}
}

func TestUpdateExpectedPayloadHeaderErrors(t *testing.T) {
	server := newTestHTTPServer(t)

	req := api.AcquireRequest{Key: "stream-expected-errors", Owner: "worker-a", TTLSeconds: 5}
	var acquire api.AcquireResponse
	if status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, req, &acquire); status != http.StatusOK {
		t.Fatalf("acquire: expected 200, got %d", status)
	}

	payload := []byte(`{"pos":1}`)

	invalidBytesReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/update?key=stream-expected-errors", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	invalidBytesReq.Header.Set("Content-Type", "application/json")
	invalidBytesReq.Header.Set("X-Lease-ID", acquire.LeaseID)
	invalidBytesReq.Header.Set("X-Txn-ID", acquire.TxnID)
	invalidBytesReq.Header.Set("X-Fencing-Token", strconv.FormatInt(acquire.FencingToken, 10))
	invalidBytesReq.Header.Set(headerExpectedBytes, "abc")

	invalidBytesResp, err := http.DefaultClient.Do(invalidBytesReq)
	if err != nil {
		t.Fatalf("do invalid bytes request: %v", err)
	}
	defer invalidBytesResp.Body.Close()
	if invalidBytesResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid bytes header, got %d", invalidBytesResp.StatusCode)
	}
	var invalidBytesErr api.ErrorResponse
	if err := json.NewDecoder(invalidBytesResp.Body).Decode(&invalidBytesErr); err != nil {
		t.Fatalf("decode invalid bytes error: %v", err)
	}
	if invalidBytesErr.ErrorCode != "invalid_expected_bytes" {
		t.Fatalf("expected invalid_expected_bytes, got %s", invalidBytesErr.ErrorCode)
	}

	mismatchReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/update?key=stream-expected-errors", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new mismatch request: %v", err)
	}
	mismatchReq.Header.Set("Content-Type", "application/json")
	mismatchReq.Header.Set("X-Lease-ID", acquire.LeaseID)
	mismatchReq.Header.Set("X-Txn-ID", acquire.TxnID)
	mismatchReq.Header.Set("X-Fencing-Token", strconv.FormatInt(acquire.FencingToken, 10))
	mismatchReq.Header.Set(headerExpectedSHA256, strings.Repeat("0", 64))

	mismatchResp, err := http.DefaultClient.Do(mismatchReq)
	if err != nil {
		t.Fatalf("do mismatch request: %v", err)
	}
	defer mismatchResp.Body.Close()
	if mismatchResp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for sha mismatch, got %d", mismatchResp.StatusCode)
	}
	var mismatchErr api.ErrorResponse
	if err := json.NewDecoder(mismatchResp.Body).Decode(&mismatchErr); err != nil {
		t.Fatalf("decode mismatch error: %v", err)
	}
	if mismatchErr.ErrorCode != "expected_sha256_mismatch" {
		t.Fatalf("expected expected_sha256_mismatch, got %s", mismatchErr.ErrorCode)
	}
}

func TestMutateAppliesLQLExpressions(t *testing.T) {
	server := newTestHTTPServer(t)

	req := api.AcquireRequest{Key: "stream-mutate", Owner: "worker-a", TTLSeconds: 5}
	var acquire api.AcquireResponse
	if status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, req, &acquire); status != http.StatusOK {
		t.Fatalf("acquire: expected 200, got %d", status)
	}

	headers := map[string]string{
		"X-Lease-ID":      acquire.LeaseID,
		"X-Txn-ID":        acquire.TxnID,
		"X-Fencing-Token": strconv.FormatInt(acquire.FencingToken, 10),
	}
	var mutateResp api.UpdateResponse
	status := doJSON(t, server, http.MethodPost, "/v1/mutate?key=stream-mutate", headers, api.MutateRequest{
		Mutations: []string{`/status="ready"`, `/count=1`},
	}, &mutateResp)
	if status != http.StatusOK {
		t.Fatalf("mutate: expected 200, got %d", status)
	}
	if mutateResp.NewVersion == 0 || mutateResp.NewStateETag == "" {
		t.Fatalf("unexpected mutate response: %+v", mutateResp)
	}

	var release api.ReleaseResponse
	if status := doJSON(t, server, http.MethodPost, "/v1/release", headers, api.ReleaseRequest{
		Key:     "stream-mutate",
		LeaseID: acquire.LeaseID,
		TxnID:   acquire.TxnID,
	}, &release); status != http.StatusOK || !release.Released {
		t.Fatalf("release failed: status=%d resp=%+v", status, release)
	}

	getReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/get?key=stream-mutate&public=1", http.NoBody)
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("public get request failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected public get 200, got %d", getResp.StatusCode)
	}
	var state map[string]any
	if err := json.NewDecoder(getResp.Body).Decode(&state); err != nil {
		t.Fatalf("decode mutated state: %v", err)
	}
	if got := state["status"]; got != "ready" {
		t.Fatalf("expected status=ready, got %v", got)
	}
	if got := state["count"]; got != float64(1) {
		t.Fatalf("expected count=1, got %v", got)
	}
}

func TestMutateIncludesStreamSummaryHeaders(t *testing.T) {
	server := newTestHTTPServer(t)

	req := api.AcquireRequest{Key: "stream-mutate-summary", Owner: "worker-a", TTLSeconds: 5}
	var acquire api.AcquireResponse
	if status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, req, &acquire); status != http.StatusOK {
		t.Fatalf("acquire: expected 200, got %d", status)
	}

	payload, err := json.Marshal(api.MutateRequest{
		Mutations: []string{`/status="ready"`, `/count=1`},
	})
	if err != nil {
		t.Fatalf("marshal mutate payload: %v", err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/mutate?key=stream-mutate-summary", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new mutate request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Lease-ID", acquire.LeaseID)
	httpReq.Header.Set("X-Txn-ID", acquire.TxnID)
	httpReq.Header.Set("X-Fencing-Token", strconv.FormatInt(acquire.FencingToken, 10))
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("mutate request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("mutate: expected 200, got %d: %s", resp.StatusCode, data)
	}
	var mutateResp api.UpdateResponse
	if err := json.NewDecoder(resp.Body).Decode(&mutateResp); err != nil {
		t.Fatalf("decode mutate response: %v", err)
	}
	if mutateResp.NewVersion == 0 || mutateResp.NewStateETag == "" {
		t.Fatalf("unexpected mutate response: %+v", mutateResp)
	}

	if got := parseIntHeader(t, resp.Header, headerMutateCandidatesSeen); got != 1 {
		t.Fatalf("expected %s=1, got %d", headerMutateCandidatesSeen, got)
	}
	if got := parseIntHeader(t, resp.Header, headerMutateCandidatesWritten); got != 1 {
		t.Fatalf("expected %s=1, got %d", headerMutateCandidatesWritten, got)
	}
	if got := parseIntHeader(t, resp.Header, headerMutateBytesRead); got <= 0 {
		t.Fatalf("expected %s > 0, got %d", headerMutateBytesRead, got)
	}
	if got := parseIntHeader(t, resp.Header, headerMutateBytesWritten); got <= 0 {
		t.Fatalf("expected %s > 0, got %d", headerMutateBytesWritten, got)
	}
	if got := parseIntHeader(t, resp.Header, headerMutateBytesCaptured); got != 0 {
		t.Fatalf("expected %s=0, got %d", headerMutateBytesCaptured, got)
	}
	if got := parseIntHeader(t, resp.Header, headerMutateSpillCount); got != 0 {
		t.Fatalf("expected %s=0, got %d", headerMutateSpillCount, got)
	}
	if got := parseIntHeader(t, resp.Header, headerMutateSpillBytes); got != 0 {
		t.Fatalf("expected %s=0, got %d", headerMutateSpillBytes, got)
	}
}

func TestMutateValidation(t *testing.T) {
	server := newTestHTTPServer(t)

	req := api.AcquireRequest{Key: "stream-mutate-errors", Owner: "worker-a", TTLSeconds: 5}
	var acquire api.AcquireResponse
	if status := doJSON(t, server, http.MethodPost, "/v1/acquire", nil, req, &acquire); status != http.StatusOK {
		t.Fatalf("acquire: expected 200, got %d", status)
	}

	headers := map[string]string{
		"X-Lease-ID":      acquire.LeaseID,
		"X-Txn-ID":        acquire.TxnID,
		"X-Fencing-Token": strconv.FormatInt(acquire.FencingToken, 10),
	}

	var errResp api.ErrorResponse
	status := doJSON(t, server, http.MethodPost, "/v1/mutate?key=stream-mutate-errors", headers, api.MutateRequest{}, &errResp)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty mutations, got %d", status)
	}
	if errResp.ErrorCode != "invalid_body" {
		t.Fatalf("expected invalid_body, got %s", errResp.ErrorCode)
	}

	errResp = api.ErrorResponse{}
	status = doJSON(t, server, http.MethodPost, "/v1/mutate?key=stream-mutate-errors", headers, api.MutateRequest{
		Mutations: []string{`/broken`},
	}, &errResp)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid mutation expression, got %d", status)
	}
	if errResp.ErrorCode != "invalid_mutations" {
		t.Fatalf("expected invalid_mutations, got %s", errResp.ErrorCode)
	}
}

func TestGetRequiresLease(t *testing.T) {
	store := memory.New()
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NoopLogger(),
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
	headers := map[string]string{"X-Lease-ID": acquire.LeaseID, "X-Fencing-Token": token, "X-Txn-ID": acquire.TxnID}
	doJSON(t, server, http.MethodPost, "/v1/update?key=alpha", headers, map[string]int{"pos": 1}, nil)

	getReq, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/get?key=alpha", http.NoBody)
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 missing lease, got %d", resp.StatusCode)
	}
}

func TestMetadataUpdateTogglesQueryHidden(t *testing.T) {
	store := memory.New()
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NoopLogger(),
		Clock:        newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   10 * time.Second,
		MaxTTL:       time.Minute,
		AcquireBlock: 2 * time.Second,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	acquireReq := api.AcquireRequest{Key: "orders", Owner: "worker", TTLSeconds: 15}
	var acquireResp api.AcquireResponse
	doJSON(t, server, http.MethodPost, "/v1/acquire", nil, acquireReq, &acquireResp)
	token := strconv.FormatInt(acquireResp.FencingToken, 10)

	headers := map[string]string{
		"X-Lease-ID":      acquireResp.LeaseID,
		"X-Fencing-Token": token,
		"X-Txn-ID":        acquireResp.TxnID,
	}
	doJSON(t, server, http.MethodPost, "/v1/update?key=orders", headers, map[string]any{"cursor": 1}, nil)

	var metaResp api.MetadataUpdateResponse
	status := doJSON(t, server, http.MethodPost, "/v1/metadata?key=orders", headers, map[string]any{"query_hidden": true}, &metaResp)
	if status != http.StatusOK {
		t.Fatalf("expected metadata update 200, got %d", status)
	}
	if metaResp.Metadata.QueryHidden == nil || !*metaResp.Metadata.QueryHidden {
		t.Fatalf("expected query_hidden metadata, got %+v", metaResp.Metadata)
	}

	// Commit and reacquire before clearing.
	releaseReq := api.ReleaseRequest{Key: "orders", LeaseID: acquireResp.LeaseID, TxnID: acquireResp.TxnID}
	status = doJSON(t, server, http.MethodPost, "/v1/release", map[string]string{"X-Fencing-Token": token}, releaseReq, nil)
	if status != http.StatusOK {
		t.Fatalf("release commit failed: %d", status)
	}
	metaRes, err := store.LoadMeta(context.Background(), namespaces.Default, "orders")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta := metaRes.Meta
	if !meta.QueryExcluded() {
		t.Fatalf("expected meta to be query excluded")
	}

	var reacquire api.AcquireResponse
	doJSON(t, server, http.MethodPost, "/v1/acquire", nil, acquireReq, &reacquire)
	token = strconv.FormatInt(reacquire.FencingToken, 10)

	clearHeaders := map[string]string{
		"X-Lease-ID":              reacquire.LeaseID,
		"X-Fencing-Token":         token,
		"X-Txn-ID":                reacquire.TxnID,
		headerMetadataQueryHidden: "false",
	}
	metaResp = api.MetadataUpdateResponse{}
	status = doJSON(t, server, http.MethodPost, "/v1/metadata?key=orders", clearHeaders, nil, &metaResp)
	if status != http.StatusOK {
		t.Fatalf("expected metadata clear 200, got %d", status)
	}
	if metaResp.Metadata.QueryHidden != nil {
		t.Fatalf("expected metadata to omit query_hidden, got %+v", metaResp.Metadata)
	}
	releaseReq = api.ReleaseRequest{Key: "orders", LeaseID: reacquire.LeaseID, TxnID: reacquire.TxnID}
	status = doJSON(t, server, http.MethodPost, "/v1/release", map[string]string{"X-Fencing-Token": token}, releaseReq, nil)
	if status != http.StatusOK {
		t.Fatalf("release clear failed: %d", status)
	}
	metaRes, err = store.LoadMeta(context.Background(), namespaces.Default, "orders")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta = metaRes.Meta
	if meta.QueryExcluded() {
		t.Fatalf("expected query exclusion cleared")
	}
}

func TestMetadataUpdateValidations(t *testing.T) {
	store := memory.New()
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NoopLogger(),
		Clock:        newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   15 * time.Second,
		MaxTTL:       time.Minute,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	acquireReq := api.AcquireRequest{Key: "alpha", Owner: "worker", TTLSeconds: 10}
	var acquireResp api.AcquireResponse
	doJSON(t, server, http.MethodPost, "/v1/acquire", nil, acquireReq, &acquireResp)
	token := strconv.FormatInt(acquireResp.FencingToken, 10)
	headers := map[string]string{
		"X-Lease-ID":      acquireResp.LeaseID,
		"X-Fencing-Token": token,
		"X-Txn-ID":        acquireResp.TxnID,
	}
	// Seed metadata via update so version is >0.
	doJSON(t, server, http.MethodPost, "/v1/update?key=alpha", headers, map[string]int{"cursor": 5}, nil)

	t.Run("invalid header value", func(t *testing.T) {
		badHeaders := map[string]string{
			"X-Lease-ID":              acquireResp.LeaseID,
			"X-Fencing-Token":         token,
			"X-Txn-ID":                acquireResp.TxnID,
			headerMetadataQueryHidden: "definitely-not-bool",
		}
		var errResp api.ErrorResponse
		status := doJSON(t, server, http.MethodPost, "/v1/metadata?key=alpha", badHeaders, nil, &errResp)
		if status != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", status)
		}
		if errResp.ErrorCode != "invalid_metadata" {
			t.Fatalf("expected invalid_metadata, got %s", errResp.ErrorCode)
		}
	})

	t.Run("missing fields", func(t *testing.T) {
		var errResp api.ErrorResponse
		status := doJSON(t, server, http.MethodPost, "/v1/metadata?key=alpha", headers, nil, &errResp)
		if status != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", status)
		}
		if errResp.ErrorCode != "invalid_metadata" {
			t.Fatalf("expected invalid_metadata, got %s", errResp.ErrorCode)
		}
	})

	t.Run("version conflict", func(t *testing.T) {
		conflictHeaders := map[string]string{
			"X-Lease-ID":      acquireResp.LeaseID,
			"X-Fencing-Token": token,
			"X-Txn-ID":        acquireResp.TxnID,
			"X-If-Version":    "999",
		}
		var errResp api.ErrorResponse
		status := doJSON(t, server, http.MethodPost, "/v1/metadata?key=alpha", conflictHeaders, map[string]any{"query_hidden": true}, &errResp)
		if status != http.StatusConflict {
			t.Fatalf("expected 409, got %d", status)
		}
		if errResp.ErrorCode != "version_conflict" {
			t.Fatalf("expected version_conflict, got %s", errResp.ErrorCode)
		}
	})
}

func TestQueueStateAcquireDefaultsHidden(t *testing.T) {
	store := memory.New()
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NoopLogger(),
		Clock:        newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   10 * time.Second,
		MaxTTL:       time.Minute,
	})
	stateKey, err := queue.StateLeaseKey(namespaces.Default, "workflow", "msg-1")
	if err != nil {
		t.Fatalf("state key: %v", err)
	}
	ctx := context.Background()
	outcome, err := handler.acquireLeaseForKey(ctx, handler.logger, acquireParams{
		Namespace: namespaces.Default,
		Key:       stateKey,
		Owner:     "worker-a",
		TTL:       5 * time.Second,
	})
	if err != nil {
		t.Fatalf("acquire state lease: %v", err)
	}
	defer handler.releaseLeaseOutcome(ctx, stateKey, outcome)
	relState := relativeKey(namespaces.Default, stateKey)
	metaRes, err := store.LoadMeta(ctx, namespaces.Default, relState)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta := metaRes.Meta
	if meta == nil || !meta.QueryExcluded() {
		t.Fatalf("expected queue state meta to be query hidden, got %+v", meta)
	}
	if val, ok := meta.GetAttribute(storage.MetaAttributeQueryExclude); !ok || val != "true" {
		t.Fatalf("expected query_hidden attribute=true, got %q ok=%v", val, ok)
	}
}

func TestQueueStateAcquireRespectsManualVisibility(t *testing.T) {
	store := memory.New()
	handler := New(Config{
		Store:        store,
		Logger:       pslog.NoopLogger(),
		Clock:        newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   10 * time.Second,
		MaxTTL:       time.Minute,
	})
	stateKey, err := queue.StateLeaseKey(namespaces.Default, "workflow", "msg-2")
	if err != nil {
		t.Fatalf("state key: %v", err)
	}
	ctx := context.Background()
	acquire := func(owner string) {
		outcome, err := handler.acquireLeaseForKey(ctx, handler.logger, acquireParams{
			Namespace: namespaces.Default,
			Key:       stateKey,
			Owner:     owner,
			TTL:       5 * time.Second,
		})
		if err != nil {
			t.Fatalf("acquire (%s): %v", owner, err)
		}
		defer handler.releaseLeaseOutcome(ctx, stateKey, outcome)
	}
	acquire("worker-init")
	relState := relativeKey(namespaces.Default, stateKey)
	metaRes, err := store.LoadMeta(ctx, namespaces.Default, relState)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta := metaRes.Meta
	etag := metaRes.ETag
	meta.SetQueryHidden(false)
	if _, err := store.StoreMeta(ctx, namespaces.Default, relState, meta, etag); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	acquire("worker-visible")
	metaRes, err = store.LoadMeta(ctx, namespaces.Default, relState)
	if err != nil {
		t.Fatalf("reload meta: %v", err)
	}
	meta = metaRes.Meta
	if meta.QueryExcluded() {
		val, _ := meta.GetAttribute(storage.MetaAttributeQueryExclude)
		t.Fatalf("expected manual opt-in to persist, got attribute=%q", val)
	}
	if val, ok := meta.GetAttribute(storage.MetaAttributeQueryExclude); !ok || val != "false" {
		t.Fatalf("expected stored attribute=false, got %q ok=%v", val, ok)
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

func parseIntHeader(t *testing.T, headers http.Header, key string) int64 {
	t.Helper()
	raw := strings.TrimSpace(headers.Get(key))
	if raw == "" {
		t.Fatalf("missing response header %q", key)
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse header %s=%q: %v", key, raw, err)
	}
	return parsed
}

func newQueryTestServer(t *testing.T, adapter search.Adapter) *httptest.Server {
	t.Helper()
	store := memory.New()
	defaultCfg := scanNamespaceConfig()
	configStore := namespaces.NewConfigStore(store, nil, nil, defaultCfg)
	handler := New(Config{
		Store:                  store,
		Logger:                 pslog.NewStructured(context.Background(), io.Discard),
		Clock:                  newStubClock(time.Unix(1_700_000_000, 0)),
		SearchAdapter:          adapter,
		NamespaceConfigs:       configStore,
		DefaultNamespaceConfig: defaultCfg,
		JSONMaxBytes:           1 << 20,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	return httptest.NewServer(mux)
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
	for range 6 {
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

type stubSearchAdapter struct {
	last   search.Request
	result search.Result
	err    error
	caps   search.Capabilities
	calls  int
}

func (s *stubSearchAdapter) Capabilities(context.Context, string) (search.Capabilities, error) {
	caps := s.caps
	if !caps.Index && !caps.Scan {
		caps.Scan = true
	}
	return caps, nil
}

func (s *stubSearchAdapter) Query(ctx context.Context, req search.Request) (search.Result, error) {
	s.calls++
	s.last = req
	if s.err != nil {
		return search.Result{}, s.err
	}
	return s.result, nil
}
