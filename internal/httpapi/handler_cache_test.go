package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	port "pkt.systems/logport"

	"pkt.systems/lockd/internal/api"
	"pkt.systems/lockd/internal/storage"
)

func TestHandlerLeaseCacheAvoidsExtraLoadMeta(t *testing.T) {
	store := newStubStore()
	h := New(Config{
		Store:        store,
		Logger:       port.NoopLogger(),
		JSONMaxBytes: 1 << 20,
		DefaultTTL:   30 * time.Second,
		MaxTTL:       time.Minute,
		AcquireBlock: 5 * time.Second,
	})

	acquireReq := bytes.NewBufferString(`{"key":"orders","owner":"worker-1","ttl_seconds":30,"block_seconds":0}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/acquire", acquireReq)
	if err := h.handleAcquire(rr, req); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("acquire status=%d body=%s", rr.Code, rr.Body.Bytes())
	}
	var acq api.AcquireResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &acq); err != nil {
		t.Fatalf("decode acquire: %v", err)
	}
	fence := strconv.FormatInt(acq.FencingToken, 10)

	loadCalls := store.resetCounters()
	if loadCalls != 1 {
		t.Fatalf("expected 1 initial load, got %d", loadCalls)
	}

	updateBody := bytes.NewBufferString("  { \"value\" : 42 }\n")
	upReq := httptest.NewRequest(http.MethodPost, "/v1/update-state?key=orders", updateBody)
	upReq.Header.Set("X-Lease-ID", acq.LeaseID)
	upReq.Header.Set("X-Fencing-Token", fence)
	rr = httptest.NewRecorder()
	if err := h.handleUpdateState(rr, upReq); err != nil {
		t.Fatalf("update: %v", err)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", rr.Code, rr.Body.Bytes())
	}
	if store.loadMetaCount != 0 {
		t.Fatalf("expected no LoadMeta during update, got %d", store.loadMetaCount)
	}
	if store.writeStateCount != 1 {
		t.Fatalf("expected write state once, got %d", store.writeStateCount)
	}

	store.resetCounters()
	keepReq := httptest.NewRequest(http.MethodPost, "/v1/keepalive", bytes.NewBufferString(`{"key":"orders","lease_id":"`+acq.LeaseID+`","ttl_seconds":45}`))
	keepReq.Header.Set("X-Fencing-Token", fence)
	rr = httptest.NewRecorder()
	if err := h.handleKeepAlive(rr, keepReq); err != nil {
		t.Fatalf("keepalive: %v", err)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("keepalive status=%d body=%s", rr.Code, rr.Body.Bytes())
	}
	if store.loadMetaCount != 0 {
		t.Fatalf("expected keepalive to use cache, got %d loads", store.loadMetaCount)
	}

	store.resetCounters()
	relReq := httptest.NewRequest(http.MethodPost, "/v1/release", bytes.NewBufferString(`{"key":"orders","lease_id":"`+acq.LeaseID+`"}`))
	relReq.Header.Set("X-Fencing-Token", fence)
	rr = httptest.NewRecorder()
	if err := h.handleRelease(rr, relReq); err != nil {
		t.Fatalf("release: %v", err)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("release status=%d body=%s", rr.Code, rr.Body.Bytes())
	}
	if _, _, _, ok := h.leaseSnapshot(acq.LeaseID); ok {
		t.Fatalf("expected lease cache to be cleared after release")
	}
	if store.loadMetaCount != 0 {
		t.Fatalf("expected release to avoid LoadMeta, got %d", store.loadMetaCount)
	}
}

type stubStore struct {
	mu              sync.Mutex
	meta            map[string]stubEntry
	loadMetaCount   int
	storeMetaCount  int
	writeStateCount int
	nextETag        int
	lastState       []byte
}

type stubEntry struct {
	meta storage.Meta
	etag string
}

func newStubStore() *stubStore {
	return &stubStore{
		meta: make(map[string]stubEntry),
	}
}

func (s *stubStore) resetCounters() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.loadMetaCount
	s.loadMetaCount = 0
	s.storeMetaCount = 0
	s.writeStateCount = 0
	return prev
}

func (s *stubStore) nextMetaETag() string {
	s.nextETag++
	return "meta-etag-" + strconv.Itoa(s.nextETag)
}

func (s *stubStore) LoadMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadMetaCount++
	if entry, ok := s.meta[key]; ok {
		metaCopy := entry.meta
		return &metaCopy, entry.etag, nil
	}
	return nil, "", storage.ErrNotFound
}

func (s *stubStore) StoreMeta(ctx context.Context, key string, meta *storage.Meta, expectedETag string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storeMetaCount++
	entry, exists := s.meta[key]
	if exists && expectedETag != "" && expectedETag != entry.etag {
		return "", storage.ErrCASMismatch
	}
	newETag := s.nextMetaETag()
	metaCopy := *meta
	s.meta[key] = stubEntry{meta: metaCopy, etag: newETag}
	return newETag, nil
}

func (s *stubStore) DeleteMeta(ctx context.Context, key string, expectedETag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.meta, key)
	return nil
}

func (s *stubStore) ListMetaKeys(ctx context.Context) ([]string, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubStore) ReadState(ctx context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	return nil, nil, storage.ErrNotFound
}

func (s *stubStore) WriteState(ctx context.Context, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeStateCount++
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	s.lastState = data
	s.nextETag++
	return &storage.PutStateResult{
		BytesWritten: int64(len(data)),
		NewETag:      "state-etag-" + strconv.Itoa(s.nextETag),
	}, nil
}

func (s *stubStore) RemoveState(ctx context.Context, key string, expectedETag string) error {
	return nil
}

func (s *stubStore) Close() error {
	return nil
}
