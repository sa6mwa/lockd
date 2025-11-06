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

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/storage"
)

func TestHandlerLeaseCacheAvoidsExtraLoadMeta(t *testing.T) {
	store := newStubStore()
	h := New(Config{
		Store:                   store,
		Logger:                  loggingutil.NoopLogger(),
		JSONMaxBytes:            1 << 20,
		DefaultTTL:              30 * time.Second,
		MaxTTL:                  time.Minute,
		AcquireBlock:            5 * time.Second,
		MetaWarmupAttempts:      0,
		StateWarmupAttempts:     0,
		MetaWarmupInitialDelay:  0,
		StateWarmupInitialDelay: 0,
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
	upReq := httptest.NewRequest(http.MethodPost, "/v1/update?key=orders", updateBody)
	upReq.Header.Set("X-Lease-ID", acq.LeaseID)
	upReq.Header.Set("X-Fencing-Token", fence)
	rr = httptest.NewRecorder()
	if err := h.handleUpdate(rr, upReq); err != nil {
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

func TestHandlerRemoveClearsMeta(t *testing.T) {
	store := newStubStore()
	h := New(Config{
		Store:                   store,
		Logger:                  loggingutil.NoopLogger(),
		JSONMaxBytes:            1 << 20,
		DefaultTTL:              30 * time.Second,
		MaxTTL:                  time.Minute,
		AcquireBlock:            5 * time.Second,
		MetaWarmupAttempts:      0,
		StateWarmupAttempts:     0,
		MetaWarmupInitialDelay:  0,
		StateWarmupInitialDelay: 0,
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
	nsKey := h.defaultNamespace + "/orders"

	updateBody := bytes.NewBufferString(`{"value":42}`)
	upReq := httptest.NewRequest(http.MethodPost, "/v1/update?key=orders", updateBody)
	upReq.Header.Set("X-Lease-ID", acq.LeaseID)
	upReq.Header.Set("X-Fencing-Token", fence)
	rr = httptest.NewRecorder()
	if err := h.handleUpdate(rr, upReq); err != nil {
		t.Fatalf("update: %v", err)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", rr.Code, rr.Body.Bytes())
	}

	store.mu.Lock()
	entry, ok := store.meta[nsKey]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("expected meta entry for %s", nsKey)
	}
	if entry.meta.StateETag == "" {
		t.Fatalf("expected state etag after update")
	}
	prevVersion := entry.meta.Version

	rmReq := httptest.NewRequest(http.MethodPost, "/v1/remove?key=orders", nil)
	rmReq.Header.Set("X-Lease-ID", acq.LeaseID)
	rmReq.Header.Set("X-Fencing-Token", fence)
	rmReq.Header.Set("X-If-State-ETag", entry.meta.StateETag)
	rr = httptest.NewRecorder()
	if err := h.handleRemove(rr, rmReq); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("remove status=%d body=%s", rr.Code, rr.Body.Bytes())
	}
	var resp api.RemoveResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode remove: %v", err)
	}
	if !resp.Removed {
		t.Fatalf("expected removal success, got %+v", resp)
	}
	if resp.NewVersion != prevVersion+1 {
		t.Fatalf("expected version %d, got %d", prevVersion+1, resp.NewVersion)
	}

	store.mu.Lock()
	entry = store.meta[nsKey]
	_, statePresent := store.state[nsKey]
	removeCount := store.removeStateCount
	store.mu.Unlock()

	if entry.meta.StateETag != "" {
		t.Fatalf("expected state etag cleared, got %q", entry.meta.StateETag)
	}
	if entry.meta.Version != resp.NewVersion {
		t.Fatalf("meta version mismatch, want %d got %d", resp.NewVersion, entry.meta.Version)
	}
	if statePresent {
		t.Fatalf("expected state to be removed from store")
	}
	if removeCount != 1 {
		t.Fatalf("expected Remove to be invoked once, got %d", removeCount)
	}
}

type stubStore struct {
	mu               sync.Mutex
	meta             map[string]stubEntry
	loadMetaCount    int
	storeMetaCount   int
	writeStateCount  int
	removeStateCount int
	nextETag         int
	lastState        []byte
	state            map[string]string
}

type stubEntry struct {
	meta storage.Meta
	etag string
}

func newStubStore() *stubStore {
	return &stubStore{
		meta:  make(map[string]stubEntry),
		state: make(map[string]string),
	}
}

func stubNamespaced(namespace, key string) string {
	if strings.TrimSpace(namespace) == "" {
		return key
	}
	return namespace + "/" + strings.TrimPrefix(key, "/")
}

func (s *stubStore) resetCounters() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := s.loadMetaCount
	s.loadMetaCount = 0
	s.storeMetaCount = 0
	s.writeStateCount = 0
	s.removeStateCount = 0
	return prev
}

func (s *stubStore) nextMetaETag() string {
	s.nextETag++
	return "meta-etag-" + strconv.Itoa(s.nextETag)
}

func (s *stubStore) LoadMeta(ctx context.Context, namespace, key string) (*storage.Meta, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadMetaCount++
	fullKey := stubNamespaced(namespace, key)
	if entry, ok := s.meta[fullKey]; ok {
		metaCopy := entry.meta
		return &metaCopy, entry.etag, nil
	}
	return nil, "", storage.ErrNotFound
}

func (s *stubStore) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storeMetaCount++
	fullKey := stubNamespaced(namespace, key)
	entry, exists := s.meta[fullKey]
	if expectedETag != "" {
		if !exists {
			return "", storage.ErrNotFound
		}
		if expectedETag != entry.etag {
			return "", storage.ErrCASMismatch
		}
	} else if exists {
		return "", storage.ErrCASMismatch
	}
	newETag := s.nextMetaETag()
	metaCopy := *meta
	s.meta[fullKey] = stubEntry{meta: metaCopy, etag: newETag}
	return newETag, nil
}

func (s *stubStore) DeleteMeta(ctx context.Context, namespace, key string, expectedETag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fullKey := stubNamespaced(namespace, key)
	delete(s.meta, fullKey)
	return nil
}

func (s *stubStore) ListMetaKeys(ctx context.Context, namespace string) ([]string, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubStore) ReadState(ctx context.Context, namespace, key string) (io.ReadCloser, *storage.StateInfo, error) {
	return nil, nil, storage.ErrNotFound
}

func (s *stubStore) WriteState(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeStateCount++
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	s.lastState = data
	s.nextETag++
	result := &storage.PutStateResult{
		BytesWritten: int64(len(data)),
		NewETag:      "state-etag-" + strconv.Itoa(s.nextETag),
	}
	fullKey := stubNamespaced(namespace, key)
	s.state[fullKey] = result.NewETag
	return result, nil
}

func (s *stubStore) Remove(ctx context.Context, namespace, key string, expectedETag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeStateCount++
	fullKey := stubNamespaced(namespace, key)
	current, ok := s.state[fullKey]
	if !ok {
		return storage.ErrNotFound
	}
	if expectedETag != "" && expectedETag != current {
		return storage.ErrCASMismatch
	}
	delete(s.state, fullKey)
	return nil
}

func (s *stubStore) ListObjects(context.Context, string, storage.ListOptions) (*storage.ListResult, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubStore) GetObject(context.Context, string, string) (io.ReadCloser, *storage.ObjectInfo, error) {
	return nil, nil, storage.ErrNotImplemented
}

func (s *stubStore) PutObject(context.Context, string, string, io.Reader, storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubStore) DeleteObject(context.Context, string, string, storage.DeleteObjectOptions) error {
	return storage.ErrNotImplemented
}

func (s *stubStore) Close() error {
	return nil
}
