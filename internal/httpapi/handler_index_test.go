package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/namespaces"
)

type stubIndexControl struct {
	pending     bool
	flushErr    error
	manifestSeq uint64
	waitErr     error
	flushCount  int
	flushCh     chan struct{}
}

func (s *stubIndexControl) Pending(string) bool { return s.pending }

func (s *stubIndexControl) FlushNamespace(ctx context.Context, namespace string) error {
	s.flushCount++
	if s.flushCh != nil {
		select {
		case s.flushCh <- struct{}{}:
		default:
		}
	}
	return s.flushErr
}

func (s *stubIndexControl) ManifestSeq(ctx context.Context, namespace string) (uint64, error) {
	return s.manifestSeq, nil
}

func (s *stubIndexControl) WaitForReadable(ctx context.Context, namespace string) error {
	return s.waitErr
}

func (s *stubIndexControl) WarmNamespace(ctx context.Context, namespace string) error {
	return nil
}

func TestHandleIndexFlushWaitMode(t *testing.T) {
	ctrl := &stubIndexControl{manifestSeq: 42}
	h := &Handler{defaultNamespace: namespaces.Default, indexControl: ctrl}

	req := httptest.NewRequest(http.MethodPost, "/v1/index/flush?namespace=testing", strings.NewReader(`{"mode":"wait"}`))
	rec := httptest.NewRecorder()
	if err := h.handleIndexFlush(rec, req); err != nil {
		t.Fatalf("handleIndexFlush: %v", err)
	}
	if ctrl.flushCount != 1 {
		t.Fatalf("expected one flush, got %d", ctrl.flushCount)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d", rec.Code)
	}
	var resp api.IndexFlushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Flushed || resp.Mode != "wait" || resp.IndexSeq != 42 {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestHandleIndexFlushAsyncMode(t *testing.T) {
	flushCh := make(chan struct{}, 1)
	ctrl := &stubIndexControl{pending: true, flushCh: flushCh}
	h := &Handler{defaultNamespace: namespaces.Default, indexControl: ctrl}

	req := httptest.NewRequest(http.MethodPost, "/v1/index/flush", strings.NewReader(`{"mode":"async"}`))
	rec := httptest.NewRecorder()
	if err := h.handleIndexFlush(rec, req); err != nil {
		t.Fatalf("handleIndexFlush: %v", err)
	}
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	select {
	case <-flushCh:
	case <-time.After(time.Second):
		t.Fatalf("async flush did not trigger")
	}
	var resp api.IndexFlushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.FlushID == "" || resp.Pending != true || resp.Flushed {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestHandleIndexFlushInvalidMode(t *testing.T) {
	ctrl := &stubIndexControl{}
	h := &Handler{defaultNamespace: namespaces.Default, indexControl: ctrl}
	req := httptest.NewRequest(http.MethodPost, "/v1/index/flush", strings.NewReader(`{"mode":"bogus"}`))
	rec := httptest.NewRecorder()
	if err := h.handleIndexFlush(rec, req); err == nil {
		t.Fatalf("expected error for invalid mode")
	}
}
