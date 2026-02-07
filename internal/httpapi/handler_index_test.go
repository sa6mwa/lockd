package httpapi

import (
	"context"
	"encoding/json"
	"errors"
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
	flushBlock  <-chan struct{}
	flushDone   chan struct{}
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
	if s.flushBlock != nil {
		select {
		case <-s.flushBlock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.flushDone != nil {
		select {
		case s.flushDone <- struct{}{}:
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

func TestHandleIndexFlushAsyncDedupesNamespace(t *testing.T) {
	block := make(chan struct{})
	flushCh := make(chan struct{}, 1)
	ctrl := &stubIndexControl{
		pending:    true,
		flushCh:    flushCh,
		flushBlock: block,
	}
	h := &Handler{
		defaultNamespace:     namespaces.Default,
		indexControl:         ctrl,
		indexFlushInFlight:   make(map[string]string),
		indexFlushAsyncLimit: 8,
	}

	req1 := httptest.NewRequest(http.MethodPost, "/v1/index/flush", strings.NewReader(`{"mode":"async","namespace":"testing"}`))
	rec1 := httptest.NewRecorder()
	if err := h.handleIndexFlush(rec1, req1); err != nil {
		t.Fatalf("first async flush: %v", err)
	}
	select {
	case <-flushCh:
	case <-time.After(time.Second):
		t.Fatal("first async flush did not start")
	}

	req2 := httptest.NewRequest(http.MethodPost, "/v1/index/flush", strings.NewReader(`{"mode":"async","namespace":"testing"}`))
	rec2 := httptest.NewRecorder()
	if err := h.handleIndexFlush(rec2, req2); err != nil {
		t.Fatalf("second async flush: %v", err)
	}

	close(block)

	if ctrl.flushCount != 1 {
		t.Fatalf("expected deduped async flush count 1, got %d", ctrl.flushCount)
	}
	if rec1.Code != http.StatusAccepted || rec2.Code != http.StatusAccepted {
		t.Fatalf("expected both async responses to be 202, got %d and %d", rec1.Code, rec2.Code)
	}
	var firstResp api.IndexFlushResponse
	if err := json.Unmarshal(rec1.Body.Bytes(), &firstResp); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	var secondResp api.IndexFlushResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &secondResp); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if firstResp.FlushID == "" || secondResp.FlushID == "" {
		t.Fatalf("expected flush IDs, got first=%q second=%q", firstResp.FlushID, secondResp.FlushID)
	}
	if firstResp.FlushID != secondResp.FlushID {
		t.Fatalf("expected deduped flush id, got first=%q second=%q", firstResp.FlushID, secondResp.FlushID)
	}
}

func TestHandleIndexFlushAsyncLimitAndRelease(t *testing.T) {
	block := make(chan struct{})
	flushDone := make(chan struct{}, 2)
	ctrl := &stubIndexControl{
		pending:    true,
		flushBlock: block,
		flushDone:  flushDone,
	}
	h := &Handler{
		defaultNamespace:     namespaces.Default,
		indexControl:         ctrl,
		indexFlushInFlight:   make(map[string]string),
		indexFlushAsyncLimit: 1,
	}

	req1 := httptest.NewRequest(http.MethodPost, "/v1/index/flush", strings.NewReader(`{"mode":"async","namespace":"ns-a"}`))
	rec1 := httptest.NewRecorder()
	if err := h.handleIndexFlush(rec1, req1); err != nil {
		t.Fatalf("first async flush: %v", err)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/v1/index/flush", strings.NewReader(`{"mode":"async","namespace":"ns-b"}`))
	rec2 := httptest.NewRecorder()
	err := h.handleIndexFlush(rec2, req2)
	if err == nil {
		t.Fatal("expected async limit error")
	}
	var httpErr httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected httpError, got %T", err)
	}
	if httpErr.Status != http.StatusConflict || httpErr.Code != "index_flush_busy" {
		t.Fatalf("unexpected error: %+v", httpErr)
	}

	close(block)
	select {
	case <-flushDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first flush to finish")
	}

	req3 := httptest.NewRequest(http.MethodPost, "/v1/index/flush", strings.NewReader(`{"mode":"async","namespace":"ns-b"}`))
	rec3 := httptest.NewRecorder()
	if err := h.handleIndexFlush(rec3, req3); err != nil {
		t.Fatalf("third async flush after release: %v", err)
	}
	if rec3.Code != http.StatusAccepted {
		t.Fatalf("expected accepted response, got %d", rec3.Code)
	}
}

func TestHandleIndexFlushRejectsOversizedBody(t *testing.T) {
	ctrl := &stubIndexControl{}
	h := &Handler{
		defaultNamespace: namespaces.Default,
		indexControl:     ctrl,
	}
	oversizedNamespace := strings.Repeat("n", indexFlushBodyLimit+1)
	body := `{"mode":"wait","namespace":"` + oversizedNamespace + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/index/flush", strings.NewReader(body))
	rec := httptest.NewRecorder()

	err := h.handleIndexFlush(rec, req)
	if err == nil {
		t.Fatal("expected invalid_body error")
	}
	var httpErr httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected httpError, got %T", err)
	}
	if httpErr.Status != http.StatusBadRequest || httpErr.Code != "invalid_body" {
		t.Fatalf("unexpected error: %+v", httpErr)
	}
}
