package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pkt.systems/pslog"
)

func TestAcquireRejectsReservedNamespace(t *testing.T) {
	h := New(Config{
		Store:                   newStubStore(),
		Logger:                  pslog.NoopLogger(),
		JSONMaxBytes:            1 << 20,
		DefaultTTL:              30 * time.Second,
		MaxTTL:                  time.Minute,
		AcquireBlock:            5 * time.Second,
		MetaWarmupAttempts:      0,
		StateWarmupAttempts:     0,
		MetaWarmupInitialDelay:  0,
		StateWarmupInitialDelay: 0,
	})

	body := bytes.NewBufferString(`{"namespace":".txns","key":"orders","owner":"worker","ttl_seconds":30}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/acquire", body)
	rr := httptest.NewRecorder()
	err := h.handleAcquire(rr, req)
	if err == nil {
		t.Fatalf("expected handler error for reserved namespace")
	}
	if httpErr, ok := err.(httpError); ok {
		if httpErr.Status != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", httpErr.Status)
		}
	} else {
		t.Fatalf("expected httpError, got %T: %v", err, err)
	}
}
