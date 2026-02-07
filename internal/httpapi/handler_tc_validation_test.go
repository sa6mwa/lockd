package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/internal/tcrm"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func newTCValidationHandler() *Handler {
	store := memory.New()
	return &Handler{
		defaultNamespace: namespaces.Default,
		tcCluster:        tccluster.NewStore(store, pslog.NoopLogger(), clock.Real{}),
		tcRM:             tcrm.NewStore(store, pslog.NoopLogger()),
		jsonMaxBytes:     1 << 20,
	}
}

func TestHandleTCClusterAnnounceRejectsInvalidSelfEndpoint(t *testing.T) {
	h := newTCValidationHandler()
	req := httptest.NewRequest(http.MethodPost, "/v1/tc/cluster/announce", strings.NewReader(`{"self_endpoint":"https://example.com?x=1"}`))
	rec := httptest.NewRecorder()

	err := h.handleTCClusterAnnounce(rec, req)
	if err == nil {
		t.Fatal("expected invalid self endpoint error")
	}
	var httpErr httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected httpError, got %T", err)
	}
	if httpErr.Status != http.StatusBadRequest || httpErr.Code != "invalid_self_endpoint" {
		t.Fatalf("unexpected error: %+v", httpErr)
	}
}

func TestHandleTCRMRegisterRejectsInvalidEndpoint(t *testing.T) {
	h := newTCValidationHandler()
	req := httptest.NewRequest(http.MethodPost, "/v1/tc/rm/register", strings.NewReader(`{"backend_hash":"hash-1","endpoint":"https://rm-1?x=1"}`))
	rec := httptest.NewRecorder()

	err := h.handleTCRMRegister(rec, req)
	if err == nil {
		t.Fatal("expected invalid endpoint error")
	}
	var httpErr httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected httpError, got %T", err)
	}
	if httpErr.Status != http.StatusBadRequest || httpErr.Code != "invalid_endpoint" {
		t.Fatalf("unexpected error: %+v", httpErr)
	}
}

func TestHandleTCRMUnregisterRejectsInvalidEndpoint(t *testing.T) {
	h := newTCValidationHandler()
	req := httptest.NewRequest(http.MethodPost, "/v1/tc/rm/unregister", strings.NewReader(`{"backend_hash":"hash-1","endpoint":"https://rm-1?x=1"}`))
	rec := httptest.NewRecorder()

	err := h.handleTCRMUnregister(rec, req)
	if err == nil {
		t.Fatal("expected invalid endpoint error")
	}
	var httpErr httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected httpError, got %T", err)
	}
	if httpErr.Status != http.StatusBadRequest || httpErr.Code != "invalid_endpoint" {
		t.Fatalf("unexpected error: %+v", httpErr)
	}
}
