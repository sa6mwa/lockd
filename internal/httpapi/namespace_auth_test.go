package httpapi

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	out, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url %q: %v", raw, err)
	}
	return out
}

func mustClaimURI(t *testing.T, namespace string, permission nsauth.Permission) *url.URL {
	t.Helper()
	claim, err := nsauth.ClaimURI(namespace, permission)
	if err != nil {
		t.Fatalf("claim uri %s: %v", namespace, err)
	}
	return claim
}

func certWithURIs(uris ...*url.URL) *x509.Certificate {
	return &x509.Certificate{URIs: uris}
}

func requestWithCert(method, target string, body string, cert *x509.Certificate) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if cert != nil {
		req.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}
	}
	return req
}

func requireHTTPErrorCode(t *testing.T, err error, code string) httpError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error code %q, got nil", code)
	}
	var httpErr httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected httpError, got %T", err)
	}
	if httpErr.Code != code {
		t.Fatalf("expected error code %q, got %q (detail=%q)", code, httpErr.Code, httpErr.Detail)
	}
	return httpErr
}

func newNamespaceAuthHandler(t *testing.T) *Handler {
	t.Helper()
	store := memory.New()
	cfgStore := namespaces.NewConfigStore(store, nil, nil, namespaces.DefaultConfig())
	handler := New(Config{
		Store:             store,
		NamespaceConfigs:  cfgStore,
		Logger:            pslog.NoopLogger(),
		Clock:             newStubClock(time.Unix(1_700_000_000, 0)),
		JSONMaxBytes:      1 << 20,
		DefaultTTL:        15 * time.Second,
		MaxTTL:            1 * time.Minute,
		AcquireBlock:      10 * time.Second,
		QueueMaxConsumers: 2,
	})
	handler.enforceClientIdentity = true
	return handler
}

func TestNamespaceAuthDenyNoClaims(t *testing.T) {
	h := newNamespaceAuthHandler(t)
	cert := certWithURIs(mustParseURL(t, "spiffe://lockd/sdk/test-client"))

	rec := httptest.NewRecorder()
	req := requestWithCert(http.MethodPost, "/v1/acquire", `{"key":"orders","owner":"worker","ttl_seconds":5}`, cert)
	err := h.handleAcquire(rec, req)
	httpErr := requireHTTPErrorCode(t, err, "namespace_forbidden")
	if !strings.Contains(strings.ToLower(httpErr.Detail), "no namespace claims") {
		t.Fatalf("expected no-claims detail, got %q", httpErr.Detail)
	}
}

func TestNamespaceAuthWriteDeniedForReadEndpoints(t *testing.T) {
	h := newNamespaceAuthHandler(t)
	cert := certWithURIs(
		mustParseURL(t, "spiffe://lockd/sdk/test-client"),
		mustClaimURI(t, "default", nsauth.PermissionWrite),
	)

	rec := httptest.NewRecorder()
	req := requestWithCert(http.MethodPost, "/v1/get?key=orders&public=1", "", cert)
	err := h.handleGet(rec, req)
	requireHTTPErrorCode(t, err, "namespace_forbidden")

	rec = httptest.NewRecorder()
	req = requestWithCert(http.MethodPost, "/v1/queue/dequeue", `{"namespace":"default","queue":"jobs","owner":"worker","wait_seconds":0}`, cert)
	err = h.handleQueueDequeue(rec, req)
	requireHTTPErrorCode(t, err, "namespace_forbidden")
}

func TestNamespaceAuthWriteAllowedForNonPublicGet(t *testing.T) {
	h := newNamespaceAuthHandler(t)
	cert := certWithURIs(
		mustParseURL(t, "spiffe://lockd/sdk/test-client"),
		mustClaimURI(t, "default", nsauth.PermissionWrite),
	)

	rec := httptest.NewRecorder()
	req := requestWithCert(http.MethodPost, "/v1/get?key=orders", "", cert)
	req.Header.Set("X-Lease-ID", xid.New().String())
	req.Header.Set("X-Fencing-Token", "1")
	err := h.handleGet(rec, req)
	if err != nil {
		var httpErr httpError
		if errors.As(err, &httpErr) && httpErr.Code == "namespace_forbidden" {
			t.Fatalf("expected non-public get to pass namespace auth for write claim: %+v", httpErr)
		}
	}
}

func TestNamespaceAuthReadDeniedForWriteEndpoints(t *testing.T) {
	h := newNamespaceAuthHandler(t)
	cert := certWithURIs(
		mustParseURL(t, "spiffe://lockd/sdk/test-client"),
		mustClaimURI(t, "default", nsauth.PermissionRead),
	)

	rec := httptest.NewRecorder()
	req := requestWithCert(http.MethodPost, "/v1/update?key=orders", `{}`, cert)
	req.Header.Set("X-Lease-ID", xid.New().String())
	req.Header.Set("X-Txn-ID", xid.New().String())
	req.Header.Set("X-Fencing-Token", "1")
	err := h.handleUpdate(rec, req)
	requireHTTPErrorCode(t, err, "namespace_forbidden")
}

func TestNamespaceAuthNamespaceConfigRequiresRW(t *testing.T) {
	h := newNamespaceAuthHandler(t)
	readOnly := certWithURIs(
		mustParseURL(t, "spiffe://lockd/sdk/test-client"),
		mustClaimURI(t, "default", nsauth.PermissionRead),
	)

	rec := httptest.NewRecorder()
	req := requestWithCert(http.MethodGet, "/v1/namespace?namespace=default", "", readOnly)
	err := h.handleNamespaceConfigGet(rec, req)
	requireHTTPErrorCode(t, err, "namespace_forbidden")

	readWrite := certWithURIs(
		mustParseURL(t, "spiffe://lockd/sdk/test-client"),
		mustClaimURI(t, "default", nsauth.PermissionReadWrite),
	)
	rec = httptest.NewRecorder()
	req = requestWithCert(http.MethodGet, "/v1/namespace?namespace=default", "", readWrite)
	err = h.handleNamespaceConfigGet(rec, req)
	if err != nil {
		t.Fatalf("expected namespace get with rw claim to pass, got %v", err)
	}
	var out api.NamespaceConfigResponse
	if decodeErr := json.Unmarshal(rec.Body.Bytes(), &out); decodeErr != nil {
		t.Fatalf("decode namespace response: %v", decodeErr)
	}
	if out.Namespace != "default" {
		t.Fatalf("expected namespace default, got %q", out.Namespace)
	}
}

func TestNamespaceAuthTxnReplayRequiresAllRW(t *testing.T) {
	h := newNamespaceAuthHandler(t)
	txnID := xid.New().String()
	defaultOnly := certWithURIs(
		mustParseURL(t, "spiffe://lockd/tc/test-client"),
		mustClaimURI(t, "default", nsauth.PermissionReadWrite),
	)

	rec := httptest.NewRecorder()
	req := requestWithCert(http.MethodPost, "/v1/txn/replay", `{"txn_id":"`+txnID+`"}`, defaultOnly)
	err := h.handleTxnReplay(rec, req)
	requireHTTPErrorCode(t, err, "namespace_forbidden")

	allRW := certWithURIs(
		mustParseURL(t, "spiffe://lockd/tc/test-client"),
		mustClaimURI(t, "ALL", nsauth.PermissionReadWrite),
	)
	rec = httptest.NewRecorder()
	req = requestWithCert(http.MethodPost, "/v1/txn/replay", `{"txn_id":"`+txnID+`"}`, allRW)
	err = h.handleTxnReplay(rec, req)
	if err == nil {
		return
	}
	var httpErr httpError
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected httpError, got %T", err)
	}
	if httpErr.Code == "namespace_forbidden" {
		t.Fatalf("expected txn replay with ALL rw to pass namespace auth, got %+v", httpErr)
	}
}

func TestNamespaceAuthRejectsMalformedNamespaceClaim(t *testing.T) {
	h := newNamespaceAuthHandler(t)
	cert := certWithURIs(
		mustParseURL(t, "spiffe://lockd/sdk/test-client"),
		mustParseURL(t, "lockd://ns/default?perm=r&perm=rw"),
	)

	rec := httptest.NewRecorder()
	req := requestWithCert(http.MethodPost, "/v1/acquire", `{"key":"orders","owner":"worker","ttl_seconds":5}`, cert)
	err := h.handleAcquire(rec, req)
	requireHTTPErrorCode(t, err, "invalid_namespace_claims")
}

func TestNamespaceAuthRejectsMissingSPIFFEWithClaims(t *testing.T) {
	h := newNamespaceAuthHandler(t)
	cert := certWithURIs(
		mustClaimURI(t, "default", nsauth.PermissionReadWrite),
	)

	rec := httptest.NewRecorder()
	req := requestWithCert(http.MethodPost, "/v1/acquire", `{"key":"orders","owner":"worker","ttl_seconds":5}`, cert)
	err := h.handleAcquire(rec, req)
	httpErr := requireHTTPErrorCode(t, err, "namespace_forbidden")
	if !strings.Contains(strings.ToLower(httpErr.Detail), "spiffe") {
		t.Fatalf("expected SPIFFE detail, got %q", httpErr.Detail)
	}
}

func TestNamespaceAuthIgnoresHostSpoofClaims(t *testing.T) {
	h := newNamespaceAuthHandler(t)
	cert := certWithURIs(
		mustParseURL(t, "spiffe://lockd/sdk/test-client"),
		mustParseURL(t, "lockd://evil/default?perm=rw"),
	)

	rec := httptest.NewRecorder()
	req := requestWithCert(http.MethodPost, "/v1/acquire", `{"key":"orders","owner":"worker","ttl_seconds":5}`, cert)
	err := h.handleAcquire(rec, req)
	httpErr := requireHTTPErrorCode(t, err, "namespace_forbidden")
	if !strings.Contains(strings.ToLower(httpErr.Detail), "no namespace claims") {
		t.Fatalf("expected no-claims detail, got %q", httpErr.Detail)
	}
}
