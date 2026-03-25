package cryptotest

import (
	"net/http"
	"testing"

	"pkt.systems/lockd"
)

func TestRequireServerHTTPClientReusesPerServerClient(t *testing.T) {
	ts := lockd.StartTestServer(t, lockd.WithTestMTLS())

	first := RequireServerHTTPClient(t, ts)
	second := RequireServerHTTPClient(t, ts)
	if first == nil || second == nil {
		t.Fatalf("expected cached server http clients")
	}
	if first != second {
		t.Fatalf("expected same cached server http client, got %p and %p", first, second)
	}
}

func TestRequireTCHTTPClientReusesPerServerClient(t *testing.T) {
	ts := lockd.StartTestServer(t, lockd.WithTestMTLS())

	first := RequireTCHTTPClient(t, ts)
	second := RequireTCHTTPClient(t, ts)
	if first == nil || second == nil {
		t.Fatalf("expected cached tc http clients")
	}
	if first != second {
		t.Fatalf("expected same cached tc http client, got %p and %p", first, second)
	}
}

func TestRequireHTTPClientsDoNotCrossServerBoundaries(t *testing.T) {
	firstTS := lockd.StartTestServer(t, lockd.WithTestMTLS())
	secondTS := lockd.StartTestServer(t, lockd.WithTestMTLS())

	firstServerClient := RequireServerHTTPClient(t, firstTS)
	secondServerClient := RequireServerHTTPClient(t, secondTS)
	assertDistinctClient(t, "server", firstServerClient, secondServerClient)

	firstTCClient := RequireTCHTTPClient(t, firstTS)
	secondTCClient := RequireTCHTTPClient(t, secondTS)
	assertDistinctClient(t, "tc", firstTCClient, secondTCClient)
}

func assertDistinctClient(t *testing.T, kind string, first, second *http.Client) {
	t.Helper()
	if first == nil || second == nil {
		t.Fatalf("expected %s http clients", kind)
	}
	if first == second {
		t.Fatalf("expected different %s http clients across servers, got shared %p", kind, first)
	}
}
