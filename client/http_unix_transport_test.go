package client

import (
	"net/http"
	"testing"
)

func TestNewUnixHTTPClientAppliesDefaultTransportTuning(t *testing.T) {
	httpClient, base, err := newUnixHTTPClient("unix:///tmp/lockd.sock")
	if err != nil {
		t.Fatalf("newUnixHTTPClient error: %v", err)
	}
	if got, want := base, "http://unix"; got != want {
		t.Fatalf("base mismatch: got %q want %q", got, want)
	}
	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok || transport == nil {
		t.Fatalf("transport type = %T, want *http.Transport", httpClient.Transport)
	}
	if transport.MaxIdleConns < DefaultMaxIdleConns {
		t.Fatalf("MaxIdleConns = %d, want >= %d", transport.MaxIdleConns, DefaultMaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost < DefaultMaxIdleConnsPerHost {
		t.Fatalf("MaxIdleConnsPerHost = %d, want >= %d", transport.MaxIdleConnsPerHost, DefaultMaxIdleConnsPerHost)
	}
}
