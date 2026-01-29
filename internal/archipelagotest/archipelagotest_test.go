package archipelagotest

import "testing"

func TestPreferredEndpointForBackend(t *testing.T) {
	expected := map[string][]string{
		"hash-a": {"https://b.example/", "https://a.example/"},
	}
	if got := preferredEndpointForBackend(expected, "hash-a"); got != "https://b.example" {
		t.Fatalf("expected first endpoint to be preferred, got %q", got)
	}
	if got := preferredEndpointForBackend(expected, "missing"); got != "" {
		t.Fatalf("expected empty result for unknown backend, got %q", got)
	}
}
