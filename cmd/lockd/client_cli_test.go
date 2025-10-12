package main

import "testing"

func TestNormalizeServerURL(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		mtls   bool
		expect string
	}{
		{name: "mtls default host", raw: "lockd.local", mtls: true, expect: "https://lockd.local"},
		{name: "mtls host port", raw: "lockd.local:9443", mtls: true, expect: "https://lockd.local:9443"},
		{name: "mtls explicit https", raw: "https://lockd.local:7777", mtls: true, expect: "https://lockd.local:7777"},
		{name: "insecure default host", raw: "lockd.local", mtls: false, expect: "http://lockd.local"},
		{name: "insecure host port", raw: "lockd.local:8080", mtls: false, expect: "http://lockd.local:8080"},
		{name: "insecure explicit https honored", raw: "https://lockd.local:8443", mtls: false, expect: "https://lockd.local:8443"},
		{name: "explicit http honored", raw: "http://lockd.local:8080", mtls: true, expect: "http://lockd.local:8080"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeServerURL(tt.raw, tt.mtls)
			if err != nil {
				t.Fatalf("normalizeServerURL returned error: %v", err)
			}
			if got != tt.expect {
				t.Fatalf("normalizeServerURL(%q, %v) = %q, want %q", tt.raw, tt.mtls, got, tt.expect)
			}
		})
	}
}
