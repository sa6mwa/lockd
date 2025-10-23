package main

import (
	"reflect"
	"testing"

	lockdclient "pkt.systems/lockd/client"
)

func TestParseServerEndpoints(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		mtls   bool
		expect []string
	}{
		{name: "mtls default host", raw: "lockd.local", mtls: true, expect: []string{"https://lockd.local:9341"}},
		{name: "mtls host port", raw: "lockd.local:9443", mtls: true, expect: []string{"https://lockd.local:9443"}},
		{name: "explicit http honored", raw: "http://lockd.local:8080", mtls: true, expect: []string{"http://lockd.local:8080"}},
		{name: "insecure default host", raw: "lockd.local", mtls: false, expect: []string{"http://lockd.local:9341"}},
		{name: "multi endpoints", raw: "localhost,localhost:9342,http://localhost:9343", mtls: true, expect: []string{"https://localhost:9341", "https://localhost:9342", "http://localhost:9343"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lockdclient.ParseEndpoints(tt.raw, tt.mtls)
			if err != nil {
				t.Fatalf("ParseEndpoints returned error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.expect) {
				t.Fatalf("ParseEndpoints(%q, %v) = %v, want %v", tt.raw, tt.mtls, got, tt.expect)
			}
		})
	}
}

func TestParseBlockSeconds(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		expect  int64
		wantErr bool
	}{
		{name: "empty", input: "", expect: lockdclient.BlockWaitForever},
		{name: "duration", input: "45s", expect: 45},
		{name: "integer seconds", input: "90", expect: 90},
		{name: "nowait", input: "nowait", expect: lockdclient.BlockNoWait},
		{name: "minus one", input: "-1", expect: lockdclient.BlockNoWait},
		{name: "forever", input: "forever", expect: lockdclient.BlockWaitForever},
		{name: "zero", input: "0", expect: lockdclient.BlockWaitForever},
		{name: "invalid", input: "banana", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBlockSeconds(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expect {
				t.Fatalf("parseBlockSeconds(%q)=%d, want %d", tt.input, got, tt.expect)
			}
		})
	}
}

func TestResolveKeyArgOrEnv(t *testing.T) {
	t.Run("flag wins", func(t *testing.T) {
		key, err := resolveKeyInput("orders", keyRequireExisting)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key != "orders" {
			t.Fatalf("key=%q, want %q", key, "orders")
		}
	})
	t.Run("env fallback", func(t *testing.T) {
		t.Setenv(envKey, "auto-key")
		key, err := resolveKeyInput("", keyRequireExisting)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key != "auto-key" {
			t.Fatalf("key=%q, want %q", key, "auto-key")
		}
	})
	t.Run("error when missing", func(t *testing.T) {
		t.Setenv(envKey, "")
		if _, err := resolveKeyInput("", keyRequireExisting); err == nil {
			t.Fatalf("expected error")
		}
	})
	t.Run("allow autogen returns empty", func(t *testing.T) {
		t.Setenv(envKey, "ignored")
		key, err := resolveKeyInput("", keyAllowAutogen)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key != "" {
			t.Fatalf("key=%q, want empty", key)
		}
	})
}
