package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pkt.systems/pslog"
)

func TestBootstrapConfigDir(t *testing.T) {
	dir := t.TempDir()
	logger := pslog.NewStructured(io.Discard)

	if err := bootstrapConfigDir(dir, logger); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}

	required := []string{"ca.pem", "server.pem", "client.pem", "tc-client.pem", "config.yaml", "server.denylist"}
	for _, name := range required {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}

	cfgBytes, err := os.ReadFile(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(string(cfgBytes), "store: "+bootstrapStoreDefault) {
		t.Fatalf("config missing store override: %s", cfgBytes)
	}
	if !strings.Contains(string(cfgBytes), "tc-client-bundle: "+filepath.Join(dir, "tc-client.pem")) {
		t.Fatalf("config missing tc-client-bundle override: %s", cfgBytes)
	}

	if err := bootstrapConfigDir(dir, logger); err != nil {
		t.Fatalf("bootstrap idempotency failed: %v", err)
	}
}
