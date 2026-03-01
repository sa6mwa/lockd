package main

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

func TestBootstrapConfigDir(t *testing.T) {
	dir := t.TempDir()
	logger := pslog.NewStructured(context.Background(), io.Discard)

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

	serverBundle, err := tlsutil.LoadBundle(filepath.Join(dir, "server.pem"), "")
	if err != nil {
		t.Fatalf("load server bundle: %v", err)
	}
	if serverBundle.ServerCert == nil {
		t.Fatalf("expected server certificate in bootstrap bundle")
	}
	expectedAllClaim, err := url.Parse("lockd://ns/ALL?perm=rw")
	if err != nil {
		t.Fatalf("parse all claim: %v", err)
	}
	if !containsURI(serverBundle.ServerCert.URIs, expectedAllClaim) {
		t.Fatalf("expected server bundle to include claim %s", expectedAllClaim.String())
	}

	if err := bootstrapConfigDir(dir, logger); err != nil {
		t.Fatalf("bootstrap idempotency failed: %v", err)
	}
}
