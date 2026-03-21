package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	lockd "pkt.systems/lockd"
	"pkt.systems/lockd/tlsutil"
)

func TestPrepareEmbeddedBenchServerBundleGeneratesValidBundle(t *testing.T) {
	t.Parallel()

	cfg := benchConfig{enableCrypto: true}
	if err := prepareEmbeddedBenchServerBundle(&cfg); err != nil {
		t.Fatalf("prepareEmbeddedBenchServerBundle: %v", err)
	}
	if len(cfg.serverBundlePEM) == 0 {
		t.Fatal("expected generated server bundle")
	}
	if _, err := tlsutil.LoadBundleFromBytes(cfg.serverBundlePEM); err != nil {
		t.Fatalf("LoadBundleFromBytes: %v", err)
	}
	lockdCfg := lockd.Config{
		Store:       "mem://bench",
		ListenProto: "unix",
		Listen:      filepath.Join(t.TempDir(), "lockd.sock"),
		DisableMTLS: true,
		BundlePEM:   cfg.serverBundlePEM,
	}
	if err := lockdCfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestPrepareEmbeddedBenchServerBundleSkippedWhenCryptoDisabled(t *testing.T) {
	t.Parallel()

	cfg := benchConfig{enableCrypto: false}
	if err := prepareEmbeddedBenchServerBundle(&cfg); err != nil {
		t.Fatalf("prepareEmbeddedBenchServerBundle: %v", err)
	}
	if len(cfg.serverBundlePEM) != 0 {
		t.Fatal("expected no generated bundle when crypto is disabled")
	}
}

func TestResolveBenchClientBundlePathRequiresExplicitBundle(t *testing.T) {
	t.Parallel()

	_, err := resolveBenchClientBundlePath(benchConfig{})
	if err == nil {
		t.Fatal("expected missing bundle error")
	}
	if !strings.Contains(err.Error(), "-bundle is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveBenchClientBundlePathExpandsEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "client.pem")
	if err := os.WriteFile(path, []byte("test"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv("LOCKD_BENCH_TEST_BUNDLE", path)
	got, err := resolveBenchClientBundlePath(benchConfig{clientBundle: "$LOCKD_BENCH_TEST_BUNDLE"})
	if err != nil {
		t.Fatalf("resolveBenchClientBundlePath: %v", err)
	}
	if got != path {
		t.Fatalf("bundle path=%q want=%q", got, path)
	}
}
