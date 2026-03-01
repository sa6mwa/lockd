package lockd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveClientBundlePathWithHintMissing(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LOCKD_CONFIG_DIR", dir)

	_, err := ResolveClientBundlePathWithHint(ClientBundleRoleSDK, "", "--client-bundle")
	if err == nil {
		t.Fatalf("expected missing bundle error")
	}
	if !strings.Contains(err.Error(), "provide --client-bundle") {
		t.Fatalf("expected custom hint in error, got %v", err)
	}
}

func TestResolveClientBundlePathWithHintMultiple(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LOCKD_CONFIG_DIR", dir)

	caBundle, err := CreateCABundle(CreateCABundleRequest{CommonName: "test-ca"})
	if err != nil {
		t.Fatalf("create ca bundle: %v", err)
	}
	clientA, err := CreateClientBundle(CreateClientBundleRequest{CABundlePEM: caBundle, CommonName: "client-a"})
	if err != nil {
		t.Fatalf("create client bundle a: %v", err)
	}
	clientB, err := CreateClientBundle(CreateClientBundleRequest{CABundlePEM: caBundle, CommonName: "client-b"})
	if err != nil {
		t.Fatalf("create client bundle b: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client-a.pem"), clientA, 0o600); err != nil {
		t.Fatalf("write client-a bundle: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "client-b.pem"), clientB, 0o600); err != nil {
		t.Fatalf("write client-b bundle: %v", err)
	}

	_, err = ResolveClientBundlePathWithHint(ClientBundleRoleSDK, "", "--client-bundle")
	if err == nil {
		t.Fatalf("expected multiple bundles error")
	}
	if !strings.Contains(err.Error(), "specify --client-bundle") {
		t.Fatalf("expected custom hint in error, got %v", err)
	}
}
