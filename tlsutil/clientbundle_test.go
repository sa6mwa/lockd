package tlsutil

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadClientBundle(t *testing.T) {
	ca, err := GenerateCA("test-ca", 24*time.Hour)
	if err != nil {
		t.Fatalf("generate ca: %v", err)
	}
	issued, err := ca.IssueClient(ClientCertRequest{CommonName: "test-client", Validity: 12 * time.Hour})
	if err != nil {
		t.Fatalf("issue client: %v", err)
	}
	clientBundle, err := EncodeClientBundle(ca.CertPEM, issued.CertPEM, issued.KeyPEM)
	if err != nil {
		t.Fatalf("encode client bundle: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "client.pem")
	if err := os.WriteFile(path, clientBundle, 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}

	parsed, err := LoadClientBundle(path)
	if err != nil {
		t.Fatalf("load client bundle: %v", err)
	}
	if parsed.ClientCert == nil {
		t.Fatal("expected client certificate")
	}
	if parsed.CAPool == nil || len(parsed.CAPool.Subjects()) == 0 {
		t.Fatal("expected CA pool populated")
	}
	if len(parsed.CACerts) != 1 {
		t.Fatalf("expected 1 CA, got %d", len(parsed.CACerts))
	}
	if parsed.Certificate.Leaf == nil {
		leaf, err := FirstCertificateFromPEM(parsed.ClientCertPEM)
		if err != nil {
			t.Fatalf("load leaf: %v", err)
		}
		parsed.Certificate.Leaf = leaf
	}
}
