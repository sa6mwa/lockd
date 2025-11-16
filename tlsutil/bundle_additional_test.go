package tlsutil

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/internal/cryptoutil"
)

func TestNormalizeSerials(t *testing.T) {
	input := []string{" 0A ", "0a", "B", "b", "", "  "}
	got := NormalizeSerials(input)
	want := []string{"0a", "b"}
	if !slices.Equal(got, want) {
		t.Fatalf("NormalizeSerials mismatch: got %v want %v", got, want)
	}
}

func TestLoadDenylistIgnoresComments(t *testing.T) {
	file := filepath.Join(t.TempDir(), "deny.txt")
	content := "# heading\n 0A \n \n0b\n# trailing\n"
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatalf("write denylist: %v", err)
	}
	deny, err := loadDenylist(file)
	if err != nil {
		t.Fatalf("load denylist: %v", err)
	}
	if len(deny) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(deny))
	}
	for _, serial := range []string{"0a", "0b"} {
		if _, ok := deny[serial]; !ok {
			t.Fatalf("missing serial %s", serial)
		}
	}
}

func TestLoadBundleMissingServerKey(t *testing.T) {
	ca, err := GenerateCA("test-ca", time.Hour)
	if err != nil {
		t.Fatalf("generate ca: %v", err)
	}
	serverCertPEM, serverKeyPEM, err := ca.IssueServer([]string{"127.0.0.1"}, "srv", time.Hour)
	if err != nil {
		t.Fatalf("issue server: %v", err)
	}
	caBundle, err := EncodeCABundle(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		t.Fatalf("encode ca bundle: %v", err)
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		t.Fatalf("extract material: %v", err)
	}
	serverBundle, err := EncodeServerBundle(ca.CertPEM, ca.KeyPEM, serverCertPEM, serverKeyPEM, nil)
	if err != nil {
		t.Fatalf("encode server bundle: %v", err)
	}
	serverBundle, err = cryptoutil.ApplyMetadataMaterial(serverBundle, material)
	if err != nil {
		t.Fatalf("apply material: %v", err)
	}
	broken := bytes.Replace(serverBundle, serverKeyPEM, []byte{}, 1)
	if _, err := LoadBundleFromBytes(broken); err == nil || !strings.Contains(err.Error(), "unable to match server key") {
		t.Fatalf("expected missing key error, got %v", err)
	}
}

func TestLoadClientBundleErrors(t *testing.T) {
	ca, err := GenerateCA("client-ca", time.Hour)
	if err != nil {
		t.Fatalf("generate ca: %v", err)
	}
	clientCertA, clientKeyA, err := ca.IssueClient("client-a", time.Hour)
	if err != nil {
		t.Fatalf("issue client a: %v", err)
	}
	data := append([]byte{}, clientCertA...)
	data = append(data, clientKeyA...)
	if _, err := LoadClientBundleFromBytes(data); err == nil || !strings.Contains(err.Error(), "CA certificate required") {
		t.Fatalf("expected CA required error, got %v", err)
	}
	_, clientKeyB, err := ca.IssueClient("client-b", time.Hour)
	if err != nil {
		t.Fatalf("issue client b: %v", err)
	}
	var buf bytes.Buffer
	buf.Write(ca.CertPEM)
	buf.Write(clientCertA)
	buf.Write(clientKeyB)
	if _, err := LoadClientBundleFromBytes(buf.Bytes()); err == nil || !strings.Contains(err.Error(), "matching private key not found") {
		t.Fatalf("expected mismatch error, got %v", err)
	}
}
