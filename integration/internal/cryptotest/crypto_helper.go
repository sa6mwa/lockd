package cryptotest

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/tlsutil"
)

const EnvVar = "LOCKD_TEST_STORAGE_ENCRYPTION"

// MaybeEnableStorageEncryption toggles storage encryption for the supplied config when the
// LOCKD_TEST_STORAGE_ENCRYPTION environment variable is set to "1". It returns true when
// encryption was enabled.
func MaybeEnableStorageEncryption(t testing.TB, cfg *lockd.Config) bool {
	t.Helper()
	if cfg == nil {
		t.Fatalf("cryptotest: nil config")
	}
	if os.Getenv(EnvVar) != "1" {
		cfg.StorageEncryptionEnabled = false
		return false
	}

	dir := t.TempDir()
	ca, err := tlsutil.GenerateCA("lockd-test-ca", 365*24*time.Hour)
	if err != nil {
		t.Fatalf("generate ca: %v", err)
	}
	caBundle, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		t.Fatalf("encode ca bundle: %v", err)
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		t.Fatalf("extract kryptograf material: %v", err)
	}

	serverCert, serverKey, err := ca.IssueServer([]string{"127.0.0.1"}, "lockd-test-server", 365*24*time.Hour)
	if err != nil {
		t.Fatalf("issue server cert: %v", err)
	}
	serverBundle, err := tlsutil.EncodeServerBundle(ca.CertPEM, nil, serverCert, serverKey, nil)
	if err != nil {
		t.Fatalf("encode server bundle: %v", err)
	}
	augmented, err := cryptoutil.ApplyMetadataMaterial(serverBundle, material)
	if err != nil {
		t.Fatalf("apply metadata material: %v", err)
	}
	bundlePath := filepath.Join(dir, "server.pem")
	if err := os.WriteFile(bundlePath, augmented, 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	cfg.StorageEncryptionEnabled = true
	cfg.BundlePath = bundlePath
	return true
}
