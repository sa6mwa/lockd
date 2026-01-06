package benchenv

import (
	"os"
	"testing"

	"pkt.systems/lockd"
)

// MaybeEnableStorageEncryption toggles storage encryption based on LOCKD_TEST_STORAGE_ENCRYPTION.
func MaybeEnableStorageEncryption(t testing.TB, cfg *lockd.Config) bool {
	t.Helper()
	if cfg == nil {
		t.Fatalf("benchenv: nil config")
	}
	if os.Getenv("LOCKD_TEST_STORAGE_ENCRYPTION") != "1" {
		cfg.DisableStorageEncryption = true
		cfg.StorageEncryptionSnappy = false
		return false
	}
	creds := mustSharedCredentials(t)
	if !creds.Valid() {
		t.Fatalf("benchenv: storage encryption requires MTLS credentials")
	}
	cfg.DisableStorageEncryption = false
	cfg.BundlePEM = creds.ServerBundle()
	cfg.BundlePath = ""
	return true
}
