package cryptotest

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/tlsutil"
)

const (
	// EnvVar toggles storage encryption for integration tests when set to "1".
	EnvVar                = "LOCKD_TEST_STORAGE_ENCRYPTION"
	bundlePathOverrideEnv = "LOCKD_TEST_STORAGE_BUNDLE"
)

var (
	sharedBundleOnce sync.Once
	sharedBundlePath string
	sharedBundleErr  error
)

// MaybeEnableStorageEncryption toggles storage encryption for the supplied config when the
// LOCKD_TEST_STORAGE_ENCRYPTION environment variable is set to "1". It returns true when
// encryption was enabled.
func MaybeEnableStorageEncryption(t testing.TB, cfg *lockd.Config) bool {
	t.Helper()
	if cfg == nil {
		t.Fatalf("cryptotest: nil config")
	}
	if os.Getenv(EnvVar) != "1" {
		cfg.DisableStorageEncryption = true
		cfg.StorageEncryptionSnappy = false
		return false
	}

	bundlePath, err := ensureSharedBundle()
	if err != nil {
		t.Fatalf("cryptotest bundle: %v", err)
	}
	cfg.DisableStorageEncryption = false
	cfg.BundlePath = bundlePath
	return true
}

func ensureSharedBundle() (string, error) {
	if path := strings.TrimSpace(os.Getenv(bundlePathOverrideEnv)); path != "" {
		return path, nil
	}
	sharedBundleOnce.Do(func() {
		sharedBundlePath, sharedBundleErr = initSharedBundle()
	})
	return sharedBundlePath, sharedBundleErr
}

func initSharedBundle() (string, error) {
	dir := filepath.Join(os.TempDir(), "lockd-cryptotest")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "server.pem")
	if _, err := os.Stat(path); err == nil {
		return path, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	ca, err := tlsutil.GenerateCA("lockd-test-ca", 365*24*time.Hour)
	if err != nil {
		return "", err
	}
	caBundle, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		return "", err
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		return "", err
	}
	serverCert, serverKey, err := ca.IssueServer([]string{"127.0.0.1"}, "lockd-test-server", 365*24*time.Hour)
	if err != nil {
		return "", err
	}
	serverBundle, err := tlsutil.EncodeServerBundle(ca.CertPEM, nil, serverCert, serverKey, nil)
	if err != nil {
		return "", err
	}
	augmented, err := cryptoutil.ApplyMetadataMaterial(serverBundle, material)
	if err != nil {
		return "", err
	}
	if err := writeBundleExclusive(path, augmented); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return path, nil
		}
		return "", err
	}
	return path, nil
}

func writeBundleExclusive(path string, payload []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(payload); err != nil {
		_ = os.Remove(path)
		return err
	}
	return f.Sync()
}
