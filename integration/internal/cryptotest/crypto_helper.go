package cryptotest

import (
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/tlsutil"
)

const (
	// EnvVar toggles storage encryption for integration tests when set to "1".
	EnvVar = "LOCKD_TEST_STORAGE_ENCRYPTION"
)

var (
	sharedCredsOnce sync.Once
	sharedCreds     lockd.TestMTLSCredentials
	sharedCredsErr  error
)

// TestMTLSEnabled reports whether integration tests should enable MTLS.
func TestMTLSEnabled() bool {
	v := strings.TrimSpace(os.Getenv("LOCKD_TEST_WITH_MTLS"))
	if v == "" {
		return true
	}
	switch strings.ToLower(v) {
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

// SharedMTLSOptions returns MTLS test server options when MTLS is enabled.
func SharedMTLSOptions(t testing.TB, reuse ...lockd.TestMTLSCredentials) []lockd.TestServerOption {
	t.Helper()
	if !TestMTLSEnabled() {
		return nil
	}
	if len(reuse) > 0 {
		if !reuse[0].Valid() {
			t.Fatalf("cryptotest: invalid MTLS credentials supplied for reuse")
		}
		return []lockd.TestServerOption{lockd.WithTestMTLSCredentials(reuse[0])}
	}
	creds := SharedMTLSCredentials(t)
	return []lockd.TestServerOption{lockd.WithTestMTLSCredentials(creds)}
}

// SharedMTLSCredentials returns the shared test MTLS credentials backed by the
// cryptotest CA and server bundle. MTLS must be enabled.
func SharedMTLSCredentials(t testing.TB) lockd.TestMTLSCredentials {
	t.Helper()
	if !TestMTLSEnabled() {
		t.Fatalf("cryptotest: MTLS credentials requested while MTLS disabled")
	}
	creds := mustSharedCredentials(t)
	if !creds.Valid() {
		t.Fatalf("cryptotest: shared MTLS credentials missing material")
	}
	return creds
}

// RequireMTLSHTTPClient constructs an MTLS http.Client or fails the test.
func RequireMTLSHTTPClient(t testing.TB, creds lockd.TestMTLSCredentials) *http.Client {
	t.Helper()
	if !TestMTLSEnabled() {
		t.Fatalf("cryptotest: RequireMTLSHTTPClient called while MTLS disabled")
	}
	if !creds.Valid() {
		t.Fatalf("cryptotest: MTLS credentials required but missing")
	}
	httpClient, err := creds.NewHTTPClient()
	if err != nil {
		t.Fatalf("cryptotest: build MTLS http client: %v", err)
	}
	return httpClient
}

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
	creds := mustSharedCredentials(t)
	if !creds.Valid() {
		t.Fatalf("cryptotest: storage encryption requires MTLS credentials")
	}
	cfg.DisableStorageEncryption = false
	cfg.BundlePEM = creds.ServerBundle()
	cfg.BundlePath = ""
	return true
}

func mustSharedCredentials(t testing.TB) lockd.TestMTLSCredentials {
	t.Helper()
	sharedCredsOnce.Do(func() {
		sharedCreds, sharedCredsErr = generateSharedCredentials()
	})
	if sharedCredsErr != nil {
		t.Fatalf("cryptotest: generate MTLS credentials: %v", sharedCredsErr)
	}
	return sharedCreds
}

func generateSharedCredentials() (lockd.TestMTLSCredentials, error) {
	ca, err := tlsutil.GenerateCA("lockd-test-ca", 365*24*time.Hour)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	caBundle, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	hosts := []string{"*", "127.0.0.1", "localhost"}
	serverCert, serverKey, err := ca.IssueServer(hosts, "lockd-test-server", 365*24*time.Hour)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	serverBundle, err := tlsutil.EncodeServerBundle(ca.CertPEM, nil, serverCert, serverKey, nil)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	augmented, err := cryptoutil.ApplyMetadataMaterial(serverBundle, material)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	clientCert, clientKey, err := ca.IssueClient("lockd-test-client", 365*24*time.Hour)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	clientBundle, err := tlsutil.EncodeClientBundle(ca.CertPEM, clientCert, clientKey)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	return lockd.NewTestMTLSCredentialsFromBundles(augmented, clientBundle)
}
