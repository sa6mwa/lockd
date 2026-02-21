package benchenv

import (
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/tlsutil"
)

var (
	sharedCredsOnce sync.Once
	sharedCreds     lockd.TestMTLSCredentials
	sharedCredsErr  error
)

// TestMTLSEnabled reports whether benchmarks should enable MTLS.
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

// SharedMTLSCredentials returns shared MTLS credentials for benchmarks.
func SharedMTLSCredentials(t testing.TB) lockd.TestMTLSCredentials {
	t.Helper()
	if !TestMTLSEnabled() {
		t.Fatalf("benchenv: MTLS credentials requested while MTLS disabled")
	}
	creds := mustSharedCredentials(t)
	if !creds.Valid() {
		t.Fatalf("benchenv: shared MTLS credentials missing material")
	}
	return creds
}

// SharedMTLSOptions returns MTLS test server options when MTLS is enabled.
func SharedMTLSOptions(t testing.TB, reuse ...lockd.TestMTLSCredentials) []lockd.TestServerOption {
	t.Helper()
	if !TestMTLSEnabled() {
		return nil
	}
	if len(reuse) > 0 {
		if !reuse[0].Valid() {
			t.Fatalf("benchenv: invalid MTLS credentials supplied for reuse")
		}
		return []lockd.TestServerOption{lockd.WithTestMTLSCredentials(reuse[0])}
	}
	creds := SharedMTLSCredentials(t)
	return []lockd.TestServerOption{lockd.WithTestMTLSCredentials(creds)}
}

func mustSharedCredentials(t testing.TB) lockd.TestMTLSCredentials {
	t.Helper()
	sharedCredsOnce.Do(func() {
		sharedCreds, sharedCredsErr = generateSharedCredentials()
	})
	if sharedCredsErr != nil {
		t.Fatalf("benchenv: generate MTLS credentials: %v", sharedCredsErr)
	}
	return sharedCreds
}

func generateSharedCredentials() (lockd.TestMTLSCredentials, error) {
	ca, err := tlsutil.GenerateCA("lockd-bench-ca", 365*24*time.Hour)
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
	nodeID := uuidv7.NewString()
	spiffeURI, err := lockd.SPIFFEURIForServer(nodeID)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	allClaim, err := nsauth.ClaimURI("ALL", nsauth.PermissionReadWrite)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	serverIssued, err := ca.IssueServerWithRequest(tlsutil.ServerCertRequest{
		CommonName: "lockd-bench-server",
		Validity:   365 * 24 * time.Hour,
		Hosts:      hosts,
		URIs:       []*url.URL{spiffeURI, allClaim},
	})
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	serverBundle, err := tlsutil.EncodeServerBundle(ca.CertPEM, nil, serverIssued.CertPEM, serverIssued.KeyPEM, nil)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	augmented, err := cryptoutil.ApplyMetadataMaterial(serverBundle, material)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	clientSPIFFE, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleSDK, "lockd-bench-client")
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	clientAllClaim, err := nsauth.ClaimURI("ALL", nsauth.PermissionReadWrite)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	clientIssued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: "lockd-bench-client",
		Validity:   365 * 24 * time.Hour,
		URIs:       []*url.URL{clientSPIFFE, clientAllClaim},
	})
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	clientBundle, err := tlsutil.EncodeClientBundle(ca.CertPEM, clientIssued.CertPEM, clientIssued.KeyPEM)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	return lockd.NewTestMTLSCredentialsFromBundles(augmented, clientBundle)
}
