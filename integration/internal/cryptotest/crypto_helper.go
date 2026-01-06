package cryptotest

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/tlsutil"
)

const (
	// EnvVar toggles storage encryption for integration tests when set to "1".
	EnvVar = "LOCKD_TEST_STORAGE_ENCRYPTION"
)

var (
	sharedCredsOnce sync.Once
	sharedCreds     sharedMTLSState
	sharedCredsErr  error

	sharedTCCredsOnce sync.Once
	sharedTCCreds     tcCredentials
	sharedTCCredsErr  error
)

type sharedMTLSState struct {
	ca           *tlsutil.CA
	material     cryptoutil.MetadataMaterial
	clientBundle []byte
	mu           sync.Mutex
}

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
	state := mustSharedCredentials(t)
	creds, err := state.issueCredentials()
	if err != nil {
		t.Fatalf("cryptotest: shared MTLS credentials issue: %v", err)
	}
	if !creds.Valid() {
		t.Fatalf("cryptotest: shared MTLS credentials missing material")
	}
	return creds
}

// SharedMTLSClientBundlePath writes the shared client bundle to a temp file and returns its path.
// Returns an empty string when MTLS is disabled for integration tests.
func SharedMTLSClientBundlePath(t testing.TB) string {
	t.Helper()
	if !TestMTLSEnabled() {
		return ""
	}
	creds := SharedMTLSCredentials(t)
	bundle := creds.ClientBundle()
	if len(bundle) == 0 {
		t.Fatalf("cryptotest: shared MTLS client bundle missing")
	}
	path := filepath.Join(t.TempDir(), "lockd-test-tc-client.pem")
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatalf("cryptotest: write client bundle: %v", err)
	}
	return path
}

// SharedTCClientBundlePath writes the shared TC client bundle to a temp file and returns its path.
// Returns an empty string when MTLS is disabled for integration tests.
func SharedTCClientBundlePath(t testing.TB) string {
	t.Helper()
	if !TestMTLSEnabled() {
		return ""
	}
	creds := sharedTCCredentials(t)
	if len(creds.clientBundle) == 0 {
		t.Fatalf("cryptotest: shared TC client bundle missing")
	}
	path := filepath.Join(t.TempDir(), "lockd-test-tc-client.pem")
	if err := os.WriteFile(path, creds.clientBundle, 0o600); err != nil {
		t.Fatalf("cryptotest: write TC client bundle: %v", err)
	}
	return path
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

type tcCredentials struct {
	caCertPEM    []byte
	clientBundle []byte
	clientParsed *tlsutil.ClientBundle
}

func sharedTCCredentials(t testing.TB) tcCredentials {
	t.Helper()
	sharedTCCredsOnce.Do(func() {
		sharedTCCreds, sharedTCCredsErr = generateTCCredentials()
	})
	if sharedTCCredsErr != nil {
		t.Fatalf("cryptotest: generate TC credentials: %v", sharedTCCredsErr)
	}
	return sharedTCCreds
}

func generateTCCredentials() (tcCredentials, error) {
	ca, err := tlsutil.GenerateCA("lockd-test-tc-ca", 365*24*time.Hour)
	if err != nil {
		return tcCredentials{}, err
	}
	spiffeURI, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleTC, "lockd-test-tc-client")
	if err != nil {
		return tcCredentials{}, err
	}
	clientIssued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: "lockd-test-tc-client",
		Validity:   365 * 24 * time.Hour,
		URIs:       []*url.URL{spiffeURI},
	})
	if err != nil {
		return tcCredentials{}, err
	}
	clientBundle, err := tlsutil.EncodeClientBundle(ca.CertPEM, clientIssued.CertPEM, clientIssued.KeyPEM)
	if err != nil {
		return tcCredentials{}, err
	}
	parsed, err := tlsutil.LoadClientBundleFromBytes(clientBundle)
	if err != nil {
		return tcCredentials{}, err
	}
	return tcCredentials{
		caCertPEM:    ca.CertPEM,
		clientBundle: clientBundle,
		clientParsed: parsed,
	}, nil
}

// ConfigureTCAuth sets TC auth defaults on the supplied config and returns the shared TC material.
func ConfigureTCAuth(t testing.TB, cfg *lockd.Config) {
	t.Helper()
	if cfg == nil {
		t.Fatalf("cryptotest: nil config")
	}
	if !TestMTLSEnabled() {
		return
	}
	creds := sharedTCCredentials(t)
	dir := filepath.Join(t.TempDir(), "tc-trust.d")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("cryptotest: create tc trust dir: %v", err)
	}
	path := filepath.Join(dir, "lockd-test-tc-ca.pem")
	if err := os.WriteFile(path, creds.caCertPEM, 0o600); err != nil {
		t.Fatalf("cryptotest: write tc ca: %v", err)
	}
	cfg.TCTrustDir = dir
	cfg.TCAllowDefaultCA = true
	cfg.TCDisableAuth = false
}

// RequireTCHTTPClient constructs an HTTP client that presents the TC client cert and
// trusts the server's default CA pool.
func RequireTCHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	if ts == nil {
		t.Fatalf("cryptotest: nil test server")
	}
	serverCreds := ts.TestMTLSCredentials()
	if !TestMTLSEnabled() || !serverCreds.Valid() {
		client, err := ts.NewHTTPClient()
		if err != nil {
			t.Fatalf("cryptotest: http client: %v", err)
		}
		return client
	}
	tcCreds := sharedTCCredentials(t)
	if tcCreds.clientParsed == nil {
		t.Fatalf("cryptotest: TC client bundle missing")
	}
	bundle, err := tlsutil.LoadBundleFromBytes(serverCreds.ServerBundle())
	if err != nil {
		t.Fatalf("cryptotest: parse server bundle: %v", err)
	}
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("cryptotest: default transport is %T, want *http.Transport", http.DefaultTransport)
	}
	transport := baseTransport.Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion:         tls.VersionTLS12,
		RootCAs:            bundle.CAPool,
		Certificates:       []tls.Certificate{tcCreds.clientParsed.Certificate},
		ClientSessionCache: tls.NewLRUClientSessionCache(512),
	}
	transport.ForceAttemptHTTP2 = true
	transport.MaxIdleConns = 2048
	transport.MaxIdleConnsPerHost = 2048
	transport.MaxConnsPerHost = 0
	transport.IdleConnTimeout = 90 * time.Second
	transport.DisableCompression = true

	return &http.Client{
		Transport: transport,
		Timeout:   0,
	}
}

// RequireServerHTTPClient constructs an HTTP client that presents the server certificate.
func RequireServerHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	if ts == nil {
		t.Fatalf("cryptotest: nil test server")
	}
	serverCreds := ts.TestMTLSCredentials()
	if !TestMTLSEnabled() || !serverCreds.Valid() {
		client, err := ts.NewHTTPClient()
		if err != nil {
			t.Fatalf("cryptotest: http client: %v", err)
		}
		return client
	}
	bundle, err := tlsutil.LoadBundleFromBytes(serverCreds.ServerBundle())
	if err != nil {
		t.Fatalf("cryptotest: parse server bundle: %v", err)
	}
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("cryptotest: default transport is %T, want *http.Transport", http.DefaultTransport)
	}
	transport := baseTransport.Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion:         tls.VersionTLS12,
		RootCAs:            bundle.CAPool,
		Certificates:       []tls.Certificate{bundle.ServerCertificate},
		ClientSessionCache: tls.NewLRUClientSessionCache(512),
	}
	transport.ForceAttemptHTTP2 = true
	transport.MaxIdleConns = 2048
	transport.MaxIdleConnsPerHost = 2048
	transport.MaxConnsPerHost = 0
	transport.IdleConnTimeout = 90 * time.Second
	transport.DisableCompression = true

	return &http.Client{
		Transport: transport,
		Timeout:   0,
	}
}

// RegisterRM registers the RM endpoint with the supplied TC endpoint.
func RegisterRM(t testing.TB, tc *lockd.TestServer, rm *lockd.TestServer) {
	RegisterRMWithTimeout(t, tc, rm, 5*time.Second)
}

// RegisterRMWithTimeout registers the RM endpoint with a configurable timeout.
func RegisterRMWithTimeout(t testing.TB, tc *lockd.TestServer, rm *lockd.TestServer, timeout time.Duration) {
	t.Helper()
	if tc == nil || rm == nil {
		t.Fatalf("cryptotest: tc/rm servers required")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	backend := rm.Backend()
	if backend == nil {
		t.Fatalf("cryptotest: rm backend required")
	}
	hash, err := backend.BackendHash(ctx)
	if err != nil {
		t.Fatalf("cryptotest: rm backend hash: %v", err)
	}
	req := api.TCRMRegisterRequest{
		BackendHash: strings.TrimSpace(hash),
		Endpoint:    rm.URL(),
	}
	if req.BackendHash == "" {
		t.Fatalf("cryptotest: rm backend hash empty")
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("cryptotest: marshal rm register: %v", err)
	}
	client := RequireServerHTTPClient(t, rm)
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("cryptotest: rm register timed out after %s", timeout)
		}
		attemptTimeout := 30 * time.Second
		if remaining < attemptTimeout {
			attemptTimeout = remaining
		}
		attemptCtx, attemptCancel := context.WithTimeout(context.Background(), attemptTimeout)
		httpReq, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, tc.URL()+"/v1/tc/rm/register", bytes.NewReader(body))
		if err != nil {
			attemptCancel()
			t.Fatalf("cryptotest: rm register request: %v", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(httpReq)
		attemptCancel()
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatalf("cryptotest: rm register failed: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return
		}
		if resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusServiceUnavailable {
			if time.Now().After(deadline) {
				t.Fatalf("cryptotest: rm register status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(data)))
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		t.Fatalf("cryptotest: rm register status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
}

// RequireTCClient constructs a lockd client configured with TC credentials.
func RequireTCClient(t testing.TB, ts *lockd.TestServer, opts ...lockdclient.Option) *lockdclient.Client {
	t.Helper()
	if ts == nil {
		t.Fatalf("cryptotest: nil test server")
	}
	httpClient := RequireTCHTTPClient(t, ts)
	endpoints := []string{ts.URL()}
	baseOpts := append([]lockdclient.Option{
		lockdclient.WithHTTPClient(httpClient),
		lockdclient.WithEndpointShuffle(false),
	}, opts...)
	cli, err := ts.NewEndpointsClient(endpoints, baseOpts...)
	if err != nil {
		t.Fatalf("cryptotest: tc client: %v", err)
	}
	return cli
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
	state := mustSharedCredentials(t)
	creds, err := state.issueCredentials()
	if err != nil {
		t.Fatalf("cryptotest: storage encryption issue: %v", err)
	}
	if !creds.Valid() {
		t.Fatalf("cryptotest: storage encryption requires MTLS credentials")
	}
	cfg.DisableStorageEncryption = false
	cfg.BundlePEM = creds.ServerBundle()
	cfg.BundlePath = ""
	return true
}

func mustSharedCredentials(t testing.TB) sharedMTLSState {
	t.Helper()
	sharedCredsOnce.Do(func() {
		sharedCreds, sharedCredsErr = generateSharedCredentials()
	})
	if sharedCredsErr != nil {
		t.Fatalf("cryptotest: generate MTLS credentials: %v", sharedCredsErr)
	}
	return sharedCreds
}

func generateSharedCredentials() (sharedMTLSState, error) {
	ca, err := tlsutil.GenerateCA("lockd-test-ca", 365*24*time.Hour)
	if err != nil {
		return sharedMTLSState{}, err
	}
	caBundle, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		return sharedMTLSState{}, err
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		return sharedMTLSState{}, err
	}
	clientIssued, err := ca.IssueClient(tlsutil.ClientCertRequest{CommonName: "lockd-test-client", Validity: 365 * 24 * time.Hour})
	if err != nil {
		return sharedMTLSState{}, err
	}
	clientBundle, err := tlsutil.EncodeClientBundle(ca.CertPEM, clientIssued.CertPEM, clientIssued.KeyPEM)
	if err != nil {
		return sharedMTLSState{}, err
	}
	return sharedMTLSState{
		ca:           ca,
		material:     material,
		clientBundle: clientBundle,
	}, nil
}

func (s *sharedMTLSState) issueCredentials() (lockd.TestMTLSCredentials, error) {
	if s == nil || s.ca == nil {
		return lockd.TestMTLSCredentials{}, fmt.Errorf("cryptotest: shared CA missing")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	hosts := []string{"*", "127.0.0.1", "localhost"}
	nodeID := uuidv7.NewString()
	spiffeURI, err := lockd.SPIFFEURIForServer(nodeID)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	serverIssued, err := s.ca.IssueServerWithRequest(tlsutil.ServerCertRequest{
		CommonName: "lockd-test-server",
		Validity:   365 * 24 * time.Hour,
		Hosts:      hosts,
		URIs:       []*url.URL{spiffeURI},
	})
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	serverBundle, err := tlsutil.EncodeServerBundle(s.ca.CertPEM, nil, serverIssued.CertPEM, serverIssued.KeyPEM, nil)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	augmented, err := cryptoutil.ApplyMetadataMaterial(serverBundle, s.material)
	if err != nil {
		return lockd.TestMTLSCredentials{}, err
	}
	return lockd.NewTestMTLSCredentialsFromBundles(augmented, s.clientBundle)
}
