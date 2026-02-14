package lockd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

func TestServerMTLSRevocationFlow(t *testing.T) {
	ca, err := tlsutil.GenerateCA("lockd-test-ca", 365*24*time.Hour)
	if err != nil {
		t.Fatalf("generate ca: %v", err)
	}
	caBundleBytes, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		t.Fatalf("encode ca bundle: %v", err)
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundleBytes)
	if err != nil {
		t.Fatalf("extract ca material: %v", err)
	}
	nodeID := uuidv7.NewString()
	spiffeURI, err := SPIFFEURIForServer(nodeID)
	if err != nil {
		t.Fatalf("spiffe uri: %v", err)
	}
	serverIssued, err := ca.IssueServerWithRequest(tlsutil.ServerCertRequest{
		CommonName: "lockd-test-server",
		Validity:   365 * 24 * time.Hour,
		Hosts:      []string{"127.0.0.1", "localhost"},
		URIs:       []*url.URL{spiffeURI},
	})
	if err != nil {
		t.Fatalf("issue server cert: %v", err)
	}
	bundleBytes, err := tlsutil.EncodeServerBundle(ca.CertPEM, ca.KeyPEM, serverIssued.CertPEM, serverIssued.KeyPEM, nil)
	if err != nil {
		t.Fatalf("encode server bundle: %v", err)
	}
	bundleBytes, err = cryptoutil.ApplyMetadataMaterial(bundleBytes, material)
	if err != nil {
		t.Fatalf("apply kryptograf material: %v", err)
	}

	tempDir := t.TempDir()
	bundlePath := filepath.Join(tempDir, "server.pem")
	if err := os.WriteFile(bundlePath, bundleBytes, 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}

	clientIssued, err := ca.IssueClient(tlsutil.ClientCertRequest{CommonName: "lockd-client-1", Validity: 365 * 24 * time.Hour})
	if err != nil {
		t.Fatalf("issue client cert: %v", err)
	}
	clientTLSCert, err := tls.X509KeyPair(clientIssued.CertPEM, clientIssued.KeyPEM)
	if err != nil {
		t.Fatalf("build client key pair: %v", err)
	}
	clientCert, err := tlsutil.FirstCertificateFromPEM(clientIssued.CertPEM)
	if err != nil {
		t.Fatalf("parse client cert: %v", err)
	}
	revokedSerial := strings.ToLower(clientCert.SerialNumber.Text(16))

	server1 := startMTLSServer(t, bundlePath)
	defer server1.Close(t)

	httpClient := newMTLSClient(t, ca.CertPEM, clientTLSCert)
	resp, err := httpClient.Get("https://" + server1.addr + "/healthz")
	if err != nil {
		t.Fatalf("mtls request failed: %v", err)
	}
	_ = resp.Body.Close()

	// Stop server, revoke the client certificate, and restart.
	server1.Close(t)

	revokedBundle, err := tlsutil.EncodeServerBundle(ca.CertPEM, ca.KeyPEM, serverIssued.CertPEM, serverIssued.KeyPEM, []string{revokedSerial})
	if err != nil {
		t.Fatalf("encode revoked bundle: %v", err)
	}
	revokedBundle, err = cryptoutil.ApplyMetadataMaterial(revokedBundle, material)
	if err != nil {
		t.Fatalf("apply kryptograf material revoked: %v", err)
	}
	if err := os.WriteFile(bundlePath, revokedBundle, 0o600); err != nil {
		t.Fatalf("write revoked bundle: %v", err)
	}

	server2 := startMTLSServer(t, bundlePath)
	defer server2.Close(t)

	httpClient = newMTLSClient(t, ca.CertPEM, clientTLSCert)
	if _, err := httpClient.Get("https://" + server2.addr + "/healthz"); err == nil {
		t.Fatalf("expected revoked client to be rejected")
	} else if !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("expected certificate failure, got %v", err)
	}

	newClientIssued, err := ca.IssueClient(tlsutil.ClientCertRequest{CommonName: "lockd-client-2", Validity: 365 * 24 * time.Hour})
	if err != nil {
		t.Fatalf("issue new client cert: %v", err)
	}
	newClientTLSCert, err := tls.X509KeyPair(newClientIssued.CertPEM, newClientIssued.KeyPEM)
	if err != nil {
		t.Fatalf("build new client key pair: %v", err)
	}
	newClient := newMTLSClient(t, ca.CertPEM, newClientTLSCert)
	resp, err = newClient.Get("https://" + server2.addr + "/healthz")
	if err != nil {
		t.Fatalf("mtls request with new client failed: %v", err)
	}
	_ = resp.Body.Close()
}

type runningServer struct {
	ts   *TestServer
	addr string
}

func startMTLSServer(t *testing.T, bundlePath string) *runningServer {
	t.Helper()
	cfg := Config{
		Listen:          "127.0.0.1:0",
		Store:           "memory://",
		DisableMTLS:     false,
		BundlePath:      bundlePath,
		SweeperInterval: time.Second,
	}
	ts, err := NewTestServer(context.Background(),
		WithTestConfig(cfg),
		WithTestListener("tcp", "127.0.0.1:0"),
		WithTestLoggerFromTB(t, pslog.TraceLevel),
		WithTestMTLS(),
		WithoutTestClient(),
	)
	if err != nil {
		t.Fatalf("start mtls test server: %v", err)
	}
	addr := ts.Addr()
	if addr == nil {
		t.Fatal("server listener not available")
	}

	server := &runningServer{
		ts:   ts,
		addr: addr.String(),
	}

	t.Cleanup(func() {
		server.Close(t)
	})

	return server
}

func (s *runningServer) Close(t *testing.T) {
	if s == nil || s.ts == nil {
		return
	}
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.ts.Stop(ctx); err != nil {
		t.Fatalf("stop mtls test server: %v", err)
	}
	s.ts = nil
}

func newMTLSClient(t *testing.T, caPEM []byte, cert tls.Certificate) *http.Client {
	t.Helper()
	caCert, err := tlsutil.FirstCertificateFromPEM(caPEM)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
		ServerName:   "127.0.0.1",
	}
	return &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsCfg,
		},
	}
}

func TestServerConnectionGuardBlocksPlainClients(t *testing.T) {
	cfg := Config{
		Listen:                    "127.0.0.1:0",
		ListenProto:               "tcp",
		Store:                     "memory://",
		DisableMTLS:               true,
		SweeperInterval:           time.Second,
		ConnguardEnabled:          true,
		ConnguardFailureThreshold: 2,
		ConnguardFailureWindow:    time.Second,
		ConnguardBlockDuration:    500 * time.Millisecond,
		ConnguardProbeTimeout:     25 * time.Millisecond,
	}
	ts, err := NewTestServer(context.Background(),
		WithTestConfig(cfg),
		WithoutTestClient(),
	)
	if err != nil {
		t.Fatalf("start test server: %v", err)
	}
	t.Cleanup(func() {
		if stopErr := ts.Stop(context.Background()); stopErr != nil {
			t.Fatalf("stop test server: %v", stopErr)
		}
	})

	addr := ts.Addr().String()

	failAttempt := func() {
		conn, dialErr := net.DialTimeout("tcp", addr, time.Second)
		if dialErr != nil {
			t.Fatalf("dial for failure attempt: %v", dialErr)
		}
		_ = conn.Close()
	}
	for i := 0; i < 2; i++ {
		failAttempt()
		time.Sleep(5 * time.Millisecond)
	}

	blocked := func() bool {
		conn, dialErr := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if dialErr != nil {
			t.Fatalf("dial while blocked: %v", dialErr)
		}
		defer func() {
			_ = conn.Close()
		}()
		if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		out := make([]byte, 1)
		_, readErr := conn.Read(out)
		if readErr == nil {
			return false
		}
		var netErr net.Error
		if errors.As(readErr, &netErr) && netErr.Timeout() {
			return false
		}
		return true
	}

	isBlocked := false
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if blocked() {
			isBlocked = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !isBlocked {
		t.Fatalf("expected suspicious TCP client to be blocked")
	}
}
