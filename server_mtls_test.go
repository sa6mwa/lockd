package lockd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/internal/cryptoutil"
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
	serverCertPEM, serverKeyPEM, err := ca.IssueServer([]string{"127.0.0.1", "localhost"}, "lockd-test-server", 365*24*time.Hour)
	if err != nil {
		t.Fatalf("issue server cert: %v", err)
	}
	bundleBytes, err := tlsutil.EncodeServerBundle(ca.CertPEM, ca.KeyPEM, serverCertPEM, serverKeyPEM, nil)
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

	clientCertPEM, clientKeyPEM, err := ca.IssueClient("lockd-client-1", 365*24*time.Hour)
	if err != nil {
		t.Fatalf("issue client cert: %v", err)
	}
	clientTLSCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		t.Fatalf("build client key pair: %v", err)
	}
	clientCert, err := tlsutil.FirstCertificateFromPEM(clientCertPEM)
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

	revokedBundle, err := tlsutil.EncodeServerBundle(ca.CertPEM, ca.KeyPEM, serverCertPEM, serverKeyPEM, []string{revokedSerial})
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

	newClientCertPEM, newClientKeyPEM, err := ca.IssueClient("lockd-client-2", 365*24*time.Hour)
	if err != nil {
		t.Fatalf("issue new client cert: %v", err)
	}
	newClientTLSCert, err := tls.X509KeyPair(newClientCertPEM, newClientKeyPEM)
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
