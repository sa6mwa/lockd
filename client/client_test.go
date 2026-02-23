package client_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

func TestUnixSocketClientLifecycle(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "lockd.sock")
	ts := lockd.StartTestServer(t,
		lockd.WithTestUnixSocket(socket),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	aCtx, aCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer aCancel()
	lease, err := cli.Acquire(aCtx, api.AcquireRequest{Key: "unix-test", Owner: "tester", TTLSeconds: 10, BlockSecs: client.BlockWaitForever})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestLeaseSessionLoadSave(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "lockd-state.sock")
	ts := lockd.StartTestServer(t,
		lockd.WithTestUnixSocket(socket),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	sessEmpty, err := cli.Acquire(ctx, api.AcquireRequest{Key: "empty", Owner: "tester", TTLSeconds: 5, BlockSecs: client.BlockWaitForever})
	if err != nil {
		t.Fatalf("acquire empty: %v", err)
	}
	defer sessEmpty.Close()
	var zero struct{ Value string }
	if err := sessEmpty.Load(ctx, &zero); err != nil {
		t.Fatalf("load empty: %v", err)
	}
	if zero.Value != "" {
		t.Fatalf("expected zero value, got %q", zero.Value)
	}

	type payload struct {
		Value string `json:"value"`
		Count int    `json:"count"`
	}

	sess, err := cli.Acquire(ctx, api.AcquireRequest{Key: "state", Owner: "writer", TTLSeconds: 5, BlockSecs: client.BlockWaitForever})
	if err != nil {
		t.Fatalf("acquire state: %v", err)
	}
	if err := sess.Save(ctx, payload{Value: "foo", Count: 7}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := sess.Release(ctx); err != nil {
		t.Fatalf("release after save: %v", err)
	}

	sess2, err := cli.Acquire(ctx, api.AcquireRequest{Key: "state", Owner: "reader", TTLSeconds: 5, BlockSecs: client.BlockWaitForever})
	if err != nil {
		t.Fatalf("reacquire state: %v", err)
	}
	defer sess2.Close()
	var out payload
	if err := sess2.Load(ctx, &out); err != nil {
		t.Fatalf("load persisted: %v", err)
	}
	if out.Value != "foo" || out.Count != 7 {
		t.Fatalf("unexpected payload: %+v", out)
	}
}

func TestClientDocumentSaveAndLoad(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "lockd-doc.sock")
	ts := lockd.StartTestServer(t,
		lockd.WithTestUnixSocket(socket),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	key := "doc-save-load"
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "doc-tester",
		TTLSeconds: 15,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	doc := client.NewDocument(namespaces.Default, key)
	if err := doc.Mutate(
		"/status=ready",
		"/attempt=1",
		"time:/timestamps/updated=NOW",
	); err != nil {
		lease.Close()
		t.Fatalf("mutate: %v", err)
	}
	if err := lease.Save(ctx, doc); err != nil {
		lease.Close()
		t.Fatalf("save document: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release after doc save: %v", err)
	}

	loaded := &client.Document{}
	if err := cli.Load(ctx, key, loaded, client.WithLoadNamespace(namespaces.Default)); err != nil {
		t.Fatalf("load into document: %v", err)
	}
	if loaded.Namespace != namespaces.Default || loaded.Key != key {
		t.Fatalf("unexpected loaded identity: %+v", loaded)
	}
	if loaded.Body["status"] != "ready" {
		t.Fatalf("expected status ready, got %+v", loaded.Body)
	}

	resp, err := cli.Get(ctx, key, client.WithGetNamespace(namespaces.Default))
	if err != nil {
		t.Fatalf("get for document: %v", err)
	}
	getDoc, err := resp.Document()
	if err != nil {
		t.Fatalf("response document: %v", err)
	}
	if getDoc == nil || getDoc.Body["status"] != "ready" {
		t.Fatalf("unexpected get doc: %+v", getDoc)
	}

	lease2, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "doc-reader",
		TTLSeconds: 15,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	readerPayload := `{"status":"reader","attempt":2}`
	if err := lease2.Save(ctx, strings.NewReader(readerPayload)); err != nil {
		lease2.Close()
		t.Fatalf("save reader payload: %v", err)
	}
	if err := lease2.Release(ctx); err != nil {
		t.Fatalf("release after reader save: %v", err)
	}

	var target struct {
		Status  string `json:"status"`
		Attempt int    `json:"attempt"`
	}
	if err := cli.Load(ctx, key, &target, client.WithLoadNamespace(namespaces.Default)); err != nil {
		t.Fatalf("load struct: %v", err)
	}
	if target.Status != "reader" || target.Attempt != 2 {
		t.Fatalf("unexpected struct load: %+v", target)
	}
}

func TestAcquireAutoGeneratesKey(t *testing.T) {
	ts := lockd.StartTestServer(t, lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)))
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Owner:      "auto",
		TTLSeconds: 10,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.Key == "" {
		t.Fatal("expected generated key")
	}
	if err := lease.Save(ctx, map[string]any{"generated": true}); err != nil {
		t.Fatalf("save: %v", err)
	}
	generatedKey := lease.Key
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	verify, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        generatedKey,
		Owner:      "verify-auto",
		TTLSeconds: 10,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	defer verify.Close()
	var state map[string]any
	if err := verify.Load(ctx, &state); err != nil {
		t.Fatalf("load: %v", err)
	}
	if state["generated"] != true {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestClientDefaultSchemeMTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var hit bool
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/describe" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("key") != "demo" {
			t.Fatalf("unexpected key: %s", r.URL.Query().Get("key"))
		}
		hit = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"key":"demo","version":1}`)
	}))
	defer ts.Close()

	base := strings.TrimPrefix(ts.URL, "https://")
	cli, err := client.New(base, client.WithHTTPClient(ts.Client()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := cli.Describe(ctx, "demo")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if resp == nil || resp.Key != "demo" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if !hit {
		t.Fatal("expected server to receive request")
	}
}

func TestClientDefaultSchemeInsecure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var hit bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/describe" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("key") != "demo" {
			t.Fatalf("unexpected key: %s", r.URL.Query().Get("key"))
		}
		hit = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"key":"demo","version":1}`)
	}))
	defer ts.Close()

	base := strings.TrimPrefix(ts.URL, "http://")
	cli, err := client.New(base, client.WithHTTPClient(ts.Client()), client.WithDisableMTLS(true))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := cli.Describe(ctx, "demo")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if resp == nil || resp.Key != "demo" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if !hit {
		t.Fatal("expected server to receive request")
	}
}

func TestClientWithBundlePathMTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ca, err := tlsutil.GenerateCA("lockd-sdk-test-ca", time.Hour)
	if err != nil {
		t.Fatalf("generate ca: %v", err)
	}
	serverIssued, err := ca.IssueServer([]string{"127.0.0.1", "localhost"}, "lockd-sdk-test-server", time.Hour)
	if err != nil {
		t.Fatalf("issue server cert: %v", err)
	}
	clientIssued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: "lockd-sdk-test-client",
		Validity:   time.Hour,
	})
	if err != nil {
		t.Fatalf("issue client cert: %v", err)
	}

	clientBundlePEM, err := tlsutil.EncodeClientBundle(ca.CertPEM, clientIssued.CertPEM, clientIssued.KeyPEM)
	if err != nil {
		t.Fatalf("encode client bundle: %v", err)
	}
	bundlePath := filepath.Join(t.TempDir(), "client.pem")
	if err := os.WriteFile(bundlePath, clientBundlePEM, 0o600); err != nil {
		t.Fatalf("write client bundle: %v", err)
	}

	serverCert, err := tls.X509KeyPair(serverIssued.CertPEM, serverIssued.KeyPEM)
	if err != nil {
		t.Fatalf("load server keypair: %v", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(ca.CertPEM) {
		t.Fatal("append ca cert")
	}

	var hit bool
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/describe" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if len(r.TLS.PeerCertificates) == 0 {
			t.Fatal("expected peer client certificate")
		}
		hit = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"key":"demo","version":1}`)
	}))
	ts.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
	ts.StartTLS()
	defer ts.Close()

	cli, err := client.New(ts.URL, client.WithBundlePath(bundlePath))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := cli.Describe(ctx, "demo")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if resp == nil || resp.Key != "demo" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if !hit {
		t.Fatal("expected server to receive request")
	}
}

func TestClientWithBundlePEMMTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts, bundlePEM, _, hit := startClientMTLSTestServer(t)

	cli, err := client.New(ts.URL, client.WithBundlePEM(bundlePEM))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := cli.Describe(ctx, "demo")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if resp == nil || resp.Key != "demo" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if !*hit {
		t.Fatal("expected server to receive request")
	}
}

func TestClientWithBundlePEMInvalid(t *testing.T) {
	_, err := client.New("https://127.0.0.1:9341", client.WithBundlePEM([]byte("not-a-bundle")))
	if err == nil {
		t.Fatal("expected error for invalid bundle PEM")
	}
	if !strings.Contains(err.Error(), "load client bundle from PEM") {
		t.Fatalf("expected PEM parse error, got %v", err)
	}
}

func TestClientWithBundlePEMOverridesBundlePath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts, bundlePEM, _, hit := startClientMTLSTestServer(t)

	cli, err := client.New(
		ts.URL,
		client.WithBundlePath(filepath.Join(t.TempDir(), "missing-client.pem")),
		client.WithBundlePEM(bundlePEM),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := cli.Describe(ctx, "demo")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if resp == nil || resp.Key != "demo" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if !*hit {
		t.Fatal("expected server to receive request")
	}
}

func TestClientWithBundlePathOverridesBundlePEM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts, _, bundlePath, hit := startClientMTLSTestServer(t)

	cli, err := client.New(
		ts.URL,
		client.WithBundlePEM([]byte("invalid")),
		client.WithBundlePath(bundlePath),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := cli.Describe(ctx, "demo")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if resp == nil || resp.Key != "demo" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if !*hit {
		t.Fatal("expected server to receive request")
	}
}

func TestClientWithBundlePathInvalid(t *testing.T) {
	_, err := client.New("https://127.0.0.1:9341", client.WithBundlePath(filepath.Join(t.TempDir(), "missing-client.pem")))
	if err == nil {
		t.Fatal("expected error for missing bundle")
	}
	if !strings.Contains(err.Error(), "load client bundle") {
		t.Fatalf("expected load client bundle error, got %v", err)
	}
}

func TestClientWithBundlePathExpandsEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LOCKD_TEST_BUNDLE_DIR", dir)
	missing := filepath.Join(dir, "missing-client.pem")
	_, err := client.New("https://127.0.0.1:9341", client.WithBundlePath("$LOCKD_TEST_BUNDLE_DIR/missing-client.pem"))
	if err == nil {
		t.Fatal("expected error for missing bundle")
	}
	if !strings.Contains(err.Error(), missing) {
		t.Fatalf("expected expanded bundle path %q in error, got %v", missing, err)
	}
	if strings.Contains(err.Error(), "$LOCKD_TEST_BUNDLE_DIR") {
		t.Fatalf("expected env var to be expanded, got %v", err)
	}
}

func TestClientWithBundlePathExpandsHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	missing := filepath.Join(home, ".lockd", "missing-client.pem")
	_, err := client.New("https://127.0.0.1:9341", client.WithBundlePath("~/.lockd/missing-client.pem"))
	if err == nil {
		t.Fatal("expected error for missing bundle")
	}
	if !strings.Contains(err.Error(), missing) {
		t.Fatalf("expected expanded home bundle path %q in error, got %v", missing, err)
	}
	if strings.Contains(err.Error(), "~/.lockd") {
		t.Fatalf("expected home path to be expanded, got %v", err)
	}
}

func TestClientWithBundlePathDisableExpansion(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LOCKD_TEST_BUNDLE_DIR", dir)
	literal := "$LOCKD_TEST_BUNDLE_DIR/missing-client.pem"
	_, err := client.New(
		"https://127.0.0.1:9341",
		client.WithBundlePath(literal),
		client.WithBundlePathDisableExpansion(),
	)
	if err == nil {
		t.Fatal("expected error for missing bundle")
	}
	if !strings.Contains(err.Error(), literal) {
		t.Fatalf("expected literal bundle path %q in error, got %v", literal, err)
	}
}

func startClientMTLSTestServer(t *testing.T) (*httptest.Server, []byte, string, *bool) {
	t.Helper()

	ca, err := tlsutil.GenerateCA("lockd-sdk-test-ca", time.Hour)
	if err != nil {
		t.Fatalf("generate ca: %v", err)
	}
	serverIssued, err := ca.IssueServer([]string{"127.0.0.1", "localhost"}, "lockd-sdk-test-server", time.Hour)
	if err != nil {
		t.Fatalf("issue server cert: %v", err)
	}
	clientIssued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: "lockd-sdk-test-client",
		Validity:   time.Hour,
	})
	if err != nil {
		t.Fatalf("issue client cert: %v", err)
	}
	clientBundlePEM, err := tlsutil.EncodeClientBundle(ca.CertPEM, clientIssued.CertPEM, clientIssued.KeyPEM)
	if err != nil {
		t.Fatalf("encode client bundle: %v", err)
	}
	bundlePath := filepath.Join(t.TempDir(), "client.pem")
	if err := os.WriteFile(bundlePath, clientBundlePEM, 0o600); err != nil {
		t.Fatalf("write client bundle: %v", err)
	}

	serverCert, err := tls.X509KeyPair(serverIssued.CertPEM, serverIssued.KeyPEM)
	if err != nil {
		t.Fatalf("load server keypair: %v", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(ca.CertPEM) {
		t.Fatal("append ca cert")
	}

	hit := new(bool)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/describe" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if len(r.TLS.PeerCertificates) == 0 {
			t.Fatal("expected peer client certificate")
		}
		*hit = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"key":"demo","version":1}`)
	}))
	ts.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
	ts.StartTLS()
	t.Cleanup(ts.Close)
	return ts, clientBundlePEM, bundlePath, hit
}

func TestAcquireWaitForeverIgnoresHTTPTimeout(t *testing.T) {
	var (
		callCount int
		delay     = 150 * time.Millisecond
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/acquire":
			callCount++
			time.Sleep(delay)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"lease_id":"L1","key":"orders","owner":"worker","expires_at_unix":1,"version":1,"fencing_token":123}`)
		case "/v1/release":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"released":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	cli, err := client.New(strings.TrimPrefix(ts.URL, "http://"),
		client.WithDisableMTLS(true),
		client.WithHTTPClient(ts.Client()),
		client.WithHTTPTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	sess, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "worker",
		TTLSeconds: 30,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected single acquire attempt, got %d", callCount)
	}
	if err := sess.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestClientCorrelationPropagation(t *testing.T) {
	expected := "corr-test"
	var acquireHeader, updateHeader, releaseHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/acquire":
			acquireHeader = r.Header.Get("X-Correlation-Id")
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Correlation-Id", expected)
			fmt.Fprintf(w, `{"lease_id":"L1","key":"orders","owner":"worker","expires_at_unix":1,"version":1,"fencing_token":1,"correlation_id":"%s"}`, expected)
		case strings.HasPrefix(r.URL.Path, "/v1/update"):
			updateHeader = r.Header.Get("X-Correlation-Id")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"new_version":2,"new_state_etag":"etag","bytes":2}`)
		case r.URL.Path == "/v1/release":
			releaseHeader = r.Header.Get("X-Correlation-Id")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"released":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	cli, err := client.New(ts.URL, client.WithHTTPClient(ts.Client()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx := client.WithCorrelationID(context.Background(), expected)
	lease, err := cli.Acquire(ctx, api.AcquireRequest{Key: "orders", Owner: "worker", TTLSeconds: 5})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.CorrelationID != expected {
		t.Fatalf("expected session correlation %q, got %q", expected, lease.CorrelationID)
	}
	if acquireHeader != expected {
		t.Fatalf("expected acquire header %q, got %q", expected, acquireHeader)
	}
	if _, err := lease.UpdateBytes(ctx, []byte(`{}`)); err != nil {
		t.Fatalf("update: %v", err)
	}
	if updateHeader != expected {
		t.Fatalf("expected update header %q, got %q", expected, updateHeader)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if releaseHeader != expected {
		t.Fatalf("expected release header %q, got %q", expected, releaseHeader)
	}
}

func TestLeaseSessionRemove(t *testing.T) {
	var removeVersion, removeETag, removeToken string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/acquire":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Fencing-Token", "41")
			fmt.Fprint(w, `{"lease_id":"L-remove","key":"orders","owner":"worker","expires_at_unix":123,"version":2,"state_etag":"etag-initial","fencing_token":41}`)
		case strings.HasPrefix(r.URL.Path, "/v1/remove"):
			removeVersion = r.Header.Get("X-If-Version")
			removeETag = r.Header.Get("X-If-State-ETag")
			removeToken = r.Header.Get("X-Fencing-Token")
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Fencing-Token", "42")
			fmt.Fprint(w, `{"removed":true,"new_version":3}`)
		case r.URL.Path == "/v1/release":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"released":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	cli, err := client.New(strings.TrimPrefix(ts.URL, "http://"),
		client.WithDisableMTLS(true),
		client.WithHTTPClient(ts.Client()),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "worker",
		TTLSeconds: 10,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	result, err := lease.Remove(ctx)
	if err != nil {
		t.Fatalf("remove state: %v", err)
	}
	if !result.Removed {
		t.Fatalf("expected removal to report success: %+v", result)
	}
	if result.NewVersion != 3 {
		t.Fatalf("expected new version 3, got %d", result.NewVersion)
	}
	if removeVersion != "2" {
		t.Fatalf("expected If-Version header 2, got %q", removeVersion)
	}
	if removeETag != "etag-initial" {
		t.Fatalf("expected If-State-ETag header, got %q", removeETag)
	}
	if removeToken != "41" {
		t.Fatalf("expected fencing token 41, got %q", removeToken)
	}

	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestCorrelationTransportMiddleware(t *testing.T) {
	expected := "cli-corr"
	var seen string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("X-Correlation-Id")
		w.Header().Set("X-Correlation-Id", expected)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	cli := client.WithCorrelationHTTPClient(nil, expected)
	req, err := http.NewRequest(http.MethodGet, ts.URL, http.NoBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Correlation-Id", "should-be-overwritten")
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	resp.Body.Close()
	if seen != expected {
		t.Fatalf("expected header %q, got %q", expected, seen)
	}
	if got := client.CorrelationIDFromResponse(resp); got != expected {
		t.Fatalf("expected response helper %q, got %q", expected, got)
	}
}

func TestAcquireForUpdateImmediateUpdate(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "lockd-drain.sock")
	ts := lockd.StartTestServer(t,
		lockd.WithTestUnixSocket(socket),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	key := "drain-regression"
	writer, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "writer",
		TTLSeconds: 10,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire writer: %v", err)
	}
	if _, err := writer.UpdateBytes(ctx, []byte(`{"value":1}`)); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if err := writer.Release(ctx); err != nil {
		t.Fatalf("release writer: %v", err)
	}

	acqCtx, acqCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer acqCancel()

	var seenState []byte
	err = cli.AcquireForUpdate(acqCtx, api.AcquireRequest{
		Key:        key,
		Owner:      "reader",
		TTLSeconds: 10,
		BlockSecs:  client.BlockWaitForever,
	}, func(handlerCtx context.Context, af *client.AcquireForUpdateContext) error {
		if af.State == nil || af.State.Reader == nil {
			return fmt.Errorf("expected state reader")
		}
		data, err := af.State.Bytes()
		if err != nil {
			return err
		}
		seenState = data

		updateCtx, updateCancel := context.WithTimeout(handlerCtx, 500*time.Millisecond)
		defer updateCancel()
		_, err = af.UpdateBytes(updateCtx, []byte(`{"value":2}`))
		return err
	})
	if err != nil {
		t.Fatalf("acquire-for-update: %v", err)
	}
	if string(seenState) != `{"value":1}` {
		t.Fatalf("unexpected snapshot: %s", seenState)
	}

	verify, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "verifier",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("verify acquire: %v", err)
	}
	defer verify.Close()

	data, err := verify.GetBytes(ctx)
	if err != nil {
		t.Fatalf("get bytes: %v", err)
	}
	if string(data) != `{"value":2}` {
		t.Fatalf("unexpected state: %s", data)
	}
}

func TestClientLoadDefaultsToPublic(t *testing.T) {
	ts := lockd.StartTestServer(t,
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	key := "public-load"
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "writer",
		TTLSeconds: 10,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := lease.UpdateBytes(ctx, []byte(`{"value":42}`)); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	var snapshot map[string]int
	if err := cli.Load(ctx, key, &snapshot); err != nil {
		t.Fatalf("load default public: %v", err)
	}
	if snapshot["value"] != 42 {
		t.Fatalf("unexpected snapshot %+v", snapshot)
	}
}

func TestClientGetPublicDisabledRequiresLease(t *testing.T) {
	ts := lockd.StartTestServer(t,
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	key := "public-disabled"
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "writer",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := lease.UpdateBytes(ctx, []byte(`{"value":1}`)); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	_, err = cli.Get(ctx, key, client.WithGetPublicDisabled(true))
	if err == nil {
		t.Fatalf("expected error when public disabled without lease")
	}
	if !strings.Contains(err.Error(), "lease_id required") {
		t.Fatalf("expected client lease validation error, got %v", err)
	}
}

func TestClientUseNamespaceDefaults(t *testing.T) {
	ts := lockd.StartTestServer(t,
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	if err := cli.UseNamespace("alpha"); err != nil {
		t.Fatalf("use namespace: %v", err)
	}

	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "ns-key",
		Owner:      "writer",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.Namespace != "alpha" {
		t.Fatalf("expected namespace alpha, got %s", lease.Namespace)
	}
	if _, err := lease.UpdateBytes(ctx, []byte(`{"value":99}`)); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	resp, err := cli.Get(ctx, "ns-key")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp == nil || resp.Namespace != "alpha" {
		t.Fatalf("expected namespace alpha response, got %+v", resp)
	}
	data, err := resp.Bytes()
	resp.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != `{"value":99}` {
		t.Fatalf("unexpected payload: %s", data)
	}
}

func TestClientUseLeaseIDDefaults(t *testing.T) {
	ts := lockd.StartTestServer(t,
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "lease-default",
		Owner:      "worker",
		TTLSeconds: 10,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	cli.UseLeaseID(lease.LeaseID)
	if _, err := lease.UpdateBytes(ctx, []byte(`{"value":7}`)); err != nil {
		t.Fatalf("update: %v", err)
	}

	resp, err := cli.Get(ctx, "lease-default", client.WithGetPublicDisabled(true))
	if err != nil {
		t.Fatalf("sticky lease get: %v", err)
	}
	if resp == nil {
		t.Fatal("expected response")
	}
	data, err := resp.Bytes()
	resp.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != `{"value":7}` {
		t.Fatalf("unexpected payload: %s", data)
	}

	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	_, err = cli.Get(ctx, "lease-default", client.WithGetPublicDisabled(true))
	if err == nil {
		t.Fatalf("expected error after lease cleared")
	}
	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		if apiErr.Response.ErrorCode != "lease_required" {
			t.Fatalf("expected lease_required after release, got %v", apiErr.Response.ErrorCode)
		}
	} else if !strings.Contains(err.Error(), "lease_id required") {
		t.Fatalf("unexpected error after release: %v", err)
	}
}

func newJSONResponse(req *http.Request, status int, body string) *http.Response {
	if body == "" {
		body = "{}"
	}
	resp := &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
	resp.Header.Set("Content-Type", "application/json")
	resp.ContentLength = int64(len(body))
	return resp
}

type failoverTransport struct {
	mu            sync.Mutex
	calls         []string
	host1Attempts int
	host2Attempts int
}

func (t *failoverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	host := req.URL.Host
	path := req.URL.Path
	t.calls = append(t.calls, host+path)
	switch {
	case strings.Contains(host, "host1"):
		t.host1Attempts++
		if path == "/v1/release" {
			return newJSONResponse(req, http.StatusOK, `{"released":true}`), nil
		}
		return nil, fmt.Errorf("host1 unavailable")
	case strings.Contains(host, "host2"):
		t.host2Attempts++
		switch path {
		case "/v1/acquire":
			if t.host1Attempts == 0 {
				return nil, fmt.Errorf("host2 waiting for host1")
			}
			resp := newJSONResponse(req, http.StatusOK, `{"lease_id":"L-failover","key":"orders","owner":"worker","expires_at_unix":1,"version":1,"fencing_token":42}`)
			resp.Header.Set("X-Correlation-Id", "cid-failover")
			return resp, nil
		case "/v1/release":
			return newJSONResponse(req, http.StatusOK, `{"released":true}`), nil
		default:
			return nil, fmt.Errorf("unexpected path %s", path)
		}
	default:
		return nil, fmt.Errorf("unexpected host %s", host)
	}
}

func TestClientAcquireFailoverAcrossEndpoints(t *testing.T) {
	transport := &failoverTransport{}
	httpClient := &http.Client{Transport: transport}
	endpoints := []string{"http://host1:9341", "http://host2:9341"}
	cli, err := client.NewWithEndpoints(endpoints,
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
		client.WithHTTPTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "worker",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	}, client.WithAcquireBackoff(5*time.Millisecond, 5*time.Millisecond, 1.0),
		client.WithAcquireJitter(0),
		client.WithAcquireFailureRetries(3))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.LeaseID != "L-failover" {
		t.Fatalf("unexpected lease id: %s", lease.LeaseID)
	}

	transport.mu.Lock()
	host1Attempts := transport.host1Attempts
	host2Attempts := transport.host2Attempts
	callCount := len(transport.calls)
	transport.mu.Unlock()

	if host1Attempts == 0 {
		t.Fatalf("expected host1 to be attempted at least once")
	}
	if host2Attempts == 0 {
		t.Fatalf("expected host2 to be attempted at least once")
	}
	if callCount < 2 {
		t.Fatalf("expected at least two HTTP calls, got %d", callCount)
	}

	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

type downTransport struct {
	mu    sync.Mutex
	calls []string
}

func (t *downTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, req.URL.Host)
	return nil, fmt.Errorf("connect: refused")
}

type certFailureTransport struct {
	mu    sync.Mutex
	calls int
}

func (t *certFailureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls++
	return nil, &url.Error{
		Op:  req.Method,
		URL: req.URL.String(),
		Err: fmt.Errorf("mtls: verify server certificate: %w", x509.UnknownAuthorityError{}),
	}
}

type acquireBlockSecsTransport struct {
	t         *testing.T
	blockSecs int64
}

func (tpt *acquireBlockSecsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tpt.t.Helper()
	if req.URL.Path != "/v1/acquire" {
		return newJSONResponse(req, http.StatusNotFound, `{"error":"not_found"}`), nil
	}
	var payload api.AcquireRequest
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
		tpt.t.Fatalf("decode acquire request: %v", err)
	}
	tpt.blockSecs = payload.BlockSecs
	body := fmt.Sprintf(`{"lease_id":"L1","key":"orders","owner":"worker","expires_at_unix":%d,"version":1,"fencing_token":1}`, time.Now().Unix()+30)
	return newJSONResponse(req, http.StatusOK, body), nil
}

type acquireAlreadyExistsTransport struct {
	mu    sync.Mutex
	calls int
}

func (tpt *acquireAlreadyExistsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tpt.mu.Lock()
	defer tpt.mu.Unlock()
	if req.URL.Path != "/v1/acquire" {
		return newJSONResponse(req, http.StatusNotFound, `{"error":"not_found"}`), nil
	}
	tpt.calls++
	return newJSONResponse(req, http.StatusConflict, `{"error":"already_exists","detail":"key already exists"}`), nil
}

func TestClientAcquireAllEndpointsDown(t *testing.T) {
	transport := &downTransport{}
	httpClient := &http.Client{Transport: transport}
	endpoints := []string{"http://hosta:9341", "http://hostb:9341"}
	cli, err := client.NewWithEndpoints(endpoints,
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = cli.Acquire(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "worker",
		TTLSeconds: 5,
		BlockSecs:  client.BlockNoWait,
	})
	if err == nil {
		t.Fatal("expected acquire to fail when all endpoints unreachable")
	}
	if !strings.Contains(err.Error(), "all endpoints unreachable") {
		t.Fatalf("unexpected error: %v", err)
	}

	transport.mu.Lock()
	callCount := len(transport.calls)
	transport.mu.Unlock()
	if callCount < len(endpoints) {
		t.Fatalf("expected at least %d attempts, got %d", len(endpoints), callCount)
	}
}

func TestClientAcquireFatalTLSFailureNoRetry(t *testing.T) {
	transport := &certFailureTransport{}
	httpClient := &http.Client{Transport: transport}
	endpoints := []string{"https://hosta:9341"}
	cli, err := client.NewWithEndpoints(endpoints,
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = cli.Acquire(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "worker",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	},
		client.WithAcquireFailureRetries(1),
		client.WithAcquireBackoff(time.Millisecond, time.Millisecond, 1),
		client.WithAcquireJitter(0),
	)
	if err == nil {
		t.Fatal("expected acquire to fail with certificate verification error")
	}
	var unknownAuthErr x509.UnknownAuthorityError
	if !errors.As(err, &unknownAuthErr) {
		t.Fatalf("expected x509.UnknownAuthorityError, got %T: %v", err, err)
	}

	transport.mu.Lock()
	callCount := transport.calls
	transport.mu.Unlock()
	if callCount != 1 {
		t.Fatalf("expected exactly one attempt on fatal TLS error, got %d", callCount)
	}
}

func TestClientAcquireBlockWaitForeverHonorsContextDeadline(t *testing.T) {
	transport := &acquireBlockSecsTransport{t: t}
	httpClient := &http.Client{Transport: transport}
	endpoints := []string{"http://hosta:9341"}
	cli, err := client.NewWithEndpoints(endpoints,
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = cli.Acquire(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "worker",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if transport.blockSecs <= 0 {
		t.Fatalf("expected block_secs > 0 when context has deadline, got %d", transport.blockSecs)
	}
}

func TestClientAcquireAlreadyExistsNoRetry(t *testing.T) {
	transport := &acquireAlreadyExistsTransport{}
	httpClient := &http.Client{Transport: transport}
	endpoints := []string{"http://hosta:9341"}
	cli, err := client.NewWithEndpoints(endpoints,
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = cli.Acquire(context.Background(), api.AcquireRequest{
		Key:         "orders",
		Owner:       "worker",
		TTLSeconds:  5,
		BlockSecs:   client.BlockWaitForever,
		IfNotExists: true,
	})
	if err == nil {
		t.Fatal("expected already_exists error")
	}
	if !client.IsAlreadyExists(err) {
		t.Fatalf("expected IsAlreadyExists=true, got %v", err)
	}
	if !errors.Is(err, client.ErrAlreadyExists) {
		t.Fatalf("expected errors.Is(err, ErrAlreadyExists), got %v", err)
	}
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError type, got %T: %v", err, err)
	}
	if apiErr.Response.ErrorCode != "already_exists" {
		t.Fatalf("expected already_exists error code, got %q", apiErr.Response.ErrorCode)
	}
	transport.mu.Lock()
	calls := transport.calls
	transport.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected single acquire attempt, got %d", calls)
	}
}

type acquireForUpdateRetryTransport struct {
	mu           sync.Mutex
	acquireCalls int
	getCalls     int
	updateCalls  int
	releaseCalls int
}

func (t *acquireForUpdateRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch req.URL.Path {
	case "/v1/acquire":
		t.acquireCalls++
		leaseID := fmt.Sprintf("L%d", t.acquireCalls)
		fencing := 70 + t.acquireCalls
		body := fmt.Sprintf(`{"lease_id":"%s","key":"orders","owner":"reader","expires_at_unix":%d,"version":1,"fencing_token":%d}`, leaseID, t.acquireCalls+1, fencing)
		resp := newJSONResponse(req, http.StatusOK, body)
		resp.Header.Set("X-Correlation-Id", fmt.Sprintf("cid-%d", t.acquireCalls))
		return resp, nil
	case "/v1/get":
		t.getCalls++
		if t.getCalls == 1 {
			return newJSONResponse(req, http.StatusConflict, `{"error":"lease_required"}`), nil
		}
		resp := newJSONResponse(req, http.StatusOK, `{"value":1}`)
		resp.Header.Set("ETag", "etag-initial")
		resp.Header.Set("X-Key-Version", "1")
		resp.Header.Set("X-Fencing-Token", "73")
		resp.Header.Set("Content-Length", strconv.Itoa(len(`{"value":1}`)))
		return resp, nil
	case "/v1/update":
		t.updateCalls++
		resp := newJSONResponse(req, http.StatusOK, `{"new_version":2,"new_state_etag":"etag-updated","bytes":9}`)
		return resp, nil
	case "/v1/release":
		t.releaseCalls++
		return newJSONResponse(req, http.StatusOK, `{"released":true}`), nil
	default:
		return newJSONResponse(req, http.StatusNotFound, `{"error":"not_found"}`), nil
	}
}

type acquireForUpdateAcquireErrorTransport struct {
	mu           sync.Mutex
	acquireCalls int
	getCalls     int
	updateCalls  int
	releaseCalls int
}

func (t *acquireForUpdateAcquireErrorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch req.URL.Path {
	case "/v1/acquire":
		t.acquireCalls++
		if t.acquireCalls == 1 {
			return nil, context.DeadlineExceeded
		}
		leaseID := fmt.Sprintf("L-acq-%d", t.acquireCalls)
		body := fmt.Sprintf(`{"lease_id":"%s","key":"orders","owner":"reader","expires_at_unix":%d,"version":1,"fencing_token":%d}`,
			leaseID, time.Now().Add(time.Minute).Unix(), 80+t.acquireCalls)
		resp := newJSONResponse(req, http.StatusOK, body)
		resp.Header.Set("X-Correlation-Id", fmt.Sprintf("cid-acq-%d", t.acquireCalls))
		return resp, nil
	case "/v1/get":
		t.getCalls++
		resp := newJSONResponse(req, http.StatusOK, `{"value":1}`)
		resp.Header.Set("ETag", "etag-one")
		resp.Header.Set("X-Key-Version", "1")
		resp.Header.Set("X-Fencing-Token", "201")
		resp.Header.Set("Content-Length", strconv.Itoa(len(`{"value":1}`)))
		return resp, nil
	case "/v1/update":
		t.updateCalls++
		return newJSONResponse(req, http.StatusOK, `{"new_version":2,"new_state_etag":"etag-two","bytes":9}`), nil
	case "/v1/release":
		t.releaseCalls++
		return newJSONResponse(req, http.StatusOK, `{"released":true}`), nil
	default:
		return newJSONResponse(req, http.StatusNotFound, `{"error":"not_found"}`), nil
	}
}

type acquireForUpdatePassiveReleaseTransport struct {
	mu           sync.Mutex
	acquireCalls int
	getCalls     int
	updateCalls  int
	releaseCalls int
}

func (t *acquireForUpdatePassiveReleaseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch req.URL.Path {
	case "/v1/acquire":
		t.acquireCalls++
		leaseID := fmt.Sprintf("L-passive-%d", t.acquireCalls)
		body := fmt.Sprintf(`{"lease_id":"%s","key":"orders","owner":"reader","expires_at_unix":%d,"version":1,"fencing_token":%d}`,
			leaseID, time.Now().Add(time.Minute).Unix(), 90+t.acquireCalls)
		resp := newJSONResponse(req, http.StatusOK, body)
		resp.Header.Set("X-Correlation-Id", fmt.Sprintf("cid-passive-%d", t.acquireCalls))
		return resp, nil
	case "/v1/get":
		t.getCalls++
		resp := newJSONResponse(req, http.StatusOK, `{"value":1}`)
		resp.Header.Set("ETag", "etag-passive")
		resp.Header.Set("X-Key-Version", "1")
		resp.Header.Set("X-Fencing-Token", "90")
		resp.Header.Set("Content-Length", strconv.Itoa(len(`{"value":1}`)))
		return resp, nil
	case "/v1/update":
		t.updateCalls++
		return newJSONResponse(req, http.StatusOK, `{"new_version":2,"new_state_etag":"etag-passive-updated","bytes":9}`), nil
	case "/v1/release":
		t.releaseCalls++
		if t.releaseCalls == 1 {
			return newJSONResponse(req, http.StatusServiceUnavailable, `{"error":"node_passive","detail":"server is passive","retry_after_seconds":1}`), nil
		}
		return newJSONResponse(req, http.StatusOK, `{"released":true}`), nil
	default:
		return newJSONResponse(req, http.StatusNotFound, `{"error":"not_found"}`), nil
	}
}

func TestAcquireForUpdateRetriesOnLeaseRequired(t *testing.T) {
	transport := &acquireForUpdateRetryTransport{}
	httpClient := &http.Client{Transport: transport}

	cli, err := client.NewWithEndpoints([]string{"http://retry:9341"},
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
		client.WithHTTPTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var updated bool
	err = cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "reader",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	}, func(handlerCtx context.Context, af *client.AcquireForUpdateContext) error {
		if af.State == nil || af.State.Reader == nil {
			return fmt.Errorf("missing state reader")
		}
		if _, err := af.State.Bytes(); err != nil {
			return err
		}
		if _, err := af.UpdateBytes(handlerCtx, []byte(`{"value":2}`)); err != nil {
			return err
		}
		updated = true
		return nil
	}, client.WithAcquireFailureRetries(3), client.WithAcquireBackoff(5*time.Millisecond, 5*time.Millisecond, 1.0), client.WithAcquireJitter(0))
	if err != nil {
		t.Fatalf("acquire-for-update: %v", err)
	}
	if !updated {
		t.Fatal("expected handler to apply update")
	}

	transport.mu.Lock()
	acquireCalls := transport.acquireCalls
	getCalls := transport.getCalls
	updateCalls := transport.updateCalls
	releaseCalls := transport.releaseCalls
	transport.mu.Unlock()

	if acquireCalls < 2 {
		t.Fatalf("expected at least two acquire attempts, got %d", acquireCalls)
	}
	if getCalls < 2 {
		t.Fatalf("expected get retries, got %d", getCalls)
	}
	if updateCalls != 1 {
		t.Fatalf("expected exactly one update, got %d", updateCalls)
	}
	if releaseCalls == 0 {
		t.Fatal("expected release to be invoked")
	}
}

func TestAcquireForUpdateRetriesReleaseOnNodePassive(t *testing.T) {
	transport := &acquireForUpdatePassiveReleaseTransport{}
	httpClient := &http.Client{Transport: transport}
	cli, err := client.NewWithEndpoints([]string{"http://hosta:9341"},
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "reader",
		TTLSeconds: 5,
	}, func(handlerCtx context.Context, af *client.AcquireForUpdateContext) error {
		return af.Save(handlerCtx, map[string]any{"value": 2})
	}, client.WithAcquireFailureRetries(3))
	if err != nil {
		t.Fatalf("acquire-for-update: %v", err)
	}
	transport.mu.Lock()
	releaseCalls := transport.releaseCalls
	transport.mu.Unlock()
	if releaseCalls < 2 {
		t.Fatalf("expected release retry after node_passive, got %d calls", releaseCalls)
	}
}

func TestAcquireForUpdateHandlerError(t *testing.T) {
	ts := lockd.StartTestServer(t,
		lockd.WithTestUnixSocket(filepath.Join(t.TempDir(), "handler-error.sock")),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        "handler-error",
		Owner:      "tester",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	}, func(handlerCtx context.Context, af *client.AcquireForUpdateContext) error {
		return fmt.Errorf("handler-failure")
	})
	if err == nil || !strings.Contains(err.Error(), "handler-failure") {
		t.Fatalf("expected handler error to propagate, got %v", err)
	}
}

func TestAcquireForUpdateRetriesAfterHandlerEOF(t *testing.T) {
	ts := lockd.StartTestServer(t,
		lockd.WithTestUnixSocket(filepath.Join(t.TempDir(), "handler-eof.sock")),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var attempts int32
	err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        "handler-eof",
		Owner:      "tester",
		TTLSeconds: 5,
		BlockSecs:  client.BlockNoWait,
	}, func(handlerCtx context.Context, af *client.AcquireForUpdateContext) error {
		if af.State != nil {
			if _, err := af.State.Bytes(); err != nil {
				return err
			}
		}
		if atomic.AddInt32(&attempts, 1) == 1 {
			return io.ErrUnexpectedEOF
		}
		doc := map[string]any{"attempt": attempts}
		if err := af.Save(handlerCtx, doc); err != nil {
			return err
		}
		return nil
	}, client.WithAcquireFailureRetries(3), client.WithAcquireBackoff(5*time.Millisecond, 5*time.Millisecond, 1.0), client.WithAcquireJitter(0))
	if err != nil {
		t.Fatalf("acquire-for-update: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected handler to retry once, got %d attempts", attempts)
	}
}

func TestAcquireForUpdateRetriesAcquireError(t *testing.T) {
	transport := &acquireForUpdateAcquireErrorTransport{}
	httpClient := &http.Client{Transport: transport}

	cli, err := client.NewWithEndpoints([]string{"http://acquire-retry:9341"},
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
		client.WithHTTPTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var updated bool
	err = cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "reader",
		TTLSeconds: 5,
		BlockSecs:  client.BlockNoWait,
	}, func(handlerCtx context.Context, af *client.AcquireForUpdateContext) error {
		if af.State == nil || af.State.Reader == nil {
			return fmt.Errorf("expected snapshot state")
		}
		if _, err := af.State.Bytes(); err != nil {
			return err
		}
		if err := af.Save(handlerCtx, map[string]any{"value": 2}); err != nil {
			return err
		}
		updated = true
		return nil
	}, client.WithAcquireFailureRetries(3), client.WithAcquireBackoff(5*time.Millisecond, 5*time.Millisecond, 1.0), client.WithAcquireJitter(0))
	if err != nil {
		t.Fatalf("acquire-for-update: %v", err)
	}

	transport.mu.Lock()
	acquireCalls := transport.acquireCalls
	getCalls := transport.getCalls
	updateCalls := transport.updateCalls
	releaseCalls := transport.releaseCalls
	transport.mu.Unlock()

	if acquireCalls < 2 {
		t.Fatalf("expected acquire retries, got %d", acquireCalls)
	}
	if getCalls != 1 {
		t.Fatalf("expected single get, got %d", getCalls)
	}
	if updateCalls != 1 {
		t.Fatalf("expected single update, got %d", updateCalls)
	}
	if releaseCalls != 1 {
		t.Fatalf("expected release, got %d", releaseCalls)
	}
	if !updated {
		t.Fatalf("expected handler to update state")
	}
}

func TestTestServerChaosProxy(t *testing.T) {
	chaos := &lockd.ChaosConfig{
		Seed:                    42,
		MinDelay:                time.Millisecond,
		MaxDelay:                3 * time.Millisecond,
		BandwidthBytesPerSecond: 0,
	}
	ts := lockd.StartTestServer(t,
		lockd.WithTestChaos(chaos),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	serverAddr := ts.Server.ListenerAddr().String()
	proxyAddr := ts.Addr().String()
	if serverAddr == proxyAddr {
		t.Fatalf("expected proxy address to differ from server address (%s)", serverAddr)
	}

	cli := ts.Client
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "chaos-key",
		Owner:      "tester",
		TTLSeconds: 30,
		BlockSecs:  client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	payload := map[string]string{"pad": strings.Repeat("x", 1024)}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if _, err := lease.UpdateBytes(ctx, data); err != nil {
		t.Fatalf("update: %v", err)
	}

	readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer readCancel()
	start := time.Now()
	readData, err := lease.GetBytes(readCtx)
	if err != nil {
		t.Fatalf("get bytes: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 3*time.Millisecond {
		t.Fatalf("expected chaos-induced latency, got %s", elapsed)
	}
	if len(readData) == 0 {
		t.Fatalf("expected payload, got empty slice")
	}

	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

type testQueueMessage struct {
	Namespace                string         `json:"namespace,omitempty"`
	Queue                    string         `json:"queue"`
	MessageID                string         `json:"message_id"`
	Attempts                 int            `json:"attempts"`
	MaxAttempts              int            `json:"max_attempts"`
	NotVisibleUntilUnix      int64          `json:"not_visible_until_unix"`
	VisibilityTimeoutSeconds int64          `json:"visibility_timeout_seconds"`
	Attributes               map[string]any `json:"attributes,omitempty"`
	PayloadContentType       string         `json:"payload_content_type"`
	PayloadBytes             int64          `json:"payload_bytes"`
	LeaseID                  string         `json:"lease_id"`
	LeaseExpiresAtUnix       int64          `json:"lease_expires_at_unix"`
	FencingToken             int64          `json:"fencing_token"`
	MetaETag                 string         `json:"meta_etag"`
	StateETag                string         `json:"state_etag,omitempty"`
	StateLeaseID             string         `json:"state_lease_id,omitempty"`
	StateFencingToken        int64          `json:"state_fencing_token,omitempty"`
	StateLeaseExpiresAtUnix  int64          `json:"state_lease_expires_at_unix,omitempty"`
	StateTxnID               string         `json:"state_txn_id,omitempty"`
	CorrelationID            string         `json:"correlation_id,omitempty"`
}

func TestClientEnqueue(t *testing.T) {
	var capturedMeta map[string]any
	var capturedPayload []byte
	var capturedPayloadType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/enqueue":
			mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
				t.Fatalf("parse multipart: %v", err)
			}
			boundary := params["boundary"]
			if boundary == "" {
				t.Fatalf("missing boundary")
			}
			mr := multipart.NewReader(r.Body, boundary)
			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("next part: %v", err)
				}
				switch part.FormName() {
				case "meta":
					if err := json.NewDecoder(part).Decode(&capturedMeta); err != nil {
						part.Close()
						t.Fatalf("decode meta: %v", err)
					}
				case "payload":
					data, err := io.ReadAll(part)
					if err != nil {
						part.Close()
						t.Fatalf("read payload: %v", err)
					}
					capturedPayload = data
					capturedPayloadType = part.Header.Get("Content-Type")
				}
				part.Close()
			}
			corr := "cid-demo"
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Correlation-Id", corr)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"queue":                      capturedMeta["queue"],
				"message_id":                 "msg-1",
				"attempts":                   0,
				"max_attempts":               5,
				"not_visible_until_unix":     time.Now().Unix(),
				"visibility_timeout_seconds": 30,
				"payload_bytes":              3,
				"correlation_id":             corr,
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	payload := []byte("hey")
	opts := client.EnqueueOptions{
		Delay:       2 * time.Second,
		Visibility:  5 * time.Second,
		TTL:         10 * time.Second,
		MaxAttempts: 7,
		Attributes:  map[string]any{"type": "demo"},
		ContentType: "text/plain",
	}
	res, err := cli.EnqueueBytes(context.Background(), "demo-queue", payload, opts)
	if err != nil {
		t.Fatalf("queue enqueue: %v", err)
	}
	if res == nil || res.MessageID != "msg-1" {
		t.Fatalf("unexpected enqueue response %#v", res)
	}
	if res.CorrelationID != "cid-demo" {
		t.Fatalf("unexpected correlation id %q", res.CorrelationID)
	}
	if capturedMeta == nil {
		t.Fatalf("expected request meta part")
	}
	if capturedMeta["queue"].(string) != "demo-queue" {
		t.Fatalf("unexpected queue %v", capturedMeta["queue"])
	}
	if capturedMeta["delay_seconds"].(float64) != 2 || capturedMeta["visibility_timeout_seconds"].(float64) != 5 || capturedMeta["ttl_seconds"].(float64) != 10 {
		t.Fatalf("unexpected timing values: %+v", capturedMeta)
	}
	if capturedMeta["max_attempts"].(float64) != 7 {
		t.Fatalf("unexpected max attempts: %+v", capturedMeta)
	}
	if capturedMeta["payload_content_type"].(string) != "text/plain" {
		t.Fatalf("unexpected content type: %v", capturedMeta["payload_content_type"])
	}
	if capturedPayloadType != "text/plain" {
		t.Fatalf("unexpected payload part type %q", capturedPayloadType)
	}
	if !bytes.Equal(capturedPayload, payload) {
		t.Fatalf("unexpected payload bytes %q", capturedPayload)
	}
}

func TestClientTxnCommit(t *testing.T) {
	var got api.TxnDecisionRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/txn/decide" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.TxnDecisionResponse{TxnID: got.TxnID, State: got.State})
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	req := api.TxnDecisionRequest{
		TxnID: "abc123",
		Participants: []api.TxnParticipant{
			{Namespace: "n1", Key: "k1"},
			{Namespace: "n2", Key: "k2"},
		},
	}
	resp, err := cli.TxnCommit(context.Background(), req)
	if err != nil {
		t.Fatalf("txn commit: %v", err)
	}
	if resp == nil || resp.TxnID != "abc123" || resp.State != "commit" {
		t.Fatalf("unexpected resp %+v", resp)
	}
	if got.State != "commit" || got.TxnID != "abc123" {
		t.Fatalf("request not populated: %+v", got)
	}
	if len(got.Participants) != 2 {
		t.Fatalf("expected 2 participants, got %d", len(got.Participants))
	}
}

type txnCommitRetryTransport struct {
	mu    sync.Mutex
	calls int
}

func (t *txnCommitRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || req.URL.Path != "/v1/txn/decide" {
		return newJSONResponse(req, http.StatusNotFound, `{"error":"not_found"}`), nil
	}
	t.mu.Lock()
	t.calls++
	call := t.calls
	t.mu.Unlock()
	if call == 1 {
		return nil, &url.Error{
			Op:  "Post",
			URL: req.URL.String(),
			Err: io.ErrUnexpectedEOF,
		}
	}
	return newJSONResponse(req, http.StatusOK, `{"txn_id":"abc123","state":"commit"}`), nil
}

func TestClientTxnCommitRetriesRetryableTransportError(t *testing.T) {
	transport := &txnCommitRetryTransport{}
	httpClient := &http.Client{Transport: transport}
	cli, err := client.NewWithEndpoints([]string{"http://retry:9341"},
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
		client.WithEndpointShuffle(false),
		client.WithFailureRetries(2),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	resp, err := cli.TxnCommit(context.Background(), api.TxnDecisionRequest{TxnID: "abc123"})
	if err != nil {
		t.Fatalf("txn commit: %v", err)
	}
	if resp == nil || resp.TxnID != "abc123" || resp.State != "commit" {
		t.Fatalf("unexpected response %+v", resp)
	}
	transport.mu.Lock()
	calls := transport.calls
	transport.mu.Unlock()
	if calls < 2 {
		t.Fatalf("expected retry after transport error, calls=%d", calls)
	}
}

type txnCommitFatalTransport struct {
	mu    sync.Mutex
	calls int
}

func (t *txnCommitFatalTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || req.URL.Path != "/v1/txn/decide" {
		return newJSONResponse(req, http.StatusNotFound, `{"error":"not_found"}`), nil
	}
	t.mu.Lock()
	t.calls++
	t.mu.Unlock()
	return nil, &url.Error{
		Op:  "Post",
		URL: req.URL.String(),
		Err: x509.UnknownAuthorityError{},
	}
}

func TestClientTxnCommitDoesNotRetryFatalTransportError(t *testing.T) {
	transport := &txnCommitFatalTransport{}
	httpClient := &http.Client{Transport: transport}
	cli, err := client.NewWithEndpoints([]string{"http://retry:9341"},
		client.WithDisableMTLS(true),
		client.WithHTTPClient(httpClient),
		client.WithEndpointShuffle(false),
		client.WithFailureRetries(5),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = cli.TxnCommit(context.Background(), api.TxnDecisionRequest{TxnID: "abc123"})
	if err == nil {
		t.Fatal("expected fatal transport error")
	}
	transport.mu.Lock()
	calls := transport.calls
	transport.mu.Unlock()
	if calls != 1 {
		t.Fatalf("fatal error must not retry, calls=%d", calls)
	}
}

func TestClientTxnReplay(t *testing.T) {
	var got api.TxnReplayRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/txn/replay" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.TxnReplayResponse{TxnID: got.TxnID, State: "commit"})
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	resp, err := cli.TxnReplay(context.Background(), "abc123")
	if err != nil {
		t.Fatalf("txn replay: %v", err)
	}
	if resp == nil || resp.TxnID != "abc123" || resp.State != "commit" {
		t.Fatalf("unexpected resp %+v", resp)
	}
	if got.TxnID != "abc123" {
		t.Fatalf("expected txn_id abc123, got %s", got.TxnID)
	}
}

func TestClientDequeueHandlesLifecycle(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Queue:                    "orders",
		MessageID:                "msg-42",
		Attempts:                 1,
		MaxAttempts:              5,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 15,
		PayloadContentType:       "application/octet-stream",
		PayloadBytes:             4,
		LeaseID:                  "lease-abc",
		LeaseExpiresAtUnix:       now + 30,
		FencingToken:             9,
		MetaETag:                 "meta-v1",
	}
	payloadBody := []byte("data")
	var extendReq map[string]any
	var nackReq map[string]any
	var ackReq map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/dequeue":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message":     message,
				"next_cursor": "cursor-1",
			}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write(payloadBody); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/extend":
			if err := json.NewDecoder(r.Body).Decode(&extendReq); err != nil {
				t.Fatalf("decode extend: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"lease_expires_at_unix":      now + 60,
				"visibility_timeout_seconds": 33,
				"meta_etag":                  "meta-extended",
			})
		case "/v1/queue/nack":
			if err := json.NewDecoder(r.Body).Decode(&nackReq); err != nil {
				t.Fatalf("decode nack: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"requeued": true, "meta_etag": "meta-requeued"})
		case "/v1/queue/ack":
			if err := json.NewDecoder(r.Body).Decode(&ackReq); err != nil {
				t.Fatalf("decode ack: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	msg, err := cli.Dequeue(context.Background(), "orders", client.DequeueOptions{Owner: "worker-1"})
	if err != nil {
		t.Fatalf("queue dequeue: %v", err)
	}
	defer msg.Close()
	buf, err := io.ReadAll(msg)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(buf) != string(payloadBody) {
		t.Fatalf("unexpected payload %q", buf)
	}

	if err := msg.Extend(context.Background(), 20*time.Second); err != nil {
		t.Fatalf("extend: %v", err)
	}
	if extendReq["queue"].(string) != "orders" {
		t.Fatalf("unexpected extend request: %+v", extendReq)
	}
	if msg.VisibilityTimeout() != 33*time.Second {
		t.Fatalf("visibility not updated")
	}

	if err := msg.Nack(context.Background(), 5*time.Second, map[string]any{"reason": "retry"}); err != nil {
		t.Fatalf("nack: %v", err)
	}
	if nackReq["delay_seconds"].(float64) != 5 {
		t.Fatalf("unexpected nack delay: %+v", nackReq)
	}
	if nackReq["intent"] != string(api.NackIntentFailure) {
		t.Fatalf("unexpected nack intent: %+v", nackReq)
	}
	if err := msg.Ack(context.Background()); err == nil {
		t.Fatalf("expected ack after nack to fail")
	}

	msg.Close()

	msg, err = cli.Dequeue(context.Background(), "orders", client.DequeueOptions{Owner: "worker-1"})
	if err != nil {
		t.Fatalf("queue dequeue second: %v", err)
	}
	defer msg.Close()
	if err := msg.Ack(context.Background()); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if ackReq["queue"].(string) != "orders" || ackReq["message_id"].(string) != "msg-42" {
		t.Fatalf("unexpected ack request: %+v", ackReq)
	}
	if err := msg.Ack(context.Background()); err == nil {
		t.Fatalf("expected double ack error")
	}
}

func TestQueueMessageDeferUsesDeferIntent(t *testing.T) {
	t.Parallel()

	var nackReq map[string]any
	payloadBody := []byte("payload")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/dequeue":
			w.Header().Set("Content-Type", "multipart/form-data; boundary=zzz")
			mw := multipart.NewWriter(w)
			if err := mw.SetBoundary("zzz"); err != nil {
				t.Fatalf("boundary: %v", err)
			}
			metaPart, _ := mw.CreatePart(textproto.MIMEHeader{
				"Content-Disposition": []string{`form-data; name="meta"`},
				"Content-Type":        []string{"application/json"},
			})
			meta := api.DequeueResponse{Message: &api.Message{
				Namespace:                "default",
				Queue:                    "orders",
				MessageID:                "msg-99",
				Attempts:                 1,
				MaxAttempts:              5,
				NotVisibleUntilUnix:      time.Now().Add(30 * time.Second).Unix(),
				VisibilityTimeoutSeconds: 30,
				LeaseID:                  "lease-99",
				LeaseExpiresAtUnix:       time.Now().Add(30 * time.Second).Unix(),
				FencingToken:             9,
				TxnID:                    "txn-99",
				MetaETag:                 "meta-99",
				PayloadBytes:             int64(len(payloadBody)),
			}}
			if err := json.NewEncoder(metaPart).Encode(meta); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadPart, _ := mw.CreatePart(textproto.MIMEHeader{
				"Content-Disposition": []string{`form-data; name="payload"`},
				"Content-Type":        []string{"application/octet-stream"},
			})
			if _, err := payloadPart.Write(payloadBody); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/nack":
			if err := json.NewDecoder(r.Body).Decode(&nackReq); err != nil {
				t.Fatalf("decode nack: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"requeued": true, "meta_etag": "meta-deferred"})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	msg, err := cli.Dequeue(context.Background(), "orders", client.DequeueOptions{Owner: "worker-1"})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if err := msg.Defer(context.Background(), 2*time.Second); err != nil {
		t.Fatalf("defer: %v", err)
	}
	if nackReq["intent"] != string(api.NackIntentDefer) {
		t.Fatalf("unexpected defer intent payload: %+v", nackReq)
	}
}

func TestClientUpdateWithMetadataHeader(t *testing.T) {
	t.Parallel()

	var (
		capturedHeader string
		capturedLease  string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/update" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		capturedHeader = r.Header.Get("X-Lockd-Meta-Query-Hidden")
		capturedLease = r.Header.Get("X-Lease-ID")
		hidden := true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.UpdateResponse{
			NewVersion:   2,
			NewStateETag: "etag-2",
			Metadata:     api.MetadataAttributes{QueryHidden: &hidden},
		})
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL,
		client.WithDisableMTLS(true),
		client.WithEndpointShuffle(false),
		client.WithHTTPClient(srv.Client()),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	opts := client.UpdateOptions{
		FencingToken: client.Int64(9),
		Metadata:     client.MetadataOptions{QueryHidden: client.Bool(true)},
	}
	res, err := cli.Update(context.Background(), "orders", "lease-1", bytes.NewReader([]byte(`{"status":"open"}`)), opts)
	if err != nil {
		t.Fatalf("update call: %v", err)
	}
	if capturedHeader != "true" {
		t.Fatalf("expected metadata header true, got %q", capturedHeader)
	}
	if capturedLease != "lease-1" {
		t.Fatalf("expected lease header, got %q", capturedLease)
	}
	if res.Metadata.QueryHidden == nil || !*res.Metadata.QueryHidden {
		t.Fatalf("expected metadata in response, got %+v", res.Metadata)
	}
}

func TestClientUpdateMetadataSendsJSONPayload(t *testing.T) {
	t.Parallel()

	var (
		capturedBody      []byte
		capturedIfVersion string
		capturedToken     string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/metadata" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		capturedIfVersion = r.Header.Get("X-If-Version")
		capturedToken = r.Header.Get("X-Fencing-Token")
		hidden := true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.MetadataUpdateResponse{
			Namespace: "default",
			Key:       "orders",
			Version:   3,
			Metadata:  api.MetadataAttributes{QueryHidden: &hidden},
		})
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL,
		client.WithDisableMTLS(true),
		client.WithEndpointShuffle(false),
		client.WithHTTPClient(srv.Client()),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	opts := client.UpdateOptions{
		FencingToken: client.Int64(1),
		IfVersion:    "3",
		Metadata:     client.MetadataOptions{QueryHidden: client.Bool(true)},
	}
	res, err := cli.UpdateMetadata(context.Background(), "orders", "lease-7", opts)
	if err != nil {
		t.Fatalf("update metadata: %v", err)
	}
	if capturedIfVersion != "3" {
		t.Fatalf("expected X-If-Version header, got %q", capturedIfVersion)
	}
	if capturedToken != "1" {
		t.Fatalf("expected fencing token header, got %q", capturedToken)
	}
	var payload map[string]any
	if err := json.Unmarshal(capturedBody, &payload); err != nil {
		t.Fatalf("decode metadata payload: %v", err)
	}
	val, ok := payload["query_hidden"].(bool)
	if !ok || !val {
		t.Fatalf("expected query_hidden=true payload, got %+v", payload)
	}
	if res.Metadata.QueryHidden == nil || !*res.Metadata.QueryHidden {
		t.Fatalf("expected metadata in response, got %+v", res.Metadata)
	}
}

func TestClientUpdateMetadataRequiresFields(t *testing.T) {
	t.Parallel()

	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL,
		client.WithDisableMTLS(true),
		client.WithEndpointShuffle(false),
		client.WithHTTPClient(srv.Client()),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = cli.UpdateMetadata(context.Background(), "orders", "lease-9", client.UpdateOptions{})
	if err == nil {
		t.Fatal("expected error for empty metadata options")
	}
	if called {
		t.Fatal("server should not have been called when metadata options missing")
	}
}

func TestClientDequeueWithState(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Queue:                    "workflow",
		MessageID:                "msg-stateful",
		Attempts:                 2,
		MaxAttempts:              6,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 10,
		PayloadContentType:       "application/json",
		PayloadBytes:             9,
		LeaseID:                  "lease-msg",
		LeaseExpiresAtUnix:       now + 20,
		FencingToken:             11,
		MetaETag:                 "meta-stateful",
		StateETag:                "state-etag",
		StateLeaseID:             "lease-state",
		StateFencingToken:        17,
		StateLeaseExpiresAtUnix:  now + 20,
	}
	payloadBody := []byte("workflow")
	var extendReq map[string]any
	var ackReq map[string]any

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/dequeueWithState":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{"message": message}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write(payloadBody); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/extend":
			if err := json.NewDecoder(r.Body).Decode(&extendReq); err != nil {
				t.Fatalf("decode extend: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"lease_expires_at_unix":       now + 50,
				"visibility_timeout_seconds":  40,
				"meta_etag":                   "meta-state-extended",
				"state_lease_expires_at_unix": now + 55,
			})
		case "/v1/queue/ack":
			if err := json.NewDecoder(r.Body).Decode(&ackReq); err != nil {
				t.Fatalf("decode ack: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	msg, err := cli.DequeueWithState(context.Background(), "workflow", client.DequeueOptions{Owner: "worker-2"})
	if err != nil {
		t.Fatalf("dequeue stateful: %v", err)
	}
	defer msg.Close()
	if msg == nil || msg.StateHandle() == nil {
		t.Fatalf("expected state handle")
	}
	buf, err := io.ReadAll(msg)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(buf) != string(payloadBody) {
		t.Fatalf("unexpected payload %q", buf)
	}

	if err := msg.Extend(context.Background(), 0); err != nil {
		t.Fatalf("extend stateful: %v", err)
	}
	if extendReq["state_lease_id"].(string) != "lease-state" {
		t.Fatalf("state lease extend missing: %+v", extendReq)
	}
	if msg.StateHandle().LeaseExpiresAt() != now+55 {
		t.Fatalf("state lease expiry not updated")
	}
	if err := msg.Ack(context.Background()); err != nil {
		t.Fatalf("ack stateful: %v", err)
	}
	if ackReq["state_lease_id"].(string) != "lease-state" {
		t.Fatalf("state lease not acknowledged: %+v", ackReq)
	}
}

func TestQueueStateHandleHelpers(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Namespace:                "default",
		Queue:                    "workflow",
		MessageID:                "msg-stateful-helpers",
		Attempts:                 1,
		MaxAttempts:              4,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 10,
		PayloadContentType:       "application/json",
		PayloadBytes:             2,
		LeaseID:                  "lease-msg",
		LeaseExpiresAtUnix:       now + 20,
		FencingToken:             11,
		MetaETag:                 "meta-stateful",
		StateETag:                "etag-initial",
		StateLeaseID:             "lease-state",
		StateFencingToken:        17,
		StateLeaseExpiresAtUnix:  now + 20,
		StateTxnID:               "state-txn-1",
		CorrelationID:            "cid-state-handle",
	}
	const expectedKey = "q/workflow/state/msg-stateful-helpers"

	type observed struct {
		Key           string
		Namespace     string
		LeaseID       string
		TxnID         string
		IfETag        string
		IfVersion     string
		FencingToken  string
		CorrelationID string
		Body          map[string]any
	}

	var (
		mu          sync.Mutex
		getReq      observed
		updateReq   observed
		metadataReq observed
		removeReq   observed
		nackReq     api.NackRequest
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record := func() observed {
			return observed{
				Key:           r.URL.Query().Get("key"),
				Namespace:     r.URL.Query().Get("namespace"),
				LeaseID:       r.Header.Get("X-Lease-ID"),
				TxnID:         r.Header.Get("X-Txn-ID"),
				IfETag:        r.Header.Get("X-If-State-ETag"),
				IfVersion:     r.Header.Get("X-If-Version"),
				FencingToken:  r.Header.Get("X-Fencing-Token"),
				CorrelationID: r.Header.Get("X-Correlation-Id"),
			}
		}

		switch r.URL.Path {
		case "/v1/queue/dequeueWithState":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{"message": message}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("{}")); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/get":
			obs := record()
			mu.Lock()
			getReq = obs
			mu.Unlock()
			w.Header().Set("ETag", `"etag-after-get"`)
			w.Header().Set("X-Key-Version", "8")
			w.Header().Set("X-Fencing-Token", "19")
			w.Header().Set("X-Correlation-Id", "cid-state-handle-get")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"step":"one"}`)
		case "/v1/update":
			obs := record()
			if err := json.NewDecoder(r.Body).Decode(&obs.Body); err != nil {
				t.Fatalf("decode update body: %v", err)
			}
			mu.Lock()
			updateReq = obs
			mu.Unlock()
			w.Header().Set("X-Fencing-Token", "21")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"new_version":9,"new_state_etag":"etag-after-update","bytes":14}`)
		case "/v1/metadata":
			obs := record()
			if err := json.NewDecoder(r.Body).Decode(&obs.Body); err != nil {
				t.Fatalf("decode metadata body: %v", err)
			}
			mu.Lock()
			metadataReq = obs
			mu.Unlock()
			w.Header().Set("X-Fencing-Token", "22")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"version":10,"metadata":{"query_hidden":true}}`)
		case "/v1/remove":
			obs := record()
			mu.Lock()
			removeReq = obs
			mu.Unlock()
			w.Header().Set("X-Fencing-Token", "23")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"removed":true,"new_version":11}`)
		case "/v1/queue/nack":
			var req api.NackRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode nack: %v", err)
			}
			mu.Lock()
			nackReq = req
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"nacked":true}`)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	msg, err := cli.DequeueWithState(context.Background(), "workflow", client.DequeueOptions{
		Namespace: "default",
		Owner:     "worker-helpers",
	})
	if err != nil {
		t.Fatalf("dequeue with state: %v", err)
	}
	state := msg.StateHandle()
	if state == nil {
		t.Fatalf("expected state handle")
	}
	if state.ETag() != "" {
		t.Fatalf("expected empty initial workflow etag, got %q", state.ETag())
	}

	var loaded struct {
		Step string `json:"step"`
	}
	if err := state.Load(context.Background(), &loaded); err != nil {
		t.Fatalf("state load: %v", err)
	}
	if loaded.Step != "one" {
		t.Fatalf("unexpected loaded state: %+v", loaded)
	}
	if state.ETag() != "etag-after-get" {
		t.Fatalf("expected refreshed etag, got %q", state.ETag())
	}

	if err := state.Save(context.Background(), map[string]any{"step": "two"}); err != nil {
		t.Fatalf("state save: %v", err)
	}
	if state.ETag() != "etag-after-update" {
		t.Fatalf("expected updated etag, got %q", state.ETag())
	}

	metaRes, err := state.UpdateMetadata(context.Background(), client.MetadataOptions{QueryHidden: client.Bool(true)})
	if err != nil {
		t.Fatalf("state update metadata: %v", err)
	}
	if metaRes == nil || metaRes.Version != 10 {
		t.Fatalf("unexpected metadata response: %+v", metaRes)
	}

	removeRes, err := state.Remove(context.Background())
	if err != nil {
		t.Fatalf("state remove: %v", err)
	}
	if removeRes == nil || !removeRes.Removed || removeRes.NewVersion != 11 {
		t.Fatalf("unexpected remove response: %+v", removeRes)
	}
	if state.ETag() != "" {
		t.Fatalf("expected etag cleared after remove, got %q", state.ETag())
	}
	if err := msg.Close(); err != nil {
		t.Fatalf("close message: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if getReq.Key != expectedKey || getReq.Namespace != "default" {
		t.Fatalf("unexpected get query: %+v", getReq)
	}
	if getReq.LeaseID != "lease-state" || getReq.FencingToken != "17" || getReq.CorrelationID != "cid-state-handle" {
		t.Fatalf("unexpected get headers: %+v", getReq)
	}

	if updateReq.Key != expectedKey || updateReq.Namespace != "default" {
		t.Fatalf("unexpected update query: %+v", updateReq)
	}
	if updateReq.IfETag != "etag-after-get" || updateReq.IfVersion != "8" {
		t.Fatalf("unexpected update conditionals: %+v", updateReq)
	}
	if updateReq.FencingToken != "19" || updateReq.TxnID != "state-txn-1" {
		t.Fatalf("unexpected update lease/txn headers: %+v", updateReq)
	}
	if updateReq.CorrelationID != "cid-state-handle" {
		t.Fatalf("unexpected update correlation id: %+v", updateReq)
	}
	if step, ok := updateReq.Body["step"].(string); !ok || step != "two" {
		t.Fatalf("unexpected update payload: %+v", updateReq.Body)
	}

	if metadataReq.Key != expectedKey || metadataReq.Namespace != "default" {
		t.Fatalf("unexpected metadata query: %+v", metadataReq)
	}
	if metadataReq.IfVersion != "9" || metadataReq.FencingToken != "21" || metadataReq.TxnID != "state-txn-1" {
		t.Fatalf("unexpected metadata headers: %+v", metadataReq)
	}
	if metadataReq.CorrelationID != "cid-state-handle" {
		t.Fatalf("unexpected metadata correlation id: %+v", metadataReq)
	}
	if hidden, ok := metadataReq.Body["query_hidden"].(bool); !ok || !hidden {
		t.Fatalf("unexpected metadata payload: %+v", metadataReq.Body)
	}

	if removeReq.Key != expectedKey || removeReq.Namespace != "default" {
		t.Fatalf("unexpected remove query: %+v", removeReq)
	}
	if removeReq.IfETag != "etag-after-update" || removeReq.IfVersion != "9" {
		t.Fatalf("unexpected remove conditionals: %+v", removeReq)
	}
	if removeReq.FencingToken != "22" || removeReq.TxnID != "state-txn-1" {
		t.Fatalf("unexpected remove lease/txn headers: %+v", removeReq)
	}
	if removeReq.CorrelationID != "cid-state-handle" {
		t.Fatalf("unexpected remove correlation id: %+v", removeReq)
	}

	if nackReq.StateLeaseID != "lease-state" {
		t.Fatalf("expected state lease in nack: %+v", nackReq)
	}
}

func TestQueueStateHandleHelperOverrides(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Namespace:                "default",
		Queue:                    "workflow",
		MessageID:                "msg-stateful-overrides",
		Attempts:                 1,
		MaxAttempts:              4,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 10,
		PayloadContentType:       "application/json",
		PayloadBytes:             2,
		LeaseID:                  "lease-msg",
		LeaseExpiresAtUnix:       now + 20,
		FencingToken:             11,
		MetaETag:                 "meta-stateful",
		StateETag:                "etag-initial",
		StateLeaseID:             "lease-state",
		StateFencingToken:        17,
		StateLeaseExpiresAtUnix:  now + 20,
		StateTxnID:               "state-txn-1",
	}

	type observed struct {
		Key       string
		Namespace string
		TxnID     string
		IfETag    string
		IfVersion string
		Token     string
	}

	var (
		mu          sync.Mutex
		updateReq   observed
		removeReq   observed
		seenNack    bool
		seenDequeue bool
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/dequeueWithState":
			mu.Lock()
			seenDequeue = true
			mu.Unlock()
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{"message": message}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("{}")); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/update":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode update body: %v", err)
			}
			mu.Lock()
			updateReq = observed{
				Key:       r.URL.Query().Get("key"),
				Namespace: r.URL.Query().Get("namespace"),
				TxnID:     r.Header.Get("X-Txn-ID"),
				IfETag:    r.Header.Get("X-If-State-ETag"),
				IfVersion: r.Header.Get("X-If-Version"),
				Token:     r.Header.Get("X-Fencing-Token"),
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"new_version":2,"new_state_etag":"etag-override","bytes":2}`)
		case "/v1/remove":
			mu.Lock()
			removeReq = observed{
				Key:       r.URL.Query().Get("key"),
				Namespace: r.URL.Query().Get("namespace"),
				TxnID:     r.Header.Get("X-Txn-ID"),
				IfETag:    r.Header.Get("X-If-State-ETag"),
				IfVersion: r.Header.Get("X-If-Version"),
				Token:     r.Header.Get("X-Fencing-Token"),
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"removed":true,"new_version":3}`)
		case "/v1/queue/nack":
			mu.Lock()
			seenNack = true
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"nacked":true}`)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	msg, err := cli.DequeueWithState(context.Background(), "workflow", client.DequeueOptions{
		Namespace: "default",
		Owner:     "worker-overrides",
	})
	if err != nil {
		t.Fatalf("dequeue with state: %v", err)
	}
	state := msg.StateHandle()
	if state == nil {
		t.Fatalf("expected state handle")
	}

	if _, err := state.UpdateWithOptions(context.Background(), strings.NewReader(`{"ok":true}`), client.UpdateOptions{
		Namespace:    "override-ns",
		TxnID:        "override-update-txn",
		IfETag:       "override-etag",
		IfVersion:    "42",
		FencingToken: client.Int64(777),
	}); err != nil {
		t.Fatalf("update with options: %v", err)
	}

	if _, err := state.RemoveWithOptions(context.Background(), client.RemoveOptions{
		Namespace:    "override-ns",
		TxnID:        "override-remove-txn",
		IfETag:       "override-remove-etag",
		IfVersion:    "43",
		FencingToken: client.Int64(778),
	}); err != nil {
		t.Fatalf("remove with options: %v", err)
	}
	if err := msg.Close(); err != nil {
		t.Fatalf("close message: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !seenDequeue {
		t.Fatalf("expected dequeue with state request")
	}
	if !seenNack {
		t.Fatalf("expected close to nack message")
	}
	if updateReq.Key != "q/workflow/state/msg-stateful-overrides" || updateReq.Namespace != "override-ns" {
		t.Fatalf("unexpected update query: %+v", updateReq)
	}
	if updateReq.TxnID != "override-update-txn" || updateReq.IfETag != "override-etag" || updateReq.IfVersion != "42" || updateReq.Token != "777" {
		t.Fatalf("unexpected update override headers: %+v", updateReq)
	}
	if removeReq.Key != "q/workflow/state/msg-stateful-overrides" || removeReq.Namespace != "override-ns" {
		t.Fatalf("unexpected remove query: %+v", removeReq)
	}
	if removeReq.TxnID != "override-remove-txn" || removeReq.IfETag != "override-remove-etag" || removeReq.IfVersion != "43" || removeReq.Token != "778" {
		t.Fatalf("unexpected remove override headers: %+v", removeReq)
	}
}

func TestClientSubscribe(t *testing.T) {
	now := time.Now().Unix()
	messages := []testQueueMessage{
		{
			Queue:                    "orders",
			MessageID:                "msg-1",
			Attempts:                 1,
			MaxAttempts:              5,
			NotVisibleUntilUnix:      now,
			VisibilityTimeoutSeconds: 15,
			PayloadContentType:       "text/plain",
			PayloadBytes:             3,
			LeaseID:                  "lease-1",
			LeaseExpiresAtUnix:       now + 30,
			FencingToken:             1,
			MetaETag:                 "etag-1",
			CorrelationID:            "cid-1",
		},
		{
			Queue:                    "orders",
			MessageID:                "msg-2",
			Attempts:                 2,
			MaxAttempts:              5,
			NotVisibleUntilUnix:      now,
			VisibilityTimeoutSeconds: 15,
			PayloadContentType:       "text/plain",
			PayloadBytes:             3,
			LeaseID:                  "lease-2",
			LeaseExpiresAtUnix:       now + 30,
			FencingToken:             2,
			MetaETag:                 "etag-2",
			CorrelationID:            "cid-2",
		},
	}
	payloads := [][]byte{
		[]byte("one"),
		[]byte("two"),
	}

	var ackRequests []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)
			for i, msg := range messages {
				metaHeader := textproto.MIMEHeader{}
				metaHeader.Set("Content-Type", "application/json")
				metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
				metaPart, err := mw.CreatePart(metaHeader)
				if err != nil {
					t.Fatalf("create meta part: %v", err)
				}
				if err := json.NewEncoder(metaPart).Encode(map[string]any{
					"message":     msg,
					"next_cursor": fmt.Sprintf("cursor-%d", i+1),
				}); err != nil {
					t.Fatalf("encode meta: %v", err)
				}
				payloadHeader := textproto.MIMEHeader{}
				payloadHeader.Set("Content-Type", msg.PayloadContentType)
				payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
				payloadPart, err := mw.CreatePart(payloadHeader)
				if err != nil {
					t.Fatalf("create payload part: %v", err)
				}
				if _, err := payloadPart.Write(payloads[i]); err != nil {
					t.Fatalf("write payload: %v", err)
				}
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/ack":
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode ack: %v", err)
			}
			ackRequests = append(ackRequests, req)
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var seen []string
	if err := cli.Subscribe(context.Background(), "orders", client.SubscribeOptions{Owner: "worker-1", Prefetch: 2}, func(ctx context.Context, msg *client.QueueMessage) error {
		defer msg.Close()
		body, err := io.ReadAll(msg)
		if err != nil {
			return err
		}
		seen = append(seen, string(body))
		return msg.Ack(context.Background())
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if len(seen) != 2 || seen[0] != "one" || seen[1] != "two" {
		t.Fatalf("unexpected messages: %v", seen)
	}
	if len(ackRequests) != 2 {
		t.Fatalf("expected 2 ack requests, got %d", len(ackRequests))
	}
}

func TestClientSubscribeNamespaceOverride(t *testing.T) {
	var subscribeReq api.DequeueRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			if err := json.NewDecoder(r.Body).Decode(&subscribeReq); err != nil {
				t.Fatalf("decode subscribe request: %v", err)
			}
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message": map[string]any{
					"queue":                      "orders",
					"message_id":                 "msg-1",
					"attempts":                   1,
					"max_attempts":               3,
					"not_visible_until_unix":     time.Now().Unix(),
					"visibility_timeout_seconds": 10,
					"payload_content_type":       "application/json",
					"payload_bytes":              2,
					"lease_id":                   "lease-1",
					"lease_expires_at_unix":      time.Now().Add(10 * time.Second).Unix(),
				},
				"next_cursor": "cursor-1",
			}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", "application/json")
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("{}")); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/ack":
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(
		srv.URL,
		client.WithDisableMTLS(true),
		client.WithEndpointShuffle(false),
		client.WithDefaultNamespace("client-default"),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	err = cli.Subscribe(ctx, "orders", client.SubscribeOptions{
		Namespace: "subscribe-override",
		Owner:     "worker-1",
	}, func(_ context.Context, msg *client.QueueMessage) error {
		defer msg.Close()
		if ackErr := msg.Ack(context.Background()); ackErr != nil {
			return ackErr
		}
		cancel()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe: %v", err)
	}

	if subscribeReq.Namespace != "subscribe-override" {
		t.Fatalf("expected subscribe namespace override, got %q", subscribeReq.Namespace)
	}
}

func TestClientSubscribeWithState(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Queue:                    "workflow",
		MessageID:                "msg-state",
		Attempts:                 1,
		MaxAttempts:              5,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 20,
		PayloadContentType:       "application/json",
		PayloadBytes:             2,
		LeaseID:                  "lease-msg",
		LeaseExpiresAtUnix:       now + 40,
		FencingToken:             11,
		MetaETag:                 "meta",
		StateETag:                "state-etag",
		StateLeaseID:             "lease-state",
		StateLeaseExpiresAtUnix:  now + 40,
		StateFencingToken:        17,
		CorrelationID:            "cid-workflow",
	}

	var ackReq map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribeWithState":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)

			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message":     message,
				"next_cursor": "cursor-1",
			}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}

			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("{}")); err != nil {
				t.Fatalf("write payload: %v", err)
			}

			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/ack":
			if err := json.NewDecoder(r.Body).Decode(&ackReq); err != nil {
				t.Fatalf("decode ack: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if err := cli.SubscribeWithState(context.Background(), "workflow", client.SubscribeOptions{Owner: "worker-a"}, func(ctx context.Context, msg *client.QueueMessage, state *client.QueueStateHandle) error {
		defer msg.Close()
		if state == nil {
			return fmt.Errorf("expected state handle")
		}
		if state.LeaseID() != "lease-state" {
			return fmt.Errorf("unexpected state lease")
		}
		return msg.Ack(context.Background())
	}); err != nil {
		t.Fatalf("subscribe with state: %v", err)
	}

	if ackReq == nil {
		t.Fatalf("expected ack request")
	}
	if stateLease, ok := ackReq["state_lease_id"].(string); !ok || stateLease != "lease-state" {
		t.Fatalf("expected state lease in ack: %+v", ackReq)
	}
}

func TestClientSubscribeWithStateAutoExtendsDuringHandler(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Queue:                    "workflow",
		MessageID:                "msg-state-extend",
		Attempts:                 1,
		MaxAttempts:              5,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 1,
		PayloadContentType:       "application/json",
		PayloadBytes:             2,
		LeaseID:                  "lease-msg",
		LeaseExpiresAtUnix:       now + 5,
		FencingToken:             13,
		MetaETag:                 "meta-before",
		StateETag:                "state-etag",
		StateLeaseID:             "lease-state",
		StateLeaseExpiresAtUnix:  now + 5,
		StateFencingToken:        19,
	}

	var (
		mu         sync.Mutex
		extendReqs []api.ExtendRequest
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribeWithState":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)

			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message":     message,
				"next_cursor": "cursor-1",
			}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}

			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("{}")); err != nil {
				t.Fatalf("write payload: %v", err)
			}

			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/extend":
			var req api.ExtendRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode extend: %v", err)
			}
			mu.Lock()
			extendReqs = append(extendReqs, req)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"lease_expires_at_unix":       now + 30,
				"visibility_timeout_seconds":  1,
				"meta_etag":                   "meta-after",
				"state_lease_expires_at_unix": now + 30,
			})
		case "/v1/queue/ack":
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		case "/v1/queue/nack":
			_ = json.NewEncoder(w).Encode(map[string]any{"requeued": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.SubscribeWithState(ctx, "workflow", client.SubscribeOptions{Owner: "worker-a"}, func(handlerCtx context.Context, msg *client.QueueMessage, state *client.QueueStateHandle) error {
		defer msg.Close()
		if state == nil {
			return fmt.Errorf("expected state handle")
		}
		select {
		case <-time.After(1200 * time.Millisecond):
		case <-handlerCtx.Done():
			return handlerCtx.Err()
		}
		return msg.Ack(handlerCtx)
	}); err != nil {
		t.Fatalf("subscribe with state: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(extendReqs) == 0 {
		t.Fatalf("expected implicit queue extend during long-running handler")
	}
	if extendReqs[0].StateLeaseID != "lease-state" {
		t.Fatalf("expected state lease in extend request, got %+v", extendReqs[0])
	}
}

func TestClientStartConsumerValidation(t *testing.T) {
	var nilClient *client.Client
	if err := nilClient.StartConsumer(context.Background(), client.ConsumerConfig{}); err == nil || !strings.Contains(err.Error(), "client is nil") {
		t.Fatalf("expected nil client validation error, got %v", err)
	}

	cli, err := client.New("http://127.0.0.1", client.WithDisableMTLS(true))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var nilCtx context.Context
	if err := cli.StartConsumer(nilCtx, client.ConsumerConfig{}); err == nil || !strings.Contains(err.Error(), "context is required") {
		t.Fatalf("expected nil context validation error, got %v", err)
	}
	if err := cli.StartConsumer(context.Background()); err == nil || !strings.Contains(err.Error(), "at least one consumer config") {
		t.Fatalf("expected empty config validation error, got %v", err)
	}
	if err := cli.StartConsumer(context.Background(), []client.ConsumerConfig{{
		Queue:          "orders",
		Options:        client.SubscribeOptions{Owner: "worker-a"},
		MessageHandler: nil,
	}}...); err == nil || !strings.Contains(err.Error(), "message handler is required") {
		t.Fatalf("expected message handler validation error, got %v", err)
	}
	if err := cli.StartConsumer(context.Background(), []client.ConsumerConfig{{
		Queue:          " ",
		Options:        client.SubscribeOptions{Owner: "worker-a"},
		MessageHandler: func(context.Context, client.ConsumerMessage) error { return nil },
	}}...); err == nil || !strings.Contains(err.Error(), "queue is required") {
		t.Fatalf("expected queue validation error, got %v", err)
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cli.StartConsumer(canceledCtx, []client.ConsumerConfig{{
		Queue:          "orders",
		Options:        client.SubscribeOptions{},
		MessageHandler: func(context.Context, client.ConsumerMessage) error { return nil },
	}}...); err != nil {
		t.Fatalf("expected nil on canceled context with default owner, got %v", err)
	}
}

func TestClientStartConsumerDefaultsOwnerWhenEmpty(t *testing.T) {
	now := time.Now().Unix()
	var (
		mu           sync.Mutex
		owners       []string
		subscribeSeq int
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			var req api.DequeueRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode subscribe request: %v", err)
			}
			mu.Lock()
			owners = append(owners, req.Owner)
			subscribeSeq++
			seq := subscribeSeq
			mu.Unlock()

			msg := testQueueMessage{
				Namespace:                "default",
				Queue:                    req.Queue,
				MessageID:                fmt.Sprintf("msg-%d", seq),
				Attempts:                 1,
				MaxAttempts:              5,
				NotVisibleUntilUnix:      now,
				VisibilityTimeoutSeconds: 10,
				PayloadContentType:       "application/json",
				PayloadBytes:             2,
				LeaseID:                  fmt.Sprintf("lease-%d", seq),
				LeaseExpiresAtUnix:       now + 30,
				FencingToken:             int64(seq),
				MetaETag:                 fmt.Sprintf("etag-%d", seq),
			}

			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)

			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message":     msg,
				"next_cursor": fmt.Sprintf("cursor-%d", seq),
			}); err != nil {
				t.Fatalf("encode subscribe meta: %v", err)
			}

			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", "application/json")
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("{}")); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/ack":
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var handled atomic.Int32
	err = cli.StartConsumer(runCtx,
		client.ConsumerConfig{
			Queue: "orders",
			Options: client.SubscribeOptions{
				Owner: "",
			},
			MessageHandler: func(ctx context.Context, cm client.ConsumerMessage) error {
				defer cm.Message.Close()
				if err := cm.Message.Ack(ctx); err != nil {
					return err
				}
				if handled.Add(1) == 2 {
					cancel()
				}
				return nil
			},
		},
		client.ConsumerConfig{
			Queue: "orders",
			Options: client.SubscribeOptions{
				Owner: "",
			},
			MessageHandler: func(ctx context.Context, cm client.ConsumerMessage) error {
				defer cm.Message.Close()
				if err := cm.Message.Ack(ctx); err != nil {
					return err
				}
				if handled.Add(1) == 2 {
					cancel()
				}
				return nil
			},
		},
	)
	if err != nil {
		t.Fatalf("expected nil shutdown, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(owners) < 2 {
		t.Fatalf("expected at least 2 subscribe owners, got %d", len(owners))
	}
	if strings.TrimSpace(owners[0]) == "" || strings.TrimSpace(owners[1]) == "" {
		t.Fatalf("expected non-empty generated owners, got %+v", owners)
	}
	if owners[0] == owners[1] {
		t.Fatalf("expected unique generated owners, got %+v", owners)
	}
}

func TestClientStartConsumerSharedMessageHandler(t *testing.T) {
	now := time.Now().Unix()
	queueMessages := map[string]testQueueMessage{
		"queue-a": {
			Queue:                    "queue-a",
			MessageID:                "msg-a",
			Attempts:                 1,
			MaxAttempts:              4,
			NotVisibleUntilUnix:      now,
			VisibilityTimeoutSeconds: 15,
			PayloadContentType:       "text/plain",
			PayloadBytes:             5,
			LeaseID:                  "lease-a",
			LeaseExpiresAtUnix:       now + 30,
			FencingToken:             11,
		},
		"queue-b": {
			Queue:                    "queue-b",
			MessageID:                "msg-b",
			Attempts:                 1,
			MaxAttempts:              4,
			NotVisibleUntilUnix:      now,
			VisibilityTimeoutSeconds: 15,
			PayloadContentType:       "text/plain",
			PayloadBytes:             5,
			LeaseID:                  "lease-b",
			LeaseExpiresAtUnix:       now + 30,
			FencingToken:             12,
		},
	}
	payloads := map[string][]byte{
		"queue-a": []byte("alpha"),
		"queue-b": []byte("bravo"),
	}

	var (
		ackCount atomic.Int32
		seenMu   sync.Mutex
		seen     = map[string]bool{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			var req api.DequeueRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode subscribe: %v", err)
			}
			msg, ok := queueMessages[req.Queue]
			if !ok {
				t.Fatalf("unexpected queue %q", req.Queue)
			}
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)

			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{"message": msg, "next_cursor": "cursor"}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", msg.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write(payloads[req.Queue]); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/ack":
			ackCount.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	handler := func(_ context.Context, cm client.ConsumerMessage) error {
		defer cm.Message.Close()
		if cm.Client != cli {
			return fmt.Errorf("handler received unexpected client pointer")
		}
		if cm.Logger == nil {
			return fmt.Errorf("handler received nil logger")
		}
		if cm.Name() != cm.Queue {
			return fmt.Errorf("expected default consumer name %q, got %q", cm.Queue, cm.Name())
		}
		if cm.WithState {
			return fmt.Errorf("expected non-stateful message")
		}
		if cm.State != nil {
			return fmt.Errorf("expected nil state handle")
		}
		body, err := io.ReadAll(cm.Message)
		if err != nil {
			return err
		}
		if string(body) != string(payloads[cm.Queue]) {
			return fmt.Errorf("unexpected payload for %s: %q", cm.Queue, body)
		}
		if err := cm.Message.Ack(context.Background()); err != nil {
			return err
		}
		seenMu.Lock()
		seen[cm.Queue] = true
		gotBoth := seen["queue-a"] && seen["queue-b"]
		seenMu.Unlock()
		if gotBoth {
			cancel()
		}
		return nil
	}

	err = cli.StartConsumer(ctx, []client.ConsumerConfig{
		{
			Queue:          "queue-a",
			Options:        client.SubscribeOptions{Owner: "worker-a"},
			MessageHandler: handler,
		},
		{
			Queue:          "queue-b",
			Options:        client.SubscribeOptions{Owner: "worker-b"},
			MessageHandler: handler,
		},
	}...)
	if err != nil {
		t.Fatalf("start consumer: %v", err)
	}

	seenMu.Lock()
	gotQueueA := seen["queue-a"]
	gotQueueB := seen["queue-b"]
	seenMu.Unlock()
	if !gotQueueA || !gotQueueB {
		t.Fatalf("expected both queues to be handled, got queue-a=%v queue-b=%v", gotQueueA, gotQueueB)
	}
	if ackCount.Load() < 2 {
		t.Fatalf("expected at least 2 ack requests, got %d", ackCount.Load())
	}
}

func TestClientStartConsumerWithStateMessage(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Queue:                    "workflow",
		MessageID:                "msg-state",
		Attempts:                 1,
		MaxAttempts:              5,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 20,
		PayloadContentType:       "application/json",
		PayloadBytes:             2,
		LeaseID:                  "lease-msg",
		LeaseExpiresAtUnix:       now + 40,
		FencingToken:             21,
		StateETag:                "state-etag",
		StateLeaseID:             "lease-state",
		StateLeaseExpiresAtUnix:  now + 40,
		StateFencingToken:        31,
	}

	var ackReq map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribeWithState":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message":     message,
				"next_cursor": "cursor-1",
			}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("{}")); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/ack":
			if err := json.NewDecoder(r.Body).Decode(&ackReq); err != nil {
				t.Fatalf("decode ack: %v", err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	err = cli.StartConsumer(ctx, []client.ConsumerConfig{
		{
			Queue:     "workflow",
			WithState: true,
			Options:   client.SubscribeOptions{Owner: "state-worker"},
			MessageHandler: func(_ context.Context, cm client.ConsumerMessage) error {
				defer cm.Message.Close()
				if !cm.WithState {
					return fmt.Errorf("expected stateful delivery")
				}
				if cm.State == nil {
					return fmt.Errorf("expected state handle")
				}
				if cm.State.LeaseID() != "lease-state" {
					return fmt.Errorf("unexpected state lease id %q", cm.State.LeaseID())
				}
				if err := cm.Message.Ack(context.Background()); err != nil {
					return err
				}
				cancel()
				return nil
			},
		},
	}...)
	if err != nil {
		t.Fatalf("start consumer: %v", err)
	}
	if ackReq == nil {
		t.Fatalf("expected ack request")
	}
	if stateLease, ok := ackReq["state_lease_id"].(string); !ok || stateLease != "lease-state" {
		t.Fatalf("expected state lease in ack: %+v", ackReq)
	}
}

func TestClientStartConsumerNamespacePrecedence(t *testing.T) {
	tests := []struct {
		name              string
		clientDefaultNS   string
		configNamespace   string
		optionsNamespace  string
		expectedNamespace string
	}{
		{
			name:              "consumer config namespace",
			clientDefaultNS:   "default-ns",
			configNamespace:   "consumer-ns",
			expectedNamespace: "consumer-ns",
		},
		{
			name:              "options override consumer namespace",
			clientDefaultNS:   "default-ns",
			configNamespace:   "consumer-ns",
			optionsNamespace:  "options-ns",
			expectedNamespace: "options-ns",
		},
		{
			name:              "fallback to client default",
			clientDefaultNS:   "default-ns",
			expectedNamespace: "default-ns",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				mu            sync.Mutex
				subscribeReqs []api.DequeueRequest
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/queue/subscribe":
					var req api.DequeueRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Fatalf("decode subscribe request: %v", err)
					}
					mu.Lock()
					subscribeReqs = append(subscribeReqs, req)
					mu.Unlock()

					mw := multipart.NewWriter(w)
					w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
					w.WriteHeader(http.StatusOK)

					metaHeader := textproto.MIMEHeader{}
					metaHeader.Set("Content-Type", "application/json")
					metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
					metaPart, err := mw.CreatePart(metaHeader)
					if err != nil {
						t.Fatalf("create meta part: %v", err)
					}
					if err := json.NewEncoder(metaPart).Encode(map[string]any{
						"message": map[string]any{
							"queue":                      "orders",
							"message_id":                 "msg-1",
							"attempts":                   1,
							"max_attempts":               3,
							"not_visible_until_unix":     time.Now().Unix(),
							"visibility_timeout_seconds": 10,
							"payload_content_type":       "application/json",
							"payload_bytes":              2,
							"lease_id":                   "lease-1",
							"lease_expires_at_unix":      time.Now().Add(10 * time.Second).Unix(),
						},
						"next_cursor": "cursor-1",
					}); err != nil {
						t.Fatalf("encode meta: %v", err)
					}

					payloadHeader := textproto.MIMEHeader{}
					payloadHeader.Set("Content-Type", "application/json")
					payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
					payloadPart, err := mw.CreatePart(payloadHeader)
					if err != nil {
						t.Fatalf("create payload part: %v", err)
					}
					if _, err := payloadPart.Write([]byte("{}")); err != nil {
						t.Fatalf("write payload: %v", err)
					}

					if err := mw.Close(); err != nil {
						t.Fatalf("close multipart: %v", err)
					}
				case "/v1/queue/ack":
					_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
				default:
					t.Fatalf("unexpected path %s", r.URL.Path)
				}
			}))
			t.Cleanup(srv.Close)

			cli, err := client.New(
				srv.URL,
				client.WithDisableMTLS(true),
				client.WithEndpointShuffle(false),
				client.WithDefaultNamespace(tc.clientDefaultNS),
			)
			if err != nil {
				t.Fatalf("new client: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err = cli.StartConsumer(ctx, client.ConsumerConfig{
				Name:      "orders-consumer",
				Queue:     "orders",
				Namespace: tc.configNamespace,
				Options: client.SubscribeOptions{
					Namespace: tc.optionsNamespace,
				},
				MessageHandler: func(handlerCtx context.Context, msg client.ConsumerMessage) error {
					if msg.Message != nil {
						defer msg.Message.Close()
						if ackErr := msg.Message.Ack(context.Background()); ackErr != nil {
							return ackErr
						}
					}
					cancel()
					return nil
				},
			})
			if err != nil {
				t.Fatalf("start consumer: %v", err)
			}

			mu.Lock()
			defer mu.Unlock()
			if len(subscribeReqs) == 0 {
				t.Fatalf("expected at least one subscribe request")
			}
			if got := subscribeReqs[0].Namespace; got != tc.expectedNamespace {
				t.Fatalf("expected namespace %q, got %q", tc.expectedNamespace, got)
			}
		})
	}
}

func TestClientStartConsumerRestartsAndCallsErrorHandler(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Queue:                    "orders",
		MessageID:                "msg-retry",
		Attempts:                 1,
		MaxAttempts:              4,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 15,
		PayloadContentType:       "text/plain",
		PayloadBytes:             5,
		LeaseID:                  "lease-retry",
		LeaseExpiresAtUnix:       now + 30,
		FencingToken:             41,
	}
	var subscribeCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			call := subscribeCalls.Add(1)
			if call <= 2 {
				http.Error(w, "temporary failure", http.StatusInternalServerError)
				return
			}
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message":     message,
				"next_cursor": "cursor-1",
			}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("ready")); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/ack":
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		case "/v1/queue/nack":
			_ = json.NewEncoder(w).Encode(map[string]any{"nacked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu         sync.Mutex
		errorCalls []client.ConsumerError
	)
	start := time.Now()
	err = cli.StartConsumer(ctx, []client.ConsumerConfig{
		{
			Name:    "orders-worker",
			Queue:   "orders",
			Options: client.SubscribeOptions{Owner: "worker-retry"},
			RestartPolicy: client.ConsumerRestartPolicy{
				ImmediateRetries: 1,
				BaseDelay:        20 * time.Millisecond,
				MaxDelay:         20 * time.Millisecond,
				Multiplier:       2.0,
			},
			MessageHandler: func(_ context.Context, cm client.ConsumerMessage) error {
				defer cm.Message.Close()
				if cm.Name() != "orders-worker" {
					return fmt.Errorf("unexpected consumer name %q", cm.Name())
				}
				if err := cm.Message.Ack(context.Background()); err != nil {
					return err
				}
				cancel()
				return nil
			},
			ErrorHandler: func(_ context.Context, event client.ConsumerError) error {
				mu.Lock()
				errorCalls = append(errorCalls, event)
				mu.Unlock()
				return nil
			},
		},
	}...)
	if err != nil {
		t.Fatalf("start consumer: %v", err)
	}
	if subscribeCalls.Load() < 3 {
		t.Fatalf("expected at least 3 subscribe attempts, got %d", subscribeCalls.Load())
	}
	elapsed := time.Since(start)
	if elapsed < 18*time.Millisecond {
		t.Fatalf("expected delayed retry, elapsed=%s", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(errorCalls) != 2 {
		t.Fatalf("expected 2 error handler calls, got %d", len(errorCalls))
	}
	if errorCalls[0].Attempt != 1 || errorCalls[0].RestartIn != 0 {
		t.Fatalf("unexpected first error event: %+v", errorCalls[0])
	}
	if errorCalls[0].Name != "orders-worker" {
		t.Fatalf("unexpected consumer name in first error event: %+v", errorCalls[0])
	}
	if errorCalls[1].Attempt != 2 || errorCalls[1].RestartIn < 20*time.Millisecond {
		t.Fatalf("unexpected second error event: %+v", errorCalls[1])
	}
}

func TestClientStartConsumerStopsOnNamespaceForbidden(t *testing.T) {
	var subscribeCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			subscribeCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(api.ErrorResponse{
				ErrorCode: "namespace_forbidden",
				Detail:    `namespace "stash" requires read permission`,
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	var (
		mu     sync.Mutex
		events []client.ConsumerError
	)
	err = cli.StartConsumer(context.Background(), client.ConsumerConfig{
		Name:  "ackOnThree",
		Queue: "testing",
		Options: client.SubscribeOptions{
			Namespace: "stash",
			Owner:     "worker-authz",
		},
		RestartPolicy: client.ConsumerRestartPolicy{
			ImmediateRetries: 5,
			BaseDelay:        10 * time.Millisecond,
			MaxDelay:         50 * time.Millisecond,
			Multiplier:       2,
		},
		MessageHandler: func(context.Context, client.ConsumerMessage) error {
			t.Fatal("message handler should not be invoked")
			return nil
		},
		ErrorHandler: func(_ context.Context, event client.ConsumerError) error {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, event)
			return nil
		},
	})
	if err == nil {
		t.Fatalf("expected non-retryable consumer error")
	}
	if !strings.Contains(err.Error(), "namespace_forbidden") {
		t.Fatalf("expected namespace_forbidden error, got %v", err)
	}
	if got := subscribeCalls.Load(); got != 1 {
		t.Fatalf("expected exactly one subscribe attempt, got %d", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 1 {
		t.Fatalf("expected one error event, got %d", len(events))
	}
	if events[0].Attempt != 1 {
		t.Fatalf("expected attempt=1, got %+v", events[0])
	}
	if events[0].RestartIn != 0 {
		t.Fatalf("expected restart_in=0 for non-retryable error, got %+v", events[0])
	}
}

func TestClientStartConsumerLifecycleHooks(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Queue:                    "orders",
		MessageID:                "msg-hooks",
		Attempts:                 1,
		MaxAttempts:              3,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 15,
		PayloadContentType:       "text/plain",
		PayloadBytes:             2,
		LeaseID:                  "lease-hooks",
		LeaseExpiresAtUnix:       now + 30,
		FencingToken:             51,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message":     message,
				"next_cursor": "cursor-hooks",
			}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("ok")); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/ack":
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		case "/v1/queue/nack":
			_ = json.NewEncoder(w).Encode(map[string]any{"nacked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var (
		mu        sync.Mutex
		startSeen []client.ConsumerLifecycleEvent
		stopSeen  []client.ConsumerLifecycleEvent
	)
	err = cli.StartConsumer(ctx, []client.ConsumerConfig{
		{
			Name:    "orders-consumer",
			Queue:   "orders",
			Options: client.SubscribeOptions{Owner: "worker-hooks"},
			OnStart: func(_ context.Context, event client.ConsumerLifecycleEvent) {
				mu.Lock()
				startSeen = append(startSeen, event)
				mu.Unlock()
			},
			OnStop: func(_ context.Context, event client.ConsumerLifecycleEvent) {
				mu.Lock()
				stopSeen = append(stopSeen, event)
				mu.Unlock()
			},
			MessageHandler: func(_ context.Context, cm client.ConsumerMessage) error {
				defer cm.Message.Close()
				if err := cm.Message.Ack(context.Background()); err != nil {
					return err
				}
				cancel()
				return nil
			},
		},
	}...)
	if err != nil {
		t.Fatalf("start consumer: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(startSeen) == 0 {
		t.Fatalf("expected onStart hook to be called")
	}
	if len(stopSeen) == 0 {
		t.Fatalf("expected onStop hook to be called")
	}
	if startSeen[0].Name != "orders-consumer" || startSeen[0].Queue != "orders" || startSeen[0].Attempt < 1 {
		t.Fatalf("unexpected onStart event: %+v", startSeen[0])
	}
	if stopSeen[0].Name != "orders-consumer" || stopSeen[0].Queue != "orders" || stopSeen[0].Attempt < 1 {
		t.Fatalf("unexpected onStop event: %+v", stopSeen[0])
	}
}

func TestClientStartConsumerRecoversMessageHandlerPanic(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Queue:                    "orders",
		MessageID:                "msg-panic",
		Attempts:                 1,
		MaxAttempts:              3,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 15,
		PayloadContentType:       "text/plain",
		PayloadBytes:             2,
		LeaseID:                  "lease-panic",
		LeaseExpiresAtUnix:       now + 30,
		FencingToken:             61,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message":     message,
				"next_cursor": "cursor-panic",
			}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("ok")); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/ack":
			_ = json.NewEncoder(w).Encode(map[string]any{"acked": true})
		case "/v1/queue/nack":
			_ = json.NewEncoder(w).Encode(map[string]any{"nacked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		handlerCalls atomic.Int32
		errorEvents  []client.ConsumerError
		mu           sync.Mutex
	)
	err = cli.StartConsumer(ctx, []client.ConsumerConfig{
		{
			Name:    "panic-consumer",
			Queue:   "orders",
			Options: client.SubscribeOptions{Owner: "worker-panic"},
			RestartPolicy: client.ConsumerRestartPolicy{
				ImmediateRetries: 1,
				BaseDelay:        20 * time.Millisecond,
				MaxDelay:         20 * time.Millisecond,
				Multiplier:       2.0,
			},
			MessageHandler: func(_ context.Context, cm client.ConsumerMessage) error {
				defer cm.Message.Close()
				if handlerCalls.Add(1) == 1 {
					panic("boom")
				}
				if err := cm.Message.Ack(context.Background()); err != nil {
					return err
				}
				cancel()
				return nil
			},
			ErrorHandler: func(_ context.Context, event client.ConsumerError) error {
				mu.Lock()
				errorEvents = append(errorEvents, event)
				mu.Unlock()
				return nil
			},
		},
	}...)
	if err != nil {
		t.Fatalf("start consumer: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(errorEvents) == 0 {
		t.Fatalf("expected at least one consumer error event from panic")
	}
	if errorEvents[0].Name != "panic-consumer" {
		t.Fatalf("expected consumer name in error event, got %+v", errorEvents[0])
	}
	if !strings.Contains(errorEvents[0].Err.Error(), "panic") || !strings.Contains(errorEvents[0].Err.Error(), "boom") {
		t.Fatalf("expected panic details in error event, got %v", errorEvents[0].Err)
	}
}

func TestClientStartConsumerRecoversLifecycleHookPanic(t *testing.T) {
	cli, err := client.New("http://127.0.0.1", client.WithDisableMTLS(true))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu         sync.Mutex
		errorCalls []client.ConsumerError
	)
	err = cli.StartConsumer(ctx, []client.ConsumerConfig{
		{
			Name:    "hook-panic-consumer",
			Queue:   "orders",
			Options: client.SubscribeOptions{Owner: "worker-hook-panic"},
			OnStart: func(context.Context, client.ConsumerLifecycleEvent) {
				panic("hook exploded")
			},
			MessageHandler: func(context.Context, client.ConsumerMessage) error {
				return nil
			},
			ErrorHandler: func(_ context.Context, event client.ConsumerError) error {
				mu.Lock()
				errorCalls = append(errorCalls, event)
				mu.Unlock()
				cancel()
				return nil
			},
		},
	}...)
	if err != nil {
		t.Fatalf("expected nil error after context cancellation, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(errorCalls) == 0 {
		t.Fatalf("expected error handler to receive lifecycle hook panic")
	}
	if !strings.Contains(errorCalls[0].Err.Error(), "lifecycle hook") || !strings.Contains(errorCalls[0].Err.Error(), "hook exploded") {
		t.Fatalf("unexpected lifecycle panic error: %v", errorCalls[0].Err)
	}
}

func TestClientStartConsumerStopsWhenErrorHandlerFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			http.Error(w, "temporary failure", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	stopErr := errors.New("stop consumer")
	err = cli.StartConsumer(context.Background(), []client.ConsumerConfig{
		{
			Queue:   "orders",
			Options: client.SubscribeOptions{Owner: "worker-stop"},
			RestartPolicy: client.ConsumerRestartPolicy{
				ImmediateRetries: 0,
				BaseDelay:        10 * time.Millisecond,
				MaxDelay:         10 * time.Millisecond,
				Multiplier:       2.0,
			},
			MessageHandler: func(context.Context, client.ConsumerMessage) error { return nil },
			ErrorHandler: func(context.Context, client.ConsumerError) error {
				return stopErr
			},
		},
	}...)
	if err == nil {
		t.Fatalf("expected start consumer error")
	}
	if !strings.Contains(err.Error(), stopErr.Error()) {
		t.Fatalf("expected wrapped error handler failure, got %v", err)
	}
}

func TestClientStartConsumerReturnsNilOnContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			http.Error(w, "temporary failure", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	err = cli.StartConsumer(ctx, []client.ConsumerConfig{
		{
			Queue:   "orders",
			Options: client.SubscribeOptions{Owner: "worker-cancel"},
			RestartPolicy: client.ConsumerRestartPolicy{
				ImmediateRetries: 0,
				BaseDelay:        100 * time.Millisecond,
				MaxDelay:         100 * time.Millisecond,
				Multiplier:       2.0,
			},
			MessageHandler: func(context.Context, client.ConsumerMessage) error { return nil },
		},
	}...)
	if err != nil {
		t.Fatalf("expected nil error on context cancellation, got %v", err)
	}
}

func TestClientStartConsumerAutoExtendErrorDoesNotDeadlock(t *testing.T) {
	now := time.Now().Unix()
	message := testQueueMessage{
		Queue:                    "orders",
		MessageID:                "msg-extend-deadlock",
		Attempts:                 1,
		MaxAttempts:              3,
		NotVisibleUntilUnix:      now,
		VisibilityTimeoutSeconds: 1,
		PayloadContentType:       "text/plain",
		PayloadBytes:             4,
		LeaseID:                  "lease-extend-deadlock",
		LeaseExpiresAtUnix:       now + 30,
		FencingToken:             71,
	}

	var extendCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/queue/subscribe":
			mw := multipart.NewWriter(w)
			w.Header().Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
			w.WriteHeader(http.StatusOK)
			metaHeader := textproto.MIMEHeader{}
			metaHeader.Set("Content-Type", "application/json")
			metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
			metaPart, err := mw.CreatePart(metaHeader)
			if err != nil {
				t.Fatalf("create meta part: %v", err)
			}
			if err := json.NewEncoder(metaPart).Encode(map[string]any{
				"message":     message,
				"next_cursor": "cursor-extend",
			}); err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			payloadHeader := textproto.MIMEHeader{}
			payloadHeader.Set("Content-Type", message.PayloadContentType)
			payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
			payloadPart, err := mw.CreatePart(payloadHeader)
			if err != nil {
				t.Fatalf("create payload part: %v", err)
			}
			if _, err := payloadPart.Write([]byte("work")); err != nil {
				t.Fatalf("write payload: %v", err)
			}
			if err := mw.Close(); err != nil {
				t.Fatalf("close multipart: %v", err)
			}
		case "/v1/queue/extend":
			extendCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(api.ErrorResponse{
				ErrorCode: "lease_conflict",
				Detail:    "simulated extend conflict",
			})
		case "/v1/queue/nack":
			_ = json.NewEncoder(w).Encode(map[string]any{"nacked": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	cli, err := client.New(srv.URL, client.WithDisableMTLS(true), client.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopErr := errors.New("stop on extend error")
	errCh := make(chan error, 1)
	go func() {
		errCh <- cli.StartConsumer(ctx, client.ConsumerConfig{
			Name:    "extend-deadlock",
			Queue:   "orders",
			Options: client.SubscribeOptions{Owner: "worker-extend"},
			RestartPolicy: client.ConsumerRestartPolicy{
				ImmediateRetries: 0,
				BaseDelay:        10 * time.Millisecond,
				MaxDelay:         10 * time.Millisecond,
				Multiplier:       2.0,
			},
			MessageHandler: func(handlerCtx context.Context, cm client.ConsumerMessage) error {
				defer cm.Message.Close()
				select {
				case <-handlerCtx.Done():
					return nil
				case <-time.After(2 * time.Second):
					return nil
				}
			},
			ErrorHandler: func(_ context.Context, event client.ConsumerError) error {
				if event.Name != "extend-deadlock" {
					return fmt.Errorf("unexpected consumer name %q", event.Name)
				}
				return stopErr
			},
		})
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected start consumer error")
		}
		if !strings.Contains(err.Error(), stopErr.Error()) {
			t.Fatalf("expected wrapped stop error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("start consumer hung waiting for auto-extend shutdown")
	}

	if extendCalls.Load() == 0 {
		t.Fatalf("expected at least one /v1/queue/extend call")
	}
}

func TestClientFlushIndexEnablesQueryResults(t *testing.T) {
	ts := lockd.StartTestServer(t,
		lockd.WithTestConfigFunc(func(cfg *lockd.Config) {
			cfg.IndexerFlushInterval = time.Hour
			cfg.IndexerFlushDocs = 1000
		}),
		lockd.WithTestCloseDefaults(lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond)),
	)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}
	httpClient, err := ts.NewHTTPClient()
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = cli.UpdateNamespaceConfig(ctx, api.NamespaceConfigRequest{
		Namespace: namespaces.Default,
		Query: &api.NamespaceQueryConfig{
			PreferredEngine: "index",
			FallbackEngine:  "none",
		},
	}, client.NamespaceConfigOptions{})
	if err != nil {
		t.Fatalf("set namespace config: %v", err)
	}
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        "flush-index-key",
		Owner:      "index-test",
		TTLSeconds: 30,
		BlockSecs:  client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := lease.Save(ctx, map[string]any{"status": "ready"}); err != nil {
		lease.Close()
		t.Fatalf("save: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	queryReq := api.QueryRequest{
		Namespace: namespaces.Default,
		Selector:  api.Selector{Eq: &api.Term{Field: "/status", Value: "ready"}},
	}
	baseURL := ts.URL()
	initial := performQuery(t, httpClient, baseURL, queryReq, "index")
	if len(initial.Keys) != 0 {
		t.Fatalf("expected no keys before flush, got %v", initial.Keys)
	}
	flushResp, err := cli.FlushIndex(ctx, namespaces.Default, client.WithFlushModeWait())
	if err != nil {
		t.Fatalf("flush index: %v", err)
	}
	if !flushResp.Flushed {
		t.Fatalf("expected flushed response, got %+v", flushResp)
	}
	final := performQuery(t, httpClient, baseURL, queryReq, "index")
	if len(final.Keys) != 1 || final.Keys[0] != "flush-index-key" {
		t.Fatalf("unexpected keys after flush: %v", final.Keys)
	}
}

func performQuery(t testing.TB, httpClient *http.Client, baseURL string, req api.QueryRequest, engine string) api.QueryResponse {
	t.Helper()
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal query request: %v", err)
	}
	values := url.Values{}
	if strings.TrimSpace(req.Namespace) != "" {
		values.Set("namespace", req.Namespace)
	}
	if strings.TrimSpace(engine) != "" {
		values.Set("engine", engine)
	}
	fullURL := baseURL + "/v1/query"
	if encoded := values.Encode(); encoded != "" {
		fullURL += "?" + encoded
	}
	httpReq, err := http.NewRequest(http.MethodPost, fullURL, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new query request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("query request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query status %d", resp.StatusCode)
	}
	var out api.QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	return out
}

func TestClientQuery(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if got := r.URL.Query().Get("engine"); got != "index" {
			t.Fatalf("unexpected engine %s", got)
		}
		if got := r.URL.Query().Get("refresh"); got != "wait_for" {
			t.Fatalf("unexpected refresh %s", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if !strings.Contains(string(body), "\"namespace\":\"default\"") {
			t.Fatalf("missing namespace in payload: %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"namespace":"default","keys":["usertable:user0001"],"cursor":"abc"}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	cli, err := client.New(srv.URL, client.WithDisableMTLS(true))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := cli.Query(context.Background(),
		client.WithQueryNamespace("default"),
		client.WithQuery("eq{field=/status,value=open}"),
		client.WithQueryLimit(5),
		client.WithQueryEngineIndex(),
		client.WithQueryRefreshWaitFor(),
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if resp == nil || resp.Cursor != "abc" || len(resp.Keys()) != 1 {
		t.Fatalf("unexpected response: %+v", resp)
	}
	count := 0
	if err := resp.ForEach(func(row client.QueryRow) error {
		count++
		if row.Namespace != "default" || row.Key != "usertable:user0001" {
			return fmt.Errorf("unexpected row %+v", row)
		}
		return nil
	}); err != nil {
		t.Fatalf("foreach: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}

func TestClientQueryInvalidLQL(t *testing.T) {
	cli, err := client.New("http://127.0.0.1", client.WithDisableMTLS(true))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = cli.Query(context.Background(), client.WithQuery("eq{field=/status"))
	if err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestClientQueryDocuments(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/query", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("return"); got != "documents" {
			t.Fatalf("unexpected return mode %s", got)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("X-Lockd-Query-Return", "documents")
		w.Header().Set("X-Lockd-Query-Cursor", "next-doc")
		w.Header().Set("X-Lockd-Query-Index-Seq", "42")
		w.Header().Set("X-Lockd-Query-Metadata", `{"hint":"scan"}`)
		fmt.Fprintln(w, `{"ns":"default","key":"doc-1","ver":7,"doc":{"status":"ready"}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	cli, err := client.New(srv.URL, client.WithDisableMTLS(true))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := cli.Query(context.Background(),
		client.WithQueryNamespace("default"),
		client.WithQueryReturnDocuments(),
	)
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if resp.Mode() != client.QueryReturnDocuments {
		t.Fatalf("expected documents mode, got %s", resp.Mode())
	}
	keys := resp.Keys()
	if len(keys) != 1 || keys[0] != "doc-1" {
		t.Fatalf("unexpected keys %v", keys)
	}
	if err := resp.Close(); err != nil {
		t.Fatalf("close drained response: %v", err)
	}
	resp2, err := cli.Query(context.Background(),
		client.WithQueryNamespace("default"),
		client.WithQueryReturnDocuments(),
	)
	if err != nil {
		t.Fatalf("query documents 2: %v", err)
	}
	defer resp2.Close()
	count := 0
	if err := resp2.ForEach(func(row client.QueryRow) error {
		count++
		if !row.HasDocument() {
			return fmt.Errorf("expected document payload")
		}
		var doc struct {
			Status string `json:"status"`
		}
		if err := row.DocumentInto(&doc); err != nil {
			return err
		}
		if doc.Status != "ready" {
			return fmt.Errorf("unexpected doc %+v", doc)
		}
		return nil
	}); err != nil {
		t.Fatalf("foreach documents: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
	if resp2.Cursor != "next-doc" {
		t.Fatalf("expected cursor next-doc, got %s", resp2.Cursor)
	}
	if resp2.IndexSeq != 42 {
		t.Fatalf("expected index seq 42, got %d", resp2.IndexSeq)
	}
	if hint := resp2.Metadata["hint"]; hint != "scan" {
		t.Fatalf("expected metadata hint=scan, got %v", resp2.Metadata)
	}
}
