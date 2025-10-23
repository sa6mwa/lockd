package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
)

func TestUnixSocketClientLifecycle(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "lockd.sock")
	ts := lockd.StartTestServer(t,
		lockd.WithTestUnixSocket(socket),
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

func TestAcquireAutoGeneratesKey(t *testing.T) {
	ts := lockd.StartTestServer(t)
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
	cli, err := client.New(base, client.WithHTTPClient(ts.Client()), client.WithMTLS(false))
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
		client.WithMTLS(false),
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
		case strings.HasPrefix(r.URL.Path, "/v1/update-state"):
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

func TestLeaseSessionRemoveState(t *testing.T) {
	var removeVersion, removeETag, removeToken string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/acquire":
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Fencing-Token", "41")
			fmt.Fprint(w, `{"lease_id":"L-remove","key":"orders","owner":"worker","expires_at_unix":123,"version":2,"state_etag":"etag-initial","fencing_token":41}`)
		case strings.HasPrefix(r.URL.Path, "/v1/remove-state"):
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
		client.WithMTLS(false),
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
	ts := lockd.StartTestServer(t, lockd.WithTestUnixSocket(socket))
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
		client.WithMTLS(false),
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

func TestClientAcquireAllEndpointsDown(t *testing.T) {
	transport := &downTransport{}
	httpClient := &http.Client{Transport: transport}
	endpoints := []string{"http://hosta:9341", "http://hostb:9341"}
	cli, err := client.NewWithEndpoints(endpoints,
		client.WithMTLS(false),
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

type acquireForUpdateRetryTransport struct {
	mu            sync.Mutex
	acquireCalls  int
	getStateCalls int
	updateCalls   int
	releaseCalls  int
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
	case "/v1/get-state":
		t.getStateCalls++
		if t.getStateCalls == 1 {
			return newJSONResponse(req, http.StatusConflict, `{"error":"lease_required"}`), nil
		}
		resp := newJSONResponse(req, http.StatusOK, `{"value":1}`)
		resp.Header.Set("ETag", "etag-initial")
		resp.Header.Set("X-Key-Version", "1")
		resp.Header.Set("X-Fencing-Token", "73")
		resp.Header.Set("Content-Length", strconv.Itoa(len(`{"value":1}`)))
		return resp, nil
	case "/v1/update-state":
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

func TestAcquireForUpdateRetriesOnLeaseRequired(t *testing.T) {
	transport := &acquireForUpdateRetryTransport{}
	httpClient := &http.Client{Transport: transport}

	cli, err := client.NewWithEndpoints([]string{"http://retry:9341"},
		client.WithMTLS(false),
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
	getStateCalls := transport.getStateCalls
	updateCalls := transport.updateCalls
	releaseCalls := transport.releaseCalls
	transport.mu.Unlock()

	if acquireCalls < 2 {
		t.Fatalf("expected at least two acquire attempts, got %d", acquireCalls)
	}
	if getStateCalls < 2 {
		t.Fatalf("expected get-state retries, got %d", getStateCalls)
	}
	if updateCalls != 1 {
		t.Fatalf("expected exactly one update, got %d", updateCalls)
	}
	if releaseCalls == 0 {
		t.Fatal("expected release to be invoked")
	}
}

func TestAcquireForUpdateHandlerError(t *testing.T) {
	ts := lockd.StartTestServer(t, lockd.WithTestUnixSocket(filepath.Join(t.TempDir(), "handler-error.sock")))
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

func TestTestServerChaosProxy(t *testing.T) {
	chaos := &lockd.ChaosConfig{
		Seed:                    42,
		MinDelay:                time.Millisecond,
		MaxDelay:                3 * time.Millisecond,
		BandwidthBytesPerSecond: 0,
	}
	ts := lockd.StartTestServer(t, lockd.WithTestChaos(chaos))
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

	cli, err := client.New(srv.URL, client.WithMTLS(false), client.WithEndpointShuffle(false))
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

	cli, err := client.New(srv.URL, client.WithMTLS(false), client.WithEndpointShuffle(false))
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
		case "/v1/queue/dequeue-with-state":
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

	cli, err := client.New(srv.URL, client.WithMTLS(false), client.WithEndpointShuffle(false))
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

	cli, err := client.New(srv.URL, client.WithMTLS(false), client.WithEndpointShuffle(false))
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
		case "/v1/queue/subscribe-with-state":
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

	cli, err := client.New(srv.URL, client.WithMTLS(false), client.WithEndpointShuffle(false))
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
