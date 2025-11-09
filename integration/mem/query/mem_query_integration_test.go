//go:build integration && mem && query

package memquery

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queriesuite "pkt.systems/lockd/integration/query/suite"
	querytestdata "pkt.systems/lockd/integration/query/testdata"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

func TestMemQuerySelectors(t *testing.T) {
	queriesuite.RunSelectors(t, startMemQueryServer)
}

func TestMemQueryPagination(t *testing.T) {
	queriesuite.RunPagination(t, startMemQueryServer)
}

func TestMemQueryNamespaceIsolation(t *testing.T) {
	queriesuite.RunNamespaceIsolation(t, startMemQueryServer)
}

func TestMemQueryResultsSupportPublicRead(t *testing.T) {
	queriesuite.RunPublicRead(t, startMemQueryServer)
}

func TestMemQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startMemQueryServer)
}

func TestMemQueryHiddenKeys(t *testing.T) {
	ts := startMemQueryServer(t)
	httpClient := newHTTPClient(t, ts)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	visible := "hidden-visible"
	hidden := "hidden-secret"
	querytestdata.SeedState(t, ctx, ts.Client, "", visible, map[string]any{"status": "open"})
	querytestdata.SeedState(t, ctx, ts.Client, "", hidden, map[string]any{"status": "open"})
	markKeyHidden(ctx, t, ts.Client, namespaces.Default, hidden)

	body, err := json.Marshal(api.QueryRequest{
		Namespace: namespaces.Default,
		Selector:  api.Selector{},
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL()+"/v1/query", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("query request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	var qr api.QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	querytestdata.ExpectKeySet(t, qr.Keys, []string{visible})
}

func startMemQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	cfg := lockd.Config{
		Store:           "mem://",
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(t, pslog.DebugLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(30*time.Second),
			lockdclient.WithCloseTimeout(30*time.Second),
			lockdclient.WithKeepAliveTimeout(30*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.DebugLevel)),
		),
	}
	opts = append(opts, cryptotest.SharedMTLSOptions(t)...)
	ts := lockd.StartTestServer(t, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	})
	return ts
}

func newHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	client, err := ts.NewHTTPClient()
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	return client
}

func markKeyHidden(ctx context.Context, t testing.TB, cli *lockdclient.Client, namespace, key string) {
	t.Helper()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      "mem-query-hidden",
		TTLSeconds: 30,
	})
	if err != nil {
		t.Fatalf("acquire %s/%s: %v", namespace, key, err)
	}
	defer lease.Release(ctx)
	_, err = cli.UpdateMetadata(ctx, key, lease.LeaseID, lockdclient.UpdateOptions{
		Namespace: namespace,
		IfVersion: strconv.FormatInt(lease.Version, 10),
		Metadata:  lockdclient.MetadataOptions{QueryHidden: lockdclient.Bool(true)},
	})
	if err != nil {
		t.Fatalf("update metadata: %v", err)
	}
}
