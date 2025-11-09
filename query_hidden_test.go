package lockd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
	"pkt.systems/lockd/namespaces"
)

func TestQuerySkipsHiddenKeys(t *testing.T) {
	ts := StartTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	visibleKey := "query-visible"
	hiddenKey := "query-hidden"
	seedState(ctx, t, ts.Client, "", visibleKey, map[string]any{"status": "open", "data": "visible"})
	seedState(ctx, t, ts.Client, "", hiddenKey, map[string]any{"status": "open", "data": "secret"})

	markKeyHidden(ctx, t, ts.Client, namespaces.Default, hiddenKey)

	httpClient, err := ts.NewHTTPClient()
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	reqBody, err := json.Marshal(api.QueryRequest{
		Namespace: namespaces.Default,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL()+"/v1/query", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("query call: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	var qr api.QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(qr.Keys) != 1 || qr.Keys[0] != visibleKey {
		t.Fatalf("expected only %q, got %+v", visibleKey, qr.Keys)
	}

	reader, _, _, err := ts.Client.GetPublicWithNamespace(ctx, namespaces.Default, hiddenKey)
	if err != nil {
		t.Fatalf("get hidden state: %v", err)
	}
	defer reader.Close()
	var payload map[string]any
	if err := json.NewDecoder(reader).Decode(&payload); err != nil {
		t.Fatalf("decode hidden payload: %v", err)
	}
	if payload["data"] != "secret" {
		t.Fatalf("unexpected hidden payload %+v", payload)
	}
}

func seedState(ctx context.Context, t testing.TB, cli *client.Client, namespace, key string, state map[string]any) {
	t.Helper()
	req := api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      "query-hidden-test",
		TTLSeconds: 30,
	}
	lease, err := cli.Acquire(ctx, req)
	if err != nil {
		t.Fatalf("acquire %s/%s: %v", namespace, key, err)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	if _, err := lease.UpdateBytes(ctx, payload); err != nil {
		t.Fatalf("update %s/%s: %v", namespace, key, err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release %s/%s: %v", namespace, key, err)
	}
}

func markKeyHidden(ctx context.Context, t testing.TB, cli *client.Client, namespace, key string) {
	t.Helper()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      "query-hidden-test",
		TTLSeconds: 30,
	})
	if err != nil {
		t.Fatalf("acquire %s/%s: %v", namespace, key, err)
	}
	defer lease.Release(ctx)
	_, err = cli.UpdateMetadata(ctx, key, lease.LeaseID, client.UpdateOptions{
		Namespace: namespace,
		IfVersion: strconv.FormatInt(lease.Version, 10),
		Metadata:  client.MetadataOptions{QueryHidden: client.Bool(true)},
	})
	if err != nil {
		t.Fatalf("update metadata: %v", err)
	}
}
