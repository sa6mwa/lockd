package querydata

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
	"pkt.systems/lockd/namespaces"
)

const (
	PaginationNamespace = "query-pagination"
)

var suiteNamespaces = []string{
	namespaces.Default,
	PaginationNamespace,
	"alpha",
	"beta",
}

// QueryNamespaces returns a copy of the namespaces exercised by the query suites.
func QueryNamespaces() []string {
	out := make([]string, len(suiteNamespaces))
	copy(out, suiteNamespaces)
	return out
}

// DatasetProfile controls how much data gets seeded by the domain helpers.
type DatasetProfile int

const (
	DatasetReduced DatasetProfile = iota
	DatasetFull
	DatasetExtended
)

// SeedState writes the provided JSON document to the requested namespace/key.
func SeedState(t testing.TB, ctx context.Context, cli *client.Client, namespace, key string, state map[string]any) {
	t.Helper()
	payload, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal state: %v", err)
	}
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      "query-testdata",
		TTLSeconds: 60,
	})
	if err != nil {
		t.Fatalf("acquire %s/%s: %v", namespace, key, err)
	}
	if _, err := lease.UpdateBytes(ctx, payload); err != nil {
		t.Fatalf("update %s/%s: %v", namespace, key, err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release %s/%s: %v", namespace, key, err)
	}
	registerCleanup(t, cli, namespace, key)
}

func registerCleanup(t testing.TB, cli *client.Client, namespace, key string) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		lease, err := cli.Acquire(cleanupCtx, api.AcquireRequest{
			Namespace:  namespace,
			Key:        key,
			Owner:      "query-testdata-cleanup",
			TTLSeconds: 30,
		})
		if err != nil {
			t.Logf("querydata: cleanup acquire %s/%s failed: %v", namespace, key, err)
			return
		}
		defer lease.Release(cleanupCtx)
		if _, err := lease.Remove(cleanupCtx); err != nil {
			t.Logf("querydata: cleanup remove %s/%s failed: %v", namespace, key, err)
		}
	})
}

// FlushQueryNamespaces forces the index writer to flush pending documents for the provided namespaces.
// When namespaces is empty, all suite namespaces are flushed.
func FlushQueryNamespaces(t testing.TB, ctx context.Context, cli *client.Client, namespaces ...string) {
	t.Helper()
	if cli == nil {
		return
	}
	targets := namespaces
	if len(targets) == 0 {
		targets = QueryNamespaces()
	}
	for _, ns := range targets {
		resp, err := cli.FlushIndex(ctx, ns, client.WithFlushModeWait())
		if err != nil {
			var apiErr *client.APIError
			if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "index_unavailable" {
				continue
			}
			t.Fatalf("flush index for namespace %s: %v", ns, err)
		}
		if resp == nil {
			continue
		}
	}
}
