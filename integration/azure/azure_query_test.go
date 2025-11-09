//go:build integration && azure && query

package azureintegration

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
	queriesuite "pkt.systems/lockd/integration/query/suite"
)

func TestAzureQuerySelectors(t *testing.T) {
	queriesuite.RunSelectors(t, startAzureQueryServer)
}

func TestAzureQueryPagination(t *testing.T) {
	queriesuite.RunPagination(t, startAzureQueryServer)
}

func TestAzureQueryNamespaceIsolation(t *testing.T) {
	queriesuite.RunNamespaceIsolation(t, startAzureQueryServer)
}

func TestAzureQueryResultsSupportPublicRead(t *testing.T) {
	queriesuite.RunPublicRead(t, startAzureQueryServer)
}

func TestAzureQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startAzureQueryServer, queriesuite.WithReducedDataset())
}

func startAzureQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	cfg := loadAzureConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)
	ts := startAzureTestServer(t, cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_ = ts.Stop(ctx)
	})
	return ts
}
