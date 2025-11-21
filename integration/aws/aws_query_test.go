//go:build integration && aws && query

package awsintegration

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
	querydata "pkt.systems/lockd/integration/query/querydata"
	queriesuite "pkt.systems/lockd/integration/query/suite"
)

func TestAWSQuerySelectors(t *testing.T) {
	queriesuite.RunSelectors(t, startAWSQueryServer)
}

func TestAWSQueryPagination(t *testing.T) {
	queriesuite.RunPagination(t, startAWSQueryServer)
}

func TestAWSQueryNamespaceIsolation(t *testing.T) {
	queriesuite.RunNamespaceIsolation(t, startAWSQueryServer)
}

func TestAWSQueryResultsSupportPublicRead(t *testing.T) {
	queriesuite.RunPublicRead(t, startAWSQueryServer)
}

func TestAWSQueryDocumentStreaming(t *testing.T) {
	queriesuite.RunDocumentStreaming(t, startAWSQueryServer)
}

func TestAWSQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startAWSQueryServer, queriesuite.WithReducedDataset())
}

func startAWSQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	cfg := loadAWSConfig(t)
	ensureStoreReady(t, context.Background(), cfg)
	CleanupQueryNamespaces(t, cfg)
	ts := startAWSTestServer(t, cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		querydata.FlushQueryNamespaces(ctx, t, ts.Client)
		_ = ts.Stop(ctx)
		CleanupQueryNamespaces(t, cfg)
	})
	return ts
}
