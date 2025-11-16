//go:build integration && minio && query

package miniointegration

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
	querydata "pkt.systems/lockd/integration/query/querydata"
	queriesuite "pkt.systems/lockd/integration/query/suite"
)

func TestMinioQuerySelectors(t *testing.T) {
	queriesuite.RunSelectors(t, startMinioQueryServer)
}

func TestMinioQueryPagination(t *testing.T) {
	queriesuite.RunPagination(t, startMinioQueryServer)
}

func TestMinioQueryNamespaceIsolation(t *testing.T) {
	queriesuite.RunNamespaceIsolation(t, startMinioQueryServer)
}

func TestMinioQueryResultsSupportPublicRead(t *testing.T) {
	queriesuite.RunPublicRead(t, startMinioQueryServer)
}

func TestMinioQueryDocumentStreaming(t *testing.T) {
	queriesuite.RunDocumentStreaming(t, startMinioQueryServer)
}

func TestMinioQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startMinioQueryServer)
}

func startMinioQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	CleanupQueryNamespaces(t, cfg)
	ts := startMinioTestServer(t, cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		querydata.FlushQueryNamespaces(t, ctx, ts.Client)
		_ = ts.Stop(ctx)
		CleanupQueryNamespaces(t, cfg)
	})
	return ts
}
