//go:build integration && minio && query

package miniointegration

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
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

func TestMinioQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startMinioQueryServer)
}

func startMinioQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)
	ts := startMinioTestServer(t, cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = ts.Stop(ctx)
	})
	return ts
}
