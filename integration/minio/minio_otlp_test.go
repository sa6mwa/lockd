//go:build integration && minio && otlp && !lq

package miniointegration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/oteltest"
	"pkt.systems/lockd/internal/uuidv7"
)

func TestMinioOTLPTraceEmission(t *testing.T) {
	collector, endpoint, err := oteltest.Start()
	if err != nil {
		t.Fatalf("start otlp collector: %v", err)
	}
	t.Cleanup(collector.Stop)

	t.Setenv("LOCKD_OTLP_ENDPOINT", fmt.Sprintf("grpc://%s", endpoint))

	cfg := loadMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ensureStoreReady(t, context.Background(), cfg)

	cfg.OTLPEndpoint = fmt.Sprintf("grpc://%s", endpoint)

	cli := startLockdServer(t, cfg)

	ctx := context.Background()
	key := "minio-otlp-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "otlp-worker", 30, 5)
	if !releaseLease(t, ctx, lease) {
		t.Fatalf("release failed")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := collector.WaitForSpans(waitCtx, 1); err != nil {
		t.Fatalf("waiting for spans: %v", err)
	}
	if !oteltest.HasCorrelationAttribute(collector.Requests()) {
		t.Fatalf("expected correlation attribute on otlp spans")
	}

	cleanupMinio(t, cfg, key)
}
