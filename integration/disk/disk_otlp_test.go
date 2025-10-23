//go:build integration && disk && otlp && !lq

package diskintegration

import (
	"context"
	"fmt"
	"testing"
	"time"

	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/oteltest"
	"pkt.systems/lockd/internal/uuidv7"
)

func TestDiskOTLPTraceEmission(t *testing.T) {
	collector, endpoint, err := oteltest.Start()
	if err != nil {
		t.Fatalf("start otlp collector: %v", err)
	}
	t.Cleanup(collector.Stop)

	t.Setenv("LOCKD_OTLP_ENDPOINT", fmt.Sprintf("grpc://%s", endpoint))

	ensureDiskRootEnv(t)

	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cfg.OTLPEndpoint = fmt.Sprintf("grpc://%s", endpoint)

	cli := startDiskServer(t, cfg)

	ctx := context.Background()
	key := "disk-otlp-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "otlp-worker", 30, lockdclient.BlockWaitForever)
	if !releaseLease(t, ctx, lease) {
		t.Fatalf("release failed")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := collector.WaitForSpans(waitCtx, 1); err != nil {
		t.Fatalf("waiting for spans: %v", err)
	}
	if !oteltest.HasCorrelationAttribute(collector.Requests()) {
		t.Fatalf("expected correlation attribute on otlp spans")
	}
}
