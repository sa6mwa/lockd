//go:build integration && minio

package miniointegration

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/hatest"
)

func TestMinioHASingleModeSkipsActiveLease(t *testing.T) {
	cfg := loadMinioConfig(t)
	cfg.HAMode = "single"

	ts := startMinioTestServer(t, cfg)
	hatest.RequireNoHALease(t, ts.Backend())
	hatest.RequireAcquireRelease(t, ts.Client, "minio-ha-single")
}

func TestMinioHAAutoPromotesToFailover(t *testing.T) {
	cfgA := loadMinioConfig(t)
	cfgA.HAMode = "auto"
	cfgA.HALeaseTTL = 5 * time.Second
	serverA := startMinioTestServer(t, cfgA)

	hatest.RequireNoHALease(t, serverA.Backend())

	cfgB := cfgA
	serverB := startMinioTestServer(t, cfgB)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	active, cli, err := hatest.FindActiveServer(ctx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	defer cli.Close()
	hatest.RequireAcquireRelease(t, cli, "minio-ha-auto")

	passive := serverA
	if active == serverA {
		passive = serverB
	}
	hatest.WaitForPassiveNode(t, passive, 30*time.Second)
}
