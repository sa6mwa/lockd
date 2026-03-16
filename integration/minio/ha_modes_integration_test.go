//go:build integration && minio

package miniointegration

import (
	"context"
	"path"
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/hatest"
	"pkt.systems/lockd/internal/uuidv7"
)

func TestMinioHASingleModeSkipsActiveLease(t *testing.T) {
	cfg := loadMinioConfig(t)
	cfg.Store = appendStorePath(t, cfg.Store, path.Join("ha-modes", uuidv7.NewString(), "single"))
	cfg.HAMode = "single"

	ts := startMinioTestServer(t, cfg)
	hatest.RequireNoHALease(t, ts.Backend())
	hatest.RequireAcquireRelease(t, ts.Client, "minio-ha-single")
}

func TestMinioHAAutoPromotesToFailover(t *testing.T) {
	cfgA := loadMinioConfig(t)
	cfgA.Store = appendStorePath(t, cfgA.Store, path.Join("ha-modes", uuidv7.NewString(), "auto"))
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
