//go:build integration && disk && !lq && !query && !crypto

package diskintegration

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/hatest"
)

func TestDiskHASingleModeSkipsActiveLease(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cfg.HAMode = "single"

	ts := startDiskTestServer(t, cfg)
	hatest.RequireNoHALease(t, ts.Backend())
	hatest.RequireAcquireRelease(t, ts.Client, "disk-ha-single")
}

func TestDiskHAAutoPromotesToFailover(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfgA := buildDiskConfig(t, root, 0)
	cfgA.HAMode = "auto"
	cfgA.HALeaseTTL = 2 * time.Second
	serverA := startDiskTestServer(t, cfgA)

	hatest.RequireNoHALease(t, serverA.Backend())

	cfgB := buildDiskConfig(t, root, 0)
	cfgB.HAMode = "auto"
	cfgB.HALeaseTTL = 2 * time.Second
	serverB := startDiskTestServer(t, cfgB)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	active, cli, err := hatest.FindActiveServer(ctx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	defer cli.Close()
	hatest.RequireAcquireRelease(t, cli, "disk-ha-auto")

	passive := serverA
	if active == serverA {
		passive = serverB
	}
	hatest.WaitForPassiveNode(t, passive, 15*time.Second)
}
