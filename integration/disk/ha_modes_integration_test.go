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
	hatest.RequireNoHAMembers(t, ts.Backend())
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

func TestDiskHASingleModeFencesAutoPeer(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfgSingle := buildDiskConfig(t, root, 0)
	cfgSingle.HAMode = "single"
	single := startDiskTestServer(t, cfgSingle)

	hatest.RequireNoHALease(t, single.Backend())
	hatest.RequireNoHAMembers(t, single.Backend())

	cfgAuto := buildDiskConfig(t, root, 0)
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 2 * time.Second
	auto := startDiskTestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 15*time.Second)
	hatest.RequireNoHALease(t, auto.Backend())
	hatest.RequireAcquireRelease(t, single.Client, "disk-ha-single-fence")
}

func TestDiskHASingleCrashExpiryPromotesAutoAndFencesRejoin(t *testing.T) {
	ensureDiskRootEnv(t)
	root := prepareDiskRoot(t, "")
	cfgSingle := buildDiskConfig(t, root, 0)
	cfgSingle.HAMode = "single"
	single := startDiskTestServer(t, cfgSingle)

	cfgAuto := buildDiskConfig(t, root, 0)
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 2 * time.Second
	auto := startDiskTestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 15*time.Second)
	if err := single.Abort(context.Background()); err != nil {
		t.Fatalf("abort single: %v", err)
	}

	activeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := hatest.WaitForActive(activeCtx, auto); err != nil {
		t.Fatalf("wait for auto active: %v", err)
	}

	rejoin := startDiskTestServer(t, cfgSingle)
	hatest.WaitForPassiveNode(t, rejoin, 15*time.Second)
	hatest.RequireNoHAMembers(t, rejoin.Backend())
	hatest.RequireAcquireRelease(t, auto.Client, "disk-ha-auto-after-crash")
}
