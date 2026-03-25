//go:build integration && nfs && !lq && !query && !crypto

package nfsintegration

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/hatest"
)

func TestNFSHASingleModeSkipsActiveLease(t *testing.T) {
	root := prepareNFSRoot(t, "")
	cfg := buildNFSConfig(t, root, 0)
	cfg.HAMode = "single"

	ts := startNFSTestServer(t, cfg)
	hatest.RequireNoHALease(t, ts.Backend())
	hatest.RequireNoHAMembers(t, ts.Backend())
	hatest.RequireAcquireRelease(t, ts.Client, "nfs-ha-single")
}

func TestNFSHAAutoPromotesToFailover(t *testing.T) {
	root := prepareNFSRoot(t, "")
	cfgA := buildNFSConfig(t, root, 0)
	cfgA.HAMode = "auto"
	cfgA.HALeaseTTL = 5 * time.Second
	serverA := startNFSTestServer(t, cfgA)

	hatest.RequireNoHALease(t, serverA.Backend())

	cfgB := buildNFSConfig(t, root, 0)
	cfgB.HAMode = "auto"
	cfgB.HALeaseTTL = 5 * time.Second
	serverB := startNFSTestServer(t, cfgB)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	active, cli, err := hatest.FindActiveServer(ctx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	defer cli.Close()
	hatest.RequireAcquireRelease(t, cli, "nfs-ha-auto")

	passive := serverA
	if active == serverA {
		passive = serverB
	}
	hatest.WaitForPassiveNode(t, passive, 20*time.Second)
}

func TestNFSHASingleModeFencesAutoPeer(t *testing.T) {
	root := prepareNFSRoot(t, "")
	cfgSingle := buildNFSConfig(t, root, 0)
	cfgSingle.HAMode = "single"
	single := startNFSTestServer(t, cfgSingle)

	hatest.RequireNoHALease(t, single.Backend())
	hatest.RequireNoHAMembers(t, single.Backend())

	cfgAuto := buildNFSConfig(t, root, 0)
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startNFSTestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 20*time.Second)
	hatest.RequireNoHALease(t, auto.Backend())
	hatest.RequireAcquireRelease(t, single.Client, "nfs-ha-single-fence")
}

func TestNFSHASingleCrashExpiryPromotesAutoAndFencesRejoin(t *testing.T) {
	root := prepareNFSRoot(t, "")
	cfgSingle := buildNFSConfig(t, root, 0)
	cfgSingle.HAMode = "single"
	single := startNFSTestServer(t, cfgSingle)

	cfgAuto := buildNFSConfig(t, root, 0)
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startNFSTestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 20*time.Second)
	if err := single.Abort(context.Background()); err != nil {
		t.Fatalf("abort single: %v", err)
	}

	activeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := hatest.WaitForActive(activeCtx, auto); err != nil {
		t.Fatalf("wait for auto active: %v", err)
	}

	rejoin := startNFSTestServer(t, cfgSingle)
	hatest.WaitForPassiveNode(t, rejoin, 20*time.Second)
	hatest.RequireNoHAMembers(t, rejoin.Backend())
	hatest.RequireAcquireRelease(t, auto.Client, "nfs-ha-auto-after-crash")
}
