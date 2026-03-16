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
	hatest.RequireAcquireRelease(t, ts.Client, "nfs-ha-single")
}

func TestNFSHAAutoPromotesToFailover(t *testing.T) {
	root := prepareNFSRoot(t, "")
	cfgA := buildNFSConfig(t, root, 0)
	cfgA.HAMode = "auto"
	cfgA.HALeaseTTL = 2 * time.Second
	serverA := startNFSTestServer(t, cfgA)

	hatest.RequireNoHALease(t, serverA.Backend())

	cfgB := buildNFSConfig(t, root, 0)
	cfgB.HAMode = "auto"
	cfgB.HALeaseTTL = 2 * time.Second
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
