//go:build integration && mem && !lq && !query && !crypto

package memintegration

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/hatest"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
)

func TestMemHASingleModeSkipsActiveLease(t *testing.T) {
	backend := memorybackend.New()
	cfg := buildMemConfig(t)
	cfg.HAMode = "single"

	ts := startMemTestServerWithBackend(t, cfg, backend)
	hatest.RequireNoHALease(t, ts.Backend())
	hatest.RequireAcquireRelease(t, ts.Client, "mem-ha-single")
}

func TestMemHAAutoPromotesToFailover(t *testing.T) {
	backend := memorybackend.New()

	cfgA := buildMemConfig(t)
	cfgA.HAMode = "auto"
	cfgA.HALeaseTTL = 2 * time.Second
	serverA := startMemTestServerWithBackend(t, cfgA, backend)

	hatest.RequireNoHALease(t, serverA.Backend())

	cfgB := buildMemConfig(t)
	cfgB.HAMode = "auto"
	cfgB.HALeaseTTL = 2 * time.Second
	serverB := startMemTestServerWithBackend(t, cfgB, backend)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	active, cli, err := hatest.FindActiveServer(ctx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	defer cli.Close()
	hatest.RequireAcquireRelease(t, cli, "mem-ha-auto")

	passive := serverA
	if active == serverA {
		passive = serverB
	}
	hatest.WaitForPassiveNode(t, passive, 15*time.Second)
}
