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
	cfg.HASinglePresenceTTL = 3 * time.Second

	ts := startMemTestServerWithBackend(t, cfg, backend)
	hatest.RequireNoHALease(t, ts.Backend())
	hatest.WaitForHAMemberMode(t, ts.Backend(), "single", 15*time.Second)
	hatest.RequireAcquireRelease(t, ts.Client, "mem-ha-single")
}

func TestMemHAAutoPromotesToFailover(t *testing.T) {
	backend := memorybackend.New()

	cfgA := buildMemConfig(t)
	cfgA.HAMode = "auto"
	cfgA.HALeaseTTL = 5 * time.Second
	serverA := startMemTestServerWithBackend(t, cfgA, backend)

	hatest.RequireNoHALease(t, serverA.Backend())

	cfgB := buildMemConfig(t)
	cfgB.HAMode = "auto"
	cfgB.HALeaseTTL = 5 * time.Second
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

func TestMemHASingleModeFencesAutoPeer(t *testing.T) {
	backend := memorybackend.New()

	cfgSingle := buildMemConfig(t)
	cfgSingle.HAMode = "single"
	cfgSingle.HASinglePresenceTTL = 3 * time.Second
	single := startMemTestServerWithBackend(t, cfgSingle, backend)

	hatest.RequireNoHALease(t, single.Backend())
	hatest.WaitForHAMemberMode(t, single.Backend(), "single", 15*time.Second)

	cfgAuto := buildMemConfig(t)
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startMemTestServerWithBackend(t, cfgAuto, backend)

	hatest.WaitForPassiveNode(t, auto, 15*time.Second)
	hatest.RequireNoHALease(t, auto.Backend())
	hatest.RequireAcquireRelease(t, single.Client, "mem-ha-single-fence")
}

func TestMemHASingleModeFencesMultipleAutoPeers(t *testing.T) {
	backend := memorybackend.New()

	cfgSingle := buildMemConfig(t)
	cfgSingle.HAMode = "single"
	cfgSingle.HASinglePresenceTTL = 3 * time.Second
	cfgSingle.SelfEndpoint = "http://single-z"
	cfgSingle.TCDisableAuth = true
	single := startMemTestServerWithBackend(t, cfgSingle, backend)

	hatest.RequireNoHALease(t, single.Backend())
	hatest.WaitForHAMemberMode(t, single.Backend(), "single", 15*time.Second)

	cfgAutoA := buildMemConfig(t)
	cfgAutoA.HAMode = "auto"
	cfgAutoA.HALeaseTTL = 5 * time.Second
	cfgAutoA.SelfEndpoint = "http://auto-a"
	cfgAutoA.TCDisableAuth = true
	autoA := startMemTestServerWithBackend(t, cfgAutoA, backend)

	cfgAutoB := buildMemConfig(t)
	cfgAutoB.HAMode = "auto"
	cfgAutoB.HALeaseTTL = 5 * time.Second
	cfgAutoB.SelfEndpoint = "http://auto-b"
	cfgAutoB.TCDisableAuth = true
	autoB := startMemTestServerWithBackend(t, cfgAutoB, backend)

	hatest.WaitForPassiveNode(t, autoA, 15*time.Second)
	hatest.WaitForPassiveNode(t, autoB, 15*time.Second)
	hatest.RequireNoHALease(t, autoA.Backend())
	hatest.RequireAcquireRelease(t, single.Client, "mem-ha-single-fence-multi-auto")
}

func TestMemHASingleCrashExpiryPromotesAutoAndFencesRejoin(t *testing.T) {
	backend := memorybackend.New()

	cfgSingle := buildMemConfig(t)
	cfgSingle.HAMode = "single"
	cfgSingle.HASinglePresenceTTL = 1500 * time.Millisecond
	single := startMemTestServerWithBackend(t, cfgSingle, backend)

	cfgAuto := buildMemConfig(t)
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startMemTestServerWithBackend(t, cfgAuto, backend)

	hatest.WaitForPassiveNode(t, auto, 15*time.Second)
	if err := single.Abort(context.Background()); err != nil {
		t.Fatalf("abort single: %v", err)
	}

	activeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := hatest.WaitForActive(activeCtx, auto); err != nil {
		t.Fatalf("wait for auto active: %v", err)
	}

	rejoin := startMemTestServerWithBackend(t, cfgSingle, backend)
	hatest.WaitForPassiveNode(t, rejoin, 15*time.Second)
	hatest.RequireNoLiveHAMemberMode(t, rejoin.Backend(), "single")
	hatest.RequireAcquireRelease(t, auto.Client, "mem-ha-auto-after-crash")
}
