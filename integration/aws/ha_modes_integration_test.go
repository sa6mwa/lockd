//go:build integration && aws

package awsintegration

import (
	"context"
	"path"
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/hatest"
	"pkt.systems/lockd/internal/uuidv7"
)

func TestAWSHASingleModeSkipsActiveLease(t *testing.T) {
	cfg := loadAWSConfig(t)
	cfg.Store = appendStorePath(t, cfg.Store, path.Join("ha-modes", uuidv7.NewString(), "single"))
	cfg.HAMode = "single"
	cfg.HASinglePresenceTTL = 5 * time.Second

	ts := startAWSTestServer(t, cfg)
	hatest.RequireNoHALease(t, ts.Backend())
	hatest.WaitForHAMemberMode(t, ts.Backend(), "single", 30*time.Second)
	hatest.RequireAcquireRelease(t, ts.Client, "aws-ha-single")
}

func TestAWSHAAutoPromotesToFailover(t *testing.T) {
	cfgA := loadAWSConfig(t)
	cfgA.Store = appendStorePath(t, cfgA.Store, path.Join("ha-modes", uuidv7.NewString(), "auto"))
	cfgA.HAMode = "auto"
	cfgA.HALeaseTTL = 5 * time.Second
	serverA := startAWSTestServer(t, cfgA)

	hatest.RequireNoHALease(t, serverA.Backend())

	cfgB := cfgA
	serverB := startAWSTestServer(t, cfgB)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	active, cli, err := hatest.FindActiveServer(ctx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	defer cli.Close()
	hatest.RequireAcquireRelease(t, cli, "aws-ha-auto")

	passive := serverA
	if active == serverA {
		passive = serverB
	}
	hatest.WaitForPassiveNode(t, passive, 30*time.Second)
}

func TestAWSHASingleModeFencesAutoPeer(t *testing.T) {
	cfgSingle := loadAWSConfig(t)
	cfgSingle.Store = appendStorePath(t, cfgSingle.Store, path.Join("ha-modes", uuidv7.NewString(), "single-auto"))
	cfgSingle.HAMode = "single"
	cfgSingle.HASinglePresenceTTL = 5 * time.Second
	single := startAWSTestServer(t, cfgSingle)

	hatest.RequireNoHALease(t, single.Backend())
	hatest.WaitForHAMemberMode(t, single.Backend(), "single", 30*time.Second)

	cfgAuto := cfgSingle
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startAWSTestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 30*time.Second)
	hatest.RequireNoHALease(t, auto.Backend())
	hatest.RequireAcquireRelease(t, single.Client, "aws-ha-single-fence")
}

func TestAWSHASingleCrashExpiryPromotesAutoAndFencesRejoin(t *testing.T) {
	cfgSingle := loadAWSConfig(t)
	cfgSingle.Store = appendStorePath(t, cfgSingle.Store, path.Join("ha-modes", uuidv7.NewString(), "single-auto-crash"))
	cfgSingle.HAMode = "single"
	cfgSingle.HASinglePresenceTTL = 5 * time.Second
	single := startAWSTestServer(t, cfgSingle)

	cfgAuto := cfgSingle
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startAWSTestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 30*time.Second)
	if err := single.Abort(context.Background()); err != nil {
		t.Fatalf("abort single: %v", err)
	}

	activeCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := hatest.WaitForActive(activeCtx, auto); err != nil {
		t.Fatalf("wait for auto active: %v", err)
	}

	rejoin := startAWSTestServer(t, cfgSingle)
	hatest.WaitForPassiveNode(t, rejoin, 30*time.Second)
	hatest.RequireNoLiveHAMemberMode(t, rejoin.Backend(), "single")
	hatest.RequireAcquireRelease(t, auto.Client, "aws-ha-auto-after-crash")
}
