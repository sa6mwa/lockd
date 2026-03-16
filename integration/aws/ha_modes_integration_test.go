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

	ts := startAWSTestServer(t, cfg)
	hatest.RequireNoHALease(t, ts.Backend())
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
