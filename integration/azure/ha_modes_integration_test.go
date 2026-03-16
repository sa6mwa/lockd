//go:build integration && azure

package azureintegration

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/hatest"
)

func TestAzureHASingleModeSkipsActiveLease(t *testing.T) {
	cfg := loadAzureConfig(t)
	cfg.HAMode = "single"

	ts := startAzureTestServer(t, cfg)
	hatest.RequireNoHALease(t, ts.Backend())
	hatest.RequireAcquireRelease(t, ts.Client, "azure-ha-single")
}

func TestAzureHAAutoPromotesToFailover(t *testing.T) {
	cfgA := loadAzureConfig(t)
	cfgA.HAMode = "auto"
	cfgA.HALeaseTTL = 5 * time.Second
	serverA := startAzureTestServer(t, cfgA)

	hatest.RequireNoHALease(t, serverA.Backend())

	cfgB := cfgA
	serverB := startAzureTestServer(t, cfgB)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	active, cli, err := hatest.FindActiveServer(ctx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	defer cli.Close()
	hatest.RequireAcquireRelease(t, cli, "azure-ha-auto")

	passive := serverA
	if active == serverA {
		passive = serverB
	}
	hatest.WaitForPassiveNode(t, passive, 30*time.Second)
}
