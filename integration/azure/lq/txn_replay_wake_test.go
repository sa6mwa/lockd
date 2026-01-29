//go:build integration && azure && lq

package azureintegration

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestAzureQueueTxnReplayWakeAcrossNodes(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-txn-replay-wake", 90*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      5 * time.Second,
		PollJitter:        0,
		ResilientInterval: 5 * time.Second,
	})
	cfg.HAMode = "concurrent"
	cfg.SweeperInterval = time.Minute

	serverTC := startAzureQueueServer(t, cfg)
	serverRM := startAzureQueueServer(t, cfg)
	restart := func(t testing.TB) *lockd.TestServer {
		return startAzureQueueServer(t, cfg)
	}

	queuetestutil.RunQueueTxnDecisionReplayWakeScenario(
		t,
		serverTC,
		serverRM,
		restart,
		queuetestutil.ReplayDeadlineForStore(cfg.Store),
		2*time.Second,
	)
}
