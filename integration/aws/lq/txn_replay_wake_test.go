//go:build integration && aws && lq

package awsintegration

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestAWSQueueTxnReplayWakeAcrossNodes(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-txn-replay-wake", 90*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      5 * time.Second,
		PollJitter:        0,
		ResilientInterval: 5 * time.Second,
		SweeperInterval:   time.Minute,
	})
	cfg.HAMode = "concurrent"

	serverTC := startAWSQueueServer(t, cfg)
	serverRM := startAWSQueueServer(t, cfg)
	restart := func(t testing.TB) *lockd.TestServer {
		return startAWSQueueServer(t, cfg)
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
