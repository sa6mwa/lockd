//go:build integration && minio && lq

package miniointegration

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestMinioQueueTxnReplayWakeAcrossNodes(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-txn-replay-wake", 90*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      5 * time.Second,
		PollJitter:        0,
		ResilientInterval: 5 * time.Second,
	})
	cfg.SweeperInterval = time.Minute

	serverTC := startMinioQueueServer(t, cfg)
	serverRM := startMinioQueueServer(t, cfg)
	restart := func(t testing.TB) *lockd.TestServer {
		return startMinioQueueServer(t, cfg)
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
