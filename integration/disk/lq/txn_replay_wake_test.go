//go:build integration && disk && lq

package disklq

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestDiskQueueTxnReplayWakeAcrossNodes(t *testing.T) {
	t.Run("FSNotify", func(t *testing.T) {
		runDiskQueueTxnReplayWakeAcrossNodes(t, true)
	})

	t.Run("Polling", func(t *testing.T) {
		runDiskQueueTxnReplayWakeAcrossNodes(t, false)
	})
}

func runDiskQueueTxnReplayWakeAcrossNodes(t *testing.T, notify bool) {
	queuetestutil.InstallWatchdog(t, "disk-txn-replay-wake", 60*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       notify,
		PollInterval:      5 * time.Second,
		PollJitter:        0,
		ResilientInterval: 5 * time.Second,
	})
	cfg.SweeperInterval = time.Minute

	bundlePath := cryptotest.SharedMTLSClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	server := startDiskQueueServer(t, cfg)
	restart := func(t testing.TB) *lockd.TestServer {
		return startDiskQueueServer(t, cfg)
	}

	queuetestutil.RunQueueTxnDecisionReplayWakeScenario(
		t,
		server,
		server,
		restart,
		queuetestutil.ReplayDeadlineForStore(cfg.Store),
		2*time.Second,
	)
}
