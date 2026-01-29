//go:build integration && disk && lq

package disklq

import (
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestDiskQueueTxnFanoutAcrossNodes(t *testing.T) {
	t.Run("FSNotify", func(t *testing.T) {
		runDiskQueueTxnFanoutAcrossNodes(t, true)
	})

	t.Run("Polling", func(t *testing.T) {
		runDiskQueueTxnFanoutAcrossNodes(t, false)
	})
}

func runDiskQueueTxnFanoutAcrossNodes(t *testing.T, notify bool) {
	queuetestutil.InstallWatchdog(t, "disk-txn-fanout", 40*time.Second)
	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       notify,
		PollInterval:      50 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	server := startDiskQueueServer(t, cfg)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-commit", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, server, server, true)
	})

	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, server, server, false)
	})

	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-stateful-commit", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, server, server, true)
	})

	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-stateful-rollback", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, server, server, false)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-mixed-commit", 22*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, server, server, true)
	})

	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-mixed-rollback", 22*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, server, server, false)
	})
}
