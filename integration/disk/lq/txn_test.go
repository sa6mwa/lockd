//go:build integration && disk && lq

package disklq

import (
	"context"
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func runDiskQueueTxnDecision(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-txn", 15*time.Second)
	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       true,
		PollInterval:      50 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})
	ts := startDiskQueueServer(t, cfg)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-commit", 8*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, true)
	})

	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-rollback", 8*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, false)
	})

	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-stateful-commit", 10*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, true)
	})

	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-stateful-rollback", 10*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, false)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-mixed-commit", 15*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, true)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-mixed-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, false)
	})

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = ts.Stop(stopCtx)
	stopCancel()

	t.Run("ReplayCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-replay-commit", 15*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startDiskQueueServer, true)
	})
	t.Run("ReplayRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-replay-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startDiskQueueServer, false)
	})
}
