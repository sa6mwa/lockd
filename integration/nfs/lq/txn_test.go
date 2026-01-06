//go:build integration && nfs && lq

package nfslq

import (
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func runNFSQueueTxnDecision(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "nfs-txn", 15*time.Second)
	root := prepareNFSQueueRoot(t)
	cfg := buildNFSQueueConfig(t, root, nfsQueueOptions{
		PollInterval:      100 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})
	ts := startNFSQueueServer(t, cfg)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "nfs-txn-commit", 8*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, true)
	})

	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "nfs-txn-rollback", 8*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, false)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "nfs-txn-mixed-commit", 15*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, true)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "nfs-txn-mixed-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, false)
	})

	t.Run("ReplayCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "nfs-txn-replay-commit", 15*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startNFSQueueServerWithOptions, true)
	})
	t.Run("ReplayRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "nfs-txn-replay-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startNFSQueueServerWithOptions, false)
	})
}
