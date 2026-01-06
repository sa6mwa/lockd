//go:build integration && disk && lq

package disklq

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestDiskQueueTxnRestartAcrossNodes(t *testing.T) {
	t.Run("FSNotify", func(t *testing.T) {
		runDiskQueueTxnRestartAcrossNodes(t, true)
	})

	t.Run("Polling", func(t *testing.T) {
		runDiskQueueTxnRestartAcrossNodes(t, false)
	})
}

func runDiskQueueTxnRestartAcrossNodes(t *testing.T, notify bool) {
	queuetestutil.InstallWatchdog(t, "disk-txn-restart", 60*time.Second)

	setup := func(t *testing.T) (*lockd.TestServer, *lockd.TestServer, func(testing.TB) *lockd.TestServer, time.Duration) {
		root := prepareDiskQueueRoot(t)
		cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
			EnableWatch:       notify,
			PollInterval:      50 * time.Millisecond,
			PollJitter:        0,
			ResilientInterval: 500 * time.Millisecond,
		})
		bundlePath := cryptotest.SharedMTLSClientBundlePath(t)
		if bundlePath == "" {
			cfg.DisableMTLS = true
		}
		serverTC := startDiskQueueServer(t, cfg)
		serverRM := startDiskQueueServer(t, cfg)
		restart := func(t testing.TB) *lockd.TestServer {
			return startDiskQueueServer(t, cfg)
		}
		return serverTC, serverRM, restart, queuetestutil.ReplayDeadlineForStore(cfg.Store)
	}

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-restart-commit", 30*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})

	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-restart-rollback", 30*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})

	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-restart-stateful-commit", 35*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})

	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-restart-stateful-rollback", 35*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-restart-mixed-commit", 45*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, true, deadline)
	})

	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-restart-mixed-rollback", 45*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, false, deadline)
	})
}
