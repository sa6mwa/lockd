//go:build integration && disk && lq

package disklq

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
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

	serverA := startDiskQueueServer(t, cfg)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	serverB := startDiskQueueServer(t, cfgB)

	stop := func(ts *lockd.TestServer) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}
	t.Cleanup(func() { stop(serverB) })
	t.Cleanup(func() { stop(serverA) })

	cryptotest.RegisterRM(t, serverB, serverA)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-commit", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, serverB, serverA, true)
	})

	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, serverB, serverA, false)
	})

	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-stateful-commit", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, serverB, serverA, true)
	})

	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-stateful-rollback", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, serverB, serverA, false)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-mixed-commit", 22*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, serverB, serverA, true)
	})

	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-txn-fanout-mixed-rollback", 22*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, serverB, serverA, false)
	})
}
