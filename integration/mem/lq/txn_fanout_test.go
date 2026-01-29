//go:build integration && mem && lq

package memlq

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
)

func TestMemQueueTxnFanoutAcrossNodes(t *testing.T) {
	for _, mode := range memQueueModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			runMemQueueTxnFanoutAcrossNodes(t, mode)
		})
	}
}

func runMemQueueTxnFanoutAcrossNodes(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-txn-fanout", 30*time.Second)

	backend := memorybackend.New()
	cfg := buildMemQueueConfig(t, mode.queueWatch)
	cfg.HAMode = "concurrent"
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	serverA := startMemQueueServerWithBackend(t, cfg, backend)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	serverB := startMemQueueServerWithBackend(t, cfgB, backend)

	stop := func(ts *lockd.TestServer) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}
	t.Cleanup(func() { stop(serverB) })
	t.Cleanup(func() { stop(serverA) })

	cryptotest.RegisterRM(t, serverB, serverA)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-fanout-commit", 12*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, serverB, serverA, true)
	})

	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-fanout-rollback", 12*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, serverB, serverA, false)
	})

	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-fanout-stateful-commit", 15*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, serverB, serverA, true)
	})

	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-fanout-stateful-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, serverB, serverA, false)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-fanout-mixed-commit", 20*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, serverB, serverA, true)
	})

	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-fanout-mixed-rollback", 20*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, serverB, serverA, false)
	})
}
