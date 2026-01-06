//go:build integration && mem && lq

package memlq

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
)

func TestMemQueueTxnRestartAcrossNodes(t *testing.T) {
	for _, mode := range memQueueModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			runMemQueueTxnRestartAcrossNodes(t, mode)
		})
	}
}

func runMemQueueTxnRestartAcrossNodes(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-txn-restart", 40*time.Second)

	setup := func(t *testing.T) (*lockd.TestServer, *lockd.TestServer, func(testing.TB) *lockd.TestServer, time.Duration) {
		backend := memorybackend.New()
		cfg := buildMemQueueConfig(t, mode.queueWatch)
		bundlePath := cryptotest.SharedMTLSClientBundlePath(t)
		if bundlePath == "" {
			cfg.DisableMTLS = true
		}
		serverTC := startMemQueueServerWithBackend(t, cfg, backend)
		serverRM := startMemQueueServerWithBackend(t, cfg, backend)
		restart := func(t testing.TB) *lockd.TestServer {
			return startMemQueueServerWithBackend(t, cfg, backend)
		}
		return serverTC, serverRM, restart, queuetestutil.ReplayDeadlineForStore(cfg.Store)
	}

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-restart-commit", 20*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})

	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-restart-rollback", 20*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})

	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-restart-stateful-commit", 25*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})

	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-restart-stateful-rollback", 25*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-restart-mixed-commit", 30*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, true, deadline)
	})

	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-restart-mixed-rollback", 30*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, false, deadline)
	})
}
