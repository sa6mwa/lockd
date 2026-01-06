//go:build integration && azure && lq

package azureintegration

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestAzureQueueTxnDecision(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-txn", 60*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      150 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 1 * time.Second,
	})
	ts := startAzureQueueServer(t, cfg)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-commit", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, true)
	})

	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, false)
	})

	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-stateful-commit", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, true)
	})

	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-stateful-rollback", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, false)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-mixed-commit", 20*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, true)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-mixed-rollback", 20*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, false)
	})

	t.Run("ReplayCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-replay-commit", 25*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startAzureQueueServerWithOptions, true)
	})
	t.Run("ReplayRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-replay-rollback", 25*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startAzureQueueServerWithOptions, false)
	})
}

func TestAzureQueueTxnFanoutAcrossNodes(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-txn-fanout", 90*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      150 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 1 * time.Second,
	})
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	tsA := startAzureQueueServer(t, cfg)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	tsB := startAzureQueueServer(t, cfgB)

	cryptotest.RegisterRM(t, tsB, tsA)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-fanout-commit", 25*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, tsB, tsA, true)
	})
	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-fanout-rollback", 25*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, tsB, tsA, false)
	})
	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-fanout-stateful-commit", 30*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, tsB, tsA, true)
	})
	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-fanout-stateful-rollback", 30*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, tsB, tsA, false)
	})
	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-fanout-mixed-commit", 35*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, tsB, tsA, true)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-fanout-mixed-rollback", 35*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, tsB, tsA, false)
	})
}

func TestAzureQueueTxnRestartAcrossNodes(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-txn-restart", 120*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      150 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 1 * time.Second,
	})
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	setup := func(t *testing.T) (*lockd.TestServer, *lockd.TestServer, func(testing.TB) *lockd.TestServer, time.Duration) {
		serverTC := startAzureQueueServer(t, cfg)
		serverRM := startAzureQueueServer(t, cfg)
		restart := func(t testing.TB) *lockd.TestServer {
			return startAzureQueueServer(t, cfg)
		}
		return serverTC, serverRM, restart, queuetestutil.ReplayDeadlineForStore(cfg.Store)
	}

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-restart-commit", 35*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})
	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-restart-rollback", 35*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})
	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-restart-stateful-commit", 45*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})
	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-restart-stateful-rollback", 45*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})
	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-restart-mixed-commit", 55*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, true, deadline)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-txn-restart-mixed-rollback", 55*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, false, deadline)
	})
}
