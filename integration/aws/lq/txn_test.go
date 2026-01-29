//go:build integration && aws && lq

package awsintegration

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestAWSQueueTxnDecision(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-txn", 60*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      50 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})
	cfg.HAMode = "concurrent"
	ts := startAWSQueueServer(t, cfg)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-commit", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, true)
	})

	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, false)
	})

	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-stateful-commit", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, true)
	})

	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-stateful-rollback", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, false)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-mixed-commit", 20*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, true)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-mixed-rollback", 20*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, false)
	})

	t.Run("ReplayCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-replay-commit", 25*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startAWSQueueServerWithOptions, true)
	})
	t.Run("ReplayRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-replay-rollback", 25*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startAWSQueueServerWithOptions, false)
	})
}

func TestAWSQueueTxnFanoutAcrossNodes(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-txn-fanout", 90*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      50 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})
	cfg.HAMode = "concurrent"
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	tsA := startAWSQueueServer(t, cfg)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	tsB := startAWSQueueServer(t, cfgB)

	cryptotest.RegisterRM(t, tsB, tsA)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-fanout-commit", 25*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, tsB, tsA, true)
	})
	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-fanout-rollback", 25*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, tsB, tsA, false)
	})
	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-fanout-stateful-commit", 30*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, tsB, tsA, true)
	})
	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-fanout-stateful-rollback", 30*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, tsB, tsA, false)
	})
	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-fanout-mixed-commit", 35*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, tsB, tsA, true)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-fanout-mixed-rollback", 35*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, tsB, tsA, false)
	})
}

func TestAWSQueueTxnRestartAcrossNodes(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-txn-restart", 120*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      50 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})
	cfg.HAMode = "concurrent"
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	setup := func(t *testing.T) (*lockd.TestServer, *lockd.TestServer, func(testing.TB) *lockd.TestServer, time.Duration) {
		serverTC := startAWSQueueServer(t, cfg)
		serverRM := startAWSQueueServer(t, cfg)
		restart := func(t testing.TB) *lockd.TestServer {
			return startAWSQueueServer(t, cfg)
		}
		return serverTC, serverRM, restart, queuetestutil.ReplayDeadlineForStore(cfg.Store)
	}

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-restart-commit", 35*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})
	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-restart-rollback", 35*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})
	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-restart-stateful-commit", 45*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})
	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-restart-stateful-rollback", 45*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})
	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-restart-mixed-commit", 55*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, true, deadline)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-txn-restart-mixed-rollback", 55*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, false, deadline)
	})
}
