//go:build integration && minio && lq

package miniointegration

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestMinioQueueTxnDecision(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-txn", 20*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      50 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})
	ts := startMinioQueueServer(t, cfg)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-commit", 10*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, true)
	})
	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-rollback", 10*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, false)
	})

	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-stateful-commit", 12*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, true)
	})

	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-stateful-rollback", 12*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, false)
	})

	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-mixed-commit", 18*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, true)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-mixed-rollback", 18*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, false)
	})

	t.Run("ReplayCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-replay-commit", 20*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startMinioQueueServerWithOptions, true)
	})
	t.Run("ReplayRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-replay-rollback", 20*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startMinioQueueServerWithOptions, false)
	})
}

func TestMinioQueueTxnFanoutAcrossNodes(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-txn-fanout", 60*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      50 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	tsA := startMinioQueueServer(t, cfg)
	cfgB := cfg
	if bundlePath != "" {
		cfgB.TCClientBundlePath = bundlePath
	}
	tsB := startMinioQueueServer(t, cfgB)

	cryptotest.RegisterRM(t, tsB, tsA)

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-fanout-commit", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, tsB, tsA, true)
	})
	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-fanout-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnDecisionFanoutScenario(t, tsB, tsA, false)
	})
	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-fanout-stateful-commit", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, tsB, tsA, true)
	})
	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-fanout-stateful-rollback", 18*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionFanoutScenario(t, tsB, tsA, false)
	})
	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-fanout-mixed-commit", 22*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, tsB, tsA, true)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-fanout-mixed-rollback", 22*time.Second)
		queuetestutil.RunQueueTxnMixedKeyFanoutScenario(t, tsB, tsA, false)
	})
}

func TestMinioQueueTxnRestartAcrossNodes(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-txn-restart", 90*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      50 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	setup := func(t *testing.T) (*lockd.TestServer, *lockd.TestServer, func(testing.TB) *lockd.TestServer, time.Duration) {
		serverTC := startMinioQueueServer(t, cfg)
		serverRM := startMinioQueueServer(t, cfg)
		restart := func(t testing.TB) *lockd.TestServer {
			return startMinioQueueServer(t, cfg)
		}
		return serverTC, serverRM, restart, queuetestutil.ReplayDeadlineForStore(cfg.Store)
	}

	t.Run("Commit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-restart-commit", 25*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})
	t.Run("Rollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-restart-rollback", 25*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})
	t.Run("StatefulCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-restart-stateful-commit", 30*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, true, deadline)
	})
	t.Run("StatefulRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-restart-stateful-rollback", 30*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnStatefulDecisionRestartScenario(t, tc, rm, restart, false, deadline)
	})
	t.Run("MixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-restart-mixed-commit", 35*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, true, deadline)
	})
	t.Run("MixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-txn-restart-mixed-rollback", 35*time.Second)
		tc, rm, restart, deadline := setup(t)
		queuetestutil.RunQueueTxnMixedKeyRestartScenario(t, tc, rm, restart, false, deadline)
	})
}
