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

func TestMemQueueTxnReplayWakeAcrossNodes(t *testing.T) {
	for _, mode := range memQueueModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			runMemQueueTxnReplayWakeAcrossNodes(t, mode)
		})
	}
}

func runMemQueueTxnReplayWakeAcrossNodes(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-txn-replay-wake", 30*time.Second)

	backend := memorybackend.New()
	cfg := buildMemQueueConfig(t, mode.queueWatch)
	cfg.HAMode = "concurrent"
	cfg.QueuePollInterval = 5 * time.Second
	cfg.QueuePollJitter = 0
	cfg.QueueResilientPollInterval = 5 * time.Second
	cfg.SweeperInterval = time.Minute

	bundlePath := cryptotest.SharedMTLSClientBundlePath(t)
	if bundlePath == "" {
		cfg.DisableMTLS = true
	}

	serverTC := startMemQueueServerWithBackend(t, cfg, backend)
	serverRM := startMemQueueServerWithBackend(t, cfg, backend)
	restart := func(t testing.TB) *lockd.TestServer {
		return startMemQueueServerWithBackend(t, cfg, backend)
	}

	queuetestutil.RunQueueTxnDecisionReplayWakeScenario(
		t,
		serverTC,
		serverRM,
		restart,
		queuetestutil.ReplayDeadlineForStore(cfg.Store),
		2*time.Second,
	)
}
