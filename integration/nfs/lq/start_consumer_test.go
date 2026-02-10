//go:build integration && nfs && lq

package nfslq

import (
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestNFSStartConsumerSmoke(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "nfs-start-consumer-smoke", 45*time.Second)

	root := prepareNFSQueueRoot(t)
	cfg := buildNFSQueueConfig(t, root, nfsQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	ts := startNFSQueueServer(t, cfg)
	queuetestutil.RunStartConsumerSmoke(t, ts.Client, queuetestutil.StartConsumerSmokeOptions{
		Label:   "nfs-start-consumer",
		Timeout: 30 * time.Second,
	})
}

func TestNFSStartConsumerStateSaveRegression(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "nfs-start-consumer-state-save", 45*time.Second)

	root := prepareNFSQueueRoot(t)
	cfg := buildNFSQueueConfig(t, root, nfsQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	ts := startNFSQueueServer(t, cfg)
	queuetestutil.RunStartConsumerStateSaveRegression(t, ts.Client, queuetestutil.StartConsumerStateSaveRegressionOptions{
		Label:   "nfs-start-consumer-state-save",
		Timeout: 30 * time.Second,
	})
}
