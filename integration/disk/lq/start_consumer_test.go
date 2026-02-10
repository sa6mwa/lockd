//go:build integration && disk && lq

package disklq

import (
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestDiskStartConsumerSmoke(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-start-consumer-smoke", 45*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	ts := startDiskQueueServer(t, cfg)
	queuetestutil.RunStartConsumerSmoke(t, ts.Client, queuetestutil.StartConsumerSmokeOptions{
		Label:   "disk-start-consumer",
		Timeout: 30 * time.Second,
	})
}

func TestDiskStartConsumerStateSaveRegression(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-start-consumer-state-save", 45*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	ts := startDiskQueueServer(t, cfg)
	queuetestutil.RunStartConsumerStateSaveRegression(t, ts.Client, queuetestutil.StartConsumerStateSaveRegressionOptions{
		Label:   "disk-start-consumer-state-save",
		Timeout: 30 * time.Second,
	})
}
