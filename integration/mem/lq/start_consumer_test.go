//go:build integration && mem && lq

package memlq

import (
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestMemStartConsumerSmoke(t *testing.T) {
	for _, mode := range memQueueModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			queuetestutil.InstallWatchdog(t, "mem-start-consumer-"+mode.name, 30*time.Second)
			cfg := buildMemQueueConfig(t, mode.queueWatch)
			ts := startMemQueueServer(t, cfg)
			queuetestutil.RunStartConsumerSmoke(t, ts.Client, queuetestutil.StartConsumerSmokeOptions{
				Label:   "mem-start-consumer-" + mode.name,
				Timeout: 20 * time.Second,
			})
		})
	}
}

func TestMemStartConsumerStateSaveRegression(t *testing.T) {
	for _, mode := range memQueueModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			queuetestutil.InstallWatchdog(t, "mem-start-consumer-state-save-"+mode.name, 30*time.Second)
			cfg := buildMemQueueConfig(t, mode.queueWatch)
			ts := startMemQueueServer(t, cfg)
			queuetestutil.RunStartConsumerStateSaveRegression(t, ts.Client, queuetestutil.StartConsumerStateSaveRegressionOptions{
				Label:   "mem-start-consumer-state-save-" + mode.name,
				Timeout: 20 * time.Second,
			})
		})
	}
}
