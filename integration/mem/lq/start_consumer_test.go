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

func TestMemStartConsumerAutoAckRegression(t *testing.T) {
	for _, mode := range memQueueModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			queuetestutil.InstallWatchdog(t, "mem-start-consumer-auto-ack-"+mode.name, 30*time.Second)
			cfg := buildMemQueueConfig(t, mode.queueWatch)
			ts := startMemQueueServer(t, cfg)
			queuetestutil.RunStartConsumerAutoAckRegression(t, ts.Client, queuetestutil.StartConsumerAutoAckRegressionOptions{
				Label:   "mem-start-consumer-auto-ack-" + mode.name,
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

func TestMemStartConsumerHandlerFailureMatrix(t *testing.T) {
	for _, mode := range memQueueModes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			queuetestutil.InstallWatchdog(t, "mem-start-consumer-failure-"+mode.name, 45*time.Second)
			cfg := buildMemQueueConfig(t, mode.queueWatch)
			ts := startMemQueueServer(t, cfg)
			result := queuetestutil.RunStartConsumerFailureMatrix(t, ts.Client, queuetestutil.StartConsumerFailureMatrixOptions{
				Label:   "mem-start-consumer-failure-" + mode.name,
				Timeout: 30 * time.Second,
			})
			t.Logf("immediate=%+v deferred=%+v", result.Immediate, result.Deferred)
		})
	}
}
