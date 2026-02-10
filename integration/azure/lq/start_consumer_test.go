//go:build integration && azure && lq

package azureintegration

import (
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestAzureStartConsumerSmoke(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-start-consumer-smoke", 2*time.Minute)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	ts := startAzureQueueServer(t, cfg)
	ensureAzureQueueWritableOrSkip(t, cfg, ts.Client)

	queuetestutil.RunStartConsumerSmoke(t, ts.Client, queuetestutil.StartConsumerSmokeOptions{
		Label:   "azure-start-consumer",
		Timeout: 90 * time.Second,
		QueueSetup: func(sharedQueue, queueA, queueB string) {
			scheduleAzureQueueCleanup(t, cfg, sharedQueue)
			scheduleAzureQueueCleanup(t, cfg, queueA)
			scheduleAzureQueueCleanup(t, cfg, queueB)
		},
	})
}

func TestAzureStartConsumerStateSaveRegression(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-start-consumer-state-save", 2*time.Minute)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	ts := startAzureQueueServer(t, cfg)
	ensureAzureQueueWritableOrSkip(t, cfg, ts.Client)

	queuetestutil.RunStartConsumerStateSaveRegression(t, ts.Client, queuetestutil.StartConsumerStateSaveRegressionOptions{
		Label:   "azure-start-consumer-state-save",
		Timeout: 90 * time.Second,
		QueueSetup: func(queue string) {
			scheduleAzureQueueCleanup(t, cfg, queue)
		},
	})
}
