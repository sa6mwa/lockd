//go:build integration && minio && lq

package miniointegration

import (
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestMinioStartConsumerSmoke(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-start-consumer-smoke", 2*time.Minute)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	ts := startMinioQueueServer(t, cfg)

	queuetestutil.RunStartConsumerSmoke(t, ts.Client, queuetestutil.StartConsumerSmokeOptions{
		Label:   "minio-start-consumer",
		Timeout: 90 * time.Second,
	})
}

func TestMinioStartConsumerStateSaveRegression(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-start-consumer-state-save", 2*time.Minute)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	ts := startMinioQueueServer(t, cfg)

	queuetestutil.RunStartConsumerStateSaveRegression(t, ts.Client, queuetestutil.StartConsumerStateSaveRegressionOptions{
		Label:   "minio-start-consumer-state-save",
		Timeout: 90 * time.Second,
	})
}
