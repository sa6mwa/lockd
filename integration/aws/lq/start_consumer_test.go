//go:build integration && aws && lq

package awsintegration

import (
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestAWSStartConsumerSmoke(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-start-consumer-smoke", 2*time.Minute)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
		SweeperInterval:   5 * time.Minute,
	})
	ts := startAWSQueueServer(t, cfg)
	ensureAWSQueueWritableOrSkip(t, ts.Client)

	queuetestutil.RunStartConsumerSmoke(t, ts.Client, queuetestutil.StartConsumerSmokeOptions{
		Label:   "aws-start-consumer",
		Timeout: 90 * time.Second,
	})
}

func TestAWSStartConsumerStateSaveRegression(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-start-consumer-state-save", 2*time.Minute)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
		SweeperInterval:   5 * time.Minute,
	})
	ts := startAWSQueueServer(t, cfg)
	ensureAWSQueueWritableOrSkip(t, ts.Client)

	queuetestutil.RunStartConsumerStateSaveRegression(t, ts.Client, queuetestutil.StartConsumerStateSaveRegressionOptions{
		Label:   "aws-start-consumer-state-save",
		Timeout: 90 * time.Second,
	})
}
