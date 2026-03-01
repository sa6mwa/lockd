//go:build integration && aws && lq

package awsintegration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestAWSQueuePollingBasics(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-poll-basics", 60*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
		SweeperInterval:   5 * time.Minute,
	})

	ts := startAWSQueueServer(t, cfg)
	cli := ts.Client
	ensureAWSQueueWritableOrSkip(t, cli)

	t.Run("AckRemovesMessage", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-poll-ack", 15*time.Second)
		queuetestutil.RunQueueAckScenario(t, cli, queuetestutil.QueueName("aws-poll-ack"), []byte("aws poll ack"))
	})

	t.Run("NackRedelivery", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-poll-nack", 15*time.Second)
		queuetestutil.RunQueueNackScenario(t, cli, queuetestutil.QueueName("aws-poll-nack"), []byte("aws poll nack"))
	})

	t.Run("ObservabilityReadOnly", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "aws-poll-observability", 20*time.Second)
		queuetestutil.RunQueueObservabilityReadOnlyScenario(t, cli, queuetestutil.QueueName("aws-poll-observability"))
	})
}

func TestAWSQueuePollingIdleEnqueueDoesNotPoll(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-poll-idle", 60*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
		SweeperInterval:   5 * time.Minute,
	})

	queue := queuetestutil.QueueName("aws-poll-idle")

	seedServer := startAWSQueueServer(t, cfg)
	seedClient := seedServer.Client
	ensureAWSQueueWritableOrSkip(t, seedClient)
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-one"))
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-two"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := seedServer.Stop(ctx); err != nil {
		t.Fatalf("stop seed server: %v", err)
	}

	ts, capture := startAWSQueueServerWithCapture(t, cfg)
	cli := ts.Client
	ensureAWSQueueWritableOrSkip(t, cli)

	consumerA := queuetestutil.QueueOwner("aws-consumer-a")
	msgA := queuetestutil.MustDequeueMessage(t, cli, queue, consumerA, 5, 15*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgA)
	ackCtxA, ackCancelA := context.WithTimeout(context.Background(), 5*time.Second)
	if err := msgA.Ack(ackCtxA); err != nil {
		t.Fatalf("ack consumer A: %v", err)
	}
	ackCancelA()

	consumerB := queuetestutil.QueueOwner("aws-consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, consumerB, 5, 15*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ackCtxB, ackCancelB := context.WithTimeout(context.Background(), 5*time.Second)
	if err := msgB.Ack(ackCtxB); err != nil {
		t.Fatalf("ack consumer B: %v", err)
	}
	ackCancelB()

	time.Sleep(1 * time.Second)
	baseline := capture.Len()

	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("post-idle"))

	time.Sleep(600 * time.Millisecond)
	lines := capture.LinesSince(baseline)
	polls := 0
	needle := fmt.Sprintf("\"queue\":\"%s\"", queue)
	for _, line := range lines {
		if strings.Contains(line, "\"msg\":\"queue.dispatcher.poll.begin\"") && strings.Contains(line, needle) {
			polls++
		}
	}
	t.Logf("idle queue polls observed: %d", polls)
	if polls > 2 {
		t.Fatalf("unexpected dispatcher polling while idle; observed %d poll begin events", polls)
	}
}
