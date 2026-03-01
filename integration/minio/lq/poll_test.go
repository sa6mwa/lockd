//go:build integration && minio && lq

package miniointegration

import (
	"context"
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestMinioQueuePollingBasics(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-poll-basics", 20*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	ts := startMinioQueueServer(t, cfg)
	cli := ts.Client

	t.Run("AckRemovesMessage", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-poll-ack", 10*time.Second)
		queuetestutil.RunQueueAckScenario(t, cli, queuetestutil.QueueName("minio-poll-ack"), []byte("minio poll ack"))
	})

	t.Run("NackRedelivery", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-poll-nack", 10*time.Second)
		queuetestutil.RunQueueNackScenario(t, cli, queuetestutil.QueueName("minio-poll-nack"), []byte("minio poll nack"))
	})

	t.Run("ObservabilityReadOnly", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "minio-poll-observability", 15*time.Second)
		queuetestutil.RunQueueObservabilityReadOnlyScenario(t, cli, queuetestutil.QueueName("minio-poll-observability"))
	})
}

func TestMinioQueuePollingIdleEnqueueDoesNotPoll(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-poll-idle", 20*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	queue := queuetestutil.QueueName("minio-poll-idle")

	seedServer := startMinioQueueServer(t, cfg)
	seedClient := seedServer.Client
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-one"))
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-two"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := seedServer.Stop(ctx); err != nil {
		t.Fatalf("stop seed server: %v", err)
	}

	ts, capture := startMinioQueueServerWithCapture(t, cfg)
	cli := ts.Client

	consumerA := queuetestutil.QueueOwner("minio-consumer-a")
	msgA := queuetestutil.MustDequeueMessage(t, cli, queue, consumerA, 1, 5*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgA)
	ackCtxA, ackCancelA := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msgA.Ack(ackCtxA); err != nil {
		t.Fatalf("ack consumer A: %v", err)
	}
	ackCancelA()

	consumerB := queuetestutil.QueueOwner("minio-consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, consumerB, 1, 5*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ackCtxB, ackCancelB := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msgB.Ack(ackCtxB); err != nil {
		t.Fatalf("ack consumer B: %v", err)
	}
	ackCancelB()

	time.Sleep(150 * time.Millisecond)
	baseline := capture.Len()

	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("post-idle"))

	time.Sleep(500 * time.Millisecond)
	if polls := capture.CountSince(baseline, "queue.dispatcher.poll.begin"); polls > 0 {
		t.Fatalf("unexpected dispatcher polling while idle; observed %d poll begin events", polls)
	}
}
