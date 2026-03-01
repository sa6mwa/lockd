//go:build integration && nfs && lq

package nfslq

import (
	"context"
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func runNFSQueuePollingBasics(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "nfs-poll-suite", 30*time.Second)

	root := prepareNFSQueueRoot(t)
	cfg := buildNFSQueueConfig(t, root, nfsQueueOptions{
		PollInterval:      200 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 1 * time.Second,
	})
	ts := startNFSQueueServer(t, cfg)
	cli := ts.Client

	t.Run("AckRemovesMessage", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "nfs-poll-ack", 10*time.Second)
		queuetestutil.RunQueueAckScenario(t, cli, queuetestutil.QueueName("nfs-poll-ack"), []byte("nfs ack payload"))
	})

	t.Run("NackRedelivery", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "nfs-poll-nack", 10*time.Second)
		queuetestutil.RunQueueNackScenario(t, cli, queuetestutil.QueueName("nfs-poll-nack"), []byte("nfs nack payload"))
	})

	t.Run("ObservabilityReadOnly", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "nfs-poll-observability", 15*time.Second)
		queuetestutil.RunQueueObservabilityReadOnlyScenario(t, cli, queuetestutil.QueueName("nfs-poll-observability"))
	})
}

func runNFSQueuePollingIdleEnqueueDoesNotPoll(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "nfs-poll-idle", 30*time.Second)

	root := prepareNFSQueueRoot(t)
	cfg := buildNFSQueueConfig(t, root, nfsQueueOptions{
		PollInterval:      200 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 1 * time.Second,
	})
	queue := queuetestutil.QueueName("nfs-poll-idle")

	seedServer := startNFSQueueServer(t, cfg)
	seedClient := seedServer.Client
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-one"))
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-two"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := seedServer.Stop(ctx); err != nil {
		t.Fatalf("stop seed server: %v", err)
	}

	ts, capture := startNFSQueueServerWithCapture(t, cfg)
	cli := ts.Client

	consumerA := queuetestutil.QueueOwner("consumer-a")
	msgA := queuetestutil.MustDequeueMessage(t, cli, queue, consumerA, 1, 5*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgA)
	ackCtxA, ackCancelA := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msgA.Ack(ackCtxA); err != nil {
		t.Fatalf("ack consumer A: %v", err)
	}
	ackCancelA()

	consumerB := queuetestutil.QueueOwner("consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, consumerB, 1, 5*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ackCtxB, ackCancelB := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msgB.Ack(ackCtxB); err != nil {
		t.Fatalf("ack consumer B: %v", err)
	}
	ackCancelB()

	time.Sleep(200 * time.Millisecond)
	baseline := capture.Len()

	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("post-idle"))

	time.Sleep(2 * time.Second)
	if polls := capture.CountSince(baseline, "queue.dispatcher.poll.begin"); polls > 0 {
		t.Fatalf("unexpected dispatcher polling while idle; observed %d poll begin events", polls)
	}
}
