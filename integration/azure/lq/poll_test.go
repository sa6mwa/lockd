//go:build integration && azure && lq

package azureintegration

import (
	"bytes"
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestAzureQueuePollingBasics(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-poll-basics", 2*time.Minute)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	ts := startAzureQueueServer(t, cfg)
	cli := ts.Client
	ensureAzureQueueWritableOrSkip(t, cfg, cli)

	t.Run("AckRemovesMessage", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-poll-ack", 60*time.Second)
		queue := queuetestutil.QueueName("azure-poll-ack")
		scheduleAzureQueueCleanup(t, cfg, queue)
		queuetestutil.RunQueueAckScenario(t, cli, queue, []byte("azure poll ack"))
	})

	t.Run("NackRedelivery", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-poll-nack", 60*time.Second)
		queue := queuetestutil.QueueName("azure-poll-nack")
		scheduleAzureQueueCleanup(t, cfg, queue)
		queuetestutil.RunQueueNackScenario(t, cli, queue, []byte("azure poll nack"))
	})

	t.Run("ObservabilityReadOnly", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "azure-poll-observability", 60*time.Second)
		queue := queuetestutil.QueueName("azure-poll-observability")
		scheduleAzureQueueCleanup(t, cfg, queue)
		queuetestutil.RunQueueObservabilityReadOnlyScenario(t, cli, queue)
	})
}

func TestAzureQueuePollingIdleEnqueueDoesNotPoll(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-poll-idle", 2*time.Minute)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	queue := queuetestutil.QueueName("azure-poll-idle")
	scheduleAzureQueueCleanup(t, cfg, queue)

	seedServer := startAzureQueueServer(t, cfg)
	seedClient := seedServer.Client
	ensureAzureQueueWritableOrSkip(t, cfg, seedClient)
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-one"))
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-two"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := seedServer.Stop(ctx); err != nil {
		t.Fatalf("stop seed server: %v", err)
	}

	ts, capture := startAzureQueueServerWithCapture(t, cfg)
	cli := ts.Client
	ensureAzureQueueWritableOrSkip(t, cfg, cli)

	consumerA := queuetestutil.QueueOwner("azure-consumer-a")
	msgA := queuetestutil.MustDequeueMessage(t, cli, queue, consumerA, 5, 15*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgA)
	ackCtxA, ackCancelA := context.WithTimeout(context.Background(), 5*time.Second)
	if err := msgA.Ack(ackCtxA); err != nil {
		t.Fatalf("ack consumer A: %v", err)
	}
	ackCancelA()

	consumerB := queuetestutil.QueueOwner("azure-consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, consumerB, 5, 15*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ackCtxB, ackCancelB := context.WithTimeout(context.Background(), 5*time.Second)
	if err := msgB.Ack(ackCtxB); err != nil {
		t.Fatalf("ack consumer B: %v", err)
	}
	ackCancelB()

	time.Sleep(200 * time.Millisecond)
	baseline := capture.Len()

	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("post-idle"))

	time.Sleep(600 * time.Millisecond)
	if polls := capture.CountSince(baseline, "queue.dispatcher.poll.begin"); polls > 0 {
		t.Fatalf("unexpected dispatcher polling while idle; observed %d poll begin events", polls)
	}
}

func ensureAzureQueueWritableOrSkip(t *testing.T, cfg lockd.Config, cli *lockdclient.Client) {
	queue := queuetestutil.QueueName("azure-permission-probe")
	scheduleAzureQueueCleanup(t, cfg, queue)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.Enqueue(ctx, queue, bytes.NewReader([]byte("probe")), lockdclient.EnqueueOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("azure enqueue probe failed (expected write access to queue namespace): %v", err)
	}
	owner := queuetestutil.QueueOwner("azure-permission-owner")
	msg := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 5, 10*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msg)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		t.Fatalf("ack probe message: %v", err)
	}
	ackCancel()
}
