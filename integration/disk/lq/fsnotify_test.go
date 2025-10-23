//go:build integration && disk && lq

package disklq

import (
	"context"
	"runtime"
	"testing"
	"time"

	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func runDiskQueueFSNotifyBasics(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("disk queue fsnotify watcher tests require Linux")
	}
	queuetestutil.InstallWatchdog(t, "disk-fsnotify-suite", 15*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       true,
		PollJitter:        -1,
		ResilientInterval: 2 * time.Second,
	})
	ts := startDiskQueueServer(t, cfg)
	cli := ts.Client

	t.Run("AckRemovesMessage", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-fsnotify-ack", 10*time.Second)
		queuetestutil.RunQueueAckScenario(t, cli, queuetestutil.QueueName("disk-fsnotify-ack"), []byte("fsnotify ack payload"))
	})

	t.Run("NackRedelivery", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-fsnotify-nack", 10*time.Second)
		queuetestutil.RunQueueNackScenario(t, cli, queuetestutil.QueueName("disk-fsnotify-nack"), []byte("fsnotify nack payload"))
	})
}

func runDiskQueueFSNotifyIdleEnqueueDoesNotPoll(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("disk queue fsnotify watcher tests require Linux")
	}
	queuetestutil.InstallWatchdog(t, "disk-fsnotify-idle", 20*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       true,
		PollJitter:        -1,
		ResilientInterval: 5 * time.Minute,
	})
	queue := queuetestutil.QueueName("disk-fsnotify-idle")

	seedServer := startDiskQueueServer(t, cfg)
	seedClient := seedServer.Client
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-one"))
	queuetestutil.MustEnqueueBytes(t, seedClient, queue, []byte("seed-two"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := seedServer.Stop(ctx); err != nil {
		t.Fatalf("stop seed server: %v", err)
	}

	ts, capture := startDiskQueueServerWithCapture(t, cfg)
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

	time.Sleep(4 * time.Second)
	if polls := capture.CountSince(baseline, "queue.dispatcher.poll.begin"); polls > 0 {
		t.Fatalf("unexpected dispatcher polling while idle; observed %d poll begin events", polls)
	}
}
