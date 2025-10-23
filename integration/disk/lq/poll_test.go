//go:build integration && disk && lq

package disklq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lockdclient "pkt.systems/lockd/client"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func runDiskQueuePollingBasics(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-poll-suite", 20*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	ts := startDiskQueueServer(t, cfg)
	cli := ts.Client

	t.Run("AckRemovesMessage", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-poll-ack", 10*time.Second)
		queuetestutil.RunQueueAckScenario(t, cli, queuetestutil.QueueName("disk-poll-ack"), []byte("poll ack payload"))
	})

	t.Run("NackRedelivery", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "disk-poll-nack", 10*time.Second)
		queuetestutil.RunQueueNackScenario(t, cli, queuetestutil.QueueName("disk-poll-nack"), []byte("poll nack payload"))
	})
}

func runDiskQueuePollingIdleEnqueueDoesNotPoll(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-poll-idle", 20*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	queue := queuetestutil.QueueName("disk-poll-idle")

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

	time.Sleep(100 * time.Millisecond)
	baseline := capture.Len()

	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("post-idle"))

	time.Sleep(500 * time.Millisecond)
	if polls := capture.CountSince(baseline, "queue.dispatcher.poll.begin"); polls > 0 {
		t.Fatalf("unexpected dispatcher polling while idle; observed %d poll begin events", polls)
	}
}

func runDiskQueueSubscribeBasics(t *testing.T) {
	t.Run("Polling", func(t *testing.T) {
		runDiskQueueSubscribeBasicsWithOpts(t, "poll", diskQueueOptions{
			EnableWatch:       false,
			PollInterval:      50 * time.Millisecond,
			PollJitter:        0,
			ResilientInterval: 250 * time.Millisecond,
		})
	})
	t.Run("FSNotify", func(t *testing.T) {
		runDiskQueueSubscribeBasicsWithOpts(t, "watch", diskQueueOptions{
			EnableWatch:       true,
			PollInterval:      200 * time.Millisecond,
			PollJitter:        0,
			ResilientInterval: time.Second,
		})
	})
}

func runDiskQueueSubscribeBasicsWithOpts(t *testing.T, label string, opts diskQueueOptions) {
	queuetestutil.InstallWatchdog(t, "disk-subscribe-basic-"+label, 15*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, opts)
	ts := startDiskQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("disk-subscribe-basic-" + label)
	payloads := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	expected := make(map[string]struct{}, len(payloads))
	for _, body := range payloads {
		res := queuetestutil.MustEnqueueBytes(t, cli, queue, body)
		expected[res.MessageID] = struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	owner := queuetestutil.QueueOwner("subscriber-basic-" + label)
	var mu sync.Mutex
	seen := make(map[string]struct{}, len(expected))

	err := cli.Subscribe(ctx, queue, lockdclient.SubscribeOptions{
		Owner:        owner,
		Prefetch:     4,
		BlockSeconds: 5,
	}, func(msgCtx context.Context, msg *lockdclient.QueueMessage) error {
		if msg == nil {
			return nil
		}
		defer msg.Close()
		if err := msg.ClosePayload(); err != nil {
			return err
		}
		ackCtx, ackCancel := context.WithTimeout(context.Background(), time.Second)
		err := msg.Ack(ackCtx)
		ackCancel()
		if err != nil {
			return err
		}
		mu.Lock()
		seen[msg.MessageID()] = struct{}{}
		complete := len(seen) == len(expected)
		mu.Unlock()
		if complete {
			cancel()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe basics %s: %v", label, err)
	}
	mu.Lock()
	if len(seen) != len(expected) {
		t.Fatalf("expected %d messages, got %d", len(expected), len(seen))
	}
	mu.Unlock()
	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runDiskQueueSubscribeWithState(t *testing.T) {
	t.Run("Polling", func(t *testing.T) {
		runDiskQueueSubscribeWithStateOpts(t, "poll", diskQueueOptions{
			EnableWatch:       false,
			PollInterval:      50 * time.Millisecond,
			PollJitter:        0,
			ResilientInterval: 250 * time.Millisecond,
		})
	})
	t.Run("FSNotify", func(t *testing.T) {
		runDiskQueueSubscribeWithStateOpts(t, "watch", diskQueueOptions{
			EnableWatch:       true,
			PollInterval:      200 * time.Millisecond,
			PollJitter:        0,
			ResilientInterval: time.Second,
		})
	})
}

func runDiskQueueSubscribeWithStateOpts(t *testing.T, label string, opts diskQueueOptions) {
	queuetestutil.InstallWatchdog(t, "disk-subscribe-state-"+label, 15*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, opts)
	ts := startDiskQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("disk-subscribe-state-" + label)
	payload := []byte("stateful payload")
	enq := queuetestutil.MustEnqueueBytes(t, cli, queue, payload)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var stateSeen atomic.Bool

	err := cli.SubscribeWithState(ctx, queue, lockdclient.SubscribeOptions{
		Owner:        queuetestutil.QueueOwner("subscriber-state-" + label),
		Prefetch:     2,
		BlockSeconds: 5,
	}, func(msgCtx context.Context, msg *lockdclient.QueueMessage, state *lockdclient.QueueStateHandle) error {
		if msg == nil {
			return nil
		}
		defer msg.Close()
		if state == nil {
			return fmt.Errorf("expected state handle")
		}
		if msg.CorrelationID() != enq.CorrelationID {
			return fmt.Errorf("message correlation mismatch: enqueue=%q subscribe=%q", enq.CorrelationID, msg.CorrelationID())
		}
		if state.CorrelationID() != enq.CorrelationID {
			return fmt.Errorf("state correlation mismatch: enqueue=%q subscribe=%q", enq.CorrelationID, state.CorrelationID())
		}
		ackCtx, ackCancel := context.WithTimeout(context.Background(), time.Second)
		err := msg.Ack(ackCtx)
		ackCancel()
		if err != nil {
			return err
		}
		stateSeen.Store(true)
		cancel()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe stateful %s: %v", label, err)
	}
	if !stateSeen.Load() {
		t.Fatalf("stateful subscribe %s did not observe message", label)
	}
	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}
