//go:build integration && mem && lq

package memlq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"runtime/pprof"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/hatest"
	shutdowntest "pkt.systems/lockd/integration/internal/shutdowntest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/qrf"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

type memQueueMode struct {
	name       string
	queueWatch bool
}

var memQueueModes = func() []memQueueMode {
	modes := []memQueueMode{{name: "notify", queueWatch: true}}
	if os.Getenv("LOCKD_LQ_INCLUDE_POLLING") == "1" || os.Getenv("LOCKD_LQ_EXTENDED") == "1" {
		modes = append(modes, memQueueMode{name: "polling", queueWatch: false})
	}
	return modes
}()

func RunMemLQScenarios(t *testing.T, mode memQueueMode) {
	t.Helper()
	extended := os.Getenv("LOCKD_LQ_EXTENDED") == "1"

	t.Run("QueueBasics", func(t *testing.T) { runMemQueueBasics(t, mode) })
	t.Run("StatefulCorrelationPersistence", func(t *testing.T) { runMemQueueStatefulCorrelationPersistence(t, mode) })
	t.Run("SubscribeBasics", func(t *testing.T) { runMemQueueSubscribeBasics(t, mode) })
	t.Run("SubscribeStateful", func(t *testing.T) { runMemQueueSubscribeStateful(t, mode) })
	t.Run("ShutdownDrainingSubscribeWithState", func(t *testing.T) { runMemQueueShutdownDrainingSubscribeWithState(t, mode) })

	if extended {
		t.Run("PrefetchBatch", func(t *testing.T) { runMemQueuePrefetch(t, mode) })
		t.Run("IdleEnqueueDoesNotPoll", func(t *testing.T) { runMemQueueIdleEnqueueDoesNotPoll(t, mode) })
		t.Run("MultiConsumerContention", func(t *testing.T) { runMemQueueMultiConsumerContention(t, mode) })
		t.Run("NackVisibilityDelay", func(t *testing.T) { runMemQueueNackVisibilityDelay(t, mode) })
		t.Run("LeaseTimeoutHandoff", func(t *testing.T) { runMemQueueLeaseTimeoutHandoff(t, mode) })
		t.Run("MultiServerRouting", func(t *testing.T) { runMemQueueMultiServerRouting(t, mode) })
		t.Run("MultiServerFailoverClient", func(t *testing.T) { runMemQueueMultiServerFailoverClient(t, mode) })
		if mode.queueWatch {
			t.Run("HighFanInFanOutSingleServer", func(t *testing.T) { runMemQueueHighFanInFanOutSingleServer(t, mode) })
			t.Run("SubscribeHighFanInFanOutSingleServer", func(t *testing.T) { runMemQueueSubscribeHighFanInFanOutSingleServer(t, mode) })
		}
	}
}

func runMemQueueBasics(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-queue-suite", 30*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	cli := ts.Client

	t.Run("AckRemovesMessage", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-ack", 5*time.Second)
		queuetestutil.RunQueueAckScenario(t, cli, queuetestutil.QueueName("mem-ack"), []byte("mem ack payload"))
	})

	t.Run("NackRedelivery", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-nack", 5*time.Second)
		queuetestutil.RunQueueNackScenario(t, cli, queuetestutil.QueueName("mem-nack"), []byte("mem nack payload"))
	})

	t.Run("ObservabilityReadOnly", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-observability", 10*time.Second)
		queuetestutil.RunQueueObservabilityReadOnlyScenario(t, cli, queuetestutil.QueueName("mem-observability"))
	})

	t.Run("TxnDecisionCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-commit", 8*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, true)
	})

	t.Run("TxnDecisionRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-rollback", 8*time.Second)
		queuetestutil.RunQueueTxnDecisionScenario(t, ts, false)
	})

	t.Run("TxnStatefulDecisionCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-stateful-commit", 8*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, true)
	})

	t.Run("TxnStatefulDecisionRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-stateful-rollback", 8*time.Second)
		queuetestutil.RunQueueTxnStatefulDecisionScenario(t, ts, false)
	})

	t.Run("TxnMixedKeyCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-mixed-commit", 15*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, true)
	})
	t.Run("TxnMixedKeyRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-mixed-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnMixedKeyScenario(t, ts, false)
	})

	t.Run("TxnReplayCommit", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-replay-commit", 15*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startMemQueueServerWithOptions, true)
	})
	t.Run("TxnReplayRollback", func(t *testing.T) {
		queuetestutil.InstallWatchdog(t, "mem-txn-replay-rollback", 15*time.Second)
		queuetestutil.RunQueueTxnReplayScenario(t, cfg, startMemQueueServerWithOptions, false)
	})
}

func runMemQueuePrefetch(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-prefetch", 10*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("mem-prefetch")
	payloads := [][]byte{
		[]byte("prefetch-1"),
		[]byte("prefetch-2"),
		[]byte("prefetch-3"),
	}
	for _, payload := range payloads {
		queuetestutil.MustEnqueueBytes(t, cli, queue, payload)
	}

	owner := queuetestutil.QueueOwner("prefetch-consumer")
	msgs := queuetestutil.MustDequeueMessages(t, cli, queue, owner, lockdclient.BlockNoWait, 5*time.Second, 3)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages in batch, got %d", len(msgs))
	}
	seen := make(map[string]bool)
	for _, msg := range msgs {
		body := queuetestutil.ReadMessagePayload(t, msg)
		seen[string(body)] = true
		ackCtx, ackCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := msg.Ack(ackCtx); err != nil {
			ackCancel()
			t.Fatalf("ack %s: %v", msg.MessageID(), err)
		}
		ackCancel()
	}
	if len(seen) != len(msgs) {
		t.Fatalf("expected unique payloads, saw %v", seen)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	remaining, err := cli.DequeueBatch(ctx, queue, lockdclient.DequeueOptions{
		Owner:        owner,
		BlockSeconds: lockdclient.BlockNoWait,
		PageSize:     2,
	})
	if err != nil {
		var apiErr *lockdclient.APIError
		if !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "waiting" {
			t.Fatalf("unexpected dequeue error: %v", err)
		}
	} else if len(remaining) > 0 {
		t.Fatalf("expected queue drained, got %d messages", len(remaining))
	}
}

func runMemQueueStatefulCorrelationPersistence(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-stateful-correlation", 10*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("mem-stateful-corr")
	payload := []byte(`{"step":"initial"}`)

	initial := queuetestutil.MustEnqueueBytes(t, cli, queue, payload)
	if initial.CorrelationID == "" {
		t.Fatalf("enqueue response missing correlation id")
	}

	owner := queuetestutil.QueueOwner("stateful-worker")
	opts := lockdclient.DequeueOptions{
		Owner:        owner,
		BlockSeconds: 1,
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	msg1, err := cli.DequeueWithState(ctx1, queue, opts)
	cancel1()
	if err != nil {
		t.Fatalf("first stateful dequeue: %v", err)
	}
	if msg1 == nil || msg1.StateHandle() == nil {
		t.Fatalf("expected state handle on first dequeue")
	}

	state1 := msg1.StateHandle()
	if got := msg1.CorrelationID(); got != initial.CorrelationID {
		t.Fatalf("message correlation mismatch: enqueue=%q dequeue=%q", initial.CorrelationID, got)
	}
	if got := state1.CorrelationID(); got != initial.CorrelationID {
		t.Fatalf("state handle correlation mismatch: enqueue=%q state=%q", initial.CorrelationID, got)
	}

	// Consume payload to mirror worker behaviour.
	_ = queuetestutil.ReadMessagePayload(t, msg1)

	nackCtx, nackCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msg1.Nack(nackCtx, 0, "simulate failure"); err != nil {
		nackCancel()
		t.Fatalf("nack message: %v", err)
	}
	nackCancel()

	// Second dequeue should present the same message and correlation identifier.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	msg2, err := cli.DequeueWithState(ctx2, queue, opts)
	cancel2()
	if err != nil {
		t.Fatalf("second stateful dequeue: %v", err)
	}
	if msg2 == nil || msg2.StateHandle() == nil {
		t.Fatalf("expected state handle on second dequeue")
	}
	if msg2.MessageID() != msg1.MessageID() {
		t.Fatalf("expected same message id after nack; first=%q second=%q", msg1.MessageID(), msg2.MessageID())
	}
	if got := msg2.CorrelationID(); got != initial.CorrelationID {
		t.Fatalf("message correlation mismatch on requeue: want %q got %q", initial.CorrelationID, got)
	}
	state2 := msg2.StateHandle()
	if got := state2.CorrelationID(); got != initial.CorrelationID {
		t.Fatalf("state handle correlation mismatch on requeue: want %q got %q", initial.CorrelationID, got)
	}

	ackCtx, ackCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msg2.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack message: %v", err)
	}
	ackCancel()
}

func runMemQueueSubscribeBasics(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-subscribe-basics", 8*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("mem-subscribe-basics")
	payloads := []string{"alpha", "beta", "gamma", "delta"}
	expected := make(map[string]struct{}, len(payloads))
	for _, body := range payloads {
		res := queuetestutil.MustEnqueueBytes(t, cli, queue, []byte(body))
		expected[res.MessageID] = struct{}{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	owner := queuetestutil.QueueOwner("subscriber")
	var mu sync.Mutex
	seen := make(map[string]struct{}, len(payloads))

	err := cli.Subscribe(ctx, queue, lockdclient.SubscribeOptions{
		Owner:        owner,
		Prefetch:     4,
		BlockSeconds: 5,
	}, func(msgCtx context.Context, msg *lockdclient.QueueMessage) error {
		defer msg.Close()
		if msg == nil {
			return nil
		}
		id := msg.MessageID()
		if id == "" {
			id = fmt.Sprintf("msg-%d", msg.Attempts())
		}
		if err := msg.ClosePayload(); err != nil {
			return err
		}
		mu.Lock()
		seen[id] = struct{}{}
		complete := len(seen) == len(expected)
		mu.Unlock()
		ackCtx, ackCancel := context.WithTimeout(context.Background(), time.Second)
		err := msg.Ack(ackCtx)
		ackCancel()
		if err != nil {
			return err
		}
		if complete {
			cancel()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe basics: %v", err)
	}
	mu.Lock()
	if len(seen) != len(expected) {
		t.Fatalf("expected %d messages, got %d", len(expected), len(seen))
	}
	for id := range expected {
		if _, ok := seen[id]; !ok {
			t.Fatalf("missing message %s", id)
		}
	}
	mu.Unlock()
	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runMemQueueSubscribeStateful(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-subscribe-stateful", 8*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("mem-subscribe-state")
	payload := []byte(`{"stage":"initial"}`)
	enq := queuetestutil.MustEnqueueBytes(t, cli, queue, payload)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	var stateSeen atomic.Bool

	err := cli.SubscribeWithState(ctx, queue, lockdclient.SubscribeOptions{
		Owner:        queuetestutil.QueueOwner("subscriber-state"),
		Prefetch:     1,
		BlockSeconds: 5,
	}, func(msgCtx context.Context, msg *lockdclient.QueueMessage, state *lockdclient.QueueStateHandle) error {
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
		t.Fatalf("subscribe with state: %v", err)
	}
	if !stateSeen.Load() {
		t.Fatalf("stateful subscription did not observe message")
	}
	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runMemQueueSubscribeHighFanInFanOutSingleServer(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-subscribe-highfan", 10*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("mem-subscribe-highfan")
	const totalMessages = 8
	payload := bytes.Repeat([]byte("x"), 512)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	var produceWg sync.WaitGroup
	produceWg.Add(1)
	go func() {
		defer produceWg.Done()
		for i := 0; i < totalMessages; i++ {
			if ctx.Err() != nil {
				return
			}
			callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
			_, err := cli.EnqueueBytes(callCtx, queue, payload, lockdclient.EnqueueOptions{})
			callCancel()
			if err != nil {
				t.Errorf("enqueue %d: %v", i, err)
				cancel()
				return
			}
		}
	}()

	var mu sync.Mutex
	acked := 0
	err := cli.Subscribe(ctx, queue, lockdclient.SubscribeOptions{
		Owner:        queuetestutil.QueueOwner("subscriber-load"),
		Prefetch:     6,
		BlockSeconds: 5,
	}, func(msgCtx context.Context, msg *lockdclient.QueueMessage) error {
		defer msg.Close()
		if msg == nil {
			return nil
		}
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
		acked++
		done := acked >= totalMessages
		mu.Unlock()
		if done {
			cancel()
		}
		return nil
	})
	produceWg.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe high fan-in/out: %v", err)
	}
	mu.Lock()
	count := acked
	mu.Unlock()
	if count != totalMessages {
		t.Fatalf("expected %d messages, got %d", totalMessages, count)
	}
	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func buildMemQueueConfig(t testing.TB, queueWatch bool) lockd.Config {
	t.Helper()
	cfg := lockd.Config{
		Store:                      "mem://",
		ListenProto:                "tcp",
		Listen:                     "127.0.0.1:0",
		DefaultTTL:                 30 * time.Second,
		MaxTTL:                     2 * time.Minute,
		AcquireBlock:               10 * time.Second,
		SweeperInterval:            2 * time.Second,
		QueuePollInterval:          5 * time.Millisecond,
		QueuePollJitter:            0,
		QueueResilientPollInterval: 200 * time.Millisecond,
		HAMode:                     "failover",
	}
	cfg.DisableMemQueueWatch = !queueWatch
	cfg.QRFDisabled = false
	cfg.QRFQueueSoftLimit = 3
	cfg.QRFQueueHardLimit = 6
	cfg.QRFLockSoftLimit = 4
	cfg.QRFLockHardLimit = 8
	cfg.QRFRecoverySamples = 1
	cfg.QRFSoftDelay = 25 * time.Millisecond
	cfg.QRFEngagedDelay = 150 * time.Millisecond
	cfg.QRFRecoveryDelay = 75 * time.Millisecond
	cfg.QRFCPUPercentSoftLimit = 0
	cfg.QRFCPUPercentHardLimit = 0
	cfg.QRFMemorySoftLimitBytes = 0
	cfg.QRFMemoryHardLimitBytes = 0
	cfg.QRFSwapSoftLimitBytes = 0
	cfg.QRFSwapHardLimitBytes = 0
	cfg.QRFMemorySoftLimitPercent = 0
	cfg.QRFMemoryHardLimitPercent = 0
	cfg.QRFSwapSoftLimitPercent = 0
	cfg.QRFSwapHardLimitPercent = 0
	cfg.QRFLoadSoftLimitMultiplier = 1.5
	cfg.QRFLoadHardLimitMultiplier = 3
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	return cfg
}

func runMemQueueIdleEnqueueDoesNotPoll(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-idle", 5*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts, capture := startMemQueueServerWithCapture(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("mem-idle")

	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("seed-one"))
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("seed-two"))

	consumerA := queuetestutil.QueueOwner("consumer-a")
	msgA := queuetestutil.MustDequeueMessage(t, cli, queue, consumerA, 1, 2*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgA)
	ackCtxA, ackCancelA := context.WithTimeout(context.Background(), 2*time.Second)
	if err := msgA.Ack(ackCtxA); err != nil {
		t.Fatalf("ack consumer A: %v", err)
	}
	ackCancelA()

	consumerB := queuetestutil.QueueOwner("consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, consumerB, 1, 2*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ackCtxB, ackCancelB := context.WithTimeout(context.Background(), 2*time.Second)
	if err := msgB.Ack(ackCtxB); err != nil {
		t.Fatalf("ack consumer B: %v", err)
	}
	ackCancelB()

	initialPolls := capture.CountSince(0, "queue.dispatcher.poll.begin")

	waiterResult := make(chan error, 1)
	go func() {
		opts := lockdclient.DequeueOptions{
			Owner:        queuetestutil.QueueOwner("waiter"),
			BlockSeconds: 1,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		msg, err := cli.Dequeue(ctx, queue, opts)
		if err != nil {
			waiterResult <- fmt.Errorf("waiter dequeue: %w", err)
			return
		}
		_ = queuetestutil.ReadMessagePayload(t, msg)
		ackCtx, ackCancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = msg.Ack(ackCtx)
		ackCancel()
		if err != nil {
			waiterResult <- fmt.Errorf("waiter ack: %w", err)
			return
		}
		waiterResult <- nil
	}()

	time.Sleep(30 * time.Millisecond)

	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("post-idle"))

	select {
	case err := <-waiterResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not receive message in time")
	}

	deltaPolls := capture.CountSince(0, "queue.dispatcher.poll.begin") - initialPolls
	if deltaPolls < 0 {
		deltaPolls = 0
	}
	if mode.queueWatch {
		if deltaPolls > 3 {
			t.Fatalf("excessive dispatcher polling in notify mode; observed %d new poll begin events", deltaPolls)
		}
	} else if deltaPolls == 0 {
		t.Fatalf("expected dispatcher polling in polling mode; observed none within 700ms window")
	}
}

func runMemQueueMultiConsumerContention(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-contention", 5*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("mem-contention")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("only-once"))

	const workers = 5
	type result struct {
		idx int
		msg *lockdclient.QueueMessage
		err error
	}
	results := make(chan result, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := queuetestutil.QueueOwner(fmt.Sprintf("worker-%d", i))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
				Owner:        owner,
				BlockSeconds: 1,
			})
			results <- result{idx: i, msg: msg, err: err}
		}(i)
	}
	wg.Wait()
	close(results)

	var winner *lockdclient.QueueMessage
	var winnerIdx int
	failures := 0

	for res := range results {
		if res.msg != nil && res.msg.MessageID() != "" {
			if winner != nil {
				t.Fatalf("multiple consumers received the message: %d and %d", winnerIdx, res.idx)
			}
			winner = res.msg
			winnerIdx = res.idx
		} else {
			failures++
			var apiErr *lockdclient.APIError
			if errors.As(res.err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
				continue
			}
			if errors.Is(res.err, context.DeadlineExceeded) {
				continue
			}
			if res.err != nil && strings.Contains(res.err.Error(), "EOF") {
				continue
			}
			if res.err != nil {
				t.Fatalf("worker %d expected waiting or timeout, got %v", res.idx, res.err)
			}
		}
	}

	if winner == nil {
		t.Fatalf("no consumer obtained the message")
	}

	_ = queuetestutil.ReadMessagePayload(t, winner)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := winner.Ack(ctx); err != nil {
		t.Fatalf("ack winner %d: %v", winnerIdx, err)
	}
	cancel()

	if failures != workers-1 {
		t.Fatalf("expected %d waiting consumers, got %d", workers-1, failures)
	}

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runMemQueueNackVisibilityDelay(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-nack-delay", 5*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("mem-nack-delay")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("mem-nack-delay"))

	owner := queuetestutil.QueueOwner("consumer")
	msg := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 1, 2*time.Second)

	ctxNack, cancelNack := context.WithTimeout(context.Background(), 2*time.Second)
	if err := msg.Nack(ctxNack, 250*time.Millisecond, nil); err != nil {
		t.Fatalf("nack: %v", err)
	}
	cancelNack()

	start := time.Now()
	next := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 1, 2*time.Second)
	delay := time.Since(start)
	if delay < 200*time.Millisecond {
		t.Fatalf("expected delay >= 200ms, got %s", delay)
	}
	if attempts := next.Attempts(); attempts != msg.Attempts()+1 {
		t.Fatalf("expected attempts %d, got %d", msg.Attempts()+1, attempts)
	}
	_ = queuetestutil.ReadMessagePayload(t, next)
	ctxAck, cancelAck := context.WithTimeout(context.Background(), 2*time.Second)
	if err := next.Ack(ctxAck); err != nil {
		t.Fatalf("ack after nack: %v", err)
	}
	cancelAck()

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runMemQueueLeaseTimeoutHandoff(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-lease-handoff", 5*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("mem-lease-handoff")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("lease-me"))

	ownerA := queuetestutil.QueueOwner("consumer-a")
	ctxA, cancelA := context.WithTimeout(context.Background(), 2*time.Second)
	msgA, err := cli.Dequeue(ctxA, queue, lockdclient.DequeueOptions{
		Owner:        ownerA,
		BlockSeconds: 1,
		Visibility:   150 * time.Millisecond,
	})
	cancelA()
	if err != nil {
		t.Fatalf("consumer A dequeue: %v", err)
	}
	firstAttempts := msgA.Attempts()
	_ = queuetestutil.ReadMessagePayload(t, msgA)
	if err := msgA.ClosePayload(); err != nil {
		t.Fatalf("close payload consumer A: %v", err)
	}

	time.Sleep(250 * time.Millisecond)

	ownerB := queuetestutil.QueueOwner("consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, ownerB, 1, 2*time.Second)
	if attempts := msgB.Attempts(); attempts <= firstAttempts {
		t.Fatalf("expected attempts to increase after lease timeout, was %d now %d", firstAttempts, attempts)
	}
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ctxAck, cancelAck := context.WithTimeout(context.Background(), 2*time.Second)
	if err := msgB.Ack(ctxAck); err != nil {
		t.Fatalf("ack consumer B: %v", err)
	}
	cancelAck()

	ctxCleanup, cancelCleanup := context.WithTimeout(context.Background(), time.Second)
	if err := msgA.Ack(ctxCleanup); err != nil {
		var apiErr *lockdclient.APIError
		if !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "lease_required" {
			t.Fatalf("cleanup ack should fail with lease_required, got %v", err)
		}
	}
	cancelCleanup()

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runMemQueueMultiServerRouting(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-multiserver-routing", 5*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	cfg.HAMode = "concurrent"
	backend := memorybackend.New()

	serverA := startMemQueueServerWithBackend(t, cfg, backend, cryptotest.SharedMTLSOptions(t)...)
	serverB := startMemQueueServerWithBackend(t, cfg, backend, cryptotest.SharedMTLSOptions(t)...)

	queue := queuetestutil.QueueName("mem-routing")
	payload := []byte("shared-backend")

	queuetestutil.MustEnqueueBytes(t, serverA.Client, queue, payload)

	owner := queuetestutil.QueueOwner("consumer-b")
	msg := queuetestutil.MustDequeueMessage(t, serverB.Client, queue, owner, 1, 2*time.Second)
	body := queuetestutil.ReadMessagePayload(t, msg)
	if string(body) != string(payload) {
		t.Fatalf("unexpected payload: got %q want %q", string(body), string(payload))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("ack via serverB: %v", err)
	}
	cancel()

	queuetestutil.EnsureQueueEmpty(t, serverA.Client, queue)
}

func runMemQueueMultiServerFailoverClient(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-multiserver-failover", 10*time.Second)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	backend := memorybackend.New()

	serverA := startMemQueueServerWithBackend(t, cfg, backend)
	serverB := startMemQueueServerWithBackend(t, cfg, backend)

	queue := queuetestutil.QueueName("mem-failover")
	activeCtx, cancelActive := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelActive()
	activeServer, activeClient, err := hatest.FindActiveServer(activeCtx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	t.Cleanup(func() { _ = activeClient.Close() })
	standbyServer := serverB
	if activeServer == serverB {
		standbyServer = serverA
	}
	standbyClient, err := standbyServer.NewClient()
	if err != nil {
		t.Fatalf("standby client: %v", err)
	}
	t.Cleanup(func() { _ = standbyClient.Close() })

	queuetestutil.MustEnqueueBytes(t, activeClient, queue, []byte("failover-payload"))

	clientCapture := queuetestutil.NewLogCapture(t)
	failoverOpts := []lockdclient.Option{
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(clientCapture.Logger()),
	}
	if cryptotest.TestMTLSEnabled() {
		creds := activeServer.TestMTLSCredentials()
		if !creds.Valid() {
			t.Fatalf("mem queue failover: missing MTLS credentials")
		}
		httpClient := cryptotest.RequireMTLSHTTPClient(t, creds)
		failoverOpts = append(failoverOpts, lockdclient.WithHTTPClient(httpClient))
	}
	failoverClient, err := lockdclient.NewWithEndpoints([]string{activeServer.URL(), standbyServer.URL()}, failoverOpts...)
	if err != nil {
		t.Fatalf("new failover client: %v", err)
	}
	t.Cleanup(func() { _ = failoverClient.Close() })

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := activeServer.Stop(stopCtx); err != nil {
		t.Fatalf("stop serverA: %v", err)
	}
	stopCancel()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 12*time.Second)
	if err := hatest.WaitForActive(waitCtx, standbyServer); err != nil {
		waitCancel()
		t.Fatalf("standby activation: %v", err)
	}
	waitCancel()

	owner := queuetestutil.QueueOwner("failover-consumer")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	msg, err := failoverClient.Dequeue(ctx, queue, lockdclient.DequeueOptions{
		Owner:        owner,
		BlockSeconds: 1,
	})
	cancel()
	if err != nil {
		t.Fatalf("failover dequeue: %v", err)
	}

	body := queuetestutil.ReadMessagePayload(t, msg)
	if string(body) != "failover-payload" {
		t.Fatalf("unexpected payload after failover: %q", string(body))
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		t.Fatalf("ack after failover: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, standbyClient, queue)
}

func runMemQueueHighFanInFanOutSingleServer(t *testing.T, mode memQueueMode) {
	queuetestutil.InstallWatchdog(t, "mem-high-fanin-fanout-single", 60*time.Second)

	const (
		producers           = 3
		consumers           = 3
		messagesPerProducer = 2
	)
	totalMessages := producers * messagesPerProducer
	payload := bytes.Repeat([]byte("x"), 1024)

	cfg := buildMemQueueConfig(t, mode.queueWatch)
	queue := queuetestutil.QueueName("mem-highfan-single")
	logToTesting := testing.Verbose()
	capture := queuetestutil.NewLogCaptureWithOptions(t, queuetestutil.LogCaptureOptions{
		MaxEntries:   2000,
		Prefixes:     []string{"lockd.qrf", "lockd.lsf"},
		LogLevel:     pslog.InfoLevel,
		LogToTesting: &logToTesting,
	})
	clientLogger := lockd.NewTestingLogger(t, pslog.InfoLevel)
	baseClientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithLogger(clientLogger),
	}
	ts := lockd.StartTestServer(t,
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(capture.Logger()),
		lockd.WithTestClientOptions(baseClientOpts...),
	)
	cli := ts.Client

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	producerQuotas := distributeQuota(totalMessages, producers)
	consumerQuotas := distributeQuota(totalMessages, consumers)

	if len(producerQuotas) != producers || len(consumerQuotas) != consumers {
		t.Fatalf("quota distribution mismatch: producers=%d consumers=%d", len(producerQuotas), len(consumerQuotas))
	}

	var produced int64
	var consumed int64

	var once sync.Once
	var recordedErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		once.Do(func() {
			recordedErr = err
			cancel()
		})
	}

	var wg sync.WaitGroup

	for i := 0; i < producers; i++ {
		quota := producerQuotas[i]
		if quota == 0 {
			continue
		}
		wg.Add(1)
		go func(worker int, quota int) {
			defer wg.Done()
			for sent := 0; sent < quota; {
				if ctx.Err() != nil {
					recordErr(ctx.Err())
					return
				}
				callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := cli.EnqueueBytes(callCtx, queue, payload, lockdclient.EnqueueOptions{})
				callCancel()
				if err != nil {
					if shouldRetryQueueError(err) {
						continue
					}
					recordErr(fmt.Errorf("enqueue (producer %d): %w", worker, err))
					return
				}
				atomic.AddInt64(&produced, 1)
				sent++
			}
		}(i, quota)
	}

	for i := 0; i < consumers; i++ {
		quota := consumerQuotas[i]
		if quota == 0 {
			continue
		}
		wg.Add(1)
		go func(worker int, quota int) {
			defer wg.Done()
			opts := lockdclient.DequeueOptions{
				Owner:        queuetestutil.QueueOwner(fmt.Sprintf("consumer-%d", worker)),
				BlockSeconds: 1,
			}
			for received := 0; received < quota; {
				if ctx.Err() != nil {
					recordErr(ctx.Err())
					return
				}
				callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
				msg, err := cli.Dequeue(callCtx, queue, opts)
				callCancel()
				if err != nil {
					if shouldRetryQueueError(err) {
						continue
					}
					recordErr(fmt.Errorf("dequeue (consumer %d): %w", worker, err))
					return
				}
				ackCtx, ackCancel := context.WithTimeout(ctx, 5*time.Second)
				if err := msg.Ack(ackCtx); err != nil {
					ackCancel()
					if shouldRetryQueueError(err) {
						continue
					}
					recordErr(fmt.Errorf("ack (consumer %d): %w", worker, err))
					return
				}
				ackCancel()
				atomic.AddInt64(&consumed, 1)
				received++
			}
		}(i, quota)
	}

	wg.Wait()

	if recordedErr != nil {
		t.Fatalf("high fan-in/out single server failed: %v", recordedErr)
	}

	if produced != int64(totalMessages) {
		t.Fatalf("expected produced %d messages, got %d", totalMessages, produced)
	}
	if consumed != int64(totalMessages) {
		t.Fatalf("expected consumed %d messages, got %d", totalMessages, consumed)
	}

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
	time.Sleep(2 * time.Second)
	stateDeadline := time.Now().Add(15 * time.Second)
	for {
		if ts.Server.QRFState() == qrf.StateDisengaged {
			break
		}
		if time.Now().After(stateDeadline) {
			ensureQueueRecovered(t, ts, cli, queue)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	deadline := time.Now().Add(15 * time.Second)
	var engagedEvents, softEvents, disengagedEvents int
	armed := false
	for {
		time.Sleep(100 * time.Millisecond)
		eng := capture.CountSince(0, "lockd.qrf.engaged")
		soft := capture.CountSince(0, "lockd.qrf.soft_arm")
		dis := capture.CountSince(0, "lockd.qrf.disengaged")
		if !armed && (eng > 0 || soft > 0) {
			armed = true
			deadline = time.Now().Add(5 * time.Second)
		}
		if dis > 0 || time.Now().After(deadline) {
			engagedEvents = eng
			softEvents = soft
			disengagedEvents = dis
			break
		}
	}
	if engagedEvents == 0 && softEvents == 0 {
		t.Fatalf("expected QRF to engage or soft-arm during high fan-in/fan-out load")
	}
	if testing.Verbose() {
		t.Logf("qrf events: soft=%d engaged=%d disengaged=%d", softEvents, engagedEvents, disengagedEvents)
	}
	if disengagedEvents == 0 {
		ensureQueueRecovered(t, ts, cli, queue)
		return
	}
}

func runMemQueueShutdownDrainingSubscribeWithState(t *testing.T, mode memQueueMode) {
	cfg := buildMemQueueConfig(t, mode.queueWatch)
	ts := startMemQueueServer(t, cfg)
	t.Cleanup(func() { _ = ts.Stop(context.Background()) })
	cli := ts.Client
	if cli == nil {
		t.Fatalf("nil client")
	}
	leaseCtx, leaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer leaseCancel()
	lease, err := cli.Acquire(leaseCtx, api.AcquireRequest{
		Key:        "mem-lq-drain-" + uuidv7.NewString(),
		Owner:      "mem-lq-holder",
		TTLSeconds: 30,
	})
	if err != nil {
		t.Fatalf("acquire drain holder: %v", err)
	}
	stopCh := make(chan error, 1)
	go func() {
		stopCh <- ts.Stop(context.Background(), lockd.WithDrainLeases(2*time.Second), lockd.WithShutdownTimeout(3*time.Second))
	}()
	payload, _ := json.Marshal(api.DequeueRequest{Queue: "drain-queue", Owner: "worker-1", PageSize: 1})
	url := ts.URL() + "/v1/queue/subscribeWithState"
	httpClient, err := ts.NewHTTPClient()
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	result := shutdowntest.WaitForShutdownDrainingAcquireWithClient(t, httpClient, url, payload)
	if result.Response.ErrorCode != "shutdown_draining" {
		t.Fatalf("expected shutdown_draining error, got %+v", result.Response)
	}
	if result.Header.Get("Shutdown-Imminent") == "" {
		t.Fatalf("expected Shutdown-Imminent header, got %v", result.Header)
	}
	_ = lease.Release(context.Background())
	select {
	case err := <-stopCh:
		if err != nil {
			t.Fatalf("server stop failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("server stop timed out")
	}
}

func startMemQueueServerWithCapture(t testing.TB, cfg lockd.Config) (*lockd.TestServer, *queuetestutil.LogCapture) {
	t.Helper()
	capture := queuetestutil.NewLogCapture(t)
	ts := queuetestutil.StartQueueTestServerWithLogger(t, cfg, capture.Logger())
	return ts, capture
}

func startMemQueueServer(t testing.TB, cfg lockd.Config) *lockd.TestServer {
	t.Helper()
	cryptotest.ConfigureTCAuth(t, &cfg)
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(pslog.NoopLogger()),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(pslog.NoopLogger()),
			lockdclient.WithHTTPTimeout(30*time.Second),
			lockdclient.WithKeepAliveTimeout(30*time.Second),
			lockdclient.WithCloseTimeout(30*time.Second),
		),
	}
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	return lockd.StartTestServer(t, options...)
}

func startMemQueueServerWithOptions(t testing.TB, cfg lockd.Config, extra ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
	cryptotest.ConfigureTCAuth(t, &cfg)
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(pslog.NoopLogger()),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(pslog.NoopLogger()),
			lockdclient.WithHTTPTimeout(30*time.Second),
			lockdclient.WithKeepAliveTimeout(30*time.Second),
			lockdclient.WithCloseTimeout(30*time.Second),
		),
	}
	options = append(options, extra...)
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	return lockd.StartTestServer(t, options...)
}

func startMemQueueServerWithBackend(t testing.TB, cfg lockd.Config, backend *memorybackend.Store, extra ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
	cryptotest.ConfigureTCAuth(t, &cfg)
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestBackend(backend),
		lockd.WithTestLogger(pslog.NoopLogger()),
		lockd.WithTestClientOptions(
			lockdclient.WithLogger(pslog.NoopLogger()),
			lockdclient.WithHTTPTimeout(30*time.Second),
			lockdclient.WithKeepAliveTimeout(30*time.Second),
			lockdclient.WithCloseTimeout(30*time.Second),
		),
	}
	options = append(options, extra...)
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	return lockd.StartTestServer(t, options...)
}

func distributeQuota(total, workers int) []int {
	if workers <= 0 {
		return nil
	}
	base := total / workers
	rem := total % workers
	out := make([]int, workers)
	for i := 0; i < workers; i++ {
		out[i] = base
		if i < rem {
			out[i]++
		}
	}
	return out
}

func shouldRetryQueueError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		time.Sleep(25 * time.Millisecond)
		return true
	}
	var apiErr *lockdclient.APIError
	if errors.As(err, &apiErr) {
		if apiErr.Response.ErrorCode == "waiting" {
			time.Sleep(25 * time.Millisecond)
			return true
		}
		if apiErr.Response.RetryAfterSeconds > 0 {
			time.Sleep(time.Duration(apiErr.Response.RetryAfterSeconds) * time.Second)
			return true
		}
	}
	if strings.Contains(err.Error(), "EOF") {
		return true
	}
	return false
}

func ensureQueueRecovered(t testing.TB, ts *lockd.TestServer, cli *lockdclient.Client, queue string) {
	t.Helper()
	dumpQRFDiagnostics(t, ts, "recovery-start")
	payload := []byte("recovery-probe")
	deadline := time.Now().Add(15 * time.Second)
	for {
		if ts != nil && ts.Server != nil {
			status := ts.Server.QRFStatus()
			if status.State != qrf.StateEngaged &&
				status.Snapshot.QueueProducerInflight == 0 &&
				status.Snapshot.QueueConsumerInflight == 0 &&
				status.Snapshot.QueueAckInflight == 0 &&
				status.Snapshot.Load1Multiplier <= 1.05 {
				t.Logf("[recovery-pass] qrf state=%s reason=%s multiplier=%.2f", status.State.String(), status.Reason, status.Snapshot.Load1Multiplier)
				return
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err := cli.EnqueueBytes(ctx, queue, payload, lockdclient.EnqueueOptions{})
		cancel()
		if err == nil {
			owner := queuetestutil.QueueOwner("recovery-check")
			msg := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 1, 3*time.Second)
			body := queuetestutil.ReadMessagePayload(t, msg)
			if string(body) != string(payload) {
				t.Fatalf("unexpected recovery payload: got %q want %q", string(body), string(payload))
			}
			ackCtx, ackCancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := msg.Ack(ackCtx); err != nil {
				ackCancel()
				dumpQRFDiagnostics(t, ts, "recovery-ack-fail")
				t.Fatalf("ack recovery probe: %v", err)
			}
			ackCancel()
			dumpQRFDiagnostics(t, ts, "recovery-success")
			return
		}
		if !shouldRetryQueueError(err) || time.Now().After(deadline) {
			dumpQRFDiagnostics(t, ts, "recovery-fail")
			t.Fatalf("expected QRF to disengage after load (enqueue kept failing): %v", err)
		}
	}
}

func dumpQRFDiagnostics(t testing.TB, ts *lockd.TestServer, label string) {
	t.Helper()
	if ts != nil && ts.Server != nil {
		status := ts.Server.QRFStatus()
		t.Logf("[%s] qrf state=%s reason=%s queue_producers=%d queue_consumers=%d queue_ack=%d load1=%.2f baseline=%.2f multiplier=%.2f cpu=%.2f mem=%.2f",
			label,
			status.State.String(),
			status.Reason,
			status.Snapshot.QueueProducerInflight,
			status.Snapshot.QueueConsumerInflight,
			status.Snapshot.QueueAckInflight,
			status.Snapshot.SystemLoad1,
			status.Snapshot.Load1Baseline,
			status.Snapshot.Load1Multiplier,
			status.Snapshot.SystemCPUPercent,
			status.Snapshot.SystemMemoryUsedPercent,
		)
	}
	if g := pprof.Lookup("goroutine"); g != nil {
		var buf bytes.Buffer
		if err := g.WriteTo(&buf, 2); err == nil {
			t.Logf("[%s] goroutines:\n%s", label, buf.String())
		}
	}
	if heap := pprof.Lookup("heap"); heap != nil {
		var buf bytes.Buffer
		if err := heap.WriteTo(&buf, 1); err == nil {
			t.Logf("[%s] heap profile:\n%s", label, buf.String())
		}
	}
}
