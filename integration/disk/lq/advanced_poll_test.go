//go:build integration && disk && lq

package disklq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/logport"
)

func runDiskQueueMultiConsumerContention(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-contention", 10*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	ts := startDiskQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("disk-contention")
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
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
			continue
		}
		failures++
		if res.err == nil {
			continue
		}
		var apiErr *lockdclient.APIError
		if errors.As(res.err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
			continue
		}
		if errors.Is(res.err, context.DeadlineExceeded) || strings.Contains(res.err.Error(), "EOF") {
			continue
		}
		t.Fatalf("worker %d expected waiting/timeout, got %v", res.idx, res.err)
	}

	if winner == nil {
		t.Fatalf("no consumer obtained the message")
	}

	_ = queuetestutil.ReadMessagePayload(t, winner)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := winner.Ack(ackCtx); err != nil {
		t.Fatalf("ack winner %d: %v", winnerIdx, err)
	}
	ackCancel()

	if failures != workers-1 {
		t.Fatalf("expected %d waiting consumers, got %d", workers-1, failures)
	}

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runDiskQueueNackVisibilityDelay(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-nack-delay", 10*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	ts := startDiskQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("disk-nack-delay")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("disk-nack-delay"))

	owner := queuetestutil.QueueOwner("consumer")
	msg := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 1, 3*time.Second)

	ctxNack, cancelNack := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msg.Nack(ctxNack, 500*time.Millisecond, nil); err != nil {
		cancelNack()
		t.Fatalf("nack: %v", err)
	}
	cancelNack()

	start := time.Now()
	msgRedelivered := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 1, 4*time.Second)
	delay := time.Since(start)
	if delay < 400*time.Millisecond {
		t.Fatalf("expected delay >= 400ms, got %s", delay)
	}
	if msgRedelivered.Attempts() != msg.Attempts()+1 {
		t.Fatalf("expected attempts %d, got %d", msg.Attempts()+1, msgRedelivered.Attempts())
	}
	_ = queuetestutil.ReadMessagePayload(t, msgRedelivered)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msgRedelivered.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack after nack: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runDiskQueueLeaseTimeoutHandoff(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-lease-handoff", 10*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	ts := startDiskQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("disk-lease-handoff")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("lease-me"))

	ownerA := queuetestutil.QueueOwner("consumer-a")
	ctxA, cancelA := context.WithTimeout(context.Background(), 3*time.Second)
	msgA, err := cli.Dequeue(ctxA, queue, lockdclient.DequeueOptions{
		Owner:        ownerA,
		BlockSeconds: 1,
		Visibility:   300 * time.Millisecond,
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

	time.Sleep(500 * time.Millisecond)

	ownerB := queuetestutil.QueueOwner("consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, ownerB, 1, 3*time.Second)
	if msgB.Attempts() <= firstAttempts {
		t.Fatalf("expected attempts to increase after lease timeout, was %d now %d", firstAttempts, msgB.Attempts())
	}
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msgB.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack consumer B: %v", err)
	}
	ackCancel()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := msgA.Ack(cleanupCtx); err != nil {
		cleanupCancel()
		var apiErr *lockdclient.APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("cleanup ack expected lease_required or not_found, got %v", err)
		}
		if apiErr.Response.ErrorCode != "lease_required" && apiErr.Response.ErrorCode != "not_found" {
			t.Fatalf("cleanup ack expected lease_required or not_found, got %s", apiErr.Response.ErrorCode)
		}
	} else {
		cleanupCancel()
		t.Fatalf("expected first consumer ack to fail after timeout")
	}

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runDiskQueueMultiServerRouting(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-multiserver-routing", 10*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	serverA := startDiskQueueServer(t, cfg)
	serverB := startDiskQueueServer(t, cfg)

	queue := queuetestutil.QueueName("disk-routing")
	payload := []byte("shared-disk-backend")

	queuetestutil.MustEnqueueBytes(t, serverA.Client, queue, payload)

	owner := queuetestutil.QueueOwner("consumer-b")
	msg := queuetestutil.MustDequeueMessage(t, serverB.Client, queue, owner, 1, 4*time.Second)
	body := queuetestutil.ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("unexpected payload: got %q want %q", string(body), string(payload))
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack via serverB: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, serverA.Client, queue)
}

func runDiskQueueMultiServerFailoverClient(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-multiserver-failover", 15*time.Second)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	serverA := startDiskQueueServer(t, cfg)
	serverB := startDiskQueueServer(t, cfg)

	queue := queuetestutil.QueueName("disk-failover")
	queuetestutil.MustEnqueueBytes(t, serverA.Client, queue, []byte("failover-payload"))

	endpoints := []string{serverA.URL(), serverB.URL()}
	capture := queuetestutil.NewLogCapture(t)
	failoverClient, err := lockdclient.NewWithEndpoints(endpoints,
		lockdclient.WithMTLS(false),
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(capture.Logger()),
	)
	if err != nil {
		t.Fatalf("new failover client: %v", err)
	}
	t.Cleanup(func() { _ = failoverClient.Close() })

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := serverA.Stop(shutdownCtx); err != nil {
		shutdownCancel()
		t.Fatalf("stop serverA: %v", err)
	}
	shutdownCancel()

	owner := queuetestutil.QueueOwner("failover-consumer")
	dequeueCtx, dequeueCancel := context.WithTimeout(context.Background(), 5*time.Second)
	msg, err := failoverClient.Dequeue(dequeueCtx, queue, lockdclient.DequeueOptions{
		Owner:        owner,
		BlockSeconds: 1,
	})
	dequeueCancel()
	if err != nil {
		t.Fatalf("failover dequeue: %v", err)
	}

	body := queuetestutil.ReadMessagePayload(t, msg)
	if string(body) != "failover-payload" {
		t.Fatalf("unexpected payload after failover: %q", string(body))
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack after failover: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, serverB.Client, queue)
}

func runDiskQueueHighFanInFanOutSingleServer(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "disk-high-fanin-fanout", 60*time.Second)

	const (
		producers           = 12
		consumers           = 12
		messagesPerProducer = 4
	)
	totalMessages := producers * messagesPerProducer
	payload := bytes.Repeat([]byte("x"), 1024)

	root := prepareDiskQueueRoot(t)
	cfg := buildDiskQueueConfig(t, root, diskQueueOptions{
		EnableWatch:       false,
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	logToTesting := false
	capture := queuetestutil.NewLogCaptureWithOptions(t, queuetestutil.LogCaptureOptions{
		MaxEntries:   2000,
		Prefixes:     []string{"lockd.qrf", "lockd.lsf"},
		LogLevel:     logport.InfoLevel,
		LogToTesting: &logToTesting,
	})
	clientLogger := lockd.NewTestingLogger(t, logport.InfoLevel)
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
	queue := queuetestutil.QueueName("disk-highfan-single")
	cli := ts.Client

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	producerQuotas := distributeQuota(totalMessages, producers)
	consumerQuotas := distributeQuota(totalMessages, consumers)

	var produced atomic.Int64
	var consumed atomic.Int64
	var produceWG sync.WaitGroup
	for i := 0; i < producers; i++ {
		quota := producerQuotas[i]
		produceWG.Add(1)
		go func() {
			defer produceWG.Done()
			for j := 0; j < quota; j++ {
				localCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
				_, err := cli.Enqueue(localCtx, queue, bytes.NewReader(payload), lockdclient.EnqueueOptions{ContentType: "application/octet-stream"})
				cancel()
				if err != nil {
					if ctx.Err() != nil {
						shutdownWithError(t, fmt.Errorf("producer context cancelled: %w", ctx.Err()))
					}
					if shouldRetryQueueError(err) {
						j--
						continue
					}
					shutdownWithError(t, err)
				}
				produced.Add(1)
			}
		}()
	}

	var consumeWG sync.WaitGroup
	for i := 0; i < consumers; i++ {
		quota := consumerQuotas[i]
		consumeWG.Add(1)
		go func() {
			defer consumeWG.Done()
			owner := queuetestutil.QueueOwner("consumer")
			for handled := 0; handled < quota; {
				localCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				msg, err := cli.Dequeue(localCtx, queue, lockdclient.DequeueOptions{
					Owner:        owner,
					BlockSeconds: 1,
				})
				cancel()
				if err != nil {
					if ctx.Err() != nil {
						shutdownWithError(t, fmt.Errorf("consumer context cancelled: %w", ctx.Err()))
					}
					if shouldRetryQueueError(err) {
						continue
					}
					shutdownWithError(t, err)
				}
				if msg == nil {
					continue
				}
				_ = queuetestutil.ReadMessagePayload(t, msg)
				ackCtx, ackCancel := context.WithTimeout(ctx, 5*time.Second)
				if err := msg.Ack(ackCtx); err != nil {
					ackCancel()
					if ctx.Err() != nil {
						shutdownWithError(t, fmt.Errorf("consumer context cancelled during ack: %w", ctx.Err()))
					}
					if shouldRetryQueueError(err) {
						continue
					}
					shutdownWithError(t, err)
				}
				ackCancel()
				handled++
				consumed.Add(1)
			}
		}()
	}

	produceWG.Wait()
	consumeWG.Wait()

	if produced.Load() != int64(totalMessages) {
		t.Fatalf("expected produced %d messages, got %d", totalMessages, produced.Load())
	}
	if consumed.Load() != int64(totalMessages) {
		t.Fatalf("expected consumed %d messages, got %d", totalMessages, consumed.Load())
	}

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
	if ts.Server != nil {
		ts.Server.ForceQRFObserve(qrf.Snapshot{QueueProducerInflight: cfg.QRFQueueSoftLimit + 1, CollectedAt: time.Now()})
		ts.Server.ForceQRFObserve(qrf.Snapshot{QueueProducerInflight: cfg.QRFQueueHardLimit, CollectedAt: time.Now()})
		ts.Server.ForceQRFObserve(qrf.Snapshot{CollectedAt: time.Now()})
		ts.Server.ForceQRFObserve(qrf.Snapshot{CollectedAt: time.Now().Add(100 * time.Millisecond)})
	}
	time.Sleep(200 * time.Millisecond)
	softEvents := capture.CountSince(0, "lockd.qrf.soft_arm")
	engagedEvents := capture.CountSince(0, "lockd.qrf.engaged")
	disengagedEvents := capture.CountSince(0, "lockd.qrf.disengaged")
	if softEvents == 0 || engagedEvents == 0 {
		t.Fatalf("expected forced QRF transitions, got soft=%d engaged=%d", softEvents, engagedEvents)
	}
	if disengagedEvents == 0 {
		t.Fatalf("expected QRF to disengage after forced transitions (soft=%d engaged=%d)", softEvents, engagedEvents)
	}
}

func shutdownWithError(t *testing.T, err error) {
	if err != nil {
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) {
			t.Fatalf("queue error: %s", apiErr.Response.Detail)
		}
		t.Fatalf("queue error: %v", err)
	}
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
