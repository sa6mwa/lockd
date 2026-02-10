//go:build integration && minio && lq

package miniointegration

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
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/hatest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/pslog"
)

func TestMinioQueueAdvancedScenarios(t *testing.T) {
	t.Run("MultiConsumerContention", runMinioQueueMultiConsumerContention)
	t.Run("NackVisibilityDelay", runMinioQueueNackVisibilityDelay)
	t.Run("LeaseTimeoutHandoff", runMinioQueueLeaseTimeoutHandoff)
	t.Run("MultiServerRouting", runMinioQueueMultiServerRouting)
	t.Run("MultiServerFailoverClient", runMinioQueueMultiServerFailoverClient)
	t.Run("HighFanInFanOutSingleServer", runMinioQueueHighFanInFanOutSingleServer)
	t.Run("QRFThrottling", runMinioQueueQRFThrottling)
	t.Run("PollingIntervalRespected", runMinioQueuePollingIntervalRespected)
}

func runMinioQueueMultiConsumerContention(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-contention", 20*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	cfg.HAMode = "concurrent"

	ts := startMinioQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("minio-contention")
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
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := winner.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack winner %d: %v", winnerIdx, err)
	}
	ackCancel()

	if failures != workers-1 {
		t.Fatalf("expected %d waiting consumers, got %d", workers-1, failures)
	}

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runMinioQueueNackVisibilityDelay(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-nack-delay", 20*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	ts := startMinioQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("minio-nack-delay")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("minio-nack-delay"))

	owner := queuetestutil.QueueOwner("consumer")
	msg := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 1, 10*time.Second)

	ctxNack, cancelNack := context.WithTimeout(context.Background(), 10*time.Second)
	if err := msg.Nack(ctxNack, time.Second, nil); err != nil {
		cancelNack()
		t.Fatalf("nack: %v", err)
	}
	cancelNack()

	start := time.Now()
	msgRedelivered := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 1, 15*time.Second)
	delay := time.Since(start)
	if delay < 800*time.Millisecond {
		t.Fatalf("expected delay >= 800ms, got %s", delay)
	}
	if msgRedelivered.Attempts() <= msg.Attempts() {
		t.Fatalf("expected attempts to increase after nack delay")
	}
	_ = queuetestutil.ReadMessagePayload(t, msgRedelivered)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := msgRedelivered.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack after nack: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runMinioQueueLeaseTimeoutHandoff(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-lease-handoff", 30*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      50 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 500 * time.Millisecond,
	})

	ts := startMinioQueueServer(t, cfg)
	cli := ts.Client

	queue := queuetestutil.QueueName("minio-lease-handoff")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("lease-me"))

	ownerA := queuetestutil.QueueOwner("consumer-a")
	ctxA, cancelA := context.WithTimeout(context.Background(), 15*time.Second)
	msgA, err := cli.Dequeue(ctxA, queue, lockdclient.DequeueOptions{
		Owner:        ownerA,
		BlockSeconds: 1,
		Visibility:   800 * time.Millisecond,
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

	time.Sleep(1100 * time.Millisecond)

	ownerB := queuetestutil.QueueOwner("consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, ownerB, 1, 15*time.Second)
	if msgB.Attempts() <= firstAttempts {
		t.Fatalf("expected attempts to increase after lease timeout, was %d now %d", firstAttempts, msgB.Attempts())
	}
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := msgB.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack consumer B: %v", err)
	}
	ackCancel()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
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

func runMinioQueueMultiServerRouting(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-routing", 30*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	cfg.HAMode = "concurrent"

	serverA := startMinioQueueServer(t, cfg)
	shared := cryptotest.SharedMTLSOptions(t)
	serverB := startMinioQueueServer(t, cfg, shared...)

	queue := queuetestutil.QueueName("minio-routing")
	payload := []byte("shared-minio-backend")

	queuetestutil.MustEnqueueBytes(t, serverA.Client, queue, payload)

	owner := queuetestutil.QueueOwner("consumer-b")
	msg := queuetestutil.MustDequeueMessage(t, serverB.Client, queue, owner, 1, 15*time.Second)
	body := queuetestutil.ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("unexpected payload: got %q want %q", string(body), string(payload))
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack via serverB: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, serverA.Client, queue)
}

func runMinioQueueMultiServerFailoverClient(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-failover", 30*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})
	cfg.HAMode = "failover"

	var sharedCreds lockd.TestMTLSCredentials
	if cryptotest.TestMTLSEnabled() {
		sharedCreds = cryptotest.SharedMTLSCredentials(t)
	}
	serverA := startMinioQueueServer(t, cfg)
	sharedOpts := cryptotest.SharedMTLSOptions(t)
	serverB := startMinioQueueServer(t, cfg, sharedOpts...)

	queue := queuetestutil.QueueName("minio-failover")
	activeCtx, cancelActive := context.WithTimeout(context.Background(), 5*time.Second)
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

	capture := queuetestutil.NewLogCapture(t)
	clientOptions := []lockdclient.Option{
		lockdclient.WithEndpointShuffle(false),
		lockdclient.WithLogger(capture.Logger()),
	}
	if cryptotest.TestMTLSEnabled() {
		httpClient := cryptotest.RequireMTLSHTTPClient(t, sharedCreds)
		clientOptions = append(clientOptions, lockdclient.WithHTTPClient(httpClient))
	}
	failoverClient, err := lockdclient.NewWithEndpoints([]string{activeServer.URL(), standbyServer.URL()}, clientOptions...)
	if err != nil {
		t.Fatalf("new failover client: %v", err)
	}
	t.Cleanup(func() { _ = failoverClient.Close() })

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := activeServer.Stop(shutdownCtx); err != nil {
		shutdownCancel()
		t.Fatalf("stop serverA: %v", err)
	}
	shutdownCancel()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := hatest.WaitForActive(waitCtx, standbyServer); err != nil {
		waitCancel()
		t.Fatalf("standby activation: %v", err)
	}
	waitCancel()

	owner := queuetestutil.QueueOwner("failover-consumer")
	dequeueCtx, dequeueCancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack after failover: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, standbyClient, queue)
}

func runMinioQueueHighFanInFanOutSingleServer(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-high-fanin-fanout", 35*time.Second)

	const (
		producers           = 10
		consumers           = 8
		messagesPerProducer = 2
	)
	totalMessages := producers * messagesPerProducer
	payload := bytes.Repeat([]byte("x"), 384)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      25 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 250 * time.Millisecond,
	})

	ts := startMinioQueueServer(t, cfg)
	queue := queuetestutil.QueueName("minio-highfan")
	cli := ts.Client

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	producerQuotas := distributeQuota(totalMessages, producers)
	consumerQuotas := distributeQuota(totalMessages, consumers)

	var produced atomic.Int64
	var consumed atomic.Int64

	var produceWG sync.WaitGroup
	for _, quota := range producerQuotas {
		q := quota
		produceWG.Add(1)
		go func() {
			defer produceWG.Done()
			for j := 0; j < q; j++ {
				localCtx, localCancel := context.WithTimeout(ctx, 8*time.Second)
				_, err := cli.Enqueue(localCtx, queue, bytes.NewReader(payload), lockdclient.EnqueueOptions{ContentType: "application/octet-stream"})
				localCancel()
				if err != nil {
					if ctx.Err() != nil {
						shutdownWithError(t, fmt.Errorf("producer context cancelled: %w", ctx.Err()))
					}
					if shouldRetryQueueError(err, nil) {
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
	for _, quota := range consumerQuotas {
		q := quota
		consumeWG.Add(1)
		go func() {
			defer consumeWG.Done()
			owner := queuetestutil.QueueOwner("consumer")
			for handled := 0; handled < q; {
				localCtx, localCancel := context.WithTimeout(ctx, 8*time.Second)
				msg, err := cli.Dequeue(localCtx, queue, lockdclient.DequeueOptions{
					Owner:        owner,
					BlockSeconds: 1,
				})
				localCancel()
				if err != nil {
					if ctx.Err() != nil {
						shutdownWithError(t, fmt.Errorf("consumer context cancelled: %w", ctx.Err()))
					}
					if shouldRetryQueueError(err, nil) {
						continue
					}
					shutdownWithError(t, err)
				}
				if msg == nil {
					continue
				}
				_ = queuetestutil.ReadMessagePayload(t, msg)
				ackCtx, ackCancel := context.WithTimeout(ctx, 8*time.Second)
				if err := msg.Ack(ackCtx); err != nil {
					ackCancel()
					if ctx.Err() != nil {
						shutdownWithError(t, fmt.Errorf("consumer context cancelled during ack: %w", ctx.Err()))
					}
					if shouldRetryQueueError(err, nil) {
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
}

func runMinioQueuePollingIntervalRespected(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-poll-interval", 30*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{
		PollInterval:      3 * time.Second,
		PollJitter:        0,
		ResilientInterval: 3 * time.Second,
	})

	logToTesting := false
	capture := queuetestutil.NewLogCaptureWithOptions(t, queuetestutil.LogCaptureOptions{
		Prefixes:     []string{"queue.dispatcher.poll", "storage.list_objects.begin"},
		LogLevel:     pslog.TraceLevel,
		LogToTesting: &logToTesting,
	})
	ts := startMinioQueueServerWithLogger(t, cfg, capture.Logger())
	queue := queuetestutil.QueueName("minio-poll-interval")
	owner := queuetestutil.QueueOwner("consumer")

	queuetestutil.VerifyPollIntervalRespect(t, capture, ts.Client, queue, owner, 3*time.Second)
}

func runMinioQueueQRFThrottling(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "minio-qrf-throttling", 15*time.Second)

	cfg := prepareMinioQueueConfig(t, minioQueueOptions{})
	cfg.QRFDisabled = false
	cfg.QRFQueueSoftLimit = 1
	cfg.QRFQueueHardLimit = 3
	cfg.QRFLockSoftLimit = 1
	cfg.QRFLockHardLimit = 2
	// Keep host-derived thresholds effectively disabled so this test remains
	// deterministic and only validates queue-driven QRF transitions.
	cfg.QRFMemorySoftLimitPercent = 1000
	cfg.QRFMemoryHardLimitPercent = 1000
	cfg.QRFMemorySoftLimitBytes = uint64(1) << 62
	cfg.QRFMemoryHardLimitBytes = uint64(1) << 62
	cfg.QRFCPUPercentSoftLimit = 0
	cfg.QRFCPUPercentHardLimit = 0
	cfg.QRFCPUPercentSoftLimitSet = true
	cfg.QRFCPUPercentHardLimitSet = true
	cfg.QRFSwapSoftLimitPercent = 0
	cfg.QRFSwapHardLimitPercent = 0
	cfg.QRFSwapSoftLimitBytes = 0
	cfg.QRFSwapHardLimitBytes = 0
	cfg.QRFLoadSoftLimitMultiplier = 1_000_000
	cfg.QRFLoadHardLimitMultiplier = 1_000_000
	cfg.QRFSoftDelay = 40 * time.Millisecond
	cfg.QRFEngagedDelay = 200 * time.Millisecond
	cfg.QRFRecoveryDelay = 120 * time.Millisecond
	cfg.QRFRecoverySamples = 1
	cfg.QRFMaxWait = time.Millisecond
	cfg.LSFSampleInterval = time.Hour
	ts, capture := startMinioQueueServerWithCapture(t, cfg)
	cli := ts.Client
	queue := queuetestutil.QueueName("minio-qrf")

	if ts.Server == nil {
		t.Fatalf("nil server")
	}

	assertThrottled := func(expectState string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := cli.Enqueue(ctx, queue, bytes.NewReader([]byte("payload")), lockdclient.EnqueueOptions{ContentType: "application/octet-stream"})
		if err == nil {
			t.Fatalf("expected throttle (%s), enqueue succeeded", expectState)
		}
		var apiErr *lockdclient.APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("expected APIError, got %v", err)
		}
		if apiErr.Status != 429 || apiErr.Response.ErrorCode != "throttled" {
			t.Fatalf("expected throttled 429, got status=%d err=%s", apiErr.Status, apiErr.Response.ErrorCode)
		}
		if apiErr.QRFState != expectState {
			t.Fatalf("expected qrf state %s, got %s", expectState, apiErr.QRFState)
		}
	}

	ts.Server.ForceQRFObserve(qrf.Snapshot{QueueProducerInflight: cfg.QRFQueueSoftLimit + 1, CollectedAt: time.Now()})
	assertThrottled("soft_arm")

	ts.Server.ForceQRFObserve(qrf.Snapshot{QueueProducerInflight: cfg.QRFQueueHardLimit, CollectedAt: time.Now()})
	assertThrottled("engaged")

	ts.Server.ForceQRFObserve(qrf.Snapshot{CollectedAt: time.Now()})
	ts.Server.ForceQRFObserve(qrf.Snapshot{CollectedAt: time.Now().Add(100 * time.Millisecond)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Enqueue(ctx, queue, bytes.NewReader([]byte("ok")), lockdclient.EnqueueOptions{ContentType: "application/octet-stream"}); err != nil {
		shutdownWithError(t, err)
	}
	msg := queuetestutil.MustDequeueMessage(t, cli, queue, queuetestutil.QueueOwner("cleanup"), 1, 5*time.Second)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("cleanup ack: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, cli, queue)

	softLogs := capture.CountSince(0, "lockd.qrf.soft_arm")
	engagedLogs := capture.CountSince(0, "lockd.qrf.engaged")
	disLogs := capture.CountSince(0, "lockd.qrf.disengaged")
	if softLogs == 0 || engagedLogs == 0 {
		t.Fatalf("expected qrf transitions, got soft=%d engaged=%d", softLogs, engagedLogs)
	}
	if disLogs == 0 {
		t.Fatalf("expected qrf to disengage after load")
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

func shouldRetryQueueError(err error, checklist *queuetestutil.QRFChecklist) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		time.Sleep(100 * time.Millisecond)
		return true
	}
	var apiErr *lockdclient.APIError
	if errors.As(err, &apiErr) {
		if checklist != nil {
			checklist.RecordError(err)
		}
		switch apiErr.Response.ErrorCode {
		case "waiting":
			time.Sleep(25 * time.Millisecond)
			return true
		case "throttled":
			if d := apiErr.RetryAfterDuration(); d > 0 {
				time.Sleep(d)
			} else if apiErr.Response.RetryAfterSeconds > 0 {
				time.Sleep(time.Duration(apiErr.Response.RetryAfterSeconds) * time.Second)
			} else {
				time.Sleep(50 * time.Millisecond)
			}
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
	if strings.Contains(err.Error(), "all endpoints unreachable") {
		time.Sleep(300 * time.Millisecond)
		return true
	}
	return false
}
