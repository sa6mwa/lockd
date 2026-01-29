//go:build integration && aws && lq

package awsintegration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/hatest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/pslog"
)

func TestAWSQueueAdvancedScenarios(t *testing.T) {
	t.Run("MultiConsumerContention", runAWSQueueMultiConsumerContention)
	t.Run("NackVisibilityDelay", runAWSQueueNackVisibilityDelay)
	t.Run("LeaseTimeoutHandoff", runAWSQueueLeaseTimeoutHandoff)
	t.Run("MultiServerRouting", runAWSQueueMultiServerRouting)
	t.Run("MultiServerFailoverClient", runAWSQueueMultiServerFailoverClient)
	t.Run("QRFThrottling", runAWSQueueQRFThrottling)
	t.Run("PollingIntervalRespected", runAWSQueuePollingIntervalRespected)
}

func runAWSQueueMultiConsumerContention(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-contention", 25*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      150 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 1500 * time.Millisecond,
		SweeperInterval:   5 * time.Minute,
	})
	cfg.HAMode = "concurrent"

	ts := startAWSQueueServer(t, cfg)
	cli := ts.Client
	ensureAWSQueueWritableOrSkip(t, cli)

	queue := queuetestutil.QueueName("aws-contention")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("only-once"))

	const (
		workers          = 5
		workerTimeout    = 12 * time.Second
		workerBlockSecs  = int64(1)
		workerVisibility = 10 * time.Second
	)
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
			ctx, cancel := context.WithTimeout(context.Background(), workerTimeout)
			defer cancel()
			msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
				Owner:        owner,
				BlockSeconds: workerBlockSecs,
				Visibility:   workerVisibility,
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
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func runAWSQueueNackVisibilityDelay(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-nack-delay", 30*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      200 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 2500 * time.Millisecond,
		SweeperInterval:   5 * time.Minute,
	})

	ts := startAWSQueueServer(t, cfg)
	cli := ts.Client
	ensureAWSQueueWritableOrSkip(t, cli)

	queue := queuetestutil.QueueName("aws-nack-delay")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("aws-nack-delay"))

	owner := queuetestutil.QueueOwner("consumer")
	initialCtx, initialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	msg, err := cli.Dequeue(initialCtx, queue, lockdclient.DequeueOptions{
		Owner:        owner,
		BlockSeconds: 1,
		Visibility:   2 * time.Second,
	})
	initialCancel()
	if err != nil {
		t.Fatalf("initial dequeue: %v", err)
	}
	if msg == nil || msg.MessageID() == "" {
		t.Fatalf("initial dequeue returned nil message")
	}

	ctxNack, cancelNack := context.WithTimeout(context.Background(), 20*time.Second)
	if err := msg.Nack(ctxNack, 1500*time.Millisecond, map[string]any{"reason": "simulated"}); err != nil {
		cancelNack()
		t.Fatalf("nack: %v", err)
	}
	cancelNack()

	start := time.Now()
	var msgRedelivered *lockdclient.QueueMessage
	deadline := time.Now().Add(12 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("redelivery not observed within 12s")
		}
		pollCtx, pollCancel := context.WithTimeout(context.Background(), 3*time.Second)
		candidate, err := cli.Dequeue(pollCtx, queue, lockdclient.DequeueOptions{
			Owner:        owner,
			BlockSeconds: 1,
			Visibility:   2 * time.Second,
		})
		pollCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				continue
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) {
				if apiErr.Response.ErrorCode == "waiting" {
					continue
				}
				if apiErr.Status >= 500 {
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}
			t.Fatalf("dequeue redelivery: %v", err)
		}
		if candidate == nil || candidate.MessageID() == "" {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		msgRedelivered = candidate
		break
	}
	delay := time.Since(start)
	if delay < 1400*time.Millisecond {
		t.Fatalf("expected delay >= 1.4s, got %s", delay)
	}
	if msgRedelivered.Attempts() <= msg.Attempts() {
		t.Fatalf("expected attempts to increase after nack delay")
	}
	_ = queuetestutil.ReadMessagePayload(t, msgRedelivered)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 8*time.Second)
	if err := msgRedelivered.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack after nack: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runAWSQueueLeaseTimeoutHandoff(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-lease-handoff", 45*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      250 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 3 * time.Second,
		SweeperInterval:   5 * time.Minute,
	})

	ts := startAWSQueueServer(t, cfg)
	cli := ts.Client
	ensureAWSQueueWritableOrSkip(t, cli)

	queue := queuetestutil.QueueName("aws-lease-handoff")
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("lease-me"))

	ownerA := queuetestutil.QueueOwner("consumer-a")
	ctxA, cancelA := context.WithTimeout(context.Background(), 15*time.Second)
	msgA, err := cli.Dequeue(ctxA, queue, lockdclient.DequeueOptions{
		Owner:        ownerA,
		BlockSeconds: 5,
		Visibility:   2200 * time.Millisecond,
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

	time.Sleep(3 * time.Second)

	ownerB := queuetestutil.QueueOwner("consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, ownerB, 5, 12*time.Second)
	if msgB.Attempts() <= firstAttempts {
		t.Fatalf("expected attempts to increase after lease timeout")
	}
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 8*time.Second)
	if err := msgB.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack consumer B: %v", err)
	}
	ackCancel()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 6*time.Second)
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

func runAWSQueueMultiServerRouting(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-routing", 40*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      150 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 1500 * time.Millisecond,
		SweeperInterval:   5 * time.Minute,
	})
	cfg.HAMode = "concurrent"

	serverA := startAWSQueueServer(t, cfg)
	serverB := startAWSQueueServer(t, cfg)

	queue := queuetestutil.QueueName("aws-routing")
	payload := []byte("shared-aws-backend")
	queuetestutil.MustEnqueueBytes(t, serverA.Client, queue, payload)

	owner := queuetestutil.QueueOwner("consumer-b")
	msg := queuetestutil.MustDequeueMessage(t, serverB.Client, queue, owner, 5, 15*time.Second)
	body := queuetestutil.ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("unexpected payload: got %q want %q", string(body), string(payload))
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 8*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack via serverB: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, serverA.Client, queue)
}

func runAWSQueueMultiServerFailoverClient(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-failover", 45*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      150 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 1500 * time.Millisecond,
		SweeperInterval:   5 * time.Minute,
	})

	serverA := startAWSQueueServer(t, cfg)
	serverB := startAWSQueueServer(t, cfg)
	ensureAWSQueueWritableOrSkip(t, serverA.Client)

	queue := queuetestutil.QueueName("aws-failover")
	activeCtx, cancelActive := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelActive()
	activeServer, activeClient, err := hatest.FindActiveServer(activeCtx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	t.Cleanup(func() { _ = activeClient.Close() })
	ensureAWSQueueWritableOrSkip(t, activeClient)
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
	if activeServer.Config.MTLSEnabled() {
		creds := activeServer.TestMTLSCredentials()
		if !creds.Valid() {
			t.Fatalf("aws failover: serverA missing MTLS credentials")
		}
		httpClient, err := creds.NewHTTPClient()
		if err != nil {
			t.Fatalf("aws failover: build MTLS http client: %v", err)
		}
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
		BlockSeconds: 5,
	})
	dequeueCancel()
	if err != nil {
		t.Fatalf("failover dequeue: %v", err)
	}

	body := queuetestutil.ReadMessagePayload(t, msg)
	if string(body) != "failover-payload" {
		t.Fatalf("unexpected payload after failover: %q", string(body))
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 8*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack after failover: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, standbyClient, queue)
}

func runAWSQueueQRFThrottling(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-qrf-throttling", 120*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{})
	cfg.QRFDisabled = false
	cfg.QRFQueueSoftLimit = 10
	cfg.QRFQueueHardLimit = 20
	cfg.QRFLockSoftLimit = 5
	cfg.QRFLockHardLimit = 10
	cfg.QRFMemorySoftLimitPercent = 70
	cfg.QRFMemoryHardLimitPercent = 85
	cfg.QRFMemorySoftLimitBytes = 0
	cfg.QRFMemoryHardLimitBytes = 0
	cfg.QRFLoadSoftLimitMultiplier = 3
	cfg.QRFLoadHardLimitMultiplier = 6
	cfg.QRFSoftDelay = 70 * time.Millisecond
	cfg.QRFEngagedDelay = 220 * time.Millisecond
	cfg.QRFRecoveryDelay = 150 * time.Millisecond
	cfg.QRFRecoverySamples = 1
	cfg.QRFMaxWait = time.Millisecond
	ts, capture := startAWSQueueServerWithCapture(t, cfg)
	cli := ts.Client
	ensureAWSQueueWritableOrSkip(t, cli)

	queue := queuetestutil.QueueName("aws-qrf")
	var checklist queuetestutil.QRFChecklist

	// Baseline enqueue/dequeue to confirm the queue is healthy before forcing QRF states.
	baseMsg := []byte("baseline")
	queuetestutil.MustEnqueueBytes(t, cli, queue, baseMsg)
	baseline := queuetestutil.MustDequeueMessage(t, cli, queue, queuetestutil.QueueOwner("baseline"), 1, 15*time.Second)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := baseline.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("baseline ack: %v", err)
	}
	ackCancel()
	queuetestutil.EnsureQueueEmpty(t, cli, queue)

	assertThrottled := func(expectState string) {
		ctxProbe, cancelProbe := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelProbe()
		_, err := cli.Enqueue(ctxProbe, queue, bytes.NewReader([]byte("probe")), lockdclient.EnqueueOptions{ContentType: "application/octet-stream"})
		if err == nil {
			t.Fatalf("expected throttle (%s), enqueue succeeded", expectState)
		}
		var apiErr *lockdclient.APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("expected APIError, got %v", err)
		}
		if apiErr.Status != http.StatusTooManyRequests || apiErr.Response.ErrorCode != "throttled" {
			t.Fatalf("expected throttled 429, got status=%d err=%s", apiErr.Status, apiErr.Response.ErrorCode)
		}
		if !strings.EqualFold(apiErr.QRFState, expectState) {
			t.Fatalf("expected qrf state %s, got %s", expectState, apiErr.QRFState)
		}
		checklist.RecordError(err)
	}

	if ts.Server == nil {
		t.Fatalf("nil server")
	}

	ts.Server.ForceQRFObserve(qrf.Snapshot{QueueProducerInflight: cfg.QRFQueueSoftLimit + 1, CollectedAt: time.Now()})
	assertThrottled("soft_arm")

	ts.Server.ForceQRFObserve(qrf.Snapshot{QueueProducerInflight: cfg.QRFQueueHardLimit, CollectedAt: time.Now()})
	assertThrottled("engaged")

	recoveryDeadline := time.Now().Add(30 * time.Second)
	var recovered bool
	for !recovered && time.Now().Before(recoveryDeadline) {
		first := time.Now()
		ts.Server.ForceQRFObserve(qrf.Snapshot{CollectedAt: first})
		ts.Server.ForceQRFObserve(qrf.Snapshot{CollectedAt: first.Add(150 * time.Millisecond)})
		time.Sleep(500 * time.Millisecond)
		status := ts.Server.QRFStatus()
		t.Logf("aws-qrf: recovery loop state=%s reason=%q", status.State.String(), status.Reason)
		if status.State == qrf.StateDisengaged {
			recovered = true
			break
		}
	}
	status := ts.Server.QRFStatus()
	if !recovered {
		t.Fatalf("expected QRF to disengage for recovery enqueue (state=%s reason=%q)", status.State.String(), status.Reason)
	}
	recoveryEnqueueCtx, recoveryEnqueueCancel := context.WithTimeout(context.Background(), 20*time.Second)
	if _, err := cli.Enqueue(recoveryEnqueueCtx, queue, bytes.NewReader([]byte("recovery")), lockdclient.EnqueueOptions{ContentType: "application/octet-stream"}); err != nil {
		recoveryEnqueueCancel()
		shutdownWithError(t, err)
	}
	recoveryEnqueueCancel()
	recoveryMsg := queuetestutil.MustDequeueMessage(t, cli, queue, queuetestutil.QueueOwner("recovery"), 1, 15*time.Second)
	recoveryAckCtx, recoveryAckCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := recoveryMsg.Ack(recoveryAckCtx); err != nil {
		recoveryAckCancel()
		t.Fatalf("recovery ack: %v", err)
	}
	recoveryAckCancel()
	queuetestutil.EnsureQueueEmpty(t, cli, queue)

	if !checklist.Throttled() {
		t.Fatalf("expected client to observe throttling responses")
	}
	if !checklist.SoftArmSeen() {
		t.Fatalf("expected client to observe soft_arm posture")
	}
	if !checklist.EngagedSeen() {
		t.Fatalf("expected client to observe engaged posture")
	}

	softLogs := capture.CountSince(0, "lockd.qrf.soft_arm")
	engagedLogs := capture.CountSince(0, "lockd.qrf.engaged")
	disLogs := capture.CountSince(0, "lockd.qrf.disengaged")
	if softLogs == 0 || engagedLogs == 0 {
		t.Fatalf("expected QRF transitions, got soft=%d engaged=%d", softLogs, engagedLogs)
	}
	if disLogs == 0 {
		t.Fatalf("expected QRF to disengage after load")
	}

	states := checklist.StateCounts()
	if states["soft_arm"] == 0 {
		t.Fatalf("client did not record soft_arm throttling state")
	}
	if states["engaged"] == 0 {
		t.Fatalf("client did not record engaged throttling state")
	}
}

func runAWSQueuePollingIntervalRespected(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-poll-interval", 60*time.Second)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      3 * time.Second,
		PollJitter:        0,
		ResilientInterval: 3 * time.Second,
		SweeperInterval:   5 * time.Minute,
	})

	logToTesting := false
	capture := queuetestutil.NewLogCaptureWithOptions(t, queuetestutil.LogCaptureOptions{
		Prefixes:     []string{"queue.dispatcher.poll", "storage.list_objects.begin"},
		LogLevel:     pslog.TraceLevel,
		LogToTesting: &logToTesting,
	})
	ts := newAWSQueueTestServer(t, cfg, capture.Logger())
	cli := ts.Client
	ensureAWSQueueWritableOrSkip(t, cli)

	queue := queuetestutil.QueueName("aws-poll-interval")
	owner := queuetestutil.QueueOwner("consumer")

	queuetestutil.VerifyPollIntervalRespect(t, capture, cli, queue, owner, 3*time.Second)
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
		time.Sleep(50 * time.Millisecond)
		return true
	}
	if strings.Contains(err.Error(), "context deadline exceeded") {
		time.Sleep(150 * time.Millisecond)
		return true
	}
	var apiErr *lockdclient.APIError
	if errors.As(err, &apiErr) {
		if checklist != nil {
			checklist.RecordError(err)
		}
		switch apiErr.Response.ErrorCode {
		case "waiting":
			time.Sleep(100 * time.Millisecond)
			return true
		case "throttled":
			if d := apiErr.RetryAfterDuration(); d > 0 {
				time.Sleep(d)
			} else if apiErr.Response.RetryAfterSeconds > 0 {
				time.Sleep(time.Duration(apiErr.Response.RetryAfterSeconds) * time.Second)
			} else {
				time.Sleep(150 * time.Millisecond)
			}
			return true
		case "internal_error":
			if strings.Contains(apiErr.Response.Detail, "context deadline exceeded") || strings.Contains(err.Error(), "context deadline exceeded") {
				time.Sleep(150 * time.Millisecond)
				return true
			}
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
		time.Sleep(200 * time.Millisecond)
		return true
	}
	return false
}
