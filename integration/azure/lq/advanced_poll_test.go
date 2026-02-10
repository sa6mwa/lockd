//go:build integration && azure && lq

package azureintegration

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

func TestAzureQueueAdvancedScenarios(t *testing.T) {
	t.Run("MultiConsumerContention", runAzureQueueMultiConsumerContention)
	t.Run("NackVisibilityDelay", runAzureQueueNackVisibilityDelay)
	t.Run("LeaseTimeoutHandoff", runAzureQueueLeaseTimeoutHandoff)
	t.Run("MultiServerRouting", runAzureQueueMultiServerRouting)
	t.Run("MultiServerFailoverClient", runAzureQueueMultiServerFailoverClient)
	t.Run("QRFThrottling", runAzureQueueQRFThrottling)
	t.Run("PollingIntervalRespected", runAzureQueuePollingIntervalRespected)
}

func runAzureQueueMultiConsumerContention(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-contention", 60*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      200 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 2 * time.Second,
	})
	cfg.HAMode = "concurrent"

	ts := startAzureQueueServer(t, cfg)
	cli := ts.Client
	ensureAzureQueueWritableOrSkip(t, cfg, cli)

	queue := queuetestutil.QueueName("azure-contention")
	scheduleAzureQueueCleanup(t, cfg, queue)
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("only-once"))

	const workers = 4
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
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
				Owner:        owner,
				BlockSeconds: 2,
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
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
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

func runAzureQueueNackVisibilityDelay(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-nack-delay", 60*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      225 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 2500 * time.Millisecond,
	})

	ts := startAzureQueueServer(t, cfg)
	cli := ts.Client
	ensureAzureQueueWritableOrSkip(t, cfg, cli)

	queue := queuetestutil.QueueName("azure-nack-delay")
	scheduleAzureQueueCleanup(t, cfg, queue)
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("azure-nack-delay"))

	owner := queuetestutil.QueueOwner("consumer")
	msg := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 2, 45*time.Second)

	ctxNack, cancelNack := context.WithTimeout(context.Background(), 45*time.Second)
	if err := msg.Nack(ctxNack, 3*time.Second, map[string]any{"reason": "visibility"}); err != nil {
		cancelNack()
		t.Fatalf("nack: %v", err)
	}
	cancelNack()

	start := time.Now()
	msgRedelivered := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 2, 60*time.Second)
	delay := time.Since(start)
	if delay < 2*time.Second {
		t.Fatalf("expected delay >= 2s, got %s", delay)
	}
	if msgRedelivered.Attempts() <= msg.Attempts() {
		t.Fatalf("expected attempts to increase after nack delay")
	}
	_ = queuetestutil.ReadMessagePayload(t, msgRedelivered)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := msgRedelivered.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack after nack: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, cli, queue)
}

func runAzureQueueLeaseTimeoutHandoff(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-lease-handoff", 90*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      250 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 3500 * time.Millisecond,
	})

	ts := startAzureQueueServer(t, cfg)
	cli := ts.Client
	ensureAzureQueueWritableOrSkip(t, cfg, cli)

	queue := queuetestutil.QueueName("azure-lease-handoff")
	scheduleAzureQueueCleanup(t, cfg, queue)
	queuetestutil.MustEnqueueBytes(t, cli, queue, []byte("lease-me"))

	ownerA := queuetestutil.QueueOwner("consumer-a")
	ctxA, cancelA := context.WithTimeout(context.Background(), 45*time.Second)
	msgA, err := cli.Dequeue(ctxA, queue, lockdclient.DequeueOptions{
		Owner:        ownerA,
		BlockSeconds: 2,
		Visibility:   4 * time.Second,
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

	time.Sleep(5 * time.Second)

	ownerB := queuetestutil.QueueOwner("consumer-b")
	msgB := queuetestutil.MustDequeueMessage(t, cli, queue, ownerB, 2, 45*time.Second)
	if msgB.Attempts() <= firstAttempts {
		t.Fatalf("expected attempts to increase after lease timeout")
	}
	_ = queuetestutil.ReadMessagePayload(t, msgB)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := msgB.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack consumer B: %v", err)
	}
	ackCancel()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
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

func runAzureQueueMultiServerRouting(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-routing", 90*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      200 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 2 * time.Second,
	})
	cfg.HAMode = "concurrent"

	serverA := startAzureQueueServer(t, cfg)
	serverB := startAzureQueueServer(t, cfg)
	ensureAzureQueueWritableOrSkip(t, cfg, serverA.Client)

	queue := queuetestutil.QueueName("azure-routing")
	scheduleAzureQueueCleanup(t, cfg, queue)
	payload := []byte("shared-azure-backend")
	queuetestutil.MustEnqueueBytes(t, serverA.Client, queue, payload)

	owner := queuetestutil.QueueOwner("consumer-b")
	msg := queuetestutil.MustDequeueMessage(t, serverB.Client, queue, owner, 2, 120*time.Second)
	body := queuetestutil.ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("unexpected payload: got %q want %q", string(body), string(payload))
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack via serverB: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, serverA.Client, queue)
}

func runAzureQueueMultiServerFailoverClient(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-failover", 90*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
		PollInterval:      200 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 2 * time.Second,
	})

	serverA := startAzureQueueServer(t, cfg)
	serverB := startAzureQueueServer(t, cfg)

	queue := queuetestutil.QueueName("azure-failover")
	scheduleAzureQueueCleanup(t, cfg, queue)
	activeCtx, cancelActive := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelActive()
	activeServer, activeClient, err := hatest.FindActiveServer(activeCtx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	t.Cleanup(func() { _ = activeClient.Close() })
	ensureAzureQueueWritableOrSkip(t, cfg, activeClient)
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
			t.Fatalf("azure failover: serverA missing MTLS credentials")
		}
		httpClient, err := creds.NewHTTPClient()
		if err != nil {
			t.Fatalf("azure failover: build MTLS http client: %v", err)
		}
		clientOptions = append(clientOptions, lockdclient.WithHTTPClient(httpClient))
	}
	failoverClient, err := lockdclient.NewWithEndpoints([]string{activeServer.URL(), standbyServer.URL()}, clientOptions...)
	if err != nil {
		t.Fatalf("new failover client: %v", err)
	}
	t.Cleanup(func() { _ = failoverClient.Close() })

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	dequeueCtx, dequeueCancel := context.WithTimeout(context.Background(), 45*time.Second)
	msg, err := failoverClient.Dequeue(dequeueCtx, queue, lockdclient.DequeueOptions{
		Owner:        owner,
		BlockSeconds: 2,
	})
	dequeueCancel()
	if err != nil {
		t.Fatalf("failover dequeue: %v", err)
	}

	body := queuetestutil.ReadMessagePayload(t, msg)
	if string(body) != "failover-payload" {
		t.Fatalf("unexpected payload after failover: %q", string(body))
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack after failover: %v", err)
	}
	ackCancel()

	queuetestutil.EnsureQueueEmpty(t, standbyClient, queue)
}

func runAzureQueuePollingIntervalRespected(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-poll-interval", 60*time.Second)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{
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
	ts := startAzureQueueServerWithLogger(t, cfg, capture.Logger())
	cli := ts.Client
	ensureAzureQueueWritableOrSkip(t, cfg, cli)

	queue := queuetestutil.QueueName("azure-poll-interval")
	scheduleAzureQueueCleanup(t, cfg, queue)
	owner := queuetestutil.QueueOwner("consumer")

	queuetestutil.VerifyPollIntervalRespect(t, capture, cli, queue, owner, 3*time.Second)
}

func runAzureQueueQRFThrottling(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-qrf-throttling", 2*time.Minute)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{})
	cfg.QRFDisabled = false
	cfg.QRFQueueSoftLimit = 8
	cfg.QRFQueueHardLimit = 16
	cfg.QRFLockSoftLimit = 4
	cfg.QRFLockHardLimit = 8
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
	cfg.QRFSoftDelay = 60 * time.Millisecond
	cfg.QRFEngagedDelay = 220 * time.Millisecond
	cfg.QRFRecoveryDelay = 150 * time.Millisecond
	cfg.QRFRecoverySamples = 1
	cfg.QRFMaxWait = time.Millisecond
	cfg.LSFSampleInterval = time.Hour

	ts, capture := startAzureQueueServerWithCapture(t, cfg)
	cli := ts.Client
	ensureAzureQueueWritableOrSkip(t, cfg, cli)

	queue := queuetestutil.QueueName("azure-qrf")
	scheduleAzureQueueCleanup(t, cfg, queue)
	var checklist queuetestutil.QRFChecklist

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

	recoveryDeadline := time.Now().Add(5 * time.Second)
	var recovered bool
	for !recovered && time.Now().Before(recoveryDeadline) {
		ts.Server.ForceQRFObserve(qrf.Snapshot{CollectedAt: time.Now()})
		time.Sleep(250 * time.Millisecond)
		ctxRecovery, cancelRecovery := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := cli.Enqueue(ctxRecovery, queue, bytes.NewReader([]byte("recovery")), lockdclient.EnqueueOptions{ContentType: "application/octet-stream"})
		cancelRecovery()
		if err == nil {
			recovered = true
			break
		}
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "throttled" {
			continue
		}
		shutdownWithError(t, err)
	}
	if !recovered {
		t.Fatalf("expected QRF to disengage for recovery enqueue")
	}

	msg := queuetestutil.MustDequeueMessage(t, cli, queue, queuetestutil.QueueOwner("azure-recovery"), 1, 10*time.Second)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("recovery ack: %v", err)
	}
	ackCancel()
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
		time.Sleep(75 * time.Millisecond)
		return true
	}
	var apiErr *lockdclient.APIError
	if errors.As(err, &apiErr) {
		if checklist != nil {
			checklist.RecordError(err)
		}
		switch apiErr.Response.ErrorCode {
		case "waiting":
			time.Sleep(150 * time.Millisecond)
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
