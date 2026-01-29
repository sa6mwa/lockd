//go:build loadtest && aws && lq

package awslqlt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

func TestAWSQRFLoadBalancing(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "aws-qrf-load", 5*time.Minute)

	cfg := prepareAWSQueueConfig(t, awsQueueOptions{
		PollInterval:      150 * time.Millisecond,
		PollJitter:        0,
		ResilientInterval: 1500 * time.Millisecond,
	})

	ts, capture := startAWSQueueServerWithCapture(t, cfg)
	cli := ts.Client
	ensureAWSQueueReadyOrFail(t, cli)

	queue := queuetestutil.QueueName("aws-qrf-load")

	const (
		producers           = 32
		consumers           = 12
		messagesPerProducer = 12
	)
	totalMessages := int64(producers * messagesPerProducer)

	payload := bytes.Repeat([]byte("x"), 1024)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	var remaining atomic.Int64
	remaining.Store(totalMessages)

	var producerThrottle atomic.Int64
	var consumerThrottle atomic.Int64

	var producerWG sync.WaitGroup
	for i := 0; i < producers; i++ {
		producerWG.Add(1)
		go func(id int) {
			defer producerWG.Done()
			sent := 0
			for sent < messagesPerProducer && ctx.Err() == nil {
				callCtx, callCancel := context.WithTimeout(ctx, 30*time.Second)
				_, err := cli.Enqueue(callCtx, queue, bytes.NewReader(payload), lockdclient.EnqueueOptions{ContentType: "application/octet-stream"})
				callCancel()
				if err != nil {
					var apiErr *lockdclient.APIError
					if errors.As(err, &apiErr) {
						if apiErr.Response.ErrorCode == "throttled" {
							producerThrottle.Add(1)
							sleepRetry(apiErr.Response.RetryAfterSeconds)
							continue
						}
						if apiErr.Response.ErrorCode == "waiting" {
							time.Sleep(50 * time.Millisecond)
							continue
						}
					}
					shutdownWithError(t, err)
				}
				sent++
			}
		}(i)
	}

	var consumerWG sync.WaitGroup
	for i := 0; i < consumers; i++ {
		consumerWG.Add(1)
		go func(id int) {
			defer consumerWG.Done()
			owner := queuetestutil.QueueOwner(fmt.Sprintf("consumer-%d", id))
			emptyStreak := 0
			for ctx.Err() == nil {
				if remaining.Load() <= 0 {
					break
				}
				callCtx, callCancel := context.WithTimeout(ctx, 30*time.Second)
				msg, err := cli.Dequeue(callCtx, queue, lockdclient.DequeueOptions{
					Owner:        owner,
					BlockSeconds: 1,
				})
				callCancel()
				if err != nil {
					var apiErr *lockdclient.APIError
					if errors.As(err, &apiErr) {
						switch apiErr.Response.ErrorCode {
						case "throttled":
							consumerThrottle.Add(1)
							sleepRetry(apiErr.Response.RetryAfterSeconds)
							continue
						case "waiting":
							emptyStreak++
							if emptyStreak > 10 && remaining.Load() <= 0 {
								return
							}
							time.Sleep(50 * time.Millisecond)
							continue
						}
					}
					shutdownWithError(t, err)
				}
				emptyStreak = 0
				if msg == nil {
					continue
				}
				_ = queuetestutil.ReadMessagePayload(t, msg)
				ackCtx, ackCancel := context.WithTimeout(ctx, 30*time.Second)
				if err := msg.Ack(ackCtx); err != nil {
					ackCancel()
					var apiErr *lockdclient.APIError
					if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "throttled" {
						consumerThrottle.Add(1)
						sleepRetry(apiErr.Response.RetryAfterSeconds)
						continue
					}
					shutdownWithError(t, err)
				}
				ackCancel()
				atomic.AddInt64(&remaining, -1)
			}
		}(i)
	}

	producerWG.Wait()
	consumerWG.Wait()

	if ctx.Err() != nil {
		t.Fatalf("load test timed out: %v", ctx.Err())
	}

	queuetestutil.EnsureQueueEmpty(t, cli, queue)

	if capture.CountSince(0, "lockd.qrf.engaged") == 0 {
		t.Fatalf("expected QRF to engage under load")
	}
	if capture.CountSince(0, "lockd.qrf.disengaged") == 0 {
		t.Fatalf("expected QRF to disengage after load")
	}

	if producerThrottle.Load() == 0 {
		t.Fatalf("expected producers to experience throttling")
	}
	if consumerThrottle.Load() > producerThrottle.Load()/2+1 {
		t.Fatalf("expected consumers to be throttled less than producers (prod=%d cons=%d)", producerThrottle.Load(), consumerThrottle.Load())
	}
}

// Helper constructs

type awsQueueOptions struct {
	PollInterval      time.Duration
	PollJitter        time.Duration
	ResilientInterval time.Duration
}

func prepareAWSQueueConfig(t testing.TB, opts awsQueueOptions) lockd.Config {
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		t.Skip("LOCKD_STORE must point to aws:// bucket for load test")
	}
	if !strings.HasPrefix(store, "aws://") {
		t.Fatalf("LOCKD_STORE must be aws://, got %q", store)
	}
	cfg := lockd.Config{
		Store:       store,
		AWSRegion:   os.Getenv("LOCKD_AWS_REGION"),
		AWSKMSKeyID: os.Getenv("LOCKD_AWS_KMS_KEY_ID"),
		QRFDisabled: false,
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = os.Getenv("AWS_REGION")
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = os.Getenv("AWS_DEFAULT_REGION")
	}
	cfg.QueuePollInterval = opts.PollInterval
	cfg.QueuePollJitter = opts.PollJitter
	cfg.QueueResilientPollInterval = opts.ResilientInterval
	if err := cfg.Validate(); err != nil {
		t.Fatalf("aws queue config validation failed: %v", err)
	}
	cfg.QRFDisabled = false
	return cfg
}

func startAWSQueueServerWithCapture(t testing.TB, cfg lockd.Config) (*lockd.TestServer, *queuetestutil.LogCapture) {
	capture := queuetestutil.NewLogCapture(t)
	ts := queuetestutil.StartQueueTestServerWithLogger(t, cfg, capture.Logger())
	return ts, capture
}

func ensureAWSQueueReadyOrFail(t testing.TB, cli *lockdclient.Client) {
	queue := queuetestutil.QueueName("aws-qrf-permission")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := cli.Enqueue(ctx, queue, bytes.NewReader([]byte("probe")), lockdclient.EnqueueOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("enqueue probe failed: %v", err)
	}
	owner := queuetestutil.QueueOwner("aws-qrf-owner")
	msg := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 1, 30*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msg)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack probe failed: %v", err)
	}
	ackCancel()
}

func sleepRetry(seconds int64) {
	if seconds <= 0 {
		time.Sleep(100 * time.Millisecond)
		return
	}
	time.Sleep(time.Duration(seconds) * time.Second)
}

func shutdownWithError(t testing.TB, err error) {
	if err != nil {
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) {
			t.Fatalf("queue error: %s", apiErr.Response.Detail)
		}
		t.Fatalf("queue error: %v", err)
	}
}
