//go:build bench && aws

package awsbench

import (
	"bytes"
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/benchmark/internal/benchenv"
	"pkt.systems/lockd/benchmark/internal/lqbench"
	lockdclient "pkt.systems/lockd/client"
	coreinternal "pkt.systems/lockd/internal/core"
	queueinternal "pkt.systems/lockd/internal/queue"
	"pkt.systems/pslog"
)

const awsQueuePayloadSize = 256

func BenchmarkAWSQueueAckLease(b *testing.B) {
	cfg := benchenv.LoadAWSConfig(b)
	applyQueueBenchConfig(&cfg)
	benchenv.EnsureAWSStoreReady(b, cfg)
	b.Cleanup(func() {
		benchenv.CleanupBenchmarkPrefix(b, cfg)
	})

	client := startAWSBenchServer(b, cfg)
	probeQueueWritable(b, client)
	payload := bytes.Repeat([]byte("x"), awsQueuePayloadSize)
	queueinternal.EnableReadyCacheStats()
	queueinternal.ResetReadyCacheStats()
	coreinternal.EnableQueueDeliveryStats()
	coreinternal.ResetQueueDeliveryStats()
	scenario := lqbench.Scenario{
		Name:          "aws-queue",
		Producers:     4,
		Consumers:     4,
		TotalMessages: 200,
		Prefetch:      4,
		UseSubscribe:  true,
		Timeout:       2 * time.Minute,
	}
	result := lqbench.Run(b, []*lockdclient.Client{client}, scenario, payload)
	if result.Produced != int64(scenario.TotalMessages) || result.Consumed != int64(scenario.TotalMessages) {
		b.Fatalf("queue bench incomplete: produced=%d consumed=%d target=%d", result.Produced, result.Consumed, scenario.TotalMessages)
	}
	reportReadyCacheListStats(b)
	reportQueueDeliveryStats(b)
}

func applyQueueBenchConfig(cfg *lockd.Config) {
	if cfg == nil {
		return
	}
	cfg.HAMode = "concurrent"
	cfg.QueuePollInterval = 50 * time.Millisecond
	cfg.QueueResilientPollInterval = 500 * time.Millisecond
	cfg.QueuePollJitter = 0
	cfg.QRFDisabled = true
	if cfg.SweeperInterval <= 0 {
		cfg.SweeperInterval = 2 * time.Second
	}
	cfg.ListenProto = "tcp"
	cfg.Listen = "127.0.0.1:0"
}

func reportReadyCacheListStats(b *testing.B) {
	stats := queueinternal.ReadyCacheStatsSnapshot()
	if stats.ListScans == 0 {
		return
	}
	b.ReportMetric(float64(stats.ListScans), "ready_cache_list_scans")
	b.ReportMetric(float64(stats.ListObjects), "ready_cache_list_objects")
	if stats.ListNanos > 0 {
		avgMs := float64(stats.ListNanos) / float64(stats.ListScans) / 1e6
		b.ReportMetric(avgMs, "ready_cache_list_avg_ms")
	}
	if stats.LoadCount > 0 && stats.LoadNanos > 0 {
		avgMs := float64(stats.LoadNanos) / float64(stats.LoadCount) / 1e6
		b.ReportMetric(avgMs, "ready_cache_load_avg_ms")
	}
	if stats.NextCalls > 0 {
		b.ReportMetric(float64(stats.NextCalls), "ready_cache_next_calls")
		if stats.ListScans > 0 {
			b.ReportMetric(float64(stats.ListScans)/float64(stats.NextCalls), "ready_cache_list_scans_per_next")
		}
		if stats.LoadCount > 0 {
			b.ReportMetric(float64(stats.LoadCount)/float64(stats.NextCalls), "ready_cache_loads_per_next")
		}
		if stats.RefreshWaits > 0 {
			b.ReportMetric(float64(stats.RefreshWaits), "ready_cache_refresh_waits")
			avgMs := float64(stats.RefreshWaitNanos) / float64(stats.RefreshWaits) / 1e6
			b.ReportMetric(avgMs, "ready_cache_refresh_wait_avg_ms")
		}
	}
}

func reportQueueDeliveryStats(b *testing.B) {
	stats := coreinternal.QueueDeliveryStatsSnapshot()
	if stats.AcquireCount > 0 {
		b.ReportMetric(float64(stats.AcquireCount), "queue_delivery_acquire_count")
		avgMs := float64(stats.AcquireNanos) / float64(stats.AcquireCount) / 1e6
		b.ReportMetric(avgMs, "queue_delivery_acquire_avg_ms")
	}
	if stats.IncrementCount > 0 {
		b.ReportMetric(float64(stats.IncrementCount), "queue_delivery_increment_count")
		avgMs := float64(stats.IncrementNanos) / float64(stats.IncrementCount) / 1e6
		b.ReportMetric(avgMs, "queue_delivery_increment_avg_ms")
	}
	if stats.PayloadCount > 0 {
		b.ReportMetric(float64(stats.PayloadCount), "queue_delivery_payload_count")
		avgMs := float64(stats.PayloadNanos) / float64(stats.PayloadCount) / 1e6
		b.ReportMetric(avgMs, "queue_delivery_payload_avg_ms")
	}
	if stats.AckCount > 0 {
		b.ReportMetric(float64(stats.AckCount), "queue_ack_count")
		avgMs := float64(stats.AckNanos) / float64(stats.AckCount) / 1e6
		b.ReportMetric(avgMs, "queue_ack_avg_ms")
	}
}

func startAWSBenchServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()
	mtlsEnabled := benchenv.TestMTLSEnabled()
	cfg.DisableMTLS = !mtlsEnabled

	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(120 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithLogger(pslog.NoopLogger()),
	}

	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLogger(pslog.NoopLogger()),
		lockd.WithTestClientOptions(clientOpts...),
		lockd.WithTestStartTimeout(30 * time.Second),
	}
	if mtlsEnabled {
		opts = append(opts, benchenv.SharedMTLSOptions(tb)...)
	} else {
		opts = append(opts,
			lockd.WithoutTestMTLS(),
			lockd.WithTestClientOptions(lockdclient.WithDisableMTLS(true)),
		)
	}
	ts := lockd.StartTestServer(tb, opts...)
	return ts.Client
}

func probeQueueWritable(tb testing.TB, cli *lockdclient.Client) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queue := "bench-probe-" + time.Now().UTC().Format("20060102150405.000000000")
	if _, err := cli.EnqueueBytes(ctx, queue, []byte("probe"), lockdclient.EnqueueOptions{}); err != nil {
		tb.Fatalf("enqueue probe: %v", err)
	}
	opts := lockdclient.DequeueOptions{
		Owner:        "bench-probe",
		BlockSeconds: 5,
		PageSize:     1,
	}
	var msg *lockdclient.QueueMessage
	for i := 0; i < 3; i++ {
		msgs, err := cli.DequeueBatch(ctx, queue, opts)
		if err != nil {
			tb.Fatalf("dequeue probe: %v", err)
		}
		if len(msgs) > 0 {
			msg = msgs[0]
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if msg == nil {
		tb.Fatalf("dequeue probe returned no messages")
	}
	ackCtx, ackCancel := context.WithTimeout(ctx, 15*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		tb.Fatalf("ack probe: %v", err)
	}
	ackCancel()
}
