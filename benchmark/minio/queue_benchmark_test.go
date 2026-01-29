//go:build bench && minio

package miniointegration

import (
	"bytes"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/benchmark/internal/benchenv"
	"pkt.systems/lockd/benchmark/internal/lqbench"
	lockdclient "pkt.systems/lockd/client"
	coreinternal "pkt.systems/lockd/internal/core"
	queueinternal "pkt.systems/lockd/internal/queue"
)

const minioQueuePayloadSize = 256

func BenchmarkMinioQueueAckLease(b *testing.B) {
	cfg := benchenv.LoadMinioConfig(b)
	if err := cfg.Validate(); err != nil {
		b.Fatalf("config validation: %v", err)
	}
	benchenv.EnsureMinioBucket(b, cfg)
	benchenv.EnsureS3StoreReady(b, cfg)
	applyQueueBenchConfig(&cfg)
	b.Cleanup(func() {
		benchenv.CleanupBenchmarkPrefix(b, cfg)
	})

	client := startMinioBenchServer(b, cfg)
	payload := bytes.Repeat([]byte("x"), minioQueuePayloadSize)
	queueinternal.EnableReadyCacheStats()
	queueinternal.ResetReadyCacheStats()
	coreinternal.EnableQueueDeliveryStats()
	coreinternal.ResetQueueDeliveryStats()
	scenario := lqbench.Scenario{
		Name:          "minio-queue",
		Producers:     6,
		Consumers:     6,
		TotalMessages: 200,
		Prefetch:      4,
		UseSubscribe:  true,
		Timeout:       90 * time.Second,
	}
	lqbench.Run(b, []*lockdclient.Client{client}, scenario, payload)
	reportReadyCacheListStats(b)
	reportQueueDeliveryStats(b)
}

func applyQueueBenchConfig(cfg *lockd.Config) {
	if cfg == nil {
		return
	}
	cfg.QueuePollInterval = 25 * time.Millisecond
	cfg.QueueResilientPollInterval = 250 * time.Millisecond
	cfg.QueuePollJitter = 0
	cfg.QRFDisabled = true
	if cfg.SweeperInterval <= 0 {
		cfg.SweeperInterval = 2 * time.Second
	}
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
