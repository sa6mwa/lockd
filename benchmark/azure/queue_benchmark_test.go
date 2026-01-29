//go:build bench && azure

package azurebench

import (
	"bytes"
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/benchmark/internal/benchenv"
	"pkt.systems/lockd/benchmark/internal/lqbench"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/pslog"
)

const azureQueuePayloadSize = 256

func BenchmarkAzureQueueAckLease(b *testing.B) {
	cfg := benchenv.LoadAzureConfig(b)
	applyQueueBenchConfig(&cfg)
	benchenv.EnsureAzureStoreReady(b, cfg)
	b.Cleanup(func() {
		benchenv.CleanupBenchmarkPrefix(b, cfg)
	})

	client := startAzureBenchServer(b, cfg)
	probeQueueWritable(b, client)
	payload := bytes.Repeat([]byte("x"), azureQueuePayloadSize)
	scenario := lqbench.Scenario{
		Name:          "azure-queue",
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

func startAzureBenchServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
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
