//go:build bench && mem && lq

package memlqbench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/logport"
)

const (
	memBenchMessageSize           = 1024
	memBenchDefaultMessagesPerCap = 20
	memBenchTotalCap              = 2000
	memBenchMinMessages           = 200
)

var errBenchmarkComplete = errors.New("mem queue benchmark complete")

type queueBenchmarkScenario struct {
	name           string
	servers        int
	producers      int
	consumers      int
	totalMessages  int
	prefill        bool
	measureEnqueue bool
	measureDequeue bool
	prefetch       int
	blockSeconds   int64
	useSubscribe   bool
}

type benchMetrics struct {
	dequeueNanos atomic.Int64
	ackNanos     atomic.Int64
	dequeueCount atomic.Int64
	ackCount     atomic.Int64
	waitingCount atomic.Int64
	batchCalls   atomic.Int64
	batchTotal   atomic.Int64
	maxGapNanos  atomic.Int64
}

var debugDeliveries atomic.Int64

func BenchmarkMemQueueThroughput(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), memBenchMessageSize)
	scenarios := []queueBenchmarkScenario{
		{
			name:           "single_server_prefetch1_100p_100c",
			servers:        1,
			producers:      100,
			consumers:      100,
			measureEnqueue: true,
			measureDequeue: true,
			prefetch:       1,
			blockSeconds:   lockdclient.BlockNoWait,
			useSubscribe:   true,
		},
		{
			name:           "single_server_prefetch4_100p_100c",
			servers:        1,
			producers:      100,
			consumers:      100,
			measureEnqueue: true,
			measureDequeue: true,
			prefetch:       4,
			blockSeconds:   lockdclient.BlockNoWait,
			useSubscribe:   true,
		},
		{
			name:           "single_server_subscribe_100p_1c",
			servers:        1,
			producers:      100,
			consumers:      1,
			totalMessages:  200,
			prefill:        true,
			measureEnqueue: false,
			measureDequeue: true,
			prefetch:       16,
			blockSeconds:   30,
			useSubscribe:   true,
		},
		{
			name:           "single_server_dequeue_guard",
			servers:        1,
			producers:      1,
			consumers:      1,
			measureEnqueue: true,
			measureDequeue: true,
			prefetch:       8,
			blockSeconds:   lockdclient.BlockNoWait,
		},
		{
			name:           "double_server_prefetch4_100p_100c",
			servers:        2,
			producers:      100,
			consumers:      100,
			measureEnqueue: true,
			measureDequeue: true,
			prefetch:       4,
			blockSeconds:   lockdclient.BlockNoWait,
			useSubscribe:   true,
		},
	}

	for _, scenario := range scenarios {
		scenario := scenario
		b.Run(scenario.name, func(b *testing.B) {
			runMemQueueBenchmark(b, scenario, payload)
		})
	}
}

func runMemQueueBenchmark(b *testing.B, scenario queueBenchmarkScenario, payload []byte) {
	b.Helper()
	debugDeliveries.Store(0)

	if override := os.Getenv("MEM_LQ_BENCH_PRODUCERS"); override != "" {
		if v, err := strconv.Atoi(override); err == nil && v >= 0 {
			scenario.producers = v
		}
	}
	if override := os.Getenv("MEM_LQ_BENCH_CONSUMERS"); override != "" {
		if v, err := strconv.Atoi(override); err == nil && v >= 0 {
			scenario.consumers = v
		}
	}

	var stopProfile func()
	if profilePath := os.Getenv("MEM_LQ_BENCH_CPUPROFILE"); profilePath != "" {
		file, err := os.Create(profilePath)
		if err != nil {
			b.Fatalf("create cpu profile: %v", err)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			_ = file.Close()
			b.Fatalf("start cpu profile: %v", err)
		}
		stopProfile = func() {
			pprof.StopCPUProfile()
			_ = file.Close()
		}
	}
	if stopProfile != nil {
		defer stopProfile()
	}

	totalMessages := scenario.totalMessages
	if totalMessages == 0 {
		if scenario.producers > 0 {
			totalMessages = scenario.producers * memBenchDefaultMessagesPerCap
		}
		if totalMessages == 0 && scenario.consumers > 0 {
			totalMessages = scenario.consumers * memBenchDefaultMessagesPerCap
		}
		if totalMessages == 0 {
			totalMessages = memBenchMinMessages
		}
	}
	if totalMessages < memBenchMinMessages {
		totalMessages = memBenchMinMessages
	}
	if totalMessages > memBenchTotalCap {
		totalMessages = memBenchTotalCap
	}
	if override := os.Getenv("MEM_LQ_BENCH_MESSAGES"); override != "" {
		if v, err := strconv.Atoi(override); err == nil && v > 0 {
			totalMessages = v
		}
	}

	backend := memorybackend.New()
	logLevel := logport.NoLevel
	if os.Getenv("MEM_LQ_BENCH_TRACE") == "1" {
		logLevel = logport.TraceLevel
	} else if os.Getenv("MEM_LQ_BENCH_DEBUG") == "1" {
		logLevel = logport.DebugLevel
	}
	servers := make([]*lockd.TestServer, 0, scenario.servers)
	for i := 0; i < scenario.servers; i++ {
		cfg := buildMemQueueConfig(b)
		var opts []lockd.TestServerOption
		var logger logport.ForLogging
		if logLevel == logport.NoLevel {
			logger = logport.NoopLogger()
		} else {
			logger = lockd.NewTestingLogger(b, logLevel)
		}
		opts = append(opts,
			lockd.WithTestConfig(cfg),
			lockd.WithTestLogger(logger),
			lockd.WithTestClientOptions(
				lockdclient.WithMTLS(false),
				lockdclient.WithHTTPTimeout(5*time.Minute),
				lockdclient.WithKeepAliveTimeout(5*time.Minute),
				lockdclient.WithLogger(logger),
			),
		)
		if scenario.servers > 1 {
			opts = append(opts, lockd.WithTestBackend(backend))
		} else if i == 0 && scenario.servers == 1 {
			opts = append(opts, lockd.WithTestBackend(backend))
		}
		ts := lockd.StartTestServer(b, opts...)
		b.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = ts.Stop(ctx)
		})
		servers = append(servers, ts)
	}

	clients := make([]*lockdclient.Client, len(servers))
	for i, srv := range servers {
		clients[i] = srv.Client
	}

	extraMetrics := os.Getenv("MEM_LQ_BENCH_EXTRA") == "1"
	benchDebug := os.Getenv("MEM_LQ_BENCH_DEBUG") == "1"

	queue := fmt.Sprintf("bench-%s-%d", slugify(scenario.name), time.Now().UnixNano())
	if extraMetrics {
		b.SetBytes(memBenchMessageSize)
	} else {
		b.SetBytes(0)
	}

	totalProduced := int64(0)
	totalConsumed := int64(0)

	if scenario.prefill {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if _, err := prefillQueue(ctx, clients[0], queue, totalMessages, payload); err != nil {
			b.Fatalf("prefill queue: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	prefetchOverride := 0
	if override := os.Getenv("MEM_LQ_BENCH_PREFETCH"); override != "" {
		if v, err := strconv.Atoi(override); err == nil && v > 0 {
			prefetchOverride = v
		}
	}

	metrics := new(benchMetrics)

	var errOnce sync.Once
	var errRecorded atomic.Bool
	var benchErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		if !errRecorded.Load() && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return
		}
		errOnce.Do(func() {
			errRecorded.Store(true)
			if benchDebug {
				var apiErr *lockdclient.APIError
				if errors.As(err, &apiErr) && apiErr != nil && apiErr.Response.ErrorCode != "" {
					fmt.Fprintf(os.Stderr, "[bench] api_error status=%d code=%s detail=%q retry_after=%d\n",
						apiErr.Status, apiErr.Response.ErrorCode, apiErr.Response.Detail, apiErr.Response.RetryAfterSeconds)
				} else {
					fmt.Fprintf(os.Stderr, "[bench] error: %v\n", err)
				}
			}
			benchErr = err
			cancel()
		})
	}

	b.ResetTimer()
	start := time.Now()

	var wg sync.WaitGroup

	var produceCursor int64
	targetMessages := int64(totalMessages)

	if scenario.producers > 0 && !scenario.prefill {
		for i := 0; i < scenario.producers; i++ {
			wg.Add(1)
			clientIdx := i % len(clients)
			go func(worker int, cli *lockdclient.Client) {
				defer wg.Done()
				for {
					if ctx.Err() != nil {
						recordErr(ctx.Err())
						return
					}
					next := atomic.AddInt64(&produceCursor, 1)
					if next > targetMessages {
						return
					}
					for {
						if ctx.Err() != nil {
							recordErr(ctx.Err())
							return
						}
						callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
						_, err := cli.EnqueueBytes(callCtx, queue, payload, lockdclient.EnqueueOptions{})
						cancel()
						if err != nil {
							if retryQueueError(err) {
								continue
							}
							recordErr(fmt.Errorf("enqueue (producer %d): %w", worker, err))
							return
						}
						atomic.AddInt64(&totalProduced, 1)
						break
					}
				}
			}(i, clients[clientIdx])
		}
	}

	warmupMessages := 20
	if override := os.Getenv("MEM_LQ_BENCH_WARMUP"); override != "" {
		if v, err := strconv.Atoi(override); err == nil && v >= 0 {
			warmupMessages = v
		}
	}

	if scenario.consumers > 0 {
		for i := 0; i < scenario.consumers; i++ {
			wg.Add(1)
			clientIdx := i % len(clients)
			go func(worker int, cli *lockdclient.Client) {
				defer wg.Done()
				batchSize := scenario.prefetch
				if prefetchOverride > 0 {
					batchSize = prefetchOverride
				}
				if batchSize <= 0 {
					batchSize = 1
				}
				blockSeconds := scenario.blockSeconds
				if blockSeconds == 0 {
					blockSeconds = lockdclient.BlockNoWait
				}
				owner := fmt.Sprintf("bench-consumer-%d", worker)

				if scenario.useSubscribe {
					debug := benchDebug
					if debug {
						fmt.Fprintf(os.Stderr, "warmup=%d\n", warmupMessages)
					}
					traceGaps := debug || os.Getenv("MEM_LQ_BENCH_TRACE_GAPS") == "1"
					for {
						if ctx.Err() != nil {
							recordErr(ctx.Err())
							return
						}
						if atomic.LoadInt64(&totalConsumed) >= targetMessages {
							return
						}
						consumerCtx, consumerCancel := context.WithCancel(ctx)
						lastDelivery := time.Now()
						deliveriesSeen := 0
						err := cli.Subscribe(consumerCtx, queue, lockdclient.SubscribeOptions{
							Owner:        owner,
							BlockSeconds: blockSeconds,
							Prefetch:     batchSize,
						}, func(msgCtx context.Context, msg *lockdclient.QueueMessage) error {
							if debug {
								fmt.Fprintf(os.Stderr, "[%s] handler.begin mid=%s lease=%s\n",
									time.Now().Format(time.RFC3339Nano),
									msg.MessageID(),
									msg.LeaseID(),
								)
							}
							if ctx.Err() != nil {
								return ctx.Err()
							}
							now := time.Now()
							gap := now.Sub(lastDelivery)
							deliveriesSeen++
							if deliveriesSeen > warmupMessages {
								metrics.dequeueNanos.Add(gap.Nanoseconds())
								for {
									prev := metrics.maxGapNanos.Load()
									if gap.Nanoseconds() <= prev || metrics.maxGapNanos.CompareAndSwap(prev, gap.Nanoseconds()) {
										break
									}
								}
								metrics.dequeueCount.Add(1)
							}
							lastDelivery = now
							if traceGaps && deliveriesSeen > warmupMessages && gap > 10*time.Millisecond {
								count := debugDeliveries.Add(1)
								fmt.Fprintf(os.Stderr, "delivery[%d] gap=%s (seen=%d warmup=%d)\n", count, gap, deliveriesSeen, warmupMessages)
							}
							if debug {
								fmt.Fprintf(os.Stderr, "[%s] ack.start mid=%s lease=%s fencing=%d\n",
									time.Now().Format(time.RFC3339Nano),
									msg.MessageID(),
									msg.LeaseID(),
									msg.FencingToken(),
								)
							}
							ackStart := time.Now()
							ackCtx, ackCancel := context.WithTimeout(ctx, 15*time.Second)
							ackErr := msg.Ack(ackCtx)
							ackCancel()
							if ackErr != nil {
								var apiErr *lockdclient.APIError
								if errors.As(ackErr, &apiErr) && apiErr.Response.ErrorCode == "lease_required" {
									if debug {
										fmt.Fprintf(os.Stderr, "[%s] ack.stale mid=%s lease=%s err=%v\n",
											time.Now().Format(time.RFC3339Nano),
											msg.MessageID(),
											msg.LeaseID(),
											ackErr,
										)
									}
									total := atomic.AddInt64(&totalConsumed, 1)
									if total >= targetMessages {
										consumerCancel()
										cancel()
										return errBenchmarkComplete
									}
									return nil
								}
								if errors.As(ackErr, &apiErr) &&
									(apiErr.Response.ErrorCode == "not_found" || apiErr.Response.ErrorCode == "cas_mismatch") {
									if debug {
										fmt.Fprintf(os.Stderr, "[%s] ack.idempotent mid=%s lease=%s err=%v\n",
											time.Now().Format(time.RFC3339Nano),
											msg.MessageID(),
											msg.LeaseID(),
											ackErr,
										)
									}
									total := atomic.AddInt64(&totalConsumed, 1)
									if total >= targetMessages {
										consumerCancel()
										cancel()
										return errBenchmarkComplete
									}
									return nil
								}
								if retryQueueError(ackErr) {
									if debug {
										fmt.Fprintf(os.Stderr, "[%s] ack.retry mid=%s err=%v\n",
											time.Now().Format(time.RFC3339Nano),
											msg.MessageID(),
											ackErr,
										)
									}
									return nil
								}
								return fmt.Errorf("ack (consumer %d): %w", worker, ackErr)
							}
							if debug {
								fmt.Fprintf(os.Stderr, "[%s] ack.success mid=%s lease=%s\n",
									time.Now().Format(time.RFC3339Nano),
									msg.MessageID(),
									msg.LeaseID(),
								)
							}
							if deliveriesSeen > warmupMessages {
								metrics.ackNanos.Add(time.Since(ackStart).Nanoseconds())
								metrics.ackCount.Add(1)
							}
							metrics.batchCalls.Add(1)
							metrics.batchTotal.Add(1)
							total := atomic.AddInt64(&totalConsumed, 1)
							if total >= targetMessages {
								consumerCancel()
								cancel()
								return errBenchmarkComplete
							}
							return nil
						})
						consumerCancel()
						if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errBenchmarkComplete) {
							return
						}
						if retryQueueError(err) {
							time.Sleep(10 * time.Millisecond)
							continue
						}
						recordErr(fmt.Errorf("subscribe (consumer %d): %w", worker, err))
						return
					}
				}

				opts := lockdclient.DequeueOptions{
					Owner:        owner,
					BlockSeconds: blockSeconds,
					PageSize:     batchSize,
				}
				for {
					if ctx.Err() != nil {
						recordErr(ctx.Err())
						return
					}
					if atomic.LoadInt64(&totalConsumed) >= targetMessages {
						return
					}
					deqStart := time.Now()
					callCtx, cancelCall := context.WithTimeout(ctx, 15*time.Second)
					msgs, err := cli.DequeueBatch(callCtx, queue, opts)
					cancelCall()
					if err != nil {
						var apiErr *lockdclient.APIError
						if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
							metrics.waitingCount.Add(1)
							continue
						}
						if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
							continue
						}
						if retryQueueError(err) {
							continue
						}
						recordErr(fmt.Errorf("dequeue (consumer %d): %w", worker, err))
						return
					}
					if len(msgs) == 0 {
						continue
					}
					metrics.batchCalls.Add(1)
					metrics.batchTotal.Add(int64(len(msgs)))
					metrics.dequeueNanos.Add(time.Since(deqStart).Nanoseconds())
					metrics.dequeueCount.Add(int64(len(msgs)))
					for _, msg := range msgs {
						ackStart := time.Now()
						ackCtx, ackCancel := context.WithTimeout(ctx, 15*time.Second)
						ackErr := msg.Ack(ackCtx)
						ackCancel()
						if ackErr != nil {
							if retryQueueError(ackErr) {
								continue
							}
							recordErr(fmt.Errorf("ack (consumer %d): %w", worker, ackErr))
							return
						}
						metrics.ackNanos.Add(time.Since(ackStart).Nanoseconds())
						metrics.ackCount.Add(1)
						total := atomic.AddInt64(&totalConsumed, 1)
						if total >= targetMessages {
							return
						}
					}
				}
			}(i, clients[clientIdx])
		}
	}

	wg.Wait()

	elapsed := time.Since(start)
	b.StopTimer()

	if extraMetrics {
		if count := metrics.dequeueCount.Load(); count > 0 {
			avg := float64(metrics.dequeueNanos.Load()) / float64(count) / 1e6
			b.ReportMetric(avg, "dequeue_ms/op")
		}
	}
	if count := metrics.ackCount.Load(); count > 0 {
		if extraMetrics {
			avg := float64(metrics.ackNanos.Load()) / float64(count) / 1e6
			b.ReportMetric(avg, "ack_ms/op")
		}
	}
	if waiting := metrics.waitingCount.Load(); waiting > 0 {
		if extraMetrics {
			b.ReportMetric(float64(waiting), "waiting_responses")
		}
	}
	if calls := metrics.batchCalls.Load(); calls > 0 {
		if extraMetrics {
			avg := float64(metrics.batchTotal.Load()) / float64(calls)
			b.ReportMetric(avg, "messages_per_batch")
		}
	}
	if maxGap := metrics.maxGapNanos.Load(); maxGap > 0 {
		if extraMetrics {
			b.ReportMetric(float64(maxGap)/1e6, "dequeue_gap_max_ms")
		}
	}

	if benchErr != nil {
		if benchDebug {
			fmt.Fprintf(os.Stderr, "benchmark error: %T %v\n", benchErr, benchErr)
		}
		b.Fatalf("benchmark failed: %v (produced=%d consumed=%d target=%d)", benchErr, atomic.LoadInt64(&totalProduced), atomic.LoadInt64(&totalConsumed), targetMessages)
	}

	consumedTotal := atomic.LoadInt64(&totalConsumed)
	b.ReportMetric(float64(consumedTotal), "consumed_total")

	producedTotal := atomic.LoadInt64(&totalProduced)
	if scenario.prefill {
		producedTotal = int64(totalMessages)
	}
	b.ReportMetric(float64(producedTotal), "produced_total")

	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1e-9
	}

	if scenario.measureEnqueue {
		produced := atomic.LoadInt64(&totalProduced)
		if scenario.prefill {
			produced = int64(totalMessages)
		}
		b.ReportMetric(float64(produced)/secs, "enqueue/s")
	}
	if scenario.measureDequeue {
		consumed := atomic.LoadInt64(&totalConsumed)
		if scenario.prefill && consumed == 0 {
			consumed = int64(totalMessages)
		}
		b.ReportMetric(float64(consumed)/secs, "dequeue/s")
	}
}

func buildMemQueueConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	cfg := lockd.Config{
		Store:                      "mem://",
		MTLS:                       false,
		ListenProto:                "tcp",
		Listen:                     "127.0.0.1:0",
		DefaultTTL:                 30 * time.Second,
		MaxTTL:                     2 * time.Minute,
		AcquireBlock:               5 * time.Second,
		SweeperInterval:            2 * time.Second,
		QueuePollInterval:          500 * time.Microsecond,
		QueuePollJitter:            500 * time.Microsecond,
		QueueResilientPollInterval: 5 * time.Millisecond,
	}
	cfg.MemQueueWatch = true
	cfg.MemQueueWatchSet = true
	cfg.QRFEnabled = false
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config validation failed: %v", err)
	}
	return cfg
}

func prefillQueue(ctx context.Context, cli *lockdclient.Client, queue string, count int, payload []byte) (int, error) {
	for i := 0; i < count; i++ {
		if _, err := cli.EnqueueBytes(ctx, queue, payload, lockdclient.EnqueueOptions{}); err != nil {
			return i, err
		}
	}
	if os.Getenv("MEM_LQ_BENCH_DEBUG") == "1" {
		fmt.Fprintf(os.Stderr, "[%s] prefill.complete queue=%s count=%d\n", time.Now().Format(time.RFC3339Nano), queue, count)
	}
	return count, nil
}

func slugify(name string) string {
	lower := strings.ToLower(name)
	replacer := strings.NewReplacer(" ", "-", "_", "-", ".", "-", "/", "-")
	return replacer.Replace(lower)
}

func retryQueueError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var apiErr *lockdclient.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.Response.ErrorCode {
		case "waiting":
			time.Sleep(200 * time.Microsecond)
			return true
		case "lease_required":
			time.Sleep(10 * time.Millisecond)
			return true
		case "throttled":
			if apiErr.Response.RetryAfterSeconds > 0 {
				time.Sleep(time.Duration(apiErr.Response.RetryAfterSeconds) * time.Second)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
			return true
		}
		if apiErr.Response.RetryAfterSeconds > 0 {
			time.Sleep(time.Duration(apiErr.Response.RetryAfterSeconds) * time.Second)
			return true
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Temporary() {
		time.Sleep(5 * time.Millisecond)
		return true
	}
	if strings.Contains(err.Error(), "EOF") {
		time.Sleep(250 * time.Microsecond)
		return true
	}
	return false
}
