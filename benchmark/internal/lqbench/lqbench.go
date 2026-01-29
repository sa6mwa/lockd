//go:build bench

package lqbench

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lockdclient "pkt.systems/lockd/client"
)

const (
	defaultMessages  = 200
	defaultProducers = 8
	defaultConsumers = 8
	defaultPrefetch  = 4
	defaultTimeout   = 90 * time.Second
)

var errBenchmarkComplete = errors.New("queue benchmark complete")

type Scenario struct {
	Name          string
	Producers     int
	Consumers     int
	TotalMessages int
	Prefetch      int
	BlockSeconds  int64
	UseSubscribe  bool
	Timeout       time.Duration
}

type Result struct {
	Produced           int64
	Consumed           int64
	AckLeaseMismatch   int64
	AckLeaseMissing    int64
	AckLeaseIDMismatch int64
	Duration           time.Duration
}

func Run(b *testing.B, clients []*lockdclient.Client, scenario Scenario, payload []byte) Result {
	b.Helper()

	if len(clients) == 0 {
		b.Fatalf("queue bench requires at least one client")
	}
	applyScenarioDefaults(&scenario)
	applyScenarioEnvOverrides(&scenario)

	ctx, cancel := context.WithTimeout(context.Background(), scenario.Timeout)
	defer cancel()

	queue := fmt.Sprintf("bench-%s-%d", slugify(scenario.Name), time.Now().UnixNano())
	targetMessages := int64(scenario.TotalMessages)

	var produced int64
	var consumed int64
	var ackLeaseMismatch atomic.Int64
	var ackLeaseMissing atomic.Int64
	var ackLeaseIDMismatch atomic.Int64

	var errOnce sync.Once
	var benchErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		errOnce.Do(func() {
			benchErr = err
			cancel()
		})
	}

	start := time.Now()
	b.ResetTimer()

	var wg sync.WaitGroup
	var produceCursor int64

	if scenario.Producers > 0 {
		for i := 0; i < scenario.Producers; i++ {
			wg.Add(1)
			client := clients[i%len(clients)]
			go func(cli *lockdclient.Client) {
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
						callCtx, callCancel := context.WithTimeout(ctx, 30*time.Second)
						_, err := cli.EnqueueBytes(callCtx, queue, payload, lockdclient.EnqueueOptions{})
						callCancel()
						if err != nil {
							if retryQueueError(err) {
								continue
							}
							recordErr(fmt.Errorf("enqueue: %w", err))
							return
						}
						atomic.AddInt64(&produced, 1)
						break
					}
				}
			}(client)
		}
	}

	if scenario.Consumers > 0 {
		for i := 0; i < scenario.Consumers; i++ {
			wg.Add(1)
			client := clients[i%len(clients)]
			go func(cli *lockdclient.Client) {
				defer wg.Done()
				batchSize := scenario.Prefetch
				if batchSize <= 0 {
					batchSize = 1
				}
				blockSeconds := scenario.BlockSeconds
				if blockSeconds == 0 {
					blockSeconds = lockdclient.BlockNoWait
				}
				owner := fmt.Sprintf("bench-consumer-%d", i)

				if scenario.UseSubscribe {
					for {
						if ctx.Err() != nil {
							recordErr(ctx.Err())
							return
						}
						if atomic.LoadInt64(&consumed) >= targetMessages {
							return
						}
						consumerCtx, consumerCancel := context.WithCancel(ctx)
						err := cli.Subscribe(consumerCtx, queue, lockdclient.SubscribeOptions{
							Owner:        owner,
							BlockSeconds: blockSeconds,
							Prefetch:     batchSize,
						}, func(_ context.Context, msg *lockdclient.QueueMessage) error {
							if msg == nil {
								return nil
							}
							ackCtx, ackCancel := context.WithTimeout(ctx, 30*time.Second)
							ackErr := msg.Ack(ackCtx)
							ackCancel()
							if ackErr != nil {
								var apiErr *lockdclient.APIError
								if errors.As(ackErr, &apiErr) {
									switch apiErr.Response.ErrorCode {
									case "queue_message_lease_mismatch":
										ackLeaseMismatch.Add(1)
										detail := strings.ToLower(apiErr.Response.Detail)
										if strings.Contains(detail, "missing") {
											ackLeaseMissing.Add(1)
										} else {
											ackLeaseIDMismatch.Add(1)
										}
										total := atomic.AddInt64(&consumed, 1)
										if total >= targetMessages {
											consumerCancel()
											cancel()
											return errBenchmarkComplete
										}
										return nil
									case "cas_mismatch", "not_found":
										total := atomic.AddInt64(&consumed, 1)
										if total >= targetMessages {
											consumerCancel()
											cancel()
											return errBenchmarkComplete
										}
										return nil
									}
								}
								if retryQueueError(ackErr) {
									return nil
								}
								return fmt.Errorf("ack: %w", ackErr)
							}
							total := atomic.AddInt64(&consumed, 1)
							if total >= targetMessages {
								consumerCancel()
								cancel()
								return errBenchmarkComplete
							}
							return nil
						})
						consumerCancel()
						if err == nil {
							if atomic.LoadInt64(&consumed) >= targetMessages {
								return
							}
							continue
						}
						if errors.Is(err, errBenchmarkComplete) {
							return
						}
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
							return
						}
						if retryQueueError(err) {
							time.Sleep(10 * time.Millisecond)
							continue
						}
						recordErr(fmt.Errorf("subscribe: %w", err))
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
					if atomic.LoadInt64(&consumed) >= targetMessages {
						return
					}
					callCtx, callCancel := context.WithTimeout(ctx, 30*time.Second)
					msgs, err := cli.DequeueBatch(callCtx, queue, opts)
					callCancel()
					if err != nil {
						var apiErr *lockdclient.APIError
						if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
							continue
						}
						if retryQueueError(err) {
							continue
						}
						recordErr(fmt.Errorf("dequeue: %w", err))
						return
					}
					if len(msgs) == 0 {
						continue
					}
					for _, msg := range msgs {
						ackCtx, ackCancel := context.WithTimeout(ctx, 30*time.Second)
						ackErr := msg.Ack(ackCtx)
						ackCancel()
						if ackErr != nil {
							var apiErr *lockdclient.APIError
							if errors.As(ackErr, &apiErr) && apiErr.Response.ErrorCode == "queue_message_lease_mismatch" {
								ackLeaseMismatch.Add(1)
								detail := strings.ToLower(apiErr.Response.Detail)
								if strings.Contains(detail, "missing") {
									ackLeaseMissing.Add(1)
								} else {
									ackLeaseIDMismatch.Add(1)
								}
								total := atomic.AddInt64(&consumed, 1)
								if total >= targetMessages {
									return
								}
								continue
							}
							if retryQueueError(ackErr) {
								continue
							}
							recordErr(fmt.Errorf("ack: %w", ackErr))
							return
						}
						total := atomic.AddInt64(&consumed, 1)
						if total >= targetMessages {
							return
						}
					}
				}
			}(client)
		}
	}

	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	if benchErr != nil {
		b.Fatalf("queue benchmark failed: %v (produced=%d consumed=%d target=%d)", benchErr, produced, consumed, targetMessages)
	}

	if ackLeaseMismatch.Load() > 0 && os.Getenv("LOCKD_LQ_BENCH_ALLOW_MISMATCH") != "1" {
		b.Fatalf("ack lease mismatch detected: %d (missing=%d mismatch=%d)", ackLeaseMismatch.Load(), ackLeaseMissing.Load(), ackLeaseIDMismatch.Load())
	}

	reportMetrics(b, elapsed, produced, consumed, targetMessages, scenario, ackLeaseMismatch.Load(), ackLeaseMissing.Load(), ackLeaseIDMismatch.Load())

	return Result{
		Produced:           produced,
		Consumed:           consumed,
		AckLeaseMismatch:   ackLeaseMismatch.Load(),
		AckLeaseMissing:    ackLeaseMissing.Load(),
		AckLeaseIDMismatch: ackLeaseIDMismatch.Load(),
		Duration:           elapsed,
	}
}

func reportMetrics(b *testing.B, elapsed time.Duration, produced, consumed, target int64, scenario Scenario, mismatch, missing, idMismatch int64) {
	if scenario.UseSubscribe || scenario.Consumers > 0 {
		b.ReportMetric(float64(consumed), "consumed_total")
	}
	if scenario.Producers > 0 {
		b.ReportMetric(float64(produced), "produced_total")
	}
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1e-9
	}
	if scenario.Producers > 0 {
		b.ReportMetric(float64(produced)/secs, "enqueue/s")
	}
	if scenario.UseSubscribe || scenario.Consumers > 0 {
		if consumed == 0 && target > 0 {
			consumed = target
		}
		b.ReportMetric(float64(consumed)/secs, "dequeue/s")
	}
	if mismatch > 0 {
		b.ReportMetric(float64(mismatch), "ack_lease_mismatch_total")
	}
	if missing > 0 {
		b.ReportMetric(float64(missing), "ack_lease_missing_total")
	}
	if idMismatch > 0 {
		b.ReportMetric(float64(idMismatch), "ack_lease_id_mismatch_total")
	}
}

func applyScenarioDefaults(s *Scenario) {
	if s.Name == "" {
		s.Name = "queue"
	}
	if s.TotalMessages <= 0 {
		s.TotalMessages = defaultMessages
	}
	if s.Producers <= 0 {
		s.Producers = defaultProducers
	}
	if s.Consumers <= 0 {
		s.Consumers = defaultConsumers
	}
	if s.Prefetch <= 0 {
		s.Prefetch = defaultPrefetch
	}
	if s.Timeout <= 0 {
		s.Timeout = defaultTimeout
	}
	if s.BlockSeconds < 0 && s.BlockSeconds != lockdclient.BlockNoWait {
		s.BlockSeconds = lockdclient.BlockNoWait
	}
	if s.BlockSeconds == 0 {
		s.BlockSeconds = lockdclient.BlockNoWait
	}
}

func applyScenarioEnvOverrides(s *Scenario) {
	if v := readEnvInt("LOCKD_LQ_BENCH_MESSAGES"); v > 0 {
		s.TotalMessages = v
	}
	if v := readEnvInt("LOCKD_LQ_BENCH_PRODUCERS"); v > 0 {
		s.Producers = v
	}
	if v := readEnvInt("LOCKD_LQ_BENCH_CONSUMERS"); v > 0 {
		s.Consumers = v
	}
	if v := readEnvInt("LOCKD_LQ_BENCH_PREFETCH"); v > 0 {
		s.Prefetch = v
	}
	if v := readEnvInt("LOCKD_LQ_BENCH_TIMEOUT_SECONDS"); v > 0 {
		s.Timeout = time.Duration(v) * time.Second
	}
}

func readEnvInt(name string) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return val
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
