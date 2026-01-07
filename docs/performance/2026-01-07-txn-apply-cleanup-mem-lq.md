# 2026-01-07 mem lq benchmark (txn apply cleanup)

Context
- Change: txn apply marks participants applied and deletes txn records once all participants are applied.
- Command: `./run-benchmark-suites.sh mem`
- Env: `LOCKD_TEST_STORAGE_ENCRYPTION=1`, `LOCKD_TEST_WITH_MTLS=1`, `BENCHTIME=1x`, `MEM_LQ_BENCH_PREFETCH_DOUBLE=8`, `OTEL_SDK_DISABLED=1`
- Host: linux/amd64, 13th Gen Intel(R) Core(TM) i7-1355U

Results
- BenchmarkMemQueueThroughput/single_server_prefetch1_100p_100c
  - ns/op: 1234704158
  - dequeue/s: 1621
  - enqueue/s: 1620
  - consumed_total: 2002
  - produced_total: 2000
  - ack_lease_mismatch_total: 39.00
  - B/op: 525509888
  - allocs/op: 2640837
- BenchmarkMemQueueThroughput/single_server_prefetch4_100p_100c
  - ns/op: 918880976
  - dequeue/s: 2179
  - enqueue/s: 2177
  - consumed_total: 2002
  - produced_total: 2000
  - B/op: 486204512
  - allocs/op: 3065333
- BenchmarkMemQueueThroughput/single_server_subscribe_100p_1c
  - ns/op: 107833560
  - dequeue/s: 1855
  - consumed_total: 200.0
  - produced_total: 200.0
  - B/op: 39639792
  - allocs/op: 233223
- BenchmarkMemQueueThroughput/single_server_dequeue_guard
  - ns/op: 178616225
  - dequeue/s: 1120
  - enqueue/s: 1120
  - consumed_total: 200.0
  - produced_total: 200.0
  - B/op: 41865616
  - allocs/op: 270387
- BenchmarkMemQueueThroughput/double_server_prefetch4_100p_100c
  - ns/op: 568250530
  - dequeue/s: 3523
  - enqueue/s: 3520
  - consumed_total: 2002
  - produced_total: 2000
  - ack_lease_mismatch_total: 3.000
  - B/op: 381996552
  - allocs/op: 2608900
