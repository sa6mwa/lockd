# YCSB attachment overlay (lockd)

Date: 2026-01-27

Environment:
- Dev stack via `devenv/docker-compose.yaml`
- Workload: `workloada`
- Record count: 10000
- Operation count: 100000
- Threads: 8
- Target: 0 (no throttling)

Commands:
- `cd ycsb && make lockd-load-attach PERF_LOG=../docs/performance/2026-01-27-ycsb-load/performance.log`
- `cd ycsb && make lockd-run-attach PERF_LOG=../docs/performance/2026-01-27-ycsb-load/performance.log`

Load + attachments:
- Run finished: 9.520484495s
- INSERT OPS: 1053.8
- Avg latency: 7.500 ms
- p50: 6.851 ms
- p90: 9.399 ms
- p95: 10.103 ms
- p99: 31.103 ms
- p99.9: 51.647 ms
- p99.99: 68.671 ms
- Measurement file: `/tmp/lockd-ycsb-measurement-811072518.txt`

Run + attachments:
- Run finished: 1m0.907865179s
- TOTAL OPS: 1642.0
- READ OPS: 820.3, Avg: 0.795 ms, p50: 0.717 ms, p90: 1.142 ms, p95: 1.320 ms, p99: 2.091 ms, p99.9: 14.487 ms, p99.99: 24.703 ms
- UPDATE OPS: 821.8, Avg: 8.492 ms, p50: 5.891 ms, p90: 18.831 ms, p95: 26.975 ms, p99: 33.375 ms, p99.9: 76.415 ms, p99.99: 508.671 ms
- Measurement file: `/tmp/lockd-ycsb-measurement-2194574244.txt`

Notes:
- Full log appended to `performance.log` in this folder.
