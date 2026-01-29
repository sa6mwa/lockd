# YCSB load baseline (lockd vs etcd)

Date: 2026-01-27

Environment:
- Dev stack via `devenv/docker-compose.yaml` (lockd + etcd)
- Workload: `workloada`
- Record count: 10000
- Operation count: 100000 (ignored for load)
- Threads: 8
- Target: 0 (no throttling)

Commands:
- `cd ycsb && make lockd-load`
- `cd ycsb && make etcd-load`

Lockd load results (see `performance.log` for full context):
- Run finished: 24.598808437s
- INSERT OPS: 408.9
- Avg latency: 19.508 ms
- p50: 16.575 ms
- p90: 29.455 ms
- p95: 31.007 ms
- p99: 43.583 ms
- p99.9: 112.895 ms
- p99.99: 147.839 ms
- Performance log entry: `2026-01-27T16:06:37Z`

Etcd load results (see `performance.log` for full context):
- Run finished: 4.149242956s
- INSERT OPS: 2411.9
- Avg latency: 3.281 ms
- p50: 2.841 ms
- p90: 4.073 ms
- p95: 5.783 ms
- p99: 22.303 ms
- p99.9: 51.487 ms
- p99.99: 65.215 ms
- Performance log entry: `2026-01-27T16:06:45Z`
