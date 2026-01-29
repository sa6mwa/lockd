# YCSB run baseline (lockd vs etcd)

Date: 2026-01-27

Environment:
- Dev stack via `devenv/docker-compose.yaml` (lockd + etcd)
- Workload: `workloada`
- Record count: 10000
- Operation count: 100000
- Threads: 8
- Target: 0 (no throttling)

Commands:
- `cd ycsb && make lockd-run PERF_LOG=../docs/performance/2026-01-27-ycsb-load/performance.log`
- `cd ycsb && make etcd-run PERF_LOG=../docs/performance/2026-01-27-ycsb-load/performance.log`

Lockd run results:
- Run finished: 1m0.943007775s
- TOTAL OPS: 1641.1
- READ OPS: 819.3, Avg: 0.772 ms, p50: 0.721 ms, p95: 1.345 ms, p99: 2.115 ms
- UPDATE OPS: 822.0, Avg: 8.184 ms, p50: 5.879 ms, p95: 26.527 ms, p99: 31.743 ms
- Measurement file: `/tmp/lockd-ycsb-measurement-3801400446.txt`

Etcd run results:
- Run finished: 15.355049633s
- TOTAL OPS: 6517.7
- READ OPS: 3263.0, Avg: 1.176 ms, p50: 1.018 ms, p95: 1.996 ms, p99: 6.607 ms
- UPDATE OPS: 3254.5, Avg: 1.254 ms, p50: 1.055 ms, p95: 1.933 ms, p99: 7.315 ms
- Measurement file: `/tmp/lockd-ycsb-measurement-3904718519.txt`

Notes:
- Full log appended to `performance.log` in this folder.
