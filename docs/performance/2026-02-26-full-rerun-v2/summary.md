# YCSB Perf Summary

- Perf log: `performance.log`
- Series: `2026-02-26-full-rerun-v2`
- Baseline series: `2026-01-27-baseline`

| Backend | Phase | Scenario | Workload | Query Engine | Query Return | Timestamp | Run ID | TOTAL ops/s | READ ops/s | UPDATE ops/s | INSERT ops/s | SCAN ops/s | Baseline TOTAL | Delta vs Baseline |
| --- | --- | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| etcd | load | workloada | core | n/a | n/a | 2026-02-26T00:13:33Z | 20260226-r2 | 10174.3 | n/a | n/a | 10175.9 | n/a | n/a | n/a |
| etcd | run | workloada | core | n/a | n/a | 2026-02-26T00:13:58Z | 20260226-r2 | 13897.1 | 6955.2 | 6945.4 | n/a | n/a | n/a | n/a |
| etcd | run | workloadb | core | n/a | n/a | 2026-02-26T00:14:20Z | 20260226-r2 | 18233.3 | 17317.0 | 917.4 | n/a | n/a | n/a | n/a |
| etcd | run | workloadc | core | n/a | n/a | 2026-02-26T00:14:37Z | 20260226-r2 | 31252.3 | 31250.8 | n/a | n/a | n/a | n/a | n/a |
| etcd | run | workloadd | core | n/a | n/a | 2026-02-26T00:14:58Z | 20260226-r2 | 19917.1 | 18905.7 | n/a | 1012.4 | n/a | n/a | n/a |
| lockd | load | workloada-attach | core | index | documents | 2026-02-26T00:11:28Z | 20260226-r2 | 17543.0 | n/a | n/a | 17543.0 | n/a | n/a | n/a |
| lockd | load | workloada-txn | core | index | documents | 2026-02-26T00:12:01Z | 20260226-r2 | 16927.2 | n/a | n/a | 16927.2 | n/a | n/a | n/a |
| lockd | load | workloada | core | index | documents | 2026-02-26T00:10:55Z | 20260226-r2 | 17218.6 | n/a | n/a | 17218.6 | n/a | n/a | n/a |
| lockd | load | workloade | core | index | documents | 2026-02-26T00:15:10Z | 20260226-r2 | 16331.2 | n/a | n/a | 16331.2 | n/a | n/a | n/a |
| lockd | run | workloada-attach | core | index | documents | 2026-02-26T00:11:49Z | 20260226-r2 | 20318.4 | 10168.3 | 10150.1 | n/a | n/a | n/a | n/a |
| lockd | run | workloada-txn | core | index | documents | 2026-02-26T00:12:22Z | 20260226-r2 | 20098.0 | 10076.8 | 10021.2 | n/a | n/a | n/a | n/a |
| lockd | run | workloada | core | index | documents | 2026-02-26T00:11:16Z | 20260226-r2 | 21017.2 | 10543.4 | 10473.8 | n/a | n/a | n/a | n/a |
| lockd | run | workloadb | core | index | documents | 2026-02-26T00:12:41Z | 20260226-r2 | 23942.3 | 22748.8 | 1193.5 | n/a | n/a | n/a | n/a |
| lockd | run | workloadc | core | index | documents | 2026-02-26T00:13:00Z | 20260226-r2 | 24720.6 | 24720.6 | n/a | n/a | n/a | n/a | n/a |
| lockd | run | workloadd | core | index | documents | 2026-02-26T00:13:19Z | 20260226-r2 | 23933.5 | 22749.8 | n/a | 1183.7 | n/a | n/a | n/a |
| lockd | run | workloade-query-index | core | index | documents | 2026-02-26T00:15:32Z | 20260226-r2 | 17480.3 | n/a | n/a | 873.3 | 16607.0 | n/a | n/a |
| lockd | run | workloade-query-scan | core | scan | documents | 2026-02-26T00:15:55Z | 20260226-r2 | 17013.8 | n/a | n/a | 847.3 | 16166.5 | n/a | n/a |
