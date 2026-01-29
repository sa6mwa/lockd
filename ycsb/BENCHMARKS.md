# Lockd YCSB benchmarks

This document captures benchmark context, caveats, and results for the lockd
YCSB driver. It replaces the older CAVEATS writeup and includes the test matrix
used for repeatable runs.

## Context and caveats

Lockd is not a plain KV store. YCSB comparisons are useful, but only when the
extra semantics are called out explicitly.

Key differences vs typical KV stores:
- **Lease-based writes, not atomic puts.** Each write is `Acquire → Update → Release`.
  The lease step is both a concurrency guard and a durability boundary, so a single
  logical write maps to multiple backend operations.
- **Multi-instance on shared storage with backend-specific HA.** Lockd supports
  shared-backend multi-server operation, but the allowed HA mode depends on the backend:
  - **disk/NFS** require a *single serialized writer* (`ha=failover`), even for
    single-server benchmarks, to preserve correctness and performance.
  - **object stores** (S3/Azure/MinIO) can use `ha=concurrent` and tolerate
    parallel writers via object semantics.
- **Full XA support.** Lockd implements a TC-driven XA protocol (prepare/commit/rollback)
  for multi-participant transactions. The single-participant fast path is optimized,
  but XA fields and metadata still exist in the model.
- **Durability boundary is release/commit.** Staged updates are not visible until
  release/commit, and release must be durable. This differs from KV stores where
  a single put is both acquire and commit.
- **Logstore-backed durability.** Disk/NFS backends write through the logstore, which
  batches appends and fsyncs; tuning (commit batch size, segment sizing) and the
  single-append fast path can materially impact write throughput.
- **Query/index integration.** Lockd can maintain a secondary index and query system.
  Index updates are asynchronous; visibility may lag, and the indexer can flush in
  the background even during steady-state workloads.
- **mTLS + encryption by default.** Benchmarks run with mTLS + crypto-on storage
  to match production defaults unless explicitly disabled.
- **Attachments are first-class.** Attachments can be staged/committed alongside
  state; this adds metadata and IO overhead in some paths.
- **Cold cache sensitivity.** Disk/NFS benchmarks are sensitive to filesystem cache
  warmth; warmup windows are enabled by default in the YCSB runner to stabilize results.

What we expect to be fast:
- **Public reads** (no lease) should be competitive once load/commit paths are tuned.
- **Index-backed queries** should be competitive once indexing flush/async paths are tuned.
- **Single-participant writes** (fast path) should approach KV throughput when batching
  and redundant IO are tuned.

What is *not* directly comparable:
- Multi-participant XA transactions vs simple KV puts.
- Multi-node shared-backend safety vs single-node local KV semantics.

## YCSB test matrix (lockd)

Baseline:
- `make lockd-load`: load core dataset into lockd.
- `make lockd-run`: run core workload (read/update mix).

Attachments overlay:
- `make lockd-load-attach`: load with attachment staging enabled.
- `make lockd-run-attach`: run with attachment staging enabled.

Explicit transactions overlay:
- `make lockd-load-txn`: load with explicit xid per operation.
- `make lockd-run-txn`: run with explicit xid per operation.

Query sync overlay (index visibility):
- `LOCKD_EXTRA_PROPS="workloads/lockd.querysync.properties"` on any lockd target
  to require index refresh before returning writes/queries.

Etcd comparison (no attachments/explicit XA):
- `make etcd-load`
- `make etcd-run`

## Performance & comparison (2026-01-27 baseline)

Baseline environment:
- Dev stack via `devenv/docker-compose.yaml` (lockd + etcd)
- Workload: `workloada`
- Record count: 10000
- Operation count: 100000
- Threads: 8
- Target: 0 (no throttling)
- Warmup: enabled by default in `ycsb/Makefile` (see `WARMUP_*` variables); set `WARMUP_OPS=0` to disable.

Baseline commands:

```bash
cd ycsb
make lockd-load
make lockd-run
make etcd-load
make etcd-run
```

Lockd vs etcd (under the hood):
- **etcd** is a Raft-backed key/value store with a native `Txn` API (compare/put/delete
  in a single etcd transaction) but it does **not** implement XA (no prepare/commit
  phases, no TC-driven multi-participant protocol).
- **lockd** implements full XA semantics (TC-driven prepare/commit/rollback) plus
  lease-based acquires for writes, optional attachment staging, and query/indexing.
  etcd does **not** provide a secondary index or document-query API, so there is
  no etcd equivalent to lockd's query-index/query-scan benchmarks. We intentionally
  omit YCSB scan (workloade) results because YCSB scans are simple key-range reads
  and do not reflect lockd's query semantics or typical production usage.

Baseline load results (core workload):
- **lockd load (workloada)**: 10.1s, 1980.0 ops/s
  - Avg: 7.996 ms; p50: 7.623 ms; p90: 9.855 ms; p95: 10.511 ms; p99: 28.927 ms;
    p99.9: 51.199 ms; p99.99: 72.063 ms
- **etcd load (workloada)**: 4.1492s, 2411.9 ops/s
  - Avg: 3.281 ms; p50: 2.841 ms; p90: 4.073 ms; p95: 5.783 ms; p99: 22.303 ms;
    p99.9: 51.487 ms; p99.99: 65.215 ms

Baseline run results (core workload, 50/50 read/update):
- **lockd run (workloada)**: 71.3s, 2803.6 ops/s
  - READ avg: 0.757 ms; p50: 0.684 ms; p90: 1.075 ms; p95: 1.249 ms; p99: 1.986 ms;
    p99.9: 12.591 ms; p99.99: 41.151 ms
  - UPDATE avg: 10.231 ms; p50: 5.951 ms; p90: 26.415 ms; p95: 28.175 ms; p99: 34.271 ms;
    p99.9: 95.935 ms; p99.99: 507.135 ms
- **etcd run (workloada)**: 15.355s, 6517.7 ops/s
  - READ avg: 1.176 ms; p50: 1.018 ms; p90: 1.703 ms; p95: 1.996 ms; p99: 6.607 ms;
    p99.9: 23.391 ms; p99.99: 41.919 ms
  - UPDATE avg: 1.254 ms; p50: 1.055 ms; p90: 1.656 ms; p95: 1.933 ms; p99: 7.315 ms;
    p99.9: 23.503 ms; p99.99: 44.767 ms

Attachment overlay results (lockd only):
- **lockd load + attachments (workloada)**: 13.5s, 1486.7 ops/s
  - Avg: 10.685 ms; p50: 8.055 ms; p90: 14.711 ms; p95: 33.663 ms; p99: 46.847 ms;
    p99.9: 93.759 ms; p99.99: 269.823 ms
- **lockd run + attachments (workloada)**: 56.7s, 3524.4 ops/s
  - READ avg: 0.792 ms; p50: 0.665 ms; p90: 1.092 ms; p95: 1.278 ms; p99: 2.255 ms;
    p99.9: 17.999 ms; p99.99: 46.879 ms
  - UPDATE avg: 7.061 ms; p50: 5.547 ms; p90: 8.175 ms; p95: 14.623 ms; p99: 33.215 ms;
    p99.9: 104.511 ms; p99.99: 507.391 ms

Explicit transaction overlay results (lockd only):
- **lockd load + explicit txn (workloada)**: 9.5s, 2111.2 ops/s
  - Avg: 7.508 ms; p50: 6.895 ms; p90: 9.527 ms; p95: 10.183 ms; p99: 25.455 ms;
    p99.9: 45.759 ms; p99.99: 51.519 ms
- **lockd run + explicit txn (workloada)**: 64.1s, 3117.9 ops/s
  - READ avg: 0.767 ms; p50: 0.692 ms; p90: 1.106 ms; p95: 1.271 ms; p99: 1.938 ms;
    p99.9: 7.055 ms; p99.99: 50.079 ms
  - UPDATE avg: 8.808 ms; p50: 5.779 ms; p90: 23.615 ms; p95: 27.439 ms; p99: 38.687 ms;
    p99.9: 258.559 ms; p99.99: 506.623 ms

Read-heavy workloads (YCSB):
- **lockd run workloadb (95% read / 5% update)**: 15.9s, 12597.2 ops/s
  - READ avg: 0.918 ms; p50: 0.896 ms; p90: 1.209 ms; p95: 1.330 ms; p99: 1.699 ms;
    p99.9: 7.539 ms; p99.99: 19.727 ms
  - UPDATE avg: 7.130 ms; p50: 6.707 ms; p90: 7.867 ms; p95: 8.263 ms; p99: 10.127 ms;
    p99.9: 263.935 ms; p99.99: 507.391 ms
- **lockd run workloadc (100% read)**: 13.2s, 15170.4 ops/s
  - READ avg: 1.045 ms; p50: 0.990 ms; p90: 1.384 ms; p95: 1.540 ms; p99: 2.069 ms;
    p99.9: 12.063 ms; p99.99: 68.415 ms
- **lockd run workloadd (95% read / 5% insert, latest distribution)**: 18.0s, 11105.3 ops/s
  - READ avg: 1.060 ms; p50: 0.990 ms; p90: 1.431 ms; p95: 1.654 ms; p99: 2.209 ms;
    p99.9: 6.215 ms; p99.99: 252.287 ms
  - INSERT avg: 7.892 ms; p50: 7.503 ms; p90: 9.231 ms; p95: 9.895 ms; p99: 17.487 ms;
    p99.9: 49.631 ms; p99.99: 267.263 ms

Read-heavy workloads (YCSB, etcd):
- **etcd run workloadb (95% read / 5% update)**: 10.0s, 20067.6 ops/s
  - READ avg: 0.766 ms; p50: 0.689 ms; p90: 1.235 ms; p95: 1.469 ms; p99: 2.137 ms;
    p99.9: 3.553 ms; p99.99: 4.991 ms
  - UPDATE avg: 1.189 ms; p50: 1.133 ms; p90: 1.621 ms; p95: 1.828 ms; p99: 2.569 ms;
    p99.9: 4.155 ms; p99.99: 7.955 ms
- **etcd run workloadc (100% read)**: 8.2s, 24380.1 ops/s
  - READ avg: 0.648 ms; p50: 0.621 ms; p90: 0.813 ms; p95: 0.934 ms; p99: 1.402 ms;
    p99.9: 2.683 ms; p99.99: 4.053 ms
- **etcd run workloadd (95% read / 5% insert, latest distribution)**: 11.5s, 17391.2 ops/s
  - READ avg: 0.877 ms; p50: 0.787 ms; p90: 1.499 ms; p95: 1.822 ms; p99: 2.549 ms;
    p99.9: 4.051 ms; p99.99: 7.371 ms
  - INSERT avg: 1.505 ms; p50: 1.448 ms; p90: 2.147 ms; p95: 2.413 ms; p99: 3.257 ms;
    p99.9: 4.799 ms; p99.99: 13.511 ms

Lockd query benchmarks (lockd-bench, embedded disk backend, ha=failover):
- **query-index** (ops=2000, concurrency=8, warmup=0): 814.4 ops/s
  - Avg: 9.811 ms; p50: 6.253 ms; p90: 23.533 ms; p95: 34.858 ms; p99: 55.073 ms;
    p99.9: 81.536 ms; min: 1.556 ms; max: 82.338 ms; index_flush: 2.457 s
- **query-scan** (ops=2000, concurrency=8, warmup=0): 422.5 ops/s
  - Avg: 18.915 ms; p50: 18.907 ms; p90: 22.713 ms; p95: 23.926 ms; p99: 26.244 ms;
    p99.9: 31.634 ms; min: 9.544 ms; max: 36.776 ms

Full logs for these runs live under `docs/performance/2026-01-27-ycsb-load/`.
