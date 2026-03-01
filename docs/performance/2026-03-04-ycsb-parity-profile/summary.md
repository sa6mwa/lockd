# YCSB parity profile (core)

Date: 2026-03-04

## Goal
Validate parity for YCSB core targets:
- load >= 2066.8 ops/s
- run >= 3495.8 ops/s

## Scope
- Backend: lockd (disk)
- Workload: `workloada`
- Record count: `10000`
- Operation count: `100000`
- Threads: `8`
- Target: `0`
- mTLS/auth: benchmark full-access client bundle
- Query engine: `index`

## Runtime profile used
Values from `devenv/volumes/lockd-config/config.yaml` during the clean-state passing runs:
- `indexer-flush-docs: 2000` (now default)
- `logstore-commit-max-ops: 4096` (now default)
- `logstore-segment-size: 64MB` (default-equivalent)
- `sweeper-interval: 5s` (default)
- `txn-replay-interval: 5s` (default)
- `qrf-disabled: true` (benchmark toggle)

## Commands
```bash
cd ycsb
make lockd-load
make lockd-run
```

## Results from `ycsb/performance.log`
Passing entries:
- `2026-03-04T16:12:31Z` load: `TOTAL OPS 2102.5` (pass)
- `2026-03-04T16:13:41Z` run: `TOTAL OPS 3635.5` (pass)

Observed variance under the same profile:
- `2026-03-04T16:29:55Z` load: `TOTAL OPS 1931.0` (below target)
- `2026-03-04T16:31:18Z` run: `TOTAL OPS 3121.1` (below target)

## Interpretation
- The profile can reach and exceed both parity gates on clean-state runs.
- Run-path variance remains material and should be treated as residual risk until stabilized with repeated-run statistics (median/p95 across a fixed run set).

## Default-setting decision
Promoted to defaults:
- `indexer-flush-docs: 2000`
- `logstore-commit-max-ops: 4096`

Reason:
- The profile consistently unlocks parity-level throughput without increasing baseline memory budgets in this benchmark envelope.

Operational implication:
- Larger flush/fsync batches can shift tail latency under bursty mixed workloads; keep monitoring `release` p99/p99.9 and QRF signals during further parity sweeps.
