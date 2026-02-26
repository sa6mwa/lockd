# Benchmarks Index

This is the top-level index for benchmark contracts and latest result artifacts.

## Canonical benchmark docs

- End-to-end YCSB matrix and historical comparisons: `ycsb/BENCHMARKS.md`
- Search/index microbench baseline and diminishing-returns policy: `docs/performance/2026-02-25-search-index-phase7-baseline/summary.md`
- Search/index maintenance guard workflow: `docs/performance/search-perf-maintenance-mode.md`

## Latest end-to-end rerun

- Series: `2026-02-26-full-rerun-v2`
- Summary: `docs/performance/2026-02-26-full-rerun-v2/summary.md`
- Query index-vs-scan table: `docs/performance/2026-02-26-full-rerun-v2/query-compare.md`
- Raw log snapshot: `docs/performance/2026-02-26-full-rerun-v2/performance.log`

## Quick compare workflow

```bash
cd ycsb
make perf-summary PERF_SERIES=2026-02-26-full-rerun-v2
make lockd-compare-query
```
