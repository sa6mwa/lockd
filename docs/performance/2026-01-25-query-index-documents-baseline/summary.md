# Query Index Return=Documents Baseline (2026-01-25)

## Command

```
go run ./cmd/lockd-bench -backend <backend> -workload query-index -query-return documents -ops <ops> -runs 1 -warmup 0
```

## Results

| Backend | Ops | Ops/s | Total avg | Query avg | Notes |
| --- | ---: | ---: | --- | --- | --- |
| disk | 1000 | 7020.7 | 1.133939ms | 738.31µs | - |
| mem | 1000 | 4860.5 | 1.637794ms | 1.185561ms | cleanup skipped (backend unavailable) |
| minio | 1000 | 519.0 | 15.240496ms | 14.438427ms | qrf soft-arm observed |
| aws | 200 | 33.6 | 236.292909ms | 213.548214ms | ops=200 (ops=1000 exceeded 5m) |
| azure | 200 | 220.8 | 35.708763ms | 31.031215ms | ops=200 |

## Comparison (vs 2026-01-24 baseline rerun)

Baseline source: `docs/performance/2026-01-24-lockd-bench-baseline-rerun/summary.csv` (query-index)

| Backend | Prev Ops/s | New Ops/s | Delta |
| --- | ---: | ---: | ---: |
| disk | 6708.6 | 7020.7 | +5% |
| mem | 4158.1 | 4860.5 | +17% |
| minio | 44.5 | 519.0 | +1066% |
| aws | 0.8 | 33.6 | +4100% |
| azure | 2.9 | 220.8 | +7500% |
