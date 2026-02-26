Query engine comparison (YCSB)
| Engine     | Latest ts            | TOTAL ops/s  | SCAN ops/s   | Prev SCAN    | Delta      | BENCHMARKS   | Delta      |
|------------|----------------------|--------------|--------------|--------------|------------|--------------|------------|
| index      | 2026-02-26T00:15:32Z |      17480.3 |      16607.0 |          n/a |        n/a |        814.4 |   1939.17% |
| scan       | 2026-02-26T00:15:55Z |      17013.8 |      16166.5 |      16715.8 |     -3.29% |        422.5 |   3726.39% |

notes:
- BENCHMARKS reference is lockd-bench query workload ops/s (not YCSB SCAN ops/s).
- Prev SCAN and Delta are computed from earlier entries in performance.log with matching query_engine metadata.
