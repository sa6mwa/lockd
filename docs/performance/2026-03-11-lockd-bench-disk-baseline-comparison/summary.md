# lockd-bench disk baseline comparison

Date: 2026-03-11

## Scope
- Current canonical disk baseline:
  - run id: `20260311T160449Z-8711b95-clean-disk`
  - source: [`docs/performance/lockd-bench-baseline-history.jsonl`](../lockd-bench-baseline-history.jsonl)
- Historical disk reference:
  - source: [`docs/performance/2026-01-24-lockd-bench-baseline-rerun/summary.csv`](../2026-01-24-lockd-bench-baseline-rerun/summary.csv)

## Comparison stance

Use this comparison in two buckets:

1. Reasonable trend comparison
- `attachments`
- `attachments-public`
- `lock`
- `public-read`
- `query-index`
- `query-scan`

These use the same backend and the same workload families, but the benchmark envelope is not identical. The current `embedded-v1` baseline changed run planning and some case sizing, so these numbers should be treated as trend-comparable rather than strict lab-grade parity.

2. Not directly comparable
- `xa-commit`
- `xa-rollback`

Do not compare the old January `xa-*` numbers to the current baseline as a product performance delta. The old benchmark shape had synthetic XA contention that has since been fixed in `lockd-bench`, so the old `xa-*` values are not a valid baseline for current behavior.

## Disk deltas vs January baseline

| Workload | Size | January ops/s | Current ops/s | Delta |
| --- | ---: | ---: | ---: | ---: |
| `attachments` | `4KiB` | `1182.0` | `3098.9` | `+162.2%` |
| `attachments` | `64KiB` | `484.0` | `2869.1` | `+492.8%` |
| `attachments` | `1MiB` | `212.3` | `538.0` | `+153.4%` |
| `attachments` | `8MiB` | `30.0` | `47.1` | `+57.0%` |
| `attachments-public` | `4KiB` | `701.5` | `1400.0` | `+99.6%` |
| `attachments-public` | `64KiB` | `261.9` | `1089.5` | `+316.0%` |
| `attachments-public` | `1MiB` | `113.6` | `235.1` | `+107.0%` |
| `attachments-public` | `8MiB` | `12.0` | `18.0` | `+50.0%` |
| `lock` | `4KiB` | `1136.0` | `4131.6` | `+263.7%` |
| `lock` | `64KiB` | `795.3` | `904.1` | `+13.7%` |
| `lock` | `1MiB` | `111.2` | `122.9` | `+10.5%` |
| `lock` | `8MiB` | `18.1` | `17.9` | `-1.1%` |
| `public-read` | `n/a` | `23430.4` | `42734.0` | `+82.4%` |
| `query-index` | `n/a` | `6708.6` | `7909.7` | `+17.9%` |
| `query-scan` | `n/a` | `533.2` | `2832.5` | `+431.2%` |

## XA note

The current disk baseline is clean:
- `xa-commit`: `2314.2 ops/s`, `errors=0`
- `xa-rollback`: `5125.8 ops/s`, `errors=0`

The January reference is not suitable for delta analysis because the old benchmark shape reused XA keys in a way that created synthetic contention. Treat the current `xa-*` numbers as the first trustworthy canonical baseline for those workloads.

## Practical guidance

For future disk comparisons:
- Compare against `20260311T160449Z-8711b95-clean-disk` for `embedded-v1`
- Use the January CSV only as historical context for non-XA workloads
- Do not use the January `xa-*` values as a regression threshold
