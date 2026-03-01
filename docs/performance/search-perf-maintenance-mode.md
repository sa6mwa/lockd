# Search Perf Maintenance Mode

Phase 7 is now in maintenance/guard mode.

## Guard Command

Run from repo root:

```bash
make perf-guard-search
```

This runs the frozen search/index benchmark set and compares current results against:

- `docs/performance/2026-02-25-search-index-phase7-baseline/summary.md`

Default regression thresholds:

- `ns/op`: fail if regression is greater than `+5%`
- `allocs/op`: fail if regression is greater than `+5%`

Override thresholds/environment when needed:

```bash
NS_REGRESSION_PCT=8 ALLOCS_REGRESSION_PCT=8 BENCHTIME=200ms make perf-guard-search
```

## Policy

- Do not land broad structural perf rewrites by default.
- Land targeted perf fixes only with benchmark proof.
- Keep the frozen benchmark set stable unless explicitly re-baselining.
- When re-baselining, update the baseline file in the same change set and call out why.
