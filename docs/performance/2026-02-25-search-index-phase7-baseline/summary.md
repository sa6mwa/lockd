# Search Index Phase 7 Baseline (2026-02-25)

## Scope

This baseline freezes the `internal/search/index` benchmark datasets and command set for LQL migration perf work.

## Frozen Dataset Profiles

| Profile | Wildcard fields | Wildcard docs/field | Recursive depth | Recursive branch | Fulltext docs | Fulltext templates |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 96 | 16 | 3 | 7 | 2,000 | 24 |
| medium | 256 | 32 | 4 | 7 | 8,000 | 48 |
| large | 512 | 48 | 4 | 9 | 24,000 | 96 |

Selectors covered by frozen profiles:
- wildcard: `icontains{f=/logs/*/message,v=TIMEOUT}`
- recursive: `icontains{f=/tree/.../message,v=TIMEOUT}` (low/high selectivity)
- fulltext-like: `icontains{f=/body,v=TIMEOUT}`

## Reproducible Command Set

Run from repo root:

```bash
GOMAXPROCS=1 go test ./internal/search/index \
  -run '^$' \
  -bench 'Benchmark(SegmentReaderResolveWildcardFields|SegmentReaderResolveRecursiveFields|SegmentReaderWildcardContains|AdapterQueryWildcardContains|AdapterQueryRecursiveDeepLowSelectivity|AdapterQueryRecursiveDeepHighSelectivity|AdapterQueryWildcardWideLowSelectivity|AdapterQueryWildcardWideHighSelectivity|AdapterQueryWildcardContainsProfiles|AdapterQueryRecursiveProfiles|AdapterQueryFullTextContainsProfiles)$' \
  -benchmem \
  -benchtime=100ms \
  -count=1
```

## Baseline Results (Current Head)

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| SegmentReaderResolveWildcardFields | 14.44 | 0 | 0 |
| SegmentReaderResolveRecursiveFields | 23.20 | 0 | 0 |
| SegmentReaderWildcardContains | 828,948 | 202,258 | 4,193 |
| AdapterQueryWildcardContains | 2,158,082 | 1,950,920 | 12,379 |
| AdapterQueryRecursiveDeepLowSelectivity | 11,034,784 | 6,996,656 | 103,614 |
| AdapterQueryRecursiveDeepHighSelectivity | 18,572,973 | 9,728,208 | 157,647 |
| AdapterQueryWildcardWideLowSelectivity | 6,669,036 | 4,352,456 | 68,758 |
| AdapterQueryWildcardWideHighSelectivity | 11,847,504 | 6,176,465 | 108,359 |
| AdapterQueryWildcardContainsProfiles/small | 1,058,406 | 831,726 | 7,671 |
| AdapterQueryWildcardContainsProfiles/medium | 4,682,316 | 3,881,692 | 24,586 |
| AdapterQueryWildcardContainsProfiles/large | 14,336,982 | 10,651,190 | 57,195 |
| AdapterQueryRecursiveProfiles/small/low | 1,427,050 | 986,138 | 13,907 |
| AdapterQueryRecursiveProfiles/small/high | 2,306,080 | 1,370,220 | 21,633 |
| AdapterQueryRecursiveProfiles/medium/low | 10,860,648 | 6,991,179 | 103,614 |
| AdapterQueryRecursiveProfiles/medium/high | 18,464,927 | 9,725,474 | 157,647 |
| AdapterQueryRecursiveProfiles/large/low | 27,053,335 | 16,507,152 | 284,189 |
| AdapterQueryRecursiveProfiles/large/high | 60,577,152 | 23,294,536 | 431,828 |
| AdapterQueryFullTextContainsProfiles/small | 395,273 | 437,491 | 733 |
| AdapterQueryFullTextContainsProfiles/medium | 1,700,476 | 1,809,152 | 2,360 |
| AdapterQueryFullTextContainsProfiles/large | 5,699,311 | 5,119,680 | 6,575 |

## Per-Slice Reporting Requirement

Each perf slice commit note must include a compact before/after table:

| Benchmark | Before ns/op | After ns/op | Delta | Before allocs/op | After allocs/op | Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |

Use this baseline as `Before` for the next slice, then roll forward to the latest committed baseline.

## Diminishing Returns Policy

Stop aggressive perf rewrites when both are true:
- median `ns/op` improvement is `<5%` across two consecutive perf slices
- median `allocs/op` improvement is `<5%` across two consecutive perf slices

Then switch to maintenance mode:
- keep frozen benchmark set in CI/perf checks
- only land targeted perf fixes with benchmark proof
- prioritize regression prevention over large structural rewrites
