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
  -bench 'Benchmark(SegmentReaderResolveWildcardFields|SegmentReaderResolveRecursiveFields|SegmentReaderWildcardContains|AdapterQueryWildcardContains|AdapterQueryRecursiveDeepLowSelectivity|AdapterQueryRecursiveDeepHighSelectivity|AdapterQueryWildcardWideLowSelectivity|AdapterQueryWildcardWideHighSelectivity|AdapterQueryWildcardContainsProfiles|AdapterQueryRecursiveProfiles|AdapterQueryFullTextContainsProfiles|AdapterQueryFullTextAllTextProfiles)$' \
  -benchmem \
  -benchtime=100ms \
  -count=1
```

## Baseline Results (Current Head)

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| SegmentReaderResolveWildcardFields | 15.66 | 0 | 0 |
| SegmentReaderResolveRecursiveFields | 26.80 | 0 | 0 |
| SegmentReaderWildcardContains | 1,357,091 | 209,528 | 4,251 |
| AdapterQueryWildcardContains | 2,491,616 | 2,095,163 | 14,813 |
| AdapterQueryRecursiveDeepLowSelectivity | 13,355,705 | 7,961,086 | 119,482 |
| AdapterQueryRecursiveDeepHighSelectivity | 25,377,261 | 11,507,748 | 186,483 |
| AdapterQueryWildcardWideLowSelectivity | 8,477,364 | 5,089,451 | 80,009 |
| AdapterQueryWildcardWideHighSelectivity | 14,584,872 | 7,541,688 | 129,117 |
| AdapterQueryWildcardContainsProfiles/small | 1,297,591 | 898,082 | 8,826 |
| AdapterQueryWildcardContainsProfiles/medium | 6,053,747 | 4,171,369 | 29,453 |
| AdapterQueryWildcardContainsProfiles/large | 16,740,725 | 11,390,784 | 66,926 |
| AdapterQueryRecursiveProfiles/small/low | 1,727,882 | 1,135,918 | 16,176 |
| AdapterQueryRecursiveProfiles/small/high | 2,818,284 | 1,630,020 | 25,757 |
| AdapterQueryRecursiveProfiles/medium/low | 17,974,611 | 7,954,776 | 119,483 |
| AdapterQueryRecursiveProfiles/medium/high | 28,377,840 | 11,526,008 | 186,484 |
| AdapterQueryRecursiveProfiles/large/low | 36,914,998 | 19,401,712 | 327,568 |
| AdapterQueryRecursiveProfiles/large/high | 72,242,296 | 28,367,296 | 510,624 |
| AdapterQueryFullTextContainsProfiles/small | 2,509,932 | 751,587 | 1,143 |
| AdapterQueryFullTextContainsProfiles/medium | 11,800,406 | 3,034,826 | 3,059 |
| AdapterQueryFullTextContainsProfiles/large | 42,052,411 | 9,513,202 | 7,852 |
| AdapterQueryFullTextAllTextProfiles/small | 2,447,584 | 787,962 | 1,169 |
| AdapterQueryFullTextAllTextProfiles/medium | 14,646,499 | 3,085,081 | 3,085 |
| AdapterQueryFullTextAllTextProfiles/large | 43,478,154 | 9,538,653 | 7,881 |

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
