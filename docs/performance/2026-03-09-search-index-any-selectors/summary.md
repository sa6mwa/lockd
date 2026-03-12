# Search Index `any=` Selector Benchmarks (2026-03-09)

## Scope

This slice adds executable benchmark coverage for indexed `contains` and `icontains`
selectors that use `any=` / `Term.Any`, and compares them directly against the
existing single-term `value=` path on the same synthetic datasets.

Raw benchmark output is stored in `bench.txt` in this directory.

## Command

Run from repo root:

```bash
GOMAXPROCS=1 go test ./internal/search/index \
  -run '^$' \
  -bench 'Benchmark(SegmentReaderWildcardContains(Any)?|AdapterQueryWildcardContainsCompareProfiles|AdapterQueryWildcardIContainsCompareProfiles|AdapterQueryFullTextContainsCompareProfiles|AdapterQueryFullTextIContainsCompareProfiles|AdapterQueryFullTextAllTextIContainsCompareProfiles)$' \
  -benchmem \
  -benchtime=100ms \
  -count=1
```

## Query-Level Comparison

`value` is the baseline. `any` uses the same matching term plus one guaranteed miss,
so the delta isolates the union cost instead of changing result size.

### `contains` wildcard profiles

| Benchmark | Value ns/op | Any ns/op | Delta | Value allocs/op | Any allocs/op | Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 13,583 | 13,712 | +0.95% | 16 | 15 | -6.25% |
| medium | 62,146 | 63,375 | +1.98% | 16 | 15 | -6.25% |
| large | 184,115 | 190,918 | +3.69% | 16 | 15 | -6.25% |

### `icontains` wildcard profiles

| Benchmark | Value ns/op | Any ns/op | Delta | Value allocs/op | Any allocs/op | Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 13,513 | 13,593 | +0.59% | 16 | 15 | -6.25% |
| medium | 62,850 | 62,102 | -1.19% | 16 | 15 | -6.25% |
| large | 187,292 | 183,763 | -1.88% | 16 | 15 | -6.25% |

### `contains` full-text profiles (`/body`)

| Benchmark | Value ns/op | Any ns/op | Delta | Value allocs/op | Any allocs/op | Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 6,007 | 6,111 | +1.73% | 16 | 15 | -6.25% |
| medium | 17,771 | 17,737 | -0.19% | 16 | 15 | -6.25% |
| large | 49,640 | 50,330 | +1.39% | 16 | 15 | -6.25% |

### `icontains` full-text profiles (`/body`)

| Benchmark | Value ns/op | Any ns/op | Delta | Value allocs/op | Any allocs/op | Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 6,011 | 6,138 | +2.11% | 16 | 15 | -6.25% |
| medium | 17,387 | 17,592 | +1.18% | 16 | 15 | -6.25% |
| large | 49,877 | 49,834 | -0.09% | 16 | 15 | -6.25% |

### `icontains` all-text profiles (`/...`)

| Benchmark | Value ns/op | Any ns/op | Delta | Value allocs/op | Any allocs/op | Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 5,955 | 6,075 | +2.02% | 16 | 15 | -6.25% |
| medium | 17,297 | 17,553 | +1.48% | 16 | 15 | -6.25% |
| large | 49,622 | 49,280 | -0.69% | 16 | 15 | -6.25% |

## Reader-Level Comparison

The raw `segmentReader` exact `contains` path shows the expected extra work more
clearly because it benchmarks only the lower-level docID resolution:

| Benchmark | Value ns/op | Any ns/op | Delta | Value allocs/op | Any allocs/op | Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| wildcard contains | 303,568 | 377,694 | +24.42% | 904 | 1,193 | +31.97% |

## Takeaways

- Query-level `any=` overhead is small on the indexed path. Across the added
  benchmark families it stays within roughly `-2%` to `+4%` versus `value=`.
- The larger overhead is in the lower-level exact `contains` reader benchmark,
  which is expected because `any=` resolves multiple posting sets and unions them.
- The regression fix does not show a large query-layer perf cliff for realistic
  indexed workloads.
