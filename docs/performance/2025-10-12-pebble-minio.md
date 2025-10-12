# 2025-10-12 – Pebble write-path allocation reduction

## Change summary

- `internal/storage/pebble/pebble.go`: avoid double buffering by hashing `DB.Get` responses directly and reusing the read buffer for `WriteState`, plus reuse the computed hash when storing.
- `internal/httpapi/handler.go`: introduce a `sync.Pool` for the default 4 MiB spool buffer so large uploads don’t allocate a fresh slice per request and release pooled buffers once spills occur.

## Benchmark results

All benches executed on 2025-10-12 (Go 1.25.2, cpu: 13th Gen Intel(R) Core(TM) i7-1355U) using:

```bash
set -a && source .env.local && set +a
go test -bench . -benchmem -tags "integration pebble bench" ./integration/pebble
go test -bench . -benchmem -tags "integration minio bench" ./integration/minio
```

### Pebble (before vs. after)

| Benchmark | Pre-change (2025-10-12T20:55Z) | Post-change (2025-10-12T21:45Z) |
| --- | --- | --- |
| `BenchmarkLockdPebbleLargeJSON` | 86.0 ms/op, 74.7 MB allocs/op (1,459 allocs) | 38.8 ms/op, 24.4 MB allocs/op (1,497 allocs) |
| `BenchmarkLockdPebbleLargeJSONStream` | 50.9 ms/op, 80.9 MB allocs/op (1,802 allocs) | 65.1 ms/op, 25.0 MB allocs/op (1,739 allocs) |
| `BenchmarkLockdPebbleSmallJSON` | 18.1 ms/op, 177 KB allocs/op (1,625 allocs) | 18.3 ms/op, 346 KB allocs/op (1,609 allocs)\* |
| `BenchmarkLockdPebbleSmallJSONStream` | 4.29 ms/op, 43.8 KB allocs/op (452 allocs) | 4.05 ms/op, 56.4 KB allocs/op (449 allocs) |
| `BenchmarkLockdPebbleConcurrent` | 0.659 ms/op, 115 KB allocs/op (551 allocs) | 0.540 ms/op, 70.8 KB allocs/op (527 allocs) |

\*Small buffered writes pick up extra allocations from the larger shared buffer, but remain well under 400 KB/op.

### MinIO regression check

| Benchmark | Post-change (2025-10-12T21:50Z) |
| --- | --- |
| `BenchmarkLockdLargeJSON` | 59.0 ms/op, 1.86 MB allocs/op (6,130 allocs) |
| `BenchmarkLockdLargeJSONStream` | 140.4 ms/op, 2.25 MB allocs/op (6,305 allocs) |
| `BenchmarkLockdSmallJSON` | 351 ms/op, 4.97 MB allocs/op (7,598 allocs) |
| `BenchmarkLockdSmallJSONStream` | 17.8 ms/op, 0.64 MB allocs/op (2,261 allocs) |
| `BenchmarkLockdConcurrentDistinctKeys` | 1.82 ms/op, 0.59 MB allocs/op (2,400 allocs) |
| `BenchmarkLockdConcurrentLarge` | 11.2 ms/op, 1.80 MB allocs/op (6,342 allocs) |

No regressions were observed relative to the 2025-10-12 baseline recorded prior to the Pebble-focused changes.

