# lockd-bench load (20k ops, YCSB JSON) - etag/meta buffer reuse

## Run config
- date: 2026-01-28
- mode: load
- backend: disk
- concurrency: 8
- ops: 20,000
- payload: YCSB-style JSON, total data bytes = 1000
- crypto: enabled
- endpoint: localhost:9341
- bundle: devenv/volumes/lockd-config/client.pem

## Results
- total throughput: **585.9 ops/s**
- total p50: 9.308ms, p90: 28.422ms, p99: 37.038ms
- errors: 0

## Artifacts
- bench output: `bench-load-endpoint.log`
- cpu profile: `lockd-server.cpu.pprof`
- cpu top: `lockd-server.cpu.top.txt`
- heap profile: `lockd-server.heap.pprof`
- heap top: `lockd-server.heap.top.txt`

## Comparison vs fastpath baseline
- Throughput: **585.9 ops/s** vs **987.7 ops/s** (regression).
- Latency: p50 **9.308ms** vs **7.745ms**; p90 **28.422ms** vs **10.101ms**.
- This run looks significantly worse; likely needs a confirm rerun to rule out environmental variance.
