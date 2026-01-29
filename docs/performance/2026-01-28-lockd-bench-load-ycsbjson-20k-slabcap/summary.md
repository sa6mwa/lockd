# lockd-bench load (20k ops, YCSB JSON) - slab cap

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
- total throughput: **997.2 ops/s**
- total p50: 7.628ms, p90: 10.027ms, p99: 25.187ms
- errors: 0

## Artifacts
- bench output: `bench-load-endpoint.log`
- cpu profile: `lockd-server.cpu.pprof`
- cpu top: `lockd-server.cpu.top.txt`
- heap profile: `lockd-server.heap.pprof`
- heap top: `lockd-server.heap.top.txt`

## Comparison vs fastpath (no slab cap)
- Throughput: **997.2 ops/s** vs **987.7 ops/s** (roughly flat).
- Heap in-use: **232.29MB** vs **145.44MB** (higher with slab cap).
- `allocRef` remains top in-use (59.57MB) and overall heap is larger, indicating the cap did not reduce memory pressure.
