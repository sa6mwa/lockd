# lockd-bench load (20k ops, YCSB JSON) - etag/meta reuse (rebuilt server)

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
- total throughput: **370.2 ops/s**
- total p50: 25.488ms, p90: 29.665ms, p99: 47.895ms
- errors: 0

## Artifacts
- bench output: `bench-load-endpoint.log`
- cpu profile: `lockd-server.cpu.pprof`
- cpu top: `lockd-server.cpu.top.txt`
- heap profile: `lockd-server.heap.pprof`
- heap top: `lockd-server.heap.top.txt`

## Notes
- This run used a freshly rebuilt lockd container (post-code changes).
- Throughput matches the earlier baseline (~375 ops/s), suggesting the prior 987 ops/s run was likely environmental or not using the updated server binary.
