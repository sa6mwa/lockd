# lockd-bench load (20k ops, YCSB JSON) - fastpath

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
- total throughput: **987.7 ops/s**
- total p50: 7.745ms, p90: 10.101ms, p99: 24.915ms
- errors: 0

## Artifacts
- bench output: `bench-load-endpoint.log`
- cpu profile: `lockd-server.cpu.pprof`
- cpu top: `lockd-server.cpu.top.txt`
- heap profile: `lockd-server.heap.pprof`
- heap top: `lockd-server.heap.top.txt`

## Comparison vs pre-fastpath (20k run)
- Throughput: **987.7 ops/s** vs **375.2 ops/s** (+~2.63x).
- Latency: p50 **7.745ms** vs **25.376ms**; p99 **24.915ms** vs **41.882ms**.
- CPU: `logNamespace.appendRecord` still visible but lower share; runtime/syscall + HTTP/2 handler path remain dominant.
- Heap: storage/logstore allocations still lead (`allocRef`, `StoreMeta`, `readSegmentLocked`), plus `namespacedKey`/UUID/hex string materialization.

## Next likely bottlenecks
- `logNamespace.allocRef` growth and record ref slabs (in-use memory heavy).
- `StoreMeta`/`WriteState` path allocations and `readSegmentLocked` churn.
- Key/UUID string shaping (`namespacedKey`, `uuid.String`, `hex.EncodeToString`).
