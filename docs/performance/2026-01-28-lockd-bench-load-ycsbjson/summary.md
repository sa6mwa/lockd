# lockd-bench load (YCSB-style JSON default)

Generated on 2026-01-28.

## Context
- Workload: lockd-bench `mode=load`
- Backend: disk (endpoint)
- Concurrency: 8
- Ops: 2000
- Runs: 1 (warmup 0)
- Payload: YCSB-style JSON (10 fields, total data bytes = 1000)
- Command:

```bash
go run ./cmd/lockd-bench -backend disk -mode load -ops 2000 -runs 1 -warmup 0 --ha failover -concurrency 8 -endpoint localhost:9341 -bundle /home/mike/g/lockd/devenv/volumes/lockd-config/client.pem
```

## Results
From `bench-load-endpoint.log`:
- total: ops/s=1052.1 avg=7.522ms p50=6.860ms p90=9.893ms p95=10.791ms p99=22.452ms p99.9=37.707ms
- acquire avg=4.289ms, update avg=1.219ms, release avg=2.014ms

## Profiling artifacts
- CPU profile: `lockd-server.cpu.pprof` (+ `lockd-server.cpu.top.txt`)
- Heap profile: `lockd-server.heap.pprof` (+ `lockd-server.heap.top.txt`)

## Bottlenecks (CPU top, server)
- **HTTP/2 request handling** dominates cumulative CPU time (`net/http` + `http2` server handlers).
- **Core path**: `Service.Release`, `Service.Update`, `Service.Acquire` are the largest lockd hot paths.
- **Disk logstore**: `disk.(*Store).WriteState`, `StoreMeta`, `logNamespace.appendRecord/flushAppends` show up in cumulative CPU.
- **GC/alloc**: `runtime.mallocgc`, `scanobject`, allocation helpers are non-trivial, likely due to JSON decode + log/meta writes.
- **QRF**: `qrf.Controller.Decide/Wait` shows measurable CPU (soft throttling logic overhead).

## Bottlenecks (heap top, in-use)
- **disk logstore allocations**: `logNamespace.allocRef`, `StoreMeta`, `readSegmentLocked` dominate in-use space.
- **Indexer memory**: `index.(*MemTable).Flush` and `index.dedupeAndSort` are present.
- **Key shaping**: `core.namespacedKey` and `core.leaseIndexKey` allocations are visible.

Notes:
- The profiling window is short (5s) because the run is short; repeated runs can give more stable pprof data.
- Payload size represents total field data bytes; JSON overhead is extra (matches YCSB-style fields).
