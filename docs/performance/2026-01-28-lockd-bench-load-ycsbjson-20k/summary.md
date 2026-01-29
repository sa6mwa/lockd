# lockd-bench load (20k ops, YCSB JSON)

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
- total throughput: **375.2 ops/s**
- total p50: 25.376ms, p90: 29.481ms, p99: 41.882ms
- errors: 0

## CPU pprof highlights (5s)
- Heavy runtime + syscall time: `syscall.Syscall6`, `runtime.futex`, `runtime.scanobject`, `runtime.gcDrain`, `runtime.mallocgc`.
- HTTP/2 server path dominates cumulative time: `golang.org/x/net/http2.(*serverConn).runHandler`, `readFrames`, `writeFrameAsync`, `net/http.(*ServeMux).ServeHTTP`.
- Lockd hot path present but not top-flat: `internal/httpapi.(*Handler).wrap.func1`, `handleAcquire`, `internal/storage/disk.(*Store).StoreMeta`, `(*logNamespace).appendRecord`, `(*Store).WriteState`.
- TLS + JSON visible but not dominant: `crypto/tls.(*Conn).writeRecordLocked`, `encoding/json.(*decodeState).object`.

## Heap pprof highlights (inuse)
- Disk logstore dominates in-use memory: `(*logNamespace).readSegmentLocked`, `allocRef`, `appendRecord`, `flushAppends`, `(*Store).StoreMeta`, `(*Store).WriteState`.
- Indexing memtable shows up: `internal/search/index.(*MemTable).Flush`, `dedupeAndSort`.
- Key shaping/UUID materialization: `internal/core.(*Service).namespacedKey`, `uuid.UUID.String`, `encoding/hex.EncodeToString`.
- HTTP/2 handler path aggregates a large cumulative footprint (server handler + cobra startup), but top flat allocations are storage-centric.

## Artifacts
- bench output: `bench-load-endpoint.log`
- cpu profile: `lockd-server.cpu.pprof`
- cpu top: `lockd-server.cpu.top.txt`
- heap profile: `lockd-server.heap.pprof`
- heap top: `lockd-server.heap.top.txt`
