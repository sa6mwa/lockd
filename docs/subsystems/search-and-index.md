# Search + Index Subsystem

## 1) Purpose

This subsystem provides key/document query over namespaced state using either full scan or segment-based indexing, with configurable engine selection and streaming document return modes.

## 2) Architecture and implementation details

- API enters through `internal/core/query.go` and becomes `search.Request`.
- Query routing:
  - `internal/search/dispatcher.go` selects index vs scan adapter using engine hint and capability checks.
  - namespace config (`namespaces.Config`) influences automatic engine selection.
- Scan engine:
  - `internal/search/scan/adapter.go` enumerates metadata keys and loads JSON state documents on demand.
  - selector evaluation is done in-memory via `scan/evaluator.go`.
- Index engine:
  - `internal/search/index/manager.go` owns per-namespace writers and visibility writers.
  - writes produce/flush manifests + segments; reads can include index sequence metadata.
- Core integration:
  - `internal/core/query.go` supports `return=keys|documents`.
  - document streaming uses bounded parallel prefetch (`QueryDocPrefetch`) while preserving output order.
  - index rebuild can be scheduled on read when format mismatch is detected.
- Operational controls:
  - explicit `/v1/index/flush` endpoint supports synchronous and asynchronous flush modes.
  - warmup/readability hooks reduce stale reads after writes.

## 3) Non-style improvements (bugs, security, reliability)

- Scan path can silently return incomplete results:
  - `internal/search/scan/adapter.go` logs `LoadMeta`/state errors and continues.
  - transient backend failures become false negatives instead of surfaced query errors.
  - fix: fail query on non-`ErrNotFound` read errors, or return explicit partial-result metadata.
- Index flush endpoint lacks body size limit and can spawn unbounded async work:
  - `handleIndexFlush` in `internal/httpapi/handler_endpoints.go` decodes from raw `r.Body` and starts one goroutine per async request.
  - this is a resource exhaustion path under high request volume.
  - fix: wrap with `http.MaxBytesReader` and enforce per-namespace in-flight dedupe/semaphores.
- Namespace writer map growth is unbounded:
  - `index.Manager` stores per-namespace writer instances in process maps with no eviction.
  - untrusted/high-cardinality namespaces can grow resident memory indefinitely.
  - fix: add lifecycle/eviction for idle namespace writers.

## Feature-aligned improvements

- Add query result metadata fields for `engine_used`, `index_age`, and `partial` to make consistency and fallback behavior explicit to clients.
