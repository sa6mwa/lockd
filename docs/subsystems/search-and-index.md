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

## 3) Implemented non-style improvements (bugs, security, reliability)

- Scan adapter error handling now surfaces real read failures:
  - non-`ErrNotFound` failures from `LoadMeta`/state reads are returned as query errors.
  - this removes false-negative query behavior where backend failures were previously logged-and-skipped.
- Index flush endpoint hardening is now in place:
  - request body size is bounded with `http.MaxBytesReader`.
  - async flushes are deduped per namespace and capped globally to prevent unbounded goroutine growth.
- Targeted regression coverage was added for query/index control-plane behavior:
  - `internal/search/scan/adapter_test.go` validates error surfacing.
  - `internal/httpapi/handler_index_test.go` validates async dedupe and in-flight cap behavior.

## Feature-aligned improvements

- Add query result metadata fields for `engine_used`, `index_age`, and `partial` to make consistency and fallback behavior explicit to clients.
