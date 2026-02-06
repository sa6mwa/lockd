# Storage Backends + Logstore + Crypto Subsystem

## 1) Purpose

This subsystem abstracts persistence across memory, disk/NFS logstore, S3/MinIO, AWS, and Azure while preserving CAS semantics, staged writes, and optional transparent encryption.

## 2) Architecture and implementation details

- Core contract is `storage.Backend` in `internal/storage/storage.go`:
  - meta/state/object APIs
  - CAS semantics via `ExpectedETag` and `IfNotExists`
  - namespace/object listing and queue change feed capabilities
- Wrappers:
  - retry wrapper in `internal/storage/retry/retry.go`
  - logging wrapper for backend observability
- Staging layer:
  - `StagingBackend` supports staged state write/promote/discard/list semantics.
  - default implementation stores staged payloads under `key/.staging/<txn>`.
- Disk/NFS path:
  - log-structured backend (`internal/storage/disk/logstore.go`) with segmenting, commit groups, batching, and single-writer controls.
- Encryption:
  - `internal/storage/crypto.go` uses kryptograf materials and per-object context binding (`state`, `attachment`, `queue-meta`, `queue-payload`).
  - descriptors are stored alongside objects/metadata to reconstruct DEKs.

## 3) Non-style improvements (bugs, security, reliability)

- Retry wrapper is unsafe for non-rewindable write bodies:
  - `WriteState`/`PutObject` retries reuse the same `io.Reader` in `internal/storage/retry/retry.go`.
  - transient retries can send truncated/empty payloads after first attempt consumes the body.
  - fix: require replayable readers or buffer once per retry sequence.
- Retry backoff is not context-responsive during sleep:
  - `withRetry` checks context before sleeping but uses blocking `clock.Sleep(delay)`.
  - cancellations during sleep are delayed until wake-up.
  - fix: replace with context-aware timer select for immediate cancellation.
- Staged listing prefix is inconsistent with staged key layout:
  - `ListStagedState` implementations list prefix `.staging/`, but staged keys are generally `<key>/.staging/<txn>`.
  - this can miss staged artifacts for sweeper/recovery tooling.
  - fix: use backend scan pattern that matches nested `/.staging/` paths or maintain an explicit staged index.

## Feature-aligned improvements

- Add an explicit `ReplayableBody` contract (or helper wrappers) for write APIs so retry safety is enforced at compile/use time rather than convention.
