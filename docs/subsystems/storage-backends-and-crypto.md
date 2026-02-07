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

## 3) Implemented non-style improvements (bugs, security, reliability)

- Retry wrapper write-body safety is now enforced:
  - `internal/storage/retry` detects replayability for write bodies and rewinds seekable readers before retry.
  - non-replayable bodies now fail fast with `ErrNonReplayableBody` on retryable failures instead of risking truncated writes.
- Staged state listing/discovery now matches actual key layout:
  - `ListStagedState` flows now scan object keys and match both root and nested `/.staging/<txn>` forms.
  - nested staged attachment objects are excluded from staged-state enumeration.
- Coverage was added for staging and retry edge cases:
  - `internal/storage/staging_list_test.go` validates filtering/pagination across staged key layouts.
  - `internal/storage/retry/retry_test.go` validates replayable retries and non-replayable fail-fast behavior.

## Feature-aligned improvements

- Add an explicit `ReplayableBody` contract (or helper wrappers) for write APIs so retry safety is enforced at compile/use time rather than convention.
