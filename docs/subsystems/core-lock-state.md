# Core Lock + State Subsystem

## 1) Purpose

The core subsystem is the lockd domain engine that enforces lease ownership, fencing, staged JSON state mutation, namespace isolation, and publish/read semantics across all transports.

## 2) Architecture and implementation details

- Primary type is `internal/core.Service`, constructed in `internal/core/service.go`.
- `Service` is transport-agnostic and depends on interfaces/adapters:
  - storage backend (`storage.Backend`)
  - staging backend (`storage.StagingBackend`)
  - queue provider/dispatcher
  - search adapter
  - namespace config store/tracker
  - optional QRF/LSF controllers
- Lease lifecycle is implemented in:
  - `internal/core/locks.go` (`Acquire`, `KeepAlive`, `Release`)
  - `internal/core/lease_cleanup.go` for transparent expiry cleanup
  - fencing tokens are incremented in metadata to prevent stale writers from committing.
- State lifecycle is implemented in:
  - `internal/core/update.go` (`Update`, `Remove`, `Metadata`)
  - `internal/core/state.go` (`Get`, `Describe`)
  - writes are staged by txn (`StageState`) first, then promoted on decision/release.
- Namespace semantics:
  - normalization and validation are centralized (`resolveNamespace`, `namespacedKey`).
  - namespace config influences query engine selection and behavior.
- Public read semantics:
  - `Get(public=1)` returns only published state versions.
  - staged/unpublished writes are hidden from public readers.
- Helper workflows:
  - acquire-for-update callback flow is implemented client-side, but the server-side contract is enforced here (`txn_id`, lease validity, fencing checks).

## 3) Non-style improvements (bugs, security, reliability)

- Retry correctness bug in staged state updates:
  - `internal/core/update.go` obtains a single `reader` from spool before the CAS retry loop, then reuses it on retries.
  - on CAS mismatch retries, the reader may already be at EOF, producing empty/truncated staged payloads.
  - fix: `Seek(0, io.SeekStart)` before each `StageState` attempt (or recreate reader each loop).
- Metadata clone is incomplete and can leak mutable map aliasing:
  - `cloneMeta` in `internal/core/locks.go` deep-copies slices but does not deep-copy map fields like `Attributes` and `StagedAttributes`.
  - this can cause cross-request mutation bleed and hard-to-debug data races/corruption with cached metadata.
  - fix: deep-copy all map fields in `cloneMeta`.
- Acquire wait loop ignores cancellation while sleeping:
  - blocking acquire backoff in `internal/core/locks.go` uses `s.clock.Sleep(sleep)` directly.
  - request cancellation/deadline is only observed after sleep completes.
  - fix: use context-aware sleep (`waitWithContext`) for cancellation responsiveness.

## Feature-aligned improvements

- Add an optional server-side lease operation journal (acquire/renew/release/reject with correlation id) for deterministic postmortems without changing API semantics.
