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

## 3) Implemented non-style improvements (bugs, security, reliability)

- Staged state CAS retries are now replay-safe:
  - `internal/core/update.go` rewinds/recreates staged payload readers before each `StageState` retry attempt.
  - this prevents empty/truncated staged writes after CAS mismatch retries.
- Core regression guards now cover staged retry correctness:
  - `internal/core/update_retry_test.go` exercises forced CAS retry paths and validates payload integrity.
- Metadata clone safety was hardened in `internal/core/locks.go`:
  - `cloneMeta` now deep-copies mutable map fields (`Attributes`, `StagedAttributes`) in addition to slices.
  - `internal/core/locks_clone_test.go` verifies no aliasing bleed across cloned metadata.

## Feature-aligned improvements

- Make acquire wait backoff fully context-aware so cancellation interrupts sleep immediately in all blocking loops.
- Add an optional server-side lease operation journal (acquire/renew/release/reject with correlation id) for deterministic postmortems without changing API semantics.
