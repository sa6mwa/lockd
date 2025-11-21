# XA-ish Roadmap for lockd

This document outlines a staged path from today’s CAS/lease semantics to a practical XA-compatible Transaction Coordinator (TC) where every lockd node can act as both Resource Manager (RM) and TC, plus adapters for non-lockd participants (e.g., Postgres). The goal is to stay idiomatic to lockd: leases, CAS, namespaced storage, and manifest-driven indexing.

## Guiding constraints
- Keep current lockd semantics intact: leases prevent concurrent writers; CAS remains the conflict detector; observed-key warm-up stays.
- Transactions must span keys and namespaces; namespace boundaries are not transaction boundaries.
- Idempotent, replay-safe operations; decisions are durable before fan-out.
- Prefer storage-native moves/CAS over heavyweight logs; background sweepers remain responsible for orphaned artifacts.

## Phase 0: Single-key transactional release (foundation)
- Add `ReleaseDecision` (`commit`, `rollback`) to the ordinary `Acquire` flow.
- Staging layout: `/<ns>/<key>/.staging/<txn-id>` plus optional undo blob; commit = CAS head → staged, rollback = delete staged.
- Lease expiry implies rollback; sweeper deletes stale staging.
- Tests: unit + `integration/.../disk/query` for staged commit/rollback, CAS conflicts, expiry rollback.

## Phase 1: Multi-key on one node (deterministic 2PC, no external TC)
- Shared txn record `/.txns/<txn-id>` (state: pending|commit|rollback, participants[], ttl, decision_etag).
- Prepare: acquire keys in sorted order, stage blobs with ExpectedETag=head, register participants in the txn record.
- Any participant can decide: if all prepared, CAS decision to `commit`; on failure/timeout, CAS to `rollback`. First wins; idempotent.
- Fan-out: participants watch the txn record; commit = CAS head→staged, rollback = delete staged.
- Recovery: on startup, scan txn records; replay commit CAS or roll back expired pending.
- Tests: add focused suite for concurrent prepares, partial failures, restart recovery.

## Phase 2: Multi-node, shared backend (lockd node can be TC+RM)
- Each node exposes an RM API (prepare/commit/rollback) backed by the Phase 1 primitives.
- TC role can be co-located: node that receives the client request writes the txn record and coordinates.
- Use backend as the durability surface: txn decision persisted before Phase-2 fan-out.
- Node liveness: lease timeouts + txn TTL handle lost coordinators; peers can read the decision record and complete.
- Tests: multi-process integration targeting `mem` and `disk` with shared FS to prove fan-out and replay.

## Phase 2.5: Queues as first-class participants (shared-backend constraint)
- Dequeue enlists at the ACK/NACK level (JMS/XA style). The queue resource joins the txn; the message body is not staged elsewhere.
- Prepare: dequeue marks the message “in-flight” with a visibility lease/hold on the queue backend (must be the same backend that stored the message).
- Commit: XA decision = ACK → delete/advance head under queue CAS guarantees.
- Rollback: XA decision = NACK → clear hold / restore visibility; message becomes available again. No staged delete list required.
- Enqueue-in-XA (optional): producer can stage enqueue data and publish on commit; rollback drops the staged segment. Useful when producer and consumer are in the same txn.
- Ordering guarantees: apply commit deletions in queue order on that backend; cross-island coordination still uses the global txn record, but dequeue/ACK remains backend-local.
- Tests: dequeue enlisted in XA (commit=ACK, rollback=NACK), mixed Acquire + queue txn, enqueue+dequeue in one txn, restart recovery of held messages.

## Phase 3: Island-aware TC (multiple backends, still within one “cluster”)
- Each RM publishes a stable `backend_hash` derived from storage endpoint + bucket/path + crypto params; collision-resistant enough to treat as island ID. If derivation is impossible or unstable, generate a UUIDv7 (or similar) once and persist it in the backend (e.g., `/.lockd/backend-id`) with CAS/ETag guard. Only regenerate when the marker is missing so all RMs that share the backend converge on the same id across restarts.
- TC groups participants by backend_hash. If all participants share a hash, it can use a single txn record on that backend; otherwise it must create per-island txn records plus a small global decision record.
- Commit protocol: decide once globally, then fan-out to each island’s txn record; per-island commit uses the same Phase 2PC. Rollback similarly.
- Optimization: islands that share a backend can short-circuit fan-out to a single CAS batch.
- Tests: simulated two-backend setup (e.g., disk + minio) verifying mixed-island commits and partial failures.

## Phase 4: External adapters (Postgres example)
- Adapter acts as an RM shim implementing lockd’s RM API: prepare = `BEGIN; PREPARE TRANSACTION '<id>'`, commit = `COMMIT PREPARED`, rollback = `ROLLBACK PREPARED`.
- Credentials/config stored in TC config; participant registry lists adapter type + endpoint + backend_hash (real storage hash unknown, so adapter supplies a synthetic one to force island fan-out).
- Durability: TC records decisions; adapter replays on recovery by reading the txn record.
- Add conformance tests with a local Postgres container; cover lost TC, lost adapter, and idempotent prepare/commit calls.

## Phase 5: XA surface polish
- Expose XA-like API: `xa_start`, `xa_end`, `xa_prepare`, `xa_commit`, `xa_rollback`, mapped to the TC endpoints; keep lockd-native `Acquire`/`Release` path intact.
- Allow TC-only deployments (no local storage) and TC+RM hybrid nodes.
- Observability: decision logs, per-island timing, watchdogs for long prepares.
- Docs: adapter authoring guide, storage hash spec, failure matrix.

## Open questions / caveats
- Storage capabilities differ (rename vs copy+delete). Shadow-write layout must work on backends without atomic rename; CAS head swap is the fallback.
- Island hash stability: config changes (bucket rename, encryption key rotation) must regenerate the hash; TC needs a migration story. If using a persisted UUID backend-id, regen only when the marker is missing; when regeneration is unavoidable, TC must treat old txn records from the prior id as completed-orphan and sweep them.
- Queue semantics: dequeues are backend-local; cross-island txns can combine queues and key ops, but visibility/ordering must stay consistent with queue CAS invariants. Need a rule for conflicting dequeue prepares when leases expire mid-txn; ACK/NACK binding to XA decision simplifies this but still needs lease-expiry handling.
- Read-your-own-writes for streaming queries during a transaction is out of scope; readers continue to see last committed heads.
- Retention policy for old versions if we keep versioned commits; ensure sweeper bounds storage growth.

## Minimal first milestone (actionable next)
- Implement Phase 0 + Phase 1 semantics (just do it, no feature flag, we are not on a stable release yet).
- Add txn record type, staging layout, and sweeper support.
- Extend integration suites (disk, mem) with multi-key commit/rollback and restart recovery cases.
- Document client/CLI UX for `Release(commit|rollback)`; keep `AcquireForUpdate` as a convenience wrapper.
