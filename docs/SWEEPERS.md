# Sweepers (Design Proposal)

## Summary

Lockd's current sweepers rely on periodic full listings of metadata and decision
records. That is incompatible with large namespaces on object stores (S3/Azure)
because listing everything every few seconds is expensive and risks throttling.

This proposal replaces background full scans with **transparent, operation-driven
sweeping** and an **idle-only maintenance sweeper** that is singleton, rate-limited,
and bounded. The design prioritizes minimal backend IO, especially for S3.

## Goals

- Minimal backend IO on all backends (especially S3).
- Correctness without relying on periodic full scans.
- Transparent cleanup: operations that require updated state enforce it locally.
- Idle maintenance sweeper that only runs when traffic is quiet.
- Strict rate limiting and singleton enforcement for any background work.

## Non-goals

- Relying on filesystem notifications (not available for object stores).
- Aggressive background cleanup regardless of traffic.
- Migration of old decision markers or lease records (cutover only).

## Key idea

**Sweepers should be a proxy to normal operations.** If an operation touches a
key or decision, it should clean up any expired state **before** proceeding. This
makes correctness independent of background sweeps and keeps IO proportional to
real usage.

Background sweeping becomes a low-priority maintenance task that only runs when
the system is idle, and only touches a bounded set of index entries.

## Data layout (no namespace bloat)

Everything remains under existing namespaces (no per-hour namespaces).

### Lease expiry index

```
<namespace>/
  meta/<key>.pb
  .lease-index/<bucket>/<key>
```

- `<bucket>` is hour-based (UTC), e.g. `2026010811`.
- The index entry is tiny (empty JSON or a compact payload).

### Txn decision expiry index

```
.txn-decisions/
  m/<txnID>                     # decision marker JSON
  e/<bucket>/<txnID>             # expiry index entry
```

- Decision markers remain JSON and remain stored under `.txn-decisions`.
- Expiry index entries are only for the idle sweeper.

## Transparent sweeps (operation-driven)

### Leases

On any operation that requires up-to-date lease state:

- Load the meta record for the key.
- If the lease is expired, clear it (CAS) before proceeding.
- Continue the requested operation normally.

This guarantees correctness without requiring any background scan.

### Txn decisions

On any operation that needs a decision:

- Load the marker by txn ID.
- If the marker is expired, delete it before replying.
- Proceed with the operation.

This ensures the decision set stays correct and bounded for active IDs.

## Idle sweeper (maintenance only)

### Trigger

- Starts only after **no operations** for `idle_grace` (default: 5 minutes).
- Singleton: if a sweep is already running, the next tick skips.

### Behavior

- Sweeps both leases and decision indices.
- Operates on **one bucket and one page** at a time.
- Uses **op delay** (sleep between entries) to avoid S3 throttling.
- Uses **caps** (max ops or max runtime) to prevent long, noisy sweeps.

### Stop-on-activity

If traffic resumes during a sweep, the sweeper should stop early:

- It checks the last-operation timestamp periodically.
- If new ops have arrived since sweep start, it exits.
- Correctness is still handled by transparent sweeping.

## Why this is S3-friendly

- No full listings of meta or decisions on a periodic timer.
- Idle sweeper lists only **small prefixes** (index buckets).
- Listing a prefix in S3 is bounded and cheap relative to full listings.
- Rate limiting prevents request spikes or bandwidth saturation.

## Rate limiting & bounds

Recommended defaults:

- `idle_grace`: 5 minutes
- `op_delay`: 100ms between processed entries
- `max_ops_per_run`: 1000
- `max_runtime_per_run`: 30 seconds

These should be configurable with sane defaults.

## Failure handling

- Index entries can be stale; sweepers tolerate them:
  - If meta/marker is missing, delete the index entry.
  - If not expired, delete the stale index entry.
- CAS mismatches are expected; sweepers should continue.
- Idle sweeper is a singleton **runner** only; it does not assume exclusive
  access to sweeping actions. Transparent sweeping and normal operations may
  race with the idle sweeper; all cleanup logic must be safe under concurrency.

## Metrics & visibility

Expose sweep metrics for:

- number of expired leases cleared
- decision markers deleted
- index entries deleted
- sweep duration
- sweep aborts due to activity

## Cutover

No migration:

- Old decision markers remain in place but are ignored by new logic.
- Idle sweeper only targets new index entries.

## Risks & tradeoffs

- Index entries add a small write overhead per lease/decision update.
- Orphaned index entries must be cleaned up (handled by idle sweeper).
- Transparent sweeping adds a small per-op cost on relevant operations,
  but avoids large background scans.

## Open questions

- Exact bucket size: 1 hour is the current proposal.
- Default idle grace: 5 minutes is the current proposal.
- Op delay tuning for large S3 deployments.
