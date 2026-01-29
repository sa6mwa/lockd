# Sweepers (Current Behavior)

Lockd uses several cleanup mechanisms to keep leases, transactions, and indexes
bounded without relying on full scans on every backend. This document describes
how sweeping works today.

## 1) Transparent cleanup on normal operations

Many operations validate lease state before proceeding (acquire, get, update,
remove, attachments, queue ack/nack/extend). If a lease is found to be expired,
lockd clears it inline using CAS and proceeds with the requested operation.
This keeps correctness independent of background jobs.

## 2) Transaction replay on active operations

On state and queue operations, lockd may run a bounded replay of transaction
records (`.txns`) to apply committed/rolled-back decisions and to roll back
expired pending transactions.

This replay is throttled by `--txn-replay-interval` (default 5s) and bounded
(currently 128 ops / 2s per run). It is best-effort and safe to run during
traffic because it applies decisions idempotently.

Transaction decision markers live under `.txn-decisions/m/<txn-id>`. An index
entry is also written under `.txn-decisions/e/<bucket>/<txn-id>` to support
bounded sweeps (see below).

## 3) Idle maintenance sweeper

A low-impact sweeper runs only when the server has been idle for
`--idle-sweep-grace` (default 5m). It executes on the global
`--sweeper-interval` tick (default 5m) and exits early if traffic resumes.

Each run is bounded by:
- `--idle-sweep-max-ops` (default 1000)
- `--idle-sweep-max-runtime` (default 30s)
- `--idle-sweep-op-delay` (default 100ms)

The idle sweeper performs three steps:

1. **Lease index sweep**
   - Each namespace maintains a bucket index at `.lease-index/buckets.json`.
   - Expiry entries are written under `.lease-index/<bucket>/<key>`.
   - Buckets are hour-based (UTC, format `2006010215`).
   - The sweeper clears expired leases using the index and removes stale
     entries.

2. **Transaction record sweep (partial)**
   - Replays a bounded subset of `.txns` records and applies decisions.

3. **Decision index sweep**
   - Decision markers live at `.txn-decisions/m/<txn-id>`.
   - Index entries live at `.txn-decisions/e/<bucket>/<txn-id>` with a
     bucket index at `.txn-decisions/buckets.json`.
   - Expired markers are deleted via the index.

If a backend does not implement the required list operations, the sweeper
skips that step.

## 4) Disk retention janitor (disk/NFS only)

When `--disk-retention` is set, the disk backend runs a separate janitor that
removes metadata and state whose `updated_at_unix` exceeds the retention age.
The interval defaults to half the retention window, clamped between 1 minute
and 1 hour (override via `--disk-janitor-interval`).

This janitor is independent of the idle sweeper and applies only to disk/NFS
storage.
