# Internal Write Batching (Disk/NFS)

Lockd performs internal batching on the disk/NFS backend to amortize fsync
costs while keeping correctness the same for every API call. There is no
client-visible bulk API or batching switch.

## How it works

For each write mutation:

1. Append logstore records to the active segment.
2. Group-commit: fsync the segment once per batch.
3. Acknowledge the request only after fsync completes.

Batching only changes *when* fsync happens; it does not change ordering or
visibility rules.

## Concurrency and HA

- Disk/NFS is **single-writer**.
- The backend runs in **failover** mode; concurrent mode is downgraded.
- When the node is active, single-writer optimizations are enabled.

## NFS behavior

On NFS, the active segment is closed once a commit batch completes to satisfy
close-to-open visibility for readers. This prevents stale reads without shared
append semantics.

## Tuning knobs

- `--logstore-commit-max-ops`: cap on logstore operations per fsync batch.
- `--logstore-segment-size`: rotate segment files at this size.

Defaults are chosen to keep latency low while still coalescing bursts under
natural backpressure.
