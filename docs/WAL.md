# Disk/NFS Write-Ahead Log (WAL) Design

## Summary
Lockd's disk backend currently performs multiple fsync-heavy writes per request
(meta/state/object + directory syncs). This is safe but expensive and shows up
as the dominant cost in profiling. This document specifies a WAL for the disk
(and NFS) backend that preserves correctness while reducing per-operation sync
cost. WAL is **enabled by default** for disk/NFS, with a `--disable-wal` knob.

## Goals
- Reduce fsync/write-amplification on disk/NFS.
- Preserve existing semantics (CAS/ETag, leases, XA, attachments, queues).
- Crash safety: after power loss or process crash, the store replays WAL to a
  consistent state.
- WAL is **disk/NFS only** and does not change other backends.
- Minimal operational complexity and a clear rollback knob.

## Non-Goals
- No WAL for S3/MinIO/Azure or memory backends.
- No cross-node replication or consensus changes.
- No attempt to change API semantics or cross-layer behavior.
- No performance tuning beyond disk/NFS writes in this phase.

## On-Disk Layout
WAL lives per namespace under a hidden directory:

```
<root>/<namespace>/.wal/
  lock                 # file lock for WAL writer/replay
  checkpoint           # last applied LSN (uint64)
  segment-00000001.wal # append-only segments
  segment-00000002.wal
```

Rationale: per-namespace WAL avoids a global bottleneck while keeping recovery
scoped and predictable. Hidden `.wal/` keeps it out of user-facing listings.

## Record Model
Each WAL entry represents a *fully validated* storage mutation. CAS checks are
performed before logging; WAL entries encode the final file writes/deletes to
apply, not the original API request.

### Header (binary)
- Magic (uint32): `0x4c574c44` ("LWLD")
- Version (uint8)
- Type (uint8)
- Flags (uint16)
- LSN (uint64)
- PayloadLen (uint32)
- PayloadCRC32 (uint32)

### Payload (binary)
Payload encodes one logical mutation. To keep replay idempotent, the payload
contains *exact target paths and bytes* to write (or delete). Suggested layout
for version 1:

Common fields:
- Namespace (u16 len + bytes)
- EntryCount (u16)

Each entry:
- OpKind (u8): WriteFile | DeleteFile
- Path (u16 len + bytes, relative to namespace root)
- BytesLen (u32) + Bytes (for WriteFile)

Examples:
- StoreMeta: WriteFile `meta/<encoded>.pb` with protobuf bytes.
- DeleteMeta: DeleteFile `meta/<encoded>.pb`.
- WriteState: WriteFile `state/<encoded>/data` and WriteFile
  `state/<encoded>/info.json`.
- RemoveState: DeleteFile `state/<encoded>/data` and `state/<encoded>/info.json`.
- PutObject: WriteFile `objects/<path>` and WriteFile `objects/<path>.info.json`.
- DeleteObject: DeleteFile `objects/<path>` and `objects/<path>.info.json`.

This keeps replay simple: apply the exact file mutations in order.

## Write Path (with WAL enabled)
For each storage mutation:
1. Acquire the existing per-key locks and perform CAS/ETag checks.
2. Build the WAL payload using the *final bytes* to be written (already
   encrypted if crypto is enabled).
3. Append WAL record, fsync WAL segment (durable commit).
4. Apply the file mutations to disk **without fsync** on each file.
5. Mark the LSN as applied in-memory.

This converts multiple fsyncs per operation into a single WAL fsync.

## Recovery
On backend startup (or when opening a namespace):
1. Acquire `.wal/lock` to prevent concurrent replay.
2. Read `checkpoint` (last applied LSN). Default is 0.
3. Scan WAL segments in order. For each record with LSN > checkpoint:
   - Verify CRC; stop at first partial/corrupt record.
   - Apply all file mutations in record order (write via temp+rename,
     delete via remove).
4. Update checkpoint to highest contiguous applied LSN.
5. Release lock.

Replay is idempotent: reapplying a WriteFile overwrites the target; DeleteFile
ignores missing files.

## Checkpointing & Compaction
A background routine periodically (or after N ops) persists the highest
contiguous applied LSN to `checkpoint` and fsyncs it. Segments with LSNs
entirely <= checkpoint can be deleted. This keeps WAL bounded.

Initial policy (tunable later):
- checkpoint every 1s or every 1k records
- segment size ~64MB

## Concurrency
- WAL append is serialized per namespace (mutex + `.wal/lock` file lock).
- Existing per-key locks and file locks remain for correctness and
  cross-process coordination.
- Replay runs at startup with exclusive `.wal/lock` to avoid races.

## Configuration
- **Default**: WAL enabled for disk/NFS.
- `--disable-wal` disables WAL for disk/NFS only.
- Server SDK: `Disk.DisableWAL` (or equivalent) to mirror CLI.
- Non-disk backends ignore the flag (documented in CLI help and docs).

## Observability
- Log once at startup: WAL enabled/disabled, namespace WAL path.
- Metrics (future): wal_append_latency, wal_replay_duration,
  wal_records_replayed, wal_segments_pruned.

## Compatibility & Rollback
- WAL is opt-out (`--disable-wal`).
- Disabling WAL reverts to the current fsync-heavy path.
- WAL files can be deleted to force full replays from data files; they are
  independent from main object/meta/state data.

## Risks & Mitigations
- **Large payloads in WAL**: can grow quickly; mitigated via rotation and
  checkpoint pruning.
- **Replay time**: bounded by checkpoint cadence and segment size.
- **Partial records**: CRC and header length allow safe truncation.

## Open Questions (Phase 2/3)
- Group commit to amortize WAL fsync across multiple ops.
- Compression for large payload records.
- WAL metrics and Grafana panels.
