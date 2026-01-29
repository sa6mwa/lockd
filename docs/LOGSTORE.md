# Disk/NFS Logstore (Current Implementation)

## Summary

The disk/NFS backend stores all state, metadata, and objects in a log-structured
store. Writes append to segment files and update an in-memory index. A manifest
tracks segment lifecycle so readers can rebuild indexes after restart. This
backend is **single-writer** and runs in **failover** mode only.

## Layout (per namespace)

```
<root>/<namespace>/logstore/
  segments/
    seg-<writer-id>-00000001.log
    seg-<writer-id>-00000002.log
  manifest/
    manifest.log
  snapshots/            # reserved; not used yet
  markers/
    writer-<writer-id>.marker
```

Notes:
- The writer id is per process start.
- The manifest is append-only and records segment open/seal events.
- Markers are used to detect new segments and refresh in-memory indexes.

## Record model

Segments store records for:
- Meta put/delete
- State put/delete
- Object put/delete (attachments, queue payloads, etc.)
- State link records for large payloads

Payloads are fully serialized (and encrypted when storage crypto is enabled).
There are no sidecar temp files in the logstore.

## Write path

1. Validate CAS/lease requirements.
2. Append record(s) to the active segment.
3. Group-commit: fsync the segment once per batch.
4. Update the in-memory index and acknowledge the request.

Batching is internal and controlled by logstore settings (see below).

## Read path

1. Look up the key in the in-memory index.
2. Seek to the segment offset and read the record.
3. Verify the record header/checksum and return data.

## Recovery

On startup, the backend:
1. Replays the manifest to discover segments.
2. Scans all discovered segments to rebuild the in-memory index.

There is no compaction or snapshotting yet; recovery time grows with total
segment volume.

## HA and concurrency

- **Single-writer only.** The disk/NFS backend does not support concurrent
  writers.
- If `--ha concurrent` is requested, lockd downgrades to **failover**.
- The active node enables single-writer optimizations; passive nodes return 503
  for write paths.

## NFS notes

- NFS close-to-open visibility is handled by closing the active segment once a
  commit batch is complete.
- There is no shared append file or multi-writer coordination on NFS.

## Configuration (disk/NFS only)

- `--logstore-segment-size` (bytes): rotate segment files at this size.
- `--logstore-commit-max-ops`: cap on logstore operations per fsync batch.

Retention and janitor settings live on the disk backend (`--disk-retention`,
`--disk-janitor-interval`) and are orthogonal to the logstore format.
