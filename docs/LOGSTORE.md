# Disk/NFS Logstore

## Summary

The disk/NFS backend stores state, metadata, and objects in a log-structured
store. Writes append to segment files and update an in-memory index. A manifest
tracks segment lifecycle so readers can rebuild indexes after restart. The
backend remains **single-writer**: `ha=concurrent` is downgraded to failover,
`ha=single` skips `.ha` single-presence heartbeats and relies on native
single-writer detection, and `ha=auto` uses the same native fencing before
promoting to failover ownership.

## Layout (per namespace)

```
<root>/<namespace>/logstore/
  segments/
    seg-<writer-id>-00000001.log
    seg-<writer-id>-00000002.log
  manifest/
    manifest.log
  snapshots/
    snap-<writer-id>-00000001.log
  markers/
    writer-<writer-id>.marker
```

Notes:
- The writer id is per process start.
- The manifest is append-only and records segment open/seal events, snapshot
  installs, and obsolete-file markers.
- Markers are used for native single-writer detection and refresh.

## Record model

Segments store records for:
- Meta put/delete
- State put/delete
- Object put/delete (attachments, queue payloads, etc.)
- State link records for large payloads

Payloads are fully serialized (and encrypted when storage crypto is enabled).
There are no sidecar temp files in the logstore. Large values can still be
represented as state-link records, but compaction rewrites live linked payloads
into self-contained snapshot records before reclaiming old files.

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
1. Replays the manifest to discover segments, installed snapshots, and obsolete files.
2. Loads the newest installed snapshot (if present).
3. Replays the remaining tail segments to rebuild in-memory indexes.
4. Migrates legacy open-only manifests forward so pre-compaction histories stay compactable.

Snapshots bound restart time by reducing how much tail history must be replayed.

## Background compaction

Disk/NFS compaction is a background snapshot compactor:

1. Only sealed history is eligible; the active segment is never compacted.
2. The compactor captures a sealed horizon, rewrites the latest live records
   into a new snapshot, validates that the captured refs are still current, and
   installs the snapshot atomically.
3. Old segments/snapshots become obsolete immediately after cutover, but are
   deleted only after a grace period so in-flight readers can finish.

Compaction is skipped when thresholds are not met:
- too few sealed candidates
- too little reclaimable data
- protected live links still point at an installed snapshot or candidate file

Successful compactions log `logstore.compaction.start` and
`logstore.compaction.complete`. Cleanup-only passes log
`logstore.compaction.cleanup.complete` at debug level. Legacy manifest
migrations log `logstore.compaction.legacy_manifest_migrated` at info level.

## HA and concurrency

- **Single-writer only.** The disk/NFS backend does not support concurrent
  writers.
- If `--ha concurrent` is requested, lockd downgrades to **failover**.
- The active node enables single-writer optimizations; passive nodes return 503
  for write paths.
- `ha=single` does not write `.ha` single-presence records on disk/NFS.
  Mixed `single`/`auto` fencing uses the native writer markers instead.

## NFS notes

- NFS close-to-open visibility is handled by closing the active segment once a
  commit batch is complete.
- There is no shared append file or multi-writer coordination on NFS.

## Configuration (disk/NFS only)

- `--logstore-segment-size` (bytes): rotate segment files at this size.
- `--logstore-commit-max-ops`: cap on logstore operations per fsync batch.
- `--logstore-compaction`: enable/disable background compaction (default `true`).
- `--logstore-compaction-interval`: how often the background compactor checks for work
  (default `30m`).
- `--logstore-compaction-min-segments`: minimum sealed snapshot/segment files before
  compaction runs (default `2`).
- `--logstore-compaction-min-reclaim-size`: minimum reclaimable sealed bytes before
  compaction runs (default `64MiB`).
- `--logstore-compaction-delete-grace`: how long obsolete files remain on disk before
  deletion (default `15m`).
- `--logstore-compaction-max-io-bytes-per-sec`: compaction IO throttle
  (default `8MiB/s`; `0` uses the default throttle).
- `--disable-logstore-compaction-throttling`: disable compaction IO throttling entirely.

Retention and janitor settings live on the disk backend (`--disk-retention`,
`--disk-janitor-interval`) and are orthogonal to the logstore format.
