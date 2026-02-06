# Attachments Subsystem

## 1) Purpose

Attachments provide binary payload support as first-class lease/transaction artifacts, allowing large non-JSON data to be staged, committed, read (including public reads), and deleted with the same consistency model as state updates.

## 2) Architecture and implementation details

- API surface:
  - HTTP: `/v1/attachments`, `/v1/attachment`
  - SDK: attachment operations under lease sessions and dedicated attachment store wrappers.
- Core implementation is in `internal/core/attachments.go`:
  - attachment writes are staged per `txn_id` (object key includes staged tx scope).
  - staged attachment metadata is persisted in key metadata (`StagedAttachments`).
  - commit/rollback promotes or discards staged objects with transaction resolution.
- Encryption:
  - object context uses namespace+key-derived context (`storage.AttachmentObjectContext`).
  - per-object material descriptors are persisted and used for decrypt-on-read.
- Access semantics:
  - lease-bound operations enforce `lease_id` + `txn_id` + fencing checks.
  - public reads require published state boundary checks.

## 3) Non-style improvements (bugs, security, reliability)

- Pipe-based encryption writer can leak/block goroutines:
  - `prepareAttachmentPayload` in `internal/core/attachments.go` uses `io.Pipe` + background `io.Copy`.
  - if `PutObject` exits early, the producer goroutine can block on writes.
  - fix: couple pipe writer with request context cancellation and close-on-consumer-failure.
- Transport-level size guard is weak for uploads:
  - `handleAttachmentUpload` passes raw `r.Body` directly to core without `http.MaxBytesReader`.
  - default attachment limit is very large (`DefaultAttachmentMaxBytes` is `1<<40`), enabling long-lived resource-heavy uploads.
  - fix: enforce conservative HTTP-level limits and require explicit opt-in for very large attachments.
- Cleanup failures are mostly ignored, causing orphaned objects:
  - multiple paths in `internal/core/attachments.go` discard delete errors (`_ = s.deleteAttachmentObject(...)`).
  - repeated delete failures leave orphaned staged blobs and inflate storage.
  - fix: persist cleanup debt and retry via sweeper, with metrics/alerts on orphan growth.
- Cross-cutting retry wrapper can corrupt attachment writes:
  - when `storage/retry.Wrap` retries `PutObject`, non-replayable readers are reused.
  - attachment upload streams can be retried with consumed readers.
  - fix: require replayable bodies for retried writes or disable retry for non-rewindable streams.

## Feature-aligned improvements

- Add a background attachment orphan reconciler keyed by staged object conventions to automatically recover from delete failures after crashes or transient backend faults.
