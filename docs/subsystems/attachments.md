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

## 3) Implemented non-style improvements (bugs, security, reliability)

- Attachment encrypted stream producers are now cancellation-aware:
  - `prepareAttachmentPayload` uses `startEncryptedPipe` with request-context cancellation and close propagation.
  - producer goroutines now stop when consumers exit early, preventing blocked pipe writes and goroutine leaks.
- Pipe lifecycle regression coverage was added:
  - `internal/core/attachments_pipe_test.go` validates cancellation and reader-close behavior for encrypted producers.
- Cross-cutting storage retry safety now protects attachment object writes:
  - `internal/storage/retry` fail-fasts non-replayable bodies on retryable write paths.
  - attachment uploads no longer risk retrying consumed bodies and persisting corrupted payloads.

## Feature-aligned improvements

- Add explicit HTTP-level attachment body limits that are stricter than storage defaults, with per-deployment override.
- Add a background attachment orphan reconciler keyed by staged object conventions to automatically recover from delete failures after crashes or transient backend faults.
