# AcquireForUpdate Overview

This note tracks the state of *AcquireForUpdate* inside the repo now that the streaming implementation has been removed and only the callback helper remains.

## 1. Server behaviour

- The server **no longer exposes** `POST /v1/acquire-for-update`. All streaming-specific code paths, trackers, and configuration knobs were dropped in favour of the normal acquire → get → update workflow.
- Existing endpoints that remain: `/v1/acquire`, `/v1/get`, `/v1/update`, `/v1/keepalive`, `/v1/release`, `/v1/describe`, `/healthz`, `/readyz`.
- No server-side caps such as `for-update-max` or `for-update-max-hold` exist anymore.

## 2. Client surface area

### 2.1 Callback helper (`client.AcquireForUpdate`)

- Signature: `func (c *Client) AcquireForUpdate(ctx context.Context, req AcquireRequest, handler AcquireForUpdateHandler, opts ...AcquireOption) error`.
- `AcquireForUpdateHandler` is `func(context.Context, *AcquireForUpdateContext) error`.
- `AcquireForUpdateContext` bundles:
  - `State *StateSnapshot` – already-fetched JSON state with helpers (`Reader`, `Bytes`, `Decode`).
  - `Session *LeaseSession` plus shortcuts (`Update`, `UpdateBytes`, `UpdateWithOptions`, `KeepAlive`, `Save`, `Load`).
- Lifecycle:
  1. The helper acquires a lease and fetches state (bounded retries via `WithAcquireFailureRetries` + `WithAcquireBackoff`).
  2. While the handler runs, the helper keeps the lease alive (TTL/2 cadence) and cancels the handler context if keepalive fails.
  3. When the handler returns, the helper releases the lease (treating `lease_required` as success) and surfaces any handler/keepalive/release errors.
- Only the handshake retries. Handler logic receives errors immediately and can decide whether to re-run the helper.

### 2.2 Removed stream session

- `AcquireForUpdateSession`, `forUpdateBody`, and `AcquireForUpdateBytes` were deleted from `client/client.go`.
- Any remaining tests or helpers that referenced the old session will fail to compile and must be ported to the callback.

## 3. Integration test status

- The callback flow is already covered by `client/client_integration_test.go` (single-server reconnect + multi-server failover).
- Backend suites (`integration/disk`, `integration/minio`, `integration/aws`, `integration/azure`) still rely on the legacy streaming helpers and are queued for migration to the callback helper.
- Migration checklist:
  1. rewrite those backend helpers so the test logic lives inside the handler callback (assert on handler errors rather than stream reads),
  2. keep chaos-proxy scenarios to verify handshake retries and multi-endpoint failover,
  3. delete the `*_stream_integration_test.go` scaffolding once replacements exist.

## 4. Practical guidance

- Build new features on top of the callback helper only.
- Keep docs (README, `doc.go`) aligned with the simplified flow whenever you touch client code.
- Tests should continue to include watchdogs for long-running scenarios, but there is no longer any streaming-specific retry logic to maintain.

## 5. Background: why the shift to callbacks?

- **Original design**: `AcquireForUpdateSession` returned a streaming reader that attempted to recover from disconnects by re-running `Get` or even reacquiring the lease mid-read.
- **Pain points**: hangs caused by `lease_required` loops, backend-specific edge cases (S3 409s, Azure timeouts), and flaky integration suites that needed large payloads and forced disconnects to trigger the recovery path.
- **Callback rationale**: most workloads already materialise the JSON snapshot before doing work. The callback reuses the proven `LeaseSession` APIs, avoids hidden retry loops, and keeps failure handling explicit.
- **Benefits**: greatly simplified client code, deterministic error propagation, faster/cleaner tests, and a clearer path for non-Go SDKs.

This document stays until all integration suites and docs are fully aligned with the callback helper. Once that work is finished, we can trim or delete the note.
