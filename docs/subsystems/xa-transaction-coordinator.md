# XA Transaction Coordinator Subsystem

## 1) Purpose

This subsystem provides XA-style transaction decisions and participant application for lock/state/queue operations, including pending decision persistence, commit/rollback replay, and backend-island fanout via TC/RM.

## 2) Architecture and implementation details

- Transaction state and participant metadata are managed in `internal/core/txn.go`.
- Decision persistence model:
  - transaction records (`.txns` namespace)
  - decision markers/index for retention and replay (`.txn-decisions` paths)
- Apply pipeline:
  - `ApplyTxnDecisionRecord` and `applyTxnDecisionWithRetry` apply decisions per participant key.
  - per-participant errors are tracked; applied participants are persisted for idempotent retry.
- Queue integration:
  - dequeue-enlisted message decisions are tracked via queue decision worklists.
  - queue decision list avoids full queue scans on restart/recovery.
- TC fanout integration:
  - `internal/txncoord/coordinator.go` records local decision, applies local participants, then fans out to remote backend groups.
  - `internal/txncoord/decider.go` forwards non-leader requests to current TC leader when leader election is enabled.

## 3) Non-style improvements (bugs, security, reliability)

- Fanout retry policy retries non-retryable failures:
  - `applyWithRetry` in `internal/txncoord/coordinator.go` retries most errors uniformly.
  - deterministic 4xx-style contract failures can be retried unnecessarily, increasing latency and load.
  - fix: retry only transient/network/5xx classes.
- Index maintenance in txn finalizers can silently fail:
  - `queueIndexInsert` finalizer in `internal/core/txn.go` ignores read/index errors (`return nil` on failures).
  - transaction apply may succeed while index visibility lags silently.
  - fix: surface finalizer failures to metrics/alerts and schedule deterministic rebuild for affected namespaces.
- Rollback cleanup ignores staged artifact delete failures:
  - rollback path in `applyTxnDecisionForMeta` uses best-effort staged state/attachment deletes.
  - repeated failures can leave stale staged data that complicates replay/sweep behavior.
  - fix: track unresolved cleanup in transaction metadata for deterministic follow-up cleanup.

## Feature-aligned improvements

- Add a transaction diagnostics endpoint returning participant apply status by backend hash, including retry counts and last error classification.
