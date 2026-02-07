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

## 3) Implemented non-style improvements (bugs, security, reliability)

- Fanout endpoint handling in txn coordinator was hardened:
  - coordinator fanout now sanitizes endpoint lists and uses safe endpoint join helpers for apply URLs.
  - malformed endpoints now fail fast before outbound apply attempts.
- Leader-forward endpoint handling in txn decider was hardened:
  - decider validates/joins leader endpoint paths instead of raw string concatenation.
  - missing or invalid leader endpoint values now produce explicit errors.
- Regression coverage was added for endpoint hardening paths:
  - `internal/txncoord/coordinator_test.go` and `internal/txncoord/decider_test.go` cover malformed endpoint and safe-join behavior.

## Feature-aligned improvements

- Add a transaction diagnostics endpoint returning participant apply status by backend hash, including retry counts and last error classification.
