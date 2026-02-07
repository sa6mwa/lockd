# HTTP API + Go SDK + CLI Subsystem

## 1) Purpose

This subsystem is lockd’s user-facing control plane: HTTP handlers, Go client, and CLI tooling that expose leases, state, query, queue, attachments, and TC/XA operations.

## 2) Architecture and implementation details

- HTTP surface:
  - `internal/httpapi/handler.go` wires route registration and cross-cutting middleware (tracing, identity, correlation, failover guardrails).
  - `internal/httpapi/handler_endpoints.go` maps transport payloads to core commands and standardized error conversion.
- Client SDK:
  - `client/client.go` implements endpoint failover, lease-session helpers, queue handles, and acquire-for-update callback orchestration.
  - SDK handles correlation id propagation and fencing token tracking.
- CLI:
  - `cmd/lockd/client_cli.go` uses cobra/viper and environment variable export conventions.
  - CLI commands are thin wrappers over SDK behavior to preserve one behavior model.
- TC-auth surfaces:
  - handler gate methods (`requireTCClient`, `requireTCServer`) enforce mTLS trust roots and EKU checks for coordinator-specific endpoints.

## 3) Implemented non-style improvements (bugs, security, reliability)

- Strict JSON decode is now enforced for mutating HTTP endpoints:
  - shared decode helpers now support unknown-field rejection and trailing-token validation.
  - mutating handlers use strict decode by default and return actionable `invalid_body` responses.
- Request-body bounds and async index flush guardrails were hardened:
  - `/v1/index/flush` now uses explicit body-size limits.
  - async flushes are deduped per namespace and globally capped, returning `index_flush_busy` on overload instead of spawning unbounded work.
- TC/RM endpoint input handling at API boundaries is now hardened:
  - endpoint values are normalized and validated before storage/fanout paths.
  - malformed endpoint input is rejected early instead of propagating into downstream fanout.

## Feature-aligned improvements

- Add optional strict hostname/SAN verification mode for TC-facing client transport profiles that require endpoint identity binding.
- Add a client-side idempotency token option for acquire-for-update so callback replay remains explicit and auditable.
