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

## 3) Non-style improvements (bugs, security, reliability)

- Input decoding is too permissive by default:
  - most JSON decoders do not call `DisallowUnknownFields`.
  - misspelled or unsupported fields can be silently ignored, causing unsafe operator assumptions.
  - fix: enable strict decoding on mutating endpoints and return actionable field errors.
- Hostname/SAN binding is intentionally bypassed but lacks hardened mode:
  - SDK/CLI TC HTTP clients use `InsecureSkipVerify` with custom chain validation (host-agnostic trust model).
  - in environments expecting endpoint identity binding, this increases misrouting/MITM blast radius.
  - fix: add optional strict hostname/SPIFFE SAN enforcement mode while keeping current mode as explicit opt-in.
- AcquireForUpdate retry behavior can replay user callback side effects:
  - retry classification in `client/client.go` includes EOF/timeout classes after handler execution paths.
  - non-idempotent handlers can be re-run unexpectedly during transient failures.
  - fix: separate transport retry from user-handler retry, and require explicit opt-in for handler re-execution.

## Feature-aligned improvements

- Add a client-side idempotency token option for acquire-for-update to make safe callback replay explicit and auditable.
