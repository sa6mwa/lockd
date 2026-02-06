# TC Leader + Cluster Membership + RM Registry Subsystem

## 1) Purpose

This subsystem coordinates multi-TC deployments: membership leases, leader election, and RM endpoint replication so XA decisions can be routed and applied across backend islands safely.

## 2) Architecture and implementation details

- Membership store:
  - `internal/tccluster/store.go` persists active TC leases under `.lockd/tc-cluster/leases/*`.
  - identity is derived from peer certificates (SPIFFE) or endpoint fallback when mTLS is disabled.
- Leader election:
  - `internal/tcleader/manager.go` runs quorum lease elections and renewals.
  - `internal/tcleader/lease.go` manages in-memory lease semantics and term progression.
- RM registry:
  - `internal/tcrm/store.go` stores backend-hash -> endpoint mappings.
  - `internal/tcrm/replicator.go` propagates register/unregister operations to peer TCs.
- Server wiring:
  - `server.go` starts membership loops, announce fanout, leave fanout, and periodic RM registration.
  - HTTP handlers in `internal/httpapi/handler_endpoints.go` enforce TC-specific certificate checks for cluster and RM APIs.

## 3) Non-style improvements (bugs, security, reliability)

- Endpoint normalization/assembly is too permissive:
  - normalization is mostly trim + trailing slash removal (`normalizeEndpoint`), and request URLs are built by string concatenation.
  - malformed/unsafe endpoint values can slip through and trigger unexpected outbound requests.
  - fix: strict URL parsing/validation (`https` only unless explicit override), then `url.JoinPath` for path assembly.
- Membership lease decode failures are silently skipped:
  - `tccluster.Store.list` logs parse/read errors and continues.
  - corrupted lease entries can hide cluster state divergence and delay detection.
  - fix: emit structured health counters and optionally fail closed when malformed lease ratio crosses a threshold.
- RM replication preflight is all-or-nothing:
  - `tcrm.Replicator.preflight` fails register/unregister when any peer is unhealthy.
  - one bad peer can block control-plane updates cluster-wide.
  - fix: allow quorum/majority policy or degrade to partial replication with explicit error reporting.

## Feature-aligned improvements

- Add a TC control-plane status endpoint summarizing membership freshness, leader term, and RM replication lag/failures for operator visibility.
