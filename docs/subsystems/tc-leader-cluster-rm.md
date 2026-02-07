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

## 3) Implemented non-style improvements (bugs, security, reliability)

- Endpoint normalization and validation is now strict at TC/RM boundaries:
  - `tccluster.NormalizeEndpoint` validates URL shape (`http`/`https`, host required, no userinfo/query/fragment).
  - malformed endpoint inputs are rejected before persistence or replication.
- Fanout URL assembly is now safe and canonical:
  - `tccluster.JoinEndpoint` validates relative suffixes and joins paths without fragile string concatenation.
  - TC cluster and RM fanout paths use joined/validated endpoint URLs.
- RM registry replication now normalizes endpoint values in store-level register/unregister paths:
  - registry membership no longer stores multiple syntactic variants of equivalent endpoints.
  - targeted tests were added in `internal/tccluster/store_test.go`, `internal/tcrm/store_test.go`, and `internal/tcrm/replicator_test.go`.

## Feature-aligned improvements

- Add a TC control-plane status endpoint summarizing membership freshness, leader term, and RM replication lag/failures for operator visibility.
