# TC Leader Election, Membership, and RM Registry (Lockd)

This document describes **current behavior** for TC leader election, TC cluster
membership, and RM registry replication in lockd. It is written to make
correctness and failure handling explicit so deviations are easy to spot and
resolve.

If you find drift between this document and the implementation, update the doc
and the code to bring them back into alignment.

## Goals

- Deterministic, secure TC leadership across backend islands without an
  external consensus system.
- Clear, self-only membership semantics (no server can remove another server).
- Robust RM registry replication across the TC cluster.
- Minimal operator burden: no extra knobs; behavior should be transparent.

## Scope

This document covers:

- TC cluster membership: announce, list, leave, TTL expiry.
- TC leader election: quorum leases, term fencing, step-down behavior.
- RM registry: register/unregister replication and failure handling.

It does not cover queue, search, or general storage APIs beyond the RC/TC
interactions described here.

## Deployment Model (TC+RM everywhere)

Every lockd server acts as **both** a TC and an RM. There is no role separation
in this design: each node exposes TC endpoints, RM/data/queue endpoints, and
participates in TC membership, leader election, and RM self-registration.

### `--join` semantics (bootstrap only, no static TC list)

- `--join` is a **bootstrap** list only; it is never treated as a static TC
  membership list.
- Any TC uses `--join` only to reach **any** TC and establish membership.
  Once membership is available, it is the sole source of truth.
- If membership is empty or unavailable, `--join` is used as a temporary
  fallback target list.
- **Startup requirement:** if `--join` is configured, the node **must**
  successfully announce to at least one join target during startup. If all join
  targets are unreachable, the process **fails hard**. Including the node's
  own endpoint in `--join` satisfies this check (useful for first-node
  bootstrap). This requirement is enforced only at startup, not after a
  successful join.

## Identity and Auth (SPIFFE only)

### SPIFFE formats in use

- Server bundles: `spiffe://lockd/server/<node-id>`
- TC client bundles: `spiffe://lockd/tc/<name>`
- SDK client bundles: `spiffe://lockd/sdk/<name>`

### Server identity

- Every server certificate **MUST** contain a SPIFFE URI of the form:
  `spiffe://lockd/server/<node-id>`.
- `<node-id>` is **auto-generated** when the server bundle is created and
  **reused** when re-issuing a bundle if the previous bundle is present.
- When mTLS is disabled, the server may still derive a **local** identity from
  a stable hash of the endpoint (`tccluster.IdentityFromEndpoint`), but
  cross-node membership is **not supported** because announce/leave still
  require a certificate-derived SPIFFE identity.
- Host/SAN verification is **not** required and **not** relied upon. Servers
  can move across IPs/hosts without identity changes.
  
**Why this matters (dedupe):**

- Membership leases are keyed by **identity**, so a stable `<node-id>` prevents
  duplicate entries for the same node after certificate rotation.
- If a node is reissued with a different `<node-id>`, it is treated as a new
  member until the old lease expires; there is no safe way to remove it
  remotely without violating self-only leave.

### Auth rules

- **TC client auth** (client certificate) is required for:
  - `/v1/tc/leader`
  - `/v1/tc/lease/*`
  - `/v1/tc/cluster/list`
  - `/v1/tc/rm/list`
- **TC server auth** (server certificate) is required for:
  - `/v1/tc/cluster/announce`
  - `/v1/tc/cluster/leave`
  - `/v1/tc/rm/register`
  - `/v1/tc/rm/unregister`
- Server certificates are issued with both **server** and **client** auth
  usages, so they can also call TC client endpoints when needed.
- There is no separate "TC server cert" vs "RM server cert" distinction; the
  same server bundle is used for TC membership, leader election calls, and RM
  registry operations.
- `tc-disable-auth` only relaxes certificate verification; membership identity
  is still derived from the caller certificate and therefore still requires a
  cert to be presented.

**mTLS requirement (multi-node):**

- Multi-node TC membership **requires** mTLS with a server certificate
  containing `spiffe://lockd/server/<node-id>`.
- If `--join` includes any non-self endpoints and mTLS is **disabled** or no
  server cert is available, startup **must fail hard**. This prevents insecure
  or ambiguous membership.
- Membership identity is **derived only** from the caller’s certificate
  (`spiffe://lockd/server/<node-id>`). Request payloads are never trusted for
  identity.

## Data Model

### TC cluster membership lease

- Namespace: `.lockd`
- Key: `tc-cluster/leases/<identity>`
- Payload includes:
  - `identity` (SPIFFE-derived node id)
  - `endpoint` (normalized endpoint string)
  - `updated_at_unix`
  - `expires_at_unix`

### RM registry

- Namespace: `.lockd`
- Key: `tc-rm-members`
- Payload groups RM endpoints by backend hash.

### Leader lease (in-memory)

- Per-node in-memory record of current leader and term.
- `LeaderID` is derived from the node’s **SPIFFE identity** (the same
  `<node-id>` used for membership). If mTLS is disabled (single-node only),
  fall back to a stable hash of the endpoint.
- Not persisted across backends; reconstructed via peer queries.

**Rationale:** aligning leader identity with membership identity keeps leader
leases stable across endpoint moves and certificate re-issuance (as long as the
node-id is reused), and avoids a second identity source to reason about.

## TC Cluster Membership

### Membership loop (server behavior)

- On startup, the server derives its **membership identity**:
  - SPIFFE `spiffe://lockd/server/<node-id>` when mTLS is enabled.
  - Endpoint hash when mTLS is disabled.
- The loop runs on an interval of `lease_TTL / 3`.
- Each tick:
  1) If the identity is **not paused**, call `Announce(identity, self_endpoint)`.
  2) Load current membership list.
  3) **Fan out** announce to:
     - current membership endpoints, and
     - self (to ensure presence).
     - If membership is empty/unavailable, fan out to `--join` bootstrap seeds.
  4) **Leader peer set** selection:
     - If membership is non-empty **and** includes self: use membership list.
     - If membership is empty: use join seeds (plus self).
     - If membership is non-empty **but does not include self**: disable
       election until membership includes self.

**mTLS requirement:**

- Fanout announce/leave requires a client certificate because identity is
  derived from SPIFFE; multi-node membership therefore requires mTLS even if
  TC auth verification is disabled.
- When `--join` includes any non-self endpoints, mTLS **must** be enabled at
  startup or the process fails hard.

### Announce

**Purpose:** establish or refresh a membership lease.

**Endpoint:** `POST /v1/tc/cluster/announce`

**Rules:**

- Caller must present a **server cert**.
- Identity is derived from the caller’s SPIFFE URI.
- `self_endpoint` is required and normalized (trim trailing `/`).
- Lease TTL is derived from TC leader TTL (`leader_TTL * 3`).
- Announce **renews** and **updates** the existing lease for the caller’s
  identity.
- The membership loop fans out announce calls to known members; join seeds are
  only used while membership is empty/unavailable. Each backend stores the same
  identity/endpoint lease.
- If the endpoint changes but the identity is stable, announce **overwrites**
  the existing lease; no duplicate membership is created for the same identity.

### Active list

**Endpoint:** `GET /v1/tc/cluster/list`

**Rules:**

- Only non-expired leases are returned.
- Endpoints are normalized and de-duplicated.
- List order is canonicalized (sorted) to keep comparisons deterministic.
- If multiple identities advertise the same endpoint, the list collapses them
  to a single endpoint entry; the underlying leases remain until expiry.

### Leave (self-only)

**Endpoint:** `POST /v1/tc/cluster/leave`

**Rules:**

- Caller must present a **server cert**.
- Identity is derived from the caller’s SPIFFE URI.
- Only that identity’s lease is removed.
- **No server can remove another server’s lease** without that server’s
  certificate.
- After leave, auto-announcements are paused for the identity until the node
  explicitly announces again or restarts.
- A graceful shutdown performs **leave + fanout leave**.
- If the leaving node is the current leader, it **releases its leader leases**
  across peers (`/v1/tc/lease/release`) so a new leader can be elected
  immediately. Peers only accept the release when the `leader_id` and `term`
  match the active lease, so this remains self-only and safe.
- An abort (test crash) **skips** leave so that expiry semantics are exercised.

**Fan-out requirement (bombproof):**

- Because each backend has its own membership store, **a leave must be applied
  to every backend** to avoid divergent membership views.
- The leaving server **MUST** fan out the leave to all TC peers using its own
  server certificate. Each peer validates the caller identity and deletes the
  corresponding lease from its local backend.
- Peers **MUST NOT** re-fan-out a leave request that was already fanned out
  (use a header guard such as `X-Lockd-TC-Leave-Fanout: 1`).
- API-initiated leave is **transactional**: if any fan-out target fails, the
  leave is rejected and the node resumes announcements. Operators should retry
  leave once the cluster is healthy. Graceful shutdown still performs a
  best-effort leave + fan-out before stopping.

### Crash and expiry

- If a server crashes, it cannot actively leave.
- Its lease remains until `expires_at_unix` and then naturally disappears from
  `list` results.
- This is acceptable; it is the only safe outcome when the server is gone.
- Tests simulate crash behavior with an **abort** that skips leave.

## TC Leader Election

### Peer set selection (authoritative)

- **Primary:** use the active membership list from `.lockd/tc-cluster/leases`.
- **Fallback:** if membership is empty, use configured `tc-join` endpoints as
  **bootstrap seeds**.
- The local endpoint is included in the peer set **only** when the node is
  considered part of the cluster (membership contains self or membership is
  empty and join seeds are used).

If membership is non-empty but does not include self, leader election is
disabled until self appears in membership.

This rule makes membership authoritative once it exists and avoids stale join
seeds pinning quorum size.

### Terms and leases

- The leader term is a **monotonically increasing** fencing token.
- To become leader, a candidate:
  1) Queries peer state via `GET /v1/tc/leader`.
  2) Sets `term = max(peer_terms) + 1`.
  3) Calls `/v1/tc/lease/acquire` on all peers (including self).
  4) Becomes leader if it receives **>= quorum** grants.

- Quorum is `floor(n/2) + 1` for `n` peers.
- Election uses **TC client auth**; server certs include client auth and can
  participate in election calls.

### Observed leader (stability)

- Non-leaders query peers via `GET /v1/tc/leader` to learn a current leader.
- A valid observed leader suppresses new elections until its lease expires.
- If a node observes itself as leader and its local lease is still valid, it
  adopts that lease without a new election.

### Renewal

- The leader renews its lease periodically (every `TTL/3`).
- Renewal failure means the leader **must** step down once the local lease
  expires.

### Step-down and quorum loss

- If the leader cannot renew enough votes to meet quorum, it must step down.
- Before stepping down, the leader **best-effort releases** its lease on all
  peers; failures are logged but do not prevent step-down.
- If quorum is lost, no leader should exist until quorum is restored.
- Non-leaders must refuse to decide transactions and return
  `tc_not_leader` or `tc_unavailable` as appropriate.

### Non-leader forwarding and term fencing

- TC deciders may forward client decisions to the current leader.
- If the leader is reachable, non-leader forward succeeds.
- If the leader is unreachable, non-leaders respond with `tc_not_leader`
  (including the last known leader endpoint).
- Decisions applied at RMs must include the current `tc_term`; stale terms
  result in `tc_term_stale` or conflict errors.

### Error codes (expected)

- `tc_unavailable`: no leader can be determined.
- `tc_not_leader`: the node is not leader; includes leader endpoint.
- `tc_term_stale`: a stale term attempted to decide/apply.

## RM Registry (Register/Unregister)

### Periodic RM registration (server loop)

- Every server periodically registers itself as an RM by calling
  `/v1/tc/rm/register` on each TC endpoint discovered via:
  - the active membership list (when available), otherwise
  - the `--join` bootstrap list (fallback only).
- This loop uses the server certificate (server auth) and relies on TC-side
  replication to converge.

### Register

**Endpoint:** `POST /v1/tc/rm/register`

- Caller must present a **server cert**.
- Payload includes `backend_hash` and `endpoint`.
- The TC writes the update locally **then replicates** to all TC peers.
- Replication performs a preflight reachability check via `/v1/tc/leader`.
- If any peer replication fails, the update **must be rolled back** locally and
  a `tc_rm_replication_failed` error is returned (HTTP 502).
  
**Idempotency:** re-registering an existing endpoint is a no-op at the store
level and should not produce divergence.

**Peer set and failure semantics:**

- Replication targets the **active membership list** at the time of the call.
- If a peer is still in membership but unreachable, preflight (or replication)
  fails, and the operation must roll back with `tc_rm_replication_failed`.
- If a peer has **left** (membership lease removed), it is not contacted, and
  replication can succeed.

### Unregister

**Endpoint:** `POST /v1/tc/rm/unregister`

- Same rules as register.
- Replication failures also trigger rollback and return
  `tc_rm_replication_failed`.

### Replica requests

- Replicated RM calls must set a replica header
  (e.g. `X-Lockd-TC-Replicate: 1`) so peers apply locally without
  re-replicating.

## Failure Handling and Invariants

These are **required** invariants for a bombproof system:

1) **Self-only leave:** a node can only delete its own membership lease.
2) **Fan-out leave:** a leave must be applied to every backend using the
   leaving node’s identity to keep membership consistent.
3) **Membership is authoritative:** once membership is non-empty, it is the
   only source for leader peer sets.
4) **Quorum loss means no leader:** leader must step down if renew quorum fails.
5) **RM registry replication is atomic:** either all peers apply or none do
   (via rollback on failure).
6) **SPIFFE-only identity:** server identity is derived from SPIFFE URIs, not
   hostnames or SANs.
7) **No operator knobs:** membership, election, and RM replication behavior are
   deterministic and not gated by config flags.
8) **Bootstrap is mandatory:** if `--join` is provided, startup must contact at
   least one join endpoint or include self to satisfy the bootstrap check.
9) **Quorum is based on active membership:** leader quorum is computed from the
   current active membership list. Tests and operational procedures **must**
   wait for membership convergence before treating quorum loss as meaningful.

## Implementation Map (for auditing)

- Membership store: `internal/tccluster.Store`
- Leader election: `internal/tcleader.Manager`
- Membership HTTP endpoints: `internal/httpapi` (`/v1/tc/cluster/*`)
- RM registry store + replicator: `internal/tcrm`
- TC decision path: `internal/txncoord` + `internal/core`
- Server membership loop and fan-out: `server.go`
- Test scaffolding: `internal/archipelagotest`

## Test Expectations

- Unit tests should validate:
  - announce/list
  - self-only leave
  - crash/expiry behavior
  - quorum loss step-down
  - RM replication rollback on failure
- Unit tests should also cover:
  - non-leader forwarding success vs leader-unavailable failure
  - term fencing (stale term rejected)
- Integration tests must validate:
  - multi-island leader failover
  - quorum loss during renew
  - RM registry replication in failure and recovery
  - membership churn across backends
