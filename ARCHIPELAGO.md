# ARCHIPELAGO: Implicit XA Across Backend Islands (Global TC Leader)

This document defines the architecture and behavior for implicit XA across
backend "islands" without a shared TC backend. It focuses on XA flow and
developer UX. **Leadership, membership, and RM registry semantics are
authoritatively defined in `docs/LEADERELECTION.md`.** This file should not
duplicate or contradict that spec.

Status: XA flow implemented in code. Use this document to orient XA behavior
and keep it aligned with `docs/LEADERELECTION.md` for TC cluster mechanics.

## Goals

- **Implicit XA UX**: normal SDK operations (Acquire/Update/Release,
  Dequeue/Ack/Nack) automatically enlist participants and drive decisions.
  Developers never need to call `/v1/txn/decide` directly.
- **Proper XA semantics**: a single, authoritative TC leader records decisions
  and fans out apply calls to RMs; RMs enforce fencing so stale leaders are
  rejected.
- **No shared backend pool** for TC decisions or leader state.
- **Robust leadership**: quorum-based leader election with leases and term
  fencing; leader identity derived from the node's SPIFFE identity.
- **Transport-agnostic core**: TC decision/forwarding lives in core logic, not
  in the HTTP handler. This keeps the design usable for future transports.
- **Keep existing auth boundaries**: TC endpoints remain TC-only with mTLS.
- **TC+RM everywhere**: all lockd servers participate as both TC and RM; there
  is no role separation in this architecture.

## Non-goals

- External consensus systems (raft/etcd/etc.).
- Per-transaction leader election.
- Changing the public API surface beyond what is described here.

## Implementation Status (Jan 3, 2026)

Implemented and wired:
- TC leader election (quorum leases + term fencing) in `internal/tcleader`.
- TC cluster membership (`lockd tc announce|leave|list`, `--self`, `--join`) in
  `internal/tccluster` + server wiring.
- RM registry + replication across TC peers in `internal/tcrm`.
- Implicit XA decider + forwarding in `internal/txncoord` (core-driven).
- RM term enforcement + idempotent apply semantics in `internal/core/txn.go`.
- TC endpoints for lease/leader/decide and RM registry in `internal/httpapi`
  (TC-auth via mTLS).
- Docs/diagrams/README aligned with implicit TC flow.
Testing expectations:
- Unit coverage lives in `archipelago_unit_test.go` and TC/cluster unit suites.
- Integration coverage lives under `integration/*/archipelago_integration_test.go`.
- Normative behavior is defined in `docs/LEADERELECTION.md`.

## Components and Responsibilities

- **lockd RM (resource manager)**: normal server path for Acquire/Update/Release
  and queue operations. Applies decisions to local participants.
- **lockd TC**: a lockd server running TC endpoints (same binary). Any TC can
  accept `/v1/txn/decide`, but only the leader records decisions and fans out.
- **TC leader manager**: background quorum lease loop that decides who is leader
  and what the current `tc_term` is.
- **TC decider (core)**: transport-agnostic component that enforces leader
  routing, stamps `tc_term`, and calls the TC coordinator.
- **TC coordinator**: records decisions and fans out to RM endpoints.
- **TC cluster store**: persisted membership leases (`.lockd/tc-cluster/leases/<identity>`).
- **RM registry**: persisted RM endpoints by backend hash
  (`.lockd/tc-rm-members`).

## Terminology

- **Island**: a backend "island" identified by `backend_hash`. All nodes
  sharing the same backend hash are on the same island.
- **RM**: resource manager (lockd node applying decisions for local
  participants).
- **TC**: transaction coordinator (records decisions, drives fan-out).
- **Leader**: the single TC instance authorized to decide transactions.
- **Quorum**: majority of TC peers, `floor(N/2)+1`.
- **Lease**: time-bounded grant issued by a TC peer to the leader.
- **Term**: monotonically increasing fencing token for TC leadership.

## Data Records

- **Txn record** (per backend): stored under `.txns/<txn_id>`
  - `state` (`pending|commit|rollback`)
  - `participants` (namespace/key/backend_hash)
  - `tc_term`
  - `created_at_unix`, `updated_at_unix`, `expires_at_unix`
  - Each backend keeps its own copy; there is no shared `.txns` record across
    islands. The TC leader records its decision locally and RMs write/merge their
    local record when applying decisions.
- **Staged state**: stored under `/<ns>/state/<key>/.staging/<txn_id>` for
  Update/Remove flows (attachments under `/<ns>/state/<key>/.staging/<txn_id>/attachments/<uuidv7>`).
- **TC cluster membership**: `.lockd/tc-cluster/leases/<identity>` (lease record).
- **RM registry**: `.lockd/tc-rm-members` (endpoints grouped by backend_hash).
- **TC leader lease record**: in-memory per TC peer (leader_id, endpoint, term,
  expiry). This is not shared across backends.

## Configuration Overview

- `--self` (config `self`): this node's endpoint. Required for TC leadership
  and RM registration.
- `--join` (config `tc-join`): bootstrap endpoints only; never a static TC list.
- `--tc-client-bundle`: client bundle used when a TC talks to peers or RMs.
- `--tc-disable-auth`, `--tc-trust-dir`, `--tc-allow-default-ca`: TC/RM auth.

Guidance:
- `--self` must match the advertised endpoint for the node.
- If `--join` is configured, the node must successfully announce to at least
  one join target at startup or fail hard. Including the node's own endpoint in
  `--join` satisfies this check (useful for first-node bootstrap).
- If `--join` includes any non-self endpoints, mTLS must be enabled at startup.
- Cluster membership leases live under `.lockd/tc-cluster/leases/<identity>` and
  are refreshed automatically by each TC. `lockd tc announce|leave|list` are
  manual helpers.
- RM registrations are stored in `.lockd/tc-rm-members` and updated via
  `/v1/tc/rm/register` and `/v1/tc/rm/unregister` (server cert required when
  mTLS is enabled).
- If only a single TC is active, the node operates in **local-only mode** with
  quorum=1. Additional TCs join as their leases appear.

## TC Cluster Membership (Announce/Leave/List)

Membership leases are stored per backend under:

- namespace: `.lockd`
- key prefix: `tc-cluster/leases/<identity>`

Endpoints:

- `POST /v1/tc/cluster/announce` (refresh lease)
- `POST /v1/tc/cluster/leave` (delete self)
- `GET /v1/tc/cluster/list`

Expected flow (`lockd tc announce`):
1. Call `announce` on any reachable TC endpoint.
2. The TC stores a lease keyed by the caller's certificate identity.
3. Peers observe active endpoints by listing leases.

Leases expire automatically. `lockd tc leave` removes only the local lease and
suppresses auto-announcements on that node until it announces again (or
restarts). Membership TTL is derived from the leader lease TTL (currently 3x).

## RM Registry

RM endpoints are registered by backend hash in `.lockd/tc-rm-members`.

Endpoints:
- `POST /v1/tc/rm/register` (server certificate required when mTLS is enabled)
- `POST /v1/tc/rm/unregister`
- `GET /v1/tc/rm/list`

Only lockd servers (RMs) should call these endpoints. Client SDKs must not be
able to register RMs directly.

Because there is no shared backend, the RM registry must be replicated across
the TC cluster so any leader has a complete view. The expected behavior is:

1. TC receives `/v1/tc/rm/register` or `/v1/tc/rm/unregister`.
2. TC writes the update to its local backend (`.lockd/tc-rm-members`).
3. TC forwards the same update to every TC peer in the cluster.
4. If any peer update fails, return an explicit error (do not silently ignore).

This guarantees that a leader change does not lose RM endpoint knowledge.

## Leader Election (Quorum Leases)

### Leader identity

Leader identity is derived from the node’s SPIFFE identity
(`spiffe://lockd/server/<node-id>`). See `docs/LEADERELECTION.md` for the
normative identity and quorum rules.

### Term model

The leader uses a monotonically increasing **term** to fence decisions.

Term selection:
- Candidate queries peer state and computes `term = max(peer_terms) + 1`.
- Term is strictly increasing across leaders.
- Each peer persists its current term until a higher term is granted.

### Lease endpoints (TC-only)

- `POST /v1/tc/lease/acquire`
  - Request: `candidate_id`, `candidate_endpoint`, `term`, `ttl_ms`
  - Response: `granted`, `leader_id`, `leader_endpoint`, `term`, `expires_at`
  - Grant if:
    - No current lease or lease expired, and
    - `term > current_term` (or same term + same leader)
- `POST /v1/tc/lease/renew`
  - Request: `leader_id`, `term`, `ttl_ms`
  - Response: `renewed`, `leader_id`, `term`, `expires_at`
  - Renew only if leader/term match current lease
- `POST /v1/tc/lease/release`
  - Request: `leader_id`, `term`
  - Response: `released`
- `GET /v1/tc/leader`
  - Response: `leader_id`, `leader_endpoint`, `term`, `expires_at`

### Election algorithm

1. Candidate collects peer terms via `GET /v1/tc/leader`.
2. Candidate computes `term = max_term + 1`.
3. Candidate calls `lease/acquire` on all TC endpoints (including itself).
4. If **>= quorum** grants, it becomes leader.
5. If quorum not reached:
   - release acquired leases,
   - back off with jitter,
   - retry.
6. Leader renews leases periodically (`ttl/3` cadence).
7. If leader fails to renew quorum, it **steps down immediately**.
8. If a leader intentionally leaves the cluster (membership removal), it
   releases its leases across peers via `/v1/tc/lease/release` so a new leader
   can be elected immediately.

### Backoff and jitter

Use bounded exponential backoff with jitter to prevent herd oscillation.

## Implicit XA Flow (Core)

All implicit XA flows route through the core TC decider, which:
- determines the current leader,
- forwards decisions to the leader when needed,
- stamps `tc_term` for the leader,
- calls the TC coordinator to record decisions + fan-out.

This keeps the transport layer thin and consistent across HTTP, gRPC, etc.
Normal client SDK flows do not call `/v1/txn/decide`; that endpoint is reserved
for TC-aware tooling and internal RM/TC interactions.

### Participant registration

Triggered by any staged operation (Update/Remove/UpdateMetadata/attachments)
or queue dequeue with `txn_id`:

1. RM registers the participant locally in `.txns/<txn_id>` using CAS.
2. RM calls the TC decider with `state=pending` and the new participant.
3. TC leader merges participants into its local `.txns/<txn_id>` record.

If leader is unknown or unreachable, the operation fails with an explicit
error (no silent partial enlistment).

### Finalize (commit/rollback)

Triggered by `Release` or queue `Ack/Nack` with `txn_id`:

1. RM builds a decision record (`commit` or `rollback`) that includes the
   relevant local participants (e.g. message + optional state sidecar).
2. RM calls the TC decider.
3. TC leader records the decision, applies locally, and fans out to other
   islands using the RM registry.
4. RMs apply locally; idempotent on the same term/state.

Important: the RM that initiated the decision does **not** apply locally when
using the TC decider. Application is driven by the TC leader (local apply) and
fan-out to the relevant RMs.

### Queue-specific behavior

- Dequeue with `txn_id` enlists the message as a participant.
- DequeueWithState enlists both the message and state sidecar.
- Commit => ACK (delete message)
- Rollback => NACK (requeue message)
- Stale leases return `409 queue_message_lease_mismatch`.

### Local-only mode

Local-only mode is permitted only when no non-self `--join` endpoints are
configured (or when `--join` contains only self). In this case:

- Decisions are recorded and applied locally.
- No TC forwarding or fan-out occurs.
- `tc_term` is not required.

If all `--join` endpoints are unreachable at startup (and self is not included
in `--join`), the process fails hard (no local-only fallback).

## Fan-out Rules

Fan-out happens **only** for participants whose `backend_hash` differs from the
TC leader's local backend hash. Single-island transactions still go through the
leader for decision recording but skip fan-out entirely.

Fan-out relies on RM registry endpoints; if an RM for a backend hash is missing,
TC returns `502 txn_fanout_failed`.

## Forwarding Behavior

Any TC endpoint can receive `/v1/txn/decide`.

- If the endpoint is **leader**, it records the decision and proceeds.
- If the endpoint is **not leader**, it forwards to the leader.
- If forwarding fails, it returns `409 tc_not_leader` with `leader_endpoint` so
  callers can retry via alternate network paths.

## Term Fencing (RM Safety)

RMs must reject stale leaders:

- Each txn record stores `tc_term` (and optionally `tc_id` later).
- `/v1/txn/decide` and RM apply payloads include `tc_term`.
- On RM apply:
  - If `term < stored_term`: reject with `409 tc_term_stale`.
  - If `term == stored_term` and decision matches: idempotent success.
  - If `term == stored_term` but decision differs: reject with
    `409 txn_conflict`.
  - If no term exists: accept and store the term.

## Error Semantics (Representative)

- `409 tc_not_leader` (leader_endpoint provided)
- `503 tc_unavailable` (no quorum leader)
- `409 tc_term_stale`
- `400 tc_term_required` (when leader election is enabled)
- `409 txn_conflict`
- `409 txn_pending` (apply with no decision record)
- `409 txn_backend_mismatch`
- `409 queue_message_lease_mismatch`
- `502 txn_fanout_failed`

## Security and Auth

- **TC client endpoints** (`/v1/tc/leader`, `/v1/tc/lease/*`, `/v1/tc/cluster/list`,
  `/v1/txn/decide`, `/v1/tc/rm/list`) require a TC client certificate when mTLS
  is enabled.
- **TC server endpoints** (`/v1/tc/cluster/announce`, `/v1/tc/cluster/leave`,
  `/v1/tc/rm/register`, `/v1/tc/rm/unregister`) require a server certificate
  when mTLS is enabled.
- Normal client SDKs must not call TC endpoints directly.
- TC-to-TC and TC-to-RM calls use `--tc-client-bundle` and trust roots from
  `--tc-trust-dir` (or `--tc-allow-default-ca`).

## Observability

Emit logs and metrics for:

- Lease acquire/renew/release (tc_id, term, quorum size, duration)
- Leader transitions (elected, stepped down)
- `/v1/txn/decide` forwarding (leader endpoint, errors)
- RM apply rejects due to term mismatch
- Fan-out duration and failures per backend

## Operational Examples

### Single island, 3 TCs

- Start 3 lockd servers with `--self` and no `--join`.
- Announce them into a cluster using `lockd tc announce`.
- RM registry has only local backend endpoints.
- Leader elected via quorum; fan-out skipped.

### Two islands, 3 TCs

- Same TC cluster membership list.
- RMs register endpoints for both backend hashes.
- Leader decides and fans out to the RM for the other island.

## Failure Modes

- **No quorum**: `/v1/txn/decide` returns `503 tc_unavailable`.
- **Leader dies**: remaining TCs elect a new leader when quorum exists.
- **N=2**: quorum=2; losing one TC means no leader (safe, unavailable).
- **Partition**: only the partition with quorum can elect a leader.

## Follow-ups / Open Questions

- Should leader state be persisted or remain in-memory only?
- Should RM apply include both `tc_id` and `term`, or only `term`?
- How aggressive should TC retries be before returning an error to the caller?
