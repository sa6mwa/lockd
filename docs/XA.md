# Lockd XA Transactions

Lockd provides XA-style, two-phase transactions across keys and queue messages. Every lockd node acts as both a TC and an RM (resource manager). A single TC leader is elected via quorum leases; the leader records durable decisions and fans out apply requests to RMs.

This document is the current, user-facing behavior (not a roadmap).

## Scope and constraints

- **Multi-key, multi-namespace** transactions are supported.
- **Transaction IDs and lease IDs** are compact xid strings.
- **Staging layout**: staged state lives under `/<ns>/state/<key>/.staging/<txn-id>`;
  staged attachments live under `/<ns>/state/<key>/.staging/<txn-id>/attachments/<uuidv7>`.
- **Decision records** live under the reserved `.txns` namespace.
- **Backend islands** are identified by `backend_hash` (persisted under `.lockd/backend-id`).
- **Queue dequeue enlistment** is supported; the message (and optional state sidecar) become txn participants.
- **Queue enqueue is not XA-aware** yet (no `txn_id` on enqueue).
- **Namespaces starting with `.` are reserved** for lockd internals.

## Terminology

- **Participant**: a `(namespace, key)` pair, optionally with `backend_hash`.
- **TC**: the elected leader that records decisions in `.txns` and fans out to RMs (non-leaders forward).
- **RM**: applies commit/rollback for local participants.

## Transaction lifecycle (state keys)

1. **Acquire**: `Acquire` mints a `txn_id` if you do not supply one. Use the same `txn_id` to join multiple keys.
2. **Stage**: `Update`/`Remove`/`UpdateMetadata`/attachment operations with the `txn_id` write staged data and register participants.
3. **Decide**:
   - **Local flow**: `Release` with `rollback=false` commits; `rollback=true` rolls back.
   - **Coordinated flow**: the core decider records `pending|commit|rollback` via the TC leader and fans out to RMs (non-leader TC endpoints forward to the leader). Normal client SDK flows do not call `/v1/txn/decide` directly.
4. **Apply**: RMs promote staged state/attachments (commit) or discard staging (rollback), clear leases, and clean metadata.
5. **Recovery**: the sweeper replays committed/rolled back decisions and rolls back expired pending decisions. `/v1/txn/replay` forces replay.

## TC and RM endpoints

- **TC decision endpoint**: `POST /v1/txn/decide` (`state` = `pending|commit|rollback`). TC-auth only; called by lockd RMs and TC tooling. Any TC endpoint can receive it; non-leaders forward to the leader.
- **RM apply endpoints**: `POST /v1/txn/commit`, `POST /v1/txn/rollback` (include `tc_term`, the TC leader term).
- **Replay**: `POST /v1/txn/replay` applies the recorded decision again.

When mTLS is enabled (and `tc-disable-auth` is not set):

- **TC client endpoints** (`/v1/tc/leader`, `/v1/tc/lease/*`, `/v1/tc/cluster/list`,
  `/v1/tc/rm/list`, `/v1/txn/decide`) require a TC client cert.
- **TC server endpoints** (`/v1/tc/cluster/announce`, `/v1/tc/cluster/leave`,
  `/v1/tc/rm/register`, `/v1/tc/rm/unregister`) require a server cert.

Use the TC trust bundle commands (`lockd tc trust add`, `lockd tc trust list`)
to configure which CAs can call TC/RM endpoints.

`tc_term` is set by the TC leader; normal client SDK callers should omit it.

`/v1/txn/decide` merges participants with any existing txn record. If the TC does not share the backend with the participants, supply the full participant list (with `backend_hash`) so fan-out can target the correct RM. The TC leader stamps the decision with `tc_term`; RMs reject stale terms.

## Backend islands (multi-backend)

Each backend exposes a stable `backend_hash`. The TC groups participants by `backend_hash` and fans out apply requests to the corresponding RMs. RMs reject mismatched requests with `409 txn_backend_mismatch`.

If all participants share the same backend hash, fan-out is skipped after the local apply.

## TC configuration (cluster + fan-out + auth)

Core knobs (CLI flags shown; each has a `LOCKD_` env var equivalent via Viper):

- `--self` this node's endpoint (used for TC leadership and RM registration).
- `--join` (config key `tc-join`) bootstrap endpoints only; never a static TC list.
  If `--join` includes any non-self endpoints, mTLS must be enabled at startup.
- `--tc-fanout-timeout`, `--tc-fanout-attempts`, `--tc-fanout-base-delay`, `--tc-fanout-max-delay`, `--tc-fanout-multiplier` for retry behavior.
- `--tc-decision-retention` to keep decisions for late replays.
- `--tc-disable-auth` to disable TC/RM endpoint certificate enforcement (mTLS still applies).
- `--tc-trust-dir` and `--tc-allow-default-ca` to control which CAs are trusted for TC/RM endpoints (default CA is not trusted unless explicitly allowed).
- `--tc-client-bundle` for outbound fan-out mTLS when the TC calls remote RMs.

Cluster membership leases are stored under `.lockd/tc-cluster/leases/<identity>` and managed via:

- `lockd tc announce --self ... --endpoint ...`
- `lockd tc leave --endpoint ...`
- `lockd tc list --endpoint ...`

RM registrations are stored under `.lockd/tc-rm-members` and managed via `/v1/tc/rm/register` and `/v1/tc/rm/unregister` (server cert required when mTLS is enabled). Registry updates replicate across the TC cluster; replication failures return `502 tc_rm_replication_failed`. Ensure the TC trust includes the server CA or enable `--tc-allow-default-ca`.

Startup requirement: if `--join` is configured, the node must successfully
announce to at least one join target at startup or fail hard. Including the
node's own endpoint in `--join` satisfies this check.

## Queue enlistment (dequeue in XA)

When you call dequeue with a `txn_id`, lockd enlists the queue message as a participant. For `DequeueWithState`, the state sidecar is also enlisted.

Decision semantics:
- **Commit** => ACK (message removed), delete state sidecar (if stateful).
- **Rollback** => NACK (message becomes visible again), state sidecar retained.

Queue lease fencing is strict. Stale ACK/NACK/txn applies return `409 queue_message_lease_mismatch`.

## SDK examples

### Multi-key transaction (local)

```go
ctx := context.Background()
txn := xid.New().String()

leaseA, _ := cli.Acquire(ctx, api.AcquireRequest{
    Namespace:  namespaces.Default,
    Key:        "xa-a",
    Owner:      "worker-1",
    TTLSeconds: 30,
    TxnID:      txn,
})
leaseB, _ := cli.Acquire(ctx, api.AcquireRequest{
    Namespace:  "beta",
    Key:        "xa-b",
    Owner:      "worker-1",
    TTLSeconds: 30,
    TxnID:      txn,
})
_ = leaseA.Save(ctx, map[string]any{"status": "ready-a"})
_ = leaseB.Save(ctx, map[string]any{"status": "ready-b"})

// Commit the txn. Rollback by passing Rollback: true.
_ = leaseA.Release(ctx)
```

### Queue dequeue in a transaction

```go
opts := lockdclient.DequeueOptions{
    Namespace: namespaces.Default,
    Owner:     "worker-1",
    TxnID:     txn,
}
msg, _ := cli.Dequeue(ctx, "orders", opts)

// Work with the message, then ACK/NACK to commit/rollback.
// ACK/NACK triggers the TC decider when a txn_id is present.
```

If you are coordinating across multiple backends, pass the queue message participant explicitly: `q/<queue>/msg/<message_id>` (and `q/<queue>/state/<message_id>` for stateful dequeues) along with the correct `backend_hash`.

## TC tooling (TC-auth only)

### TC decision (multi-backend)

```go
parts := []api.TxnParticipant{
    {Namespace: "default", Key: "orders/alpha", BackendHash: "backend-a"},
    {Namespace: "default", Key: "orders/beta", BackendHash: "backend-b"},
}
resp, err := cli.TxnCommit(ctx, api.TxnDecisionRequest{TxnID: txn, Participants: parts})
_ = resp
_ = err
```

## CLI examples

### Multi-key transaction (local)

```sh
# Acquire key A (exports LOCKD_CLIENT_TXN_ID and lease info)
eval "$(lockd client acquire --key xa-a --ttl 30s)"
txn=$LOCKD_CLIENT_TXN_ID
leaseA=$LOCKD_CLIENT_LEASE
fenceA=$LOCKD_CLIENT_FENCING_TOKEN

# Join key B in the same txn
LOCKD_CLIENT_TXN_ID=$txn eval "$(lockd client acquire --key xa-b --ttl 30s)"
leaseB=$LOCKD_CLIENT_LEASE
fenceB=$LOCKD_CLIENT_FENCING_TOKEN

# Stage updates
printf '{"status":"ready"}' | lockd client update --key xa-a --lease "$leaseA" --fencing-token "$fenceA" --txn-id "$txn"
printf '{"status":"ready"}' | lockd client update --key xa-b --lease "$leaseB" --fencing-token "$fenceB" --txn-id "$txn"

# Commit (rollback with --rollback)
lockd client release --key xa-a --lease "$leaseA" --fencing-token "$fenceA" --txn-id "$txn"
```

### TC decision (TC-auth only)

```sh
lockd txn commit \
  --txn-id "$LOCKD_CLIENT_TXN_ID" \
  --participant default:orders/alpha@backend-a \
  --participant default:orders/beta@backend-b
```

Queue dequeue with `txn_id` is currently exposed via the SDK; the CLI does not yet accept a `--txn-id` for queue dequeue.

## Failure modes and recovery

- **Lease expiry**: pending transactions roll back when the lease TTL expires.
- **Pending decision**: RM apply without a decision record returns `409 txn_pending`.
- **Backend mismatch**: RM apply with a mismatched `target_backend_hash` returns `409 txn_backend_mismatch`.
- **Queue lease mismatch**: stale ACK/NACK or txn apply returns `409 queue_message_lease_mismatch`.
- **Fan-out failures**: TC returns `502 txn_fanout_failed` if remote apply fails. The decision is still recorded; retry or call `/v1/txn/replay` after fixing the RM endpoint.

## Diagrams

PlantUML sources live under `docs/diagrams/`:

- `xa-happy-path.puml` - multi-key commit on a single backend.
- `xa-rollback-timeout.puml` - rollback after lease/decision expiry.
- `xa-tc-rm-fanout.puml` - TC decision + RM fan-out across backends.
- `xa-queue-dequeue.puml` - queue dequeue enlisted in a transaction.
