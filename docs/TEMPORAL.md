# Lockd + Temporal CLI: feasibility and required work

Date: 2026-01-11

## Scope and assumptions

This document analyzes what it would take to replace the Temporal CLI dev-server persistence backend (currently SQLite) with lockd, and what would be required to build a Temporal-like workflow engine directly on top of lockd.

Primary focus: a lockd-backed persistence implementation for Temporal CLI dev server (approach 1). Secondary: an adapter that mimics SQLite semantics using lockd (approach 2). Also includes a separate analysis for a lockd-native workflow engine.

The analysis is based on the Temporal CLI repo and Temporal server v1.29.1 (the version the CLI currently depends on).

## Temporal CLI dev server wiring (what exists today)

The Temporal CLI dev server constructs a Temporal server config that is always SQL-backed and always uses the SQLite plugin:

- `internal/devserver/server.go` builds a `config.Config` where `Persistence.DefaultStore` and `VisibilityStore` are both set to `sqlite-default`.
- It builds a `config.SQL` with `PluginName: sqlite` and a file or memory DB, and applies SQLite schema migrations via `go.temporal.io/server/schema/sqlite` before server start.
- Namespaces are created pre-start using `schema/sqlite` helpers.
- The server is started via `temporal.NewServer(...)` with default module wiring.

This is important because any lockd backend option must either:

- Inject a **custom datastore factory** into Temporal’s DI wiring (preferred), or
- Replace the SQL plugin with a lockd-backed SQL-like adapter (fallback).

## Option A (preferred): custom datastore + visibility store

Temporal server supports custom datastores via an **AbstractDataStoreFactory** hook and a **CustomDatastoreConfig** in persistence config. The relevant control points are:

- `temporal.WithCustomDataStoreFactory(...)` (server option)
- `config.Persistence.DataStores[...].CustomDataStoreConfig`
- `temporal.WithCustomVisibilityStoreFactory(...)` (visibility store hook)

In other words: the dev server would need to be adjusted to pass a custom datastore factory and to emit a config that selects the custom store. That is feasible, but the required implementation is large.

### Interfaces required (core persistence)

The persistence surface you must implement is defined in `go.temporal.io/server/common/persistence`.

The **minimum** persistence implementation includes a DataStoreFactory and all stores below:

- `persistence.DataStoreFactory`
  - `NewTaskStore()`
  - `NewFairTaskStore()`
  - `NewShardStore()`
  - `NewMetadataStore()`
  - `NewExecutionStore()`
  - `NewQueue(queueType)`
  - `NewQueueV2()`
  - `NewClusterMetadataStore()`
  - `NewNexusEndpointStore()`

And the corresponding stores:

- `ShardStore`: shard record and rangeID ownership.
- `TaskStore`: task queues (v1 and v2), user data, fairness scheduling.
- `ExecutionStore`: workflow mutable state, history branching, history node storage, replication DLQ, scan/list of executions, etc.
- `MetadataStore`: namespaces + global metadata.
- `ClusterMetadataStore`: cluster info + membership records.
- `Queue`: legacy queue (namespace replication).
- `QueueV2`: general named FIFO queue, dynamic names.
- `NexusEndpointStore`: nexus endpoints (versioned records).

Additionally, **visibility** is a separate persistence layer with its own factory:

- `visibility.VisibilityStoreFactory` -> `VisibilityStore`
- `VisibilityStore` operations include:
  - Record started/closed
  - Upsert
  - Delete
  - List / Count / Get
  - AddSearchAttributes

### Required Temporal semantics (non-negotiable)

Temporal’s stores depend on strong invariants that a lockd backend must satisfy:

- **Conditional updates / optimistic concurrency**: many writes depend on expected `range_id`, `db_record_version`, `next_event_id`, etc. Conditional update failures must be surfaced with specific error types (e.g., `CurrentWorkflowConditionFailedError`).
- **History branching**: history is a tree (not a single log). You must support `AppendHistoryNodes`, `ForkHistoryBranch`, and `DeleteHistoryBranch` with causal ordering and ID ordering constraints.
- **Range scans by time or task ID**: for scheduled/transfer/timer/replication tasks, Temporal expects ordered reads and range deletes.
- **Queue V2**: named queues, pagination tokens, and partitioned message storage for long queues.
- **Visibility query**: support Temporal’s query language and search attributes (including predefined fields and custom types). SQLite visibility uses generated columns and FTS indexes; you need an equivalent capability.
- **Namespace metadata and global cluster metadata**: must persist across restarts, and be mergeable with config at startup.

### Mapping to lockd capabilities (what already fits)

Lockd already has several primitives that align well with Temporal’s persistence model:

- **Namespaced storage**: maps directly to Temporal’s namespace isolation.
- **Document + key storage with query/index**: a strong base for visibility and list operations.
- **Queue and streaming semantics**: maps to task queues and queue v2.
- **XA-style transactions**: can implement Temporal’s conditional updates and multi-record mutations.
- **Attachments**: can store large event histories or state blobs without bloating indexable documents.
- **Encryption**: aligns with Temporal’s expectation of encrypted persistence.
- **SDK/CLI**: already present, can be used for internal tooling and migrations.

### Likely gaps (what you would still need)

Based on Temporal’s persistence contract and the SQLite schema, the following capabilities are likely missing or would need extension in lockd or a lockd-based adapter:

- **High-cardinality range scans with strict ordering** (e.g., task queues by `task_id`, timers by `(timestamp, task_id)`), including pagination tokens.
- **Multi-entity conditional updates** with Temporal-specific failure semantics (e.g., `CurrentWorkflowConditionFailedError`, `ShardOwnershipLostError`).
- **History tree support** (branching, node ordering, fork semantics) as a first-class model.
- **Visibility query compatibility** with Temporal’s query language (filters, ordering, full-text search, list fields).
- **FTS-style search attributes** (e.g., KeywordList and Text types) with exact Temporal semantics.
- **Schema management / migrations** for persistence and visibility stores.
- **Operational compatibility** with Temporal’s persistence test suites (see `common/persistence/tests` in server repo).

### Minimal changes required in Temporal CLI dev server

To use a lockd backend without rewriting Temporal core, you would need to modify the dev server wiring in the CLI:

1) **Support passing a custom datastore factory** into the server startup (`temporal.WithCustomDataStoreFactory`).
2) **Set persistence config to custom datastore** instead of SQLite.
3) **Supply custom visibility store factory** if visibility is not handled by the same backend.
4) **Replace sqlite schema migration + namespace creation** with lockd equivalents.

This is all doable, but it is a significant change to the Temporal CLI (not just lockd). The majority of effort is implementing the data store and visibility interfaces.

### Expected implementation footprint

Even a “minimal” lockd-backed persistence implementation would likely include:

- A new module implementing `persistence.DataStoreFactory` and all store interfaces listed above.
- A lockd-backed visibility store implementation.
- Schema bootstrap and namespace creation logic.
- A compatibility layer for Temporal’s persistence test suites.
- CLI changes to choose the custom datastore and wire the factory into Temporal server startup.

## Option B (fallback): SQL-like adapter over lockd

The fallback approach is to implement a **SQL plugin** (or SQL driver) that presents Temporal’s SQL tables using lockd under the hood. This would allow the CLI dev server to keep the same SQLite-oriented flow.

Why this is less attractive:

- The SQL plugin surface is very large (`sqlplugin.TableCRUD`), covering all tables in the SQL schema.
- You’d still need to emulate SQL behaviors (indexes, ordering, constraints) to satisfy Temporal’s expectations.
- Schema migrations and DDL semantics would need to be simulated or no-op’d.

In short: this is effectively re-implementing a SQL engine on top of lockd’s storage API. It is likely more work and risk than a direct custom datastore.

## Summary: feasibility of lockd-backed Temporal persistence

- **Feasible but large**: the data store interfaces are extensive and highly coupled to Temporal’s internal state machine.
- **Risky**: subtle semantics around conditional updates, history branching, and visibility query compatibility are easy to get wrong.
- **Best path**: use the custom datastore interfaces in Temporal server, not a SQL adapter.
- **Effort**: expect multiple weeks of work to reach a stable dev-server backend, plus ongoing maintenance against Temporal’s internal interfaces.

## Critical reanalysis (risks + corrections)

This section corrects or emphasizes issues that can otherwise be underestimated.

- **Custom datastore is experimental in Temporal**: the `WithCustomDataStoreFactory` path is explicitly marked experimental and may change across Temporal versions. Expect rework on upgrades and build guardrails for version pinning and compatibility testing.
- **Error mapping is a correctness feature**: Temporal’s retry/metrics layers key off typed persistence errors. If lockd returns generic errors, the server can retry incorrectly or miss fatal conditions (e.g., `ShardOwnershipLostError`, `CurrentWorkflowConditionFailedError`). This is not optional.
- **Ordering guarantees are stronger than they appear**: task queues, timers, and history rely on strict ordering and stable pagination tokens. Any ambiguity in lockd’s ordering or pagination semantics will surface as data loss or infinite retries.
- **Visibility is a behavioral API, not just storage**: the SQLite visibility schema encodes Temporal’s query semantics (including list fields and full-text behavior). If lockd visibility does not match these semantics closely, CLI queries and SDK behavior will diverge.
- **UI depends on visibility**: the dev UI exercises list/query heavily. A partial visibility implementation will look “fine” at server boot but fail user expectations immediately.
- **History tree invariants are hard**: `AppendHistoryNodes`, `ForkHistoryBranch`, and `ReadHistoryBranch` must uphold temporal ordering and idempotency. This is the most error-prone persistence surface and should be proven early.
- **Lockd backend variability matters**: lockd supports multiple backends (disk, s3/minio, azure). Temporal persistence expects strong consistency. If any backend is eventually consistent, the workflow engine must include compensating logic (e.g., observed-key warmups, read-after-write fences).

## Detailed lockd mapping to Temporal persistence (Option A)

The goal here is to make the mapping concrete enough to estimate actual implementation scope.

### Core data model (suggested lockd namespaces)

- `temporal/system`: cluster metadata, namespace metadata, global defaults
- `temporal/history`: workflow mutable state, current execution, history tree
- `temporal/tasks`: task queues, matching, scheduled and immediate tasks
- `temporal/queues`: namespace replication queue + QueueV2 storage
- `temporal/visibility`: indexed docs for visibility + search attributes
- `temporal/nexus`: nexus endpoint storage

This can also be represented as a single namespace with key prefixes, but separating namespaces aligns with lockd’s existing isolation and cleanup semantics.

### ShardStore

Temporal’s shard record contains:

- RangeID (ownership/version)
- ShardInfo blob (serialized proto)

Lockd mapping:

- Key: `shard/{shard_id}`
- Value: serialized `ShardInfo` blob + RangeID metadata
- Operations:
  - `GetOrCreateShard`: conditional create if absent; return existing if present
  - `UpdateShard`: CAS on RangeID
  - `AssertShardOwnership`: check RangeID

Lockd primitives required:

- Conditional update on a single record.
- RangeID stored in document metadata (or value wrapper).

### MetadataStore (namespaces)

Temporal stores namespaces as records keyed by ID and name; also a global notification version.

Lockd mapping:

- Key: `namespace/id/{namespace_id}` -> NamespaceDetail blob
- Key: `namespace/name/{name}` -> pointer to ID
- Key: `namespace/meta` -> notification version

Operations:

- CreateNamespace: atomic write of id record, name record, and notification version bump.
- GetNamespace: by ID or by name.
- UpdateNamespace: conditional update by notification version or resource version.
- RenameNamespace: update name index, preserve ID record.

Lockd primitives required:

- Multi-record transaction (XA) with conditional checks.
- Secondary index by name (lockd query or explicit key index).

### ClusterMetadataStore

Cluster metadata includes: cluster info records, versioning, and membership entries.

Lockd mapping:

- Key: `cluster/{cluster_name}` -> ClusterInfo blob
- Key: `cluster/membership/{host_id}` -> membership record

Operations:

- List/Load cluster metadata records with pagination.
- Upsert membership with TTL/expiry semantics (Temporal expects stale records to be prunable).

Lockd primitives required:

- TTL or expiry fields (could be enforced by sweeper).
- Range list with pagination.

### TaskStore (matching)

Temporal task queues include:

- Task queues (metadata + ack levels)
- Tasks table (task data keyed by task_id)
- Fair task queues (v2)
- Task queue user data
- Build ID -> task queue mapping

Lockd mapping:

- Task queue metadata: `taskq/{namespace}/{task_queue}/{type}`
- Task items: `task/{namespace}/{task_queue}/{type}/{task_id}`
- Fair tasks: `taskv2/{namespace}/{task_queue}/{type}/{pass}/{task_id}`
- User data: `tq_userdata/{namespace}/{task_queue}`
- Build ID map: `tq_build/{namespace}/{build_id}/{task_queue}`

Operations:

- CreateTaskQueue: create metadata record with range_id
- Get/Update TaskQueue: CAS on range_id
- CreateTasks/GetTasks/CompleteTasksLessThan: ordered ranges by task_id
- ListTaskQueue: list keys by prefix
- UpdateTaskQueueUserData: CAS on version
- Build ID lookups: list by prefix

Lockd primitives required:

- Range scan by ordered key suffix (task_id)
- Prefix scans with pagination tokens
- Conditional updates by range_id/version

### ExecutionStore (workflows + history)

This is the largest surface. Required data includes:

- Executions table: mutable state, execution state, next_event_id, last_write_version, db_record_version
- Current execution table (workflow_id -> current run)
- Buffered events
- Activity/timer/child/signal/request_cancel maps
- Signals requested set
- Chasm node maps (newer versions)
- History tree + history nodes
- Replication tasks + DLQ

Lockd mapping (example):

- `exec/{shard}/{namespace}/{workflow_id}/{run_id}` -> execution mutable state
- `exec_current/{shard}/{namespace}/{workflow_id}` -> current run record
- `buffered/{shard}/{namespace}/{workflow_id}/{run_id}/{id}` -> buffered event
- `activity/{...}/{schedule_id}` -> activity map
- `timer/{...}/{timer_id}` -> timer map
- `child/{...}/{initiated_id}` -> child execution map
- `cancel/{...}/{initiated_id}` -> request cancel map
- `signal/{...}/{initiated_id}` -> signal map
- `signal_requested/{...}/{signal_id}` -> signal requested set
- `chasm/{...}/{path}` -> chasm nodes
- `history_tree/{shard}/{tree_id}/{branch_id}` -> history branch metadata
- `history_node/{shard}/{tree_id}/{branch_id}/{node_id}/{txn_id}` -> history node data
- `replication/{shard}/{task_id}` -> replication task
- `replication_dlq/{cluster}/{shard}/{task_id}` -> DLQ task

Operations require multi-record transactions with strong conditional checks:

- `CreateWorkflowExecution` is a transaction on:
  - current execution record
  - execution mutable state
  - history branch metadata
  - tasks to enqueue
- `UpdateWorkflowExecution` is a conditional update that must fail with `CurrentWorkflowConditionFailedError` or `WorkflowConditionFailedError` when appropriate.
- `ConflictResolveWorkflowExecution` handles zombie updates and bypass current.
- History tree operations must preserve ordering invariants.

Lockd primitives required:

- Transactions spanning multiple keys (XA semantics, already supported).
- Conditional updates on version/next_event_id.
- Range reads for history nodes.
- Efficient storage of large blobs (use attachments).

### Queue (legacy)

Used mainly for namespace replication queue.

Lockd mapping:

- `queue/{queue_type}/{message_id}` -> message
- `queue_meta/{queue_type}` -> ack levels + version
- `queue_dlq/{queue_type}/{message_id}` -> DLQ messages

Operations:

- Append and read by increasing message_id.
- Range delete before ack level.
- DLQ operations with pagination.

Lockd primitives required:

- Monotonic ID allocation (sequence or time-ordered IDs).
- Range deletion of keys <= id.

### QueueV2

Named queues with partitions.

Lockd mapping:

- `queuev2/{type}/{name}/meta` -> queue metadata
- `queuev2/{type}/{name}/p/{partition}/{message_id}` -> message

Operations:

- CreateQueue: fail if exists.
- EnqueueMessage: append with increasing message_id.
- ReadMessages: by page size + token (partitioned by offset).
- RangeDeleteMessages: delete <= id.
- ListQueues: list by prefix.

Lockd primitives required:

- Efficient list + pagination by prefix and ordered suffix.
- Stable next-page tokens.

### NexusEndpointStore

Versioned records with table-version semantics.

Lockd mapping:

- `nexus/endpoints/{id}` -> record + version
- `nexus/partition` -> table version

Operations:

- Create/update with version check.
- Delete, list with pagination.

## Draft implementation plan: Temporal CLI dev backend (lockd)

### Phase 0: design + test harness

- Build a standalone `temporal-lockd` module that compiles against Temporal server v1.29.1 interfaces.
- Add a small adapter layer that can run a subset of Temporal persistence tests (use Temporal’s persistence test suite where possible).
- Decide on key layout + document/attachment split for large blobs.

### Phase 1: minimal Temporal boot (no workflows)

- Implement `MetadataStore`, `ClusterMetadataStore`, `ShardStore` for server boot.
- Implement a minimal `VisibilityStore` that can accept writes and list with very basic filters (sufficient for startup).
- Modify CLI dev server to choose custom datastore + visibility store.
- Validate server boots and can register namespaces.

### Phase 2: workflow execution core

- Implement `ExecutionStore` core create/update/delete and history branch/node operations.
- Implement `TaskStore` (basic queueing, not fairness yet).
- Implement `Queue` (namespace replication queue).
- Add compatibility for Temporal’s unit tests around execution and history.

### Phase 3: advanced matching + queue v2

- Implement `TaskStore` fairness scheduling (v2 tasks table semantics).
- Implement `QueueV2` with named queues and pagination tokens.
- Integrate `HistoryTaskQueueManager` expectations.

### Phase 4: visibility parity

- Implement visibility query semantics matching Temporal query language.
- Add search attributes (predefined + custom) with index configuration.
- Validate list/search behavior with Temporal CLI commands.

### Phase 5: polish + operability

- Ensure error mapping to Temporal persistence errors.
- Performance profiling, compaction, and backpressure controls.
- Add migration/upgrade strategy.

## Lockd-native workflow engine (approach 1 + 3)

This section describes a lockd-native workflow engine that provides Temporal-like functionality while leveraging lockd’s strengths. The recommended baseline is:

- **Approach 1**: workflow engine is a Go service that uses lockd for persistence and queues.
- **Approach 3**: add a small workflow-core subsystem to lockd (state transition primitives + storage helpers) while keeping the orchestration logic outside.

This avoids deep coupling of workflow semantics inside lockd core while still leveraging lockd’s transaction and queue primitives.

### Design goals

- Single-binary deployment for lockd (core storage) remains intact.
- Workflow engine is a thin Go service or a Go BFF running alongside lockd.
- Deterministic workflow execution semantics (replayable history).
- Clear workflow model that maps cleanly to lockd’s namespaces, documents, and queues.
- A simple UI (vanilla JS) and SDK with excellent DX.

### System architecture (proposed)

**Services**

- `lockd`: storage + query + queue + XA transactions.
- `wf-engine`: workflow orchestration service (Go), handles decisions, history, timers.
- `wf-worker`: worker process (Go) running user code (SDK) and polling tasks.
- `wf-ui`: static JS frontend + lightweight Go BFF (or served by wf-engine).

**Data flow**

1) SDK starts workflow -> `wf-engine` writes initial state and history to lockd.
2) `wf-engine` enqueues workflow task on lockd queue.
3) Worker polls queue, replays history, executes workflow code, returns commands.
4) `wf-engine` applies commands as a transaction, appends history, schedules activities/timers.
5) Activities execute (external workers) and report results to `wf-engine`.

### Core data model (lockd)

Use lockd namespaces to organize data:

- `wf/meta`: namespaces, workflow types, system config
- `wf/executions`: workflow state + history index
- `wf/history`: history event blobs (attachments)
- `wf/tasks`: workflow tasks, activity tasks, timer tasks
- `wf/visibility`: indexed docs for UI/search
- `wf/queues`: task queue metadata

Suggested key layout:

- `exec/{namespace}/{workflow_id}/{run_id}`: workflow execution state (document)
- `exec_current/{namespace}/{workflow_id}`: pointer to current run_id
- `history/{namespace}/{workflow_id}/{run_id}/{event_id}`: history event blob (attachment)
- `task/wf/{namespace}/{task_queue}/{task_id}`: workflow task
- `task/act/{namespace}/{task_queue}/{task_id}`: activity task
- `task/timer/{namespace}/{fire_at}/{task_id}`: timer task (ordered by timestamp)
- `visibility/{namespace}/{workflow_id}/{run_id}`: visibility document

### Workflow execution state machine

Define an explicit state model:

- `RUNNING`, `COMPLETED`, `FAILED`, `CANCELED`, `TERMINATED`, `CONTINUED_AS_NEW`, `TIMED_OUT`

The execution state record contains:

- `namespace`, `workflow_id`, `run_id`
- `state`, `status`
- `start_time`, `close_time`, `execution_time`
- `task_queue`
- `next_event_id`
- `last_write_version` (optional, for future multi-cluster)
- `memo` + `search_attributes`
- `activity_state` (map)
- `timer_state` (map)
- `child_state` (map)
- `signals` (map or log)
- `pending_commands` (optional for optimistic updates)

Use lockd attachments for large fields:

- `history` events as attachments
- `workflow input` and `activity payloads` as attachments when large

### History model

- Append-only list of history events.
- Each event is immutable and stored as a separate attachment or in a chunked blob.
- The execution record stores `next_event_id` and `last_event_id`.
- Replay semantics: worker loads history by streaming attachments.

Event types (subset of Temporal):

- WorkflowExecutionStarted
- WorkflowTaskScheduled/Started/Completed
- ActivityTaskScheduled/Started/Completed/Failed/TimedOut
- TimerStarted/TimerFired
- WorkflowExecutionSignaled
- WorkflowExecutionCompleted/Failed/Canceled/Terminated
- WorkflowExecutionContinuedAsNew
- MarkerRecorded

### Task queues and leasing

- Task queues are lockd queues with visibility timeouts.
- Each task has a `lease_expiry` and `attempt` counter.
- Polling worker acquires lease (CAS on task record) and acknowledges completion.
- If lease expires, task returns to queue.

Implement three queue types:

- `workflow` tasks (decision tasks)
- `activity` tasks
- `timer` tasks (driven by a timer scanner or scheduled queue)

### Timers

Timers can be modeled as:

- `task/timer/{namespace}/{fire_at}/{id}` keys, scanned by range on `fire_at`.
- A timer scanner in `wf-engine` regularly queries due timers and enqueues workflow tasks.

Lockd’s query + ordered key scan should be used here; if not sufficient, introduce a dedicated timer queue with scheduling by timestamp.

### Concurrency control and transactions

Each workflow transition is an XA transaction that updates:

- `exec` record
- `history` events (append)
- `visibility` record
- tasks queue entries

Conditional checks:

- `exec.next_event_id` to prevent concurrent updates
- `exec.run_id` for current execution pointer

### Signals and queries

- Signals append to history and optionally mutate execution state.
- Queries execute against the current cached state in the worker (like Temporal).
- If query handler is not in-memory, worker can replay history.

### Activities

- Activities are tasks with retry policy, timeouts, and backoff.
- Activity results are appended as history events and stored in attachments.
- Retries are scheduled by `wf-engine` when failures occur.

### Continue-as-new

- Create new run_id with a new execution record.
- Current execution is marked `CONTINUED_AS_NEW` and links to new run_id.
- History is closed for old run; new run has fresh history.

### Visibility and search

Index a summary document in `wf/visibility`:

- Fields: namespace, workflow_id, run_id, workflow_type, start_time, close_time, status, task_queue, search_attributes, memo, parent ids
- Query semantics: align with lockd selector syntax, but document a mapping from Temporal-style queries if desired.
- Full-text search: use lockd search index with tokenization for text fields.

### SDK and developer experience

**SDK (Go first)**

- Define workflow function and activity function interfaces.
- Provide deterministic APIs (time, random, side effects, signals).
- Provide workflow context with access to history and state.
- Provide testing harness: in-memory lockd backend or stubbed engine.

**Worker model**

- Worker registers workflow + activity handlers.
- Worker polls tasks from lockd queues, executes code, posts results.
- Worker can maintain an in-memory cache of workflow states to speed queries.

**CLI**

- `wf start`, `wf signal`, `wf query`, `wf list`, `wf describe`.
- CLI uses lockd SDK directly or via wf-engine API.

### UI (vanilla JS)

Primary screens:

- Workflow list (filters: status, type, task queue, time range)
- Workflow details (history timeline, inputs/outputs, signals)
- Task queue dashboard (backlog size, active workers)
- Search attributes manager

The UI can be served as static assets by wf-engine, with a JSON API:

- `GET /api/workflows`
- `GET /api/workflows/{id}/{run_id}`
- `POST /api/workflows/{id}/signal`
- `POST /api/workflows/{id}/query`
- `GET /api/task-queues`

### Observability (minimal)

- Structured logs in `wf-engine` and `wf-worker` with consistent subsystem names.
- Metrics: workflow started/completed, task queue lag, activity retries, timer lag.
- Optional tracing hooks (future).

### Lockd “workflow core” primitives (approach 3)

To support approach 3, introduce small, reusable primitives in lockd core without embedding full workflow logic:

- **Versioned document writes**: helpers that wrap conditional writes and return typed errors.
- **Append-only log helpers**: efficient append + range read for history.
- **Ordered task set**: index type that supports ordered scans by `(timestamp, id)`.
- **Leased queue entries**: standardized lease + retry metadata.
- **Transaction helper**: multi-key atomic commit with expected versions.

These can be exposed as internal helpers to simplify wf-engine implementation, while keeping the engine logic outside lockd.

### Why not put full workflow logic in lockd?

- It couples lockd releases to workflow semantics, which will iterate fast.
- Workflow engines benefit from flexible SDK evolution and isolated failure domains.
- Keeping the logic in a separate service keeps lockd simpler and more stable.

### MVP scope (lockd-native engine)

**Phase 1: minimal workflow engine**

- Workflow start, completion, failure
- Workflow + activity task queues
- Timers and signals
- History replay
- Simple CLI and minimal UI

**Phase 2: robustness**

- Retries and backoff policies
- Query support
- Continue-as-new
- Search attributes with index

**Phase 3: DX + scale**

- Workflow versioning
- Sticky task queues
- Advanced visibility filters
- UI polish and metrics

## API sketches (wf-engine)

The wf-engine exposes a JSON/HTTP API (Go BFF), with stable request/response types. gRPC can be added later.

### Namespaces

- `POST /api/namespaces`
  - Request:
    - `name` (string)
    - `retention_days` (int)
    - `search_attributes` (map)
  - Response:
    - `namespace_id` (string)

- `GET /api/namespaces`
  - Response: list of namespaces

### Workflows

- `POST /api/workflows`
  - Request:
    - `namespace`
    - `workflow_id`
    - `workflow_type`
    - `task_queue`
    - `input` (bytes or JSON)
    - `memo` (map)
    - `search_attributes` (map)
    - `timeout` (duration)
    - `id_reuse_policy` (string)
  - Response:
    - `run_id`

- `GET /api/workflows`
  - Query params: `namespace`, `status`, `workflow_type`, `task_queue`, `start_time`, `end_time`, `query`, `page_size`, `page_token`
  - Response: list + next_page_token

- `GET /api/workflows/{workflow_id}/{run_id}`
  - Response: execution state + summary + history (optional)

- `POST /api/workflows/{workflow_id}/{run_id}/signal`
  - Request:
    - `signal_name`
    - `payload`
  - Response: ok

- `POST /api/workflows/{workflow_id}/{run_id}/query`
  - Request:
    - `query_name`
    - `payload`
  - Response: `result`

- `POST /api/workflows/{workflow_id}/{run_id}/terminate`
- `POST /api/workflows/{workflow_id}/{run_id}/cancel`

### Tasks

- `GET /api/tasks/next?queue={queue}&type=workflow|activity`
  - Response: task payload (lease granted)

- `POST /api/tasks/{task_id}/complete`
  - Request: result or workflow commands

- `POST /api/tasks/{task_id}/fail`
  - Request: failure info

### Visibility/search

- `POST /api/search-attributes`
  - Request: `{name: type}` map

- `GET /api/search-attributes`

### UI-support endpoints

- `GET /api/task-queues`
- `GET /api/metrics` (optional)

## Go SDK surface (proposed)

### Core types

- `Client`
- `WorkflowOptions`
- `ActivityOptions`
- `WorkflowContext`
- `ActivityContext`
- `WorkflowRun` (has `Get`, `GetID`, `GetRunID`)
- `Future` / `Promise`

### Workflow registration

- `worker.RegisterWorkflow(name, func(ctx WorkflowContext, input T) (R, error))`
- `worker.RegisterActivity(name, func(ctx ActivityContext, input T) (R, error))`

### Client API (examples)

```
type Client interface {
    StartWorkflow(ctx context.Context, opts WorkflowOptions, workflow any, input any) (WorkflowRun, error)
    SignalWorkflow(ctx context.Context, workflowID, runID, signal string, payload any) error
    QueryWorkflow(ctx context.Context, workflowID, runID, query string, payload any) (any, error)
    TerminateWorkflow(ctx context.Context, workflowID, runID, reason string) error
    CancelWorkflow(ctx context.Context, workflowID, runID string) error
    ListWorkflows(ctx context.Context, req ListWorkflowsRequest) (ListWorkflowsResponse, error)
}
```

### Deterministic workflow APIs

- `workflow.Now(ctx)`
- `workflow.Sleep(ctx, d)`
- `workflow.SideEffect(ctx, func() any) any`
- `workflow.Random(ctx)` (deterministic seed)
- `workflow.GetSignalChannel(ctx, name)`
- `workflow.SetQueryHandler(ctx, name, handler)`
- `workflow.ExecuteActivity(ctx, name, input)`
- `workflow.NewTimer(ctx, d)`
- `workflow.Select(ctx, ...)`

### Data conversion (determinism guardrail)

- Introduce a `DataConverter` interface with default JSON + optional binary encoding.
- Store payloads with `{encoding, data}` to ensure replays decode with the same codec.
- Version the converter settings in workflow start events to prevent drift.

### Example workflow

```
func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
    workflow.GetSignalChannel(ctx, "update").Receive(ctx, &name)
    if err := workflow.Sleep(ctx, time.Second); err != nil {
        return "", err
    }
    var result string
    if err := workflow.ExecuteActivity(ctx, "Greet", name).Get(ctx, &result); err != nil {
        return "", err
    }
    return result, nil
}
```

### Example activity

```
func Greet(ctx context.Context, name string) (string, error) {
    return "hello " + name, nil
}
```

### Worker

```
worker := wf.NewWorker(client, wf.WorkerOptions{TaskQueue: "default"})
worker.RegisterWorkflow("GreetingWorkflow", GreetingWorkflow)
worker.RegisterActivity("Greet", Greet)
err := worker.Run(ctx)
```

### Testing

- In-memory engine for unit tests.
- Replay tests: given history, ensure workflow deterministic.
- Worker test harness with fake clock.

## Temporal persistence implementation plan with estimates

Estimates are rough and assume a single experienced Go engineer with lockd context.

### Phase 0: architecture + test harness (1-2 weeks)

- Define key layout, blob strategies, and concurrency model.
- Build test harness to run Temporal persistence tests against lockd.

### Phase 1: boot-critical stores (2-3 weeks)

- ShardStore + MetadataStore + ClusterMetadataStore.
- Minimal VisibilityStore for namespace bootstrap.

### Phase 2: execution core + history (4-6 weeks)

- ExecutionStore with history tree + node handling.
- Conditional update semantics and error mapping.

### Phase 3: tasks + queues (3-5 weeks)

- TaskStore (v1 + v2) + Queue + QueueV2.
- HistoryTaskQueueManager compatibility.

### Phase 4: visibility parity (3-5 weeks)

- Search attributes (custom + predefined), filtering, ordering, pagination.
- Full-text or keyword list semantics.

### Phase 5: CLI wiring + stability (2-3 weeks)

- CLI dev-server config changes and migrations.
- Performance tuning, compaction, and documentation.

Total: ~15-24 weeks of focused engineering to reach a stable, comparable dev-server backend.
Note: this estimate assumes no foundational lockd changes; if ordering/pagination or visibility require core index work, expect the timeline to expand materially (often 2x).

## Updated recommendations and go/no-go criteria

### For a Temporal-compatible backend

Proceed only if the goal is Temporal compatibility, not “Temporal-like” workflows. Treat this as a long-lived integration with recurring maintenance costs.

Go/no-go checkpoints (in order):

1) **Ordering + pagination proof**: demonstrate stable, deterministic ordering for task queues, timer scans, and history reads with pagination tokens that survive restarts.
2) **Conditional update fidelity**: implement and validate error mapping for all persistence error types in `common/persistence` (especially shard ownership and workflow conditional failures).
3) **History tree compliance**: pass Temporal history-branch tests (`AppendHistoryNodes`, `ForkHistoryBranch`, `ReadHistoryBranch`, `DeleteHistoryBranch`) under concurrency.
4) **Visibility behavior parity**: implement a query subset that matches Temporal CLI and SDK expectations (status, workflow_id, workflow_type, time range, and search attributes).
5) **Persistence test suite**: pass the Temporal persistence tests for the lockd backend with no skipped tests.

If any checkpoint cannot be satisfied without invasive lockd changes, stop and reassess (likely redirecting to the lockd-native engine).

### For a lockd-native workflow engine

Proceed if the primary goal is developer experience and workflow semantics rather than Temporal wire-compatibility.

Recommended path:

1) **Engine outside lockd** (approach 1) with a minimal `workflowcore` helper layer (approach 3).
2) **Define a deterministic data converter** (JSON + optional binary; versioned) to prevent replay drift.
3) **Freeze the event model early** (history events are the API contract).
4) **Ship a small UI + SDK quickly** to validate DX and expose missing semantics.
5) **Add advanced features only after replay stability** (versioning, sticky queues, advanced queries).

### Near-term experiments (2-4 weeks total)

- Build a spike that implements lockd ordered scans + pagination tokens for task queues and timers.
- Prototype history append + replay with attachments and validate deterministic replays under concurrency.
- Implement a minimal visibility index and query path to validate selector performance at scale.

If these experiments are successful, the lockd-native engine becomes the lower-risk and faster delivery path. If any experiment fails, the Temporal backend is unlikely to succeed without major lockd changes.

## Internal data schema (lockd-native engine)

This is a concrete schema proposal for the lockd-native engine (approach 1 + 3). It assumes JSON documents for state/index and attachments for large blobs.

### Execution document (`exec/{namespace}/{workflow_id}/{run_id}`)

```
{
  "namespace": "string",
  "workflow_id": "string",
  "run_id": "string",
  "workflow_type": "string",
  "task_queue": "string",
  "state": "RUNNING|COMPLETED|FAILED|CANCELED|TERMINATED|CONTINUED_AS_NEW|TIMED_OUT",
  "status": "RUNNING|COMPLETED|FAILED|CANCELED|TERMINATED|CONTINUED_AS_NEW|TIMED_OUT",
  "start_time": "timestamp",
  "execution_time": "timestamp",
  "close_time": "timestamp|null",
  "next_event_id": 1,
  "last_event_id": 0,
  "last_heartbeat_time": "timestamp|null",
  "history_size_bytes": 0,
  "memo": {"k": "v"},
  "search_attributes": {"k": "v"},
  "parent": {"workflow_id": "string", "run_id": "string"},
  "root": {"workflow_id": "string", "run_id": "string"},
  "pending_activities": {
    "activity_id": {
      "name": "string",
      "schedule_event_id": 0,
      "attempt": 1,
      "state": "SCHEDULED|STARTED|COMPLETED|FAILED|TIMED_OUT|CANCELED",
      "timeout": "duration",
      "heartbeat_timeout": "duration",
      "last_heartbeat": "timestamp|null",
      "retry_policy": {"max_attempts": 0, "backoff": "duration"}
    }
  },
  "pending_timers": {
    "timer_id": {"fire_at": "timestamp", "started_event_id": 0}
  },
  "pending_signals": {
    "signal_id": {"name": "string", "event_id": 0}
  },
  "pending_child_workflows": {
    "child_id": {"workflow_id": "string", "run_id": "string", "state": "OPEN|CLOSED"}
  },
  "version": 0
}
```

### Current execution pointer (`exec_current/{namespace}/{workflow_id}`)

```
{
  "run_id": "string",
  "state": "RUNNING|CLOSED",
  "start_time": "timestamp",
  "last_write_version": 0,
  "version": 0
}
```

### History event blob (`history/{namespace}/{workflow_id}/{run_id}/{event_id}`)

Stored as attachment. Metadata may include:

```
{
  "event_id": 1,
  "event_type": "WorkflowExecutionStarted|...",
  "timestamp": "timestamp",
  "attributes_encoding": "json|proto",
  "attributes_size": 1234
}
```

### Workflow task (`task/wf/{namespace}/{task_queue}/{task_id}`)

```
{
  "task_id": 0,
  "namespace": "string",
  "workflow_id": "string",
  "run_id": "string",
  "scheduled_event_id": 0,
  "attempt": 1,
  "lease_expires_at": "timestamp|null",
  "timeout": "duration",
  "priority": 0,
  "version": 0
}
```

### Activity task (`task/act/{namespace}/{task_queue}/{task_id}`)

```
{
  "task_id": 0,
  "namespace": "string",
  "workflow_id": "string",
  "run_id": "string",
  "activity_id": "string",
  "activity_name": "string",
  "scheduled_event_id": 0,
  "attempt": 1,
  "lease_expires_at": "timestamp|null",
  "timeout": "duration",
  "heartbeat_timeout": "duration",
  "version": 0
}
```

### Timer task (`task/timer/{namespace}/{fire_at}/{task_id}`)

```
{
  "task_id": 0,
  "namespace": "string",
  "workflow_id": "string",
  "run_id": "string",
  "timer_id": "string",
  "fire_at": "timestamp",
  "version": 0
}
```

### Visibility document (`visibility/{namespace}/{workflow_id}/{run_id}`)

```
{
  "namespace": "string",
  "workflow_id": "string",
  "run_id": "string",
  "workflow_type": "string",
  "task_queue": "string",
  "status": "RUNNING|COMPLETED|FAILED|CANCELED|TERMINATED|CONTINUED_AS_NEW|TIMED_OUT",
  "start_time": "timestamp",
  "execution_time": "timestamp",
  "close_time": "timestamp|null",
  "history_length": 0,
  "history_size_bytes": 0,
  "memo": {"k": "v"},
  "search_attributes": {"k": "v"},
  "parent_workflow_id": "string",
  "parent_run_id": "string",
  "root_workflow_id": "string",
  "root_run_id": "string"
}
```

## State transition table (commands -> history -> mutations)

This table anchors deterministic workflow replay. The worker emits commands, and wf-engine persists them with history events.

### Workflow start

- Command: `StartWorkflow`
- History:
  - `WorkflowExecutionStarted`
  - `WorkflowTaskScheduled`
- Mutations:
  - Create execution doc
  - Create current pointer
  - Enqueue workflow task
  - Create visibility doc

### Workflow task completion

- Command: `CompleteWorkflowTask(commands...)`
- History:
  - `WorkflowTaskCompleted`
  - One event per command (see below)
  - `WorkflowTaskScheduled` (if more work)
- Mutations:
  - Apply workflow commands
  - Append history events
  - Schedule tasks/timers/activities

### Activity schedule

- Command: `ScheduleActivity`
- History:
  - `ActivityTaskScheduled`
- Mutations:
  - Insert activity task
  - Update execution pending_activities

### Activity start

- Command: `StartActivity`
- History:
  - `ActivityTaskStarted`
- Mutations:
  - Mark activity as started
  - Set lease and attempt

### Activity completion

- Command: `CompleteActivity`
- History:
  - `ActivityTaskCompleted`
- Mutations:
  - Remove pending activity
  - Enqueue workflow task

### Activity failure/timeout

- Command: `FailActivity` or `TimeoutActivity`
- History:
  - `ActivityTaskFailed` or `ActivityTaskTimedOut`
- Mutations:
  - Retry if policy allows (schedule new activity task)
  - Otherwise mark workflow failure or continue

### Timer start

- Command: `StartTimer`
- History:
  - `TimerStarted`
- Mutations:
  - Insert timer task

### Timer fired

- Command: `FireTimer`
- History:
  - `TimerFired`
- Mutations:
  - Remove timer
  - Enqueue workflow task

### Signal received

- Command: `SignalWorkflow`
- History:
  - `WorkflowExecutionSignaled`
- Mutations:
  - Record pending signal
  - Enqueue workflow task

### Complete workflow

- Command: `CompleteWorkflow`
- History:
  - `WorkflowExecutionCompleted`
- Mutations:
  - Set execution state closed
  - Update visibility close_time/status
  - Delete current pointer

### Fail workflow

- Command: `FailWorkflow`
- History:
  - `WorkflowExecutionFailed`
- Mutations:
  - Set execution state closed
  - Update visibility
  - Delete current pointer

### Cancel/terminate workflow

- Command: `CancelWorkflow` / `TerminateWorkflow`
- History:
  - `WorkflowExecutionCanceled` / `WorkflowExecutionTerminated`
- Mutations:
  - Set execution state closed
  - Update visibility
  - Delete current pointer

### Continue as new

- Command: `ContinueAsNew`
- History:
  - `WorkflowExecutionContinuedAsNew`
  - New run: `WorkflowExecutionStarted`
- Mutations:
  - Close old run
  - Create new execution + current pointer
  - Enqueue workflow task for new run

## Minimal UI spec (vanilla JS)

### Routes

- `/` -> workflow list
- `/workflow/:workflowId/:runId` -> workflow details
- `/task-queues` -> task queue dashboard
- `/namespaces` -> namespace list

### Components

- Workflow list table with filters (status, type, task queue, time range)
- Workflow detail header (ids, status, timestamps)
- History timeline (virtualized list)
- Payload viewer (expandable JSON)
- Signals panel (send signal)
- Query panel (execute query)
- Task queue list (backlog, active workers)

### API payload examples

`GET /api/workflows` response:

```
{
  "executions": [{
    "workflow_id": "wf-1",
    "run_id": "r-1",
    "workflow_type": "GreetingWorkflow",
    "status": "RUNNING",
    "start_time": "2026-01-11T00:00:00Z",
    "task_queue": "default"
  }],
  "next_page_token": "..."
}
```

`GET /api/workflows/{id}/{run_id}` response:

```
{
  "execution": {
    "workflow_id": "wf-1",
    "run_id": "r-1",
    "status": "RUNNING",
    "start_time": "...",
    "task_queue": "default"
  },
  "history": [{
    "event_id": 1,
    "event_type": "WorkflowExecutionStarted",
    "timestamp": "...",
    "attributes": {"input": "..."}
  }]
}
```

## Protobuf/Go struct draft

This is a sketch of core structs (Go) and their protobuf equivalents. The intent is to keep storage payloads stable and versioned.

### Go structs (engine-level)

```
package wf

type WorkflowExecution struct {
    Namespace         string
    WorkflowID        string
    RunID             string
    WorkflowType      string
    TaskQueue         string
    State             WorkflowState
    Status            WorkflowStatus
    StartTime         time.Time
    ExecutionTime     time.Time
    CloseTime         *time.Time
    NextEventID       int64
    LastEventID       int64
    HistorySizeBytes  int64
    Memo              map[string]any
    SearchAttributes  map[string]any
    Parent            *WorkflowRef
    Root              *WorkflowRef
    PendingActivities map[string]ActivityState
    PendingTimers     map[string]TimerState
    PendingSignals    map[string]SignalState
    PendingChildren   map[string]ChildState
    Version           int64
}

type WorkflowRef struct {
    WorkflowID string
    RunID      string
}

type ActivityState struct {
    Name            string
    ScheduleEventID int64
    Attempt         int32
    State           ActivityStatus
    Timeout         time.Duration
    HeartbeatTimeout time.Duration
    LastHeartbeat   *time.Time
    RetryPolicy     RetryPolicy
}

type TimerState struct {
    TimerID        string
    FireAt         time.Time
    StartedEventID int64
}

type SignalState struct {
    SignalID string
    Name     string
    EventID  int64
}

type ChildState struct {
    WorkflowID string
    RunID      string
    State      string
}
```

### Protobuf sketch

```
message WorkflowExecution {
  string namespace = 1;
  string workflow_id = 2;
  string run_id = 3;
  string workflow_type = 4;
  string task_queue = 5;
  string state = 6;
  string status = 7;
  int64 start_time_unix_nanos = 8;
  int64 execution_time_unix_nanos = 9;
  int64 close_time_unix_nanos = 10;
  int64 next_event_id = 11;
  int64 last_event_id = 12;
  int64 history_size_bytes = 13;
  map<string, bytes> memo = 14;
  map<string, bytes> search_attributes = 15;
  WorkflowRef parent = 16;
  WorkflowRef root = 17;
  map<string, ActivityState> pending_activities = 18;
  map<string, TimerState> pending_timers = 19;
  map<string, SignalState> pending_signals = 20;
  map<string, ChildState> pending_children = 21;
  int64 version = 22;
}
```

## Lockd workflow-core helper APIs (approach 3)

These APIs live in `internal/workflowcore` and are used by `wf-engine`.

### Versioned document helpers

```
// VersionedPut writes a document if the expected version matches.
func VersionedPut(ctx context.Context, ns, key string, expectedVersion int64, doc any) error

// VersionedGet returns the document and its version.
func VersionedGet(ctx context.Context, ns, key string, out any) (version int64, err error)
```

### Append-only history helpers

```
// AppendHistory appends an event and returns the new event id.
func AppendHistory(ctx context.Context, ns, execKey string, event any) (eventID int64, err error)

// ReadHistory streams history from eventID to end.
func ReadHistory(ctx context.Context, ns, execKey string, fromEventID int64) (iter EventIterator, err error)
```

### Ordered task set helpers

```
// EnqueueTask inserts a task ordered by (timestamp, id).
func EnqueueTask(ctx context.Context, ns, queue string, ts time.Time, taskID int64, payload any) error

// DequeueTasks reads due tasks ordered by (timestamp, id).
func DequeueTasks(ctx context.Context, ns, queue string, before time.Time, limit int) ([]TaskItem, error)
```

### Lease helpers

```
// AcquireLease sets a lease on a task if unleased or expired.
func AcquireLease(ctx context.Context, ns, key string, leaseUntil time.Time, token string) (bool, error)

// CompleteLease removes a task if the lease token matches.
func CompleteLease(ctx context.Context, ns, key string, token string) error
```

### Transaction helper

```
// Txn executes a multi-key transaction with conditional checks.
func Txn(ctx context.Context, ops []TxnOp) error
```

## Package layout sketch

```
/cmd/wf-engine
/cmd/wf-worker
/cmd/wf-ui

/wf/engine
  - orchestration, task dispatch, timers
/wf/worker
  - polling + execution
/wf/sdk
  - client API, workflow DSL
/wf/sdk/internal/replay
  - deterministic replay logic
/wf/sdk/internal/commands
  - workflow commands + event mapping
/wf/storage
  - lockd client abstraction

/internal/workflowcore
  - lockd workflow primitives

/ui
  - vanilla JS app
  - static assets
```
