# Lockd Queue (LQ) Architecture

Lockd Queue (LQ) layers an at-least-once message queue on top of the existing
lock + state primitives. This document captures the moving parts, how they fit
together, and the execution flow from enqueue to acknowledgement.

## Overview

```
┌──────────────┐      HTTP/JSON      ┌────────────┐      Storage Backend
│ Queue Client │ <-----------------> │  lockd API │ <----------------────┐
│   (Go/CLI)   │                     │  /v1/queue │                      │
└──────────────┘                     └─────┬──────┘                      │
                                           │                             │
                                           ▼                             │
                                 ┌──────────────────┐                    │
                                 │ Queue Dispatcher │                    │
                                 │  (per server)    │                    │
                                 └──────┬───────────┘                    │
                                        │                                │
                                        ▼                                │
                              ┌────────────────────┐                     │
                              │ queue.Service      │                     │
                              │  (storage facade)  │                     │
                              └────────────────────┘                     │
                                        │                                │
                                        ▼                                │
                              ┌────────────────────┐                     │
                              │ storage.Backend    │<──────┬─────────────┘
                              │ (disk/s3/azure/…)  │       │
                              └────────────────────┘       │
                                        ▲                 │
                                        │                 │
                                 ┌─────────────┐          │
                                 │ Queue Watch │──────────┘
                                 │ (optional)  │  file-system events
                                 └─────────────┘
```

At a high level:

1. Clients enqueue JSON metadata + optional binary payload via HTTP.
2. The queue service writes metadata (`q/<queue>/msg/<id>.pb`) and payload
   (`q/<queue>/msg/<id>.bin`) to the configured storage backend using CAS.
3. Consumers call `/v1/queue/dequeue`, `/v1/queue/dequeueWithState`, or the
   streaming variants `/v1/queue/subscribe` and `/v1/queue/subscribeWithState`.
   The API does **not** list storage directly; instead it blocks on the dispatcher.
4. The dispatcher is a per-server background component that watches for new
   messages (either via storage polling or disk inotify) and delivers
   descriptors to waiting consumers exactly once.
5. The handler acquires leases, increments attempts, fetches payload, and
   returns a response to the client. Ack/Nack/Extend go back through the
   queue service and release leases.

## Storage Layout

All queue artefacts live under a single prefix for each queue:

```
q/<queue>/msg/<message_id>.pb    metadata document
q/<queue>/msg/<message_id>.bin     payload blob (optional, may be empty)
q/<queue>/state/<message_id>.json  workflow state (DequeueWithState)
q/<queue>/dlq/msg/<message_id>.pb    dead-letter metadata
q/<queue>/dlq/msg/<message_id>.bin     dead-letter payload
q/<queue>/dlq/state/<message_id>.json  dead-letter workflow state
```

### Metadata document (`queue.Service`)

`internal/queue/service.go` performs all storage operations. Key duties:

- `Enqueue` generates a UUIDv7, streams payload to storage (conditional create),
  and writes a metadata document with CAS (`PutObject` + `If-None-Match`).
- `NextCandidate` paginates `ListObjects` over metadata files, filtering based on
  `not_visible_until`, `attempts < max_attempts`, TTL, etc. It surfaces a single
  ready descriptor and cursor for the next scan.
- `IncrementAttempts`, `Reschedule`, and `ExtendVisibility` mutate the metadata
  document via CAS (If-Match on ETag) to bump attempts or adjust visibility.
- `EnsureStateExists`, `LoadState`, `Ack`, `Nack`, and `MoveToDLQ` manage the
  state sidecar during stateless/stateful dequeues.
- S3/Azure/disk share the same logic; the service simply translates operations
  into the backend interface.

## Dispatcher Mechanics

`internal/queue/dispatcher.go` contains the scheduler that prevents every
consumer from hammering storage independently. Each lockd process owns one
dispatcher instance.

### Data structures

- **queueState** (per queue)
  - `waiters`: FIFO queue of consumers currently blocked on that queue.
  - `pending`: descriptors already fetched but not yet delivered (e.g. when a
    waiter cancels).
  - `cursor`: `ListObjects` position for the next storage scan.
  - `watchSub`: optional change subscription (fsnotify via disk backend).
  - `watchDisabled`: tracks whether the watcher reported `ErrNotImplemented`.

- **Scheduler loop**
  - There is a single goroutine (`runScheduler`) per process.
  - It wakes when new waiters arrive (`Wait` enqueues a waiter and nudges the
    scheduler) or when a change notification fires.
  - Each tick iterates over queueState instances that still have waiters and
    invokes `poll()` to fetch candidates.
  - `poll()` repeatedly calls `NextCandidate` until either all waiters are
    satisfied or storage signals no ready messages.

### Event flow

```
client Dequeue --> HTTP handler --> dispatcher.Wait(queue)
                                         |
  waiter appended -----------------------┘
                                         |
                                         v
                                  scheduler wakes
                                         |
                                         v
                         queueState.poll() -> service.NextCandidate()
                                         |
                                    Candidate
                                         |
                                         v
                                  waiter.deliver()
```

- `Wait` blocks on either `waiter.result` (candidate), `waiter.err`, or context
  cancellation. If a candidate was already in `pending`, it is delivered
  immediately with zero storage calls.
- Each create/update/delete on the storage backend can notify the dispatcher:
  - S3/Azure/other backends rely on periodic polling (`queuePollInterval` +
    jitter).
  - Disk (Linux, non-NFS) delivers fsnotify events, triggering an immediate
    scheduler wake without waiting for the next interval.
- Consumer cap (`queueMaxConsumers`) guards against resource exhaustion per
  process.

## HTTP → Dispatcher Wiring

`internal/httpapi/handler.go` is the façade that drives the queue mechanics:

1. `handleQueueDequeue` / `handleQueueDequeueWithState`
2. `handleQueueSubscribe` / `handleQueueSubscribeWithState`
   - Parse request body (owner, visibility override, optional wait seconds).
   - Wrap context with user-specified wait timeout if provided.
   - Call `consumeQueue`, which delegates to the dispatcher and then to
     `prepareQueueDelivery`.

3. `consumeQueue`
   - Repeatedly calls `dispatcher.Wait()` until it either gets a candidate or
     times out. If `prepareQueueDelivery` returns a retryable condition (lease
     conflict, metadata CAS mismatch, payload missing), the dispatcher is nudged
     and the loop continues.

4. `prepareQueueDelivery`
   - Acquires a lease for the message key (`q/<queue>/msg/<id>`).
   - Calls `queue.Service.IncrementAttempts` to CAS-update attempts and
     `not_visible_until`.
   - Opens a streaming reader for the payload and bundles it with the response
     metadata so the HTTP handler can emit a multipart response without
     buffering.
   - For stateful consumers, ensures a workflow state doc exists and acquires a
     companion lease (`q/<queue>/state/<id>`).
   - Returns the assembled delivery (metadata + reader) to the handler.

5. `/v1/queue/ack`, `/v1/queue/nack`, `/v1/queue/extend`
   - Validate lease ownership using meta CAS.
   - Validate the message metadata fence (`lease_id`, `lease_txn_id`, `lease_fencing_token`); stale leases return `409 queue_message_lease_mismatch`.
   - Call `queue.Service` helpers to update metadata or remove payload/state.
   - Release leases and notify dispatcher as appropriate (e.g. requeue after
     nack).

## Change Notifications (Disk Backend)

`internal/storage/disk/queue_watch_linux.go` implements optional inotify
integration:

- Enabled when `disk.Config.QueueWatch` is true (default) **and** the filesystem
  is not NFS (`statfs` check against `nfsSuperMagic`).
- `SubscribeQueueChanges` attaches an `fsnotify.Watcher` to the queue’s message
  directory. Any file create, write, or remove pushes a signal onto a buffered
  channel.
- The dispatcher wraps the subscription via `queue.WatchFactoryFromStorage`.
  Watchers are instantiated lazily when the first waiter arrives for a queue,
  and shut down when the last waiter leaves. If the watcher returns
  `storage.ErrNotImplemented` (unsupported platform), the dispatcher falls back
  to polling and marks the queue as “watch disabled”.

When watch is available, polling is reduced but not eliminated: the dispatcher
still runs a safety poll on `queue-resilient-poll-interval` to recover from
missed events. For other backends (S3/Azure/minio/disk without watch), regular
polling remains in place.

## Configuration Knobs

| Config / Flag | Purpose | Default |
|---------------|---------|---------|
| `QueueMaxConsumers` (`--queue-max-consumers`) | Caps concurrent dispatcher waiters per server to avoid resource pressure. | Auto (≈64 × CPUs, clamped between 128–4096) |
| `QueuePollInterval` (`--queue-poll-interval`) | Base interval between storage scans when no watcher events fire. | 3s |
| `QueuePollJitter` (`--queue-poll-jitter`) | Random spread added to the interval to desynchronise multiple servers. Set to 0 to disable. | 500ms |
| `QueueResilientPollInterval` (`--queue-resilient-poll-interval`) | Safety poll interval when watchers are active. Set to 0 to disable. | 5m |
| `DiskQueueWatch` (`--disk-queue-watch`) | Enables inotify watcher on Linux disk backend. Automatically ignored on unsupported filesystems (e.g. NFS). | true |
| `DisableMemQueueWatch` (`--disable-mem-queue-watch`) | Disables in-process notifications for the memory backend. Set to true to force pure polling (useful for regression tests). | false |

All flags have `LOCKD_*` environment-variable equivalents via Viper.

## Correlation IDs

Every enqueued message receives a correlation identifier. If the client does
not supply a normalized `X-Correlation-Id`, the server generates one during
enqueue and returns it in the HTTP response (`correlation_id` on
`EnqueueResponse` and the `X-Correlation-Id` header). The identifier
travels with the message for its entire lifecycle:

- `QueueMessage.correlation_id` is included on every dequeue, including
  stateful leases, and the response header mirrors the value.
- `QueueAck`, `QueueNack`, and `QueueExtend` responses echo the same identifier
  so clients can continue to tag follow-up calls.
- Workflow state created via `DequeueWithState` inherits the message correlation
  ID, ensuring subsequent acquire/extend calls carry the same trace attributes.
- Moves to the dead-letter queue preserve the correlation identifier.

Clients should forward the value on subsequent operations so logs, telemetry,
and retry loops stay linked to the originating enqueue.

## Client / CLI Interaction

### Go Client (`client.Dequeue`, etc.)

- Uses `requestContextNoTimeout` so dequeue/ack/nack/extend requests can hold
  open connections indefinitely while waiting for the dispatcher.
- `Dequeue` returns a `QueueMessage`, which implements `io.ReadCloser`. The
  message streams the payload and exposes helpers such as `WritePayloadTo`,
  `DecodePayloadJSON`, `Ack`, `Nack`, and (for stateful dequeues) `StateHandle`.
  If the caller forgets to ack or nack explicitly, `Close()` automatically
  issues a nack with the optional `OnCloseDelay` from `DequeueOptions`.
- `QueueAck`, `QueueNack`, and `QueueExtend` remain available on the client for
  tooling that operates purely on exported metadata.
- `StartConsumer` provides a worker-runner abstraction over
  `Subscribe` and `SubscribeWithState`. It runs one goroutine per
  `ConsumerConfig`, auto-generates `Options.Owner` when empty, applies
  restart backoff via `ConsumerRestartPolicy`, and routes panics in handlers
  and lifecycle hooks through the same restart/error path.

### CLI (`lockd client queue …`)

- `lockd client queue dequeue <name>` blocks until a message is
  available or the command is cancelled. Successful runs export
  `LOCKD_QUEUE_*` variables for downstream commands (`ack`, `nack`, `extend`).
  The payload is streamed to disk (temp file unless `--payload-out` is set) and
  exposed via `LOCKD_QUEUE_PAYLOAD_PATH`.
- The CLI honours the same `--visibility`, `--block`, `--stateful`, and
  `--owner` options as the Go client, ensuring parity.

## Internal Communication Summary

| Producer | Consumer | Channel | Purpose |
|----------|----------|---------|---------|
| HTTP handler | Queue dispatcher | `Wait(queue)` call | Block until there is a candidate for the queue. |
| Dispatcher scheduler | Queue service | `NextCandidate` | Enumerate storage to find ready messages. |
| Queue service | Storage backend | `ListObjects`, `PutObject`, `DeleteObject`, etc. | Persist metadata/payload and enforce CAS. |
| Storage backend (disk) | Dispatcher | `QueueChangeSubscription.Events` | Notify about file changes to minimise polling. |
| Dispatcher | HTTP handler | `Candidate` | Signals a specific message is ready to lease. |
| Handler | Queue service | `IncrementAttempts`, `Ack`, `Nack`, `ExtendVisibility` | Update message metadata and state. |
| Handler | Storage backend | `StoreMeta`, `EnsureStateExists`, `GetPayload` | Lease orchestration and payload streaming. |
| Queue handler | Dispatcher | `Notify(queue)` | Wake waiters after enqueue, ack (removal), or zero-delay nack. |

## Failure Handling Notes

- If `prepareQueueDelivery` hits a lease conflict or CAS mismatch, it returns
  `(nil, retry=true)`. The handler re-notifies the dispatcher to ensure the next
  waiter attempt runs promptly.
- Disk watchers automatically deactivate if an error other than EOF occurs;
  the dispatcher then reverts to polling for that queue only.
- If a watcher emits `ErrNotImplemented`, `queueState.watchDisabled` prevents
  future watcher attempts for that queue, avoiding noisy log spam.
- `QueueAck` and `QueueNack` release leases even when delete/update fails,
  bubbling the storage error so the client can retry with fresh metadata.

## Future Extensions

- Exposing queue-level policy documents (`q/<queue>/policy.json`) to override
  defaults such as visibility timeout and max attempts.
- Fan-out of change notifications for non-disk backends (SQS/SNS, object store
  events) via a pluggable watcher interface.

This snapshot reflects the current state of the implementation; revisit as LQ
evolves.
