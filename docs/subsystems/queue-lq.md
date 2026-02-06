# Queue (LQ) Subsystem

## 1) Purpose

The queue subsystem provides at-least-once delivery with visibility timeouts, dead-letter handling, optional stateful dequeue, and centralized dispatcher polling/watch integration to reduce backend load.

## 2) Architecture and implementation details

- Core queue data model is in `internal/queue/service.go`:
  - metadata objects per message
  - payload objects
  - optional state objects (`dequeueWithState` flow)
- Dispatching:
  - `internal/queue/dispatcher.go` centralizes polling/watch events by namespace+queue.
  - multiple consumers share a single queue state machine and pending candidate buffer.
- Core orchestration:
  - `internal/core/queuefacade.go` and `internal/core/queuedelivery.go` integrate dispatcher with lease/fencing/transaction semantics.
  - dequeue can apply queued XA decisions before leasing messages.
- Delivery lifecycle:
  - dequeue -> lease message (and optional state lease) -> ack/nack/extend via `internal/core/queueack.go`.
  - max-attempts overflow moves message to DLQ (`MoveToDLQ`).
- Change feed support:
  - queue dispatcher can attach backend watch subscriptions when available; otherwise it polls.

## 3) Non-style improvements (bugs, security, reliability)

- Batch dequeue is effectively disabled by implementation bug:
  - `consumeQueueBatch` in `internal/core/queuedelivery.go` forces `pageSize` down to `1` when `pageSize > 1`.
  - this defeats multi-message dequeue behavior and can regress throughput.
  - fix: remove forced clamp and keep bounded fill behavior.
- Nil-descriptor panic risk in dispatcher fetch paths:
  - `internal/queue/dispatcher.go` dereferences `result.Descriptor` in `fetch` and `fetchWithContext` without a hard nil guard.
  - a nil descriptor from provider/mocks can crash queue consumers.
  - fix: guard nil descriptors before dereference and treat as no-candidate.
- Cancellation is not propagated consistently:
  - `fetch()` and readiness checks call queue service with `context.Background()`.
  - shutdown/caller cancellation is ignored in those paths, increasing tail latency and hanging risk.
  - fix: propagate caller/scheduler contexts end-to-end for fetch/readiness calls.
- Streaming encryption goroutine leak risk:
  - `preparePayloadReader` and `copyObject` use `io.Pipe` + goroutines.
  - if downstream write exits early, writer goroutines can block on pipe writes.
  - fix: cancellation-aware copy and guaranteed pipe shutdown on all consumer error paths.

## Feature-aligned improvements

- Add a queue-consumer diagnostics endpoint exposing dispatcher queue stats and watch mode per queue to speed production triage.
