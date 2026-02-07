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

## 3) Implemented non-style improvements (bugs, security, reliability)

- Batch dequeue behavior was restored:
  - `consumeQueueBatch` no longer clamps `pageSize > 1` down to `1`.
  - bounded fill heuristics now apply to actual requested batch sizes.
- Dispatcher fetch safety was hardened:
  - nil descriptor results are guarded before dereference.
  - fetch paths handle context cancellation/deadline errors explicitly instead of treating them as generic failures.
- Context propagation in polling/fetch paths was tightened:
  - dispatcher polling now fetches with a poll context derived from active waiters when available.
  - readiness checks in fetch paths run under the same call context.
- Queue encrypted stream copy paths are now cancellation-aware:
  - `startEncryptedPipe` in queue service ensures producer shutdown on cancellation/consumer close.
  - regression guards were added in `internal/queue/service_pipe_test.go`.

## Feature-aligned improvements

- Add a queue-consumer diagnostics endpoint exposing dispatcher queue stats and watch mode per queue to speed production triage.
