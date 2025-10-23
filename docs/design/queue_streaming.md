# Queue Streaming Dequeue Prototype

## Purpose

Benchmarks for the in-process `mem://` backend show that even with batching
enabled we still pay a substantial per-request cost. Every batch requires a new
HTTP round trip and the socket layer spends most of its time waiting in
`accept`/`read`.  The dispatcher is able to supply candidates quickly, but the
transport chatter dominates end-to-end latency.  We need a dequeue path that
keeps a request open and streams deliveries as they become available so we can
measure the real throughput ceiling of the queue service.

## Proposed API sketch

_Status:_ the initial streaming implementation now lives under
`/v1/queue/subscribe` and `/v1/queue/subscribe-with-state`, with the Go SDK
exposing `Client.Subscribe` / `Client.SubscribeWithState`. The outline below
records the original design intent for future refinement.

### Client-side

Expose a new helper that accepts a user-supplied handler instead of returning a
single `QueueMessageHandle`:

```go
type ConsumeOptions struct {
        Queue      string
        Visibility time.Duration
        Prefetch   int           // number of messages to keep in flight
        Block      time.Duration // 0 == wait forever, <0 == no wait
}

type MessageHandler func(context.Context, *QueueMessageHandle) error

func (c *Client) Consume(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error
```

The client maintains a single long-lived HTTP request per server connection
(HTTP/2 if available).  Messages are read as a framed JSON stream and delivered
to the handler sequentially.  The handler is responsible for ack/nack.  A helper
`ConsumeOne` wraps this for CLI-style synchronous use.

### Server-side

Add a streaming endpoint (`POST /v1/queue/consume`) that:

1. Performs the same auth/owner bookkeeping as `/v1/queue/dequeue`.
2. Enters a loop where it writes one JSON envelope per delivery separated by
   newlines.  Each envelope includes the message metadata, optional payload
   descriptor, and a cursor token.
3. Flushes after every delivery to keep latency low.
4. Terminates when the client cancels, the visibility timeout expires, or the
   service detects too many consecutive send failures.

The existing dispatcher and ready cache are reused; the loop calls a new helper
`queue.Service.ConsumeStream` that yields candidates on demand.  Flow control is
driven by the `Prefetch` setting: the server attempts to stay `Prefetch`
deliveries ahead of the client while respecting the queue’s visibility rules.

## Incremental rollout plan

1. **Instrumentation** (done in this change): timing hooks in `consumeQueue*`
   and HTTP tracing on the client to observe reuse behaviour during benchmarks.
2. **Prototype transport**: build the streaming HTTP handler and a lightweight
   client harness guarded by an experiment build tag so we can measure gains
   without destabilising the existing SDK.
3. **Public API polish**: once satisfied with the performance, graduate the new
   client API and deprecate the current single-message `Dequeue` helper
   (retaining `DequeueOne` for CLI workflows).
4. **Backends**: validate the streaming path across `mem://`, disk, MinIO, AWS,
   and Azure to ensure no backend-specific quirks appear.
5. **Documentation and tests**: extend the integration suites with streaming
   coverage (notify + polling) and update README/SDK docs accordingly.

This staged approach lets us gather data quickly while keeping a rollback path
open if a backend behaves unexpectedly.  The instrumentation added in steps 1
and 2 will tell us whether the streaming prototype removes the current
socket-level bottleneck before investing in the full API surface.
