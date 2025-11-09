// Package client provides the Go SDK for talking to a lockd cluster over HTTP.
// It mirrors the CLI behaviour while exposing a type-safe API that is easy to
// embed in workers, controllers, and administrative tools.
//
// Copyright (C) 2025 Michel Blomgren <https://pkt.systems>
//
// # Quick start
//
// Construct a client with either `client.New` (single endpoint) or
// `client.NewWithEndpoints`. The URL scheme decides the transport:
//
//   - https://host:9341 – mTLS (default for production)
//   - http://host:9341 – plaintext for trusted networks or local testing
//   - unix:///path/to/lockd.sock – Unix-domain sockets when the server listens on a local socket
//
// A minimal loop that acquires a lease, reads state, updates, and releases looks
// like:
//
//	ctx := context.Background()
//	cli, err := client.New("https://lockd.example.com")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer cli.Close()
//
//	req := api.AcquireRequest{
//	    Namespace: "workflows", // omit to use the server's default namespace
//	    Key:        "orders",
//	    Owner:      "worker-1",
//	    TTLSeconds: 30,
//	    BlockSecs:  client.BlockWaitForever,
//	}
//	lease, err := cli.Acquire(ctx, req)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer lease.Close()
//
//	var checkpoint map[string]any
//	if err := lease.Load(ctx, &checkpoint); err != nil && !errors.Is(err, client.ErrStateNotFound) {
//	    log.Fatal(err)
//	}
//	// mutate checkpoint...
//	if err := lease.Save(ctx, checkpoint); err != nil {
//	    log.Fatal(err)
//	}
//
// When most operations live in a single namespace, wrap the client with
// client.WithDefaultNamespace("workflows") so explicit Namespace fields can be
// omitted without relying on the server fallback. The CLI mirrors this via
// --namespace / LOCKD_CLIENT_NAMESPACE for leases and LOCKD_QUEUE_NAMESPACE for
// queue flows.
//
// The lease session tracks fencing tokens automatically; the same `Client`
// instance should be reused for subsequent operations so KeepAlive, Get, Update,
// Release, and Remove calls carry the correct `X-Fencing-Token`. When the
// server enters its drain window it sets a `Shutdown-Imminent` header; the Go
// SDK notices this and, by default, releases leases after in-flight work
// completes. You can disable the behaviour via `client.WithDrainAwareShutdown`
// or the CLI flag `--drain-aware-shutdown=false` if your workflow prefers to
// hold leases until the next session.
//
// # Acquire-for-update
//
// `AcquireForUpdate` bundles the typical acquire → get → handler → release flow
// and keeps the lease alive in the background while your handler runs:
//
//	err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
//	    Namespace: "workflows",
//	    Key:        "orders",
//	    Owner:      "worker-1",
//	    TTLSeconds: 45,
//	}, func(ctx context.Context, af *client.AcquireForUpdateContext) error {
//	    var state map[string]any
//	    if err := af.Load(ctx, &state); err != nil && !errors.Is(err, client.ErrStateNotFound) {
//	        return err
//	    }
//	    state["progress"] = "done"
//	    return af.Save(ctx, state)
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// The helper keeps the lease alive on a background goroutine, retries the
// initial acquire/get handshake with bounded backoff, and always releases the
// lease—even when the handler returns an error. The context provided to the
// handler is cancelled if keepalives fail or the lease is lost, so long-running
// work can react quickly.
//
// # Perimeter defence interoperability
//
// Lockd’s perimeter defence pairs a Local Security Force (LSF) sampler with a
// Quick Reaction Force (QRF) controller. The server may respond with HTTP 429
// when the QRF throttles traffic. Responses include a `Retry-After` header
// (seconds) and an `X-Lockd-QRF-State` marker (`disengaged`, `soft_arm`,
// `engaged`, or `recovery`). The Go SDK honours these hints automatically during
// acquire, dequeue, and other retry loops. The parsed values are exposed on
// `*client.APIError` (`RetryAfter`, `QRFState`) for custom workflows, and the
// CLI exports `LOCKD_CLIENT_RETRY_AFTER` when throttled so shell scripts can
// respect the back-off. The default server posture enforces host memory
// budgets (80 % soft / 90 % hard) and load-based multipliers (4× soft / 8×
// hard); queue/lock inflight caps remain disabled unless configured.
//
// # Queue API
//
// The queue helpers expose lockd's at-least-once message delivery primitives.
// Enqueue a payload by providing a queue name and optional delivery controls.
// Any `io.Reader` can be supplied; the SDK streams data directly to the server:
//
//	reader := strings.NewReader(`{"op":"ship"}`)
//	qres, err := cli.Enqueue(ctx, "orders", reader, client.EnqueueOptions{
//		ContentType: "application/json",
//		Delay:       2 * time.Second,
//		Visibility:  30 * time.Second,
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	log.Printf("queued message %s", qres.MessageID)
//
// Workers call `Dequeue` (stateless) or `DequeueWithState` (message +
// workflow state) with their owner identity. The returned handle wraps the
// leased message, exposes a streaming payload reader, and tracks the fencing
// tokens required for Ack/Nack/Extend:
//
//	msg, err := cli.Dequeue(ctx, "orders", client.DequeueOptions{
//	    Namespace: "workflows",
//	    Owner:      "worker-1",
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer msg.Close()
//	buf, err := io.ReadAll(msg)
//	if err != nil {
//		log.Fatal(err)
//	}
//	log.Printf("attempt %d payload=%s", msg.Attempts(), string(buf))
//	// ...do work...
//	if err := msg.Ack(ctx); err != nil {
//		log.Fatal(err)
//	}
//
// The returned message implements `io.ReadCloser` and exposes `Nack` (with
// optional delay), `Extend`, `WritePayloadTo`, and a `StateHandle` when using
// `DequeueWithState`. The latter mirrors the message APIs and keeps the
// workflow state lease in sync when extending or acknowledging the message. If
// a caller returns without acking explicitly, the deferred `Close` issues a
// nack so another worker can resume. Tune the behaviour via
// `DequeueOptions.OnCloseDelay` or `msg.SetOnCloseDelay`. Control how long the
// client waits for messages with `DequeueOptions.BlockSeconds`
// (`client.BlockNoWait` for immediate return, positive values for bounded
// polling).
//
// For higher throughput, `Subscribe` / `SubscribeWithState` keep a single
// streaming request open and invoke a user-supplied handler for every delivery.
// The handler is responsible for Ack/Nack just like the one-shot helpers, but
// avoids per-message HTTP setup and reuses the underlying connection.
//
// Direct request-style helpers (`Client.QueueAck`, `QueueNack`, `QueueExtend`)
// are also available when metadata is sourced from environment variables or
// other transports.
//
// # State removal
//
// Lease holders can delete JSON state explicitly via
// `(*LeaseSession).Remove` or `Client.Remove`. CAS headers (If-ETag,
// If-Version) are supported to guard against concurrent, stale deletes. When the
// state is deleted the server also clears the metadata entry so a subsequent
// acquire sees a clean slate.
//
// # Metadata attributes and query visibility
//
// Every key has a metadata document that stores lease details, versions, and
// user-controlled attributes. The server exposes `POST /v1/metadata` so callers
// can toggle attributes—such as hiding a key from `/v1/query`—without uploading
// a new JSON blob. The Go SDK layers ergonomic helpers on top:
//
//	lease, err := cli.Acquire(ctx, api.AcquireRequest{Key: "orders", TTLSeconds: 30})
//	// ...
//	if _, err := lease.UpdateMetadata(ctx, client.MetadataOptions{
//	    QueryHidden: client.Bool(true),
//	}); err != nil {
//	    log.Fatal(err)
//	}
//
// The variadic `Update`/`Save` helpers accept `client.WithQueryHidden()` /
// `client.WithQueryVisible()` options when you want metadata mutations to ride
// along with a state write:
//
//	if err := lease.Save(ctx, checkpoint, client.WithQueryHidden()); err != nil {
//	    log.Fatal(err)
//	}
//
// Advanced workflows can pass `client.WithMetadata(...)` directly to `Update`
// for multiple attributes or for forward compatibility with future metadata
// fields. Low-level access lives on `Client.UpdateMetadata` which returns a
// `MetadataResult` so tools can inspect the server’s synthesized attributes.
// All metadata mutations honor CAS headers (`X-If-Version`) and the handler
// echoes the effective attributes in the JSON response.
//
// # Multi-endpoint failover
//
// Builders that point the client at multiple endpoints get deterministic
// failover semantics:
//
//	cli, err := client.NewWithEndpoints([]string{
//	    "https://lockd-primary.example.com",
//	    "https://lockd-backup.example.com",
//	}, client.WithDisableMTLS(false))
//
// The SDK rotates through endpoints when transport errors occur and keeps the
// same bounded retry budget as the CLI so tests remain deterministic. When
// using mTLS, ensure all endpoints present certificates signed by the same CA.
//
// # Correlation IDs and logging
//
// Use `client.WithCorrelationID` or `client.GenerateCorrelationID` to tag a
// context. The client automatically propagates the correlation identifier on
// subsequent requests (and logs) so server- and client-side traces can be tied
// together. Alternatively, register a logger that implements
// `pslog.Base` when constructing the client to capture structured
// traces emitted by the SDK.
// Queue workflows surface the identifier via `QueueMessage.CorrelationID`, and
// the helpers (`Ack`, `Nack`, `Extend`) forward it automatically so DLQ moves
// and stateful leases stay linked to the originating enqueue.
//
// # In-process testing
//
// The `client/inprocess` package spins up a lockd server in-process (MTLS
// disabled, Unix sockets) and returns a ready-to-use client facade. It is
// perfect for unit tests and local experiments:
//
//	ctx := context.Background()
//	inproc, err := inprocess.New(ctx, lockd.Config{Store: "mem://"})
//	if err != nil {
//	    t.Fatal(err)
//	}
//	defer inproc.Close(ctx)
//
//	lease, err := inproc.Acquire(ctx, api.AcquireRequest{Owner: "test", TTLSeconds: 10})
//	// ...
//
// # Authentication and mTLS
//
// By default the client expects mTLS and uses the CA found in the bundle passed
// to `client.New`. To connect over plaintext HTTP, construct the client with
// `client.WithDisableMTLS(true)` or by using an `http://` endpoint. Clients connecting
// over mTLS must present certificates with the `ClientAuth` extended key usage
// signed by the same CA as the server certificate.
package client
