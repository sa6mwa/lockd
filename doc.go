// Package lockd exposes the Go APIs behind the single-binary coordination
// service that delivers exclusive leases, atomic JSON state, and an
// at-least-once queue. The server is designed to run cleanly as PID 1, but the
// package also makes it easy to embed the server, spin up sidecars, or talk to
// lockd from Go clients.
//
// Copyright (C) 2025 Michel Blomgren <https://pkt.systems>
//
// # Running a server
//
// The server listens on the network specified by `Config.ListenProto` (default
// `tcp`) and address `Config.Listen`. Mutual TLS is enabled by default.
//
//	cfg := lockd.Config{
//	    Store:       "s3://locks/prod",
//	    Listen:      ":9341",
//	    ListenProto: "tcp",
//	    BundlePath:  "/etc/lockd/server.pem",
//	    DefaultNamespace: "analytics",
//	}
//	srv, err := lockd.NewServer(cfg)
//	if err != nil { log.Fatal(err) }
//	go func() {
//	    if err := srv.Start(); err != nil {
//	        log.Fatalf("lockd: %v", err)
//	    }
//	}()
//	defer func() {
//	    if err := srv.Shutdown(context.Background()); err != nil {
//	        log.Printf("lockd shutdown: %v", err)
//	    }
//	}()
//
// Namespaces partition keys and metadata. When callers omit the namespace, the
// server falls back to `Config.DefaultNamespace` (default "default"). Setting
// the field on `Config`, providing `Namespace` in `api.AcquireRequest`, or
// configuring clients via `client.WithDefaultNamespace` keeps each workload’s
// state isolated under its own prefix.
//
// # Unix domain sockets
//
// For same-host sidecars you can serve over a Unix socket by setting
// `ListenProto` to "unix". Clean-up is automatic and mTLS can be disabled when
// the connection never leaves the machine.
//
//	cfg := lockd.Config{
//	    Store:        "mem://",
//	    ListenProto:  "unix",
//	    Listen:       "/var/run/lockd.sock",
//	    DisableMTLS:  true,
//	}
//	srv, stop, err := lockd.StartServer(ctx, cfg)
//	if err != nil { log.Fatal(err) }
//	defer stop(context.Background())
//
// # Client SDK
//
// The Go client (`pkt.systems/lockd/client`) wraps the HTTP API. The base URL
// decides the transport:
//
//   - `https://host:9341` – production mTLS connection (default)
//   - `http://host:9341` – plain HTTP for trusted networks or local testing
//   - `unix:///path/to/lockd.sock` – Unix-domain sockets (requires `DisableMTLS`
//     or supplying a client bundle)
//
// Example:
//
//	cli, err := client.New("https://lockd.example.com")
//	if err != nil { log.Fatal(err) }
//	sess, err := cli.Acquire(ctx, api.AcquireRequest{
//	    Namespace: "orders-v2",
//	    Key:        "orders",
//	    Owner:      "worker-1",
//	    TTLSeconds: 30,
//	    BlockSecs:  client.BlockWaitForever,
//	})
//	if err != nil { log.Fatal(err) }
//	defer sess.Close()
//	var state struct {
//	    Hello   string
//	    World   string
//	    Counter int
//	}
//	if err := sess.Load(ctx, &state); err != nil { log.Fatal(err) }
//	state.Counter++
//	if err := sess.Save(ctx, state); err != nil { log.Fatal(err) }
//
// The client tracks fencing tokens automatically; reusing the same `Client`
// instance ensures follow-up KeepAlive/Get/Update/Release calls include the
// freshest `X-Fencing-Token`. For multi-process flows the CLI exports the token
// via `LOCKD_CLIENT_FENCING_TOKEN`, and your program can register it manually
// with `Client.RegisterLeaseToken`.
//
// Shutdown sequencing is controlled via `Config.DrainGrace` and
// `Config.ShutdownTimeout`. Draining gives existing lease holders time to wrap
// up (default 10 s) before the HTTP server begins closing connections; the
// timeout caps the combined drain + HTTP shutdown interval (default 10 s). The
// defaults mirror the CLI flags (`--drain-grace`, `--shutdown-timeout`) and can
// be disabled by setting them to `0`. Each server splits the budget 80/20 so a
// 10 s timeout reserves ~8 s for draining and ~2 s for `http.Server.Shutdown`.
//
// The Go client and CLI are drain-aware by default: when the server emits the
// `Shutdown-Imminent` header, the SDK auto-releases leases once in-flight work
// is done. Opt out by using `client.WithDrainAwareShutdown(false)` or passing
// `--drain-aware-shutdown=false` / `LOCKD_CLIENT_DRAIN_AWARE=false` to the CLI.
//
// Multi-host deployments can construct the client with multiple base URLs via
// `client.NewWithEndpoints([]string{...})`. The SDK rotates through the
// provided endpoints on failure, carrying the same bounded retry budget so that
// reacquire attempts remain deterministic even when the primary server drops
// mid-request.
//
// # Acquire-for-update workflow
//
// `AcquireForUpdate` wraps the usual acquire → get → update → release cycle in
// a single helper. Callers supply a function that receives an
// `AcquireForUpdateContext`; the context exposes the current `StateSnapshot`
// (so the handler can stream or decode the JSON) and forwards convenience
// methods like `Update`/`UpdateBytes`/`Save`/`Remove`. The helper keeps the lease alive
// in the background while the handler executes and always releases the lease
// when the handler returns. Both `LeaseSession` and `AcquireForUpdateContext`
// expose `Remove` helpers that delete the JSON blob while holding the lease.
// They honour the same conditional headers (`X-If-Version`, `X-If-State-ETag`)
// as updates and clear the server-side metadata when the delete succeeds.
//
// Handshake failures (for example `lease_required` during the initial
// `Get`) consume the same bounded retry budget controlled by
// `client.WithAcquireFailureRetries` and `client.WithAcquireBackoff`. Once the
// handler starts running, further errors are surfaced immediately—the typical
// pattern is to return the error (acquire-for-update propagates it) and decide
// whether to retry at a higher level.
//
// Explicit Release calls elsewhere still treat `lease_required` as success so
// teardown never hangs even if the lease has already been reclaimed.
//
// # Queue service
//
// lockd includes an at-least-once queue built on the same storage backends,
// encryption pipeline, and namespace layout as the lease/state surface. The
// HTTP API exposes `/v1/queue/enqueue`, `/v1/queue/dequeue`,
// `/v1/queue/dequeue/state`, `/v1/queue/subscribe`, `/v1/queue/ack`,
// `/v1/queue/nack`, and `/v1/queue/extend`. Namespaces are required for every
// queue call; omitting the field falls back to `Config.DefaultNamespace`, and
// the Go client/CLI mirror the same default/override behaviour (`LOCKD_QUEUE_NAMESPACE`
// for CLI flows).
//
// Producers stream payloads (optionally zero-length) alongside JSON metadata
// describing `queue`, `delay_seconds`, `visibility_timeout_seconds`,
// `ttl_seconds`, `max_attempts`, and arbitrary attributes. Consumers issue
// dequeue requests with an owner identity; responses stream message metadata
// and payload via multipart/related parts so large blobs never buffer in RAM.
// State-aware dequeues acquire a workflow lease in the same request and expose
// helpers that keep message + state fencing tokens aligned when acking,
// nacking, or extending visibility.
//
// The queue dispatcher multiplexes watchers/pollers across namespaces and
// storage backends. Disk/mem stores use native notifications (inotify /
// in-process) when available and fall back to polling when disabled or running
// on NFS. Object stores rely on the same observed-key tracking used by the
// lock surface so stale reads never reset metadata.
//
// # Perimeter defence (LSF + QRF)
//
// Each server embeds a **Local Security Force (LSF)** observer that samples
// host metrics (memory, swap, load averages, CPU) plus per-endpoint inflight
// counters, and a **Quick Reaction Force (QRF)** controller that applies
// adaptive back-pressure. When the QRF soft-arms or engages, lockd responds
// with HTTP 429, surfaces a `Retry-After` hint, and tags the response with
// `X-Lockd-QRF-State`. The Go client honours these signals automatically; other
// clients should do the same to keep queues draining while the perimeter
// defence recovers. Configuration knobs and workflow details live in
// docs/QRF.md. By default the controller leans on host-wide memory budgets
// (80 % soft / 90 % hard) and load-average multipliers derived from the LSF
// baseline (4×/8×) while queue/lock inflight guards remain disabled unless
// configured explicitly.
//
// # Embedding and helpers
//
// `StartServer` launches a server in a goroutine, waits for readiness, and
// returns a stop function. It’s useful when wiring lockd into existing
// processes or sidecars. The `client/inprocess` package builds on top of it,
// starting an embedded server (MTLS disabled) and returning a ready-to-use
// client facade:
//
//	cfg := lockd.Config{Store: "mem://", DisableMTLS: true}
//	inproc, err := inprocess.New(ctx, cfg)
//	if err != nil { log.Fatal(err) }
//	defer inproc.Close(ctx)
//
//	sess, err := inproc.Acquire(ctx, api.AcquireRequest{Key: "demo", Owner: "inproc", TTLSeconds: 20})
//	if err != nil { log.Fatal(err) }
//	defer sess.Close()
//
// # Storage backends
//
// Configure the storage layer via `Config.Store`:
//
//   - `mem://` – in-memory (tests and local experimentation)
//   - `disk:///var/lib/lockd-data` – SSD/NVMe-oriented disk backend with optional retention
//   - `azure://account/container` – Azure Blob Storage (Shared Key or SAS auth)
//   - `aws://bucket/prefix` – AWS S3 (uses standard AWS credential sources, requires region)
//   - `s3://host:port/bucket` – MinIO or other S3-compatible stores (TLS on unless `?insecure=1`)
//
// JSON uploads are compacted using the selected compactor
// (see `Config.JSONUtil`), and large payloads spill to disk after
// `Config.SpoolMemoryThreshold`.
//
// Consult README.md for detailed guidance, additional examples, and operational
// considerations (TLS, auth bundles, environment variables).
package lockd
