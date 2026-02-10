// Package lockd exposes the Go APIs behind the single-binary coordination
// plane that combines exclusive leases, atomic JSON state (with search/index),
// binary attachments, and an at-least-once queue. The server runs cleanly as
// PID 1 or can be embedded as a library; the same storage abstraction powers
// disk, S3/MinIO, Azure Blob, and in-memory backends with optional envelope
// encryption.
//
// Copyright (C) 2025 Michel Blomgren <https://pkt.systems>
//
// # Running a server
//
// The server listens on the network specified by Config.ListenProto (default
// tcp) and address Config.Listen. Mutual TLS is enabled by default.
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
// Disk/NFS backends use a log-structured store with durable group commit.
// Batching is driven by natural backpressure: fsync occurs for each batch and
// the queue builds only while prior fsyncs are in-flight. LogstoreCommitMaxOps
// caps batch size. LogstoreSegmentSize controls when segment files roll.
// DiskLockFileCacheSize caps cached lockfile descriptors for disk/NFS (default
// 2048; set negative to disable caching).
// Hot state reads can be cached in-process via StateCacheBytes (default 64 MiB;
// set 0 to disable).
// QueryDocPrefetch controls parallel fetch depth for query return=documents
// (default 8; set 1 to disable).
// In HA concurrent mode, single-writer optimizations are disabled.
//
//	cfg := lockd.Config{
//	    Store:      "disk:///var/lib/lockd-data",
//	    LogstoreCommitMaxOps:   128,                // disk/NFS only
//	    LogstoreSegmentSize:    64 << 20,           // disk/NFS only (bytes)
//	    DiskLockFileCacheSize:  2048,               // disk/NFS only
//	    StateCacheBytes:        64 << 20,           // cache hot state payloads
//	    QueryDocPrefetch:       8,                  // return=documents prefetch depth
//	}
//
// The CLI mirrors this with --logstore-commit-max-ops,
// --logstore-segment-size, --disk-lock-file-cache-size,
// --state-cache-bytes, and --query-doc-prefetch.
//
// # HA modes (concurrent vs failover)
//
// When multiple lockd servers share the same backend, HAMode controls
// concurrent vs failover behaviour. HAMode="failover" (default) uses a
// lease stored under the internal .ha/activelease key to elect a single
// active writer; passive nodes return HTTP 503 so clients can retry another
// endpoint. HAMode="concurrent" enables multi-writer semantics. HALeaseTTL
// controls the lease duration and renewal cadence.
//
//	cfg := lockd.Config{
//	    Store:      "disk:///var/lib/lockd-data",
//	    HAMode:     "failover",
//	    HALeaseTTL: 5 * time.Second,
//	}
//
// The CLI mirrors this with --ha and --ha-lease-ttl.
//
// Namespaces partition keys and metadata. When callers omit the namespace, the
// server falls back to Config.DefaultNamespace (default "default"). Setting
// the field on Config, providing Namespace in api.AcquireRequest, or
// configuring clients via client.WithDefaultNamespace keeps each workload’s
// state isolated under its own prefix.
// Namespaces that start with a dot are reserved for lockd internals (e.g.
// .txns stores transaction records) and are rejected by both the HTTP layer
// and the core service. Always use user-defined namespaces that do not begin
// with '.'.
//
// # Unix domain sockets
//
// For same-host sidecars you can serve over a Unix socket by setting
// ListenProto to "unix". Clean-up is automatic and mTLS can be disabled when
// the connection never leaves the machine.
//
//	cfg := lockd.Config{
//	    Store:        "mem://",
//	    ListenProto:  "unix",
//	    Listen:       "/var/run/lockd.sock",
//	    DisableMTLS:  true,
//	}
//	handle, err := lockd.StartServer(ctx, cfg)
//	if err != nil { log.Fatal(err) }
//	defer handle.Stop(context.Background())
//
// # Client SDK
//
// The Go client (pkt.systems/lockd/client) wraps the HTTP API. The base URL
// decides the transport:
//
//   - https://host:9341 – production mTLS connection (default)
//   - http://host:9341 – plain HTTP for trusted networks or local testing
//   - unix:///path/to/lockd.sock – Unix-domain sockets (requires DisableMTLS
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
//	    // Optional: join an existing transaction across keys.
//	    TxnID:      existingTxnID,
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
// The lease session carries the TxnID minted by Acquire. All lease-bound
// mutations (Update/Remove/UpdateMetadata/Release/attachments) require that
// transaction id. The SDK wires X-Txn-ID automatically when you use
// LeaseSession; custom HTTP clients must supply it (and include txn_id in
// the release request body).
//
// The client tracks fencing tokens automatically; reusing the same Client
// instance ensures follow-up KeepAlive/Get/Update/Release calls include the
// freshest X-Fencing-Token. For multi-process flows the CLI exports the token
// via LOCKD_CLIENT_FENCING_TOKEN, and your program can register it manually
// with Client.RegisterLeaseToken.
//
// Shutdown sequencing is controlled via Config.DrainGrace and
// Config.ShutdownTimeout. Draining gives existing lease holders time to wrap
// up (default 10 s) before the HTTP server begins closing connections; the
// timeout caps the combined drain + HTTP shutdown interval (default 10 s). The
// defaults mirror the CLI flags (--drain-grace, --shutdown-timeout) and can
// be disabled by setting them to 0. Each server splits the budget 80/20 so a
// 10 s timeout reserves ~8 s for draining and ~2 s for http.Server.Shutdown.
//
// Sweeping is split into two paths: (1) transparent cleanup on relevant ops
// (e.g., if an acquire encounters an expired lease) and (2) an idle maintenance
// sweeper that runs only after a period of inactivity. The idle sweeper is
// rate-limited (max ops and max runtime) and can be configured to pause between
// operations to reduce backend pressure. Transaction replay has its own
// throttle so active traffic can kick replay without inheriting the idle
// sweeper cadence. Configure these in the server SDK with:
//
//	cfg := lockd.Config{
//	    Store:            "aws://lockd-prod",
//	    SweeperInterval:  5 * time.Minute,   // idle sweep tick
//	    IdleSweepGrace:   5 * time.Minute,   // idle time before a sweep can start
//	    IdleSweepOpDelay: 100 * time.Millisecond,
//	    IdleSweepMaxOps:  1000,
//	    IdleSweepMaxRuntime: 30 * time.Second,
//	    TxnReplayInterval:   5 * time.Second, // throttle for replay on active ops
//	}
//
// To disable the idle sweeper entirely, set SweeperInterval <= 0 (replay
// throttling is independent and controlled by TxnReplayInterval).
//
// Queue transactions use a dedicated worklist to avoid list scans when a
// queue message is enlisted in a transaction decision. When a commit/rollback
// includes a queue message participant, the TC records a small per-queue
// decision list under .txn-queue-decisions. Dequeue checks that worklist at
// most once per cache window (no background sweeps) and applies up to a capped
// number of items, making rollback-visible messages reappear after restarts
// without scanning the queue. Non-transactional queues do not write these
// markers, so they incur no extra IO beyond the cached check.
//
// Configure the worklist behavior with:
//
//	cfg := lockd.Config{
//	    QueueDecisionCacheTTL: 60 * time.Second, // cache empty worklist checks
//	    QueueDecisionMaxApply: 50,               // max items applied per dequeue
//	    QueueDecisionApplyTimeout: 2 * time.Second, // time budget per dequeue apply
//	}
//
// The CLI mirrors these as --queue-decision-cache-ttl,
// --queue-decision-max-apply, and --queue-decision-apply-timeout.
//
// The Go client and CLI are drain-aware by default: when the server emits the
// Shutdown-Imminent header, the SDK auto-releases leases once in-flight work
// is done. Opt out by using client.WithDrainAwareShutdown(false) or passing
// --drain-aware-shutdown=false / LOCKD_CLIENT_DRAIN_AWARE=false to the CLI.
//
// Multi-host deployments can construct the client with multiple base URLs via
// client.NewWithEndpoints([]string{...}). The SDK rotates through the
// provided endpoints on failure, carrying the same bounded retry budget so that
// reacquire attempts remain deterministic even when the primary server drops
// mid-request.
//
// # Acquire-for-update workflow
//
// AcquireForUpdate wraps the usual acquire → get → update → release cycle in
// a single helper. Callers supply a function that receives an
// AcquireForUpdateContext; the context exposes the current StateSnapshot
// (so the handler can stream or decode the JSON) and forwards convenience
// methods like Update/UpdateBytes/Save/Remove. The helper keeps the lease alive
// in the background while the handler executes and always releases the lease
// when the handler returns. Both LeaseSession and AcquireForUpdateContext
// expose Remove helpers that delete the JSON blob while holding the lease.
// They honour the same conditional headers (X-If-Version, X-If-State-ETag)
// as updates and clear the server-side metadata when the delete succeeds.
//
// Handshake failures (for example lease_required during the initial
// Get) consume the same bounded retry budget controlled by
// client.WithAcquireFailureRetries and client.WithAcquireBackoff. Once the
// handler starts running, further errors are surfaced immediately—the typical
// pattern is to return the error (acquire-for-update propagates it) and decide
// whether to retry at a higher level.
//
// Explicit Release calls elsewhere still treat lease_required as success so
// teardown never hangs even if the lease has already been reclaimed.
//
// # State attachments
//
// Keys can carry multiple named binary attachments. Attachments are staged
// under the lease transaction just like JSON state updates: attach files while
// holding the lease, and they become visible to public reads once the lease is
// released (commit). Attachments are stored under state/<key>/attachments/<id>
// and flow through the same encryption-at-rest pipeline as state/queue payloads.
// Lease-bound attachment operations require X-Txn-ID; public reads do not.
//
// Use ListAttachments/RetrieveAttachment on the lease to inspect staged
// files, and DeleteAttachment/DeleteAllAttachments to stage removals that
// apply on release (rollbacks discard staged changes).
//
//	lease, err := cli.Acquire(ctx, api.AcquireRequest{Key: "orders", Owner: "worker-1", TTLSeconds: 30})
//	if err != nil { log.Fatal(err) }
//	defer lease.Close()
//
//	if _, err := lease.Attach(ctx, client.AttachRequest{
//	    Name:        "invoice.pdf",
//	    ContentType: "application/pdf",
//	    Body:        fileReader,
//	}); err != nil {
//	    log.Fatal(err)
//	}
//	if err := lease.Release(ctx); err != nil { log.Fatal(err) }
//
//	// Public reads can list/retrieve attachments after release.
//	resp, err := cli.Get(ctx, "orders")
//	if err != nil { log.Fatal(err) }
//	defer resp.Close()
//	attachments, err := resp.ListAttachments(ctx)
//	if err != nil { log.Fatal(err) }
//	_ = attachments
//
// # Queue service
//
// lockd includes an at-least-once queue built on the same storage backends,
// encryption pipeline, and namespace layout as the lease/state surface. The
// HTTP API exposes /v1/queue/enqueue, /v1/queue/dequeue,
// /v1/queue/dequeue/state, /v1/queue/subscribe, /v1/queue/ack,
// /v1/queue/nack, and /v1/queue/extend. Namespaces are required for every
// queue call; omitting the field falls back to Config.DefaultNamespace, and
// the Go client/CLI mirror the same default/override behaviour (LOCKD_QUEUE_NAMESPACE
// for CLI flows).
//
// Producers stream payloads (optionally zero-length) alongside JSON metadata
// describing queue, delay_seconds, visibility_timeout_seconds,
// ttl_seconds, max_attempts, and arbitrary attributes. Consumers issue
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
// adaptive back-pressure. When the QRF soft-arms or engages, lockd paces
// requests server-side by waiting for a computed delay before continuing.
// Only if the delay exceeds the configured max wait does lockd respond with
// HTTP 429, surface a Retry-After hint, and tag the response with
// X-Lockd-QRF-State. The Go client honours these signals automatically;
// other clients should do the same to keep queues draining while the perimeter
// defence recovers. Configuration knobs and workflow details live in
// docs/QRF.md. By default the controller leans on host-wide memory budgets
// (80 % soft / 90 % hard) and load-average multipliers derived from the LSF
// baseline (4×/8×) while queue/lock/query inflight guards remain disabled
// unless configured explicitly.
//
// # Telemetry
//
// Traces are exported over OTLP when Config.OTLPEndpoint is set (gRPC by
// default; use grpc://, grpcs://, http://, or https:// to force a
// transport). Metrics are exposed via a Prometheus scrape endpoint when
// Config.MetricsListen is non-empty (for example :9464). Runtime profiling
// metrics (goroutines, heap, scheduler latency) are opt-in via
// Config.EnableProfilingMetrics. A pprof debug listener can be exposed with
// Config.PprofListen. All three can be enabled together or independently:
//
//	cfg := lockd.Config{
//	    Store:         "disk:///var/lib/lockd",
//	    OTLPEndpoint:  "localhost:4317",
//	    MetricsListen: ":9464",
//	    EnableProfilingMetrics: true,
//	    PprofListen:            ":6060",
//	}
//	srv, err := lockd.NewServer(cfg)
//	if err != nil { log.Fatal(err) }
//	defer srv.Close(context.Background())
//
// Embedded servers can override metrics via lockd.WithMetricsListen when
// using helpers such as StartServer or client/inprocess.
//
// # Embedding and helpers
//
// StartServer launches a server in a goroutine, waits for readiness, and
// returns a handle with a Stop method. It’s useful when wiring lockd into existing
// processes or sidecars. The client/inprocess package builds on top of it,
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
// Configure the storage layer via Config.Store:
//
//   - mem:// – in-memory (tests and local experimentation)
//   - disk:///var/lib/lockd-data – SSD/NVMe-oriented disk backend with optional retention
//   - azure://account/container – Azure Blob Storage (Shared Key or SAS auth)
//   - aws://bucket/prefix – AWS S3 (uses standard AWS credential sources, requires region)
//   - s3://host:port/bucket – MinIO or other S3-compatible stores (TLS on unless ?insecure=1)
//
// JSON uploads are compacted using the selected compactor (see
// Config.JSONUtil), and large payloads spill to disk after
// Config.SpoolMemoryThreshold.
//
// # LQL query & mutation language
//
// Both the CLI and HTTP APIs share a common selector/mutation DSL implemented
// by pkt.systems/lql. Selectors accept JSON Pointer field paths
// (and.eq{field=/status,value=open}, or.1.range{field=/progress/percent,gte=50}),
// while mutations cover assignments, arithmetic (++, --, =+5), removals
// (rm:/delete:), time: aliases for RFC3339 timestamps, and brace
// shorthand that fans out to nested keys. Examples:
//
//	lockd client set --key ledger \
//	    '/data{/hello key="mars traveler",/count++}' \
//	    /meta/previous=world \
//	    time:/meta/processed=NOW
//
// Keys follow RFC 6901 JSON Pointer semantics (leading /; escape / as ~1
// and ~ as ~0). Commas/newlines can be mixed freely—making it practical to
// paste production-style JSON paths into CLI tests, Go unit tests, or query
// strings (/v1/query?and.eq{...}).
//
// Consult README.md for detailed guidance, additional examples, and operational
// considerations (TLS, auth bundles, environment variables).
package lockd
