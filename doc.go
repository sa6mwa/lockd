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
// # Unix domain sockets
//
// For same-host sidecars you can serve over a Unix socket by setting
// `ListenProto` to "unix". Clean-up is automatic and mTLS can be disabled when
// the connection never leaves the machine.
//
//	cfg := lockd.Config{
//	    Store:       "mem://",
//	    ListenProto: "unix",
//	    Listen:      "/var/run/lockd.sock",
//	    MTLS:        false,
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
//   - `unix:///path/to/lockd.sock` – Unix-domain sockets (requires `MTLS=false`
//     or supplying a client bundle)
//
// Example:
//
//	cli, err := client.New("https://lockd.example.com")
//	if err != nil { log.Fatal(err) }
//	sess, err := cli.Acquire(ctx, api.AcquireRequest{
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
// `GetState`) consume the same bounded retry budget controlled by
// `client.WithAcquireFailureRetries` and `client.WithAcquireBackoff`. Once the
// handler starts running, further errors are surfaced immediately—the typical
// pattern is to return the error (acquire-for-update propagates it) and decide
// whether to retry at a higher level.
//
// Explicit Release calls elsewhere still treat `lease_required` as success so
// teardown never hangs even if the lease has already been reclaimed.
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
//	cfg := lockd.Config{Store: "mem://", MTLS: false}
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
// # Benchmark suites
//
// Optional benchmark packages compare raw backend throughput against the lockd
// API (both buffered and streaming paths). They are handy when tuning payload
// sizes or spool thresholds:
//
//	set -a && source .env.local && set +a && go test -run=^$ -bench='BenchmarkLockd(LargeJSON|SmallJSON)' -benchmem ./integration/minio -tags "integration minio bench"
//	set -a && source .env.local && set +a && go test -run=^$ -bench='Benchmark(Disk|LockdDisk)' -benchmem ./integration/disk -tags "integration disk bench"
//
// Consult README.md for detailed guidance, additional examples, and operational
// considerations (TLS, auth bundles, environment variables).
package lockd
