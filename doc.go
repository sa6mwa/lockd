// lockd Copyright (C) 2025 Michel Blomgren <https://pkt.systems>
//
// Package lockd exposes the Go APIs behind the lock + state coordination
// service. Most users interact with the binary, but the package makes it easy
// to embed the server, spin up sidecars, or talk to lockd from Go clients.
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
//	lease, err := cli.Acquire(ctx, client.AcquireRequest{
//	    Key:        "orders",
//	    Owner:      "worker-1",
//	    TTLSeconds: 30,
//	    BlockSecs:  20,
//	})
//	if err != nil { log.Fatal(err) }
//	defer cli.Release(ctx, client.ReleaseRequest{Key: "orders", LeaseID: lease.LeaseID})
//
// The client streams state to avoid buffering large payloads. Use
// `GetStateBytes` / `UpdateStateBytes` for convenience if you prefer working
// with slices.
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
//	lease, err := inproc.Acquire(ctx, client.AcquireRequest{Key: "demo", Owner: "inproc", TTLSeconds: 20})
//	if err != nil { log.Fatal(err) }
//	defer inproc.Release(ctx, client.ReleaseRequest{Key: "demo", LeaseID: lease.LeaseID})
//
// # Storage backends
//
// Configure the storage layer via `Config.Store`:
//
//   - `mem://` – in-memory (tests and local experimentation)
//   - `pebble:///var/lib/lockd` – embedded Pebble database
//   - `disk:///var/lib/lockd-data` – SSD/NVMe-oriented disk backend with optional retention
//   - `azure://account/container` – Azure Blob Storage (Shared Key or SAS auth)
//   - `s3://bucket/prefix` – AWS S3 (requires region or custom endpoint)
//   - `minio://host:port/bucket` – MinIO or other S3-compatible stores
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
//	set -a && source .env.local && set +a && go test -run=^$ -bench='Benchmark(Pebble|LockdPebble)' -benchmem ./integration/pebble -tags "integration pebble bench"
//
// Consult README.md for detailed guidance, additional examples, and operational
// considerations (TLS, auth bundles, environment variables).
package lockd
