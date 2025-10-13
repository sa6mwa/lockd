# lockd

`lockd` is a minimal, single-binary coordination service that provides **exclusive
leases** and **atomic JSON state** for distributed workers. It is intentionally
small, CGO-free, and suitable for static linking (e.g. `FROM scratch` images).

Typical flow:

1. A worker acquires a lease for a key (e.g. `orders`).
2. The worker reads the last committed JSON state / checkpoint.
3. Work is performed, the JSON state is updated atomically (CAS).
4. The lease is released so the next worker can continue.

If the worker crashes or the connection drops, TTL expiry allows the next
worker to resume from the last committed state.

---

## Features

- **Exclusive leases** (one holder per key) with configurable TTLs, keepalive,
  and a background sweeper to reap expired locks.
- **Atomic JSON state** up to ~50 MB with CAS via version + ETag headers.
- **Monotonic fencing tokens** protect against delayed clients; every lease-bound
  request must include the latest `X-Fencing-Token` issued by the server.
- **Simple HTTP/JSON API** (no gRPC) capable of running with or without TLS.
- **Storage backends**
  - **S3 / S3-compatible** object stores using a conditional copy pattern.
  - **Azure Blob Storage** with Shared Key or SAS authentication.
  - **Disk** backend optimised for SSD/NVMe with optional retention.
  - **Pebble** embedded KV store for single-node deployments.
  - **In-memory** backend for tests.
- **Go SDK** (`client` package) with automatic retries and structured errors.
- **Cobra/Viper CLI** with environment parity (`LOCKD_*`).
- **Diagnostics** (`lockd verify store`) and integration suites (AWS & Pebble).

---

## Architecture Overview

```
+----------------+        HTTP/JSON        +--------------------+
|  Worker / App  | <---------------------> |       lockd        |
| (Go client)    | Acquire / KeepAlive /   |    (this repo)     |
|                | UpdateState / Release   |  Storage backend   |
+----------------+                         | (S3 / Pebble / …)  |
                                            +--------------------+
```

### Request flow

1. **Acquire** – `POST /v1/acquire` → acquire lease (optionally blocking).
2. **Get state** – `POST /v1/get_state` → stream JSON state with CAS headers.
   Supply `X-Lease-ID` + `X-Fencing-Token` from the acquire response.
3. **Update state** – `POST /v1/update_state` → upload new JSON with
   `X-If-Version` and/or `X-If-State-ETag` to enforce CAS. Include the current
   `X-Fencing-Token`.
4. **Release** – `POST /v1/release` → release lease with the same fencing token;
   the sweeper handles timeouts for crashed workers.

### Internal layout

- `server.go` – server wiring, storage retry wrapper, sweeper.
- `internal/httpapi` – HTTP handlers for the API surface.
- `internal/storage` – backend interface, retry wrapper, S3/Pebble/Memory.
- `client` – public Go SDK.
- `cmd/lockd` – CLI entrypoint (Cobra/Viper).
- `internal/tlsutil` – bundle loading/generation helpers.
- `integration/` – end-to-end tests (AWS & Pebble).

---

## Storage Backends

### Selecting a backend

`lockd` picks the storage implementation from the `--store` flag (or `LOCKD_STORE`
environment variable) by inspecting the URL scheme:

| Scheme | Example | Backend | Notes |
|--------|---------|---------|-------|
| `mem://` or empty | `mem://` | In-memory | Ephemeral; test only. |
| `s3://` | `s3://my-bucket/prefix` | AWS S3 (or any S3-compatible endpoint) | Provide AWS credentials via `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (and optional `AWS_SESSION_TOKEN`). Requires `--s3-region` or `--s3-endpoint`. |
| `minio://` | `minio://localhost:9000/lockd-data?insecure=1` | MinIO | TLS **enabled by default**. Append `?insecure=1` (or set `LOCKD_S3_DISABLE_TLS=1`) to use plain HTTP. Supply credentials with `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` or `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`. |
| `pebble://` | `pebble:///var/lib/lockd` | Embedded Pebble KV | Directory must exist and be writable by the process. |
| `disk://` | `disk:///var/lib/lockd-data` | SSD/NVMe-tailored disk backend | Stores state/meta beneath the provided root; optional retention window. |
| `azure://` | `azure://account/container/prefix` | Azure Blob Storage | Account name in host, container + optional prefix in path. Authentication via account key or SAS token. |

Credentials are loaded from the standard environment variables that the MinIO
SDK supports. No secret keys are stored in the `lockd` config file.

### S3 / S3-compatible

- Uses temp uploads + `CopyObject` with conditional headers for CAS.
- Supports SSE (`aws:kms` / `AES256`) and custom endpoints (MinIO, Localstack).
- Default retry budget: 12 attempts, 500 ms base delay, capped at 15 s.

Configuration (flags or env via `LOCKD_` prefix):

| Flag / Env                 | Description                                   |
|---------------------------|-----------------------------------------------|
| `--store` / `LOCKD_STORE` | `s3://bucket/prefix`                           |
| `--s3-region`             | AWS region (required unless endpoint provided) |
| `--s3-endpoint`           | Custom S3 endpoint                             |
| `--s3-sse`                | `AES256` or `aws:kms`                          |
| `--s3-kms-key-id`         | KMS key when using `aws:kms`                   |
| `--s3-max-part-size`      | Multipart upload part size                     |

### Pebble (embedded)

- Uses [cockroachdb/pebble](https://github.com/cockroachdb/pebble).
- Per-key mutexes ensure metadata CAS semantics under aggressive concurrency.
- Default retry budget: 6 attempts, 100 ms base delay, capped at 5 s.
- Configure with `--store pebble:///var/lib/lockd`.

### Memory

- In-process backend used for unit tests.

---

## Configuration & CLI

`lockd` exposes flags mirrored by `LOCKD_*` environment variables. Example:

```sh
# AWS S3 with region-based endpoint
export LOCKD_STORE="s3://my-bucket/prefix"
export LOCKD_S3_REGION="us-west-2"
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
lockd \
  --listen :9341 \
  --store "$LOCKD_STORE" \
  --json-max 100MB \
  --default-ttl 30s \
  --max-ttl 2m \
  --acquire-block 60s \
  --sweeper-interval 5s \
  --bundle $HOME/.lockd/server.pem

# MinIO running locally over HTTP
export LOCKD_STORE="minio://localhost:9000/lockd-data?insecure=1"
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="minioadmin"
lockd --store "$LOCKD_STORE" --listen :9341 --bundle $HOME/.lockd/server.pem

# Azure Blob Storage (account key)
set -a && source .env.azure && set +a
export LOCKD_STORE="azure://$LOCKD_AZURE_ACCOUNT/lockd-data/prefix"
lockd --store "$LOCKD_STORE" --listen :9341 --mtls=false

# IPv4-only binding
lockd --listen-proto tcp4 --listen 0.0.0.0:9341 --store "$LOCKD_STORE"

# Prefer stdlib JSON compaction for tiny payloads
lockd --store mem:// --json-util stdlib
```

The default listen address is `:9341`, chosen from the unassigned IANA space to
avoid clashes with common cloud-native services.

### Lease fencing tokens

Every successful `acquire` response includes a `fencing_token` and echoing
`X-Fencing-Token` on follow-up requests is mandatory. The Go SDK manages the
token automatically when you reuse the same `client.Client`. For CLI workflows
you can export the token so subsequent commands pick it up:

```sh
eval "$(lockd client acquire --server localhost:9341 --owner worker orders)"
lockd client keepalive --lease "$LOCKD_CLIENT_LEASE_ID" orders
lockd client update --lease "$LOCKD_CLIENT_LEASE_ID" --fencing-token "$LOCKD_CLIENT_FENCING_TOKEN" orders payload.json
```

If the server detects a stale token it returns `403 fencing_mismatch`, ensuring
delayed or replayed requests cannot clobber state after a lease changes hands.

#### Why fencing tokens matter

The token is a strictly monotonic counter that advances on every successful
`acquire`. Compared with relying purely on lease IDs and CAS/version checks, it
adds several key safeguards:

- **Lease turnover without state writes** – metadata `version` only increments
  when the JSON blob changes. If lease A expires and lease B acquires but has not
  updated the state yet, the version remains unchanged. The fencing token has
  already increased, so any delayed keepalive/update from lease A is rejected
  immediately.
- **Ordering, not just identity** – lease IDs are random, so downstream systems
  cannot tell which one is newer. By carrying the token, workers give databases
  and queues a simple “greater-than” check: accept writes with the highest token,
  reject anything older.
- **Cache resilience inside the server** – the handler caches lease metadata to
  avoid extra storage reads. A stale request might otherwise slip through by
  reading the cached entry; the fencing token still forces a mismatch and blocks
  the outdated lease holder.
- **Protection for downstream systems** – workers can forward the token to other
  services (databases, queues) and let them reject stale writers. CAS keeps
  lockd’s JSON state consistent, while fencing tokens extend that guarantee to
  anything else the worker touches.
- **Operational guardrails** – CLI/scripts that stash lease IDs in environment
  variables gain an extra safety net. If an operator forgets to refresh after a
  re-acquire, the stale token triggers a clear 403 instead of silently updating
  the wrong state.

### Configuration files

`lockd` can also read a YAML configuration file (loaded via Viper). Generate a
template with sensible defaults using the helper command:

```sh
lockd config gen            # writes $HOME/.lockd/config.yaml
lockd config gen --stdout   # print the template instead of writing a file
lockd config gen --out /tmp/lockd.yaml --force
```

The generated file contains the same keys as the CLI flags (for example
`listen-proto`, `json-max`, `json-util`, `payload-spool-mem`, `disk-retention`, `disk-janitor-interval`, `s3-region`, `s3-disable-tls`). When present, the configuration file
is read before environment variables so you can override individual settings via
`LOCKD_*` exports or command-line flags.

### JSON Compaction Engines

Lockd ships with three drop-in JSON compactors. Select one with `--json-util` or
the `LOCKD_JSON_UTIL` environment variable:

- `lockd` (default) – streaming writer tuned for multi-megabyte payloads.
- `jsonv2` – tokenizer inspired by Go 1.25’s experimental JSONv2 runtime.
- `stdlib` – Go’s stock `encoding/json.Compact` helper for minimal dependencies.

Benchmarks on a 13th Gen Intel Core i7-1355U (Go 1.25.2):

| Implementation | Small (~150B) ns/op (allocs) | Medium (~60KB) ns/op (allocs) | Large (~2MB) ns/op (allocs) |
|---------------|-------------------------------|--------------------------------|-------------------------------|
| `encoding/json` | **380 (1)** | **69,818 (1)** | 4,032,017 (1) |
| `lockd` | 1,609 (5) | 98,083 (20) | **2,818,759 (26)** |
| `jsonv2` | 1,572 (5) | 92,892 (16) | 3,046,118 (23) |

The default remains `lockd`, which provides the best throughput on large payloads
— the primary lockd use-case — while `jsonv2` or `stdlib` can be selected for
small, latency-sensitive workloads. Re-run the suite locally with:

```sh
go test -bench=BenchmarkCompact -benchmem ./internal/jsonutil
```

### Streaming State Updates & Payload Spooling

The client SDK now exposes streaming helpers so large JSON blobs no longer need
to be buffered in memory. On the same 13th Gen Intel Core i7-1355U host:

| Benchmark | ns/op | MB/s | B/op | allocs/op |
|-----------|------:|-----:|-----:|----------:|
| `BenchmarkClientGetStateBytes` | 707,158 | 370.72 | 1,218,417 | 131 |
| `BenchmarkClientGetStateStream` | **219,847** | **1,192.45** | **8,100** | 97 |
| `BenchmarkClientUpdateStateBytes` | **241,807** | **1,084.16** | 43,330 | 114 |
| `BenchmarkClientUpdateStateStream` | 402,225 | 651.74 | **9,759** | 122 |

Streaming reads cut allocations by ~150× and uploads by ~4.4×; throughput is a
touch lower for uploads because the payload is generated on the fly, but avoids
materialising entire documents in memory.

On the server side, lockd compacts JSON through an in-memory spool that spills
to disk once a threshold is exceeded. By default up to 4 MiB of the request is
kept in RAM. You can tune this via `--payload-spool-mem` /
`LOCKD_PAYLOAD_SPOOL_MEM` / `payload-spool-mem` in the config file to trade
memory for CPU (or vice versa).

Running the MinIO-backed benchmarks with the default threshold:

| Benchmark | ns/op | MB/s | B/op | allocs/op |
|-----------|------:|-----:|-----:|----------:|
| `BenchmarkLockdLargeJSON` | 112,064,222 | 46.78 | 22,309,298 | 6,166 |
| `BenchmarkLockdLargeJSONStream` | **59,486,906** | **88.14** | 22,279,726 | 6,315 |
| `BenchmarkLockdSmallJSON` | 76,891,365 | 0.01 | 1,790,225 | 7,513 |
| `BenchmarkLockdSmallJSONStream` | **18,178,942** | **0.03** | **439,805** | **2,261** |

Large uploads still allocate heavily because the spool buffers the first 4 MiB
before spilling to disk. Lowering the threshold (for example `--payload-spool-mem=1MB`)
pushes more work onto disk IO, which may improve tail latency on constrained
hosts. Small updates benefit significantly from streaming even with the default
threshold. Choose a value that matches your workload and disk characteristics;
the benchmarks above were gathered via:

```sh
set -a && source .env.local && set +a && go test -run=^$ -bench=BenchmarkClientGetState -benchmem ./client
set -a && source .env.local && set +a && go test -run=^$ -bench=BenchmarkClientUpdateState -benchmem ./client
set -a && source .env.local && set +a && go test -run='^$' -bench='BenchmarkLockd(LargeJSON|SmallJSON)' -benchmem ./integration/minio -tags "integration minio bench"
```

### Example Use-cases

In addition to coordinating workflow checkpoints, lockd’s lease + atomic JSON
model unlocks several other patterns once performance and durability goals are
met:

- **Feature flag shards** – hold per-segment experiment state and atomically
  roll back under contention without adding a new datastore.
- **Session handoff / sticky routing** – track live client sessions across
  stateless edge workers using short leases and JSON metadata blobs.
- **IoT rollout controller** – drive firmware or configuration rollouts where
  each device claims work and reports progress exactly once.
- **Distributed cron / windowing** – serialize recurring jobs (per key) so
  retries don’t overlap, while keeping per-run state directly in lockd.

### Benchmarking with MinIO

With MinIO running locally (for example on `localhost:9000`) you can compare raw
object-store performance against `lockd` by running the benchmark suite:

```sh
# Large (5 MiB) and small payload benchmarks + concurrency tests
go test -bench . -run '^$' -tags "integration minio bench" ./integration/minio
```

The harness measures both sequential and concurrent scenarios for large (~5 MiB)
and small (~512 B) payloads:

- Raw MinIO `PutObject` throughput (large/small).
- `lockd` acquire/update/release cycles (large/small).
- Raw MinIO concurrent writers on distinct keys (large/small).
- `lockd` concurrent writers on distinct keys (large/small).

Benchmarks assume the same environment variables as the MinIO integration tests
(`LOCKD_STORE`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, etc.). Use
`LOCKD_STORE=minio://localhost:9000/lockd-integration?insecure=1` for a default
local setup, or point it at HTTPS by omitting the `?insecure=1` query string.

### Benchmarking with Pebble

The Pebble suite mirrors the MinIO scenarios, contrasting raw Pebble API usage
with the lockd HTTP path (bytes vs streaming updates). Run it with:

```sh
set -a && source .env.local && set +a && go test -run=^$ -bench='Benchmark(Pebble|LockdPebble)' -benchmem ./integration/pebble -tags "integration pebble bench"
```

Recent measurements on the same host:

| Benchmark | ns/op | MB/s | B/op | allocs/op |
|-----------|------:|-----:|-----:|----------:|
| `BenchmarkPebbleRawLargeJSON` | 27,881,303 | 188.04 | 14,003,307 | 585 |
| `BenchmarkLockdPebbleLargeJSON` | 49,827,087 | 105.22 | 75,057,644 | 1,446 |
| `BenchmarkLockdPebbleLargeJSONStream` | 52,101,989 | 100.63 | 77,212,850 | 1,861 |
| `BenchmarkPebbleRawSmallJSON` | 11,229,583 | 0.05 | 2,644 | 32 |
| `BenchmarkLockdPebbleSmallJSON` | 18,707,014 | 0.03 | 179,173 | 1,624 |
| `BenchmarkLockdPebbleSmallJSONStream` | 4,288,340 | 0.12 | 43,350 | 452 |
| `BenchmarkPebbleRawConcurrent` | 242,977 | 2.12 | 110 | 1 |
| `BenchmarkLockdPebbleConcurrent` | 665,014 | 0.78 | 114,455 | 549 |

As with the MinIO numbers, streaming updates cut allocations and reduce the
cost of small payloads. Large payloads still benefit from tuning
`--payload-spool-mem` based on the host’s disk and memory profile.

### Benchmarking with Disk

The disk backend benchmarks pit raw `disk.Store` throughput against the HTTP API
and include an optional NFS-targeted scenario. Run them with:

```sh
set -a && source .env.local && set +a && go test -run=^$ -bench='Benchmark(Disk|LockdDisk)' -benchmem ./integration/disk -tags "integration disk bench"
set -a && source .env.local && set +a && go test -run ^$ -bench BenchmarkLockdDiskLargeJSONNFS -benchmem ./integration/disk -tags "integration disk bench"
```

Source `.env.disk` (or export the variables manually) before running; the suite
fails fast if the required paths are missing:

- `LOCKD_DISK_ROOT` – absolute path on SSD/NVMe for local disk benchmarks.
- `LOCKD_DISK_NFS_ROOT` – absolute path to an NFS mount (optional but required
  for the NFS benchmark). If both `/mnt/nfs4-lockd` and `/mnt/nfs-lockd` are
  unset/unavailable the test fails.

### In-process client & background server helper

For tests or embedded use-cases you can run lockd entirely in-process. The
`client/inprocess` package starts a private Unix-socket server and returns a
regular client facade:

```go
ctx := context.Background()
cfg := lockd.Config{Store: "mem://", MTLS: false}
inproc, err := inprocess.New(ctx, cfg)
if err != nil { log.Fatal(err) }
defer inproc.Close(ctx)

lease, err := inproc.Acquire(ctx, client.AcquireRequest{Key: "jobs", Owner: "worker", TTLSeconds: 15})
if err != nil { log.Fatal(err) }
defer inproc.Release(ctx, client.ReleaseRequest{Key: "jobs", LeaseID: lease.LeaseID})
```

Behind the scenes it relies on `lockd.StartServer`, which launches the server in
a goroutine and returns a shutdown function. You can use the helper directly
when wiring tests around Unix-domain sockets:

```go
cfg := lockd.Config{Store: "mem://", ListenProto: "unix", Listen: "/tmp/lockd.sock", MTLS: false}
srv, stop, err := lockd.StartServer(ctx, cfg)
if err != nil { log.Fatal(err) }
defer stop(context.Background())

cli, err := client.New("unix:///tmp/lockd.sock")
if err != nil { log.Fatal(err) }
lease, err := cli.Acquire(ctx, client.AcquireRequest{Key: "demo", Owner: "worker", TTLSeconds: 20})
if err != nil { log.Fatal(err) }
defer cli.Release(ctx, client.ReleaseRequest{Key: "demo", LeaseID: lease.LeaseID})
```

Health endpoints:

- `/healthz` – liveness probe
- `/readyz` – readiness probe

### Storage verification

`lockd verify store` validates credentials (list/get/put/delete) and prints a
suggested IAM policy when access fails. Disk backends run a multi-replica
simulation (metadata CAS and payload writes) so locking bugs or stale CAS tokens
fail fast.

```sh
# Verify a disk mount before starting the server
LOCKD_STORE=disk:///var/lib/lockd lockd verify store

# Verify Azure Blob credentials (relies on LOCKD_AZURE_* env vars)
LOCKD_STORE=azure://lockdaccount/lockd-container lockd verify store
```

When `--store` uses `disk://`, the same verification runs automatically during
server startup and the process exits if any check fails.

---

## TLS (mTLS)

mTLS is **enabled by default**. `lockd` looks for a bundle at
`$HOME/.lockd/server.pem` unless `--bundle` points elsewhere. Disable with
`--mtls=false` (testing only).

Bundle format (PEM concatenated):

1. CA certificate
2. CA private key
3. Server certificate (leaf + chain)
4. Server private key

### Generating certificates

```sh
# Generate CA + server certificate bundle (server.pem + ca.pem)
lockd auth new server --hosts "lockd.example.com,127.0.0.1"

# Issue a new client certificate signed by the bundle CA
lockd auth new client --cn worker-1

# Revoke previously issued client certificates (by serial number)
lockd auth revoke client <hex-serial> [<hex-serial>...]

# Inspect bundle details (CA, server cert, denylist)
lockd auth inspect server
lockd auth inspect client --in $HOME/.lockd/client-*.pem

# Verify bundles (validity, EKUs, denylist enforcement)
lockd auth verify server --in $HOME/.lockd/server.pem
lockd auth verify client --server-in $HOME/.lockd/server.pem
```

The commands default to `$HOME/.lockd/`, creating the directory with 0700 and
files with 0600 permissions. Use `--out`/`--ca-out`/`--force` to override file
locations. `ca.pem` contains the trust anchor and is intended to be stored in a
secure location separate from the server runtime bundle.

`lockd auth verify` ensures that the server bundle presents a CA + ServerAuth
certificate (with matching private keys) and that client bundles were issued by
the same CA and are not present on the denylist.

---

## Go Client Usage

```go
cli, err := client.New("https://lockd.example.com")
if err != nil { log.Fatal(err) }
lease, err := cli.Acquire(ctx, client.AcquireRequest{
    Key:        "orders",
    Owner:      "worker-1",
    TTLSeconds: 30,
    BlockSecs:  20,
})
if err != nil { log.Fatal(err) }
body, etag, version, err := cli.GetState(ctx, "orders", lease.LeaseID)
if err != nil { log.Fatal(err) }
var state []byte
if body != nil {
    defer body.Close()
    state, err = io.ReadAll(body)
    if err != nil { log.Fatal(err) }
}
_, err = cli.UpdateState(ctx, "orders", lease.LeaseID, bytes.NewReader(newStateBytes),
    client.UpdateStateOptions{IfETag: etag, IfVersion: version})
if err != nil { log.Fatal(err) }
cli.Release(ctx, client.ReleaseRequest{Key: "orders", LeaseID: lease.LeaseID})
```

To connect over a Unix domain socket (useful when the server runs on the same
host), point the client at `unix:///path/to/lockd.sock`:

```go
cli, err := client.New("unix:///var/run/lockd.sock")
if err != nil { log.Fatal(err) }
// run the server with --mtls=false (or supply a client bundle)
lease, err := cli.Acquire(ctx, client.AcquireRequest{Key: "orders", Owner: "worker-uds", TTLSeconds: 30})
if err != nil { log.Fatal(err) }
defer cli.Release(ctx, client.ReleaseRequest{Key: "orders", LeaseID: lease.LeaseID})
```

`Acquire` automatically retries conflicts and transient 5xx/429 responses with
exponential backoff.

### Client CLI

`lockd client` ships alongside the server binary for quick interactions with a
running cluster. Flags mirror the Go SDK defaults and honour `LOCKD_CLIENT_*`
environment variables.

```
# Acquire and release leases (exports LOCKD_CLIENT_* env vars)
eval "$(lockd client acquire --server 127.0.0.1:9341 --owner worker-1 --ttl 30s orders)"
lockd client keepalive --ttl 45s orders
lockd client release orders

# State operations / pipe through edit
lockd client get orders -o - \
  | lockd client edit status.counter++ \
  | lockd client update orders

# Atomic JSON mutations (mutate using an existing lease)
lockd client set orders progress.step=fetch progress.count++ time:progress.updated=NOW

# Local JSON helper (no server interaction)
lockd client edit checkpoint.json progress.step="done" progress.count=+5
```

The CLI auto-discovers `client*.pem` bundles under `$HOME/.lockd/` (or use
`--bundle`) and performs the same host-agnostic mTLS verification as the SDK.
`set` operates on an existing lease and accepts simple `path=value`
expressions, arithmetic updates (`++`, `--`, `=+3`), `rm:`/`delete:` prefixes to
remove keys, and `time:` prefixes for RFC3339 timestamps.

Use `-` with `--output` to stream results to standard output or with file inputs
to read from standard input (e.g. `-o -`, `lockd client update ... -`). When the
acquire command runs in text mode it prints shell-compatible `export
LOCKD_CLIENT_*=` assignments, making `eval "$(lockd client acquire ...)"` a
convenient way to populate environment variables for subsequent commands.

When `--mtls` is enabled (default) the CLI assumes HTTPS for bare `host[:port]`
values; when `--mtls=false` it assumes HTTP. Supplying an explicit
`http://...`/`https://...` URL is always honoured.

---

## Sequence Diagrams

- [![Lockd Lease Lifecycle (Overview)](docs/diagrams/lockd-overview.jpeg)](docs/diagrams/lockd-overview.jpeg)
- [![Lockd Lease Lifecycle (Disk Backend)](docs/diagrams/lockd-disk.jpeg)](docs/diagrams/lockd-disk.jpeg)
- [![Lockd Lease Lifecycle (S3 Backend)](docs/diagrams/lockd-s3.jpeg)](docs/diagrams/lockd-s3.jpeg)
- [![Lockd Lease Lifecycle (Azure Blob)](docs/diagrams/lockd-azure.jpeg)](docs/diagrams/lockd-azure.jpeg)

---

## Integration Tests

- **AWS** (`integration/aws`) – requires real S3 credentials via `.env`. Run:
  ```sh
  set -a && source .env && set +a && go test -tags "integration aws" ./integration/aws
  ```
- **Pebble** (`integration/pebble`) – fully local, runs aggressive concurrency:
  ```sh
  go test -tags "integration pebble" ./integration/pebble
  ```

---

## Roadmap

- Azure Blob backend & integration tests.
- Client helpers (auto keepalive, JSON patch utilities).
- Metrics/observability and additional diagnostics.
- Revocation tooling (`lockd auth revoke`, denylist management).

---

## License

MIT – see [`LICENSE`](LICENSE).

## Third-Party Notices

- All bundled dependencies and their original license texts are recorded in [`THIRD_PARTY_LICENSES.md`](THIRD_PARTY_LICENSES.md); regenerate the file with `go run ./cmd/licensegen` as part of the release pipeline.
- Apache-2.0 components (for example `github.com/minio/minio-go/v7`, `github.com/spf13/cobra`, `github.com/prometheus/client_golang`, AWS SDK submodules) require preserving their license text and any `NOTICE` files; both are embedded in the generated third-party bundle.
- The MPL-2.0 dependency (`github.com/hashicorp/hcl`, pulled in by Viper) allows dynamic/static linking without additional obligations, but any direct modifications to MPL-covered source must be published under MPL-2.0.
- BSD-2-Clause / BSD-3-Clause libraries (e.g. `github.com/cockroachdb/pebble`, `golang.org/x/*`, `github.com/google/uuid`) and MIT-licensed packages are satisfied by retaining their copyright and disclaimer text inside the third-party report.
- When distributing binaries or container images, ship the `LICENSE` file together with `THIRD_PARTY_LICENSES.md` to meet attribution requirements.
### Disk (SSD/NVMe)

- Streams JSON payloads directly to files beneath the store root, hashing on the fly to produce deterministic ETags.
- Keeps metadata in per-key JSON documents; state lives under `state/<encoded-key>/data`.
- Optional retention (`--disk-retention`, `LOCKD_DISK_RETENTION`) prunes keys whose metadata `updated_at_unix` is older than the configured duration. Set to `0` (default) to keep data indefinitely.
- The janitor sweep interval defaults to half the retention window (clamped between 1 minute and 1 hour). Override via `--disk-janitor-interval`.
- Configure with `--store disk:///var/lib/lockd-data`. All files live beneath the specified root; lockd creates `meta/`, `state/`, and `tmp/` directories automatically.

### Azure Blob Storage

- URL format: `azure://<account>/<container>/<optional-prefix>`.
- Supply credentials via flags/env vars (`--azure-account`, `--azure-key`, `--azure-sas-token`, `LOCKD_AZURE_ACCOUNT`, `LOCKD_AZURE_ACCOUNT_KEY`, `LOCKD_AZURE_SAS_TOKEN`). If the account name is present in the URL host it does not need to be repeated via flags.
- Default endpoint is `https://<account>.blob.core.windows.net`; override with `--azure-endpoint` or `?endpoint=` on the store URL when using custom domains/emulators.
- Authentication supports either account keys (Shared Key) or SAS tokens. Set `LOCKD_AZURE_ACCOUNT_KEY` for the former, or `LOCKD_AZURE_SAS_TOKEN` for the latter.
- Example:

```sh
set -a && source .env.azure && set +a
lockd --store "azure://lockdintegration/container/pipelines" --listen :9341 --mtls=false
```
