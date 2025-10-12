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
- **Simple HTTP/JSON API** (no gRPC) capable of running with or without TLS.
- **Storage backends**
  - **S3 / S3-compatible** object stores using a conditional copy pattern.
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
3. **Update state** – `POST /v1/update_state` → upload new JSON with
   `X-If-Version` and/or `X-If-State-ETag` to enforce CAS.
4. **Release** – `POST /v1/release` → release lease; sweeper handles timeouts.

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

# IPv4-only binding
lockd --listen-proto tcp4 --listen 0.0.0.0:9341 --store "$LOCKD_STORE"
```

The default listen address is `:9341`, chosen from the unassigned IANA space to
avoid clashes with common cloud-native services.

### Configuration files

`lockd` can also read a YAML configuration file (loaded via Viper). Generate a
template with sensible defaults using the helper command:

```sh
lockd config gen            # writes $HOME/.lockd/config.yaml
lockd config gen --stdout   # print the template instead of writing a file
lockd config gen --out /tmp/lockd.yaml --force
```

The generated file contains the same keys as the CLI flags (for example
`listen-proto`, `json-max`, `s3-region`, `s3-disable-tls`). When present, the configuration file
is read before environment variables so you can override individual settings via
`LOCKD_*` exports or command-line flags.

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

Health endpoints:

- `/healthz` – liveness probe
- `/readyz` – readiness probe

### Storage verification

`lockd verify store` validates credentials (list/get/put/delete) and prints a
suggested IAM policy when access fails.

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
state, etag, version, err := cli.GetState(ctx, "orders", lease.LeaseID)
...
_, err = cli.UpdateState(ctx, "orders", lease.LeaseID, newStateBytes,
    client.UpdateStateOptions{IfETag: etag, IfVersion: version})
...
cli.Release(ctx, client.ReleaseRequest{Key: "orders", LeaseID: lease.LeaseID})
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
