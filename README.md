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
lockd \
  --listen :8443 \
  --store s3://my-bucket/prefix \
  --json-max 100MB \
  --default-ttl 30s \
  --max-ttl 2m \
  --acquire-block 60s \
  --sweeper-interval 5s \
  --bundle $HOME/.lockd/server.pem
```

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
# Generate CA + server certificate bundle
lockd auth new server --hosts "lockd.example.com,127.0.0.1"

# Issue a new client certificate signed by the bundle CA
lockd auth new client --cn worker-1

# Revoke previously issued client certificates (by serial number)
lockd auth revoke client <hex-serial> [<hex-serial>...]

# Inspect bundle details (CA, server cert, denylist)
lockd auth inspect
```

The commands default to `$HOME/.lockd/`, creating the directory with 0700 and
files with 0600 permissions. Use `--out` to override and `--force` to overwrite
existing files.

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
