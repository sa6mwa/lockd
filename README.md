# lockd

> ⚠️ **This is v0.0, beware of bugs and breaking changes.**

`lockd` is a single-binary coordination service that delivers **exclusive
leases**, **atomic JSON state**, and an **at-least-once queue** for distributed
workers. It is intentionally small, CGO-free, suitable for static linking (e.g.
`FROM scratch` images), and runs cleanly as PID 1 inside containers or minimal
VMs.

Typical flow:

1. A worker acquires a lease for a key (e.g. `orders`).
2. The worker reads the last committed JSON state / checkpoint.
3. Work is performed, the JSON state is updated atomically (CAS).
4. The lease is released so the next worker can continue.

If the worker crashes or the connection drops, TTL expiry allows the next
worker to resume from the last committed state.

> **Note from the author**
>
> *lockd started as an experiment to see how far you could really go with AI today. I’ve spent over two decades working across IT operations and software development, with a strong focus on distributed systems - enough experience to lead a project like lockd, but not the time, cognitive bandwidth, or budget to build something like this on my own.
>
> What’s been achieved here genuinely amazes me. It shows what augmentation really means — how far human expertise can be amplified when combined with capable AI systems. It’s not fair to say lockd was "vibe coded"; you still need solid foundations in distributed computing. But nearly all of the code was written by an AI coding agent, while I’ve acted as the solution architect - steering design decisions, ensuring technical feasibility, and keeping the vision cohesive.
>
> The result is far beyond what I first imagined. Just a few years ago, if someone told you I built lockd in less than two weeks of spare time, you’d probably laugh. Today, you might just believe it.
>
> We’re living in an incredible moment - where building at the speed of twelve parsecs or less is no longer science fiction.*

## Features

- **Exclusive leases** (one holder per key) with configurable TTLs, keepalive,
  and a background sweeper to reap expired locks.
- **Atomic JSON state** up to ~50 MB with CAS via version + ETag headers.
- **Monotonic fencing tokens** protect against delayed clients; every lease-bound
  request must include the latest `X-Fencing-Token` issued by the server.
- **Acquire-for-update helper** wraps acquire + get + update into a single callback that
  keeps the lease alive while your function reads the snapshot and posts an
  update. The helper releases the lease automatically when the function
  returns.
- **Remove-state API** lets lease holders delete the JSON blob with the same CAS
  semantics (`X-If-Version` / `X-If-State-ETag`) used by updates.
- **Queue primitives** built atop the same storage backend offering enqueue,
  stateless/stateful dequeue, ack/nack/extend helpers, and automatic DLQ moves
  once `max_attempts` is reached.
- **Simple HTTP/JSON API** (no gRPC) capable of running with or without TLS.
- **Storage backends**
  - **S3 / S3-compatible** object stores using a conditional copy pattern.
  - **Azure Blob Storage** with Shared Key or SAS authentication.
  - **Disk** backend optimised for SSD/NVMe with optional retention. On Linux,
    queue consumers receive change notifications via inotify when the root
    filesystem is not NFS; disable with `--disk-queue-watch=false` if you prefer
    pure polling.
  - **In-memory** backend for tests.
- **Go SDK** (`client` package) with automatic retries, structured errors,
  and automatic retries for handshake failures.
- **Cobra/Viper CLI** with environment parity (`LOCKD_*`).
- **Diagnostics** (`lockd verify store`) and integration suites (AWS, MinIO, Disk, Azure, OTLP, queues).

---

## Architecture Overview

```
                                  +---------------------------+
                                  |      Storage Backends     |
                                  |  (S3 / Disk / Memory …)   |
                                  +-------------+-------------+
                                                ^
                                      encrypted I/O |
                                                |
                                  +-------------+-------------+
                                  | Storage Crypto           |
                                  | (kryptograf DEKs +       |
                                  |  optional Snappy)        |
                                  +-------------+-------------+
                                                ^
                                      CAS meta/state |
                                                |
+------------------+    HTTPS + mTLS    +-------+--------+
|  Worker Apps     |<------------------>|   HTTP API     |
|  (Go client SDK) |                    |    (lockd)     |
+---------+--------+                    +-------+--------+
          |                                     |
          | leases / queue ops                  | request dispatch
          v                                     v
+---------+---------+         queue ops   +-----+-------+
| Lease Manager &   |-------------------->| Queue Service|
| Sweeper           |                     |  (meta/state)|
+-------------------+                     +------+-------+
                                                  |
                     reschedule / ready set       |
                                                  v
                                    +-------------+-------------+
                                    | Queue Dispatcher          |
                                    | + Ready Cache             |
                                    +------+------+-------------+
                                           |    ^
                           notify/watch    |    | mark demand
                      +--------------------+----+------------------+
                      | Storage Change Feed (fsnotify/S3/mem)      |
                      +--------------------+----+------------------+
                                           |    |
                 throttle decisions        |    | usage & metrics
+------------------+                +------+----+------+
| QRF (perimeter)  |<---------------|  HTTP API routes |
+------------------+                +------------------+
         ^
         | samples
+--------+--------+
|  LSF (local)    |
|  sampling       |
+-----------------+
```

The ASCII sketch mirrors the PlantUML component diagram in
`docs/diagrams/lockd-architecture.puml`.

### Key subsystems

- **Worker apps / Go client SDK** – issue lease, state, and queue operations over HTTPS with mTLS.
- **HTTP API** – authenticates requests, applies QRF throttling, and fans out to the internal services.
- **Lease manager & sweeper** – hands out exclusive leases, keeps them alive, and reaps expired holders.
- **Queue service** – owns queue metadata, state blobs, and CAS coordination with the storage layer.
- **Dispatcher + ready cache** – multiplexes consumer demand, batches storage reads, and feeds ready messages to waiters.
- **Storage change feed** – abstracts fsnotify/S3/memory notifications into dispatcher wakeups.
- **Storage crypto** – wraps kryptograf usage for metadata and per-object DEKs, encrypting blobs before they reach the backend and decrypting them on the way out.
- **Storage backends** – pluggable CAS store for meta/state and payload blobs (S3, disk, MinIO, Azure, mem, …).
- **LSF (Local Security Force)** – samples host-level metrics and feeds them into
  **QRF (Quick Reaction Force)**, which enforces backpressure/throttling decisions.

### Request flow

1. **Acquire** – `POST /v1/acquire` → acquire lease (optionally blocking).
2. **Get state** – `POST /v1/get-state` → stream JSON state with CAS headers.
   Supply `X-Lease-ID` + `X-Fencing-Token` from the acquire response.
3. **Update state** – `POST /v1/update-state` → upload new JSON with
   `X-If-Version` and/or `X-If-State-ETag` to enforce CAS. Include the current
   `X-Fencing-Token`.
4. **Remove state (optional)** – `POST /v1/remove-state` → delete the stored JSON
   blob while holding the lease. Honor the same `X-Lease-ID`, `X-Fencing-Token`,
   and CAS headers (`X-If-Version`, `X-If-State-ETag`) as updates.
5. **Release** – `POST /v1/release` → release lease with the same fencing token;
   the sweeper handles timeouts for crashed workers.

### Atomic acquire + update helper

`AcquireForUpdate` combines the normal acquire → get → update → release flow
into a single call that takes a user-supplied function. The handler receives an
`AcquireForUpdateContext` exposing the current `StateSnapshot` along with helper
methods (`Update`, `UpdateBytes`, `Save`, `Remove`, etc.). While the handler runs the
client keeps the lease alive in the background; once the handler returns the
helper always releases the lease.

Only the initial acquire/get handshake retries automatically (respecting
`client.WithAcquireFailureRetries` and related backoff settings). After the
handler begins executing, any error is surfaced immediately so callers can
decide whether to re-run the helper.

The legacy `/v1/acquire-for-update` streaming endpoint has been removed;
all clients must use the callback helper described above.


### Internal layout

- `server.go` – server wiring, storage retry wrapper, sweeper.
- `internal/httpapi` – HTTP handlers for the API surface.
- `internal/storage` – backend interface, retry wrapper, S3/Disk/Memory.
- `client` – public Go SDK.
- `cmd/lockd` – CLI entrypoint (Cobra/Viper).
- `internal/tlsutil` – bundle loading/generation helpers.
- `integration/` – end-to-end tests (mem, disk, NFS, AWS, MinIO, Azure, OTLP, queues).

---

## Storage Backends

### Selecting a backend

`lockd` picks the storage implementation from the `--store` flag (or `LOCKD_STORE`
environment variable) by inspecting the URL scheme:

| Scheme | Example | Backend | Notes |
|--------|---------|---------|-------|
| `mem://` or empty | `mem://` | In-memory | Ephemeral; test only. |
| `aws://` | `aws://my-bucket/prefix` | AWS S3 | Provide AWS credentials via `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (and optional `AWS_SESSION_TOKEN`). Set the region via `--aws-region`, `LOCKD_AWS_REGION`, `AWS_REGION`, or `AWS_DEFAULT_REGION`. |
| `s3://` | `s3://localhost:9000/lockd-data?insecure=1` | S3-compatible (MinIO, Localstack, etc.) | TLS **enabled by default**. Append `?insecure=1` for HTTP. Supply credentials with `LOCKD_S3_ACCESS_KEY_ID`/`LOCKD_S3_SECRET_ACCESS_KEY` (falls back to `LOCKD_S3_ROOT_USER`/`LOCKD_S3_ROOT_PASSWORD`). |
| `disk://` | `disk:///var/lib/lockd-data` | SSD/NVMe-tailored disk backend | Stores state/meta beneath the provided root; optional retention window. |
| `azure://` | `azure://account/container/prefix` | Azure Blob Storage | Account name in host, container + optional prefix in path. Authentication via account key or SAS token. |

For AWS, the standard credential chain (`AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`, profiles, IAM roles, etc.) is used. For other
S3-compatible stores, `lockd` reads `LOCKD_S3_ACCESS_KEY_ID` and
`LOCKD_S3_SECRET_ACCESS_KEY` (falling back to
`LOCKD_S3_ROOT_USER`/`LOCKD_S3_ROOT_PASSWORD`). No secret keys are stored in the
`lockd` config file.

### S3 / S3-compatible

- Uses temp uploads + `CopyObject` with conditional headers for CAS.
- Supports SSE (`aws:kms` / `AES256`) and custom endpoints (MinIO, Localstack).
- Default retry budget: 12 attempts, 500 ms base delay, capped at 15 s.

Configuration (flags or env via `LOCKD_` prefix):

| Flag / Env                 | Description                                   |
|---------------------------|-----------------------------------------------|
| `--store` / `LOCKD_STORE` | `aws://bucket/prefix` or `s3://host:port/bucket` |
| `--aws-region`            | AWS region for `aws://` stores (required unless provided via env) |
| `--s3-sse`                | `AES256` or `aws:kms`                          |
| `--s3-kms-key-id`         | KMS key for generic `s3://` stores             |
| `--aws-kms-key-id`        | KMS key for `aws://` stores                    |
| `--s3-max-part-size`      | Multipart upload part size                     |

### Memory

- In-process backend used for unit tests.

---

## Configuration & CLI

`lockd` exposes flags mirrored by `LOCKD_*` environment variables. Example:

```sh
# AWS S3 with region-based endpoint
export LOCKD_STORE="aws://my-bucket/prefix"
export LOCKD_AWS_REGION="us-west-2"
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
lockd \
  --listen :9341 \
  --store "$LOCKD_STORE" \
  --json-max 100MB \
  --default-ttl 30s \
  --max-ttl 30m \
  --acquire-block 60s \
  --sweeper-interval 5s \
  --bundle $HOME/.lockd/server.pem

# MinIO running locally over HTTP
export LOCKD_STORE="s3://localhost:9000/lockd-data?insecure=1"
export LOCKD_S3_ACCESS_KEY_ID="minioadmin"
export LOCKD_S3_SECRET_ACCESS_KEY="minioadmin"
lockd --store "$LOCKD_STORE" --listen :9341 --bundle $HOME/.lockd/server.pem

# Azure Blob Storage (account key)
set -a && source .env.azure && set +a
# .env.azure should export LOCKD_STORE=azure://account/container/prefix and LOCKD_AZURE_ACCOUNT_KEY=...
lockd --store "$LOCKD_STORE" --listen :9341 --mtls=false

# IPv4-only binding
lockd --listen-proto tcp4 --listen 0.0.0.0:9341 --store "$LOCKD_STORE"

# Prefer stdlib JSON compaction for tiny payloads
lockd --store mem:// --json-util stdlib
```

The default listen address is `:9341`, chosen from the unassigned IANA space to
avoid clashes with common cloud-native services.

#### Queue dispatcher tuning

The queue dispatcher multiplexes storage polling across all connected consumers.
Use these flags (or matching `LOCKD_QUEUE_*` environment variables) to adjust
its behaviour:

| Flag / Env | Description |
|------------|-------------|
| `--queue-max-consumers` (`LOCKD_QUEUE_MAX_CONSUMERS`) | Caps the number of concurrent dequeue waiters handled by a single server (default `1000`). |
| `--queue-poll-interval` (`LOCKD_QUEUE_POLL_INTERVAL`) | Baseline interval between storage scans when no change notifications arrive (default `3s`). |
| `--queue-poll-jitter` (`LOCKD_QUEUE_POLL_JITTER`) | Adds randomised delay up to the specified duration to stagger concurrent servers (default `500ms`; set `0` to disable). |
| `--disk-queue-watch` (`LOCKD_DISK_QUEUE_WATCH`) | Enables Linux/inotify queue watchers on the disk backend (default `true`; automatically ignored on unsupported filesystems such as NFS). |
| `--mem-queue-watch` (`LOCKD_MEM_QUEUE_WATCH`) | Enables in-process queue notifications for the in-memory backend (default `true`; disable to force pure polling). |

### Observability (OTLP)

Set `--otlp-endpoint` (or `LOCKD_OTLP_ENDPOINT`) to export traces and metrics to
an OTLP collector. Omit the scheme to default to OTLP/gRPC on port `4317`; use
`grpc://`/`grpcs://` to force gRPC, or `http://`/`https://` (default port `4318`)
for the HTTP transport. For the provided `observability/docker-compose.yaml`
collector:

```sh
LOCKD_STORE=mem:// \
LOCKD_OTLP_ENDPOINT=localhost \
lockd
```

All HTTP endpoints are wrapped with `otelhttp`, storage backends emit child
spans, and structured logs attach `trace_id`/`span_id` when a span is active.

### Correlation IDs

Every request processed by lockd carries a correlation identifier:

- Incoming clients may provide `X-Correlation-Id`; the server accepts printable
  ASCII values up to 128 characters. Invalid identifiers are ignored and a fresh
  UUID is generated instead.
- Successful acquire responses include `correlation_id` in the JSON payload and
  echo `X-Correlation-Id` in the response headers. Follow-up operations must
  continue sending the same header.
- Spans exported via OTLP include the `lockd.correlation_id` attribute so traces
  and logs can be tied together.
- Queue enqueue/dequeue responses and the ack/nack/extend APIs echo
  `correlation_id`, preserving the identifier across retries, DLQ moves, and
  stateful leases.
- The Go client automatically propagates correlation IDs. Seed a context with
  `client.WithCorrelationID(ctx, id)` (use `client.GenerateCorrelationID()` to
  mint one) and all lease/session operations will emit the header. For generic
  API clients, wrap an `http.Client` with `client.WithCorrelationHTTPClient` or
  a custom transport via `client.WithCorrelationTransport` to overwrite
  `X-Correlation-Id` on every request. `client.CorrelationIDFromResponse`
  extracts the identifier from HTTP responses when needed.

### Lease fencing tokens

Every successful `acquire` response includes a `fencing_token` and echoing
`X-Fencing-Token` on follow-up requests is mandatory. The Go SDK manages the
token automatically when you reuse the same `client.Client`. For CLI workflows
you can export the token so subsequent commands pick it up:

```sh
eval "$(lockd client acquire --server localhost:9341 --owner worker --key orders)"
lockd client keepalive --lease "$LOCKD_CLIENT_LEASE_ID" --key orders
lockd client update --lease "$LOCKD_CLIENT_LEASE_ID" --fencing-token "$LOCKD_CLIENT_FENCING_TOKEN" --key orders payload.json
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

### Client retries & callback behaviour

Handshake retries for `AcquireForUpdate` are bounded by `DefaultFailureRetries`
(currently 5). Each time the initial acquire/get sequence encounters
`lease_required`, throttling, or a transport error, the helper burns one retry
according to the configured backoff (`client.WithAcquireFailureRetries`,
`client.WithAcquireBackoff`). Once the handler starts running, any subsequent
error is returned immediately so the caller can decide whether to invoke the
helper again.

While the handler executes, the helper issues keepalives at half the TTL. A
failed keepalive cancels the handler’s context, surfaces the error, and the
helper releases the lease (treating `lease_required` as success). Other client
calls (`GetState`, `UpdateState`, `KeepAlive`, `Release`) continue to surface
`lease_required` immediately so callers can choose their own retry strategy.

For multi-host deployments, build clients with
`client.NewWithEndpoints([]string{...})`. The SDK rotates through the supplied
base URLs when a request fails, carrying the same bounded retry budget across
endpoints, so failovers remain deterministic.

### Configuration files

`lockd` can also read a YAML configuration file (loaded via Viper). At start-up
the server looks for `$HOME/.lockd/config.yaml`; use `-c/--config` (or
`LOCKD_CONFIG`) to point at an alternate file:

```sh
lockd -c /etc/lockd/config.yaml
# or
LOCKD_CONFIG=/etc/lockd/config.yaml lockd
```

Generate a
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
  stateless edge workers using short leases and protobuf metadata blobs.
- **IoT rollout controller** – drive firmware or configuration rollouts where
  each device claims work and reports progress exactly once.
- **Distributed cron / windowing** – serialize recurring jobs (per key) so
  retries don’t overlap, while keeping per-run state directly in lockd.

Acquire-for-update is particularly useful for these scenarios because the state
reader holds the lease while a worker inspects the JSON payload. Once it computes
the next cursor it can call `UpdateState` followed by `Release`, avoiding any race
window between separate update and release calls.

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
`LOCKD_STORE=s3://localhost:9000/lockd-integration?insecure=1` for a default
local setup, or point it at HTTPS by omitting the `?insecure=1` query string.

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
- `LOCKD_NFS_ROOT` – absolute path to an NFS mount (optional but required
  for the NFS benchmark). If both `/mnt/nfs4-lockd` and `/mnt/nfs-lockd` are
  unset/unavailable the test fails.

### In-memory queue benchmarks

The `benchmark/mem/lq` package measures dispatcher throughput using the
in-process `mem://` backend. Each case launches real servers and clients so the
numbers reflect the full subscribe/ack handshake rather than synthetic mocks:

```sh
go test -bench . -tags "bench mem lq" ./benchmark/mem/lq
```

Scenarios currently included:

| Name | Description |
| --- | --- |
| `single_server_prefetch1_100p_100c` | 100 producers / 100 consumers on one server with subscribe prefetch=1 (baseline). |
| `single_server_prefetch4_100p_100c` | Same workload with prefetch batches of four (default tuning). |
| `single_server_subscribe_100p_1c` | High fan-in into a single blocking subscriber (prefetch=16). |
| `single_server_dequeue_guard` | Legacy dequeue path kept as a regression guard. |
| `double_server_prefetch4_100p_100c` | Two servers sharing the same mem store to verify routing/failover performance. |

Only the total messages and enqueue/dequeue rates are printed by default so CI
stays readable. Extra instrumentation can be toggled per run:

- `MEM_LQ_BENCH_EXTRA=1` – export latency-derived metrics such as
  `dequeue_ms/op`, `ack_ms/op`, `messages_per_batch`, and
  `dequeue_gap_max_ms`.
- `MEM_LQ_BENCH_DEBUG=1` – emit verbose client logs (including stale/idempotent
  ACKs) when chasing data races.
- `MEM_LQ_BENCH_TRACE=1` – attach the trace logger to both servers; combine
  with `LOCKD_READY_CACHE_TRACE=1` to trace ready-cache refreshes.
- `MEM_LQ_BENCH_TRACE_GAPS=1` – print per-delivery gaps over 10 ms to stdout.
- `MEM_LQ_BENCH_PRODUCERS`, `MEM_LQ_BENCH_CONSUMERS`,
  `MEM_LQ_BENCH_MESSAGES`, `MEM_LQ_BENCH_PREFETCH`, and
  `MEM_LQ_BENCH_WARMUP` – override the baked-in workload sizes without editing
  the source file.
- `MEM_LQ_BENCH_CPUPROFILE=/tmp/cpu.pprof` – capture a CPU profile for the run.

These knobs let you keep the fast, quiet default for routine regression runs
while enabling deep tracing when profiling throughput locally.

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

lease, err := inproc.Acquire(ctx, api.AcquireRequest{Key: "jobs", Owner: "worker", TTLSeconds: 15})
if err != nil { log.Fatal(err) }
defer inproc.Release(ctx, api.ReleaseRequest{Key: "jobs", LeaseID: lease.LeaseID})
```

Use `client.BlockWaitForever` (default) to wait indefinitely when acquiring, or `client.BlockNoWait` to fail immediately if the lease is already held.

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
sess, err := cli.Acquire(ctx, api.AcquireRequest{Key: "demo", Owner: "worker", TTLSeconds: 20})
if err != nil { log.Fatal(err) }
defer sess.Close()
```

Health endpoints:

- `/healthz` – liveness probe
- `/readyz` – readiness probe

### Test server helper & chaos proxy

The repository includes a dedicated testing harness (`lockd.NewTestServer`) that
starts a fully configured server, returns a shutdown function, and can emit logs
through `testing.T`. It also accepts a `ChaosConfig` that injects bounded
latency, drops, or a single forced disconnect via an in-process TCP proxy. This
is used heavily by the Go client’s integration tests to validate
AcquireForUpdate failover logic across multiple endpoints.

### Storage verification

`lockd verify store` validates credentials (list/get/put/delete) and prints a
suggested IAM policy when access fails. Disk backends run a multi-replica
simulation (metadata CAS and payload writes) so locking bugs or stale CAS tokens
fail fast.

```sh
# Verify a disk mount before starting the server
LOCKD_STORE=disk:///var/lib/lockd lockd verify store

# Verify Azure Blob credentials (requires LOCKD_AZURE_ACCOUNT_KEY or LOCKD_AZURE_SAS_TOKEN)
LOCKD_STORE=azure://lockdaccount/lockd-container LOCKD_AZURE_ACCOUNT_KEY=... lockd verify store
```

When `--store` uses `disk://`, the same verification runs automatically during
server startup and the process exits if any check fails.

## TLS (mTLS)

mTLS is **enabled by default**. `lockd` looks for a bundle at
`$HOME/.lockd/server.pem` unless `--bundle` points elsewhere. Disable with
`--mtls=false` (testing only).

Bundle format (PEM concatenated):

1. CA certificate (trust anchor)
2. Server certificate (leaf + chain)
3. Server private key
4. Optional denylist block (`LOCKD DENYLIST`)

The CA private key lives in `ca.pem` and should be stored securely. Keep it
offline when possible; only the CA certificate is bundled with each server.

### Generating certificates

```sh
# Create a Certificate Authority bundle (ca.pem)
lockd auth new ca --cn lockd-root

# Issue a server certificate signed by the CA
lockd auth new server --ca-in $HOME/.lockd/ca.pem --hosts "lockd.example.com,127.0.0.1"

# Issue a new client certificate signed by the bundle CA
lockd auth new client --ca-in $HOME/.lockd/ca.pem --cn worker-1

# Revoke previously issued client certificates (by serial number)
lockd auth revoke client <hex-serial> [<hex-serial>...]

# Inspect bundle details (CA, server cert, denylist)
lockd auth inspect server        # lists all server*.pem bundles
lockd auth inspect client --in $HOME/.lockd/client-*.pem

# Verify bundles (validity, EKUs, denylist enforcement)
lockd auth verify server        # scans all server*.pem in the config dir
lockd auth verify client --server-in $HOME/.lockd/server.pem
```

The commands default to `$HOME/.lockd/`, creating the directory with 0700 and
files with 0600 permissions. Use `--out`/`--ca-in`/`--force` to override file
locations. `ca.pem` contains the trust anchor + private key and is intended to
be stored in a secure location separate from the server runtime bundle.

When `lockd auth new server` writes to the default location and `server.pem`
already exists, the CLI automatically picks the next available name
(`server02.pem`, `server03.pem`, …) so existing bundles are preserved without
requiring `--force`.

Client issuance follows the same pattern: the first default bundle is written
to `client.pem`, then `client02.pem`, `client03.pem`, and so on when rerun.

`lockd auth verify` ensures that the server bundle presents a CA + ServerAuth
certificate (with matching server private key) and that client bundles were
issued by the same CA and are not present on the denylist.

`lockd auth revoke client` updates the denylist for every `server*.pem` bundle
in the same directory as the referenced server bundle so multi-replica nodes
block revoked certificates consistently. Pass `--propagate=false` to limit the
update to just the specified bundle when needed (e.g. staging experiments).

## Storage Encryption

All metadata, lock state JSON blobs, and queue payloads are encrypted at rest by
default using [pkt.systems/kryptograf](https://pkg.go.dev/pkt.systems/kryptograf).
When you run `lockd auth new ca` the CLI generates a kryptograf root key and a
global metadata descriptor and embeds them alongside the CA certificate/key in
`ca.pem`. Subsequent `lockd auth new server` invocations propagate the same
material into each `server.pem` bundle so every replica can reconstruct the
metadata DEK on startup.

At runtime the server mints per-object DEKs (one per lock state blob, per queue
message metadata document, and per queue payload) derived from stable
contexts (e.g. `state:<key>`, `queue-meta:q/<queue>/msg/<id>.pb`,
`queue-payload:q/<queue>/msg/<id>.bin`). Descriptors for these DEKs are stored
alongside the objects themselves so reads remain stateless. Encrypted objects
use deterministic content-types:

- Metadata protobuf: `application/vnd.lockd+protobuf-encrypted`
- JSON state: `application/vnd.lockd+json-encrypted`
- Queue payloads / DLQ binaries: `application/vnd.lockd.octet-stream+encrypted`

Disable encryption (testing only) with `--storage-encryption=false` (or
`LOCKD_STORAGE_ENCRYPTION=false`). Optional Snappy compression is available via
`--storage-encryption-snappy`; when encryption is disabled, the original
content-types (`application/x-protobuf`, `application/json`,
`application/octet-stream`) are restored automatically.

`lockd verify store` now exercises the decrypt path by reading (or, when the
store is empty, synthesising and deleting) sample metadata/state and queue
objects. Failures surface immediately so misconfigured bundles or mismatched
descriptors are caught during deployment. Because storage encryption is tied to
the bundle, servers must load `server.pem` even when mTLS is disabled.

## Go Client Usage

```go
cli, err := client.New("https://lockd.example.com")
if err != nil { log.Fatal(err) }
sess, err := cli.Acquire(ctx, api.AcquireRequest{
    Key:        "orders",
    Owner:      "worker-1",
    TTLSeconds: 30,
    BlockSecs:  client.BlockWaitForever,
})
if err != nil { log.Fatal(err) }
defer sess.Close()

var payload struct {
    Data []byte
    Counter int
}

if err := sess.Load(ctx, &payload); err != nil { log.Fatal(err) }
payload.Counter++
if err := sess.Save(ctx, payload); err != nil { log.Fatal(err) }

// Customise timeouts (HTTP requests + close/release window) when constructing the client:
cli, err := client.New(
    "https://lockd.example.com",
    client.WithHTTPTimeout(30*time.Second),
    client.WithCloseTimeout(10*time.Second),
    client.WithKeepAliveTimeout(8*time.Second),
)
```

To connect over a Unix domain socket (useful when the server runs on the same
host), point the client at `unix:///path/to/lockd.sock`:

```go
cli, err := client.New("unix:///var/run/lockd.sock")
if err != nil { log.Fatal(err) }
// run the server with --mtls=false (or supply a client bundle)
sess, err := cli.Acquire(ctx, api.AcquireRequest{Key: "orders", Owner: "worker-uds", TTLSeconds: 30})
if err != nil { log.Fatal(err) }
defer sess.Close()
```

`Acquire` automatically retries conflicts and transient 5xx/429 responses with
exponential backoff.

### Client CLI

`lockd client` ships alongside the server binary for quick interactions with a
running cluster. Flags mirror the Go SDK defaults and honour `LOCKD_CLIENT_*`
environment variables.

```
# Acquire and release leases (exports LOCKD_CLIENT_* env vars)
eval "$(lockd client acquire --server 127.0.0.1:9341 --owner worker-1 --ttl 30s --key orders)"
lockd client keepalive --ttl 45s --key orders
lockd client release --key orders

# State operations / pipe through edit
lockd client get --key orders -o - \
  | lockd client edit status.counter++ \
  | lockd client update --key orders
lockd client remove --key orders

# Atomic JSON mutations (mutate using an existing lease)
lockd client set --key orders progress.step=fetch progress.count++ time:progress.updated=NOW

# Local JSON helper (no server interaction)
lockd client edit checkpoint.json progress.step="done" progress.count=+5
```

Every `lockd client` subcommand accepts an optional `--key` (`-k`) flag. When
you omit `--key`, the command falls back to `LOCKD_CLIENT_KEY` (typically set
by the most recent `acquire`). Invoking `acquire` without `--key` requests a
server-generated identifier; the resulting value is exported via
`LOCKD_CLIENT_KEY` so follow-up calls can rely on the environment.

### Queue operations

Queue commands ship alongside the standard lease helpers:

```sh
# Enqueue a JSON payload (stdin, --data, or --file)
lockd client queue enqueue \
  --queue orders \
  --content-type application/json \
  --data '{"op":"ship","order_id":42}'

# Dequeue and export LOCKD_QUEUE_* environment variables
eval "$(lockd client queue dequeue --queue orders --owner worker-1)"
printf 'payload stored at %s\n' "$LOCKD_QUEUE_PAYLOAD_PATH"

# Use the exported metadata to ack/nack/extend
lockd client queue ack
lockd client queue nack --delay 15s --reason "upstream retry"
lockd client queue extend --extend 45s
```

`queue dequeue` supports a `--stateful` flag which acquires both the message
and workflow state leases; the exported `LOCKD_QUEUE_STATE_*` variables align
with the fields consumed by `queue ack`/`nack`/`extend`.

Payloads are streamed directly to disk. When `--payload-out` is omitted the CLI
creates a temporary file and exports its location via
`LOCKD_QUEUE_PAYLOAD_PATH`, making it easy to hand off large bodies to other
tools without buffering in memory.

The CLI auto-discovers `client*.pem` bundles under `$HOME/.lockd/` (or use
`--bundle`) and performs the same host-agnostic mTLS verification as the SDK.
`set` operates on an existing lease and accepts simple `path=value`
expressions, arithmetic updates (`++`, `--`, `=+3`), `rm:`/`delete:` prefixes to
remove keys, and `time:` prefixes for RFC3339 timestamps.

Timeout knobs mirror the Go client: `--timeout` (HTTP dial+request),
`--close-timeout` (release window), and `--keepalive-timeout`
(`LOCKD_CLIENT_TIMEOUT`, `LOCKD_CLIENT_CLOSE_TIMEOUT`, and
`LOCKD_CLIENT_KEEPALIVE_TIMEOUT` respectively).

Use `-` with `--output` to stream results to standard output or with file inputs
to read from standard input (e.g. `-o -`, `lockd client update ... -`). When the
acquire command runs in text mode it prints shell-compatible `export
LOCKD_CLIENT_*=` assignments, making `eval "$(lockd client acquire ...)"` a
convenient way to populate environment variables for subsequent commands.

When `--mtls` is enabled (default) the CLI assumes HTTPS for bare `host[:port]`
values; when `--mtls=false` it assumes HTTP. Supplying an explicit
`http://...`/`https://...` URL is always honoured.

## Sequence Diagrams

- [![Lockd Lease Lifecycle (Overview)](docs/diagrams/lockd-overview.jpeg)](docs/diagrams/lockd-overview.jpeg)
- [![Lockd Lease Lifecycle (Disk Backend)](docs/diagrams/lockd-disk.jpeg)](docs/diagrams/lockd-disk.jpeg)
- [![Lockd Lease Lifecycle (S3 Backend)](docs/diagrams/lockd-s3.jpeg)](docs/diagrams/lockd-s3.jpeg)
- [![Lockd Lease Lifecycle (Azure Blob)](docs/diagrams/lockd-azure.jpeg)](docs/diagrams/lockd-azure.jpeg)

## Integration Tests

Integration suites are selected via build tags. Every run must include the
`integration` tag plus one (or more) backend tags; optional feature tags narrow
the scope further. The general pattern is:

```sh
go test -tags "integration <backend> [feature ...]" ./integration/...
```

Current backends:

| Backend | Notes | Examples |
|---------|-------|----------|
| `mem`   | Uses the in-memory store; no environment needed. | `go test -tags "integration mem" ./integration/...` (all mem suites) / `go test -tags "integration mem lq" ./integration/...` (queue scenarios only). |
| `disk`  | Local disk backend. Requires `.env.disk` (see `integration/disk`). | `set -a && source .env.disk && set +a && go test -tags "integration disk" ./integration/...` / `... -tags "integration disk lq" ...` for queue-only coverage. |
| `nfs`   | Disk backend mounted on NFS. Source `.env.nfs` so `LOCKD_NFS_ROOT` is set. | `set -a && source .env.nfs && set +a && go test -tags "integration nfs lq" ./integration/...`. |
| `aws`   | Real S3 credentials via `.env`. | `set -a && source .env && set +a && go test -tags "integration aws" ./integration/...`. |
| `minio`, `azure` | S3-compatible / Azure Blob suites. | e.g. `go test -tags "integration minio" ./integration/...` (requires appropriate env). |

The queue-specific feature tag is `lq`. A suite built with `integration && mem
&& lq`, for example, only compiles the queue wrappers in `integration/mem/lq`.
We’ll extend the same layout to the AWS, Azure, and MinIO queue suites next so
`go test -tags "integration aws lq" ./integration/...` (and similar) will target
their queue scenarios without running unrelated tests.

For the Go client’s AcquireForUpdate failover tests:

```sh
go test -tags integration -run 'TestAcquireForUpdateCallback(SingleServer|FailoverMultiServer)' ./client
```

These harnesses ship with watchdog timers that panic after 5–10 s and print
full goroutine dumps via `runtime.Stack`, making hangs immediately actionable—
retain these guards whenever tests are updated.

## Roadmap

- Azure Blob backend & integration tests.
- Client helpers (auto keepalive, JSON patch utilities).
- Metrics/observability and additional diagnostics.
- Revocation tooling (`lockd auth revoke`, denylist management).

## License

MIT – see [`LICENSE`](LICENSE).

## Third-Party Notices

- All bundled dependencies and their original license texts are recorded in [`THIRD_PARTY_LICENSES.md`](THIRD_PARTY_LICENSES.md); regenerate the file with `go run ./cmd/licensegen` as part of the release pipeline.
- Apache-2.0 components (for example `github.com/minio/minio-go/v7`, `github.com/spf13/cobra`, `github.com/prometheus/client_golang`, AWS SDK submodules) require preserving their license text and any `NOTICE` files; both are embedded in the generated third-party bundle.
- The MPL-2.0 dependency (`github.com/hashicorp/hcl`, pulled in by Viper) allows dynamic/static linking without additional obligations, but any direct modifications to MPL-covered source must be published under MPL-2.0.
- BSD-2-Clause / BSD-3-Clause libraries (e.g. `golang.org/x/*`, `github.com/google/uuid`) and MIT-licensed packages are satisfied by retaining their copyright and disclaimer text inside the third-party report.
- When distributing binaries or container images, ship the `LICENSE` file together with `THIRD_PARTY_LICENSES.md` to meet attribution requirements.

### Disk (SSD/NVMe)

- Streams JSON payloads directly to files beneath the store root, hashing on the fly to produce deterministic ETags.
- Keeps metadata in per-key protobuf documents; state lives under `state/<encoded-key>/data`.
- Optional retention (`--disk-retention`, `LOCKD_DISK_RETENTION`) prunes keys whose metadata `updated_at_unix` is older than the configured duration. Set to `0` (default) to keep data indefinitely.
- The janitor sweep interval defaults to half the retention window (clamped between 1 minute and 1 hour). Override via `--disk-janitor-interval`.
- Configure with `--store disk:///var/lib/lockd-data`. All files live beneath the specified root; lockd creates `meta/`, `state/`, and `tmp/` directories automatically.

### Azure Blob Storage

- URL format: `azure://<account>/<container>/<optional-prefix>` (the account comes from the host component).
- Supply credentials via `--azure-key` / `LOCKD_AZURE_ACCOUNT_KEY` (Shared Key) or `--azure-sas-token` / `LOCKD_AZURE_SAS_TOKEN` (SAS). Standard Azure environment variables such as `AZURE_STORAGE_ACCOUNT` are also honoured if the account is omitted from the URL.
- Default endpoint is `https://<account>.blob.core.windows.net`; override with `--azure-endpoint` or `?endpoint=` on the store URL when using custom domains/emulators.
- Authentication supports either account keys (Shared Key) or SAS tokens. Provide exactly one of the above secrets; the CLI no longer requires `--azure-account` because the account name is embedded in the store URL.
- Example:

```sh
set -a && source .env.azure && set +a
lockd --store "azure://lockdintegration/container/pipelines" --listen :9341 --mtls=false
```
