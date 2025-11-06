# AGENTS.md

## Purpose

This repo delivers a tiny, single-binary **lock + state + queue** service—“just enough etcd”—focused on **exclusive leases**, **atomic JSON state**, and an **at-least-once queue**. It coordinates workers so **only one holder at a time** can read/advance a shared checkpoint or dequeue a message (JSON up to ~100 MB by default, configurable). It favors reliability and simplicity, runs well in `FROM scratch`, and avoids POSIX assumptions.

## Context (what matters)

* **Language**: Go.
* **Runtime**: `FROM scratch` via `pkt.systems/psi`; CGO-free static build.
* **Concurrency**: v0 is **one holder per key** (no `limit>1`).
* **Transport**: `net/http` (HTTP/2 via TLS).
* **Auth**: **mTLS by default**. We **trust identities, not hostnames** (host-agnostic mTLS).
* **Config**: cobra CLI + viper (env prefix `LOCKD_`).
* **Layout**:

* `/` → server library (export `lockd.Server`, `lockd.Config`, queue dispatcher helpers, etc.).
* `cmd/lockd/` → main server app using the library and bundling the CLI (leases + queue commands).
* `client/` → Go client SDK (`client.New(...)`) plus CLI helpers.

## General guidelines (no hard requirements)

- We usually prefer kebab-case for URI paths, but the v1 API now ships `/v1/get`, `/v1/update`, and `/v1/remove` for brevity; match whatever the public surface uses.
- We like all integration tests to finish within 120 seconds in order to iterate fast, adjust technique (not goal of the test) to this constraint (for example timeouts, retries and exponential backoff)
- We like unit tests to be fast and snappy; a total go test ./... run should finish in less than 10 seconds
- We need good concurrency, fuzz, and race testing, but do not over-do it, instead split the concurrency testing into smaller loads (e.g: one test for two concurrent workers competing for the same lock, then ramp up to 5 concurrent worker in another test)
- Always add a test case per storage backend with two servers using the same storage endpoint and at least two workers connected to each competing for the lock (beware of http.Client timeouts
- Import `pkt.systems/pslog` as `pslog`; do not alias it to shorter names unless a conflict forces it.

## Workflow notes

* Record every performance-related experiment (benchmarks, profiling-driven optimizations, regression checks) in `docs/performance/` as a dated markdown note. Include the commands used, environment details, before/after metrics, and a short description of the code changes that produced the measurements.
* As new features land, record outstanding integration/performance/UX testing tasks in `BACKLOG.md` so we can track follow-up work.
* When wiring new cloud storage backends, expose configuration via both CLI flags and `LOCKD_*` environment variables, mirroring the behaviour of existing backends (S3/MinIO/Azure). Document how to source `.env.*` helper files in README.md.

## Debugging guardrails

* **Watchdogs everywhere** – Long-running unit or integration tests (AcquireForUpdate, queue chaos, etc.) must install watchdog timers that panic after 5–10 s and dump all goroutines via `runtime.Stack`. Keep these guards in place when modifying the tests so hangs always yield actionable traces.
* **Hangs are bugs** – Reproduce any reported hang with watchdogs enabled first, capture the stack trace, and only then add instrumentation. Never let suites spin indefinitely.
* **Deterministic chaos** – The chaos proxy used by test harnesses should default to a single disconnect (`MaxDisconnects=1`) when simulating network drops. This gives deterministic “one failover” coverage without thrashing the client retry logic.
* **Real clients only** – Integration tests (lease + queue) must exercise the production `client.Client` APIs exactly as shipped. Do not introduce test-only clients to “make a scenario pass”; fix the real client (and document it) so user workflows and tests stay aligned.
* **Acquire-for-update contract** – Acquire-for-update handshake retries are bounded by `DefaultFailureRetries` (default 5; override with `client.WithAcquireFailureRetries`). Keep README.md / doc.go / client/doc.go aligned when adjusting the constant, and note that other client calls surface `lease_required` immediately so callers can decide how to react.
* **Namespace + queue coverage** – Every backend-facing integration suite needs at least one test with two servers sharing the same store and multiple workers per namespace competing for the same lock/queue. This guarantees namespace wiring never regresses and queue watchers remain deterministic.
* **Observed key tracking** – Object-store backends may surface stale `LoadMeta`/`ReadState` results briefly; the handler tracks “observed” keys and uses short warm-up retries before treating a key as new. Preserve that logic (and the observed-key tracking) whenever touching storage code so we don’t accidentally wipe metadata on S3/Blob eventual reads.

## Primary use-case

A single worker holds a lease for a **key** (stream), reads the JSON checkpoint/cursor, does work, updates the checkpoint atomically (CAS), and releases. If the worker dies, lease expiry hands off to the next worker which resumes from the last committed state.

A sibling workflow feeds the **at-least-once queue**: producers enqueue payloads (optionally with workflow state), consumers dequeue with owner identities, process the payload, then ack/nack/extend using fencing-protected helpers. DLQ moves happen automatically after `max_attempts`, and queue state shares the same namespace/observed-key semantics as the lock surface.

## HTTP API (behavioral sketch, `/v1`)

Common:

* Base: `https://host:port/v1/...`
* Auth: mTLS (host-agnostic; see below).
* Headers: requests may use `X-Idempotency-Key`, `X-If-Version`, `X-If-State-ETag`; responses may include `X-Key-Version`, `ETag`.
* Errors (`4xx/5xx`):
  `{"error":"<code>","detail":"...","current_version":7,"current_etag":"abc123","retry_after_seconds":5}`

Endpoints:

* `POST /v1/acquire` → `{key, ttl_seconds, owner, block_seconds}` → `200 {lease_id,...,version}` or `409 waiting`.
* `POST /v1/keepalive` → `{lease_id, ttl_seconds}` → `200 {expires_at_unix}`.
* `POST /v1/release` → `{lease_id}` → `200 {released:true}`.
* `POST /v1/get` (requires live lease) → body streamed JSON; headers `X-Key-Version`, `ETag`.
* `POST /v1/update` (requires live lease) → raw JSON body (streamed/compacted), optional CAS headers; `200 {new_version,new_state_etag,bytes}` or `409`.
* `GET /v1/describe?key=...` → meta only (no state).
* `GET /healthz`, `GET /readyz`.

Blocking semantics: `acquire` long-polls/loops up to `block_seconds`; on timeout returns `409 waiting` with `retry_after_seconds`.

## Host-agnostic mTLS (default)

* **We trust certs, not hostnames**. CN/SANs are not used for hostname auth; certs are movable.
* Server & client both authenticate via **chain to the project CA**, **proper EKUs**, and **denylist** (revoked serials).
* **Client → Server**: custom peer verification that **skips hostname checks**, verifies chain to embedded CA and `ServerAuth` EKU.
* **Server → Client**: verifies client chain to CA, `ClientAuth` EKU, checks denylist.

**Bundles**

* `ca.pem`: CA certificate + CA private key (keep offline/safe).
* `server.pem`: CA certificate (trust anchor), server certificate, server private key, optional embedded denylist/CRL.
* `client*.pem` (one per client, auto-numbered `client.pem`, `client02.pem`, …): CA cert, client cert, client key.

**CLI (cobra)**

```
lockd auth new ca --out ca.pem --cn "lockd-root"
lockd auth new server --ca-in ca.pem --out server.pem --cn "lockd-anywhere" --hosts "*"
lockd auth new client --ca-in ca.pem --out client1.pem --cn "worker-1"
lockd auth revoke client --server-in server.pem --out server.pem <hex-serial>...
lockd auth inspect server        # lists all server*.pem bundles
lockd auth inspect client --in client1.pem
lockd auth verify server        # scans all server*.pem in config dir
lockd auth verify client --server-in server.pem --in client1.pem

# bundled client CLI (mTLS by default, auto-discovers client*.pem when possible)
eval "$(lockd client acquire --server localhost:9341 --owner worker-1 --ttl 30s --key orders)"
lockd client keepalive --ttl 45s --key orders
lockd client get --key orders -o - \
  | lockd client edit status.counter++ \
  | lockd client update --key orders
lockd client set --key orders progress.step=fetch progress.count++ time:progress.updated=NOW
lockd client edit --file checkpoint.json progress.step="done" progress.count=+5
lockd client release --key orders
```

* Run `lockd auth new ca` once per trust domain, then issue per-server bundles with `lockd auth new server`.
* Default server bundles dedupe automatically (`server.pem`, `server02.pem`, …) to avoid clobbering existing files.
* Default client bundles dedupe automatically (`client.pem`, `client02.pem`, …) so repeated issuance preserves earlier certificates unless you pass `--out` or `--force`.
* `lockd auth verify` validates that bundles contain the expected CA/server material and that revoked client serials are surfaced from the denylist.
* `lockd auth revoke client` rewrites every `server*.pem` alongside the selected bundle so denylist updates reach all replicas automatically; use `--propagate=false` to update only the referenced bundle.
* `lockd client` subcommands wrap the Go SDK and honour the same mTLS defaults; the `set` helper mutates JSON using an existing lease (supports `path=value`, arithmetic `++/--/=+N`, `rm:/delete:` removals, and `time:` assignments). `lockd client acquire` prints `export LOCKD_CLIENT_*=` assignments so `eval "$(...)"` seeds the environment for follow-up commands, and every subcommand falls back to `LOCKD_CLIENT_KEY` when you omit the key flag.
* Bare `host[:port]` values default to HTTPS when mTLS is enabled (HTTP when `--disable-mtls` is set); explicit `http://` / `https://` URLs are always honoured.
* Default listen port is `9341`, selected from unassigned IANA space to avoid clashes with common cloud-native services.
* Bare `host[:port]` values default to HTTPS when mTLS is enabled (HTTP when `--disable-mtls` is set); explicit `http://` / `https://` URLs are always honoured.

## Storage model

### Abstraction

A narrow **KV + Blob** interface the server uses; implementations can be swapped (adapter pattern where each adapter implements a port interface).

* **Meta** (small): per-key protobuf with lease + version + state etag, updated via **CAS**.
* **State** (large): per-key JSON blob, streamed, updated atomically with **CAS**.

**Logical keys**

* `meta/<key>.pb`
* `state/<key>.json`
* `q/<queue>/msg/<id>.pb` (queue metadata) and `q/<queue>/msg/<id>.bin` (queue payloads / DLQ copies)

**Meta shape**

```json
{
  "lease": { "lease_id":"L-...", "owner":"worker-42", "expires_at_unix":1690000000 },
  "version": 7,
  "state_etag": "abc123",
  "updated_at_unix": 1690000100
}
```

### Backend 1: **S3 / Blob (preferred)**

*All metadata, state blobs, queue metadata, and queue payload objects **must** flow through `internal/storage.Crypto`. The kryptograf root key and global metadata descriptor live in the CA/server bundles; storage/queue code must never skip descriptor propagation or write plaintext alongside encrypted data.*

**Why**: No POSIX needed, works well from scratch, handles big blobs, now offers **strong read-after-write consistency**.

**CAS strategy on S3**

Preferred S3/Blob client: github.com/minio/minio-go/v7 (already pulled into the module).

* **Meta (small)**: We emulate CAS using **Copy-over with conditionals**:

  1. Read current meta ETag `M_old`.
  2. Write new meta protobuf to a **temp** key, e.g. `meta/tmp/<uuid>.pb`.
  3. `CopyObject` temp → `meta/<key>.pb` with `CopySourceIfMatch: M_old` (and `MetadataDirective=REPLACE` to set correct content type).
  4. If condition fails → CAS conflict.
  5. Garbage-collect temp.
* **State (large)**:

  1. Upload new state to `state/tmp/<uuid>.json` (multipart, streaming, no buffering).
  2. `CopyObject` tmp → `state/<key>.json` with **optional** `CopySourceIfMatch: S_old` (old state ETag) to enforce blob CAS; for creates use `CopySourceIfNoneMatch:"*"` or pass no precondition.
  3. Delete temp.
* This yields atomic “install new object iff previous ETag matches”, without relying on `PUT If-Match` (which is inconsistently supported).

**Consistency**

* S3 now provides strong R.A.W. for puts & overwrites; still, using conditional copy keeps our **CAS explicit** and portable.

**Other blob providers**

* The same pattern (temp-upload + conditional copy/move) works on many object stores. Optionally use `gocloud.dev/blob` to widen support (copy-with-precondition fallback may vary).

**Encryption & settings**

* Support SSE (`aws:kms` or `AES256`) via config.
* Multipart threshold & part size configurable.
* Timeouts/retries via SDK default retryer.

**Layout hygiene**

* Keep temp objects under `meta/tmp/` and `state/tmp/`.
* Optionally run a background janitor for orphaned temps.

### Backend 2: **Disk (local/NFS)**

* The disk backend targets SSD/NVMe roots but supports NFS mounts when multiple
  servers share the same storage.
* Metadata is persisted as protobuf (`meta/<key>.pb`) and state lives under
  `state/<key>/data`, both gated by CAS tokens.
* On local SSDs we rely on per-key file locks; under NFS we still require a
  single-writer semantic (multiple lockd replicas coordinate via HTTP but read
  the same object store).
* Background janitor (`--disk-janitor-interval`) prunes stale temp files; an
  optional retention window deletes old keys automatically.

### JSON handling (both backends)

* Incoming state is **streamed** through a compactor:

  * Parse incrementally, strip insignificant whitespace, **write directly** to uploader (multipart or file).
  * Enforce `json_max_bytes` (default ~100 MB) early.
  * Reject non-JSON bodies with `400`.
* On `get`, stream directly from backend (no full buffering), with `ETag` header and `X-Key-Version`.

### Failure semantics

* **Lease/Meta update**:

  * We write meta last. If state upload succeeds but meta CAS fails, the old `version` remains and the new blob is unreferenced (we’ll GC temp; the final key isn’t swapped).
* **Crash mid-upload**: multipart temp never completes → no final state; meta not bumped.
* **Replay / idempotency**: optional `X-Idempotency-Key` stored in meta for the last update to dedupe client retries (future extension).

### Configuration (storage)

Env/flags (viper; all mirror to `LOCKD_*`):

* `--store` (e.g., `s3://bucket/prefix` | `disk:///var/lib/lockd`)
* **S3 specifics**:

  * `--s3-region`
  * `--s3-endpoint` (for MinIO/Localstack)
  * `--s3-sse` (`AES256`|`aws:kms`)
  * `--s3-kms-key-id`
  * `--s3-max-part-size` (e.g., `16MB`)
  * `--s3-timeout`
* **General**:

  * `--json-max` (default `100MB`)
  * `--gc-temps-interval` (janitor)
* Credentials: rely on AWS default chain (env, IAM role, web identity). No special handling in code required.

## Behavior & guarantees

* **Exclusive lease** per key; only holder can read/write state.
* **Atomic** state install (temp upload → conditional copy → meta bump).
* **CAS** on meta (`version`) and optional CAS on blob (`ETag`).
* **TTL + keepalive**; expired leases reaped.
* **FIFO-ish** acquire with long-polling/retry hints.
* **Large JSON** streamed end-to-end.

## Server library (embedding)

Expose a small surface:

```go
// lockd server config
type Config struct {
  Listen           string
  Store            string // aws://..., s3://..., disk:///, etc.
  JSONMaxBytes     int64
  DefaultTTL       time.Duration
  MaxTTL           time.Duration
  AcquireBlock     time.Duration
  SweeperInterval  time.Duration
  // mTLS
  DisableMTLS      bool
  BundlePath       string // server.pem
  DenylistPath     string
  DisableStorageEncryption bool
  StorageEncryptionSnappy  bool
  // Object store options (may also come from env)
  S3SSE            string
  S3KMSKeyID       string
  S3MaxPartSize    int64
  AWSKMSKeyID      string
  AWSRegion        string
}

type Server struct{ /* ... */ }

func NewServer(cfg Config) (*Server, error)
func (s *Server) Handler() http.Handler
func (s *Server) Start() error
func (s *Server) Shutdown(ctx context.Context) error
```

The storage backend is chosen by parsing `cfg.Store`. The S3 impl uses the conditional-copy CAS pattern; the local impl does simple CAS with its internal sequencing.

## Client SDK (Go)

`client.New(baseURL, bundlePath string, opts ...Option) (*Client, error)` builds an mTLS HTTP/2 client that **skips hostname verification** and pins to the CA in `clientX.pem`. Methods: `Acquire`, `KeepAlive`, `Get`, `Update`, `Release`. Optional auto-keepalive helper.

## Configuration (viper/env)

Key flags/env (mirror to `LOCKD_*`): `LISTEN`, `DISABLE_MTLS`, `DISABLE_STORAGE_ENCRYPTION`, `BUNDLE`, `STORE`, `DEFAULT_TTL`, `MAX_TTL`, `JSON_MAX`, `ACQUIRE_BLOCK`, `SWEEPER_INTERVAL`, `DENYLIST_PATH`, plus the object-store options above (`AWS_REGION`, `S3_KMS_KEY_ID`, etc.).

## Non-goals (v0)

No multi-holder per key; no clustering/consensus; no advanced policy; no JSON schema enforcement.

## Example flow

1. `Acquire("orders", 30s, block=60s)` → gets a lease.
2. `Get()` → JSON + `(version, etag)`.
3. Work.
4. `Update(if_version=<seen>)` → CAS + bump `version`.
5. `Release()`.

All over **host-agnostic mTLS** with **object-store-backed CAS** so you can run it anywhere without POSIX.
