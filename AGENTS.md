# AGENTS.md

## Purpose

This repo delivers a tiny, single-binary **lock + state** service—“just enough etcd”—focused on **exclusive leases** and **atomic JSON state**. It coordinates workers so **only one holder at a time** can read/advance a shared checkpoint (JSON up to ~50 MB). It favors reliability and simplicity, runs well in `FROM scratch`, and avoids POSIX assumptions.

## Context (what matters)

* **Language**: Go.
* **Runtime**: `FROM scratch` via `pkt.systems/psi`; CGO-free static build.
* **Concurrency**: v0 is **one holder per key** (no `limit>1`).
* **Transport**: `net/http` (HTTP/2 via TLS).
* **Auth**: **mTLS by default**. We **trust identities, not hostnames** (host-agnostic mTLS).
* **Config**: cobra CLI + viper (env prefix `LOCKD_`).
* **Layout**:

  * `/` → server library (export `lockd.Server`, `lockd.Config`, etc.).
  * `cmd/lockd/` → main server app using the library.
  * `client/` → Go client SDK (`client.New(...)`).

## Primary use-case

A single worker holds a lease for a **key** (stream), reads the JSON checkpoint/cursor, does work, updates the checkpoint atomically (CAS), and releases. If the worker dies, lease expiry hands off to the next worker which resumes from the last committed state.

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
* `POST /v1/get_state` (requires live lease) → body streamed JSON; headers `X-Key-Version`, `ETag`.
* `POST /v1/update_state` (requires live lease) → raw JSON body (streamed/compacted), optional CAS headers; `200 {new_version,new_state_etag,bytes}` or `409`.
* `GET /v1/describe?key=...` → meta only (no state).
* `GET /healthz`, `GET /readyz`.

Blocking semantics: `acquire` long-polls/loops up to `block_seconds`; on timeout returns `409 waiting` with `retry_after_seconds`.

## Host-agnostic mTLS (default)

* **We trust certs, not hostnames**. CN/SANs are not used for hostname auth; certs are movable.
* Server & client both authenticate via **chain to the project CA**, **proper EKUs**, and **denylist** (revoked serials).
* **Client → Server**: custom peer verification that **skips hostname checks**, verifies chain to embedded CA and `ServerAuth` EKU.
* **Server → Client**: verifies client chain to CA, `ClientAuth` EKU, checks denylist.

**Bundles**

* `server.pem` (single file): CA cert, CA private key (optional password-protected with strong KDF + AEAD), server cert, server key, and optional embedded denylist/CRL.
* `clientX.pem` (one per client): CA cert, client cert, client key.

**CLI (cobra)**

```
lockd auth new server --out server.pem --cn "lockd-anywhere" --hosts "*" --ca-pass-prompt
lockd auth new client --server-in server.pem --out client1.pem --cn "worker-1"
lockd auth revoke client --server-in server.pem --out server.pem <hex-serial>...
lockd auth inspect server --in server.pem
lockd auth inspect client --in client1.pem
lockd auth verify server --in server.pem
lockd auth verify client --server-in server.pem --in client1.pem

# bundled client CLI (mTLS by default, auto-discovers client*.pem when possible)
eval "$(lockd client acquire --server localhost:9341 --owner worker-1 --ttl 30s orders)"
lockd client keepalive --ttl 45s orders
lockd client get orders -o - \
  | lockd client edit status.counter++ \
  | lockd client update orders
lockd client set orders progress.step=fetch progress.count++ time:progress.updated=NOW
lockd client edit --file checkpoint.json progress.step="done" progress.count=+5
lockd client release orders
```

* `lockd auth new server` writes both `server.pem` and `ca.pem` so the CA cert can be stored separately (secure vault, etc.).
* `lockd auth verify` validates that bundles contain the expected CA/server material and that revoked client serials are surfaced from the denylist.
* `lockd client` subcommands wrap the Go SDK and honour the same mTLS defaults; the `set` helper mutates JSON using an existing lease (supports `path=value`, arithmetic `++/--/=+N`, `rm:/delete:` removals, and `time:` assignments). `lockd client acquire` prints `export LOCKD_CLIENT_*=` assignments so `eval "$(...)"` seeds the environment for follow-up commands.
* Bare `host[:port]` values default to HTTPS when mTLS is enabled (HTTP when `--mtls=false`); explicit `http://` / `https://` URLs are always honoured.
* Default listen port is `9341`, selected from unassigned IANA space to avoid clashes with common cloud-native services.
* Bare `host[:port]` values default to HTTPS when mTLS is enabled (HTTP when `--mtls=false`); explicit `http://` / `https://` URLs are always honoured.

## Storage model

### Abstraction

A narrow **KV + Blob** interface the server uses; implementations can be swapped (adapter pattern where each adapter implements a port interface).

* **Meta** (small): per-key JSON with lease + version + state etag, updated via **CAS**.
* **State** (large): per-key JSON blob, streamed, updated atomically with **CAS**.

**Logical keys**

* `meta/<key>.json`
* `state/<key>.json`

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

**Why**: No POSIX needed, works well from scratch, handles big blobs, now offers **strong read-after-write consistency**.

**CAS strategy on S3**

Preferred S3/Blob client: github.com/minio/minio-go/v7 (already pulled into the module).

* **Meta (small)**: We emulate CAS using **Copy-over with conditionals**:

  1. Read current meta ETag `M_old`.
  2. Write new meta JSON to a **temp** key, e.g. `meta/tmp/<uuid>.json`.
  3. `CopyObject` temp → `meta/<key>.json` with `CopySourceIfMatch: M_old` (and `MetadataDirective=REPLACE` to set correct content type).
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

### Backend 2: **Embedded local**

Preferred: github.com/cockroachdb/pebble (already in go.mod).

* A pure-Go store like **Pebble** (preferred) for **single-host** deployments:

  * Store meta under a normal keyspace.
  * Store state either inline (if small) or as side files referenced from Pebble (with our own CAS tokens).
* **Do not** share a local embedded DB over NFS. If you must use NFS, run a **single** server instance that owns the DB locally and expose the API over the network.

> Default backend for v0 can be S3; Pebble is optional and behind a build tag if you want to keep the base image tiny.

### JSON handling (both backends)

* Incoming state is **streamed** through a compactor:

  * Parse incrementally, strip insignificant whitespace, **write directly** to uploader (multipart or file).
  * Enforce `json_max_bytes` (default ~100 MB) early.
  * Reject non-JSON bodies with `400`.
* On `get_state`, stream directly from backend (no full buffering), with `ETag` header and `X-Key-Version`.

### Failure semantics

* **Lease/Meta update**:

  * We write meta last. If state upload succeeds but meta CAS fails, the old `version` remains and the new blob is unreferenced (we’ll GC temp; the final key isn’t swapped).
* **Crash mid-upload**: multipart temp never completes → no final state; meta not bumped.
* **Replay / idempotency**: optional `X-Idempotency-Key` stored in meta for the last update to dedupe client retries (future extension).

### Configuration (storage)

Env/flags (viper; all mirror to `LOCKD_*`):

* `--store` (e.g., `s3://bucket/prefix` | `pebble:///var/lib/lockd`)
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
  Store            string // s3://... or pebble:///
  JSONMaxBytes     int64
  DefaultTTL       time.Duration
  MaxTTL           time.Duration
  AcquireBlock     time.Duration
  SweeperInterval  time.Duration
  // mTLS
  MTLS             bool
  BundlePath       string // server.pem
  DenylistPath     string
  // S3 options (optional; may also come from env)
  S3Region         string
  S3Endpoint       string
  S3SSE            string
  S3KMSKeyID       string
  S3MaxPartSize    int64
}

type Server struct{ /* ... */ }

func NewServer(cfg Config) (*Server, error)
func (s *Server) Handler() http.Handler
func (s *Server) Start() error
func (s *Server) Shutdown(ctx context.Context) error
```

The storage backend is chosen by parsing `cfg.Store`. The S3 impl uses the conditional-copy CAS pattern; the local impl does simple CAS with its internal sequencing.

## Client SDK (Go)

`client.New(baseURL, bundlePath string, opts ...Option) (*Client, error)` builds an mTLS HTTP/2 client that **skips hostname verification** and pins to the CA in `clientX.pem`. Methods: `Acquire`, `KeepAlive`, `GetState`, `UpdateState`, `Release`. Optional auto-keepalive helper.

## Configuration (viper/env)

Key flags/env (mirror to `LOCKD_*`): `LISTEN`, `MTLS`, `BUNDLE`, `STORE`, `DEFAULT_TTL`, `MAX_TTL`, `JSON_MAX`, `ACQUIRE_BLOCK`, `SWEEPER_INTERVAL`, `DENYLIST_PATH`, plus S3 options above.

## Non-goals (v0)

No multi-holder per key; no clustering/consensus; no advanced policy; no JSON schema enforcement.

## Example flow

1. `Acquire("orders", 30s, block=60s)` → gets a lease.
2. `GetState()` → JSON + `(version, etag)`.
3. Work.
4. `UpdateState(if_version=<seen>)` → CAS + bump `version`.
5. `Release()`.

All over **host-agnostic mTLS** with **object-store-backed CAS** so you can run it anywhere without POSIX.

## Stretch goals

### `lockc`, the lockd CLI client

Location: `cmd/lockc` (e.g `cmd/lockc/main.go`)

Should be able to acquire the lock like `lockc acquire -s server -i client.pem <key>`, continue with a context somehow, then be able to `lockc state get -o dump.json` or `-o dump.yaml`, edit json or yaml, then `lockc state update -i dump.yaml` (serializes back to json, that's the underlying format), also be able to do `lockc keepalive --ttl 30s`, and `lockc release [key]` when done.
