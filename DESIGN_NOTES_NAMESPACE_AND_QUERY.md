## Current status (November 2025)

This section captures the state of the repo *today*. The sections that follow describe the **future/target** design (secondary indexes, Cassandra-like performance, etc.).

### Namespaces

- ✅ Fully shipped. Every meta/state/blob path is prefixed with `<namespace>/…` on all backends (disk, S3/MinIO, Azure, mem).
- ✅ Server defaults to `default` but CLI/SDK/tests now plumb `--namespace` / `client.WithDefaultNamespace`.
- ✅ Queue metadata/state obey the same layout; cache + dispatcher stats are recorded per-namespace.
- ✅ Integration suites (mem/disk/minio/aws/azure) all exercise non-default namespaces, and storage GC walks namespace roots safely.

### Public reads (`public=1`)

- ✅ `/v1/get` accepts `public=1` query flag (and SDK/CLI expose `--public` / `client.LoadPublic*` helpers).
- ✅ Reads the latest **published** blob guarded by the metadata commit marker; no lease required.
- ✅ Client SDK gained `LoadPublic`, `LoadPublicWithNamespace`, `LoadPublicBytes`, and metadata-aware helpers (e.g., `WithQueryHidden`).
- ✅ Integration coverage (mem query suite + unit tests) ensures public reads stay consistent and hidden keys never leak.

### Query language + scan adapter

- ✅ Exported `lql` package parses both dot-notation and brace shorthand into the shared Selector AST (JSON form stays the wire contract). Parser supports `and/or/not/eq/prefix/range/in/exists`, arrays, quoting, whitespace, etc.
- ✅ `/v1/query` is live today, backed by the **scan adapter** that walks namespace metadata/state directly. It supports selectors, pagination, hidden key filtering, and returns `{keys, cursor, metadata}`.
- ✅ Scan adapter ignores internal prefixes (e.g., `lockd-diagnostics/*`) and respects metadata flags like `lockd.query.exclude`.
- ✅ Query-specific integration suites for **mem, disk, minio, aws, azure** seed realistic datasets (vouchers, firmware rollouts, SALUTE reports, flight telemetry) and assert selectors/pagination/public-read behaviour.
- ✅ Namespace + metadata plumbing is complete: selectors default to the request namespace, and metadata attributes (`query_hidden`) can be toggled via `/v1/metadata` + SDK options.

### LQL mutations

- ✅ Same DSL now powers CLI mutations via the exported `lql` package. Brace & dot notations interoperate; fuzz corpuses became regression tests; tests exercise JSON mutations over “production-like” payloads (finance, IoT, telemetry).

### Remaining work (high level)

- ❌ No secondary indexes yet; `/v1/query` still performs full scans (sufficient for small namespaces and dev backends but not production scale).
- ❌ No index manifests/segments/compaction machinery; search adapter interface exists but only the scan implementation is wired.
- ❌ Cassandra-like performance goals (sharded postings, per-field manifests, measured latencies) remain TODO.
- ❌ Namespace-level caching / query stats (to drive auto-indexing) are not implemented.
- ❌ Config/documentation still describes the future index design but not the current scan-based behaviour—this doc update is the first step, README will need a similar “current vs future” note when the index lands.

---

The next sections capture the **intended** index/search design (Cassandra-like). Treat them as the backlog for the next phase now that namespaces + public reads + the scan-based `/v1/query` are done.

## Roadmap context

We are rolling out two related features on top of the Acquire → Get → Update → Release cycle:

1. **Namespaces (DONE).** Every key (including queue state) is prefixed by its namespace inside the storage backend (`<namespace>/meta/...`, `<namespace>/state/...`). The server defaults to `default` unless overridden via `--default-namespace` / `LOCKD_DEFAULT_NAMESPACE`; inputs are normalized to lowercase `[a-z0-9._-]`. This is fully implemented throughout the APIs and clients.
2. **Query + public reads (DESIGN).** Next we need efficient per-namespace search and a fast read path that does not require a lease.

The query work is the large portion: it introduces secondary indexes and a `/v1/query` endpoint that returns ordinary keys which can later be acquired. Public reads give us a read-only path for published snapshots without acquiring a lease.

## Public reads (`/v1/get?public=1`)

Rather than add a vaguely named `/v1/read`, we will extend `/v1/get` with a `public=1` flag. When the flag is present:

* The handler skips lease validation and serves the latest **published** blob (commit marker must exist).
* Responses still include `ETag`/`X-Key-Version` so clients can detect stale data.
* The call may lag slightly; read-your-write consistency remains the domain of lease holders.
* CLI/SDK add opt-in knobs (`lockd client get --public`, `client.WithPublicGet()`).

Implementation steps:

1. Mark updates as published once the blob + metadata are durable (commit marker).
2. Teach `/v1/get` to branch on `public`. Lease-bound flows (no flag) retain current semantics.
3. Document the behaviour in README/Swagger so users understand the two modes.

## Query/index system

The design below (Cassandra-inspired) remains the target. The main delta is that it now explicitly operates inside a namespace partition, and it consumes the same commit markers used by public reads.

### DSL

Primary representation is JSON:

```json
{
  "and": [
    { "eq":    { "field": "status", "value": "open" } },
    { "range": { "field": "amount", "gte": 100, "lt": 500 } }
  ],
  "limit": 200,
  "cursor": "opaque-token"
}
```

For CLI/query-string use we mirror the client mutation syntax, e.g.

```
selector.and.0.eq.field=status
selector.and.0.eq.value=open
selector.and.1.range.field=amount
selector.and.1.range.gte=100
selector.and.1.range.lt=500
limit=200
```

Both forms parse into a `Selector` AST with `and`, `or`, `eq`, `prefix`, `range`, `in`, and `exists`. Namespaces are mandatory request fields so queries never spill across partitions.

### Outline (unchanged from earlier notes, now scoped to namespaces)

Everything below remains valid; the only clarification is that every data structure (memtables, segments, manifests, postings) is sharded by `(namespace, field, shard)` so queries remain strictly per-namespace. The same commit marker that unlocks `/v1/get?public=1` also gates index visibility.

# High-level shape

* **Namespace = partition.** All objects in a namespace are queryable together (heterogeneous types allowed).
* **Unlocked Get** reads only “published” objects (commit marker), so queries/reads never need locks.
* **Secondary indexes** are **segmented** (immutable runs + manifests + background compaction). This is the core performance lever.

# Indexing model (storage-agnostic)

* **Two index families**

  * **Eq/Prefix**: inverted index (term → docIDs).
  * **Range**: sorted rows ((encoded_value, docID)), for numeric/timestamp range scans.
* **Segments**: immutable files created by flushing in-memory **memtables**; many small segments are periodically merged by a **compactor**.
* **Manifests**: tiny per-(field, shard) files with segment metadata (min/max, count, size). Query planner consults manifests to open the **fewest segments**.
* **Sharding**: per term/value hash to N shards (e.g., 256) to spread hot keys (`status=open`) and increase parallelism. Query fan-out is bounded and predictable.

## What gets indexed (policy)

* Default: **top-level fields**, capped (e.g., 16–32 fields/object).
* Strings: **exact** + **short prefix** (first K chars; verify true prefix at read time).
* Range: only **opt-in** numeric/timestamp fields (avoid overspending on range indexes).
* Optional conventional `_type` field to filter by “document kind.”

## Write path (commit=true)

1. Write blob atomically.
2. Extract/normalize indexed fields (lowercase strings, sortable encodings for numbers/timestamps).
3. Append to **memtables** (per field+shard). When a memtable hits target size, **flush** → new segment.
4. Update per-shard **manifest** (append segment ref).
5. Publish a **commit marker** (so reads/queries see it).
   (Option: “wait for index” vs “eventual” publishing is a single config flag.)

## Compaction

* Background process merges small segments into bigger ones (tiered/leveled).
* Benefits: fewer files, faster queries, lower heap during intersections.
* Online and lock-free: manifests swap atomically after new segments are written.

# Query execution (fast path)

* Input: **Selector** with `AND/OR` and atoms (`eq`, `in` (small), `prefix`, `range`, `exists`).
* **Planning heuristics (simple, effective):**

  1. Normalize to DNF/CNF enough to identify **selective atoms**.
  2. For `AND`, start with the **smallest posting** (estimated via term docfreq in manifest) to minimize intersection cost.
  3. For `OR`, evaluate branches in **increasing cost** order; short-circuit once `limit` is reached.
  4. For `range`, open only overlapping **range segments** via manifest min/max; stream merge (k-way) across segments.
  5. **Early stop** when `limit` is satisfied; paginate with a **cursor** (last (segment, key) position).
* **Post-filter** always (for prefix verification, mixed types, or stale segments) to guarantee correctness.
* **Return only keys** (docIDs). Payloads are fetched with `Get` or after `Acquire`.

## Cursors & pagination

* Cursor holds: selector hash, last emitted `(docID)` and, for range, last `(value, docID)`, plus segment ref.
* Stable across compactions by using **docID** order (UUIDv7 works well) and resuming at or after the last tuple.

## Caching

* Small **LRU** of hot posting blocks / range slices.
* Optional **Bloom** or **term dictionary** in segments to skip reads quickly (useful when many terms miss).

# List (namespace enumeration)

Two interchangeable implementations:

* **Index-backed** (recommended): maintain a synthetic `_key` eq index → List is just a range over `_key` with cursor.
* **Store-backed**: iterate `/commit/` prefix in backend store (S3/disk) and page. Zero extra index, but backend-specific semantics.

# Consistency knobs

* **Eventual** by default: publish commit without waiting for index flush; queries might lag briefly.
* **Read-your-write** when needed:

  * Return `index_seq` from Update; Query accepts `after_seq` and blocks until manifests ≥ that seq.
  * Or enable “publish waits for index” globally/namespace-wide.

# Storage specifics

* **S3/disk**: segments are just objects/files; strong GET after MPU complete/rename. Commit marker gates visibility.
* **Postgres (future)**: same Selector and RPCs. Map Eq/Prefix → JSONB GIN; Range → BTREE on extracted columns; use DB planner instead of manifests. Keep the **SearchAdapter** interface so PG can plug in transparently.

# Interfaces (only what matters)

* **SearchAdapter** with `Insert`, `Delete`, `Query`, `List`.
* **Selector** (AST) and a pluggable parser (string DSL and/or JSON). Keep the parser minimal; you can swap it later (expr/govaluate/cel).

# Default numbers (good starting points; tune later)

* Shards: **256** per field.
* Segment target: **8–32 MiB**.
* Max indexed fields/object: **24**.
* String prefix length: **3–5**.
* Query max limit: **1000** (require namespace lease for anything broader/full scans).

# Why this is fast

* **Touch few files**: manifests prune segments aggressively; shards keep hotspots spread.
* **Work proportional to matches**: intersections start from the smallest posting; ranges read only overlapping slices; early stop at `limit`.
* **Immutable segments**: lock-free queries, cheap compaction, predictable IO.
* **Backend-agnostic**: same access pattern on S3/disk; PG can short-circuit with native indexes later.
