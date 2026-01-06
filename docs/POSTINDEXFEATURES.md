# Post-Index Feature Analysis

This document tracks the feature work that followed the initial rollout of the
secondary-index query path. It is split into two parts:

1. **Shipped enhancements** – improvements that already landed in `search`.
2. **Remaining roadmap** – features we still plan to implement, with the latest
   design notes.

## 1. Shipped Enhancements

### 1.1 Public TLS helpers

- `pkt.systems/lockd/tlsutil` is now a public package. External tools can call
  `tlsutil.LoadClientBundle` / `LoadServerBundle` / `VerifyPeerCertificate`
  instead of re-implementing PEM parsing.
- Impact: the go-ycsb driver, CLI utilities, and future operators share one
  vetted code path for mTLS configuration.

### 1.2 Query API parity (options-driven SDK)

- `Client.Query` mirrors `Client.Get` semantics: `func (c *Client) Query(ctx,
  optFns ...QueryOption) (*client.QueryResponse, error)`.
- Callers compose requests with helpers such as:
  - `WithQueryNamespace`, `WithQueryLimit`, `WithQueryCursor`.
  - `WithQuery(expr string)` for RFC 6901/LQL selectors.
  - `WithQuerySelector(sel api.Selector)` when an AST already exists.
  - `WithQueryEngine{Auto,Index,Scan}` and `WithQueryRefresh{WaitFor}` /
    `WithQueryBlock` for capability hints.
- `client.QueryResponse` exposes `Keys()` and the current (keys-only)
  `ForEach`. The streaming reader discussed below is still pending, but the SDK
  surface is ready for it.

### 1.3 Namespace defaults on the client

- `Client.UseNamespace`, `UseLeaseID`, `ClearLeaseID`, and `Clone` let callers
  set sticky defaults instead of repeating options on every call.

### 1.4 LQL mutation helpers (DX)

- Added `lql.Mutations(now, exprs...)`, `lql.Mutate(doc, exprs...)`, and
  `lql.MutateWithTime(doc, now, exprs...)` so callers can write:
  `lql.Mutate(doc, "/status=ready", "/count++")`.
- Under the hood these wrap the existing `ParseMutations`/`ApplyMutations`
  helpers, but the new API matches what CLI users type every day.
- Examples, tests, and docs now use the concise helpers.

### 1.5 Document-aware client helpers

- Introduced `client.Document` with JSON Pointer mutation helpers plus
  streaming `Reader`/`Write` support so docs can flow through `io.Copy`.
- `Client.Load` auto-detects `*client.Document`, populating namespace/key,
  version, etag, and body in one call; `GetResponse.Document()` offers the same
  convenience without a struct literal.
- `LeaseSession.Save` type-switches on `*client.Document` (and generic
  `io.Reader`) to avoid double-marshalling, keeping large state updates
  zero-copy for callers that already have JSON bytes.

### 1.6 Query streaming (`return=documents`)

- `/v1/query` accepts `return=documents` (body or query string) to stream
  newline-delimited results with `{ns,key,ver,doc}` rows.
- Responses include cursor/index metadata via headers
  (`X-Lockd-Query-Cursor`, `X-Lockd-Query-Index-Seq`, `X-Lockd-Query-Metadata`)
  regardless of mode; document streams set `Content-Type:
  application/x-ndjson`.
- The Go SDK gained `client.WithQueryReturn{Keys,Documents}` plus a streaming
  `QueryResponse` – `Mode()` reveals the active return type, `Keys()` drains the
  stream when necessary, and `ForEach` hands back `QueryRow` values with
  `DocumentReader()` / `DocumentInto()` helpers (remember to close the reader
  when you take ownership of it).
- Keys-only behaviour is unchanged, so existing callers pick up headers without
  modifying their JSON parsing.

## 2. Remaining Roadmap

### 2.1 Namespace capability discovery in the SDK

- Add `Client.GetNamespaceConfig(ctx, ns)` returning a result struct with the
  server’s namespace metadata + ETag (`/v1/namespace` response).
- Provide typed helpers (`NamespaceCapabilities`) so code can answer “does this
  namespace allow public reads?”, “what’s the preferred query engine?”, etc.
- Impact: YCSB and production apps can adapt automatically instead of guessing.

### 2.2 Read-your-writes (RYW)

- Namespace config flag (e.g. `read_your_writes=true`) to guarantee that `/v1/query`
  and `/v1/get?public=1` observe the caller’s latest acknowledged write.
- Options include a per-namespace write sequence or piggybacking on the indexer
  flush sequence.
- Client helper (`ReadYourWritesBarrier` or `WithReadYourWrites`) would expose a
  friendly API for benchmarks/tests that require deterministic visibility.

### 2.3 Additional DX ideas

- Structured `GetOptions` / `LoadOptions` builders to complement the existing
  functional options.
- Exposing namespace capabilities (Section 2.2) to the client so misuse (e.g.
  trying public reads on a private namespace) surfaces as helpful errors.
- Once the streaming query work lands, revisit `QueryResponse` to add `Keys()`
  vs. `Documents()` convenience helpers.

### 2.4 Attachment-aware documents (large binary fields)

**Motivation:** Some workflows persist multi-MB JSON states with embedded
base64 data (screen captures, binaries). Even with streaming GETs, the server
still compacts/uploads the entire JSON payload and clients must decode the blob
before writing elsewhere.

**Inspiration:** CouchDB/Couchbase attachments (JSON metadata + lazily fetched
blobs) and MongoDB GridFS (documents referencing chunk collections) show that
storing large binaries next to—but not inside—the JSON keeps APIs ergonomic
while enabling streaming readers.

**Potential plan for lockd:**

1. **Server-side streaming parser**: while `/v1/update` compacts the JSON, it
   detects large base64 fields (guided by namespace policies or heuristics) and
   spools them to sidecar objects such as `state/<key>/attachments/<uuid>.bin`
   instead of keeping them inline.
2. **Metadata annotations**: meta records capture the JSON Pointer(s), content
   type, size, checksum, and storage key for each attachment so the logical
   document still knows those fields exist.
3. **Client attachment API**: `client.Document` could expose helpers such as
   `doc.AttachReader("/payload/raw", r)` when writing and
   `row.AttachmentReader("/payload/raw")` (or `Document().Attachment(...)`)
   when reading. Under the hood the SDK would stream from a new
   `/v1/state/attachment` endpoint or directly from the store when co-located.
4. **Full JSON reconstruction**: callers needing the original base64 field can
   opt into “hydrate attachments”, combining streaming attachment reads with a
   JSON writer to rebuild the exact payload without holding it entirely in
   memory.

This design preserves the simple JSON contract while enabling “zero alloc”
flows for large blobs. Open questions include: how backends advertise default
thresholds, how to encode per-field policies (`lockd.attach:/path` metadata),
and how namespace configs expose attachment settings (chunk size, encryption
requirements, replication rules).

### 2.5 HTTP/2 multiplexing experiments

**Motivation:** `/v1/query?return=documents` now streams everything down a
single NDJSON response. It’s efficient, but callers still receive every document
even if they drop some halfway through. HTTP/2’s multiplexing could let us
decouple metadata from document payloads.

**Ideas:**

1. **Multipart over HTTP/2** – send each document as a separate part (or even a
   secondary request) so clients can close a single part without affecting the
   rest of the stream. Multiplexed responses keep latency low while avoiding
   long-lived per-doc connections.
2. **Batch query + on-demand fetch** – keep `/v1/query` lightweight (keys only)
   and add a follow-up endpoint that accepts a list of keys and streams just
   those documents. Clients use HTTP/2 to issue multiple fetches in parallel.
3. **Namespace-level HTTP/2 upgrades** – evaluate how other flows (queue dequeue
   payload streaming, long polling for drain events, etc.) could benefit from
   multiplexed channels, especially when multiple watchers share the same TLS
   session.

This is still exploratory. We’d need to design a clear wire contract, evaluate
library support (Go server/client, other SDKs), and ensure the fallback to
HTTP/1.1 remains functional. For now it stays on the roadmap as a future
optimization track.

### 2.5 Streaming query documents (chunked NDJSON)

**Goal:** Avoid allocating entire documents during `/v1/query?return=documents`
responses. Today the server copies each `doc` field into memory and the client
holds a `json.RawMessage`, defeating the streaming benefits for large blobs.

**Sketch:**

1. **Server-side encoder** – replace the current “decode row → re-encode into
   json.RawMessage” flow with a streaming JSON encoder that writes the `doc`
   field directly from the storage backend. For each row we’d do something like:

   ```go
   enc.EncodeToken(json.Delim('{'))
   writeField("ns", namespace)
   writeField("key", key)
   writeField("ver", version)
   enc.EncodeToken(json.Delim('"')) // "doc"
   copyStateStreamToEncoder(stateReader)
   enc.EncodeToken(json.Delim('}'))
   ```

   This keeps the NDJSON shape unchanged but never buffers the entire document.

2. **Client-side decoder** – swap `json.Decoder.Decode` for a streaming decoder
   that walks tokens and captures the `doc` field as an `io.Reader` instead of a
   `[]byte`. Internally we can wrap the response body with a small buffering
   reader so `QueryRow.DocumentReader()` pipes the JSON payload directly into
   user code (or into `client.Document.LoadFrom`) without allocating.

3. **QueryRow API** – expose helpers such as `row.DocumentReader()` /
   `row.Document()` that lazily hydrate a `client.Document` by feeding the
   streaming reader into `Document.LoadFrom`. For callers who only need the raw
   bytes, `DocumentReader` can be handed directly to `io.Copy`.

**Plan:**

1. Update the `/v1/query` handler to use a streaming encoder (possibly via the
   existing `compactWriter` + `io.Copy` into the response) so each document is
   copied from storage → HTTP without intermediate buffers.
2. Extend the client `QueryResponse` parser to use a token-level decoder that
   extracts metadata (`ns`, `key`, `ver`) while leaving the `doc` value as a
   streamed segment. That likely means introducing a small state machine around
   `json.Decoder.Token()` and handing back a custom `io.Reader` tied to the body.
3. Adjust `QueryRow` to hold that streaming reader (instead of `json.RawMessage`)
   and provide convenience methods for users to hydrate `client.Document` or
   consume the stream directly.
4. Update docs/examples to highlight the new zero-copy flow and note that keys
   mode remains unchanged.

This keeps the NDJSON contract stable while finally achieving “near-zero alloc”
queries: we only touch each document as it flows through the HTTP stream, and
callers opt into materializing it when needed.

---

_This document will continue to track which items move from “roadmap” to
“shipped”. When you implement one of the outstanding proposals, update the
sections above so downstream users always have an accurate picture of what’s
available._
