# Lockd Agent Overlay

The master AGENTS.md controls the operating model, collaboration contract,
commit discipline, and general production-control behavior. This file is the
Lockd-specific overlay: it records repo-local product facts, architectural
invariants, implementation rules, and verification expectations that must shape
work in this repository.

## Product Invariants

Lockd is a single-binary lock + state + queue service ("just enough etcd") that
ships with these stable capabilities:

- Namespaced storage with search/index/query support for keys and documents.
- XA-style transactions coordinated by a transaction coordinator.
- Attachments as first-class payloads.
- A production-ready Go SDK and Cobra/Viper CLI that expose every API surface,
  including streaming queries.
- Multiple storage backends behind a common port: disk, S3/MinIO, Azure Blob,
  and memory.
- Transparent encryption through shared storage crypto plumbing.
- Comprehensive verification coverage through unit tests, CLI tests,
  backend-specific integration suites, and YCSB benchmarks.

New work should build on those capabilities. Do not reintroduce older
assumptions that treat query/indexing, transactions, attachments, streaming, or
multi-backend support as experimental side paths.

## Architecture Boundaries

- `server.go` and `cmd/lockd/` own server startup and CLI wiring for leases,
  queue, indexer, namespace administration, and client commands.
- `client/` owns the Go SDK, CLI helpers, streaming query response types, and
  document helpers.
- `internal/storage/` owns storage backend implementations and shared crypto
  plumbing.
- `internal/search/` owns index management, query dispatch, index-vs-scan
  selection, and storage/search adapters.
- `integration/` owns backend suites for memory, disk, NFS, AWS, Azure, and
  MinIO, plus focused query and lock-queue suites.
- `run-integration-suites.sh` is the CI-style backend integration entrypoint.
- `ycsb/` is a standalone benchmark module and must be treated as a first-class
  client.

Package boundaries matter. Public API packages expose stable surfaces;
`internal/...` packages hold implementation details. If two or more
implementations or adapters share a role, define an interface at the appropriate
boundary. Constructors for implementations should be named `New...` and return
the interface type when an interface exists.

Avoid cyclic imports. If shared behavior causes a cycle, extract the common
pieces into a core package or subpackage that both callers can import.

## Public API Shape

Preserve the SDK and CLI as high-DX surfaces:

- User-facing Go functions or interface methods with more than four parameters,
  including `context.Context`, should move non-context inputs into a request
  struct.
- User-facing Go functions or interface methods should return at most two
  values: `(T, error)`.
- When multiple non-error outputs are required, return a response struct plus
  `error`.
- CLI commands must exercise real SDK calls. Cobra wiring must not drift from
  SDK behavior.
- CLI flags, help, examples, defaults, and error messages are part of the public
  developer experience.

## Streaming Rules

Streaming must be real producer-to-consumer streaming. Do not hide full-message
materialization behind a streaming-looking API.

- `client.Query` and `QueryResponse` must preserve streaming semantics.
- CLI query output with `--documents` streams NDJSON without buffering the full
  result set.
- `io.ReadAll` and equivalent full-buffer reads are prohibited on document,
  state, attachment, query-document, update-body, and mutate-stream payload
  paths.
- Use streaming readers/writers and bounded spool thresholds where needed.
- Exceptions are limited to small control-plane JSON envelopes and tests with
  deliberately bounded fixtures.

If a dependency cannot support true streaming through its public API, stop and
surface that constraint instead of silently implementing buffered behavior.

## Storage And Encryption

- Crypto is on by default. Every backend must route state, metadata, queue,
  document, attachment, and index payload paths through `internal/storage.Crypto`
  when encryption is enabled.
- CAS semantics are part of the storage contract. Writes use
  `PutObjectOptions{ExpectedETag}` where applicable.
- Backends must translate provider-specific conditional-write and missing-object
  failures into `storage.ErrCASMismatch` and `storage.ErrNotFound`.
- Index manifests and segments live under `index/`.
- Backend cleanup helpers must delete encrypted objects, metadata, state, index
  manifests, and index segments for the affected namespace.
- Do not remove observed-key warm-up logic in storage handlers. It protects
  correctness under eventual-consistency behavior.

## Query And Indexing

- Selector syntax includes RFC 6901 JSON Pointer plus Lockd shorthand such as
  `/field>=10` and brace-based forms.
- Query behavior must cover selectors, pagination, namespace isolation, public
  reads, return modes, document streaming, and indexed-vs-scan dispatch.
- Query syntax or behavior changes require corresponding README and CLI help
  updates.
- Integration query datasets use dataset profile guards; preserve those guards
  when adding coverage.

## Queue And Concurrency

- Queue suites cover advanced polling, QRF throttling, chaos scenarios,
  namespace contention, and multi-server/multi-worker behavior.
- Watchdogs must remain active for long or parallel tests such as
  acquire-for-update loops, queue chaos, and YCSB pre-checks.
- Chaos tests should be deterministic. Network-drop simulations default to a
  single disconnect with `MaxDisconnects=1`.
- Acquire-for-update uses bounded retries, defaulting to five. If that contract
  changes, update code, docs, and tests together.
- Integration tests should use real `client.Client` behavior. If a scenario
  fails, fix the SDK, CLI, or server path rather than mocking around it.

## Verification Matrix

Use the strongest applicable verification for the change. For executable
behavior, tests are the contract.

| Layer | Command | Notes |
| --- | --- | --- |
| Unit and CLI | `go test ./...` | Must stay fast locally; add watchdogs for long tests. |
| Vet | `go vet ./...` | Required before completion when Go code changes. |
| Lint | `golint ./...` | Required before completion when available. |
| Lint meta | `golangci-lint run ./...` | Required before completion when available. |
| Integration suites | `./run-integration-suites.sh <suite>` | Use targeted impacted suites such as `disk/query` or `minio/lq`. |
| Full integration sweep | `./run-integration-suites.sh` | Required before releases and large cross-backend refactors. |
| Benchmarks | `run-benchmark-suites.sh` or supported `ycsb/` targets | Record benchmark evidence in `docs/performance/`. |

For documentation-only changes, use readback and Markdown/config validation
where available. If a required verifier is unavailable in the environment, state
that explicitly in the completion report.

## Benchmark Workflow

Benchmark and perf-guard work must use supported repo entrypoints before ad hoc
commands:

- Prefer `make perf-guard-*`, `make perf-freeze-*`,
  `make perf-show-frozen-baselines`, `make bench`,
  `run-benchmark-suites.sh`, and documented `ycsb/` targets.
- Only run direct commands such as `go run ./cmd/lockd-bench ...` when debugging
  the harness itself or when no supported entrypoint exists. State that reason
  explicitly.
- If a perf target fails, inspect the exact `Makefile` or script path before
  attempting manual reproduction.

YCSB perf runs use this established flow:

1. Rebuild the dev stack with `nerdctl compose -f devenv/docker-compose.yaml down`
   and `nerdctl compose -f devenv/docker-compose.yaml up --build -d`.
2. Keep `LOCKD_OTLP_ENDPOINT` empty in `devenv/docker-compose.yaml` to avoid
   tracing overhead during perf runs.
3. From `ycsb/`, run `make lockd-load` then `make lockd-run` for the 10k-record
   baseline target.
4. Capture CPU pprof during a run:
   `curl -s "http://127.0.0.1:6060/debug/pprof/profile?seconds=10" -o /tmp/lockd.pprof`
   then `go tool pprof -top /tmp/lockd.pprof`.
5. If a perf regression appears, capture heap too:
   `curl -s "http://127.0.0.1:6060/debug/pprof/heap" -o /tmp/lockd-heap.pprof`
   then `go tool pprof -top -alloc_space /tmp/lockd-heap.pprof`.

## Documentation And Repo Hygiene

- Every package should have a `doc.go` with a standard Go package comment.
- If a generator is not tightly bound to a single package, put `generate.go` at
  the top-level module folder.
- If a generator is package-specific, put `generate.go` in that package folder,
  with generator runner `main` packages underneath as appropriate.
- Update README, CLI help, docs, and `BACKLOG.md` when behavior, operational
  guidance, or follow-up commitments change.
- Record benchmark results in `docs/performance/`.
- Keep structured logging intact. Server and CLI logs are part of the debugging
  surface; preserve subsystem names such as `server.lifecycle.core`,
  `search.index`, and `queue.dispatcher`.

If `.golangci.yml` is missing from the repo root, create it with:

```yaml
version: "2"
linters:
  disable:
    - errcheck
  exclusions:
    rules:
      # staticcheck style nits we don't want to chase
      - linters: [staticcheck]
        text: "QF1003"
      - linters: [staticcheck]
        text: "S1017"
      - linters: [staticcheck]
        text: "QF1001"
      - linters: [staticcheck]
        text: "S1009"
```
