# AGENTS.md

## Collaboration Profile

You are collaborating with a highly opinionated Go architect. Optimize for Go-idiomatic design, separation of concerns, and developer experience (DX).

## Workflow (mandatory)

1. Restate the goal and constraints.
2. Propose 1-3 designs with explicit tradeoffs (complexity, maintainability, DX, performance, risk).
3. Get alignment before writing significant code or making structural changes.
4. Implement in small, reviewable steps.
5. Run quality gates (see below) before declaring completion.
6. Git commit messages must follow Conventional Commits: https://www.conventionalcommits.org/en/v1.0.0/

## Purpose

Lockd is a single-binary lock + state + queue service ("just enough etcd") that now ships with:

- Namespaced storage with search/index/query support (keys and documents).
- XA-style transactions (TC-driven) and attachments as first-class features.
- A production-ready Go SDK + CLI (cobra/viper) that expose every API surface, including streaming queries.
- Multiple storage backends behind a common port (disk, S3/MinIO, Azure Blob, memory) with transparent encryption.
- Comprehensive testing: unit tests, CLI tests, backend-specific integration suites (`run-integration-suites.sh`), and YCSB benchmarks (`ycsb/`).

Everything in this repo assumes those pieces are stable. New work should build on that reality rather than older assumptions.

## Operating Principles

1. Outcome over clock time. If a change touches many layers, finish it even if it takes longer.
2. Refactor with confidence. The backend abstraction, integration suites, and watchdogs exist so we can land large changes safely.
3. Tests are the contract. A feature is not done until `go test ./...` and relevant integration suites pass.
4. Surface regressions quickly. Watchdogs, QRF dashboards, and structured logs must remain enabled in tests.
5. Document as you go. Update README/docs when behavior changes. Record benchmarks in `docs/performance/`. Track follow-ups in `BACKLOG.md`.

## Refactors (strong preference)

- Avoid feature flags for refactoring tasks.
- Prefer clean refactors with a clear cutover:
  - No lingering legacy implementations, structs, or parallel codepaths.
  - Remove dead code and migrate call sites in the same change-set/sequence.
- Only keep parallel implementations or legacy structures if explicitly requested.

## Architecture & Packaging

- Separation starts at the package boundary:
  - Public API packages for exported surfaces.
  - `internal/...` for non-exported implementation details.
- If there are two or more variants/adapters of an implementation, use an interface.
- Constructors:
  - Provide a `New...` constructor for implementations.
  - Constructors must return the interface type, never a concrete type.
- Cyclic imports:
  - If cycles occur, extract core functionality into a `core` package or subpackage so both main/module code and subpackages can import it without cycles.

## Public API Shape (strong preference)

- Inputs:
  - If a user-facing function or interface method takes more than 4 parameters total (including `ctx context.Context`), move non-`ctx` inputs into a request struct (e.g., `FooRequest`).
- Outputs:
  - A user-facing function or interface method must return no more than two values: `(T, error)`.
  - If more than one non-error value is required, return a response struct as the first value and `error` as the second.

## Documentation & Generators

- Every package must have a `doc.go` with standard Go package comment documentation.
- Code generation:
  - If generators are not tightly bound to a single package, put `generate.go` at the top-level module folder.
  - If tightly bound to a single package, put `generate.go` in that package folder (with generator runner `main` packages underneath as appropriate).

## Repo Hygiene

- If `.golangci.yml` does not exist in the repo root, create it with the contents below:

```yaml
version: "2"
linters:
  exclusions:
    rules:
      # errcheck noise we explicitly accept for now
      - linters: [errcheck]
        path: ".*_test\\.go"
      - linters: [errcheck]
        text: "resp.Body.Close"
      - linters: [errcheck]
        text: "fmt.Fprint"
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

## Project Layout (snapshot)

- `server.go` / `cmd/lockd/`: main server + CLI wiring (leases, queue, indexer, namespace admin).
- `client/`: Go SDK, CLI helpers, streaming query response types, document helpers.
- `internal/storage/`: backend implementations (disk, s3, azure, memory) and shared crypto plumbing.
- `internal/search/`: index manager, dispatcher (index vs scan), adapters.
- `integration/`: backend suites (mem/disk/nfs/aws/azure/minio) plus focus suites (`.../query`, `.../lq`).
- `run-integration-suites.sh`: entrypoint for CI-style coverage across backends.
- `ycsb/`: standalone module for YCSB benchmarks.

## Workflows

### Planning & Execution

- Start with the tests. Identify which unit/integration suites must cover the change. Add or extend tests before or alongside the implementation.
- Leverage storage abstraction. Touch common code first, then backend-specific overrides.
- Iterate in place. Avoid abandoning branches mid-feature. If a direction is wrong, explain why and pivot.
- Keep logging intact. Server/CLI logs (pslog) are part of the debugging surface. Maintain subsystem names (`server.lifecycle.core`, `search.index`, `queue.dispatcher`, etc.).

### Testing Expectations

| Layer | Command | Notes |
| --- | --- | --- |
| Unit & CLI | `go test ./...` | Must stay fast (<10s locally). Add watchdogs for long tests. |
| Vet | `go vet ./...` | Required before completion. |
| Lint | `golint ./...` | Required before completion. |
| Lint (meta) | `golangci-lint run ./...` | Required before completion. |
| Integration suites | `./run-integration-suites.sh <suite>` | Use targeted suites (e.g. `disk/query`, `minio/lq`). Run all impacted suites before landing. |
| Full sweep | `./run-integration-suites.sh` | Required before releases or large refactors. |
| Benchmarks | `run-benchmark-suites.sh` / `ycsb` | Record results in `docs/performance/`. |

### CLI & SDK Changes

- Single source of truth. CLI commands must exercise real SDK calls; unit tests (e.g. `cmd/lockd/client_cli_test.go`) ensure Cobra wiring doesn’t drift.
- Query ergonomics. Selector syntax is RFC 6901 + shorthand (`/field>=10`, braces, etc.). Update README + CLI help when syntax/extensions change.
- Streaming. `client.Query` + `QueryResponse` must keep streaming semantics zero-copy; CLI `--documents` streams NDJSON (no buffering).

### Integration Suite Coverage

- Each backend suite (`mem`, `disk`, `nfs`, `aws`, `azure`, `minio`) must:
  - Run selectors, pagination, namespace isolation, public reads.
  - Run query domain datasets (with dataset profile guards) and document streaming tests.
  - Clean up meta/state/index artifacts via the backend-specific helpers (`CleanupQueryNamespaces`, etc.).

- Queue suites (`.../lq`) cover advanced polling, QRF throttling, and chaos scenarios. Keep watchdogs active and prefer real `client.Client` usage.

### Storage & Encryption

- Crypto-on by default. Every backend routes state/meta/queue payloads through `internal/storage.Crypto` when enabled. Helpers (MinIO/Azure/AWS) must clean both encrypted objects and index manifests.
- CAS semantics. Use `PutObjectOptions{ExpectedETag}` for writes; backends must translate storage-specific errors into `storage.ErrCASMismatch` / `storage.ErrNotFound`.
- Indexing. Index manifests and segments live under `index/`. Cleaning routines must delete both manifest + segment files per namespace.

### Debugging Guardrails (unchanged principles, refreshed scope)

- Watchdogs on all long/parallel tests (AcquireForUpdate loops, queue chaos, YCSB pre-checks). Panic >=10s hangs and dump stacks.
- Deterministic chaos: when simulating network drops, default to a single disconnect (`MaxDisconnects=1`).
- Real clients only in integration tests. If a scenario fails, fix the SDK/CLI rather than mocking around it.
- Acquire-for-update contract: bounded retries (default 5). Update docs/tests if the limit changes.
- Namespace + queue contention: ensure every backend suite includes multi-server, multi-worker tests so namespace wiring and queue watchers stay healthy.
- Observed key tracking: never remove the observed-key warm-up logic in storage handlers; it shields us from eventual-consistency quirks.

## Collaboration Notes

- Record big work. Long-running benchmarks or refactors should leave breadcrumbs (PR notes, `docs/performance/`, `BACKLOG.md`).
- Communicate blockers. If a suite flakes, capture the log (`integration-logs/*.log`) and note it in the issue/PR before retrying.
- YCSB driver: treat it as a first-class client. Benchmark code lives in its own module so we can add dependencies without polluting the main build.

With this playbook, we can confidently evolve lockd (namespaces, query/index, CLI, YCSB) without clinging to outdated guardrails. Finish what you start, lean on tests, and keep shipping.
