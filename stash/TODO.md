# LQL Migration + Index Engine Roadmap TODO

Date: 2026-02-25
Owner: lockd core/search
Scope: lockd-side migration and performance program to complete true inline streaming and lucene-like index behavior.

## How To Use This Checklist
- [ ] Before each iteration, pick one phase slice and check only items fully completed in that slice.
- [ ] Every checked item must be backed by code + tests + benchmark delta (where applicable).
- [ ] Every slice must end with a commit and updated benchmark notes in this file.
- [ ] Do not mark items complete if behavior only works on happy-path datasets.

## Completion Gates (apply to every implementation slice)
- [ ] `go test ./...`
- [ ] `go vet ./...`
- [ ] `golint ./...`
- [ ] `golangci-lint run ./...`
- [ ] Impacted integration suites (minimum: `./run-integration-suites.sh disk/query`)
- [ ] Benchmarks re-run for impacted hot paths with `-benchmem`
- [ ] No uncommitted changes left after commit

## Phase 0: Completed Foundation (already landed)
- [x] P0.1 Merge `lockd-mcp` into migration branch.
- [x] P0.2 `/v1/query?return=documents` changed to trailer-based metadata contract.
- [x] P0.3 Query documents stream path switched to inline sink flow (no handler re-query).
- [x] P0.4 Scan adapter uses LQL QueryStream for selector evaluation.
- [x] P0.5 Index adapter supports `contains`, `icontains`, `iprefix` selector families.
- [x] P0.6 `/v1/mutate` streaming endpoint added and wired through SDK + CLI + MCP tool.
- [x] P0.7 Query document streaming uses LQL `QueryDecisionPlusValue` capture path.
- [x] P0.8 Lockd temp-spool duplication removed from query-doc stream path.
- [x] P0.9 Index adapter post-filter only used when selector not index-native.
- [x] P0.10 Trigram contains correctness guard added (candidate verification; no false positives).
- [x] P0.11 Wildcard field expansion (`*`, `[]`) in index selector resolution.
- [x] P0.12 Recursive field expansion (`**`, `...`) via trie matcher with bounded fallback.
- [x] P0.13 Contains hot-path allocation reduction (workspace reuse) landed.

## Phase 1: Functional Parity Hardening (remaining correctness work)
- [x] P1.1 Build index-vs-scan selector parity suite over generated corpora.
- [x] P1.2 Add property tests for nested `and/or/not` with wildcard + recursive selectors.
- [x] P1.3 Add parity tests for mixed selector families: `{eq,range,in,exists,contains,prefix}` combinations.
- [x] P1.4 Add parity tests for malformed/edge pointers with wildcard tokens and escaped segments.
- [x] P1.5 Add parity tests for recursive selectors crossing numeric-like path segments.
- [x] P1.6 Add parity tests where some segments are missing from index (partial shard coverage).
- [x] P1.7 Add deterministic ordering tests under wildcard/recursive expansions.
- [x] P1.8 Add integration tests for large namespace + low-match selectors in return=keys mode.
- [x] P1.9 Add integration tests for large namespace + low-match selectors in return=documents mode.
- [x] P1.10 Add explicit tests for selector cursor stability across pages under wildcard queries.

## Phase 2: Query Streaming/Mutation Operational Hardening
- [x] P2.1 Add instrumentation counters for query-doc streaming: candidates seen/matched/captured/spilled.
- [x] P2.2 Add instrumentation counters for mutate streaming: bytes read/written + candidate counts.
- [x] P2.3 Enforce/verify strict single-candidate mutate mode where required by server API semantics.
- [x] P2.4 Add explicit non-retryable error classification tests in mutate + consumer restart surfaces.
- [x] P2.5 Add soak tests for query-doc stream cancellation/backpressure.
- [x] P2.6 Add soak tests for mutate stream cancellation/backpressure.
- [x] P2.7 Confirm no full-document in-memory buffering in lockd query-doc paths (heap profile proof).

## Phase 3: Index Performance Program (lucene-near trajectory)

### P3.A In-memory query representation (no format bump)
- [x] P3.A1 Add per-segment compiled cache that interns keys to dense `docID`.
- [x] P3.A2 Convert postings to query-time `[]docID` caches on segment load.
- [x] P3.A3 Keep string-key lookups at load boundary only; remove from hot query loops.
- [x] P3.A4 Add cache lifecycle tests (load, eviction/reload, correctness).
- [x] P3.A5 Benchmark delta report for A slice (`ns/op`, `B/op`, `allocs/op`).

### P3.B Set engine rewrite (core win)
- [x] P3.B1 Replace map-based set ops with sorted `[]docID` merge ops (union/intersect/subtract).
- [x] P3.B2 Use branch-friendly iterators for intersection-heavy paths.
- [x] P3.B3 Defer key materialization until response assembly only.
- [x] P3.B4 Preserve stable key ordering and cursor semantics.
- [x] P3.B5 Benchmark delta report for B slice.

### P3.C Dictionary and postings acceleration
- [x] P3.C1 Add in-memory field dictionary IDs and term dictionary IDs.
- [x] P3.C2 Route evaluator over IDs, not raw strings, in hot loops.
- [x] P3.C3 Add small-object pools for temporary posting iterators/work buffers.
- [x] P3.C4 Benchmark delta report for C slice.

### P3.D Index format v5 (persistent lucene-like structure)
- [x] P3.D1 Write v5 format design doc (manifest + segment schema + migration behavior).
- [x] P3.D2 Persist field dictionary + term dictionary + docID postings in segment format.
- [x] P3.D3 Implement reader/writer compatibility for v4 and v5.
- [ ] P3.D4 Add rebuild path and mixed-format handling tests.
- [ ] P3.D5 Add integration tests on upgrade/rebuild scenarios.
- [ ] P3.D6 Benchmark delta report for D slice.

### P3.E Compressed postings (optional but likely needed)
- [ ] P3.E1 Evaluate roaring/bitset strategy for dense postings.
- [ ] P3.E2 Evaluate delta+varint for sparse postings.
- [ ] P3.E3 Add adaptive representation policy (sparse vs dense).
- [ ] P3.E4 Add benchmarks comparing representations by selectivity bands.

## Phase 4: Wildcard + Recursive Query Planner Maturity
- [ ] P4.1 Add planner cost model for wildcard/recursive expansions (avoid pathological traversal).
- [ ] P4.2 Add selector-to-plan caching keyed by normalized selector AST.
- [ ] P4.3 Add safeguards for recursive explosion with deterministic fallback path.
- [ ] P4.4 Add benchmark profile for deep recursive selectors over wide field trees.
- [ ] P4.5 Add benchmark profile for wide wildcard selectors (`/a/*/*/*`) with low/high selectivity.
- [ ] P4.6 Add explicit fail-safe behavior tests when plan state cap is exceeded.

## Phase 5: Full-text Lucene-like Capability (index-side)
- [ ] P5.1 Define full-text scope for lockd (filtering only vs optional scoring/ranking).
- [ ] P5.2 Introduce analyzers (minimum): lowercase, token split, optional stemming toggle.
- [ ] P5.3 Add per-field text indexing mode (`raw`, `tokenized`, `both`).
- [ ] P5.4 Add optional namespace-level `_all_text` synthetic field for global text filters.
- [ ] P5.5 Route `icontains{f=/...,v=...}` through analyzer-aware postings where configured.
- [ ] P5.6 Keep exact contains semantics available where tokenized FTS would change behavior.
- [ ] P5.7 Add phrase/term-set behavior specification (if enabled) with compatibility tests.
- [ ] P5.8 Add full-text benchmark corpus generator (document length, vocabulary size, Zipf controls).
- [ ] P5.9 Add benchmark suite for tokenized full-text selectors across selectivity buckets.

## Phase 6: Integration Contract Verification (lockd test contract)
- [x] P6.1 Run full integration sweep with `./run-integration-suites.sh all`.
- [x] P6.2 For each failure, classify regression vs stale integration assumption.
- [x] P6.3 Update integration fixture assumptions for slow object-store backends where needed.
- [x] P6.4 Re-run full sweep to green and record suite summary.

## Phase 7: Benchmark Program and Diminishing Returns Policy
- [ ] P7.1 Freeze benchmark datasets (small/medium/large + wildcard/recursive/fulltext).
- [ ] P7.2 Add reproducible benchmark command set in docs.
- [ ] P7.3 Capture baseline table for all hot benchmarks (current head).
- [ ] P7.4 Require per-slice before/after table in commit notes.
- [ ] P7.5 Define diminishing-returns stop condition (e.g. <5% gain across 2 consecutive slices).
- [ ] P7.6 Once stop condition reached, switch to maintenance/perf-regression guard mode.

## Phase 8: Documentation + DX cleanup
- [ ] P8.1 Update `docs/subsystems/search-and-index.md` with current architecture.
- [ ] P8.2 Document selector support matrix (index-native vs post-filter fallback).
- [ ] P8.3 Document wildcard/recursive semantics and complexity notes.
- [ ] P8.4 Document benchmark workflow and expected acceptance gates.
- [ ] P8.5 Update CLI/SDK query docs where behavior changed (trailers, streaming details).

## Immediate Next Iteration Queue (pick in order)
- [x] N1 Execute P3.A1-P3.A3 in one focused slice (compiled segment cache + docID postings in memory).
- [ ] N2 Execute P3.B1-P3.B3 in next slice (set engine rewrite to `[]docID`).
- [x] N3 Re-run benchmark suite and record delta in this file under "Iteration Log".

## Iteration Log (append after each slice)
- [x] 2026-02-25: Wildcard field expansion (`*`, `[]`) + benchmark harness landed (`4f595f3`).
- [x] 2026-02-25: Recursive trie expansion (`**`, `...`) + contains alloc reduction landed (`641d77c`).
- [x] 2026-02-25: P3.A compiled docID segment cache landed (`9bc1900`): wildcard resolve `16.69 ns/op` (0 alloc), recursive resolve `25.01 ns/op` (0 alloc), wildcard contains `1,718,182 ns/op`, `891,440 B/op`, `3,397 allocs/op`, adapter wildcard contains `4,002,689 ns/op`, `2,279,944 B/op`, `9,593 allocs/op`.
- [x] 2026-02-25: Phase 1 parity base landed (`a27d9f0`): added index-vs-scan synthetic/property suites and aligned index pointer validation with scan behavior.
- [x] 2026-02-25: Phase 1 wildcard/recursive ordering + cursor stability landed (`3bf33b7`): added deterministic ordering checks, wildcard pagination cursor stability checks, and recursive numeric-path parity coverage.
- [x] 2026-02-25: Phase 1 segmented sparse-coverage parity landed (`8ba1398`): added multi-segment parity coverage where fields are sparse/missing across segments.
- [x] 2026-02-25: Phase 1 large-namespace low-match integration landed (`c1a7c2b`): added return=keys and return=documents low-match tests in shared query suite and wired all backend query wrappers.
- [x] 2026-02-25: Phase 2 instrumentation slice landed (`56071e2`): query-doc stream counters now exported in query metadata (headers/trailers), mutate stream summary counters exported in response headers, and adapter/http tests added.
- [x] 2026-02-25: Phase 2 strict-mutate + error-classification tests landed (`ec1498b`): verified single-object mutate mode rejection for non-object state and pinned mutate stream error-to-HTTP mapping.
- [x] 2026-02-25: Phase 2 query-doc flow-control integration landed (`ffed8d1`): added cancel-mid-stream and slow-reader/backpressure coverage in shared query suite and backend wrappers.
- [x] 2026-02-25: Phase 2 mutate flow-control tests landed (`20d7cf2`): added slow-writer backpressure completion test and cancellation-under-backpressure resilience test.
- [x] 2026-02-25: Phase 2 spill guard test landed (`b2ee40f`): added large-document scan query-doc test asserting spill counters are non-zero for matched streaming candidates.
- [x] 2026-02-25: Phase 2 heap-bounded streaming proof landed (pending commit): added disk-backed large-match query-doc test with live heap sampling, spill/capture assertions, and bounded heap delta checks.
- [x] 2026-02-25: P3.A4 cache lifecycle tests landed (pending commit): added compiled cache load/hit/evict/reload tests plus store segment-cache eviction/reload coverage.
- [x] 2026-02-25: P3.B1-P3.B3 docID set engine rewrite landed (pending commit): selector evaluator and index clause resolvers now operate on sorted `[]docID` with merge-based union/intersect/subtract; keys are materialized only at response assembly. Bench: wildcard contains `3,416,445 ns/op`, `2,535,359 B/op`, `7,966 allocs/op`; adapter wildcard contains `14,570,571 ns/op`, `2,499,966 B/op`, `13,639 allocs/op`.
- [x] 2026-02-25: P3.B4 ordering/cursor stability tests landed (pending commit): added explicit OR and NOT selector pagination tests to pin lexicographic ordering and cursor progression under docID set engine.
- [x] 2026-02-25: P3.C1-P3.C2 field/term dictionary IDs landed (pending commit): compiled segments now keep field-ID and term-ID dictionaries with postings by term-ID; evaluator hot loops resolve selector fields to IDs and query postings by IDs. Bench: wildcard resolve `14.74 ns/op` (0 alloc), recursive resolve `25.06 ns/op` (0 alloc), wildcard contains `2,311,919 ns/op`, `2,539,641 B/op`, `8,214 allocs/op`, adapter wildcard contains `4,048,498 ns/op`, `2,578,739 B/op`, `14,435 allocs/op`.
- [x] 2026-02-25: Scan/LQL numeric-map-key parity + object-store integration hardening landed (`9bef106`): scan adapter now preserves selector semantics for numeric JSON pointer segments by compatibility matching path; query integration suite now scales flow-control/low-match fixtures on object stores (`aws://`, `azure://`, `s3://`) to avoid false timeout failures while preserving behavioral assertions.
- [x] 2026-02-25: Full integration contract sweep passed (`./run-integration-suites.sh all`, elapsed `52:46`): mem/disk/nfs/aws/azure/minio/mixed all green.
- [x] 2026-02-25: P3.C3-P3.C4 pooling slice landed (pending commit): added pooled `docIDAccumulator` work buffers for repeated union/intersect/subtract loops in selector evaluation and postings aggregation. Bench: wildcard resolve `15.83 ns/op` (0 alloc), recursive resolve `24.92 ns/op` (0 alloc), wildcard contains `854,746 ns/op`, `186,210 B/op`, `4,106 allocs/op`, adapter wildcard contains `2,747,160 ns/op`, `1,963,217 B/op`, `12,386 allocs/op`.
- [x] 2026-02-25: P3.D1-P3.D3 v5 segment format wiring landed (pending commit): added v5 protobuf schema (`doc_table`, `field_dict`, `term_dict`, `doc_ids` postings), implemented v5 encode/decode in `Segment.ToProto/FromProto`, fixed manifest cache format cloning, added writer manifest-format inheritance, and added mixed v4/v5 query compatibility tests.
- [ ] YYYY-MM-DD: Next slice commit + benchmark delta summary.
