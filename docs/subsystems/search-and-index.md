# Search + Index Subsystem

## Purpose

The search subsystem serves namespaced selector queries and streaming document scans with two execution engines:
- `index` for postings-based lookup
- `scan` for full storage traversal

It also owns server-side streaming mutation (`/v1/mutate`) so selector/mutator logic stays close to storage I/O without buffering whole documents in memory.

## Query and Mutate Flows

### Query (`/v1/query`)

- `return=keys`:
  - response body: JSON envelope (`namespace`, `keys`, `cursor`, metadata)
  - response headers: `X-Lockd-Query-*`
- `return=documents`:
  - response body: NDJSON rows (`{"ns","key","ver","doc":...}`)
  - query metadata/cursor/index sequence are emitted in trailers:
    - `X-Lockd-Query-Cursor`
    - `X-Lockd-Query-Index-Seq`
    - `X-Lockd-Query-Metadata`

`X-Lockd-Query-Metadata` includes stream counters (`query_candidates_seen`, `query_candidates_matched`, `query_bytes_captured`, `query_spill_count`, `query_spill_bytes`).

### Mutate (`/v1/mutate`)

- server loads lease-visible JSON for the key
- applies LQL mutate plan in streaming mode (`MutateSingleObjectOnly`)
- streams mutated JSON directly into update path (`core.Update`) via pipe
- returns normal update response + mutate stream headers:
  - `X-Lockd-Mutate-Candidates-Seen`
  - `X-Lockd-Mutate-Candidates-Written`
  - `X-Lockd-Mutate-Bytes-Read`
  - `X-Lockd-Mutate-Bytes-Written`
  - `X-Lockd-Mutate-Bytes-Captured`
  - `X-Lockd-Mutate-Spill-Count`
  - `X-Lockd-Mutate-Spill-Bytes`

## Engine Selection and Fallback

- `internal/search/dispatcher.go` routes by requested engine hint (`auto|index|scan`)
- `auto` tries index first, then scan on capability/streaming fallback boundaries
- index adapter supports full document streaming (`QueryDocuments`), so query-doc paths remain single-pass and do not re-query per match

## Selector Support Matrix

Current index-native selector families:

| Family | Index-native |
| --- | --- |
| `eq` | yes |
| `prefix` | yes |
| `iprefix` | yes |
| `contains` | yes |
| `icontains` | yes |
| `range` | yes |
| `in` | yes |
| `exists` | yes |
| `and` / `or` / `not` compositions | yes (recursive evaluation) |

Path expansion support in index engine:
- wildcard object/array child: `*`, `[]`
- immediate mixed child: `**`
- recursive descendant: `...`

If a selector family is not index-native, the index adapter falls back to:
- index candidate enumeration (`allDocIDs`)
- per-key LQL post-filter evaluation

This preserves correctness while keeping index-native families fast.

## Full-text Behavior (Phase 5)

- scope is filtering-only; no ranking/scoring layer is added in lockd
- string indexing supports per-field mode:
  - `raw`: exact raw term + trigram postings only
  - `tokenized`: analyzer token postings only
  - `both`: raw/trigram + tokenized postings
- analyzer behavior (default analyzer):
  - lowercase normalization
  - token split on non-alphanumeric boundaries
  - optional simple suffix stemming toggle (`ing|ed|es|s`)
- optional synthetic `_all_text` token field is supported via `__tok__:_all_text`

`icontains` execution semantics:
- tokenized postings are used as analyzer-aware prefilter when present
- when raw postings exist, final match still validates exact substring semantics
- when field is tokenized-only (no raw postings), `icontains` resolves by token-set match

`contains` execution semantics:
- remains raw substring semantics; tokenized postings are not used
- this preserves exact-contains behavior when tokenized full-text mode differs

Phrase/term-set behavior:
- single token query uses direct token term lookup
- multi-token query uses AND-intersection over tokens as candidate set
- with raw postings present, candidates are validated against raw substring match
- with tokenized-only mode, intersection result is the final match set

## Wildcard/Recursive Semantics and Complexity

- wildcard and recursive selectors are resolved against indexed field dictionaries
- planner heuristics bound expansion state; large recursive trees can trigger deterministic fallback traversal
- cursor/key ordering is still lexicographic and pagination-stable under wildcard/recursive selectors
- `contains`/`icontains` use trigram postings for candidate narrowing and term verification for correctness

## Index Format and Rebuild

- v5 segments persist doc tables + field/term dictionaries + docID postings
- reader remains compatible with v4 and v5 segments
- rebuild/reset preserves target format and removes lower-format legacy segments when required

See `docs/subsystems/index-format-v5.md` for schema and migration notes.

## Benchmark Workflow and Acceptance Gates

Phase-7 benchmark contract:
- frozen datasets: `small|medium|large` across wildcard/recursive/fulltext-like selectors
- reproducible command and baseline table:
  - `docs/performance/2026-02-25-search-index-phase7-baseline/summary.md`

Per perf slice requirements:
- run frozen benchmark set with `-benchmem`
- include before/after table in commit notes
- stop structural perf rewrites after two consecutive slices with median `<5%` gains in both `ns/op` and `allocs/op`; then move to regression-guard mode
