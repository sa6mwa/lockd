# Index Format v5 Design

Date: 2026-02-25  
Owner: search/index

## Goal

Index format v5 persists the runtime dictionary representation introduced in the
in-memory evaluator:

- field dictionary IDs
- term dictionary IDs per field
- docID postings

This removes repeated string materialization on segment load and establishes a
stable on-disk representation for ID-based evaluation.

## Non-goals

- Scoring/ranking semantics.
- Query language changes.
- Changing query response ordering/cursor behavior.

## Compatibility Requirements

- Readers must support both v4 and v5 segment payloads.
- Writers may emit v5 only when the namespace/index format is explicitly set to
  v5 (or upgraded by rebuild).
- Mixed-format manifests are allowed during rolling upgrade/rebuild windows.
- Query behavior must remain identical across v4 and v5.

## Manifest v5

Manifest stays structurally compatible with the current shape:

- `Format=5` signals v5-capable shards/segments.
- Existing `Shard -> SegmentRef` topology remains.
- No API change required for query response metadata.

## Segment v5 Schema

Each segment stores:

- `DocTable`:
  - dense `docID -> key` list (keys in lexical order optional; query ordering is
    still enforced at response assembly).
- `FieldDict`:
  - dense `fieldID -> fieldPath`.
- `FieldBlocks` keyed by `fieldID`:
  - `TermDict`: dense `termID -> normalized term`.
  - `Postings`: `termID -> []docID` (sorted unique).

Optional metadata:

- doc metadata side table (`key/docID -> DocumentMetadata`) retained as today.

### Encoding notes

- All IDs are `uint32`.
- Postings arrays must be strictly sorted and deduplicated at write time.
- Future compression (Phase 3.E) wraps `Postings` payloads without changing
  semantic shape.

## Reader Behavior

### v5 segment load

Reader hydrates:

- `keyByDocID`
- `fieldIDByName`
- `termIDByValue` per field block
- postings arrays

No string-key expansion is needed in hot query loops.

### v4 segment load

Reader converts legacy map/string layout into the same internal v5-like
compiled structure (current behavior), preserving compatibility.

## Writer Behavior

- v4 writer path remains unchanged for compatibility mode.
- v5 writer path emits dictionaries + postings arrays directly.
- Segment validation must enforce sorted/unique postings for deterministic query
  behavior.

## Migration & Rebuild

Upgrade path:

1. Deploy reader that supports v4+v5.
2. Enable rebuild/compaction writer with `Format=5`.
3. Rebuild namespace index shards to v5.
4. Flip manifest `Format=5` after at least one valid v5 segment exists per
   shard.

Rollback path:

- Keep v4 reader support.
- Rebuild back to v4 if required.

## Test Plan

- Unit:
  - v5 segment round-trip encode/decode.
  - v4->internal and v5->internal equivalence for selector evaluation.
  - mixed manifest (v4+v5 segments) parity.
- Integration:
  - namespace rebuild from v4 to v5 preserves query/mutate behavior.
  - restart/reload with v5 segments.
- Performance:
  - benchmark deltas on wildcard/contains/range selectors with `-benchmem`.

## Open Decisions

- Whether `DocTable` should be globally sorted or preserve insertion order.
  Current recommendation: keep query output sorting as the behavioral source of
  truth, independent of doc table order.
- Compression codec choice for Phase 3.E (roaring vs delta-varint by density).
