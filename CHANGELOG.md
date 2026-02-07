# Changelog

## 2026-02-07 - Audit hardening sweep (chunks 1-8 + verification)

### Summary

This update closes the subsystem audit implementation plan from `TODO.md` and lands the prioritized hardening work across storage, core state handling, queueing, streaming, query/index control paths, TC/RM endpoint safety, and HTTP API input validation.

### Chunk outcomes

- Chunk 1 (storage replay safety + staged CAS retries):
  - Enforced replay-safe retry semantics for write bodies in `internal/storage/retry`.
  - Added fail-fast behavior for non-replayable bodies under retry conditions.
  - Fixed staged state CAS retry path to restage using rewound payload readers.
- Chunk 2 (queue correctness):
  - Restored dequeue batching behavior for `page_size > 1`.
  - Hardened dispatcher fetch paths against nil descriptor dereferences.
  - Improved context propagation in queue dispatcher polling/fetch.
- Chunk 3 (streaming robustness):
  - Added cancellation-aware encrypted pipe producer shutdown for queue and attachment paths.
  - Prevented producer goroutine leaks on early consumer exit.
- Chunk 4 (query/index hardening):
  - Added async index flush in-flight dedupe and cap.
  - Added body-size limits for index flush requests.
  - Surfaced scan adapter read failures instead of silently dropping matches.
- Chunk 5 (TC/RM endpoint hardening):
  - Added endpoint normalization/validation for TC cluster and RM registry flows.
  - Switched fanout URL construction to safe endpoint join helpers.
- Chunk 6 (metadata/staging consistency):
  - Fixed staged object discovery/listing behavior for nested `/.staging/` layouts.
  - Deep-copied metadata map fields to prevent aliasing and mutation bleed.
- Chunk 7 (mutating endpoint strict decode):
  - Enforced strict JSON decode for mutating HTTP API endpoints (with compatibility-aware exceptions).
- Chunk 8 (regression guard backfill):
  - Added focused regression coverage for replay safety, queue batching, and strict decoding.

### Verification and closure

- Full release gate run:
  - `go test ./...`: PASS
  - `go vet ./...`: PASS
  - `golint ./...`: PASS
  - `golangci-lint run ./...`: PASS after cleanup below
  - Full integration sweep (`mem|disk|nfs|aws|azure|minio|mixed` variants): PASS (`Elapsed: 41:20`)
- Benchmark suites:
  - `./run-benchmark-suites.sh disk minio mem aws azure`: all suites PASS
- Post-verification cleanup:
  - Removed dead helper causing lint failure in integration query suite.

### Commits by chunk

- `1cd9353` `fix(storage): enforce replay-safe write retries and restage CAS updates`
- `8d9862d` `fix(queue): restore dequeue batching and harden dispatcher fetch safety`
- `71e4f04` `fix(streaming): cancel encrypted pipe producers on early exit`
- `e0f83af` `fix(query): cap async index flushes and surface scan read failures`
- `dbef30d` `fix(tc): validate endpoints and harden fanout URL joining`
- `fc12ab6` `fix(storage): correct staged discovery and deep-copy metadata`
- `fca0e9b` `fix(httpapi): enforce strict decode on mutating JSON endpoints`
- `9219441` `test(regression): backfill chunk 8 coverage guards`
- `77609a4` `chore(integration): remove unused query suite helper`
