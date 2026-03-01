# lockd-bench parity confirmation (disk)

Date: 2026-03-04

## Scope
- Backend: `disk`
- Host: `wopr`, `nproc=12`
- Profile under test:
  - `indexer-flush-docs: 2000`
  - `logstore-commit-max-ops: 4096`
  - `logstore-segment-size: 64MB`
  - `qrf-disabled: true` (server profile for YCSB)

## Result summary

| Workload | Command shape | Reference | Observed | Status |
| --- | --- | ---: | ---: | --- |
| `attachments` | embedded, `ops=10000`, `runs=3`, `warmup=1`, `concurrency=8`, `payload=256`, `attachment=256` | 692.9 | 1408.0 | PASS |
| `attachments-public` | embedded, `ops=10000`, `runs=3`, `warmup=1`, `concurrency=8`, `payload=256`, `attachment=256` | 719.7 | 815.5 | PASS |
| `public-read` | embedded, `ops=10000`, `runs=3`, `warmup=1`, `concurrency=8`, `payload=256` | 47182.5 | 49680.3 | PASS |
| `query-index` | embedded, `ops=2000`, `runs=3`, `warmup=0`, `concurrency=8`, `return=documents`, `qrf-disabled` | 10215.7 (update median) | 10214.9 / 10006.7 | PASS (within recent band) |
| `query-scan` | embedded, `ops=2000`, `runs=3`, `warmup=0`, `concurrency=8`, `return=documents`, `qrf-disabled` | 3063.6 (update median) | 2768.0 / 2822.7 | PASS (within recent validation band) |

Reference sources:
- Attach/public parity: `docs/performance/2026-01-26-lockd-bench-attachments-xa-public-parity/summary.md`
- Query medians + validation spread: `docs/performance/2026-02-28-benchmark-update/lockd-bench-query/*.log`, `docs/performance/2026-02-28-benchmark-validation/*.log`

## Notes

### Query workload variance and QRF
- With embedded QRF enabled, intermittent `soft_arm` events produced large outliers (for example query-index dropping to ~4.8k ops/s in one run).
- Running query parity with `-qrf-disabled` removed those outliers and restored stable medians aligned with February validation behavior.

### Endpoint load command drift
- Historical endpoint load doc used:
  - `-bundle /home/mike/g/lockd/devenv/volumes/lockd-config/client.pem`
- Under current namespace ACL semantics this now returns `namespace_forbidden` for generated `bench-*` namespaces.
- Current valid benchmark bundle is:
  - `-bundle /home/mike/g/lockd/devenv/volumes/lockd-config/bench-client.pem`
- With `bench-client.pem`, endpoint `mode=load` measured `861.3 ops/s` in this pass; this is lower than the January `1052.1` reference and should be treated as a separate endpoint-load follow-up rather than a blocker for embedded lockd-bench parity.

## Follow-up iteration: release fast-path (no staged changes)

Code change:
- Added a hot path in `internal/core/locks.go` for non-explicit releases with no staged changes, bypassing full txn-apply machinery and directly clearing lease/meta.
- Added regression coverage in `internal/core/release_test.go`:
  - `TestReleaseNoStagingRetriesOnMetaCASMismatch`

Verification:
- `go test ./...`
- `go vet ./...`
- `golint ./...`
- `golangci-lint run ./...`

Endpoint load re-check after rebuild (`ops=20000`, endpoint mode, `bench-client.pem`):
- `total ops/s=925.2` (same order as pre-change endpoint range ~`920-932`).

Interpretation:
- This optimization is correctness-preserving and should reduce CPU for pure lease-clear release paths.
- It does not move the current endpoint load bottleneck materially; the remaining hotspot is staged-commit/index path (`applyTxnDecisionForMeta` / release handling), which requires a separate optimization pass.

## Follow-up iteration: staged index-doc reuse in txn apply

Code change:
- Updated `internal/core/txn.go` so commit indexing can reuse a prepared document from staged state instead of always doing an extra post-promote state read.
- Kept mismatch semantics intact: when committed head ETag differs, indexing now explicitly reads/indexes the committed head (not the staged payload).
- Added regression coverage in `internal/core/txn_test.go`:
  - `TestApplyTxnDecisionCommitIndexesStateUsesSingleReadState`
  - `TestApplyTxnDecisionCommitIndexesHeadOnETagMismatch`

Verification:
- `go test ./internal/core`

Endpoint load re-check after rebuild (`ops=20000`, endpoint mode, `bench-client.pem`):
- run #1: `total ops/s=975.1`
- run #2: `total ops/s=969.8`
- previous iteration reference: `925.2`

Interpretation:
- This pass improved endpoint load throughput by roughly 4.8%-5.4% versus the previous `925.2` result.
- Endpoint load still remains below the January reference (`1052.1`), so additional staged-commit/release-path optimization is still required.

## Follow-up iteration: retry staging + lease-expiry boundary hardening

Code changes:
- `internal/storage/retry/retry.go`
  - Added `storage.StagingBackend` support on the retry wrapper (`StageState`, `LoadStagedState`, `PromoteStagedState`, `DiscardStagedState`, `ListStagedState`) so staging operations get the same transient retry policy as other storage writes.
- `internal/core/lease_validation.go`
  - Tightened lease expiry check from `< now.Unix()` to `<= now.Unix()` to match call sites that already treat `ExpiresAtUnix <= now.Unix()` as expired.
  - Prevents nil-result edge cases in update/remove style flows at exact second boundaries.
- `internal/storage/disk/staging.go`
  - Updated `lockStateKeys` to avoid double-locking identical stripe mutexes when different keys map to the same lock stripe.

Regression tests:
- `internal/storage/retry/retry_test.go`
  - `TestWrapExposesStagingBackendAndPromoteFlow`
  - `TestStageStateRetriesWithDefaultStagingFallback`
- `internal/core/lease_validation_test.go`
  - `TestValidateLeaseTreatsEqualExpiryAsExpired`
  - `TestUpdateReturnsLeaseExpiredAtExpiryBoundary`
- `internal/storage/disk/staging_test.go`
  - `TestDiskLockStateKeysStripeCollisionNoDeadlock`

Verification:
- `go test ./...`
- `go vet ./...`
- `golint ./...`
- `golangci-lint run ./...`
- `./run-integration-suites.sh mem disk minio`

Endpoint load re-check after rebuild (`ops=20000`, endpoint mode, `bench-client.pem`):
- run #1: `total ops/s=965.2`
- run #2: `total ops/s=971.6`

Interpretation:
- Throughput remains in the recent parity band (close to prior `969.8`/`975.1`) with no transport errors in re-runs.
- The lease-expiry edge panic path is now covered by regression tests.

## Follow-up iteration: default promotion

Code/config change:
- Promoted the validated parity profile defaults:
  - `DefaultLogstoreCommitMaxOps: 4096` (was 1024)
  - `DefaultIndexerFlushDocs: 2000` (was 1000)
  - Disk backend `IndexerFlushDefaults(): 2000, 10s`

Regression coverage:
- `config_test.go`: `TestConfigValidateDefaults` now asserts `LogstoreCommitMaxOps` default.
- `internal/storage/disk/disk_test.go`: `TestDiskIndexerFlushDefaults` asserts disk index defaults.

Sample validation (embedded lockd-bench load, disk, `ops=20000`, `concurrency=8`, `ha=failover`):
- `commit_max_ops=4096`
- `total ops/s=1323.5`
- `errors=0`
