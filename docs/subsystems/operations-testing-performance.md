# Operations, Sweepers, Testing, and Performance Subsystem

## 1) Purpose

This subsystem covers runtime safeguards (QRF/LSF, idle sweepers), verification workflows (unit/integration suites), and performance characterization (benchmarks/YCSB/profiling artifacts).

## 2) Architecture and implementation details

- Runtime safeguards:
  - QRF state machine in `internal/qrf/controller.go` applies adaptive throttling by operation kind.
  - LSF observer in `internal/lsf/observer.go` samples runtime/system pressure and feeds QRF snapshots.
- Maintenance:
  - idle sweeper loop is started in `server.go` and delegates to `core.SweepIdle`.
  - transparent cleanup and replay logic are integrated into active request paths (`clearExpiredLease`, `maybeReplayTxnRecords`).
- Testing workflows:
  - repository standard includes `go test ./...`, `go vet ./...`, linting, and backend integration suites via `run-integration-suites.sh`.
  - integration suites are backend-tagged (`mem`, `disk`, `nfs`, `aws`, `azure`, `minio`, queue/query variants).
- Performance workflows:
  - benchmark and profiling guidance is captured in `ycsb/README.md`, `ycsb/BENCHMARKS.md`, and `docs/performance/*`.
  - artifacts track queue baselines, query return=document baselines, attachments, and YCSB-style load runs.

## 3) Non-style improvements (bugs, security, reliability)

- Missing regression tests for known reader-retry hazards:
  - there is no direct test proving staged state retries rewind payload readers before CAS retry loops.
  - add focused tests around CAS mismatch + staged write retries to prevent silent zero-byte promotions.
- Missing regression tests for queue batch semantics:
  - current queue batch path should be guarded by tests that assert `page_size > 1` actually returns multiple deliveries.
  - this would have caught current batch-size clamping regressions earlier.
- Retry wrapper safety is under-specified in tests:
  - add tests for `internal/storage/retry` with non-rewindable readers and transient errors to prove no data corruption paths.
- TC endpoint validation lacks negative-path integration coverage:
  - add integration tests for malformed/unsafe TC cluster and RM endpoint inputs to ensure request fanout cannot be abused.

## Feature-aligned improvements

- Introduce an automated performance gate that compares current run output with previous `docs/performance` baselines and flags regressions by workload profile (queue, query-documents, attachments, ycsb-load).
