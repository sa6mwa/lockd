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
  - search/index perf slices use the frozen benchmark profile set and command line in `docs/performance/2026-02-25-search-index-phase7-baseline/summary.md`.
  - artifacts track queue baselines, query return=document baselines, attachments, and YCSB-style load runs.

## 3) Implemented non-style improvements (bugs, security, reliability)

- Regression guard coverage was backfilled for completed hardening chunks:
  - replay-safety and non-replayable fail-fast tests in `internal/storage/retry/retry_test.go`.
  - queue batch behavior and dispatcher fetch safety guards in `internal/core/queuedelivery_batch_test.go`.
  - strict-decode behavior guards in `internal/httpapi/handler_strict_decode_test.go`.
- Full release verification gate was executed after chunk landing:
  - `go test ./...`, `go vet ./...`, `golint ./...`, `golangci-lint run ./...`, and full `run-integration-suites.sh` sweep.
- Full benchmark verification was executed:
  - `run-benchmark-suites.sh` completed for `disk`, `minio`, `mem`, `aws`, and `azure`.
- Post-verification lint cleanup was completed:
  - removed unused `waitForBucketEmpty` helper from `integration/query/suite/suite.go`.

## Feature-aligned improvements

- Introduce an automated performance gate that compares current run output with previous `docs/performance` baselines and flags regressions by workload profile (queue, query-documents, attachments, ycsb-load).
