# Testing Notes

This document captures the recent additions to our integration test harness so
future changes understand why things look the way they do.

## Structured Log Recording

Many of our integration assertions depend on the precise sequence of client log
events (e.g. “primary endpoint failed, backup succeeded”). To make those checks
easy, we introduced `integration/internal/testlog/testlog.go`:

- `testlog.NewRecorder(t, level)` returns a `logport` logger that still writes
  through `t.Log` but also records every structured entry (timestamp, message,
  fields).  The recorder exposes helpers to fetch the first entry that matches a
  predicate, the first entry after a given timestamp, and a human-readable
  summary of the most recent entries.
- The recorder is intentionally dumb – it never simulates behaviour – it simply
  watches the real logs emitted by the client/server.
- Every test that needs to reason about failover order can now capture those
  logs and fail with a readable summary when the expected sequence is missing.

## Deterministic Endpoint Ordering

Previously the client shuffled endpoints on every request. That’s great for
production but makes log-order assertions brittle, so the client now exposes
`client.WithEndpointShuffle(false)`.  When disabled the endpoints are tried in
the order provided, while default behaviour (shuffle enabled) remains the same.

## Backend / tag layout

Integration suites compile only when both the `integration` tag and a backend
tag are supplied. Optional feature tags further restrict the set. The queue
helpers currently recognise `lq`; additional features will follow the same
pattern.

```
go test -tags "integration <backend> [feature ...]" ./integration/...
```

Current backends and expected commands:

- `mem`: fast, deterministic queues using the in-memory store.
  - `go test -tags "integration mem" ./integration/...` (all mem suites)
  - `go test -tags "integration mem lq" ./integration/...` (queue-only)
- `disk`: local filesystem with optional queue watcher.
  - `set -a && source .env.disk && set +a && go test -tags "integration disk" ./integration/...`
  - `… -tags "integration disk lq" …` for queue-only runs
- `nfs`: disk backend pointed at an NFS mount.
  - `set -a && source .env.nfs && set +a && go test -tags "integration nfs lq" ./integration/...`
- `aws`, `minio`, `azure`: existing suites continue to use their dedicated env
  files. Queue wrappers will adopt the same `integration <backend> lq` flow as
  we extend coverage.

All queue suites attach trace-level loggers to both server and client. Expect
verbose output while the helpers assert dispatcher behaviour.

Mem queue suites default to the fast “notify” path only. Set
`LOCKD_LQ_INCLUDE_POLLING=1` to add the polling variants back in, or
`LOCKD_LQ_EXTENDED=1` to run the long-form stress/subscribe scenarios that are
normally skipped to keep CI under 30 s.

When debugging delivery stalls, `LOCKD_READY_CACHE_TRACE=1` adds ready-cache
refresh breadcrumbs to the server logs, while the benchmark harness exposes
`MEM_LQ_BENCH_DEBUG`, `MEM_LQ_BENCH_TRACE`, and `MEM_LQ_BENCH_EXTRA` for more
granular client-side instrumentation.

For object-store backends (`disk`, `minio`, `aws`, `azure`) we now include a
`PollingIntervalRespected` subtest. Each run verifies that, with subscribers
connected, the dispatcher still honours the configured 3 s poll floor against
the backing store (S3/Azure APIs) while delivering all queued work via the
watch path. This makes it easy to spot regressions where a code change reverts
to busy polling or skips the watch fallback entirely.

## Disk integration environment

Disk-backed suites now **require** real filesystem roots. Populate
`LOCKD_DISK_ROOT` (local disk, from `.env.disk`) and `LOCKD_NFS_ROOT`
(NFS mount, from `.env.nfs`) before running any disk or NFS tests—for example:

```
set -a
source .env.disk   # sets LOCKD_DISK_ROOT
source .env.nfs    # sets LOCKD_NFS_ROOT
set +a
go test -tags 'integration disk' ./integration/...
```

If either variable is missing the suite fails immediately; we no longer fall
back to temporary directories. This guarantees the “disk” subtests operate on
`LOCKD_DISK_ROOT` while the NFS subtests operate on `LOCKD_NFS_ROOT`,
matching the environment you provisioned.

## Disk AcquireForUpdate Failover Tests

`TestDiskAcquireForUpdateCallbackFailoverMultiServer` exercises the callback
helper under three deterministic failover phases:

1. **handler-start** – shut down the primary immediately after the callback
   begins and before reading any state.
2. **before-save** – acquire the snapshot, then drop the primary before
   writing the updated content.
3. **after-save** – run the entire callback, save the state, then drop the
   primary before release and verification.

Each subtest:

- Starts primary and backup servers, seeding the state through the backup so the
  callback has something to read.
- Creates a failover client with endpoint shuffling disabled and wraps it in a
  `testlog` recorder.
- Triggers the real failover by calling `primary.Server.Shutdown` at the chosen
  phase.
- Uses the recorded logs to assert that a `client.http.error` on the primary is
  followed (in timestamp order) by a `client.http.success` on the backup.
- Dumps the full log summary even on success, making it obvious what the
  expected flow looks like when someone scans CI output.

This deterministic pattern guarantees:

- We never rely on probabilistic chaos: every phase explicitly kills the
  primary.
- We verify the *actual* sequence of client operations, not a mocked or assumed
  flow.
- Tests fail with actionable diagnostics (the log summary) when the sequence
  changes.

## Extending the Pattern

The utilities are designed to be reused:

- To add similar checks for other backends (MinIO, AWS, Azure), create an
  equivalent helper that wires in the `testlog` recorder, disables endpoint
  shuffling, and triggers shutdown/failover at the phase you want to exercise.
- If future scenarios require different checkpoints (e.g. mid-stream reads,
  verify a recovery retry loop, etc.) just add another `failoverPhase` enum case
  and call `triggerFailover` at the relevant spot.

Because everything hangs off real servers and real clients, these tests reflect
production behaviour while still being repeatable and debuggable.  When new
failover paths appear, add a new phase + assertion and the log recorder will
give you reliable coverage.

## Queue client unit tests

The queue SDK surface is covered by dedicated unit tests in `client/client_test.go`.
`TestClientEnqueue` validates the multipart upload emitted by the client (meta JSON,
payload streaming, timing fields, attribute map). `TestClientDequeueHandlesLifecycle`
and `TestClientDequeueWithState` drive the `QueueMessage` wrapper through extend →
nack/ack flows against an `httptest.Server`, asserting that it updates fencing tokens,
ETags, and lease expiries exactly as the real API would. These tests intentionally
avoid mocks—the server handler inspects the real HTTP requests so regressions in the
wire format are caught early.

### Finding the “proof” in logs

Every successful failover test prints the recorder summary so the expected event
order is visible in CI logs. Grep for:

```
failover log summary:
```

and you’ll see the ordered `client.http.error` (primary) followed by
`client.http.success` (backup) entries that prove the client actually failed
over. The summary includes timestamps, messages, and endpoint fields which makes
manual verification quick when triaging incidents.
