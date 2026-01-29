# Quick Reaction Force (QRF) and Local Security Force (LSF)

Lockd ships with a perimeter-defence layer that keeps the service and its
storage backends healthy under unbounded client traffic. Two subsystems work in
concert:

* **Local Security Force (LSF)** samples process- and host-level telemetry,
  tracks in-flight operations, and forwards snapshots.
* **Quick Reaction Force (QRF)** evaluates the snapshots, transitions between
  throttling postures, and instructs the HTTP handlers to pace requests.

Together they prevent runaway fan-in/fan-out patterns from exhausting CPU,
memory, or storage bandwidth while still favouring work that helps the system
recover.

## Components

### Local Security Force (LSF)

* Runs inside every lockd process (enabled by default).
* Samples on a configurable cadence (200 ms by default).
* Tracks per-kind in-flight counters:
  * queue producers (enqueue)
  * queue consumers (dequeue)
  * queue acknowledgers (ack/nack/extend)
  * lock/state operations (acquire/keepalive/update/remove)
  * query operations (`/v1/query`)
* Captures environment telemetry via `unix.Sysinfo` and `/proc` (prefers `MemAvailable`, falls back to the stricter `freeram+bufferram` view when `/proc` is unavailable):
  * process RSS and Go heap usage
  * system memory and swap utilisation (percentage and absolute bytes)
  * 1/5/15-minute load averages and exponential baselines
  * CPU utilisation deltas
  * goroutine counts
* Emits debug samples using the `lockd.lsf.sample` log event (default cadence: every 15 s; tune with `--lsf-log-interval`, set `0` to disable).
* Forwards snapshots to the QRF controller.

### Quick Reaction Force (QRF)

* Evaluates each snapshot and maintains a state machine:
  * **disengaged** – all clear, no throttling.
  * **soft_arm** – early warning, light pacing of producers and background work.
  * **engaged** – aggressive throttling applied, prioritising consumers and
    idempotent control paths (ack/nack/extend) so queues can drain.
  * **recovery** – metrics improving; throttling gradually relaxes until the
    system is healthy.
* Distinguishes between queue producers/consumers/acks, lock calls, and queries
  so the right workloads get preference while recovering. When host-wide
  pressure (memory/load/CPU) is the trigger, QRF targets whichever inflight
  category is dominant rather than blindly throttling everything.
* Logs transitions via `lockd.qrf.soft_arm`, `lockd.qrf.engaged`,
  `lockd.qrf.recovery`, and `lockd.qrf.disengaged`, including the trigger reason
  (queue inflight, consumer ceiling, memory, swap, CPU, or load-multiplier
  breach). When only strict `sysinfo` data is available, the controller
  automatically subtracts a configurable headroom (15 % by default) before
  evaluating memory percentages so cache-heavy hosts are not over-throttled.
* Provides `Server.QRFStatus()` for diagnostics and `/v1` handlers query it on
  every response.

## Back-pressure semantics

When the QRF decides to throttle a request, the handler **paces the request
server-side** by waiting for a computed delay before proceeding. This keeps
latency predictable while avoiding the “thrash” pattern of immediate retries.

Only if the delay exceeds `--qrf-max-wait` (or the request context deadline)
does the handler return **HTTP 429 Too Many Requests** with:

* JSON body containing `retry_after_seconds`.
* `Retry-After` response header (always seconds for now).
* `X-Lockd-QRF-State` header reporting the current posture
  (`disengaged`, `soft_arm`, `engaged`, or `recovery`).

The official Go client honours these signals automatically (see below). CLI
commands also surface the recommended sleep via the `LOCKD_CLIENT_RETRY_AFTER`
environment variable.

On success responses the same headers may appear so operators can observe the
current posture without hitting an error path.

## Configuration

All knobs live on `lockd.Config` and can be tuned via CLI flags or environment
variables (prefixed with `LOCKD_`). Every knob below lists the flag, the
corresponding environment variable, and a short description:

| Flag | Environment variable | Meaning |
|------|----------------------|---------|
| `--qrf-disabled` | `LOCKD_QRF_DISABLED` | Disable the perimeter defence (default: false). |
| `--qrf-queue-soft-limit` | `LOCKD_QRF_QUEUE_SOFT_LIMIT` | Soft inflight threshold across producers/consumers/acks (`0` keeps it disabled). |
| `--qrf-queue-hard-limit` | `LOCKD_QRF_QUEUE_HARD_LIMIT` | Hard inflight threshold that immediately engages the QRF (`0` keeps it disabled). |
| `--qrf-queue-consumer-soft-limit` | `LOCKD_QRF_QUEUE_CONSUMER_SOFT_LIMIT` | Soft ceiling for concurrent queue consumers; defaults to ~75 % of `QueueMaxConsumers`. |
| `--qrf-queue-consumer-hard-limit` | `LOCKD_QRF_QUEUE_CONSUMER_HARD_LIMIT` | Hard ceiling for concurrent queue consumers; defaults to `QueueMaxConsumers`. |
| `--qrf-lock-soft-limit` | `LOCKD_QRF_LOCK_SOFT_LIMIT` | Soft inflight threshold for lock/state operations (`0` disables). |
| `--qrf-lock-hard-limit` | `LOCKD_QRF_LOCK_HARD_LIMIT` | Hard inflight threshold for lock/state operations (`0` disables). |
| `--qrf-query-soft-limit` | `LOCKD_QRF_QUERY_SOFT_LIMIT` | Soft inflight threshold for query operations (`0` disables). |
| `--qrf-query-hard-limit` | `LOCKD_QRF_QUERY_HARD_LIMIT` | Hard inflight threshold for query operations (`0` disables). |
| `--qrf-memory-soft-limit` | `LOCKD_QRF_MEMORY_SOFT_LIMIT` | Process RSS soft guardrail (blank/`0` disables; off by default). |
| `--qrf-memory-hard-limit` | `LOCKD_QRF_MEMORY_HARD_LIMIT` | Process RSS hard guardrail (blank/`0` disables; off by default). |
| `--qrf-memory-soft-limit-percent` | `LOCKD_QRF_MEMORY_SOFT_LIMIT_PERCENT` | Host-wide memory usage percentage that soft-arms the QRF (default: 80 %). |
| `--qrf-memory-hard-limit-percent` | `LOCKD_QRF_MEMORY_HARD_LIMIT_PERCENT` | Host-wide memory usage percentage that fully engages the QRF (default: 90 %). |
| `--qrf-memory-strict-headroom-percent` | `LOCKD_QRF_MEMORY_STRICT_HEADROOM_PERCENT` | Percentage of usage to discount when `/proc/meminfo` is unavailable and only strict `sysinfo` readings exist (default: 15 %). |
| `--qrf-swap-soft-limit` | `LOCKD_QRF_SWAP_SOFT_LIMIT` | Swap usage soft guardrail (bytes, human-friendly units allowed). |
| `--qrf-swap-hard-limit` | `LOCKD_QRF_SWAP_HARD_LIMIT` | Swap usage hard guardrail (bytes). |
| `--qrf-swap-soft-limit-percent` | `LOCKD_QRF_SWAP_SOFT_LIMIT_PERCENT` | Swap utilisation percentage that soft-arms the QRF (`0` disables). |
| `--qrf-swap-hard-limit-percent` | `LOCKD_QRF_SWAP_HARD_LIMIT_PERCENT` | Swap utilisation percentage that fully engages the QRF (`0` disables). |
| `--qrf-cpu-soft-limit` | `LOCKD_QRF_CPU_SOFT_LIMIT` | System CPU utilisation percentage that soft-arms the QRF (default: 80 %; set `0` to disable). |
| `--qrf-cpu-hard-limit` | `LOCKD_QRF_CPU_HARD_LIMIT` | System CPU utilisation percentage that fully engages the QRF (default: 90 %; set `0` to disable). |
| `--qrf-load-soft-limit-multiplier` | `LOCKD_QRF_LOAD_SOFT_LIMIT_MULTIPLIER` | Load-average multiplier that soft-arms the QRF (default: 4× the LSF baseline). |
| `--qrf-load-hard-limit-multiplier` | `LOCKD_QRF_LOAD_HARD_LIMIT_MULTIPLIER` | Load-average multiplier that fully engages the QRF (default: 8× the LSF baseline). |
| `--qrf-recovery-samples` | `LOCKD_QRF_RECOVERY_SAMPLES` | Number of consecutive healthy samples before softening/disengaging (default: 5). |
| `--qrf-soft-delay` | `LOCKD_QRF_SOFT_DELAY` | Base delay used while the QRF is soft-armed. |
| `--qrf-engaged-delay` | `LOCKD_QRF_ENGAGED_DELAY` | Base delay used while the QRF is fully engaged. |
| `--qrf-recovery-delay` | `LOCKD_QRF_RECOVERY_DELAY` | Base delay used while the QRF is recovering. |
| `--qrf-max-wait` | `LOCKD_QRF_MAX_WAIT` | Maximum delay applied before returning HTTP 429. |
| `--lsf-sample-interval` | `LOCKD_LSF_SAMPLE_INTERVAL` | Override the Local Security Force sampling cadence (default: 200 ms). |
| `--lsf-log-interval` | `LOCKD_LSF_LOG_INTERVAL` | Interval between `lockd.lsf.sample` logs; set `0` to disable (default: 15 s). |

When left unset, the consumer guardrails derive from `QueueMaxConsumers`: the
soft limit defaults to roughly 75 % of that value while the hard limit tracks
the configured maximum. This lets the controller ease into throttling while
still honouring the absolute per-server ceiling. If producers substantially
outnumber consumers, the controller continues to admit consumer requests (up to
the hard limit) so queues can drain faster.

Defaults prioritise quick producer throttling while giving consumers room to
drain the backlog. On multi-tenant clusters you can tailor the soft limits per
queue by running multiple lockd deployments with different configuration.

## Observability

* **Logs**:
  * `lockd.lsf.sample` – periodic snapshots with memory, CPU, and load metrics.
  * `lockd.qrf.soft_arm`, `lockd.qrf.engaged`, `lockd.qrf.recovery`, `lockd.qrf.disengaged` – posture changes with reason and latest snapshot details.
  * `queue.dispatcher.qrf_throttle` (and similar) – per-request decisions.
* **Runtime inspection**:
  * `Server.QRFState()` returns the current posture.
  * `Server.QRFStatus()` returns a status struct with state, last reason, and
    latest snapshot (useful in unit/integration tests or health endpoints).
  * `/v1/readyz` stays green even while the QRF is engaged; consumers should rely
    on HTTP status codes and headers for pacing.

When investigating throttling, capture both the QRF transition logs and the
LSF samples to understand which metric tripped the guardrail.

## Client behaviour

The Go SDK inspects `Retry-After` and `X-Lockd-QRF-State` on every response
when a 429 is returned:

* Automatic backoff honours the suggested delay during `Acquire`, `AcquireForUpdate`,
  and queue operations.
* Structured logs include the active QRF state, so client-side telemetry mirrors
  the server.
* `APIError` exposes the parsed `RetryAfter` duration and `QRFState` string for
  custom handlers or third-party SDKs.

Other clients should follow the same contract:

1. Parse `Retry-After` (seconds) and sleep for at least that long before retrying.
2. Surface the QRF posture to operators (e.g. metrics, logs).
3. Prefer draining work (acks, dequeues) over adding new load while throttled.

For a hands-on walkthrough of queue behaviour under load, see the integration
tests under `integration/*/lq` and the AWS load test in `loadtest/aws/lq/`.
