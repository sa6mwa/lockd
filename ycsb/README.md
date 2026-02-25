# lockd + YCSB

This module hosts the lockd binding for [go-ycsb](https://github.com/pingcap/go-ycsb). It lets us benchmark the lock/state/query surfaces with industry-standard workloads and quickly compare storage backends (disk, MinIO/S3, Azure Blob, etc.) by simply pointing the driver at the corresponding lockd deployment.

## Contents

- `db/lockd`: go-ycsb database driver that speaks the lockd HTTP API via the official Go SDK.
- `cmd/lockd-ycsb`: convenience runner (forked from go-ycsb) that auto-registers the lockd driver.
- `workloads/`: property files you can use as templates when running benchmarks.

## Building the runner

```bash
cd ycsb
go build ./cmd/lockd-ycsb
```

This produces a `lockd-ycsb` binary with all upstream workloads plus the lockd driver. You can pass the same flags as `go-ycsb`; for example:

```bash
./lockd-ycsb load lockd -P workloads/workloada.properties -p lockd.endpoints=https://127.0.0.1:9341 -p lockd.bundle=$HOME/.config/lockd/client.pem
./lockd-ycsb run lockd -P workloads/workloada.properties -p recordcount=10000 -p operationcount=100000
```

## Makefile quickstart

The `ycsb/Makefile` wraps the runner with sane defaults and comparable lockd vs etcd commands. It also prints a hint if the local compose stack is not running.

```bash
cd ycsb
make build
make lockd-load
make lockd-run
make etcd-load
make etcd-run
make lockd-load-query
make lockd-run-query-pair
```

Each `make *-load` / `make *-run` appends a summary to `performance.log` with the final YCSB stats.
Disable logging by setting `PERF_LOG=` (empty) or run the binary with `--perf-log=`.
For baseline runs, we also store a snapshot under `docs/performance/` so results are tracked alongside other perf notes.
Each log entry now includes `query_engine` and `query_return` metadata for lockd runs so index-vs-scan comparisons can be parsed deterministically.

Benchmark methodology, caveats, and results live in [BENCHMARKS.md](BENCHMARKS.md).

Override the workload mix or scale as needed:

```bash
make lockd-run WORKLOAD=workloadb RECORDCOUNT=50000 OPERATIONCOUNT=500000 THREADS=16
```

For scan-heavy query comparisons (index engine vs scan engine), use:

```bash
make lockd-load-query
make lockd-run-query-index
make lockd-run-query-scan
make lockd-compare-query
```

> **Note**: The standard `go-ycsb` binary can also exercise the lockd driver; just `go build` it from this module so the `_ "github.com/sa6mwa/lockd/ycsb/db/lockd"` import is included.

## Driver properties

All properties use the `lockd.` prefix. Key settings:

| Property | Default | Description |
| --- | --- | --- |
| `lockd.endpoints` | `https://127.0.0.1:9341` | Comma-separated list of lockd endpoints. |
| `lockd.namespace` | `default` | Namespace to target. |
| `lockd.bundle` | _required_ | Client bundle (PEM) when mTLS is enabled. |
| `lockd.disable_mtls` | `false` | Set to `true` for HTTP deployments (no client cert). |
| `lockd.public_read` | `true` | Use `/v1/get?public=1` so reads do not acquire leases. |
| `lockd.lease_ttl` | `30s` | TTL for lease-based mutations (insert/update/delete). |
| `lockd.acquire_block` | `0` | Block seconds for acquire when contention occurs. |
| `lockd.owner_prefix` | `ycsb-worker` | Prefix for generated owner IDs. |
| `lockd.http_timeout` | `30s` | Per-request timeout. |
| `lockd.query.engine` | `index` | `auto`, `scan`, or `index` hint for `/v1/query`. |
| `lockd.query.refresh` | _empty_ | Set to `wait_for` to block until the namespace index flushes (and have writes wait for query visibility). |
| `lockd.query.limit` | `100` | Cap on the number of keys fetched per scan call. |
| `lockd.query.return` | `documents` | `documents` streams NDJSON payloads; set to `keys` to fall back to key-only scans. |
| `lockd.attach.enable` | `false` | When true, stage a single attachment on each insert/update. |
| `lockd.attach.bytes` | `1024` | Attachment size in bytes when attachment staging is enabled. |
| `lockd.attach.read` | `false` | When true, read one attachment per record during reads/scans. |
| `lockd.txn.explicit` | `false` | When true, the driver supplies an explicit xid `txn_id` on acquire. |
| `lockd.phase_metrics` | `false` | When true, emit per-phase YCSB metrics for lockd acquire/update/release/remove. |

The driver stores each YCSB record as a JSON document with:

- `_table`, `_key`, `_seq`, `_updated_at` metadata fields.
- A `data` object that contains the original YCSB fields. Use JSON Pointer selectors such as `/data/field0` when crafting queries.

Scans are implemented via `/v1/query` and default to `return=documents`, so the driver streams each record once without issuing follow-up `Get` calls. The driver automatically sets selectors so that:

- Only documents for the requested table are returned.
- The `_seq` field (derived from the numeric suffix of the YCSB key) is used as the range cursor so scans start at the requested key.

### Attachments and explicit transactions

When `lockd.attach.enable=true`, the driver stages a single attachment named `ycsb.bin` on every insert/update. Set `lockd.attach.bytes` to control the payload size. Enable `lockd.attach.read=true` to list attachments and stream one attachment per record during reads/scans (useful for exercising attachment read paths).

When `lockd.txn.explicit=true`, the driver generates and supplies an explicit xid `txn_id` on each acquire. This remains a per-operation transaction (YCSB operations are single-record); it does **not** batch multiple keys into the same transaction.

## Workloads and overlays

`workloads/workload[a-f].properties` are the standard YCSB core mixes (A-F) copied from go-ycsb. To keep lockd vs etcd comparable, run the same workload file and scale via `RECORDCOUNT`/`OPERATIONCOUNT`/`THREADS` in the Makefile or via `-p` overrides.

Driver overlays live under:

- `workloads/lockd.properties` (lockd defaults)
- `workloads/etcd.properties` (etcd defaults)
- `workloads/lockd.attach.properties` (attachment overlay)
- `workloads/lockd.txn.properties` (explicit txn overlay)
- `workloads/lockd.querysync.properties` (sync query visibility; opt-in)

You can supply multiple overlays via `LOCKD_EXTRA_PROPS`, for example:

```bash
make lockd-run LOCKD_EXTRA_PROPS="workloads/lockd.attach.properties workloads/lockd.txn.properties"
```

To force synchronous query visibility (wait for index refresh), add the query-sync overlay:

```bash
make lockd-run LOCKD_EXTRA_PROPS="workloads/lockd.querysync.properties"
```

## Example property file

```properties
recordcount=10000
operationcount=100000
workload=core
readallfields=true
requestdistribution=zipfian
threadcount=16
lockd.endpoints=https://127.0.0.1:9341
lockd.bundle=/etc/lockd/client.pem
lockd.public_read=true
lockd.query.engine=index
```

To compare indexed queries with full scans, flip `lockd.query.engine` between `index` and `scan` (you can also use `auto` to let the server decide). Run load and run phases as usual:

```bash
./lockd-ycsb load lockd -P workloads/workload.lockd.properties
./lockd-ycsb run lockd -P workloads/workload.lockd.properties
```
