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
| `lockd.query.refresh` | _empty_ | Set to `wait_for` to block until the namespace index flushes. |
| `lockd.query.limit` | `100` | Cap on the number of keys fetched per scan call. |
| `lockd.query.return` | `documents` | `documents` streams NDJSON payloads; set to `keys` to fall back to key-only scans. |

The driver stores each YCSB record as a JSON document with:

- `_table`, `_key`, `_seq`, `_updated_at` metadata fields.
- A `data` object that contains the original YCSB fields. Use JSON Pointer selectors such as `/data/field0` when crafting queries.

Scans are implemented via `/v1/query` and default to `return=documents`, so the driver streams each record once without issuing follow-up `Get` calls. The driver automatically sets selectors so that:

- Only documents for the requested table are returned.
- The `_seq` field (derived from the numeric suffix of the YCSB key) is used as the range cursor so scans start at the requested key.

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
