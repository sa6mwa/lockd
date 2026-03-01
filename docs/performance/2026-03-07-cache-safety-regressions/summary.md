# Cache Safety Regression Validation

Date: 2026-03-07

## Scope
- Correctness/safety fixes under test:
  - bypass sorted-key cache for relative date selectors (`since=now|today|yesterday`)
  - bound certificate-derived HTTP API caches (identity + namespace claims)
  - evict superseded staged index-doc cache entries on restage
- Comparison targets:
  - lockd-bench parity reference: `docs/performance/2026-03-04-lockd-bench-parity/summary.md`
  - YCSB parity stability reference: `docs/performance/2026-03-05-ycsb-parity-stability/summary.md`

## lockd-bench (embedded, disk, qrf-disabled)

Command shape:
- `go run ./cmd/lockd-bench -backend disk -ha failover -workload query-index|query-scan -ops 2000 -concurrency 8 -warmup 0 -runs 3 -query-return documents -qrf-disabled -log-level error`

Observed medians:
- query-index: `10167.8 ops/s` (`/tmp/lockd-bench-query-index-after-cachefix.log`)
- query-scan: `2732.1 ops/s` (`/tmp/lockd-bench-query-scan-after-cachefix.log`)

Reference comparison:
- query-index vs recent parity band (`10006.7..10214.9`): within band
- query-scan vs recent parity points (`2768.0`, `2822.7`): `-1.30%` and `-3.21%`

Interpretation:
- Relative-date cache bypass does not materially impact standard query-index throughput in parity workload shape.
- Query-scan remains in recent variance range, slightly below prior points but without correctness regressions.

## YCSB (lockd, rebuilt devenv, clean storage)

Workflow:
1. `nerdctl compose -f devenv/docker-compose.yaml down`
2. `nerdctl compose -f devenv/docker-compose.yaml up --build -d`
3. stop lockd, clear `devenv/volumes/lockd-storage/*`, restart lockd
4. `cd ycsb && make lockd-load PERF_LOG=/tmp/ycsb-after-cachefix.log`
5. `cd ycsb && make lockd-run PERF_LOG=/tmp/ycsb-after-cachefix.log`

Observed totals:
- load: `2600.9 ops/s`
- run: `6886.9 ops/s`

Saved reference (2026-03-05):
- mean load/run: `2662.5 / 6888.0`
- min load/run: `2514.9 / 6855.9`
- gates: load `>=2066.8`, run `>=3495.8`

Delta:
- load vs saved mean: `-2.31%`
- run vs saved mean: `-0.02%`
- load vs saved min: `+3.42%`
- run vs saved min: `+0.45%`
- both gates: PASS

## Verification gates
- `go test ./...`
- `go vet ./...`
- `golint ./...`
- `golangci-lint run ./...`
- `./run-integration-suites.sh mem disk minio`
