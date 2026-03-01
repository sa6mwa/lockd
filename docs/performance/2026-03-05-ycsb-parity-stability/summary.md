# YCSB parity stability rerun (core)

Date: 2026-03-05

## Goal
Verify that parity remains stable after the failover no-sync fixes and does not regress to the prior noisy behavior.

Parity gates:
- load >= 2066.8 ops/s
- run >= 3495.8 ops/s

## Environment
- Backend: lockd (disk)
- Workload: `workloada`
- Record count: `10000`
- Operation count: `100000`
- Threads: `8`
- Target: `0`
- Runtime profile:
  - `indexer-flush-docs: 2000`
  - `logstore-commit-max-ops: 4096`
  - `qrf-disabled: true`
- Lockd image rebuilt from current branch before measurements.

## Method
For each iteration (n=4):
1. Delete `devenv/volumes/lockd-storage/*`
2. Restart `lockd` container
3. Run `cd ycsb && make lockd-load`
4. Run `cd ycsb && make lockd-run`

## Results
| Iteration | Load total ops/s | Run total ops/s |
| --- | ---: | ---: |
| 1 | 2800.7 | 6888.9 |
| 2 | 2655.7 | 6925.2 |
| 3 | 2514.9 | 6881.9 |
| 4 | 2678.7 | 6855.9 |

All iterations pass both parity gates.

Aggregate:
- Load mean: `2662.5` ops/s; min/max: `2514.9/2800.7`; spread vs mean: `10.73%`
- Run mean: `6888.0` ops/s; min/max: `6855.9/6925.2`; spread vs mean: `1.01%`

## Root-cause conclusion
The prior parity noise was primarily caused by synchronous durability churn in failover mode on index persistence paths:
- segment persistence path not honoring no-sync
- lease-index writes not honoring no-sync

After applying those fixes and rebuilding lockd, the old low-throughput behavior did not reproduce in repeated clean-state runs.

Residual observation:
- Warmup window OPS can still vary, but full-run totals remain stable and far above parity gates.
