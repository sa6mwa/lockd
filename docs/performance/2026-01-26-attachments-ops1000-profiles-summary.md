# Attachments ops=1000 profile summary (disk/minio/aws/azure)

Generated on 2026-01-26. Profiles live in `benchmark-logs/`.

| Backend | Ops | Concurrency | Payload (bytes) | Total |
| --- | ---:| ---:| ---:| --- |
| disk | 1000 | 8 | 256 | total: ops=1000 ops/s=1175.1 avg=6.764505ms p50=6.720459ms p90=8.472932ms p95=9.282153ms p99=11.007983ms p99.9=15.574641ms min=2.906958ms max=15.714825ms errors=0 |
| minio | 1000 | 8 | 256 | total: ops=1000 ops/s=85.0 avg=93.550756ms p50=53.296577ms p90=283.633888ms p95=355.966034ms p99=408.583829ms p99.9=428.505719ms min=26.387878ms max=437.301836ms errors=0 |
| aws | 1000 | 8 | 256 | total: ops=1000 ops/s=7.2 avg=1.102389794s p50=1.08135319s p90=1.195995356s p95=1.22368194s p99=1.309153425s p99.9=1.441817108s min=979.483694ms max=1.4522497s errors=0 |
| azure | 1000 | 8 | 256 | total: ops=1000 ops/s=23.1 avg=344.769192ms p50=323.36991ms p90=368.048831ms p95=383.711903ms p99=759.109309ms p99.9=809.191363ms min=288.356373ms max=809.255528ms errors=0 |

See `docs/performance/2026-01-26-attachments-ops1000-profiles.md` for full CPU/alloc tables.

Profiles:
- `benchmark-logs/2026-01-26-disk-attachments-ops1000.{cpu,mem}.pprof`
- `benchmark-logs/2026-01-26-minio-attachments-ops1000.{cpu,mem}.pprof`
- `benchmark-logs/2026-01-26-aws-attachments-ops1000.{cpu,mem}.pprof`
- `benchmark-logs/2026-01-26-azure-attachments-ops1000.{cpu,mem}.pprof`
