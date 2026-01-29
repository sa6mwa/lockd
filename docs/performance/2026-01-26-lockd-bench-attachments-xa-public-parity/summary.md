# lockd-bench attachments/public parity (mem/disk/minio)

Generated on 2026-01-26. Logs are in this directory; `*.log` is ignored by git.

| Log | Workload | Ops | Concurrency | Payload (bytes) | Total |
| --- | --- | ---:| ---:| ---:| --- |
| disk-attachments-ops10000-runs3.log | attachments | 10000 | 8 | 256 | total: ops=10000 ops/s=692.9 avg=11.53951ms p50=7.363197ms p90=26.214762ms p95=28.134158ms p99=30.117768ms p99.9=40.634349ms min=2.53865ms max=63.233796ms errors=0 |
| disk-attachments-public-ops10000-runs3.log | attachments-public | 10000 | 8 | 256 | total: ops=10000 ops/s=719.7 avg=11.106992ms p50=10.716387ms p90=12.809177ms p95=14.025802ms p99=24.750191ms p99.9=38.212263ms min=5.124688ms max=42.542424ms errors=0 |
| disk-public-read-ops10000-runs3.log | public-read | 10000 | 8 | 256 | total: ops=10000 ops/s=47182.5 avg=167.913µs p50=126.85µs p90=278.79µs p95=351.415µs p99=723.928µs p99.9=1.378976ms min=33.121µs max=2.760531ms errors=0 |
| mem-attachments-ops10000-runs3.log | attachments | 10000 | 8 | 256 | total: ops=10000 ops/s=2939.8 avg=2.718306ms p50=1.072917ms p90=2.476279ms p95=3.373808ms p99=37.799787ms p99.9=180.025329ms min=467.053µs max=243.255914ms errors=0 |
| mem-attachments-public-ops10000-runs3.log | attachments-public | 10000 | 8 | 256 | total: ops=10000 ops/s=1688.6 avg=4.733314ms p50=2.00219ms p90=4.631254ms p95=5.887848ms p99=94.760707ms p99.9=205.382074ms min=887.069µs max=221.242781ms errors=0 |
| mem-public-read-ops10000-runs3.log | public-read | 10000 | 8 | 256 | total: ops=10000 ops/s=52308.2 avg=151.107µs p50=118.951µs p90=264.187µs p95=324.304µs p99=619.188µs p99.9=1.123827ms min=31.58µs max=3.3168ms errors=0 |
| minio-attachments-ops2000-runs2.log | attachments | 2000 | 8 | 256 | total: ops=2000 ops/s=46.3 avg=173.965346ms p50=137.235064ms p90=358.357886ms p95=405.019893ms p99=462.124106ms p99.9=526.58118ms min=42.196051ms max=580.002838ms errors=0 |
| minio-attachments-public-ops2000-runs2.log | attachments-public | 2000 | 8 | 256 | total: ops=2000 ops/s=38.9 avg=226.501015ms p50=159.705027ms p90=477.876574ms p95=534.738752ms p99=651.834325ms p99.9=746.908768ms min=62.740928ms max=752.245961ms errors=0 |
| minio-public-read-ops2000-runs2.log | public-read | 2000 | 8 | 256 | total: ops=2000 ops/s=2140.7 avg=3.927976ms p50=2.225153ms p90=6.640138ms p95=19.533136ms p99=30.619151ms p99.9=33.219244ms min=593.4µs max=33.93567ms errors=0 |

Notes:
- MinIO 10k-op logs are present in this directory but intentionally excluded from parity results due to earlier long runtimes/timeouts.
