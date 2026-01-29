# lockd-bench attachments/public/xa baseline (post promote)

Generated on 2026-01-26. Logs are in this directory; 
note: "*.log" is ignored by git, so this summary captures key totals.

| Log | Workload | Ops | Concurrency | Payload (bytes) | Total |
| --- | --- | ---:| ---:| ---:| --- |
| aws-attachments-ops1000-runs1.log | attachments | 1000 | 8 | 256 | total: ops=1000 ops/s=7.2 avg=1.108717s p50=1.089711373s p90=1.20107714s p95=1.227454878s p99=1.290872084s p99.9=1.507758539s min=985.511173ms max=2.989894431s errors=0 |
| aws-attachments-ops2000-runs2.log | attachments | 2000 | 8 | 256 | total: ops=2000 ops/s=7.5 avg=1.067356699s p50=1.032088024s p90=1.152262177s p95=1.174116545s p99=1.327199171s p99.9=1.989823881s min=959.888717ms max=2.709278692s errors=0 |
| aws-attachments-public-ops1000-runs1.log | attachments-public | 1000 | 8 | 256 | total: ops=1000 ops/s=4.1 avg=1.9296176s p50=1.905766353s p90=2.049588292s p95=2.1488986s p99=2.466900043s p99.9=2.842563193s min=1.744017279s max=2.907963514s errors=0 |
| aws-attachments-public-ops2000-runs2.log |  |  |  |  |  |
| aws-public-read-ops1000-runs1.log | public-read | 1000 | 8 | 256 | timed out (>15m) |
| aws-public-read-ops100-runs1.log | public-read | 100 | 8 | 256 | total: ops=100 ops/s=63.1 avg=122.026627ms p50=129.194306ms p90=186.218391ms p95=223.781958ms p99=267.414644ms p99.9=284.506669ms min=59.814131ms max=284.506669ms errors=0 |
| aws-public-read-ops500-runs1.log | public-read | 500 | 8 | 256 | total: ops=500 ops/s=67.8 avg=116.657776ms p50=126.167778ms p90=148.740308ms p95=164.824241ms p99=219.317918ms p99.9=2.156084458s min=58.878116ms max=2.156084458s errors=0 |
| aws-xa-commit-ops200-runs1.log | xa-commit | 200 | 8 | 256 | total: ops=200 ops/s=1.2 avg=6.141435588s p50=2.109271969s p90=25.31082763s p95=28.398035272s p99=30.144414926s p99.9=30.996498635s min=1.947823087s max=30.996498635s errors=0 |
| aws-xa-rollback-ops100-runs1.log | xa-rollback | 100 | 8 | 256 | total: ops=100 ops/s=1.0 avg=6.82746798s p50=1.82006448s p90=27.968233422s p95=29.658004367s p99=29.957170169s p99.9=30.020307729s min=1.689877218s max=30.020307729s errors=0 |
| aws-xa-rollback-ops200-runs1.log |  |  |  |  |  |
| azure-attachments-ops1000-runs1.log | attachments | 1000 | 8 | 256 | total: ops=1000 ops/s=23.5 avg=339.321359ms p50=329.196497ms p90=376.97798ms p95=384.306488ms p99=420.124305ms p99.9=536.068599ms min=297.679881ms max=574.825766ms errors=0 |
| azure-attachments-ops2000-runs2.log | attachments | 2000 | 8 | 256 | total: ops=2000 ops/s=24.0 avg=335.317011ms p50=324.823101ms p90=369.917228ms p95=376.373461ms p99=410.976772ms p99.9=459.35078ms min=290.338968ms max=556.922969ms errors=0 |
| azure-attachments-public-ops1000-runs1.log | attachments-public | 1000 | 8 | 256 | total: ops=1000 ops/s=13.3 avg=597.304177ms p50=592.539138ms p90=642.239304ms p95=658.970306ms p99=698.513483ms p99.9=793.348004ms min=532.03726ms max=854.935388ms errors=0 |
| azure-attachments-public-ops2000-runs2.log | attachments-public | 2000 | 8 | 256 | total: ops=2000 ops/s=13.7 avg=593.010366ms p50=582.888286ms p90=622.190024ms p95=635.321353ms p99=928.959122ms p99.9=1.267042733s min=541.112148ms max=1.399867922s errors=0 |
| azure-public-read-ops1000-runs1.log | public-read | 1000 | 8 | 256 | total: ops=1000 ops/s=218.9 avg=36.329251ms p50=42.218498ms p90=45.210722ms p95=46.759439ms p99=72.046821ms p99.9=115.157241ms min=19.725802ms max=140.413532ms errors=0 |
| azure-public-read-ops2000-runs2.log |  |  |  |  |  |
| azure-xa-commit-ops200-runs1.log | xa-commit | 200 | 8 | 256 | total: ops=200 ops/s=1.3 avg=5.859922691s p50=731.994161ms p90=28.675884791s p95=29.294950786s p99=30.086662438s p99.9=30.171099369s min=660.692725ms max=30.171099369s errors=0 |
| azure-xa-rollback-ops200-runs1.log | xa-rollback | 200 | 8 | 256 | total: ops=200 ops/s=1.6 avg=4.887874688s p50=617.047541ms p90=27.511837613s p95=29.123542849s p99=29.5150312s p99.9=30.040163529s min=555.144049ms max=30.040163529s errors=0 |
| disk-attachments-public.log | attachments-public | 10000 | 8 | 256 | total: ops=10000 ops/s=701.1 avg=11.399171ms p50=10.781547ms p90=13.104872ms p95=15.173363ms p99=26.766265ms p99.9=82.549287ms min=4.970003ms max=153.981182ms errors=0 |
| disk-attachments.log | attachments | 10000 | 8 | 256 | total: ops=10000 ops/s=522.7 avg=15.298857ms p50=13.668049ms p90=27.695867ms p95=28.903807ms p99=31.592374ms p99.9=52.735229ms min=2.664905ms max=67.909004ms errors=0 |
| disk-public-read.log | public-read | 10000 | 8 | 256 | total: ops=10000 ops/s=46283.9 avg=171.171µs p50=125.353µs p90=308.238µs p95=410.164µs p99=783.097µs p99.9=1.805075ms min=33.031µs max=2.931133ms errors=0 |
| disk-xa-commit-ops200-runs1.log | xa-commit | 200 | 8 | 256 | total: ops=200 ops/s=1.7 avg=4.189993341s p50=12.186101ms p90=29.673948315s p95=29.943834231s p99=29.968092687s p99.9=29.991654841s min=7.670445ms max=29.991654841s errors=0 |
| disk-xa-rollback-ops200-runs1.log | xa-rollback | 200 | 8 | 256 | total: ops=200 ops/s=1.7 avg=4.794085749s p50=10.596933ms p90=29.918838497s p95=29.969006799s p99=29.989572556s p99.9=29.989965206s min=8.304399ms max=29.989965206s errors=0 |
| mem-attachments-public.log | attachments-public | 10000 | 8 | 256 | total: ops=10000 ops/s=1245.2 avg=6.419098ms p50=2.389838ms p90=4.212216ms p95=5.314736ms p99=222.615318ms p99.9=285.021584ms min=1.115941ms max=389.123459ms errors=0 |
| mem-attachments.log | attachments | 10000 | 8 | 256 | total: ops=10000 ops/s=2250.8 avg=3.550966ms p50=1.242505ms p90=2.591834ms p95=3.472681ms p99=46.610968ms p99.9=312.638907ms min=554.895µs max=318.186546ms errors=0 |
| mem-public-read.log | public-read | 10000 | 8 | 256 | total: ops=10000 ops/s=51062.7 avg=155.059µs p50=120.874µs p90=263.696µs p95=338.88µs p99=760.686µs p99.9=1.844025ms min=33.056µs max=3.713308ms errors=0 |
| mem-xa-commit-ops200-runs1.log | xa-commit | 200 | 8 | 256 | total: ops=200 ops/s=1.7 avg=3.892525487s p50=4.456133ms p90=29.769033373s p95=29.974268497s p99=29.992608033s p99.9=30.002464209s min=862.929µs max=30.002464209s errors=0 |
| mem-xa-commit-ops2000.log | xa-commit | 2000 | 8 | 256 | total: ops=2000 ops/s=5.6 avg=1.34895839s p50=2.5339ms p90=6.444801ms p95=10.735898ms p99=29.964294542s p99.9=29.9921726s min=571.317µs max=29.999228702s errors=0 |
| mem-xa-commit.log | xa-commit | 10000 | 8 | 256 | total: ops=10000 ops/s=11.1 avg=701.974351ms p50=2.655647ms p90=5.589569ms p95=7.534074ms p99=29.885746072s p99.9=29.96014421s min=382.601µs max=29.9977713s errors=0 |
| mem-xa-rollback-ops200-runs1.log | xa-rollback | 200 | 8 | 256 | total: ops=200 ops/s=1.3 avg=5.090697824s p50=3.994104ms p90=29.972205577s p95=29.990911235s p99=29.996230382s p99.9=30.002947077s min=1.140524ms max=30.002947077s errors=0 |
| minio-attachments-ops2000-runs2.log | attachments | 2000 | 8 | 256 | total: ops=2000 ops/s=57.6 avg=146.481703ms p50=84.007122ms p90=296.003412ms p95=328.84176ms p99=396.613979ms p99.9=1.162889274s min=45.939172ms max=1.291804575s errors=0 |
| minio-attachments-public-ops2000-runs2.log | attachments-public | 2000 | 8 | 256 | total: ops=2000 ops/s=33.0 avg=244.004567ms p50=222.619156ms p90=471.78587ms p95=510.62189ms p99=577.724908ms p99.9=753.808594ms min=71.733524ms max=882.869114ms errors=0 |
| minio-attachments-public.log | attachments-public | 10000 | 8 | 256 | total: ops=10000 ops/s=34.9 avg=228.868283ms p50=129.675467ms p90=512.819693ms p95=564.771521ms p99=678.970495ms p99.9=807.123434ms min=55.242383ms max=905.928564ms errors=0 |
| minio-attachments.log | attachments | 10000 | 8 | 256 | total: ops=10000 ops/s=61.3 avg=130.478703ms p50=73.395701ms p90=281.567968ms p95=315.517907ms p99=369.731309ms p99.9=434.726927ms min=31.752943ms max=527.724427ms errors=0 |
| minio-public-read-ops2000-runs2.log | public-read | 2000 | 8 | 256 | total: ops=2000 ops/s=2076.7 avg=4.174939ms p50=2.385715ms p90=7.181118ms p95=24.711702ms p99=34.335734ms p99.9=36.661485ms min=604.434µs max=38.063212ms errors=0 |
| minio-xa-commit-ops200-runs1.log | xa-commit | 200 | 8 | 256 | total: ops=200 ops/s=1.3 avg=5.247638037s p50=82.978625ms p90=29.543046678s p95=29.720422463s p99=29.999556575s p99.9=30.002226103s min=59.647381ms max=30.002226103s errors=0 |
| minio-xa-rollback-ops200-runs1.log | xa-rollback | 200 | 8 | 256 | total: ops=200 ops/s=1.3 avg=5.099190745s p50=80.097349ms p90=29.527540673s p95=29.784764172s p99=29.92941437s p99.9=30.005847384s min=55.467481ms max=30.005847384s errors=0 |

Notes:
- AWS public-read at ops=1000 (runs=1, warmup=0) exceeded 15 minutes, so the run was aborted; ops=100 was used to confirm throughput.
