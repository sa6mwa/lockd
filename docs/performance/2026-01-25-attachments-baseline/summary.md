# Attachments/Public-Read Baseline (2026-01-25)

Generated from lockd-bench logs in `docs/performance/2026-01-25-attachments-baseline/logs`.

| backend | workload | size_bytes | ops | conc | ops/s | p50 | log |
|---|---|---:|---:|---:|---:|---:|---|
| aws | attachments | 4096 | 200 | 4 | 3.8 | 1.031497861s | `aws-attachments-4096.log` |
| aws | attachments | 65536 | 200 | 4 | 3.8 | 1.040102026s | `aws-attachments-65536.log` |
| aws | attachments | 1048576 | 200 | 4 | 3.0 | 1.25099999s | `aws-attachments-1048576.log` |
| aws | attachments | 8388608 | 50 | 2 | 0.6 | 3.18401834s | `aws-attachments-8388608.log` |
| aws | attachments-public | 4096 | 200 | 4 | 2.2 | 1.747337115s | `aws-attachments-public-4096.log` |
| aws | attachments-public | 65536 | 200 | 4 | 2.1 | 1.862632569s | `aws-attachments-public-65536.log` |
| aws | attachments-public | 1048576 | 200 | 4 | 1.7 | 2.274030057s | `aws-attachments-public-1048576.log` |
| aws | attachments-public | 8388608 | 20 | 2 | 0.3 | 5.857222681s | `aws-attachments-public-8388608.log` |
| aws | lock | 4096 | 200 | 4 | 4.1 | 950.977465ms | `aws-lock-4096.log` |
| aws | lock | 65536 | 200 | 4 | 3.3 | 1.200234945s | `aws-lock-65536.log` |
| aws | lock | 1048576 | 200 | 4 | 1.9 | 1.881102964s | `aws-lock-1048576.log` |
| aws | lock | 8388608 | 50 | 2 | 0.4 | 4.681304615s | `aws-lock-8388608.log` |
| aws | public-read | 4096 | 200 | 4 | 25.9 | 180.329935ms | `aws-public-read-4096.log` |
| aws | public-read | 65536 | 50 | 2 | 12.9 | 179.4784ms | `aws-public-read-65536.log` |
| aws | public-read | 1048576 | 50 | 2 | 7.9 | 325.471348ms | `aws-public-read-1048576.log` |
| aws | public-read | 8388608 | 20 | 2 | 1.0 | 1.708291765s | `aws-public-read-8388608.log` |
| azure | attachments | 4096 | 200 | 4 | 12.1 | 318.498845ms | `azure-attachments-4096.log` |
| azure | attachments | 65536 | 200 | 4 | 12.1 | 319.016753ms | `azure-attachments-65536.log` |
| azure | attachments | 1048576 | 50 | 2 | 3.6 | 541.949481ms | `azure-attachments-1048576.log` |
| azure | attachments | 8388608 | 20 | 2 | 0.9 | 2.074890449s | `azure-attachments-8388608.log` |
| azure | attachments-public | 4096 | 200 | 4 | 6.9 | 568.405665ms | `azure-attachments-public-4096.log` |
| azure | attachments-public | 65536 | 200 | 4 | 6.7 | 590.070402ms | `azure-attachments-public-65536.log` |
| azure | attachments-public | 1048576 | 50 | 2 | 1.8 | 1.086871736s | `azure-attachments-public-1048576.log` |
| azure | attachments-public | 8388608 | 20 | 2 | 0.5 | 3.958849065s | `azure-attachments-public-8388608.log` |
| azure | lock | 4096 | 200 | 4 | 8.3 | 332.216444ms | `azure-lock-4096.log` |
| azure | lock | 4096 | 200 | 4 | 8.3 | 332.216444ms | `azure-summary.log` |
| azure | lock | 65536 | 200 | 4 | 9.3 | 416.619401ms | `azure-lock-65536.log` |
| azure | lock | 1048576 | 50 | 2 | 1.9 | 1.004673529s | `azure-lock-1048576.log` |
| azure | lock | 8388608 | 20 | 2 | 0.5 | 3.856785881s | `azure-lock-8388608.log` |
| azure | public-read | 4096 | 200 | 4 | 108.0 | 42.171592ms | `azure-public-read-4096.log` |
| azure | public-read | 65536 | 200 | 4 | 91.0 | 43.902559ms | `azure-public-read-65536.log` |
| azure | public-read | 1048576 | 30 | 2 | 11.6 | 179.288081ms | `azure-public-read-1048576.log` |
| azure | public-read | 8388608 | 10 | 1 | 0.9 | 1.3899965s | `azure-public-read-8388608.log` |
| disk | attachments | 4096 | 1000 | 8 | 1101.4 | 6.876617ms | `disk-attachments-4096.log` |
| disk | attachments | 65536 | 1000 | 8 | 1068.8 | 7.44622ms | `disk-attachments-65536.log` |
| disk | attachments | 1048576 | 1000 | 8 | 214.7 | 32.795214ms | `disk-attachments-1048576.log` |
| disk | attachments | 8388608 | 500 | 6 | 32.7 | 183.751023ms | `disk-attachments-8388608.log` |
| disk | attachments-public | 4096 | 1000 | 8 | 295.9 | 14.045455ms | `disk-attachments-public-4096.log` |
| disk | attachments-public | 65536 | 1000 | 8 | 696.8 | 11.017795ms | `disk-attachments-public-65536.log` |
| disk | attachments-public | 1048576 | 1000 | 8 | 77.4 | 87.238838ms | `disk-attachments-public-1048576.log` |
| disk | attachments-public | 8388608 | 500 | 6 | 14.9 | 362.299383ms | `disk-attachments-public-8388608.log` |
| disk | lock | 4096 | 1000 | 8 | 1059.0 | 7.396609ms | `disk-lock-4096.log` |
| disk | lock | 4096 | 1000 | 8 | 1059.0 | 7.396609ms | `disk-summary.log` |
| disk | lock | 65536 | 1000 | 8 | 296.5 | 27.324097ms | `disk-lock-65536.log` |
| disk | lock | 1048576 | 1000 | 8 | 111.5 | 68.204108ms | `disk-lock-1048576.log` |
| disk | lock | 8388608 | 500 | 6 | 19.4 | 276.798875ms | `disk-lock-8388608.log` |
| disk | public-read | 4096 | 1000 | 8 | 18632.9 |  | `disk-public-read-4096.log` |
| disk | public-read | 65536 | 1000 | 8 | 9830.3 |  | `disk-public-read-65536.log` |
| disk | public-read | 1048576 | 1000 | 8 | 716.2 | 7.501861ms | `disk-public-read-1048576.log` |
| disk | public-read | 8388608 | 500 | 6 | 109.5 | 55.082357ms | `disk-public-read-8388608.log` |
| mem | attachments | 4096 | 1000 | 8 | 1230.6 | 6.168183ms | `mem-attachments-4096.log` |
| mem | attachments | 65536 | 1000 | 8 | 1065.8 | 7.191729ms | `mem-attachments-65536.log` |
| mem | attachments | 1048576 | 1000 | 8 | 271.4 | 21.595312ms | `mem-attachments-1048576.log` |
| mem | attachments | 8388608 | 500 | 6 | 69.1 | 82.037068ms | `mem-attachments-8388608.log` |
| mem | attachments-public | 4096 | 1000 | 8 | 926.1 | 8.808769ms | `mem-attachments-public-4096.log` |
| mem | attachments-public | 65536 | 1000 | 8 | 761.1 | 10.30259ms | `mem-attachments-public-65536.log` |
| mem | attachments-public | 1048576 | 1000 | 8 | 172.6 | 34.537275ms | `mem-attachments-public-1048576.log` |
| mem | attachments-public | 8388608 | 500 | 6 | 44.9 | 134.372825ms | `mem-attachments-public-8388608.log` |
| mem | lock | 4096 | 1000 | 8 | 3140.1 | 1.823182ms | `mem-lock-4096.log` |
| mem | lock | 4096 | 1000 | 8 | 3140.1 | 1.823182ms | `mem-summary.log` |
| mem | lock | 65536 | 1000 | 8 | 1659.5 | 4.676094ms | `mem-lock-65536.log` |
| mem | lock | 1048576 | 1000 | 8 | 195.4 | 39.609087ms | `mem-lock-1048576.log` |
| mem | lock | 8388608 | 500 | 6 | 20.3 | 273.256904ms | `mem-lock-8388608.log` |
| mem | public-read | 4096 | 1000 | 8 | 45236.5 |  | `mem-public-read-4096.log` |
| mem | public-read | 65536 | 1000 | 8 | 23228.3 |  | `mem-public-read-65536.log` |
| mem | public-read | 1048576 | 1000 | 8 | 686.5 | 7.440251ms | `mem-public-read-1048576.log` |
| mem | public-read | 8388608 | 500 | 6 | 103.4 | 58.052982ms | `mem-public-read-8388608.log` |
| minio | attachments | 4096 | 1000 | 8 | 62.0 | 64.300638ms | `minio-attachments-4096.log` |
| minio | attachments | 65536 | 1000 | 8 | 98.1 | 50.542411ms | `minio-attachments-65536.log` |
| minio | attachments | 1048576 | 1000 | 8 | 35.3 | 209.278219ms | `minio-attachments-1048576.log` |
| minio | attachments | 8388608 | 500 | 6 | 11.4 | 513.524721ms | `minio-attachments-8388608.log` |
| minio | attachments-public | 4096 | 1000 | 8 | 28.9 | 227.318332ms | `minio-attachments-public-4096.log` |
| minio | attachments-public | 65536 | 1000 | 8 | 31.4 | 210.041544ms | `minio-attachments-public-65536.log` |
| minio | attachments-public | 1048576 | 1000 | 8 | 20.7 | 355.324568ms | `minio-attachments-public-1048576.log` |
| minio | attachments-public | 8388608 | 200 | 4 | 5.3 | 743.668812ms | `minio-attachments-public-8388608.log` |
| minio | lock | 4096 | 1000 | 8 | 42.1 | 141.925294ms | `minio-lock-4096.log` |
| minio | lock | 4096 | 1000 | 8 | 42.1 | 141.925294ms | `minio-summary.log` |
| minio | lock | 65536 | 1000 | 8 | 45.0 | 147.996421ms | `minio-lock-65536.log` |
| minio | lock | 1048576 | 1000 | 8 | 32.2 | 242.82055ms | `minio-lock-1048576.log` |
| minio | lock | 8388608 | 500 | 6 | 9.4 | 643.02217ms | `minio-lock-8388608.log` |
| minio | public-read | 4096 | 1000 | 8 | 1969.5 | 2.447387ms | `minio-public-read-4096.log` |
| minio | public-read | 65536 | 1000 | 8 | 1877.0 | 2.674734ms | `minio-public-read-65536.log` |
| minio | public-read | 1048576 | 1000 | 8 | 353.1 | 13.097024ms | `minio-public-read-1048576.log` |
| minio | public-read | 8388608 | 200 | 4 | 70.2 | 58.255239ms | `minio-public-read-8388608.log` |
