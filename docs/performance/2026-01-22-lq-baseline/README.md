# LQ baseline (2026-01-22)

Environment:
- Crypto enabled (LOCKD_TEST_STORAGE_ENCRYPTION=1)
- mTLS enabled (LOCKD_TEST_WITH_MTLS=1)
- BENCHTIME=1x
- MEM_LQ_BENCH_PREFETCH_DOUBLE=8

Results (dequeue/s):
- mem: 2653 (prefetch1), 2982 (prefetch4), 5407 (double-server prefetch4)
- disk: 415.8
- minio: 243.3
- nfs: 53.64
- aws: 9.123
- azure: 31.40

Raw logs:
- mem.log
- disk.log
- minio.log
- nfs.log
- aws.log
- azure.log
