# 2026-01-23 LQ benchmark baseline

Date: 2026-01-23

Command:
- `./run-benchmark-suites.sh mem disk minio aws azure`

Environment:
- `LOCKD_TEST_STORAGE_ENCRYPTION=1`
- `LOCKD_TEST_WITH_MTLS=1`
- `BENCHTIME=1x`
- `MEM_LQ_BENCH_PREFETCH_DOUBLE=8`
- `OTEL_SDK_DISABLED=1`

Queue throughput (dequeue/s):
- mem/single_server_prefetch1_100p_100c: 2156
- mem/single_server_prefetch4_100p_100c: 2387
- mem/single_server_subscribe_100p_1c: 1962
- mem/double_server_subscribe_100p_1c: 2211
- mem/double_server_dequeue_100p_1c: 1079
- mem/single_server_dequeue_guard: 969.5
- mem/double_server_prefetch4_100p_100c: 6735
- disk/queue_ack_lease: 854.2
- minio/queue_ack_lease: 327.6
- aws/queue_ack_lease: 8.895
- azure/queue_ack_lease: 30.48

Logs:
- `mem.log`
- `disk.log`
- `minio.log`
- `aws.log`
- `azure.log`
