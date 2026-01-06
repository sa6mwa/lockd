# 2025-12-25 XA phase 2.5 mem/lq dequeue perf

## Context
- Focus: mem/lq dequeue regression after XA phase 2/2.5 changes (prepare, no-delete on apply, queue message lease fencing, etc.).
- Target baseline: ~5.6k dequeues/s (crypto+mtls ON) for double_server_prefetch4_100p_100c from the pre-XA era.
- Branch: xa
- Commit: c690ce1f8b89787b1b679d8acca70506a8cbfcc9
- Recent perf-related tweaks in this branch: server-side HTTP/2 MaxConcurrentStreams bump; client transport idle limit defaults.

## Environment
- Host: 13th Gen Intel(R) Core(TM) i7-1355U
- OS/arch: linux/amd64
- Go: go1.25.5
- Bench flags: -benchtime=1x -count=1 -benchmem
- Env: LOCKD_TEST_STORAGE_ENCRYPTION=1, MEM_LQ_BENCH_PREFETCH_DOUBLE=8, OTEL_SDK_DISABLED=1
- mTLS toggled per run (LOCKD_TEST_WITH_MTLS=1 or 0)

## Benchmark results (double_server_prefetch4_100p_100c)

mTLS ON (LOCKD_TEST_WITH_MTLS=1):

```
BenchmarkMemQueueThroughput/double_server_prefetch4_100p_100c-12          1  435290635 ns/op   1.000 ack_lease_mismatch_total  2000 consumed_total  4595 dequeue/s  4595 enqueue/s  2000 produced_total  376614984 B/op  2253112 allocs/op
```

mTLS OFF (LOCKD_TEST_WITH_MTLS=0):

```
BenchmarkMemQueueThroughput/double_server_prefetch4_100p_100c-12          1  404932408 ns/op   3.000 ack_lease_mismatch_total  2001 consumed_total  4942 dequeue/s  4939 enqueue/s  2000 produced_total  306857168 B/op  2028487 allocs/op
```

## Profiles
Profiles are stored in benchmark-logs/ (ignored by git):
- benchmark-logs/2025-12-25-mem-double-mtls-on-cpu.pprof
- benchmark-logs/2025-12-25-mem-double-mtls-on-mem.pprof
- benchmark-logs/2025-12-25-mem-double-mtls-off-cpu.pprof
- benchmark-logs/2025-12-25-mem-double-mtls-off-mem.pprof

### CPU (mTLS ON) - pprof top -cum -nodecount=25
```
      flat  flat%   sum%        cum   cum%
         0     0%     0%         1s 27.40%  golang.org/x/net/http2.(*serverConn).runHandler
     0.01s  0.27%  0.27%      0.96s 26.30%  net/http.initALPNRequest.ServeHTTP
         0     0%  0.27%      0.95s 26.03%  net/http.(*ServeMux).ServeHTTP
         0     0%  0.27%      0.95s 26.03%  net/http.serverHandler.ServeHTTP
         0     0%  0.27%      0.94s 25.75%  net/http.HandlerFunc.ServeHTTP
         0     0%  0.27%      0.94s 25.75%  pkt.systems/lockd/internal/httpapi.(*Handler).wrap.func1
         0     0%  0.27%      0.67s 18.36%  runtime.systemstack
         0     0%  0.27%      0.61s 16.71%  pkt.systems/lockd/internal/httpapi.(*Handler).handleQueueSubscribe
     0.01s  0.27%  0.55%      0.61s 16.71%  pkt.systems/lockd/internal/httpapi.(*Handler).handleQueueSubscribeInternal
     0.57s 15.62% 16.16%      0.57s 15.62%  internal/runtime/syscall.Syscall6
         0     0% 16.16%      0.48s 13.15%  internal/poll.ignoringEINTRIO (inline)
         0     0% 16.16%      0.48s 13.15%  syscall.RawSyscall6
         0     0% 16.16%      0.48s 13.15%  syscall.Syscall
     0.03s  0.82% 16.99%      0.47s 12.88%  runtime.mallocgc
         0     0% 16.99%      0.46s 12.60%  pkt.systems/lockd/internal/core.(*Service).Dequeue
         0     0% 16.99%      0.46s 12.60%  pkt.systems/lockd/internal/core.(*Service).consumeQueueBatch
         0     0% 16.99%      0.45s 12.33%  pkt.systems/lockd/internal/core.(*Service).consumeQueue
         0     0% 16.99%      0.40s 10.96%  net/http.(*http2clientStream).doRequest
     0.01s  0.27% 17.26%      0.38s 10.41%  bufio.(*Writer).Flush
         0     0% 17.26%      0.37s 10.14%  net/http.(*http2clientStream).writeRequest
```

### CPU (mTLS OFF) - pprof top -cum -nodecount=25
```
      flat  flat%   sum%        cum   cum%
         0     0%     0%      0.73s 33.33%  net/http.(*conn).serve
         0     0%     0%      0.56s 25.57%  net/http.(*ServeMux).ServeHTTP
         0     0%     0%      0.56s 25.57%  net/http.serverHandler.ServeHTTP
         0     0%     0%      0.55s 25.11%  net/http.HandlerFunc.ServeHTTP
         0     0%     0%      0.55s 25.11%  pkt.systems/lockd/internal/httpapi.(*Handler).wrap.func1
     0.45s 20.55% 20.55%      0.45s 20.55%  internal/runtime/syscall.Syscall6
         0     0% 20.55%      0.45s 20.55%  runtime.systemstack
         0     0% 20.55%      0.42s 19.18%  syscall.RawSyscall6
         0     0% 20.55%      0.41s 18.72%  syscall.Syscall
         0     0% 20.55%      0.40s 18.26%  internal/poll.ignoringEINTRIO (inline)
         0     0% 20.55%      0.38s 17.35%  bufio.(*Writer).Flush
         0     0% 20.55%      0.37s 16.89%  internal/poll.(*FD).Write
         0     0% 20.55%      0.37s 16.89%  net.(*conn).Write
         0     0% 20.55%      0.37s 16.89%  net.(*netFD).Write
     0.01s  0.46% 21.00%      0.36s 16.44%  syscall.Write (inline)
         0     0% 21.46%      0.35s 15.98%  net/http.(*persistConn).writeLoop
         0     0% 21.46%      0.35s 15.98%  syscall.write
     0.01s  0.46% 21.92%      0.34s 15.53%  runtime.mallocgc
         0     0% 21.92%      0.29s 13.24%  net/http.(*Request).write
```

### Alloc (mTLS ON) - pprof top -alloc_space -nodecount=25
```
      flat  flat%   sum%        cum   cum%
   67.05MB 15.97% 15.97%    69.57MB 16.57%  io.copyBuffer
   41.06MB  9.78% 25.75%    41.06MB  9.78%  pkt.systems/lockd/internal/storage/memory.(*Store).ListObjects
   30.56MB  7.28% 33.02%    30.56MB  7.28%  net/http.(*http2clientStream).writeRequestBody
   12.51MB  2.98% 36.00%    12.51MB  2.98%  bytes.growSlice
   11.51MB  2.74% 38.74%    11.51MB  2.74%  io.ReadAll
   11.01MB  2.62% 41.36%    11.01MB  2.62%  crypto/internal/fips140/aes/gcm.New
      10MB  2.38% 43.75%       10MB  2.38%  crypto/internal/fips140/aes.New
    9.50MB  2.26% 46.01%     9.50MB  2.26%  path.Join
    8.53MB  2.03% 48.04%     8.53MB  2.03%  bufio.NewReaderSize (inline)
       8MB  1.91% 49.95%        8MB  1.91%  crypto/internal/fips140/sha256.New (inline)
```

### Alloc (mTLS OFF) - pprof top -alloc_space -nodecount=25
```
      flat  flat%   sum%        cum   cum%
   63.96MB 23.07% 23.07%    65.47MB 23.62%  io.copyBuffer
   13.25MB  4.78% 27.85%    13.25MB  4.78%  pkt.systems/lockd/internal/storage/memory.(*Store).ListObjects
    9.51MB  3.43% 31.28%     9.51MB  3.43%  io.ReadAll
    9.04MB  3.26% 34.54%     9.04MB  3.26%  bufio.NewReaderSize
       9MB  3.25% 37.79%        9MB  3.25%  path.Join
    8.01MB  2.89% 40.68%     8.01MB  2.89%  crypto/internal/fips140/aes/gcm.New (inline)
       7MB  2.53% 43.20%        7MB  2.53%  bytes.growSlice
    6.50MB  2.35% 45.55%     6.50MB  2.35%  net/textproto.readMIMEHeader
    5.51MB  1.99% 47.54%     5.51MB  1.99%  encoding/json.(*Decoder).refill
       5MB  1.80% 49.34%        5MB  1.80%  net/textproto.MIMEHeader.Set
```
