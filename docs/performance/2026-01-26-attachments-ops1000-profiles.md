# Attachments ops=1000 profile summary (disk/minio/aws/azure)

Generated on 2026-01-26. Profiles live in `benchmark-logs/`:
- `benchmark-logs/2026-01-26-disk-attachments-ops1000.{cpu,mem}.pprof`
- `benchmark-logs/2026-01-26-minio-attachments-ops1000.{cpu,mem}.pprof`
- `benchmark-logs/2026-01-26-aws-attachments-ops1000.{cpu,mem}.pprof`
- `benchmark-logs/2026-01-26-azure-attachments-ops1000.{cpu,mem}.pprof`

## CPU top (flat)


### Disk
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: cpu
Time: 2026-01-26 16:05:50 CET
Duration: 3.58s, Total samples = 4.43s (123.73%)
Showing nodes accounting for 2.20s, 49.66% of 4.43s total
Dropped 498 nodes (cum <= 0.02s)
Showing top 25 nodes out of 375
      flat  flat%   sum%        cum   cum%
     0.85s 19.19% 19.19%      0.85s 19.19%  internal/runtime/syscall.Syscall6
     0.15s  3.39% 22.57%      0.15s  3.39%  runtime.usleep
     0.11s  2.48% 25.06%      0.11s  2.48%  runtime.nextFreeFast (inline)
     0.11s  2.48% 27.54%      0.11s  2.48%  runtime.procyield
     0.10s  2.26% 29.80%      0.28s  6.32%  runtime.scanobject
     0.08s  1.81% 31.60%      0.08s  1.81%  runtime.(*gcBits).bitp (inline)
     0.08s  1.81% 33.41%      0.08s  1.81%  runtime.(*mspan).writeHeapBitsSmall
     0.07s  1.58% 34.99%      0.07s  1.58%  runtime.(*mspan).base (inline)
     0.06s  1.35% 36.34%      0.06s  1.35%  runtime.futex
     0.06s  1.35% 37.70%      0.06s  1.35%  runtime.memmove
     0.05s  1.13% 38.83%      0.05s  1.13%  aeshashbody
     0.05s  1.13% 39.95%      0.06s  1.35%  runtime.findObject
     0.05s  1.13% 41.08%      0.05s  1.13%  runtime.memclrNoHeapPointers
     0.04s   0.9% 41.99%      0.11s  2.48%  runtime.lock2
     0.04s   0.9% 42.89%      0.51s 11.51%  runtime.mallocgc
     0.04s   0.9% 43.79%      0.07s  1.58%  runtime.selectgo
     0.03s  0.68% 44.47%      0.04s   0.9%  internal/runtime/maps.(*Map).getWithoutKeySmallFastStr
     0.03s  0.68% 45.15%      0.04s   0.9%  internal/sync.(*Mutex).Unlock
     0.03s  0.68% 45.82%      0.08s  1.81%  net/http.Header.writeSubset
     0.03s  0.68% 46.50%      0.04s   0.9%  net/url.unescape
     0.03s  0.68% 47.18%      0.03s  0.68%  runtime.(*unwinder).resolveInternal
     0.03s  0.68% 47.86%      0.32s  7.22%  runtime.gcDrain
     0.03s  0.68% 48.53%      0.03s  0.68%  runtime.unlock2
     0.03s  0.68% 49.21%      0.03s  0.68%  sync.(*poolDequeue).pushHead
     0.02s  0.45% 49.66%      0.10s  2.26%  encoding/json.(*decodeState).object
```

### MinIO
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: cpu
Time: 2026-01-26 16:05:59 CET
Duration: 13.45s, Total samples = 12.65s (94.03%)
Showing nodes accounting for 5.93s, 46.88% of 12.65s total
Dropped 872 nodes (cum <= 0.06s)
Showing top 25 nodes out of 341
      flat  flat%   sum%        cum   cum%
     1.74s 13.75% 13.75%      1.74s 13.75%  internal/runtime/syscall.Syscall6
     0.54s  4.27% 18.02%      0.54s  4.27%  runtime.futex
     0.47s  3.72% 21.74%      1.48s 11.70%  runtime.scanobject
     0.37s  2.92% 24.66%      0.37s  2.92%  runtime.memclrNoHeapPointers
     0.28s  2.21% 26.88%      0.35s  2.77%  runtime.findObject
     0.25s  1.98% 28.85%      0.25s  1.98%  runtime.(*mspan).base (inline)
     0.19s  1.50% 30.36%      0.19s  1.50%  runtime.memmove
     0.17s  1.34% 31.70%      0.17s  1.34%  runtime.(*gcBits).bitp (inline)
     0.17s  1.34% 33.04%      0.26s  2.06%  runtime.scanblock
     0.17s  1.34% 34.39%      0.17s  1.34%  runtime.usleep
     0.16s  1.26% 35.65%      0.26s  2.06%  runtime.pcvalue
     0.13s  1.03% 36.68%      0.15s  1.19%  runtime.typePointers.next
     0.12s  0.95% 37.63%      0.12s  0.95%  regexp/syntax.(*Inst).MatchRunePos
     0.12s  0.95% 38.58%      0.12s  0.95%  runtime.(*mspan).writeHeapBitsSmall
     0.12s  0.95% 39.53%      2.22s 17.55%  runtime.gcDrain
     0.11s  0.87% 40.40%      0.31s  2.45%  regexp.(*Regexp).doOnePass
     0.11s  0.87% 41.26%      0.27s  2.13%  runtime.adjustframe
     0.11s  0.87% 42.13%      0.11s  0.87%  runtime.nextFreeFast
     0.09s  0.71% 42.85%      1.41s 11.15%  runtime.findRunnable
     0.09s  0.71% 43.56%      0.19s  1.50%  runtime.selectgo
     0.09s  0.71% 44.27%      0.39s  3.08%  runtime.stealWork
     0.09s  0.71% 44.98%      0.09s  0.71%  time.nextStdChunk
     0.08s  0.63% 45.61%      0.15s  1.19%  net/textproto.canonicalMIMEHeaderKey
     0.08s  0.63% 46.25%      0.08s  0.63%  net/url.unescape
     0.08s  0.63% 46.88%      0.08s  0.63%  runtime.(*mspan).divideByElemSize (inline)
```

### AWS
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: cpu
Time: 2026-01-26 16:06:18 CET
Duration: 220.44s, Total samples = 24.45s (11.09%)
Showing nodes accounting for 9.95s, 40.70% of 24.45s total
Dropped 1324 nodes (cum <= 0.12s)
Showing top 25 nodes out of 472
      flat  flat%   sum%        cum   cum%
     3.38s 13.82% 13.82%      3.38s 13.82%  internal/runtime/syscall.Syscall6
     1.02s  4.17% 18.00%      1.02s  4.17%  runtime.futex
     0.47s  1.92% 19.92%      1.86s  7.61%  runtime.scanobject
     0.41s  1.68% 21.60%      0.41s  1.68%  runtime.memmove
     0.40s  1.64% 23.23%      0.40s  1.64%  runtime.(*gcBits).bitp (inline)
     0.37s  1.51% 24.74%      0.45s  1.84%  runtime.step
     0.33s  1.35% 26.09%      0.33s  1.35%  runtime.memclrNoHeapPointers
     0.32s  1.31% 27.40%      0.32s  1.31%  runtime.nextFreeFast (inline)
     0.31s  1.27% 28.67%      0.31s  1.27%  runtime.(*mspan).base (inline)
     0.28s  1.15% 29.82%      1.47s  6.01%  runtime.newobject
     0.25s  1.02% 30.84%      0.35s  1.43%  net/url.unescape
     0.21s  0.86% 31.70%      0.24s  0.98%  runtime.(*mspan).writeHeapBitsSmall
     0.21s  0.86% 32.56%      0.26s  1.06%  runtime.findObject
     0.21s  0.86% 33.42%      0.22s   0.9%  runtime.pageIndexOf (inline)
     0.19s  0.78% 34.19%      0.21s  0.86%  runtime.(*spanSet).push
     0.19s  0.78% 34.97%      0.71s  2.90%  runtime.pcvalue
     0.19s  0.78% 35.75%      0.43s  1.76%  runtime.scanblock
     0.18s  0.74% 36.48%      0.18s  0.74%  net/url.shouldEscape
     0.16s  0.65% 37.14%      0.16s  0.65%  crypto/internal/fips140/bigmod.addMulVVW2048
     0.16s  0.65% 37.79%      0.51s  2.09%  runtime.(*unwinder).resolveInternal
     0.16s  0.65% 38.45%      3.12s 12.76%  runtime.gcDrain
     0.15s  0.61% 39.06%      2.49s 10.18%  runtime.mallocgc
     0.14s  0.57% 39.63%      1.22s  4.99%  runtime.mallocgcSmallScanNoHeader
     0.13s  0.53% 40.16%      0.14s  0.57%  internal/runtime/maps.(*Iter).Next
     0.13s  0.53% 40.70%      0.13s  0.53%  runtime.(*lfstack).pop (inline)
```

### Azure
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: cpu
Time: 2026-01-26 16:10:06 CET
Duration: 74.78s, Total samples = 13.82s (18.48%)
Showing nodes accounting for 6.37s, 46.09% of 13.82s total
Dropped 918 nodes (cum <= 0.07s)
Showing top 25 nodes out of 417
      flat  flat%   sum%        cum   cum%
     2.53s 18.31% 18.31%      2.53s 18.31%  internal/runtime/syscall.Syscall6
     0.77s  5.57% 23.88%      0.77s  5.57%  runtime.futex
     0.32s  2.32% 26.19%      0.32s  2.32%  runtime.memmove
     0.25s  1.81% 28.00%      0.25s  1.81%  runtime.(*mspan).base (inline)
     0.24s  1.74% 29.74%      0.24s  1.74%  runtime.nextFreeFast (inline)
     0.18s  1.30% 31.04%      0.95s  6.87%  runtime.scanobject
     0.15s  1.09% 32.13%      0.15s  1.09%  aeshashbody
     0.15s  1.09% 33.21%      0.22s  1.59%  net/url.unescape
     0.14s  1.01% 34.23%      0.14s  1.01%  runtime.(*gcBits).bitp (inline)
     0.14s  1.01% 35.24%      0.19s  1.37%  runtime.findObject
     0.14s  1.01% 36.25%      0.39s  2.82%  runtime.stealWork
     0.12s  0.87% 37.12%      0.12s  0.87%  runtime.usleep
     0.11s   0.8% 37.92%      0.16s  1.16%  net/url.escape
     0.11s   0.8% 38.71%      0.60s  4.34%  runtime.newobject
     0.10s  0.72% 39.44%      0.12s  0.87%  runtime.findfunc
     0.10s  0.72% 40.16%      0.10s  0.72%  runtime.memclrNoHeapPointers
     0.10s  0.72% 40.88%      0.20s  1.45%  runtime.scanblock
     0.10s  0.72% 41.61%      0.12s  0.87%  runtime.step
     0.10s  0.72% 42.33%      0.10s  0.72%  runtime.typePointers.next
     0.09s  0.65% 42.98%      0.11s   0.8%  internal/runtime/maps.(*Iter).Next
     0.09s  0.65% 43.63%      0.09s  0.65%  runtime.(*spanSet).push
     0.09s  0.65% 44.28%      1.18s  8.54%  runtime.mallocgc
     0.09s  0.65% 44.93%      0.26s  1.88%  runtime.selectgo
     0.08s  0.58% 45.51%      0.08s  0.58%  indexbytebody
     0.08s  0.58% 46.09%      0.09s  0.65%  runtime.(*mspan).writeHeapBitsSmall
```

## CPU top (cumulative)


### Disk
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: cpu
Time: 2026-01-26 16:05:50 CET
Duration: 3.58s, Total samples = 4.43s (123.73%)
Showing nodes accounting for 1.01s, 22.80% of 4.43s total
Dropped 498 nodes (cum <= 0.02s)
Showing top 25 nodes out of 375
      flat  flat%   sum%        cum   cum%
         0     0%     0%      1.65s 37.25%  net/http.(*conn).serve
         0     0%     0%      1.25s 28.22%  net/http.(*ServeMux).ServeHTTP
         0     0%     0%      1.25s 28.22%  net/http.serverHandler.ServeHTTP
         0     0%     0%      1.23s 27.77%  net/http.HandlerFunc.ServeHTTP
     0.01s  0.23%  0.23%      1.23s 27.77%  pkt.systems/lockd/internal/httpapi.(*Handler).wrap.func1
     0.85s 19.19% 19.41%      0.85s 19.19%  internal/runtime/syscall.Syscall6
     0.01s  0.23% 19.64%      0.71s 16.03%  syscall.RawSyscall6
     0.02s  0.45% 20.09%      0.65s 14.67%  runtime.mcall
         0     0% 20.09%      0.64s 14.45%  runtime.systemstack
         0     0% 20.09%      0.59s 13.32%  runtime.schedule
         0     0% 20.09%      0.55s 12.42%  pkt.systems/lockd/internal/storage/retry.(*backend).withRetry
         0     0% 20.09%      0.54s 12.19%  runtime.park_m
     0.02s  0.45% 20.54%      0.53s 11.96%  runtime.findRunnable
     0.04s   0.9% 21.44%      0.51s 11.51%  runtime.mallocgc
         0     0% 21.44%      0.42s  9.48%  main.runAttachmentWorkload.func1
         0     0% 21.44%      0.42s  9.48%  main.runWorkloadOps.func1
         0     0% 21.44%      0.40s  9.03%  syscall.Syscall
         0     0% 21.44%      0.35s  7.90%  runtime.gcBgMarkWorker
         0     0% 21.44%      0.34s  7.67%  pkt.systems/lockd/internal/httpapi.(*Handler).handleAttachmentUpload
         0     0% 21.44%      0.34s  7.67%  pkt.systems/lockd/internal/httpapi.(*Handler).handleAttachments
         0     0% 21.44%      0.32s  7.22%  runtime.gcBgMarkWorker.func2
     0.03s  0.68% 22.12%      0.32s  7.22%  runtime.gcDrain
     0.01s  0.23% 22.35%      0.31s  7.00%  pkt.systems/lockd/internal/httpapi.(*Handler).handleAttachment
     0.02s  0.45% 22.80%      0.30s  6.77%  runtime.newobject
         0     0% 22.80%      0.29s  6.55%  syscall.Syscall6
```

### MinIO
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: cpu
Time: 2026-01-26 16:05:59 CET
Duration: 13.45s, Total samples = 12.65s (94.03%)
Showing nodes accounting for 2.59s, 20.47% of 12.65s total
Dropped 872 nodes (cum <= 0.06s)
Showing top 25 nodes out of 341
      flat  flat%   sum%        cum   cum%
         0     0%     0%      3.61s 28.54%  net/http.(*conn).serve
         0     0%     0%      3.26s 25.77%  net/http.(*ServeMux).ServeHTTP
         0     0%     0%      3.26s 25.77%  net/http.serverHandler.ServeHTTP
         0     0%     0%      3.24s 25.61%  net/http.HandlerFunc.ServeHTTP
     0.01s 0.079% 0.079%      3.24s 25.61%  pkt.systems/lockd/internal/httpapi.(*Handler).wrap.func1
     0.01s 0.079%  0.16%         3s 23.72%  runtime.systemstack
         0     0%  0.16%      2.68s 21.19%  github.com/minio/minio-go/v7.(*Client).executeMethod
     0.02s  0.16%  0.32%      2.65s 20.95%  github.com/minio/minio-go/v7.(*Client).executeMethod-range1
         0     0%  0.32%      2.65s 20.95%  github.com/minio/minio-go/v7.(*Client).executeMethod.(*Client).newRetryTimer.func3 (inline)
     0.01s 0.079%   0.4%      2.58s 20.40%  pkt.systems/lockd/internal/storage/retry.(*backend).withRetry
         0     0%   0.4%      2.35s 18.58%  runtime.gcBgMarkWorker
         0     0%   0.4%      2.22s 17.55%  runtime.gcBgMarkWorker.func2
     0.12s  0.95%  1.34%      2.22s 17.55%  runtime.gcDrain
     0.01s 0.079%  1.42%      2.07s 16.36%  github.com/minio/minio-go/v7.(*Client).newRequest
         0     0%  1.42%      1.77s 13.99%  net/http.(*persistConn).writeLoop
     1.74s 13.75% 15.18%      1.74s 13.75%  internal/runtime/syscall.Syscall6
         0     0% 15.18%      1.60s 12.65%  runtime.mcall
     0.02s  0.16% 15.34%      1.58s 12.49%  runtime.schedule
         0     0% 15.34%      1.56s 12.33%  github.com/minio/minio-go/v7.(*Client).PutObject
         0     0% 15.34%      1.55s 12.25%  github.com/minio/minio-go/v7.(*Client).putObject
         0     0% 15.34%      1.51s 11.94%  github.com/minio/minio-go/v7.(*Client).putObjectDo
     0.47s  3.72% 19.05%      1.48s 11.70%  runtime.scanobject
     0.06s  0.47% 19.53%      1.44s 11.38%  runtime.mallocgc
     0.09s  0.71% 20.24%      1.41s 11.15%  runtime.findRunnable
     0.03s  0.24% 20.47%      1.38s 10.91%  runtime.park_m
```

### AWS
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: cpu
Time: 2026-01-26 16:06:18 CET
Duration: 220.44s, Total samples = 24.45s (11.09%)
Showing nodes accounting for 0.17s, 0.7% of 24.45s total
Dropped 1324 nodes (cum <= 0.12s)
Showing top 25 nodes out of 472
      flat  flat%   sum%        cum   cum%
         0     0%     0%      8.18s 33.46%  github.com/aws/smithy-go/middleware.decoratedHandler.Handle
     0.02s 0.082% 0.082%      7.30s 29.86%  github.com/aws/smithy-go/middleware.decoratedFinalizeHandler.HandleFinalize (partial-inline)
     0.01s 0.041%  0.12%      7.10s 29.04%  github.com/aws/aws-sdk-go-v2/aws/retry.(*Attempt).HandleFinalize
     0.01s 0.041%  0.16%         7s 28.63%  github.com/aws/aws-sdk-go-v2/aws/retry.MetricsHeader.HandleFinalize
     0.01s 0.041%   0.2%      6.95s 28.43%  github.com/aws/aws-sdk-go-v2/service/s3.(*resolveAuthSchemeMiddleware).HandleFinalize
     0.01s 0.041%  0.25%      6.94s 28.38%  github.com/aws/aws-sdk-go-v2/aws/retry.(*Attempt).handleAttempt
     0.02s 0.082%  0.33%      6.93s 28.34%  github.com/aws/smithy-go/transport/http.(*InterceptBeforeRetryLoop).HandleFinalize
     0.01s 0.041%  0.37%      6.91s 28.26%  github.com/aws/smithy-go/transport/http.(*InterceptAttempt).HandleFinalize
     0.01s 0.041%  0.41%      6.80s 27.81%  github.com/aws/aws-sdk-go-v2/service/s3.(*spanRetryLoop).HandleFinalize
     0.01s 0.041%  0.45%      6.62s 27.08%  github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding.(*DisableGzip).HandleFinalize
         0     0%  0.45%      6.47s 26.46%  github.com/aws/smithy-go/middleware.(*FinalizeStep).HandleMiddleware
         0     0%  0.45%      6.06s 24.79%  github.com/aws/smithy-go/middleware.buildWrapHandler.HandleBuild
     0.01s 0.041%  0.49%      6.03s 24.66%  github.com/aws/smithy-go/middleware.decoratedBuildHandler.HandleBuild (partial-inline)
         0     0%  0.49%      5.92s 24.21%  github.com/aws/aws-sdk-go-v2/aws/middleware.(*RecursionDetection).HandleBuild
     0.01s 0.041%  0.53%      5.76s 23.56%  github.com/aws/aws-sdk-go-v2/internal/middleware.AddTimeOffsetMiddleware.HandleBuild
         0     0%  0.53%      5.69s 23.27%  github.com/aws/aws-sdk-go-v2/aws/middleware.(*RequestUserAgent).HandleBuild
         0     0%  0.53%      5.66s 23.15%  github.com/aws/aws-sdk-go-v2/service/s3.setCredentialSourceMiddleware.HandleBuild
         0     0%  0.53%      5.55s 22.70%  github.com/aws/smithy-go/transport/http.(*ComputeContentLength).HandleBuild
         0     0%  0.53%      5.46s 22.33%  github.com/aws/aws-sdk-go-v2/aws/middleware.ClientRequestID.HandleBuild
         0     0%  0.53%      5.43s 22.21%  github.com/aws/aws-sdk-go-v2/service/s3.(*getIdentityMiddleware).HandleFinalize
         0     0%  0.53%      5.40s 22.09%  github.com/aws/smithy-go/middleware.(*BuildStep).HandleMiddleware
     0.03s  0.12%  0.65%      5.33s 21.80%  github.com/aws/aws-sdk-go-v2/service/s3.(*resolveEndpointV2Middleware).HandleFinalize
         0     0%  0.65%      5.21s 21.31%  github.com/aws/smithy-go/middleware.decoratedSerializeHandler.HandleSerialize (inline)
         0     0%  0.65%      5.05s 20.65%  github.com/aws/smithy-go/middleware.serializeWrapHandler.HandleSerialize
     0.01s 0.041%   0.7%      4.88s 19.96%  github.com/aws/aws-sdk-go-v2/service/s3.(*isExpressUserAgent).HandleSerialize
```

### Azure
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: cpu
Time: 2026-01-26 16:10:06 CET
Duration: 74.78s, Total samples = 13.82s (18.48%)
Showing nodes accounting for 2.71s, 19.61% of 13.82s total
Dropped 918 nodes (cum <= 0.07s)
Showing top 25 nodes out of 417
      flat  flat%   sum%        cum   cum%
     0.01s 0.072% 0.072%      4.49s 32.49%  net/http.(*conn).serve
         0     0% 0.072%      3.69s 26.70%  net/http.(*ServeMux).ServeHTTP
         0     0% 0.072%      3.69s 26.70%  net/http.serverHandler.ServeHTTP
     0.01s 0.072%  0.14%      3.66s 26.48%  net/http.HandlerFunc.ServeHTTP
         0     0%  0.14%      3.65s 26.41%  pkt.systems/lockd/internal/httpapi.(*Handler).wrap.func1
         0     0%  0.14%      3.08s 22.29%  pkt.systems/lockd/internal/storage/retry.(*backend).withRetry
     0.02s  0.14%  0.29%      2.73s 19.75%  net/http.(*persistConn).writeLoop
     2.53s 18.31% 18.60%      2.53s 18.31%  internal/runtime/syscall.Syscall6
     0.01s 0.072% 18.67%      2.30s 16.64%  runtime.systemstack
         0     0% 18.67%      2.09s 15.12%  syscall.RawSyscall6
     0.01s 0.072% 18.74%      2.02s 14.62%  syscall.Syscall
     0.02s  0.14% 18.89%         2s 14.47%  github.com/Azure/azure-sdk-for-go/sdk/azcore/internal/exported.(*Request).Next
         0     0% 18.89%         2s 14.47%  github.com/Azure/azure-sdk-for-go/sdk/azcore/internal/exported.Pipeline.Do
     0.03s  0.22% 19.10%         2s 14.47%  github.com/Azure/azure-sdk-for-go/sdk/azcore/internal/exported.PolicyFunc.Do
         0     0% 19.10%      1.99s 14.40%  github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime.includeResponsePolicy
         0     0% 19.10%      1.99s 14.40%  runtime.mcall
         0     0% 19.10%      1.97s 14.25%  github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime.telemetryPolicy.Do
         0     0% 19.10%      1.96s 14.18%  github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime.(*retryPolicy).Do
         0     0% 19.10%      1.91s 13.82%  internal/poll.ignoringEINTRIO (inline)
     0.01s 0.072% 19.18%      1.86s 13.46%  runtime.schedule
         0     0% 19.18%      1.75s 12.66%  runtime.park_m
         0     0% 19.18%      1.71s 12.37%  bufio.(*Writer).Flush
         0     0% 19.18%      1.70s 12.30%  github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/internal/exported.(*SharedKeyCredPolicy).Do
     0.06s  0.43% 19.61%      1.69s 12.23%  runtime.findRunnable
         0     0% 19.61%      1.66s 12.01%  github.com/Azure/azure-sdk-for-go/sdk/storage/azblob.(*Client).UploadStream
```

## Alloc space top


### Disk
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: alloc_space
Time: 2026-01-26 16:05:54 CET
Showing nodes accounting for 127.83MB, 54.71% of 233.62MB total
Dropped 144 nodes (cum <= 1.17MB)
Showing top 20 nodes out of 208
      flat  flat%   sum%        cum   cum%
   33.49MB 14.34% 14.34%    33.49MB 14.34%  io.copyBuffer
    7.53MB  3.22% 17.56%     7.53MB  3.22%  bufio.NewWriterSize
       7MB  3.00% 20.56%       12MB  5.14%  pkt.systems/pslog.encodeBaseJSONPlain
    6.50MB  2.78% 23.34%        7MB  3.00%  net/textproto.readMIMEHeader
       6MB  2.57% 25.91%        6MB  2.57%  net/textproto.MIMEHeader.Set
       6MB  2.57% 28.48%        6MB  2.57%  pkt.systems/lockd/internal/storage/disk.(*logNamespace).appendInline
    5.52MB  2.36% 30.84%     5.52MB  2.36%  bufio.NewReaderSize
    5.50MB  2.35% 33.19%    28.51MB 12.20%  pkt.systems/pslog.(*jsonPlainLogger).With
    5.26MB  2.25% 35.44%     5.26MB  2.25%  pkt.systems/lockd/internal/storage/disk.(*logNamespace).allocRef
       5MB  2.14% 37.59%        5MB  2.14%  net/http.(*Request).WithContext
       5MB  2.14% 39.73%        5MB  2.14%  pkt.systems/pslog.appendJSONValuePlain
    4.51MB  1.93% 41.66%     4.51MB  1.93%  runtime.allocm
    4.50MB  1.93% 43.58%     4.50MB  1.93%  pkt.systems/pslog.cloneFields (inline)
    4.50MB  1.93% 45.51%        6MB  2.57%  go.opentelemetry.io/otel/metric.WithAttributes
       4MB  1.71% 47.22%        4MB  1.71%  crypto/internal/fips140/aes/gcm.New
       4MB  1.71% 48.93%        5MB  2.14%  net/url.parseQuery
       4MB  1.71% 50.65%        4MB  1.71%  pkt.systems/pslog.(*loggerBase).withFields (inline)
    3.50MB  1.50% 52.15%        9MB  3.85%  pkt.systems/lockd/internal/storage/disk.(*Store).LoadMeta
       3MB  1.28% 53.43%        3MB  1.28%  encoding/json.(*Decoder).refill
       3MB  1.28% 54.71%        3MB  1.28%  pkt.systems/lockd/client.(*Client).enrichKeyvals
```

### MinIO
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: alloc_space
Time: 2026-01-26 16:06:13 CET
Showing nodes accounting for 857.53MB, 75.13% of 1141.44MB total
Dropped 355 nodes (cum <= 5.71MB)
Showing top 20 nodes out of 185
      flat  flat%   sum%        cum   cum%
  399.99MB 35.04% 35.04%   441.50MB 38.68%  github.com/minio/minio-go/v7/pkg/signer.StreamingSignV4
   77.73MB  6.81% 41.85%    77.73MB  6.81%  regexp.(*bitState).reset
   59.32MB  5.20% 47.05%    59.32MB  5.20%  net/http.init.func15
   42.51MB  3.72% 50.77%    42.51MB  3.72%  crypto/internal/fips140/sha256.New (inline)
   34.48MB  3.02% 53.79%    93.18MB  8.16%  io.copyBuffer
   34.02MB  2.98% 56.77%    39.02MB  3.42%  net/textproto.readMIMEHeader
   31.77MB  2.78% 59.56%    31.77MB  2.78%  pkt.systems/kryptograf/stream.newBufferSet
   26.01MB  2.28% 61.84%    26.01MB  2.28%  strings.(*Builder).grow
   25.50MB  2.23% 64.07%    67.51MB  5.91%  crypto/internal/fips140/hmac.New[go.shape.interface { BlockSize int; Reset; Size int; Sum []uint8; Write  }]
   20.51MB  1.80% 65.87%    20.51MB  1.80%  net/textproto.MIMEHeader.Set
   12.60MB  1.10% 66.97%    12.60MB  1.10%  io.init.func1
   12.52MB  1.10% 68.07%    12.52MB  1.10%  sync.(*Pool).pinSlow
   11.50MB  1.01% 69.07%    42.51MB  3.72%  pkt.systems/pslog.(*jsonPlainLogger).With
   10.54MB  0.92% 70.00%    10.54MB  0.92%  bufio.NewReaderSize
   10.50MB  0.92% 70.92%    46.38MB  4.06%  github.com/minio/minio-go/v7.(*Client).GetObject
   10.50MB  0.92% 71.84%    17.01MB  1.49%  pkt.systems/pslog.encodeBaseJSONPlain
      10MB  0.88% 72.71%       10MB  0.88%  net/http.Header.Clone
    9.50MB  0.83% 73.55%    12.50MB  1.10%  net/http.(*Transport).getConn
    9.03MB  0.79% 74.34%     9.03MB  0.79%  bufio.NewWriterSize
       9MB  0.79% 75.13%    12.50MB  1.10%  github.com/minio/minio-go/v7/pkg/signer.getStringToSignV4
```

### AWS
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: alloc_space
Time: 2026-01-26 16:09:59 CET
Showing nodes accounting for 530.26MB, 46.78% of 1133.52MB total
Dropped 573 nodes (cum <= 5.67MB)
Showing top 20 nodes out of 329
      flat  flat%   sum%        cum   cum%
   60.55MB  5.34%  5.34%    64.05MB  5.65%  github.com/aws/aws-sdk-go-v2/service/s3.addProtocolFinalizerMiddlewares
   41.01MB  3.62%  8.96%    41.01MB  3.62%  strings.(*Builder).grow
   39.51MB  3.49% 12.45%    39.51MB  3.49%  net/http.Header.Clone (inline)
   36.57MB  3.23% 15.67%    44.17MB  3.90%  io.copyBuffer
   36.51MB  3.22% 18.89%    36.51MB  3.22%  net/url.parse
   35.01MB  3.09% 21.98%    39.51MB  3.49%  github.com/aws/smithy-go.(*Properties).Set (inline)
   29.40MB  2.59% 24.58%    29.40MB  2.59%  net/http.init.func15
   26.01MB  2.29% 26.87%    27.01MB  2.38%  net/textproto.readMIMEHeader
   21.08MB  1.86% 28.73%    21.08MB  1.86%  bufio.NewReaderSize
   20.52MB  1.81% 30.54%   718.82MB 63.41%  github.com/aws/aws-sdk-go-v2/service/s3.(*Client).invokeOperation
   20.01MB  1.76% 32.30%    50.51MB  4.46%  net/http.(*Request).Clone
   19.52MB  1.72% 34.03%   324.67MB 28.64%  github.com/aws/aws-sdk-go-v2/service/s3.(*getIdentityMiddleware).HandleFinalize
   19.52MB  1.72% 35.75%    20.02MB  1.77%  github.com/aws/aws-sdk-go-v2/service/s3.addSpanRetryLoop (inline)
   19.50MB  1.72% 37.47%    19.50MB  1.72%  strings.(*Builder).WriteString (partial-inline)
   19.01MB  1.68% 39.15%    36.51MB  3.22%  github.com/aws/aws-sdk-go-v2/aws/signer/v4.(*httpSigner).buildCanonicalHeaders
   18.50MB  1.63% 40.78%    59.51MB  5.25%  github.com/aws/smithy-go/endpoints/private/rulesfn.ParseURL
   18.01MB  1.59% 42.37%    19.51MB  1.72%  net/url.parseQuery
   17.01MB  1.50% 43.87%    20.01MB  1.77%  github.com/aws/aws-sdk-go-v2/service/s3.addCredentialSource
      17MB  1.50% 45.37%       17MB  1.50%  net/textproto.canonicalMIMEHeaderKey
   16.01MB  1.41% 46.78%    17.51MB  1.55%  github.com/aws/aws-sdk-go-v2/service/s3.(*resolveAuthSchemeMiddleware).selectScheme
```

### Azure
```
File: lockd-bench
Build ID: 89113e95c7262451726ef5682766e8ef0e8ed732
Type: alloc_space
Time: 2026-01-26 16:11:20 CET
Showing nodes accounting for 287.97MB, 55.60% of 517.93MB total
Dropped 332 nodes (cum <= 2.59MB)
Showing top 20 nodes out of 279
      flat  flat%   sum%        cum   cum%
   33.53MB  6.47%  6.47%    33.53MB  6.47%  github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/internal/exported.getWeightTables
   32.43MB  6.26% 12.74%    35.98MB  6.95%  io.copyBuffer
   30.01MB  5.80% 18.53%    34.01MB  6.57%  net/textproto.readMIMEHeader
   25.50MB  4.92% 23.46%    25.50MB  4.92%  strings.(*Builder).grow
   19.51MB  3.77% 27.22%    19.51MB  3.77%  net/textproto.MIMEHeader.Set (inline)
   15.91MB  3.07% 30.29%    16.43MB  3.17%  io.ReadAll
   13.05MB  2.52% 32.81%    13.05MB  2.52%  bufio.NewReaderSize
   12.38MB  2.39% 35.20%    12.38MB  2.39%  net/http.init.func15
   11.59MB  2.24% 37.44%    11.59MB  2.24%  pkt.systems/kryptograf/stream.newBufferSet
   10.51MB  2.03% 39.47%    10.51MB  2.03%  crypto/internal/fips140/aes/gcm.New
   10.50MB  2.03% 41.50%    10.50MB  2.03%  net/http.Header.Clone (inline)
   10.50MB  2.03% 43.53%    10.50MB  2.03%  path.Join
      10MB  1.93% 45.46%   149.48MB 28.86%  github.com/Azure/azure-sdk-for-go/sdk/azcore/internal/exported.(*Request).Next
    9.50MB  1.83% 47.29%     9.50MB  1.83%  net/textproto.canonicalMIMEHeaderKey
       8MB  1.55% 48.84%        8MB  1.55%  crypto/internal/fips140/aes.New
       8MB  1.54% 50.38%    14.50MB  2.80%  net/http.NewRequestWithContext
    7.50MB  1.45% 51.83%     7.50MB  1.45%  net/url.escape
       7MB  1.35% 53.18%     9.50MB  1.83%  context.(*cancelCtx).propagateCancel
    6.52MB  1.26% 54.44%     6.52MB  1.26%  bytes.growSlice
       6MB  1.16% 55.60%       13MB  2.51%  pkt.systems/pslog.(*jsonPlainLogger).With
```
