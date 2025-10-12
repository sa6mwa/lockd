# The big lockd TODO

All major points *to do* are headers. Each header section provide context and possibly output that clarify the task to improve, implement, change, update or otherwise modify.

### Unit tests should fail when necessary envvars are missing

```terminal
$ test -v -tags 'integration aws' ./integration/aws/
=== RUN   TestAWSStoreVerification
    aws_integration_test.go:19: LOCKD_STORE must reference an s3:// URI for AWS integration test
--- SKIP: TestAWSStoreVerification (0.00s)
PASS
ok      pkt.systems/lockd/integration/aws       0.006s
```

### Use OpenBAO for storing and retrieving certificates and keys

TODO: describe the implementation, not sure how this should look like yet.

### Debug loglevel should output all ops by all clients

`acquire`, `getstate`, etc, all of them, should be logged if log level is `debug`.

### Improve performance metrics

With stdlib json (non-streaming compacting)...

  - Raw MinIO, single large: 26.9 ms/op, 0.85 MB allocs.
  - lockd, single large: 136 ms/op, 27.8 MB allocs.
  - Raw MinIO, single small: 30.1 ms/op, 0.80 MB allocs.
  - lockd, single small: 92.8 ms/op, 2.0 MB allocs.
  - Raw MinIO, concurrent small: 0.38 ms/op, 0.11 MB allocs.
  - lockd, concurrent small: 2.3 ms/op, 0.53 MB allocs.
  - Raw MinIO, concurrent large: 6.3 ms/op, 0.85 MB allocs.
  - lockd, concurrent large: 16.0 ms/op, 27.9 MB allocs.
 
With internal/jsonutil (streaming compacter)...

  - MinIO raw large: 26.8 ms/op, 0.85 MB allocs
  - lockd large: 131 ms/op, 23.3 MB allocs
  - MinIO raw small: 32.3 ms/op, 0.80 MB allocs
  - lockd small: 95.4 ms/op, 2.08 MB allocs
  - MinIO raw concurrent small: 0.37 ms/op, 0.10 MB allocs
  - lockd concurrent small: 2.23 ms/op, 0.54 MB allocs
  - MinIO raw concurrent large: 15.1 ms/op, 0.86 MB allocs
  - lockd concurrent large: 21.5 ms/op, 23.4 MB allocs
