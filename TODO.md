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
