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

