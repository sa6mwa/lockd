//go:build integration && aws && mcp

package awsmcp

import (
	"os"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/storepath"
	mcpsuite "pkt.systems/lockd/integration/mcp/suite"
	"pkt.systems/pslog"
)

func TestAWSMCPE2E(t *testing.T) {
	t.Run("go-sdk", func(t *testing.T) {
		mcpsuite.RunFacadeE2E(t, startAWSServer)
	})
	t.Run("google-adk", func(t *testing.T) {
		mcpsuite.RunGoogleADKE2E(t, startAWSServer)
	})
}

func startAWSServer(tb testing.TB) *lockd.TestServer {
	tb.Helper()
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" || !strings.HasPrefix(store, "aws://") {
		tb.Fatalf("LOCKD_STORE must be set to an aws:// URI for aws/mcp integration tests")
	}
	store = storepath.Scoped(tb, store, "aws/mcp")
	region := strings.TrimSpace(os.Getenv("LOCKD_AWS_REGION"))
	if region == "" {
		region = strings.TrimSpace(os.Getenv("AWS_REGION"))
	}
	if region == "" {
		region = strings.TrimSpace(os.Getenv("AWS_DEFAULT_REGION"))
	}

	cfg := lockd.Config{
		Store:                   store,
		AWSRegion:               region,
		AWSKMSKeyID:             strings.TrimSpace(os.Getenv("LOCKD_AWS_KMS_KEY_ID")),
		S3SSE:                   strings.TrimSpace(os.Getenv("LOCKD_S3_SSE")),
		S3KMSKeyID:              strings.TrimSpace(os.Getenv("LOCKD_S3_KMS_KEY_ID")),
		Listen:                  "127.0.0.1:0",
		ListenProto:             "tcp",
		DefaultTTL:              30 * time.Second,
		MaxTTL:                  2 * time.Minute,
		AcquireBlock:            5 * time.Second,
		SweeperInterval:         5 * time.Second,
		StorageRetryMaxAttempts: 12,
		StorageRetryBaseDelay:   500 * time.Millisecond,
		StorageRetryMaxDelay:    15 * time.Second,
	}
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)

	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
	}
	opts = append(opts, cryptotest.SharedMTLSOptions(tb)...)
	return lockd.StartTestServer(tb, opts...)
}
