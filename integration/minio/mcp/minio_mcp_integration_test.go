//go:build integration && minio && mcp

package miniomcp

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

func TestMinioMCPE2E(t *testing.T) {
	t.Run("go-sdk", func(t *testing.T) {
		mcpsuite.RunFacadeE2E(t, startMinioServer)
	})
	t.Run("google-adk", func(t *testing.T) {
		mcpsuite.RunGoogleADKE2E(t, startMinioServer)
	})
}

func startMinioServer(tb testing.TB) *lockd.TestServer {
	tb.Helper()
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" || !strings.HasPrefix(store, "s3://") {
		tb.Fatalf("LOCKD_STORE must be set to an s3:// URI for minio/mcp integration tests")
	}
	store = storepath.Scoped(tb, store, "minio/mcp")
	cfg := lockd.Config{
		Store:                   store,
		S3MaxPartSize:           8 << 20,
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
