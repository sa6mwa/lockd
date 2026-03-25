//go:build integration && disk && mcp

package diskmcp

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/storetest"
	mcpsuite "pkt.systems/lockd/integration/mcp/suite"
	"pkt.systems/pslog"
)

func TestDiskMCPE2E(t *testing.T) {
	t.Run("go-sdk", func(t *testing.T) {
		mcpsuite.RunFacadeE2E(t, startDiskServer)
	})
	t.Run("google-adk", func(t *testing.T) {
		mcpsuite.RunGoogleADKE2E(t, startDiskServer)
	})
}

func startDiskServer(tb testing.TB) *lockd.TestServer {
	tb.Helper()
	root := storetest.PrepareDiskStoreSubdir(tb, "disk", "", "lockd-mcp")

	cfg := lockd.Config{
		Store:           storetest.DiskStoreURL(root),
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
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
