//go:build integration && nfs && mcp

package nfsmcp

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	mcpsuite "pkt.systems/lockd/integration/mcp/suite"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

func TestNFSMCPE2E(t *testing.T) {
	t.Run("go-sdk", func(t *testing.T) {
		mcpsuite.RunFacadeE2E(t, startNFSServer)
	})
	t.Run("google-adk", func(t *testing.T) {
		mcpsuite.RunGoogleADKE2E(t, startNFSServer)
	})
}

func startNFSServer(tb testing.TB) *lockd.TestServer {
	tb.Helper()
	base := strings.TrimSpace(os.Getenv("LOCKD_NFS_ROOT"))
	if base == "" {
		tb.Fatalf("LOCKD_NFS_ROOT must be set (source .env.nfs before running nfs/mcp integration tests)")
	}
	root := filepath.Join(base, "lockd-mcp-"+uuidv7.NewString())
	if err := os.MkdirAll(root, 0o755); err != nil {
		tb.Fatalf("mkdir nfs root: %v", err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(root) })

	cfg := lockd.Config{
		Store:           (&url.URL{Scheme: "disk", Path: root}).String(),
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
		HAMode:          "failover",
		HALeaseTTL:      30 * time.Second,
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
