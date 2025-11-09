//go:build integration && disk && query

package diskquery

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queriesuite "pkt.systems/lockd/integration/query/suite"
	"pkt.systems/pslog"
)

func TestDiskQuerySelectors(t *testing.T) {
	queriesuite.RunSelectors(t, startDiskQueryServer)
}

func TestDiskQueryPagination(t *testing.T) {
	queriesuite.RunPagination(t, startDiskQueryServer)
}

func TestDiskQueryNamespaceIsolation(t *testing.T) {
	queriesuite.RunNamespaceIsolation(t, startDiskQueryServer)
}

func TestDiskQueryResultsSupportPublicRead(t *testing.T) {
	queriesuite.RunPublicRead(t, startDiskQueryServer)
}

func TestDiskQueryDomainDatasets(t *testing.T) {
	queriesuite.RunDomainDatasets(t, startDiskQueryServer)
}

func startDiskQueryServer(t testing.TB) *lockd.TestServer {
	t.Helper()
	root := prepareDiskQueryRoot(t)
	cfg := lockd.Config{
		Store:           diskStoreURL(root),
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(t, pslog.DebugLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(45*time.Second),
			lockdclient.WithCloseTimeout(45*time.Second),
			lockdclient.WithKeepAliveTimeout(45*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.DebugLevel)),
		),
	}
	opts = append(opts, cryptotest.SharedMTLSOptions(t)...)
	ts := lockd.StartTestServer(t, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	})
	return ts
}

func prepareDiskQueryRoot(t testing.TB) string {
	t.Helper()
	base := os.Getenv("LOCKD_DISK_ROOT")
	if base == "" {
		t.Fatalf("LOCKD_DISK_ROOT must be set (source .env.disk before running disk query tests)")
	}
	root := filepath.Join(base, fmt.Sprintf("lockd-query-%d", time.Now().UnixNano()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir disk root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func diskStoreURL(root string) string {
	if root == "" {
		root = "/tmp/lockd-disk"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return (&url.URL{Scheme: "disk", Path: root}).String()
}
