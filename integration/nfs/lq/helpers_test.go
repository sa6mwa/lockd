//go:build integration && nfs && lq

package nfslq

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/uuidv7"
)

type nfsQueueOptions struct {
	PollInterval      time.Duration
	PollJitter        time.Duration
	ResilientInterval time.Duration
}

func prepareNFSQueueRoot(t testing.TB) string {
	t.Helper()
	base := strings.TrimSpace(os.Getenv("LOCKD_NFS_ROOT"))
	if base == "" {
		t.Skip("LOCKD_NFS_ROOT not set; skipping NFS queue tests")
	}
	info, err := os.Stat(base)
	if err != nil || !info.IsDir() {
		t.Fatalf("LOCKD_NFS_ROOT %q unavailable: %v", base, err)
	}
	root := filepath.Join(base, "lockd-queue-"+uuidv7.NewString())
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", root, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func buildNFSQueueConfig(t testing.TB, root string, opts nfsQueueOptions) lockd.Config {
	t.Helper()
	cfg := lockd.Config{
		Store:                      nfsStoreURL(root),
		MTLS:                       false,
		ListenProto:                "tcp",
		Listen:                     "127.0.0.1:0",
		DefaultTTL:                 30 * time.Second,
		MaxTTL:                     2 * time.Minute,
		AcquireBlock:               10 * time.Second,
		SweeperInterval:            2 * time.Second,
		DiskRetention:              0,
		DiskQueueWatch:             false,
		QueuePollInterval:          opts.PollInterval,
		QueuePollJitter:            opts.PollJitter,
		QueueResilientPollInterval: opts.ResilientInterval,
	}
	if cfg.QueuePollInterval <= 0 {
		cfg.QueuePollInterval = 100 * time.Millisecond
	}
	if cfg.QueueResilientPollInterval <= 0 {
		cfg.QueueResilientPollInterval = 500 * time.Millisecond
	}
	if cfg.QueuePollJitter < 0 {
		cfg.QueuePollJitter = 0
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation failed: %v", err)
	}
	return cfg
}

func startNFSQueueServer(t testing.TB, cfg lockd.Config) *lockd.TestServer {
	t.Helper()
	return queuetestutil.StartQueueTestServer(t, cfg)
}

func startNFSQueueServerWithCapture(t testing.TB, cfg lockd.Config) (*lockd.TestServer, *queuetestutil.LogCapture) {
	t.Helper()
	capture := queuetestutil.NewLogCapture(t)
	return queuetestutil.StartQueueTestServerWithLogger(t, cfg, capture.Logger()), capture
}

func nfsStoreURL(root string) string {
	clean := filepath.Clean(root)
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	return (&url.URL{Scheme: "disk", Path: clean}).String()
}
