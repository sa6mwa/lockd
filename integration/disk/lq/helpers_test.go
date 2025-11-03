//go:build integration && disk && lq

package disklq

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
	"pkt.systems/pslog"
)

type diskQueueOptions struct {
	EnableWatch       bool
	PollInterval      time.Duration
	PollJitter        time.Duration
	ResilientInterval time.Duration
}

func prepareDiskQueueRoot(t testing.TB) string {
	t.Helper()
	base := strings.TrimSpace(os.Getenv("LOCKD_DISK_ROOT"))
	if base == "" {
		t.Fatalf("LOCKD_DISK_ROOT must be set (source .env.disk before running disk queue integration tests)")
	}
	info, err := os.Stat(base)
	if err != nil || !info.IsDir() {
		t.Fatalf("LOCKD_DISK_ROOT %q unavailable: %v", base, err)
	}
	root := filepath.Join(base, "lockd-queue-"+uuidv7.NewString())
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", root, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func buildDiskQueueConfig(t testing.TB, root string, opts diskQueueOptions) lockd.Config {
	t.Helper()
	cfg := lockd.Config{
		Store:           diskStoreURL(root),
		DisableMTLS:     true,
		ListenProto:     "tcp",
		Listen:          "127.0.0.1:0",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
		DiskRetention:   0,
		DiskQueueWatch:  opts.EnableWatch,
	}
	cfg.QRFEnabled = true
	cfg.QRFQueueSoftLimit = 6
	cfg.QRFQueueHardLimit = 12
	cfg.QRFLockSoftLimit = 4
	cfg.QRFLockHardLimit = 8
	cfg.QRFSwapSoftLimitBytes = 0
	cfg.QRFSwapHardLimitBytes = 0
	cfg.QRFCPUPercentSoftLimit = 0
	cfg.QRFCPUPercentHardLimit = 0
	cfg.QRFMemorySoftLimitPercent = 0
	cfg.QRFMemoryHardLimitPercent = 0
	cfg.QRFSwapSoftLimitPercent = 0
	cfg.QRFSwapHardLimitPercent = 0
	cfg.QRFLoadSoftLimitMultiplier = 3
	cfg.QRFLoadHardLimitMultiplier = 6
	cfg.QRFRecoverySamples = 1
	cfg.QRFSoftRetryAfter = 50 * time.Millisecond
	cfg.QRFEngagedRetryAfter = 200 * time.Millisecond
	cfg.QRFRecoveryRetryAfter = 100 * time.Millisecond
	if opts.PollInterval > 0 {
		cfg.QueuePollInterval = opts.PollInterval
	}
	if opts.PollJitter >= 0 {
		cfg.QueuePollJitter = opts.PollJitter
	}
	if opts.ResilientInterval > 0 {
		cfg.QueueResilientPollInterval = opts.ResilientInterval
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation failed: %v", err)
	}
	return cfg
}

func startDiskQueueServer(t testing.TB, cfg lockd.Config) *lockd.TestServer {
	t.Helper()
	return queuetestutil.StartQueueTestServer(t, cfg)
}

func startDiskQueueServerWithLogger(t testing.TB, cfg lockd.Config, logger pslog.Logger) *lockd.TestServer {
	t.Helper()
	return queuetestutil.StartQueueTestServerWithLogger(t, cfg, logger)
}

func startDiskQueueServerWithCapture(t testing.TB, cfg lockd.Config) (*lockd.TestServer, *queuetestutil.LogCapture) {
	t.Helper()
	capture := queuetestutil.NewLogCapture(t)
	return startDiskQueueServerWithLogger(t, cfg, capture.Logger()), capture
}

func diskStoreURL(root string) string {
	clean := filepath.Clean(root)
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	return (&url.URL{Scheme: "disk", Path: clean}).String()
}
