//go:build bench && nfs

package nfsbench

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/benchmark/internal/benchenv"
	"pkt.systems/lockd/benchmark/internal/lqbench"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

const nfsQueuePayloadSize = 256

func BenchmarkNFSQueueAckLease(b *testing.B) {
	root := prepareNFSRoot(b)
	cfg := buildNFSConfig(b, root)
	applyQueueBenchConfig(&cfg)

	client := startNFSBenchServer(b, cfg)
	payload := bytes.Repeat([]byte("x"), nfsQueuePayloadSize)
	scenario := lqbench.Scenario{
		Name:          "nfs-queue",
		Producers:     6,
		Consumers:     6,
		TotalMessages: 200,
		Prefetch:      4,
		UseSubscribe:  true,
		Timeout:       90 * time.Second,
	}
	lqbench.Run(b, []*lockdclient.Client{client}, scenario, payload)
}

func prepareNFSRoot(tb testing.TB) string {
	tb.Helper()
	benchenv.LoadEnvFile(tb, ".env.nfs")
	base := strings.TrimSpace(os.Getenv("LOCKD_NFS_ROOT"))
	if base == "" {
		tb.Fatalf("LOCKD_NFS_ROOT must be set (source .env.nfs before running nfs benchmarks)")
	}
	info, err := os.Stat(base)
	if err != nil || !info.IsDir() {
		tb.Fatalf("LOCKD_NFS_ROOT %q unavailable: %v", base, err)
	}
	root := filepath.Join(base, "lockd-"+uuidv7.NewString())
	if err := os.MkdirAll(root, 0o755); err != nil {
		tb.Fatalf("mkdir nfs root: %v", err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func buildNFSConfig(tb testing.TB, root string) lockd.Config {
	tb.Helper()
	mtlsEnabled := benchenv.TestMTLSEnabled()
	cfg := lockd.Config{
		Store:           diskStoreURL(root),
		DisableMTLS:     !mtlsEnabled,
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
	}
	benchenv.MaybeEnableStorageEncryption(tb, &cfg)
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config validation failed: %v", err)
	}
	return cfg
}

func diskStoreURL(root string) string {
	if root == "" {
		root = "/tmp/lockd-nfs"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return (&url.URL{Scheme: "disk", Path: root}).String()
}

func applyQueueBenchConfig(cfg *lockd.Config) {
	if cfg == nil {
		return
	}
	cfg.QueuePollInterval = 25 * time.Millisecond
	cfg.QueueResilientPollInterval = 250 * time.Millisecond
	cfg.QueuePollJitter = 0
	cfg.QRFDisabled = true
	if cfg.SweeperInterval <= 0 {
		cfg.SweeperInterval = 2 * time.Second
	}
}

func startNFSBenchServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()
	mtlsEnabled := benchenv.TestMTLSEnabled()
	cfg.DisableMTLS = !mtlsEnabled

	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithLogger(pslog.NoopLogger()),
	}

	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLogger(pslog.NoopLogger()),
		lockd.WithTestClientOptions(clientOpts...),
	}
	if mtlsEnabled {
		opts = append(opts, benchenv.SharedMTLSOptions(tb)...)
	} else {
		opts = append(opts,
			lockd.WithoutTestMTLS(),
			lockd.WithTestClientOptions(lockdclient.WithDisableMTLS(true)),
		)
	}
	ts := lockd.StartTestServer(tb, opts...)
	return ts.Client
}
