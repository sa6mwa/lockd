//go:build integration && nfs && lq

package nfslq

import (
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/storetest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
)

type nfsQueueOptions struct {
	PollInterval      time.Duration
	PollJitter        time.Duration
	ResilientInterval time.Duration
}

func prepareNFSQueueRoot(t testing.TB) string {
	t.Helper()
	return storetest.PrepareDiskStoreSubdir(t, "nfs", "", "lockd-queue")
}

func buildNFSQueueConfig(t testing.TB, root string, opts nfsQueueOptions) lockd.Config {
	t.Helper()
	cfg := lockd.Config{
		Store:                      storetest.DiskStoreURL(root),
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
		HAMode:                     "failover",
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
	return cfg
}

func startNFSQueueServer(t testing.TB, cfg lockd.Config) *lockd.TestServer {
	t.Helper()
	return queuetestutil.StartQueueTestServer(t, cfg)
}

func startNFSQueueServerWithOptions(t testing.TB, cfg lockd.Config, opts ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
	return queuetestutil.StartQueueTestServerWithOptions(t, cfg, opts)
}

func startNFSQueueServerWithCapture(t testing.TB, cfg lockd.Config) (*lockd.TestServer, *queuetestutil.LogCapture) {
	t.Helper()
	capture := queuetestutil.NewLogCapture(t)
	return queuetestutil.StartQueueTestServerWithLogger(t, cfg, capture.Logger()), capture
}
