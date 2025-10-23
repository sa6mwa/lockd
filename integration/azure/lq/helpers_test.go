//go:build integration && azure && lq

package azureintegration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/logport"
)

type azureQueueOptions struct {
	PollInterval      time.Duration
	PollJitter        time.Duration
	ResilientInterval time.Duration
}

func prepareAzureQueueConfig(t testing.TB, opts azureQueueOptions) lockd.Config {
	cfg := loadAzureQueueConfig(t)
	ensureAzureStoreReady(t, context.Background(), cfg)

	if opts.PollInterval > 0 {
		cfg.QueuePollInterval = opts.PollInterval
	} else {
		cfg.QueuePollInterval = 25 * time.Millisecond
	}
	cfg.QueuePollJitter = opts.PollJitter
	if cfg.QueuePollJitter < 0 {
		cfg.QueuePollJitter = 0
	}
	if opts.ResilientInterval > 0 {
		cfg.QueueResilientPollInterval = opts.ResilientInterval
	} else {
		cfg.QueueResilientPollInterval = 250 * time.Millisecond
	}

	cfg.QRFEnabled = false

	cfg.MTLS = false
	cfg.ListenProto = "tcp"
	cfg.Listen = "127.0.0.1:0"

	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("azure queue config validation failed: %v", err)
	}
	return cfg
}

func loadAzureQueueConfig(t testing.TB) lockd.Config {
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		t.Skip("LOCKD_STORE must reference an azure:// URI (see .env.azure)")
	}
	if !strings.HasPrefix(store, "azure://") {
		t.Fatalf("LOCKD_STORE must reference an azure:// URI, got %q", store)
	}
	cfg := lockd.Config{
		Store:           store,
		AzureEndpoint:   os.Getenv("LOCKD_AZURE_ENDPOINT"),
		AzureSASToken:   os.Getenv("LOCKD_AZURE_SAS_TOKEN"),
		AzureAccountKey: os.Getenv("LOCKD_AZURE_ACCOUNT_KEY"),
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("azure queue config validation: %v", err)
	}
	return cfg
}

func ensureAzureStoreReady(t testing.TB, ctx context.Context, cfg lockd.Config) {
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		t.Fatalf("store verification failed: %+v", res)
	}
}

func startAzureQueueServer(t testing.TB, cfg lockd.Config) *lockd.TestServer {
	return startAzureQueueServerWithLogger(t, cfg, lockd.NewTestingLogger(t, logport.TraceLevel))
}

func startAzureQueueServerWithCapture(t testing.TB, cfg lockd.Config) (*lockd.TestServer, *queuetestutil.LogCapture) {
	capture := queuetestutil.NewLogCapture(t)
	ts := startAzureQueueServerWithLogger(t, cfg, capture.Logger())
	return ts, capture
}

func startAzureQueueServerWithLogger(t testing.TB, cfg lockd.Config, logger logport.ForLogging) *lockd.TestServer {
	clientLogger := lockd.NewTestingLogger(t, logport.TraceLevel)
	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(120 * time.Second),
		lockdclient.WithKeepAliveTimeout(120 * time.Second),
		lockdclient.WithCloseTimeout(120 * time.Second),
		lockdclient.WithLogger(clientLogger),
	}
	return lockd.StartTestServer(t,
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(logger),
		lockd.WithTestClientOptions(clientOpts...),
		lockd.WithTestStartTimeout(30*time.Second),
	)
}
