//go:build integration && azure && lq

package azureintegration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	azuretest "pkt.systems/lockd/integration/azuretest"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/storepath"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

type azureQueueOptions struct {
	PollInterval      time.Duration
	PollJitter        time.Duration
	ResilientInterval time.Duration
}

var (
	azureQueueStoreVerifyOnce sync.Once
	azureQueueStoreVerifyErr  error
)

func prepareAzureQueueConfig(t testing.TB, opts azureQueueOptions) lockd.Config {
	cfg := loadAzureQueueConfig(t)
	cfg.Store = withQueueTestPrefix(t, cfg.Store)
	cfg.HAMode = "failover"
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

	cfg.QRFDisabled = true

	cfg.ListenProto = "tcp"
	cfg.Listen = "127.0.0.1:0"

	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("azure queue config validation failed: %v", err)
	}
	return cfg
}

func withQueueTestPrefix(t testing.TB, store string) string {
	t.Helper()
	suffix := "lq-" + uuidv7.NewString()
	return storepath.Append(t, store, suffix)
}

func loadAzureQueueConfig(t testing.TB) lockd.Config {
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		t.Fatalf("LOCKD_STORE must reference an azure:// URI (see .env.azure)")
	}
	if !strings.HasPrefix(store, "azure://") {
		t.Fatalf("LOCKD_STORE must reference an azure:// URI, got %q", store)
	}
	store = storepath.Scoped(t, store, "azure")
	cfg := lockd.Config{
		Store:           store,
		AzureEndpoint:   os.Getenv("LOCKD_AZURE_ENDPOINT"),
		AzureSASToken:   os.Getenv("LOCKD_AZURE_SAS_TOKEN"),
		AzureAccountKey: os.Getenv("LOCKD_AZURE_ACCOUNT_KEY"),
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	return cfg
}

func ensureAzureStoreReady(t testing.TB, ctx context.Context, cfg lockd.Config) {
	azuretest.ResetContainerForCrypto(t, cfg)
	azureQueueStoreVerifyOnce.Do(func() {
		res, err := storagecheck.VerifyStore(ctx, cfg)
		if err != nil {
			azureQueueStoreVerifyErr = err
			return
		}
		if !res.Passed() {
			azureQueueStoreVerifyErr = fmt.Errorf("store verification failed: %+v", res)
		}
	})
	if azureQueueStoreVerifyErr != nil {
		t.Fatalf("store verification failed: %v", azureQueueStoreVerifyErr)
	}
}

func startAzureQueueServer(t testing.TB, cfg lockd.Config) *lockd.TestServer {
	return startAzureQueueServerWithLogger(t, cfg, lockd.NewTestingLogger(t, pslog.TraceLevel))
}

func startAzureQueueServerWithOptions(t testing.TB, cfg lockd.Config, extra ...lockd.TestServerOption) *lockd.TestServer {
	return startAzureQueueServerWithLogger(t, cfg, lockd.NewTestingLogger(t, pslog.TraceLevel), extra...)
}

func startAzureQueueServerWithCapture(t testing.TB, cfg lockd.Config) (*lockd.TestServer, *queuetestutil.LogCapture) {
	capture := queuetestutil.NewLogCapture(t)
	ts := startAzureQueueServerWithLogger(t, cfg, capture.Logger())
	return ts, capture
}

func startAzureQueueServerWithLogger(t testing.TB, cfg lockd.Config, logger pslog.Logger, extra ...lockd.TestServerOption) *lockd.TestServer {
	cryptotest.ConfigureTCAuth(t, &cfg)
	clientLogger := lockd.NewTestingLogger(t, pslog.TraceLevel)
	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(120 * time.Second),
		lockdclient.WithKeepAliveTimeout(120 * time.Second),
		lockdclient.WithCloseTimeout(120 * time.Second),
		lockdclient.WithLogger(clientLogger),
	}
	closeDefaults := lockd.WithTestCloseDefaults(
		lockd.WithDrainLeases(0),
		lockd.WithShutdownTimeout(2*time.Second),
	)
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(logger),
		lockd.WithTestClientOptions(clientOpts...),
		lockd.WithTestStartTimeout(30 * time.Second),
		closeDefaults,
	}
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	options = append(options, extra...)
	return lockd.StartTestServer(t, options...)
}

func scheduleAzureQueueCleanup(t *testing.T, cfg lockd.Config, queue string) {
	t.Helper()
	t.Cleanup(func() {
		azuretest.CleanupQueue(t, cfg, namespaces.Default, queue)
	})
}

func scheduleAzureLockCleanup(t *testing.T, cfg lockd.Config, key string) {
	t.Helper()
	t.Cleanup(func() {
		azuretest.CleanupKey(t, cfg, namespaces.Default, key)
	})
}
