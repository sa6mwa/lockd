//go:build integration && azure

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
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

var (
	azureStoreVerifyOnce sync.Once
	azureStoreVerifyErr  error
)

func loadAzureConfig(tb testing.TB) lockd.Config {
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		tb.Fatalf("LOCKD_STORE must reference an azure:// URI for Azure integration tests")
	}
	if !strings.HasPrefix(store, "azure://") {
		tb.Fatalf("LOCKD_STORE must reference an azure:// URI, got %q", store)
	}
	cfg := lockd.Config{
		Store:         store,
		AzureEndpoint: os.Getenv("LOCKD_AZURE_ENDPOINT"),
		AzureSASToken: os.Getenv("LOCKD_AZURE_SAS_TOKEN"),
	}
	cfg.AzureAccountKey = os.Getenv("LOCKD_AZURE_ACCOUNT_KEY")
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	return cfg
}

func ensureAzureStoreReady(tb testing.TB, ctx context.Context, cfg lockd.Config) {
	azuretest.ResetContainerForCrypto(tb, cfg)
	azureStoreVerifyOnce.Do(func() {
		res, err := storagecheck.VerifyStore(ctx, cfg)
		if err != nil {
			azureStoreVerifyErr = err
			return
		}
		if !res.Passed() {
			azureStoreVerifyErr = fmt.Errorf("store verification failed: %+v", res)
		}
	})
	if azureStoreVerifyErr != nil {
		tb.Fatalf("store verification failed: %v", azureStoreVerifyErr)
	}
}

func startAzureTestServer(t testing.TB, cfg lockd.Config, opts ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
	cfgCopy := cfg
	if cfgCopy.JSONMaxBytes == 0 {
		cfgCopy.JSONMaxBytes = 100 << 20
	}
	if cfgCopy.DefaultTTL == 0 {
		cfgCopy.DefaultTTL = 30 * time.Second
	}
	if cfgCopy.MaxTTL == 0 {
		cfgCopy.MaxTTL = 2 * time.Minute
	}
	if cfgCopy.AcquireBlock == 0 {
		cfgCopy.AcquireBlock = 5 * time.Second
	}
	if cfgCopy.SweeperInterval == 0 {
		cfgCopy.SweeperInterval = 5 * time.Second
	}
	if cfgCopy.StorageRetryMaxAttempts < 12 {
		cfgCopy.StorageRetryMaxAttempts = 12
	}
	if cfgCopy.StorageRetryBaseDelay < 500*time.Millisecond {
		cfgCopy.StorageRetryBaseDelay = 500 * time.Millisecond
	}
	if cfgCopy.StorageRetryMaxDelay < 15*time.Second {
		cfgCopy.StorageRetryMaxDelay = 15 * time.Second
	}
	cryptotest.ConfigureTCAuth(t, &cfgCopy)

	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfgCopy),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithForUpdateTimeout(2*time.Minute),
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
		),
	}
	options = append(options, opts...)
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	closeDefaults := lockd.WithTestCloseDefaults(
		lockd.WithDrainLeases(-1),
		lockd.WithShutdownTimeout(10*time.Second),
	)
	options = append(options, closeDefaults)
	ts, err := lockd.NewTestServer(context.Background(), options...)
	if err != nil {
		t.Fatalf("start test server: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := ts.Stop(ctx); err != nil {
			t.Fatalf("stop test server: %v", err)
		}
	})
	t.Cleanup(func() {
		if backend := ts.Backend(); backend != nil {
			azuretest.CleanupNamespaceIndexesWithStore(t, backend, namespaces.Default)
			return
		}
		azuretest.CleanupNamespaceIndexes(t, cfg, namespaces.Default)
	})
	return ts
}

func directClient(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
	t.Helper()
	addr := ts.Server.ListenerAddr()
	if addr == nil {
		t.Fatalf("listener not initialised")
	}
	scheme := "http"
	if ts.Config.MTLSEnabled() {
		scheme = "https"
	}
	endpoint := fmt.Sprintf("%s://%s", scheme, addr.String())
	cli, err := ts.NewEndpointsClient([]string{endpoint},
		lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
		lockdclient.WithHTTPTimeout(2*time.Minute),
		lockdclient.WithEndpointShuffle(false),
	)
	if err != nil {
		t.Fatalf("direct client: %v", err)
	}
	return cli
}

func cleanupAzure(t *testing.T, cfg lockd.Config, key string) {
	azuretest.CleanupKey(t, cfg, namespaces.Default, key)
}
