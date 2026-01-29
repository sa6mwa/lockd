//go:build bench

package benchenv

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
)

// LoadAzureConfig loads the Azure queue benchmark configuration.
func LoadAzureConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	LoadEnvFile(tb, ".env.azure")

	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		tb.Fatalf("LOCKD_STORE must be set to an azure:// URI (see .env.azure)")
	}
	if !strings.HasPrefix(store, "azure://") {
		tb.Fatalf("LOCKD_STORE must reference an azure:// URI, got %q", store)
	}
	store = ensureBenchmarkPrefix(tb, store)

	cfg := lockd.Config{
		Store:           store,
		AzureEndpoint:   os.Getenv("LOCKD_AZURE_ENDPOINT"),
		AzureSASToken:   os.Getenv("LOCKD_AZURE_SAS_TOKEN"),
		AzureAccountKey: os.Getenv("LOCKD_AZURE_ACCOUNT_KEY"),
	}

	MaybeEnableStorageEncryption(tb, &cfg)
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("azure config validation failed: %v", err)
	}
	return cfg
}

// EnsureAzureStoreReady runs a quick store verification to catch misconfigured credentials.
func EnsureAzureStoreReady(tb testing.TB, cfg lockd.Config) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		tb.Fatalf("azure store verification failed: %v", err)
	}
	if !res.Passed() {
		tb.Fatalf("azure store verification failed: %+v", res)
	}
}
