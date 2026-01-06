//go:build integration && mixed

package mixedintegration

import (
	"context"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
)

func buildMixedMemConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	cfg := lockd.Config{
		Store:           "mem://",
		Listen:          "127.0.0.1:0",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    10 * time.Second,
		SweeperInterval: 2 * time.Second,
		TCFanoutTimeout: 15 * time.Second,
	}
	cfg.DisableMemQueueWatch = false
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	return cfg
}

func loadMixedAWSConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	store := firstEnv("LOCKD_AWS_STORE", "LOCKD_STORE")
	if store == "" {
		tb.Fatalf("LOCKD_AWS_STORE must be set to an aws:// URI for mixed integration tests")
	}
	if !strings.HasPrefix(store, "aws://") {
		tb.Fatalf("LOCKD_AWS_STORE must reference an aws:// URI, got %q", store)
	}
	cfg := lockd.Config{
		Store:           store,
		AWSRegion:       firstEnv("LOCKD_AWS_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"),
		AWSKMSKeyID:     firstEnv("LOCKD_AWS_KMS_KEY_ID"),
		S3SSE:           firstEnv("LOCKD_S3_SSE"),
		S3KMSKeyID:      firstEnv("LOCKD_S3_KMS_KEY_ID"),
		S3MaxPartSize:   16 << 20,
		TCFanoutTimeout: 15 * time.Second,
	}
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config validation: %v", err)
	}
	return cfg
}

func loadMixedAzureConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	store := firstEnv("LOCKD_AZURE_STORE", "LOCKD_STORE")
	if store == "" {
		tb.Fatalf("LOCKD_AZURE_STORE must reference an azure:// URI for mixed integration tests")
	}
	if !strings.HasPrefix(store, "azure://") {
		tb.Fatalf("LOCKD_AZURE_STORE must reference an azure:// URI, got %q", store)
	}
	cfg := lockd.Config{
		Store:           store,
		AzureEndpoint:   firstEnv("LOCKD_AZURE_ENDPOINT"),
		AzureSASToken:   firstEnv("LOCKD_AZURE_SAS_TOKEN"),
		TCFanoutTimeout: 15 * time.Second,
	}
	cfg.AzureAccountKey = firstEnv("LOCKD_AZURE_ACCOUNT_KEY")
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	return cfg
}

func ensureStoreReady(tb testing.TB, cfg lockd.Config) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		tb.Fatalf("store verification failed: %v", err)
	}
	if !res.Passed() {
		tb.Fatalf("store verification failed: %+v", res)
	}
}
