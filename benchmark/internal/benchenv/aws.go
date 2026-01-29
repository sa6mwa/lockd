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

// LoadAWSConfig loads the AWS queue benchmark configuration.
func LoadAWSConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	LoadEnvFile(tb, ".env.aws")

	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		tb.Fatalf("LOCKD_STORE must be set to an aws:// URI (see .env.aws)")
	}
	if !strings.HasPrefix(store, "aws://") {
		tb.Fatalf("LOCKD_STORE must reference an aws:// URI, got %q", store)
	}
	store = ensureBenchmarkPrefix(tb, store)

	cfg := lockd.Config{
		Store:         store,
		AWSRegion:     os.Getenv("LOCKD_AWS_REGION"),
		AWSKMSKeyID:   os.Getenv("LOCKD_AWS_KMS_KEY_ID"),
		S3SSE:         os.Getenv("LOCKD_S3_SSE"),
		S3KMSKeyID:    os.Getenv("LOCKD_S3_KMS_KEY_ID"),
		S3MaxPartSize: 16 << 20,
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = os.Getenv("AWS_REGION")
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = os.Getenv("AWS_DEFAULT_REGION")
	}

	MaybeEnableStorageEncryption(tb, &cfg)
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("aws config validation failed: %v", err)
	}
	return cfg
}

// EnsureAWSStoreReady runs a quick store verification to catch misconfigured credentials.
func EnsureAWSStoreReady(tb testing.TB, cfg lockd.Config) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		tb.Fatalf("aws store verification failed: %v", err)
	}
	if !res.Passed() {
		tb.Fatalf("aws store verification failed: %+v", res)
	}
}
