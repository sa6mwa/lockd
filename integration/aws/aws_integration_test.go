//go:build integration && aws

package awsintegration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
)

func TestAWSStoreVerification(t *testing.T) {
	store := os.Getenv("LOCKD_STORE")
	if store == "" || !strings.HasPrefix(store, "s3://") {
		t.Skip("LOCKD_STORE must reference an s3:// URI for AWS integration test")
	}
	cfg := lockd.Config{
		Store:           store,
		S3Region:        os.Getenv("LOCKD_S3_REGION"),
		S3Endpoint:      os.Getenv("LOCKD_S3_ENDPOINT"),
		S3SSE:           os.Getenv("LOCKD_S3_SSE"),
		S3KMSKeyID:      os.Getenv("LOCKD_S3_KMS_KEY_ID"),
		S3ForcePath:     os.Getenv("LOCKD_S3_PATH_STYLE") == "1",
		S3DisableTLS:    os.Getenv("LOCKD_S3_DISABLE_TLS") == "1",
		S3MaxPartSize:   16 << 20,
		SweeperInterval: time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation: %v", err)
	}
	res, err := storagecheck.VerifyStore(context.Background(), cfg)
	if err != nil {
		t.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		t.Fatalf("aws verification failed: %+v", res)
	}
}
