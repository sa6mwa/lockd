//go:build integration && minio

package miniointegration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
)

func TestMinioS3Lifecycle(t *testing.T) {
	store := os.Getenv("LOCKD_STORE")
	if store == "" || !strings.HasPrefix(store, "s3://") {
		t.Skip("LOCKD_STORE must reference MinIO s3:// endpoint for integration test")
	}
	cfg := lockd.Config{
		Store:           store,
		S3Region:        os.Getenv("LOCKD_S3_REGION"),
		S3Endpoint:      os.Getenv("LOCKD_S3_ENDPOINT"),
		S3ForcePath:     os.Getenv("LOCKD_S3_PATH_STYLE") == "1",
		S3DisableTLS:    os.Getenv("LOCKD_S3_DISABLE_TLS") == "1",
		S3MaxPartSize:   8 << 20,
		SweeperInterval: time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		t.Fatalf("minio verification failed: %+v", res)
	}
}
