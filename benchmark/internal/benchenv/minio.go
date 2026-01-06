package benchenv

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage/s3"
)

var (
	minioVerifyOnce sync.Once
	minioVerifyErr  error
)

// EnsureMinioCredentials validates that MinIO credentials are present.
func EnsureMinioCredentials(tb testing.TB) {
	tb.Helper()
	accessKey := strings.TrimSpace(os.Getenv("LOCKD_S3_ACCESS_KEY_ID"))
	secretKey := strings.TrimSpace(os.Getenv("LOCKD_S3_SECRET_ACCESS_KEY"))
	rootUser := strings.TrimSpace(os.Getenv("LOCKD_S3_ROOT_USER"))
	rootPass := strings.TrimSpace(os.Getenv("LOCKD_S3_ROOT_PASSWORD"))
	if (accessKey == "" || secretKey == "") && (rootUser == "" || rootPass == "") {
		tb.Fatalf("MinIO benchmarks require either LOCKD_S3_ACCESS_KEY_ID/LOCKD_S3_SECRET_ACCESS_KEY or LOCKD_S3_ROOT_USER/LOCKD_S3_ROOT_PASSWORD to be set")
	}
}

// LoadMinioConfig builds the MinIO config used by benchmarks.
func LoadMinioConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	EnsureMinioCredentials(tb)
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		tb.Fatalf("LOCKD_STORE must be set to an s3:// URI for MinIO benchmarks")
	}
	if !strings.HasPrefix(store, "s3://") {
		tb.Fatalf("LOCKD_STORE must reference an s3:// URI for MinIO benchmarks, got %q", store)
	}
	store = withBenchmarkPrefix(tb, store)
	cfg := lockd.Config{
		Store:           store,
		S3MaxPartSize:   8 << 20,
		SweeperInterval: time.Second,
	}
	MaybeEnableStorageEncryption(tb, &cfg)
	return cfg
}

func withBenchmarkPrefix(tb testing.TB, store string) string {
	tb.Helper()
	u, err := url.Parse(store)
	if err != nil {
		tb.Fatalf("parse LOCKD_STORE: %v", err)
	}
	path := strings.Trim(strings.TrimPrefix(u.Path, "/"), "/")
	if path == "" {
		return store
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
		return store
	}
	prefix := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	u.Path = "/" + parts[0] + "/" + prefix
	return u.String()
}

// EnsureMinioBucket makes sure the configured bucket exists.
func EnsureMinioBucket(tb testing.TB, cfg lockd.Config) {
	tb.Helper()
	minioResult, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	minioCfg := minioResult.Config
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new minio store: %v", err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exists, err := store.Client().BucketExists(ctx, minioCfg.Bucket)
	if err != nil {
		tb.Fatalf("bucket exists: %v", err)
	}
	if !exists {
		if err := store.Client().MakeBucket(ctx, minioCfg.Bucket, minio.MakeBucketOptions{Region: minioCfg.Region}); err != nil {
			tb.Fatalf("make bucket: %v", err)
		}
	}
}

// EnsureS3StoreReady verifies the MinIO store once per process.
func EnsureS3StoreReady(tb testing.TB, cfg lockd.Config) {
	tb.Helper()
	minioVerifyOnce.Do(func() {
		res, err := storagecheck.VerifyStore(context.Background(), cfg)
		if err != nil {
			minioVerifyErr = err
			return
		}
		if !res.Passed() {
			minioVerifyErr = errStoreVerificationFailed{res: res}
		}
	})
	if minioVerifyErr != nil {
		tb.Fatalf("store verification failed: %v", minioVerifyErr)
	}
}

type errStoreVerificationFailed struct {
	res storagecheck.Result
}

func (e errStoreVerificationFailed) Error() string {
	if len(e.res.Checks) == 0 {
		return "store verification failed"
	}
	var failures []string
	for _, check := range e.res.Checks {
		if check.Err == nil {
			continue
		}
		failures = append(failures, fmt.Sprintf("%s=%v", check.Name, check.Err))
	}
	if len(failures) == 0 {
		return "store verification failed"
	}
	return "store verification failed: " + strings.Join(failures, "; ")
}
