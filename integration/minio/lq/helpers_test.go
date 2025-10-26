//go:build integration && minio && lq

package miniointegration

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/logport"
)

type minioQueueOptions struct {
	PollInterval      time.Duration
	PollJitter        time.Duration
	ResilientInterval time.Duration
}

func prepareMinioQueueConfig(t testing.TB, opts minioQueueOptions) lockd.Config {
	t.Helper()

	cfg := loadMinioQueueConfig(t)
	ensureMinioQueueBucket(t, cfg)
	ensureMinioQueueReady(t, context.Background(), cfg)

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
	cfg.QRFQueueSoftLimit = 0
	cfg.QRFQueueHardLimit = 0
	cfg.QRFLockSoftLimit = 0
	cfg.QRFLockHardLimit = 0
	cfg.QRFMemorySoftLimitPercent = 0
	cfg.QRFMemoryHardLimitPercent = 0
	cfg.QRFMemorySoftLimitBytes = 0
	cfg.QRFMemoryHardLimitBytes = 0
	cfg.QRFLoadSoftLimitMultiplier = lockd.DefaultQRFLoadSoftLimitMultiplier
	cfg.QRFLoadHardLimitMultiplier = lockd.DefaultQRFLoadHardLimitMultiplier

	cfg.DisableMTLS = true
	cfg.ListenProto = "tcp"
	cfg.Listen = "127.0.0.1:0"

	// Ensure no lingering environment flags accidentally enable watch mode.
	_ = os.Unsetenv("LOCKD_DISK_QUEUE_WATCH")

	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("minio queue config validation failed: %v", err)
	}
	return cfg
}

func startMinioQueueServer(t testing.TB, cfg lockd.Config) *lockd.TestServer {
	t.Helper()
	return startMinioQueueServerWithLogger(t, cfg, lockd.NewTestingLogger(t, logport.TraceLevel))
}

func startMinioQueueServerWithCapture(t testing.TB, cfg lockd.Config) (*lockd.TestServer, *queuetestutil.LogCapture) {
	t.Helper()
	capture := queuetestutil.NewLogCapture(t)
	ts := startMinioQueueServerWithLogger(t, cfg, capture.Logger())
	return ts, capture
}

func startMinioQueueServerWithLogger(t testing.TB, cfg lockd.Config, logger logport.ForLogging) *lockd.TestServer {
	t.Helper()
	clientLogger := lockd.NewTestingLogger(t, logport.TraceLevel)
	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithLogger(clientLogger),
	}
	return lockd.StartTestServer(t,
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(logger),
		lockd.WithTestClientOptions(clientOpts...),
		lockd.WithTestStartTimeout(20*time.Second),
	)
}

func ensureMinioQueueEnv(t testing.TB) {
	accessKey := strings.TrimSpace(os.Getenv("LOCKD_S3_ACCESS_KEY_ID"))
	secretKey := strings.TrimSpace(os.Getenv("LOCKD_S3_SECRET_ACCESS_KEY"))
	rootUser := strings.TrimSpace(os.Getenv("LOCKD_S3_ROOT_USER"))
	rootPass := strings.TrimSpace(os.Getenv("LOCKD_S3_ROOT_PASSWORD"))
	if (accessKey == "" || secretKey == "") && (rootUser == "" || rootPass == "") {
		t.Fatalf("MinIO queue integration tests require either LOCKD_S3_ACCESS_KEY_ID/LOCKD_S3_SECRET_ACCESS_KEY or LOCKD_S3_ROOT_USER/LOCKD_S3_ROOT_PASSWORD to be set")
	}
}

func loadMinioQueueConfig(t testing.TB) lockd.Config {
	ensureMinioQueueEnv(t)
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		t.Fatalf("LOCKD_STORE must be set to an s3:// URI for MinIO queue integration tests")
	}
	if !strings.HasPrefix(store, "s3://") {
		t.Fatalf("LOCKD_STORE must reference an s3:// URI for MinIO integration tests, got %q", store)
	}

	cfg := lockd.Config{
		Store:           store,
		S3MaxPartSize:   8 << 20,
		SweeperInterval: time.Second,
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("minio queue config validation: %v", err)
	}
	return cfg
}

func ensureMinioQueueBucket(t testing.TB, cfg lockd.Config) {
	minioCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		t.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		t.Fatalf("create minio store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exists, err := store.Client().BucketExists(ctx, minioCfg.Bucket)
	if err != nil {
		t.Fatalf("bucket exists: %v", err)
	}
	if !exists {
		if err := store.Client().MakeBucket(ctx, minioCfg.Bucket, minio.MakeBucketOptions{Region: minioCfg.Region}); err != nil {
			t.Fatalf("make bucket: %v", err)
		}
	}
}

func ensureMinioQueueReady(t testing.TB, ctx context.Context, cfg lockd.Config) {
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		t.Fatalf("store verification failed: %+v", res)
	}
}
