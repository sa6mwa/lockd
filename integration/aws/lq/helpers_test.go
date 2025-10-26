//go:build integration && aws && lq

package awsintegration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/logport"
)

type awsQueueOptions struct {
	PollInterval      time.Duration
	PollJitter        time.Duration
	ResilientInterval time.Duration
	SweeperInterval   time.Duration
}

var (
	awsQueueStoreVerifyOnce sync.Once
	awsQueueStoreVerifyErr  error
)

func prepareAWSQueueConfig(t testing.TB, opts awsQueueOptions) lockd.Config {
	cfg := loadAWSQueueConfig(t)
	cfg.ListenProto = "tcp"
	cfg.Listen = "127.0.0.1:0"
	ensureAWSQueueReady(t, context.Background(), cfg)

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
	if opts.SweeperInterval > 0 {
		cfg.SweeperInterval = opts.SweeperInterval
	}

	cfg.QRFEnabled = false

	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("aws queue config validation failed: %v", err)
	}
	return cfg
}

func startAWSQueueServer(t testing.TB, cfg lockd.Config) *lockd.TestServer {
	return newAWSQueueTestServer(t, cfg, lockd.NewTestingLogger(t, logport.TraceLevel))
}

func startAWSQueueServerWithCapture(t testing.TB, cfg lockd.Config) (*lockd.TestServer, *queuetestutil.LogCapture) {
	capture := queuetestutil.NewLogCapture(t)
	ts := newAWSQueueTestServer(t, cfg, capture.Logger())
	return ts, capture
}

func newAWSQueueTestServer(t testing.TB, cfg lockd.Config, serverLogger logport.ForLogging, opts ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()

	if serverLogger == nil {
		serverLogger = lockd.NewTestingLogger(t, logport.TraceLevel)
	}
	clientLogger := lockd.NewTestingLogger(t, logport.TraceLevel)
	baseClientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithLogger(clientLogger),
	}

	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(serverLogger),
		lockd.WithTestClientOptions(baseClientOpts...),
		lockd.WithTestStartTimeout(30 * time.Second),
	}
	options = append(options, opts...)
	return lockd.StartTestServer(t, options...)
}

func loadAWSQueueConfig(t testing.TB) lockd.Config {
	ensureAWSQueueEnv(t)
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		t.Fatalf("LOCKD_STORE must be set to an aws:// URI (see .env.aws)")
	}
	if !strings.HasPrefix(store, "aws://") {
		t.Fatalf("LOCKD_STORE must reference an aws:// URI, got %q", store)
	}
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
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("aws queue config validation: %v", err)
	}
	return cfg
}

func ensureAWSQueueReady(t testing.TB, ctx context.Context, cfg lockd.Config) {
	resetAWSBucketForCrypto(t, cfg)
	awsQueueStoreVerifyOnce.Do(func() {
		res, err := storagecheck.VerifyStore(ctx, cfg)
		if err != nil {
			awsQueueStoreVerifyErr = err
			return
		}
		if !res.Passed() {
			awsQueueStoreVerifyErr = fmt.Errorf("store verification failed: %+v", res)
		}
	})
	if awsQueueStoreVerifyErr != nil {
		t.Fatalf("store verification failed: %v", awsQueueStoreVerifyErr)
	}
	ensureAWSQueuePrefixWritable(t, cfg)
}

func ensureAWSQueueEnv(t testing.TB) {
	if _, ok := os.LookupEnv("LOCKD_S3_ACCESS_KEY_ID"); !ok {
		if v := os.Getenv("AWS_ACCESS_KEY_ID"); v != "" {
			t.Setenv("LOCKD_S3_ACCESS_KEY_ID", v)
		}
	}
	if _, ok := os.LookupEnv("LOCKD_S3_SECRET_ACCESS_KEY"); !ok {
		if v := os.Getenv("AWS_SECRET_ACCESS_KEY"); v != "" {
			t.Setenv("LOCKD_S3_SECRET_ACCESS_KEY", v)
		}
	}
}

func ensureAWSQueuePrefixWritable(t testing.TB, cfg lockd.Config) {
	awsCfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		t.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(awsCfg)
	if err != nil {
		t.Fatalf("create s3 store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := "q/" + uuidv7.NewString() + "/msg/bootstrap.bin"
	data := []byte("probe")
	_, err = store.Client().PutObject(ctx, awsCfg.Bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("put object probe failed (check bucket permissions for queue namespace): %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = store.Client().RemoveObject(ctx, awsCfg.Bucket, key, minio.RemoveObjectOptions{})
	}()
}

func ensureAWSQueueWritableOrSkip(t *testing.T, cli *lockdclient.Client) {
	queue := queuetestutil.QueueName("aws-permission-probe")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := cli.Enqueue(ctx, queue, bytes.NewReader([]byte("probe")), lockdclient.EnqueueOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("aws enqueue probe failed (expected write access to queue namespace): %v", err)
	}
	owner := queuetestutil.QueueOwner("aws-permission-owner")
	msg := queuetestutil.MustDequeueMessage(t, cli, queue, owner, 5, 30*time.Second)
	_ = queuetestutil.ReadMessagePayload(t, msg)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := msg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("ack probe message: %v", err)
	}
	ackCancel()
}
