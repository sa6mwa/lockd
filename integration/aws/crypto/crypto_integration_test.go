//go:build integration && aws && crypto

package awscrypto

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	awsintegration "pkt.systems/lockd/integration/aws"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/pslog"
)

func TestCryptoAWSLocks(t *testing.T) {
	cfg := buildAWSConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cleanupAWSLock(t, cfg, "crypto-aws-lock")
	ensureStoreReady(t, ctx, cfg)

	cli := startServer(t, cfg)
	t.Cleanup(func() {
		cleanupAWSLock(t, cfg, "crypto-aws-lock")
	})

	sessCtx, sessCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer sessCancel()
	sess, err := cli.Acquire(sessCtx, api.AcquireRequest{
		Key:        "crypto-aws-lock",
		Owner:      "aws-worker",
		TTLSeconds: 45,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer sess.Release(context.Background())

	state := map[string]any{"phase": "start", "count": 1}
	if err := sess.Save(sessCtx, state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	var loaded map[string]any
	if err := sess.Load(sessCtx, &loaded); err != nil {
		t.Fatalf("load state: %v", err)
	}
	if fmt.Sprint(loaded["phase"]) != "start" {
		t.Fatalf("unexpected state: %+v", loaded)
	}

	if _, err := sess.Remove(sessCtx); err != nil {
		t.Fatalf("remove state: %v", err)
	}
	if err := sess.Release(sessCtx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestCryptoAWSQueues(t *testing.T) {
	cfg := buildAWSConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ensureStoreReady(t, ctx, cfg)

	cli := startServer(t, cfg)

	queue := queuetestutil.QueueName("crypto-aws-queue")
	t.Cleanup(func() {
		cleanupAWSQueue(t, cfg, queue)
	})
	payload := []byte("aws-crypto-message")

	if _, err := cli.EnqueueBytes(ctx, queue, payload, lockdclient.EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	msg := mustDequeueMessage(t, cli, queue, "aws-consumer")
	body := queuetestutil.ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("dequeue payload mismatch: got %q want %q", body, payload)
	}
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("ack message: %v", err)
	}

	subPayload := []byte("aws-crypto-subscribe")
	res := queuetestutil.MustEnqueueBytes(t, cli, queue, subPayload)

	deliveries := make(chan string, 1)
	subCtx, subCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer subCancel()

	err := cli.Subscribe(subCtx, queue, lockdclient.SubscribeOptions{
		Owner:        "aws-subscriber",
		Prefetch:     2,
		BlockSeconds: 5,
	}, func(msgCtx context.Context, msg *lockdclient.QueueMessage) error {
		if msg == nil {
			return nil
		}
		defer msg.Close()
		if err := msg.ClosePayload(); err != nil {
			return err
		}
		ackCtx, ackCancel := context.WithTimeout(context.Background(), time.Second)
		err := msg.Ack(ackCtx)
		ackCancel()
		if err != nil {
			return err
		}
		select {
		case deliveries <- msg.MessageID():
		default:
		}
		subCancel()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe: %v", err)
	}

	select {
	case msgID := <-deliveries:
		if msgID != res.MessageID {
			t.Fatalf("subscribe delivered unexpected message id %s want %s", msgID, res.MessageID)
		}
	default:
		t.Fatalf("subscribe completed without delivery")
	}
}

func buildAWSConfig(t testing.TB) lockd.Config {
	t.Helper()
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		t.Skip("LOCKD_STORE not configured for AWS tests")
	}
	if !strings.HasPrefix(store, "aws://") {
		t.Fatalf("LOCKD_STORE must reference an aws:// URI, got %q", store)
	}
	cfg := lockd.Config{
		Store:                      store,
		AWSRegion:                  strings.TrimSpace(os.Getenv("LOCKD_AWS_REGION")),
		AWSKMSKeyID:                strings.TrimSpace(os.Getenv("LOCKD_AWS_KMS_KEY_ID")),
		S3SSE:                      strings.TrimSpace(os.Getenv("LOCKD_S3_SSE")),
		S3KMSKeyID:                 strings.TrimSpace(os.Getenv("LOCKD_S3_KMS_KEY_ID")),
		S3MaxPartSize:              16 << 20,
		QueuePollInterval:          250 * time.Millisecond,
		QueuePollJitter:            0,
		QueueResilientPollInterval: time.Second,
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = strings.TrimSpace(os.Getenv("AWS_REGION"))
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = strings.TrimSpace(os.Getenv("AWS_DEFAULT_REGION"))
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation: %v", err)
	}
	return cfg
}

func ensureStoreReady(t *testing.T, ctx context.Context, cfg lockd.Config) {
	t.Helper()
	awsintegration.ResetAWSBucketForCrypto(t, cfg)
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		t.Fatalf("store verification failed: %+v", res)
	}
}

func cleanupAWSLock(tb testing.TB, cfg lockd.Config, key string) {
	tb.Helper()
	awsCfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(awsCfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup state %s: %v", key, err)
	}
	if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup meta %s: %v", key, err)
	}
}

func cleanupAWSQueue(tb testing.TB, cfg lockd.Config, queue string) {
	tb.Helper()
	awsCfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(awsCfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	client := store.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := strings.Trim(awsCfg.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	prefix = prefix + "q/" + queue + "/"
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	for obj := range client.ListObjects(ctx, awsCfg.Bucket, opts) {
		if obj.Err != nil {
			tb.Fatalf("list queue objects: %v", obj.Err)
		}
		_ = client.RemoveObject(ctx, awsCfg.Bucket, obj.Key, minio.RemoveObjectOptions{})
	}
}

func startServer(t testing.TB, cfg lockd.Config) *lockdclient.Client {
	t.Helper()
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(90*time.Second),
			lockdclient.WithKeepAliveTimeout(90*time.Second),
			lockdclient.WithCloseTimeout(90*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
		),
	}
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	ts := lockd.StartTestServer(t, options...)
	if ts.Client != nil {
		return ts.Client
	}
	cli, err := ts.NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return cli
}

func mustDequeueMessage(t testing.TB, cli *lockdclient.Client, queue, owner string) *lockdclient.QueueMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
		Owner:        queuetestutil.QueueOwner(owner),
		BlockSeconds: 5,
	})
	if err != nil {
		t.Fatalf("dequeue %s: %v", queue, err)
	}
	if msg == nil {
		t.Fatalf("expected message for %s", queue)
	}
	return msg
}
