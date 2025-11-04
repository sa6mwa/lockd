//go:build integration && minio

package miniocrypto

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
	"pkt.systems/lockd/integration/internal/cryptotest"
	miniointegration "pkt.systems/lockd/integration/minio"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/pslog"
)

func TestCryptoMinioLocks(t *testing.T) {
	cfg := buildMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ensureStoreReady(t, ctx, cfg)

	cli := startMinioServer(t, cfg)
	t.Cleanup(func() {
		cleanupMinioLock(t, cfg, "crypto-minio-lock")
	})

	sessCtx, sessCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer sessCancel()
	sess, err := cli.Acquire(sessCtx, api.AcquireRequest{
		Key:        "crypto-minio-lock",
		Owner:      "minio-worker",
		TTLSeconds: 30,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer sess.Release(context.Background())

	state := map[string]any{"step": "start", "count": 1}
	if err := sess.Save(sessCtx, state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	var loaded map[string]any
	if err := sess.Load(sessCtx, &loaded); err != nil {
		t.Fatalf("load state: %v", err)
	}
	if fmt.Sprint(loaded["step"]) != "start" {
		t.Fatalf("unexpected state: %+v", loaded)
	}

	if _, err := sess.Remove(sessCtx); err != nil {
		t.Fatalf("remove state: %v", err)
	}
	if err := sess.Release(sessCtx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestCryptoMinioQueues(t *testing.T) {
	cfg := buildMinioConfig(t)
	ensureMinioBucket(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ensureStoreReady(t, ctx, cfg)

	cli := startMinioServer(t, cfg)

	queue := queuetestutil.QueueName("crypto-minio-queue")
	t.Cleanup(func() {
		cleanupMinioQueue(t, cfg, queue)
	})
	payload := []byte("minio-crypto-message")

	if _, err := cli.EnqueueBytes(ctx, queue, payload, lockdclient.EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	msg := mustDequeueMessage(t, cli, queue, "minio-consumer")
	body := queuetestutil.ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("dequeue payload mismatch: got %q want %q", body, payload)
	}
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("ack message: %v", err)
	}

	subPayload := []byte("minio-crypto-subscribe")
	res := queuetestutil.MustEnqueueBytes(t, cli, queue, subPayload)

	deliveries := make(chan string, 1)
	subCtx, subCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer subCancel()

	err := cli.Subscribe(subCtx, queue, lockdclient.SubscribeOptions{
		Owner:        "minio-subscriber",
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

func buildMinioConfig(t testing.TB) lockd.Config {
	t.Helper()
	ensureMinioCredentials(t)
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		t.Skip("LOCKD_STORE not configured for MinIO tests")
	}
	if !strings.HasPrefix(store, "s3://") {
		t.Fatalf("LOCKD_STORE must reference an s3:// URI, got %q", store)
	}

	cfg := lockd.Config{
		Store:                      store,
		S3MaxPartSize:              8 << 20,
		SweeperInterval:            time.Second,
		QueuePollInterval:          200 * time.Millisecond,
		QueuePollJitter:            0,
		QueueResilientPollInterval: time.Second,
	}
	cfg.ListenProto = "tcp"
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:0"
	}
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation: %v", err)
	}
	return cfg
}

func ensureMinioCredentials(tb testing.TB) {
	tb.Helper()
	accessKey := strings.TrimSpace(os.Getenv("LOCKD_S3_ACCESS_KEY_ID"))
	secretKey := strings.TrimSpace(os.Getenv("LOCKD_S3_SECRET_ACCESS_KEY"))
	rootUser := strings.TrimSpace(os.Getenv("LOCKD_S3_ROOT_USER"))
	rootPass := strings.TrimSpace(os.Getenv("LOCKD_S3_ROOT_PASSWORD"))
	if (accessKey == "" || secretKey == "") && (rootUser == "" || rootPass == "") {
		tb.Skip("MinIO credentials not configured")
	}
}

func ensureMinioBucket(tb testing.TB, cfg lockd.Config) {
	tb.Helper()
	minioCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new s3 store: %v", err)
	}
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

func cleanupMinioLock(tb testing.TB, cfg lockd.Config, key string) {
	tb.Helper()
	minioCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new s3 store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.Remove(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup state %s: %v", key, err)
	}
	if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup meta %s: %v", key, err)
	}
}

func cleanupMinioQueue(tb testing.TB, cfg lockd.Config, queue string) {
	tb.Helper()
	minioCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new s3 store: %v", err)
	}
	client := store.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	prefix := strings.Trim(minioCfg.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	prefix = prefix + "q/" + queue + "/"
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	for obj := range client.ListObjects(ctx, minioCfg.Bucket, opts) {
		if obj.Err != nil {
			tb.Fatalf("list queue objects: %v", obj.Err)
		}
		_ = client.RemoveObject(ctx, minioCfg.Bucket, obj.Key, minio.RemoveObjectOptions{})
	}
}

func ensureStoreReady(tb testing.TB, ctx context.Context, cfg lockd.Config) {
	tb.Helper()
	miniointegration.WithMinioStorageLock(tb, func() {
		miniointegration.ResetMinioBucketForCrypto(tb, cfg)
		res, err := storagecheck.VerifyStore(ctx, cfg)
		if err != nil {
			tb.Fatalf("verify store: %v", err)
		}
		if !res.Passed() {
			tb.Fatalf("store verification failed: %+v", res)
		}
	})
}

func startMinioServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(45*time.Second),
			lockdclient.WithKeepAliveTimeout(45*time.Second),
			lockdclient.WithCloseTimeout(45*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
	}
	options = append(options, cryptotest.SharedMTLSOptions(tb)...)
	ts := lockd.StartTestServer(tb, options...)
	if ts.Client != nil {
		return ts.Client
	}
	cli, err := ts.NewClient()
	if err != nil {
		tb.Fatalf("new client: %v", err)
	}
	return cli
}

func mustDequeueMessage(t testing.TB, cli *lockdclient.Client, queue, owner string) *lockdclient.QueueMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
