//go:build integration && azure && crypto

package azurecrypto

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	azurestore "pkt.systems/lockd/internal/storage/azure"
	"pkt.systems/pslog"
)

func TestCryptoAzureLocks(t *testing.T) {
	cfg := buildAzureConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cleanupAzureKeys(t, cfg, "crypto-azure", "azure-")
	ensureStoreReady(t, ctx, cfg)

	cli := startServer(t, cfg)
	t.Cleanup(func() {
		cleanupAzureKeys(t, cfg, "crypto-azure")
	})

	sessCtx, sessCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer sessCancel()
	sess, err := cli.Acquire(sessCtx, api.AcquireRequest{
		Key:        "crypto-azure-lock",
		Owner:      "azure-worker",
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

func TestCryptoAzureQueues(t *testing.T) {
	cfg := buildAzureConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ensureStoreReady(t, ctx, cfg)

	cli := startServer(t, cfg)

	queue := queuetestutil.QueueName("crypto-azure-queue")
	t.Cleanup(func() {
		cleanupAzureQueue(t, cfg, queue)
	})
	payload := []byte("azure-crypto-message")

	if _, err := cli.EnqueueBytes(ctx, queue, payload, lockdclient.EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	msg := mustDequeueMessage(t, cli, queue, "azure-consumer")
	body := queuetestutil.ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("dequeue payload mismatch: got %q want %q", body, payload)
	}
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("ack message: %v", err)
	}

	subPayload := []byte("azure-crypto-subscribe")
	res := queuetestutil.MustEnqueueBytes(t, cli, queue, subPayload)

	deliveries := make(chan string, 1)
	subCtx, subCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer subCancel()

	err := cli.Subscribe(subCtx, queue, lockdclient.SubscribeOptions{
		Owner:        "azure-subscriber",
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

func buildAzureConfig(t testing.TB) lockd.Config {
	t.Helper()
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		t.Skip("LOCKD_STORE not configured for Azure tests")
	}
	if !strings.HasPrefix(store, "azure://") {
		t.Fatalf("LOCKD_STORE must reference an azure:// URI, got %q", store)
	}

	cfg := lockd.Config{
		Store:                      store,
		AzureAccount:               strings.TrimSpace(os.Getenv("LOCKD_AZURE_ACCOUNT")),
		AzureAccountKey:            strings.TrimSpace(os.Getenv("LOCKD_AZURE_ACCOUNT_KEY")),
		AzureSASToken:              strings.TrimSpace(os.Getenv("LOCKD_AZURE_SAS_TOKEN")),
		QueuePollInterval:          250 * time.Millisecond,
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

func ensureStoreReady(t *testing.T, ctx context.Context, cfg lockd.Config) {
	t.Helper()
	res, err := storagecheck.VerifyStore(ctx, cfg)
	if err != nil {
		t.Fatalf("verify store: %v", err)
	}
	if !res.Passed() {
		t.Fatalf("store verification failed: %+v", res)
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

func cleanupAzureKeys(tb testing.TB, cfg lockd.Config, prefixes ...string) {
	tb.Helper()
	if len(prefixes) == 0 {
		return
	}
	azureCfg, err := lockd.BuildAzureConfig(cfg)
	if err != nil {
		tb.Fatalf("build azure config: %v", err)
	}
	store, err := azurestore.New(azureCfg)
	if err != nil {
		tb.Fatalf("new azure store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	keys, err := store.ListMetaKeys(ctx)
	if err != nil {
		tb.Fatalf("list meta: %v", err)
	}
	for _, key := range keys {
		for _, prefix := range prefixes {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if err := store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Fatalf("cleanup azure state %s: %v", key, err)
			}
			if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Fatalf("cleanup azure meta %s: %v", key, err)
			}
			break
		}
	}
}

func cleanupAzureQueue(tb testing.TB, cfg lockd.Config, queue string) {
	tb.Helper()
	azureCfg, err := lockd.BuildAzureConfig(cfg)
	if err != nil {
		tb.Fatalf("build azure config: %v", err)
	}
	store, err := azurestore.New(azureCfg)
	if err != nil {
		tb.Fatalf("new azure store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("q/%s/", queue)
	objs, err := store.ListObjects(ctx, storage.ListOptions{Prefix: prefix, Limit: 1000})
	if err != nil {
		tb.Fatalf("list azure queue objects: %v", err)
	}
	for _, obj := range objs.Objects {
		if err := store.DeleteObject(ctx, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Fatalf("delete azure queue object %s: %v", obj.Key, err)
		}
	}
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
