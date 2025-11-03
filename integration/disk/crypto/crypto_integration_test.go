//go:build integration && disk

package diskcrypto

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/lockd/internal/loggingutil"
)

func TestCryptoDiskLocks(t *testing.T) {
	cfg := buildDiskConfig(t)
	ts := lockd.StartTestServer(t,
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(loggingutil.NoopLogger()),
		lockd.WithTestClientOptions(
			lockdclient.WithDisableMTLS(true),
			lockdclient.WithHTTPTimeout(30*time.Second),
			lockdclient.WithKeepAliveTimeout(30*time.Second),
			lockdclient.WithCloseTimeout(30*time.Second),
			lockdclient.WithLogger(loggingutil.NoopLogger()),
		),
	)
	cli := ts.Client

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sess, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "crypto-disk-lock",
		Owner:      "disk-worker",
		TTLSeconds: 30,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer sess.Release(context.Background())

	state := map[string]any{"stage": "start", "count": 1}
	if err := sess.Save(ctx, state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	var loaded map[string]any
	if err := sess.Load(ctx, &loaded); err != nil {
		t.Fatalf("load state: %v", err)
	}
	if fmt.Sprint(loaded["stage"]) != "start" {
		t.Fatalf("unexpected state: %+v", loaded)
	}

	if _, err := sess.Remove(ctx); err != nil {
		t.Fatalf("remove state: %v", err)
	}
	if err := sess.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestCryptoDiskQueues(t *testing.T) {
	cfg := buildDiskConfig(t)
	ts := lockd.StartTestServer(t,
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(loggingutil.NoopLogger()),
		lockd.WithTestClientOptions(
			lockdclient.WithDisableMTLS(true),
			lockdclient.WithHTTPTimeout(30*time.Second),
			lockdclient.WithKeepAliveTimeout(30*time.Second),
			lockdclient.WithCloseTimeout(30*time.Second),
			lockdclient.WithLogger(loggingutil.NoopLogger()),
		),
	)
	cli := ts.Client

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queue := queuetestutil.QueueName("crypto-disk-queue")
	payload := []byte("disk-crypto-message")

	if _, err := cli.EnqueueBytes(ctx, queue, payload, lockdclient.EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	msg := mustDequeueMessage(t, cli, queue, "disk-consumer")
	body := queuetestutil.ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("dequeue payload mismatch: got %q want %q", body, payload)
	}
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("ack message: %v", err)
	}

	subPayload := []byte("disk-crypto-subscribe")
	res := queuetestutil.MustEnqueueBytes(t, cli, queue, subPayload)

	deliveries := make(chan string, 1)
	subCtx, subCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer subCancel()

	err := cli.Subscribe(subCtx, queue, lockdclient.SubscribeOptions{
		Owner:        "disk-subscriber",
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

func buildDiskConfig(t testing.TB) lockd.Config {
	t.Helper()
	root := t.TempDir()
	cfg := lockd.Config{
		Store:       diskStoreURL(root),
		ListenProto: "tcp",
		Listen:      "127.0.0.1:0",
		DisableMTLS: true,
	}
	cfg.QueuePollInterval = 150 * time.Millisecond
	cfg.QueuePollJitter = 0
	cfg.QueueResilientPollInterval = time.Second
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validation failed: %v", err)
	}
	return cfg
}

func diskStoreURL(root string) string {
	if !filepath.IsAbs(root) {
		root = "/" + root
	}
	return (&url.URL{Scheme: "disk", Path: root}).String()
}

func mustDequeueMessage(t testing.TB, cli *lockdclient.Client, queue, owner string) *lockdclient.QueueMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
		Owner:        queuetestutil.QueueOwner(owner),
		BlockSeconds: 5,
	})
	if err != nil {
		t.Fatalf("dequeue %s: %v", queue, err)
	}
	if msg == nil {
		t.Fatalf("expected message for queue %s", queue)
	}
	return msg
}
