//go:build integration && mem && crypto

package memcrypto

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/pslog"
)

func TestCryptoMemLocks(t *testing.T) {
	cfg := buildMemConfig(t)
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(pslog.NoopLogger()),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(20*time.Second),
			lockdclient.WithKeepAliveTimeout(20*time.Second),
			lockdclient.WithCloseTimeout(20*time.Second),
			lockdclient.WithLogger(pslog.NoopLogger()),
		),
	}
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	ts := lockd.StartTestServer(t, options...)
	cli := ts.Client

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sess, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "crypto-mem-lock",
		Owner:      "worker-crypto",
		TTLSeconds: 30,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer sess.Release(context.Background())

	var existing map[string]any
	if err := sess.Load(ctx, &existing); err != nil {
		t.Fatalf("load empty state: %v", err)
	}

	state := map[string]any{"counter": 1, "payload": "crypto"}
	if err := sess.Save(ctx, state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	var loaded map[string]any
	if err := sess.Load(ctx, &loaded); err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if fmt.Sprint(loaded["payload"]) != "crypto" {
		t.Fatalf("unexpected state payload: %+v", loaded)
	}

	if _, err := sess.Remove(ctx); err != nil {
		t.Fatalf("remove state: %v", err)
	}

	if err := sess.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestCryptoMemQueues(t *testing.T) {
	cfg := buildMemConfig(t)
	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestLogger(pslog.NoopLogger()),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(20*time.Second),
			lockdclient.WithKeepAliveTimeout(20*time.Second),
			lockdclient.WithCloseTimeout(20*time.Second),
			lockdclient.WithLogger(pslog.NoopLogger()),
		),
	}
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	ts := lockd.StartTestServer(t, options...)
	cli := ts.Client

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queue := queuetestutil.QueueName("crypto-mem-queue")

	payload := []byte("mem-crypto-message")
	if _, err := cli.EnqueueBytes(ctx, queue, payload, lockdclient.EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	msg := mustDequeueMessage(t, cli, queue, "crypto-worker")
	body := queuetestutil.ReadMessagePayload(t, msg)
	if !bytes.Equal(body, payload) {
		t.Fatalf("dequeue payload mismatch: got %q want %q", body, payload)
	}
	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("ack message: %v", err)
	}

	// Re-enqueue for subscription path.
	subPayload := []byte("mem-crypto-subscribe")
	res := queuetestutil.MustEnqueueBytes(t, cli, queue, subPayload)

	deliveries := make(chan string, 1)
	subCtx, subCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer subCancel()

	err := cli.Subscribe(subCtx, queue, lockdclient.SubscribeOptions{
		Owner:        "crypto-subscriber",
		Prefetch:     1,
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

func buildMemConfig(t testing.TB) lockd.Config {
	t.Helper()
	cfg := lockd.Config{
		Store:       "mem://",
		ListenProto: "tcp",
		Listen:      "127.0.0.1:0",
	}
	cfg.DisableMemQueueWatch = false
	cfg.QueuePollInterval = 100 * time.Millisecond
	cfg.QueuePollJitter = 0
	cfg.QueueResilientPollInterval = time.Second
	cryptotest.MaybeEnableStorageEncryption(t, &cfg)
	return cfg
}

func mustDequeueMessage(t testing.TB, cli *lockdclient.Client, queue string, owner string) *lockdclient.QueueMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
		Owner:        queuetestutil.QueueOwner(owner),
		BlockSeconds: 5,
	})
	if err != nil {
		t.Fatalf("dequeue %s: %v", queue, err)
	}
	if msg == nil {
		t.Fatalf("expected message from %s", queue)
	}
	return msg
}
