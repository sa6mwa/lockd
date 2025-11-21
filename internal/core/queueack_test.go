package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage/memory"
)

func newQueueCoreForTest(t *testing.T) (*Service, *queue.Service) {
	t.Helper()
	store := memory.NewWithConfig(memory.Config{QueueWatch: false})
	qsvc, err := queue.New(store, clock.Real{}, queue.Config{})
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	coreSvc := New(Config{
		Store:            store,
		QueueService:     qsvc,
		DefaultNamespace: "default",
		DefaultTTL:       30 * time.Second,
		MaxTTL:           2 * time.Minute,
		Logger:           loggingutil.NoopLogger(),
	})
	return coreSvc, qsvc
}

func TestQueueAckReleasesLease(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q1", strings.NewReader("hi"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Acquire delivery lease.
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:   "default",
		Key:         relativeKey("default", msgLeaseKey(t, "default", "q1", msg.ID)),
		Owner:       "worker",
		TTLSeconds:  30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	doc, metaETag, err := qsvc.GetMessage(ctx, "default", "q1", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	res, err := coreSvc.Ack(ctx, QueueAckCommand{
		Namespace:    "default",
		Queue:        "q1",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		FencingToken: acq.FencingToken,
		Stateful:     false,
	})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !res.Acked {
		t.Fatalf("expected acked")
	}
	meta, _, err := coreSvc.ensureMeta(ctx, "default", msgLeaseKey(t, "default", "q1", doc.ID))
	if err != nil {
		t.Fatalf("ensureMeta: %v", err)
	}
	if meta.Lease != nil {
		t.Fatalf("expected lease cleared after ack")
	}
}

func TestQueueNackClearsLease(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q2", strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:   "default",
		Key:         relativeKey("default", msgLeaseKey(t, "default", "q2", msg.ID)),
		Owner:       "worker",
		TTLSeconds:  30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	doc, metaETag, err := qsvc.GetMessage(ctx, "default", "q2", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	res, err := coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q2",
		MessageID:    doc.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Stateful:     false,
		Delay:        0,
		FencingToken: acq.FencingToken,
	})
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if !res.Requeued {
		t.Fatalf("expected requeued")
	}
	meta, _, err := coreSvc.ensureMeta(ctx, "default", msgLeaseKey(t, "default", "q2", doc.ID))
	if err != nil {
		t.Fatalf("ensureMeta: %v", err)
	}
	if meta.Lease != nil {
		t.Fatalf("expected lease cleared after nack")
	}
}

func TestQueueExtendRenewsLease(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q3", strings.NewReader("x"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:   "default",
		Key:         relativeKey("default", msgLeaseKey(t, "default", "q3", msg.ID)),
		Owner:       "worker",
		TTLSeconds:  5,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	doc, metaETag, err := qsvc.GetMessage(ctx, "default", "q3", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	extension := 10 * time.Second
	res, err := coreSvc.Extend(ctx, QueueExtendCommand{
		Namespace:    "default",
		Queue:        "q3",
		MessageID:    doc.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Visibility:   extension,
		FencingToken: acq.FencingToken,
	})
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	if res.LeaseExpiresAtUnix <= acq.ExpiresAt {
		t.Fatalf("expected lease expiry to increase")
	}
}

// helpers
const apiBlockNoWait = int64(-1)

func msgLeaseKey(t *testing.T, namespace, queueName, id string) string {
	t.Helper()
	key, err := queue.MessageLeaseKey(namespace, queueName, id)
	if err != nil {
		t.Fatalf("MessageLeaseKey: %v", err)
	}
	return key
}
