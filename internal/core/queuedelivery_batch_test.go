package core

import (
	"bytes"
	"context"
	"io"
	"testing"

	"pkt.systems/lockd/internal/queue"
	"pkt.systems/pslog"
)

func TestDequeueRespectsBatchPageSize(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)
	coreSvc.queueDispatcher = queue.NewDispatcher(qsvc, queue.WithLogger(pslog.NoopLogger()))

	const (
		namespace = "default"
		queueName = "batch-q"
	)
	for i := 0; i < 3; i++ {
		if _, err := qsvc.Enqueue(ctx, namespace, queueName, bytes.NewReader([]byte("payload")), queue.EnqueueOptions{}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	res, err := coreSvc.Dequeue(ctx, QueueDequeueCommand{
		Namespace:    namespace,
		Queue:        queueName,
		Owner:        "worker-batch",
		BlockSeconds: apiBlockNoWait,
		PageSize:     3,
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got, want := len(res.Deliveries), 3; got != want {
		t.Fatalf("expected %d deliveries, got %d", want, got)
	}
	for i, delivery := range res.Deliveries {
		if delivery == nil || delivery.Message == nil {
			t.Fatalf("delivery %d missing payload/message", i)
		}
		if delivery.Payload != nil {
			if _, err := io.Copy(io.Discard, delivery.Payload); err != nil {
				t.Fatalf("drain payload %d: %v", i, err)
			}
			_ = delivery.Payload.Close()
		}
		if delivery.Finalize != nil {
			delivery.Finalize(false)
		}
	}
}
