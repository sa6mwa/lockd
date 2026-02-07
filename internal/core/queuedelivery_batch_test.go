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
	tests := []struct {
		name     string
		pageSize int
		enqueue  int
		want     int
	}{
		{name: "defaults_to_one_when_zero", pageSize: 0, enqueue: 3, want: 1},
		{name: "respects_requested_batch", pageSize: 3, enqueue: 5, want: 3},
		{name: "caps_at_max_batch", pageSize: maxQueueDequeueBatch + 10, enqueue: maxQueueDequeueBatch + 20, want: maxQueueDequeueBatch},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			coreSvc, qsvc := newQueueCoreForTest(t)
			coreSvc.queueDispatcher = queue.NewDispatcher(qsvc, queue.WithLogger(pslog.NoopLogger()))

			const namespace = "default"
			queueName := "batch-q-" + tc.name
			for i := 0; i < tc.enqueue; i++ {
				if _, err := qsvc.Enqueue(ctx, namespace, queueName, bytes.NewReader([]byte("payload")), queue.EnqueueOptions{}); err != nil {
					t.Fatalf("enqueue %d: %v", i, err)
				}
			}

			res, err := coreSvc.Dequeue(ctx, QueueDequeueCommand{
				Namespace:    namespace,
				Queue:        queueName,
				Owner:        "worker-batch",
				BlockSeconds: apiBlockNoWait,
				PageSize:     tc.pageSize,
			})
			if err != nil {
				t.Fatalf("dequeue: %v", err)
			}
			if got := len(res.Deliveries); got != tc.want {
				t.Fatalf("deliveries=%d want=%d", got, tc.want)
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
		})
	}
}
