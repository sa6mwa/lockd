package httpapi

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync/atomic"
	"testing"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
	memorystore "pkt.systems/lockd/internal/storage/memory"
)

type flakyPutStore struct {
	storage.Backend
	targetKey string
	failFlag  uint32
}

func (s *flakyPutStore) PutObject(ctx context.Context, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	if s.targetKey != "" && key == s.targetKey && opts.ExpectedETag != "" && atomic.CompareAndSwapUint32(&s.failFlag, 0, 1) {
		return nil, storage.ErrNotFound
	}
	return s.Backend.PutObject(ctx, key, body, opts)
}

func TestHandleQueueDequeueRetriesOnMissingMetaDuringIncrement(t *testing.T) {
	baseStore := memorystore.New()
	store := &flakyPutStore{Backend: baseStore}

	qSvc, err := queue.New(store, clock.Real{}, queue.Config{})
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}

	handler := New(Config{
		Store:             store,
		QueueService:      qSvc,
		Logger:            loggingutil.NoopLogger(),
		JSONMaxBytes:      1 << 20,
		QueueMaxConsumers: 32,
	})

	ctx := context.Background()
	queueName := "retry-queue"
	msg, err := qSvc.Enqueue(ctx, queueName, bytes.NewReader([]byte("payload")), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	store.targetKey = path.Join("q", queueName, "msg", msg.ID+".pb")

	body := `{"queue":"` + queueName + `","owner":"worker","wait_seconds":0}`
	req := httptest.NewRequest(http.MethodPost, "/v1/queue/dequeue", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if err := handler.handleQueueDequeue(rec, req); err != nil {
		t.Fatalf("handleQueueDequeue error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
	if got := atomic.LoadUint32(&store.failFlag); got != 1 {
		t.Fatalf("expected exactly one simulated failure, got %d", got)
	}
}

func TestHandlerReleasePendingDeliveriesAbortsAndCleans(t *testing.T) {
	h := &Handler{}
	delivery := &queueDelivery{
		handler: h,
		message: &api.Message{MessageID: "m-1"},
	}

	aborted := false
	delivery.finalizeMu.Lock()
	delivery.finalize = func(success bool) {
		if success {
			t.Fatalf("finalize called with success")
		}
		aborted = true
	}
	delivery.finalizeMu.Unlock()

	h.trackPendingDelivery("queue", "worker", delivery)

	h.releasePendingDeliveries("queue", "worker")

	if !aborted {
		t.Fatalf("expected delivery to be aborted")
	}

	if _, ok := h.pendingDeliveries.Load("queue"); ok {
		t.Fatalf("expected queue entry to be cleared")
	}
}

func TestHandlerClearPendingDeliveryRemovesMessage(t *testing.T) {
	h := &Handler{}
	delivery := &queueDelivery{
		handler: h,
		message: &api.Message{MessageID: "m-1"},
	}

	h.trackPendingDelivery("queue", "worker", delivery)

	h.clearPendingDelivery("queue", "worker", "m-1")

	if _, ok := h.pendingDeliveries.Load("queue"); ok {
		t.Fatalf("expected queue entry to be removed after clear")
	}
}
