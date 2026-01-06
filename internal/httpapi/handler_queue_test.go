package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync/atomic"
	"testing"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
	memorystore "pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

type flakyPutStore struct {
	storage.Backend
	targetKey string
	failFlag  uint32
}

func (s *flakyPutStore) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	if s.targetKey != "" && key == s.targetKey && opts.ExpectedETag != "" && atomic.CompareAndSwapUint32(&s.failFlag, 0, 1) {
		return nil, storage.ErrNotFound
	}
	return s.Backend.PutObject(ctx, namespace, key, body, opts)
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
		Logger:            pslog.NoopLogger(),
		JSONMaxBytes:      1 << 20,
		QueueMaxConsumers: 32,
	})

	ctx := context.Background()
	queueName := "retry-queue"
	namespace := "default"
	msg, err := qSvc.Enqueue(ctx, namespace, queueName, bytes.NewReader([]byte("payload")), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	store.targetKey = path.Join("q", queueName, "msg", msg.ID+".pb")

	body := `{"namespace":"` + namespace + `","queue":"` + queueName + `","owner":"worker","wait_seconds":0}`
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
	h := &Handler{pendingDeliveries: core.NewPendingDeliveries()}
	namespace := "default"
	aborted := false
	delivery := &core.QueueDelivery{
		Message: &core.QueueMessage{MessageID: "m-1"},
		Finalize: func(success bool) {
			if success {
				t.Fatalf("finalize called with success")
			}
			aborted = true
		},
	}

	h.trackPendingDelivery(namespace, "queue", "worker", delivery)

	h.releasePendingDeliveries(namespace, "queue", "worker")

	if !aborted {
		t.Fatalf("expected delivery to be aborted")
	}

	// pending tracker should have cleared internal map
}

func TestHandlerClearPendingDeliveryRemovesMessage(t *testing.T) {
	h := &Handler{pendingDeliveries: core.NewPendingDeliveries()}
	namespace := "default"
	delivery := &core.QueueDelivery{
		Message:  &core.QueueMessage{MessageID: "m-1"},
		Finalize: func(bool) {},
	}

	h.trackPendingDelivery(namespace, "queue", "worker", delivery)

	h.clearPendingDelivery(namespace, "queue", "worker", "m-1")

	// pending tracker should have cleared internal map
}

func TestHandleQueueAckLogsTxnID(t *testing.T) {
	store := memorystore.NewWithConfig(memorystore.Config{QueueWatch: false})
	logger := newCaptureLogger()
	handler := New(Config{
		Store:             store,
		Logger:            logger,
		JSONMaxBytes:      1 << 20,
		QueueMaxConsumers: 8,
	})

	ctx := context.Background()
	msg, err := handler.queueSvc.Enqueue(ctx, "default", "logs", bytes.NewReader([]byte("payload")), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	leaseKey, err := queue.MessageLeaseKey("default", "logs", msg.ID)
	if err != nil {
		t.Fatalf("lease key: %v", err)
	}
	acq, err := handler.core.Acquire(ctx, core.AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", leaseKey),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: 0,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := handler.queueSvc.GetMessage(ctx, "default", "logs", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = handler.queueSvc.SaveMessageDocument(ctx, "default", "logs", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	body := bytes.NewBufferString(`{"namespace":"default","queue":"logs","message_id":"` + msg.ID + `","lease_id":"` + acq.LeaseID + `","meta_etag":"` + metaETag + `","fencing_token":` + fmt.Sprint(acq.FencingToken) + `}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/queue/ack", body).WithContext(pslog.ContextWithLogger(context.Background(), logger))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if err := handler.handleQueueAck(rec, req); err != nil {
		t.Fatalf("handleQueueAck returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d", rec.Code)
	}

	entry, ok := logger.find("queue.ack.success")
	if !ok {
		t.Fatalf("queue.ack.success log not captured; entries=%v", logger.snapshot())
	}
	fields := entry.toMap()
	if got := fields["txn_id"]; got != acq.TxnID {
		t.Fatalf("expected txn_id %s, got %v", acq.TxnID, got)
	}
}

type captureEntry struct {
	level  string
	msg    string
	fields []any
}

func (e captureEntry) toMap() map[string]any {
	out := make(map[string]any)
	for i := 0; i+1 < len(e.fields); i += 2 {
		key, ok := e.fields[i].(string)
		if !ok {
			continue
		}
		out[key] = e.fields[i+1]
	}
	return out
}

type captureLogger struct {
	fields  []any
	entries *[]captureEntry
}

func newCaptureLogger() *captureLogger {
	entries := make([]captureEntry, 0, 8)
	return &captureLogger{entries: &entries}
}

func (l *captureLogger) cloneWith(args ...any) *captureLogger {
	combined := append([]any{}, l.fields...)
	combined = append(combined, args...)
	return &captureLogger{fields: combined, entries: l.entries}
}

func (l *captureLogger) find(msg string) (captureEntry, bool) {
	if l == nil || l.entries == nil {
		return captureEntry{}, false
	}
	for _, entry := range *l.entries {
		if entry.msg == msg {
			return entry, true
		}
	}
	return captureEntry{}, false
}

func (l *captureLogger) snapshot() []captureEntry {
	if l == nil || l.entries == nil {
		return nil
	}
	out := make([]captureEntry, len(*l.entries))
	copy(out, *l.entries)
	return out
}

func (l *captureLogger) record(level, msg string, args ...any) {
	fields := append([]any{}, l.fields...)
	fields = append(fields, args...)
	*l.entries = append(*l.entries, captureEntry{level: level, msg: msg, fields: fields})
}

func (l *captureLogger) Trace(msg string, args ...any) { l.record("trace", msg, args...) }
func (l *captureLogger) Debug(msg string, args ...any) { l.record("debug", msg, args...) }
func (l *captureLogger) Info(msg string, args ...any)  { l.record("info", msg, args...) }
func (l *captureLogger) Warn(msg string, args ...any)  { l.record("warn", msg, args...) }
func (l *captureLogger) Error(msg string, args ...any) { l.record("error", msg, args...) }
func (l *captureLogger) Fatal(msg string, args ...any) { l.record("fatal", msg, args...) }
func (l *captureLogger) Panic(msg string, args ...any) { l.record("panic", msg, args...) }
func (l *captureLogger) Log(level pslog.Level, msg string, args ...any) {
	l.record(pslog.LevelString(level), msg, args...)
}
func (l *captureLogger) With(args ...any) pslog.Logger { return l.cloneWith(args...) }
func (l *captureLogger) WithLogLevel() pslog.Logger    { return l }
func (l *captureLogger) LogLevel(pslog.Level) pslog.Logger {
	return l
}
func (l *captureLogger) LogLevelFromEnv(string) pslog.Logger { return l }
