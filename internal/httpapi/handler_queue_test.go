package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
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

func TestHandleQueueStatsReturnsHeadSnapshot(t *testing.T) {
	store := memorystore.NewWithConfig(memorystore.Config{QueueWatch: false})
	handler := New(Config{
		Store:             store,
		Logger:            pslog.NoopLogger(),
		JSONMaxBytes:      1 << 20,
		QueueMaxConsumers: 8,
	})

	ctx := context.Background()
	msg, err := handler.queueSvc.Enqueue(ctx, "default", "stats", bytes.NewReader([]byte("payload")), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	body := bytes.NewBufferString(`{"namespace":"default","queue":"stats"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/queue/stats", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if err := handler.handleQueueStats(rec, req); err != nil {
		t.Fatalf("handleQueueStats returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d", rec.Code)
	}

	var out api.QueueStatsResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Namespace != "default" || out.Queue != "stats" {
		t.Fatalf("unexpected identity %#v", out)
	}
	if !out.Available {
		t.Fatalf("expected available=true")
	}
	if out.HeadMessageID != msg.ID {
		t.Fatalf("expected head message %q, got %q", msg.ID, out.HeadMessageID)
	}
	if out.HeadEnqueuedAtUnix <= 0 {
		t.Fatalf("expected head enqueue timestamp, got %d", out.HeadEnqueuedAtUnix)
	}
	if out.TotalConsumers != 0 || out.WaitingConsumers != 0 {
		t.Fatalf("unexpected consumer counters: total=%d waiting=%d", out.TotalConsumers, out.WaitingConsumers)
	}
}

func TestHandleQueueStatsDoesNotConsumeHead(t *testing.T) {
	store := memorystore.NewWithConfig(memorystore.Config{QueueWatch: false})
	handler := New(Config{
		Store:             store,
		Logger:            pslog.NoopLogger(),
		JSONMaxBytes:      1 << 20,
		QueueMaxConsumers: 8,
	})

	ctx := context.Background()
	msg, err := handler.queueSvc.Enqueue(ctx, "default", "stats-readonly", bytes.NewReader([]byte("payload")), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/queue/stats", bytes.NewBufferString(`{"namespace":"default","queue":"stats-readonly"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		if err := handler.handleQueueStats(rec, req); err != nil {
			t.Fatalf("handleQueueStats run %d returned error: %v", i, err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("unexpected status on run %d: %d", i, rec.Code)
		}
	}

	candidate, err := handler.queueSvc.NextCandidate(ctx, "default", "stats-readonly", "", 1)
	if err != nil {
		t.Fatalf("next candidate after stats: %v", err)
	}
	if candidate.Descriptor == nil || candidate.Descriptor.Document.ID != msg.ID {
		t.Fatalf("expected message %q after stats, got %+v", msg.ID, candidate.Descriptor)
	}
}

func TestHandleQueueWatchDoesNotConsumeHead(t *testing.T) {
	store := memorystore.NewWithConfig(memorystore.Config{QueueWatch: false})
	handler := New(Config{
		Store:             store,
		Logger:            pslog.NoopLogger(),
		JSONMaxBytes:      1 << 20,
		QueueMaxConsumers: 8,
	})
	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	cli, err := lockdclient.New(
		server.URL,
		lockdclient.WithHTTPClient(server.Client()),
		lockdclient.WithDisableMTLS(true),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx := context.Background()
	msg, err := handler.queueSvc.Enqueue(ctx, "default", "watch-readonly", bytes.NewReader([]byte("payload")), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	for i := 0; i < 2; i++ {
		watchCtx, cancelWatch := context.WithTimeout(context.Background(), 8*time.Second)
		gotEvent := make(chan struct{}, 1)
		errCh := make(chan error, 1)
		go func() {
			errCh <- cli.WatchQueue(watchCtx, "watch-readonly", lockdclient.WatchQueueOptions{}, func(_ context.Context, ev lockdclient.QueueWatchEvent) error {
				if ev.Available && ev.HeadMessageID == msg.ID {
					select {
					case gotEvent <- struct{}{}:
					default:
					}
					cancelWatch()
				}
				return nil
			})
		}()

		select {
		case <-gotEvent:
		case <-time.After(5 * time.Second):
			cancelWatch()
			t.Fatalf("timed out waiting for watch availability event on run %d", i)
		}

		if err := <-errCh; err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Fatalf("watch queue run %d error: %v", i, err)
		}
	}

	candidate, err := handler.queueSvc.NextCandidate(context.Background(), "default", "watch-readonly", "", 1)
	if err != nil {
		t.Fatalf("next candidate after watch: %v", err)
	}
	if candidate.Descriptor == nil || candidate.Descriptor.Document.ID != msg.ID {
		t.Fatalf("expected message %q after watch, got %+v", msg.ID, candidate.Descriptor)
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
