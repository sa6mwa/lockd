package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/api"
)

type closeTrackerReadCloser struct {
	reader io.Reader
	closed int
}

func (c *closeTrackerReadCloser) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *closeTrackerReadCloser) Close() error {
	c.closed++
	return nil
}

func TestQueueMessageWritePayloadToClosesPayload(t *testing.T) {
	payload := []byte("hello")
	tracker := &closeTrackerReadCloser{reader: bytes.NewReader(payload)}
	handle := &QueueMessageHandle{payloadStream: tracker}
	msg := newQueueMessage(handle, nil, 0)

	var out bytes.Buffer
	n, err := msg.WritePayloadTo(&out)
	if err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("expected %d bytes written, got %d", len(payload), n)
	}
	if out.String() != "hello" {
		t.Fatalf("unexpected payload %q", out.String())
	}
	if tracker.closed < 1 {
		t.Fatalf("expected payload close to be called, got %d", tracker.closed)
	}
	if !handle.payloadClosed {
		t.Fatalf("expected handle payload to be marked closed")
	}
}

func TestQueueMessageDecodePayloadJSONClosesPayload(t *testing.T) {
	tracker := &closeTrackerReadCloser{reader: bytes.NewReader([]byte(`{"status":"ok","count":3}`))}
	handle := &QueueMessageHandle{payloadStream: tracker}
	msg := newQueueMessage(handle, nil, 0)

	var decoded struct {
		Status string `json:"status"`
		Count  int    `json:"count"`
	}
	if err := msg.DecodePayloadJSON(&decoded); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if decoded.Status != "ok" || decoded.Count != 3 {
		t.Fatalf("unexpected decoded payload: %+v", decoded)
	}
	if tracker.closed < 1 {
		t.Fatalf("expected payload close to be called, got %d", tracker.closed)
	}
	if !handle.payloadClosed {
		t.Fatalf("expected handle payload to be marked closed")
	}
}

func TestQueueMessageWritePayloadToNil(t *testing.T) {
	var msg *QueueMessage
	if _, err := msg.WritePayloadTo(io.Discard); err == nil {
		t.Fatalf("expected nil queue message error")
	}
}

func TestQueueMessageDecodePayloadJSONNil(t *testing.T) {
	var msg *QueueMessage
	var target map[string]any
	if err := msg.DecodePayloadJSON(&target); err == nil {
		t.Fatalf("expected nil queue message error")
	}
}

func TestFinalizeManagedConsumerMessageBoundsSettlementAfterCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		defer close(exited)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		<-exited
		ts.Close()
	})

	cli, err := New(
		strings.TrimPrefix(ts.URL, "http://"),
		WithDisableMTLS(true),
		WithHTTPClient(ts.Client()),
		WithCloseTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	msg := newQueueMessage(&QueueMessageHandle{
		client: cli,
		msg: api.Message{
			Namespace: "default", Queue: "jobs", MessageID: "message-1", LeaseID: "lease-1", FencingToken: 1, MetaETag: "etag-1",
		},
	}, nil, 0)

	result := make(chan error, 1)
	started := time.Now()
	go func() {
		result <- cli.finalizeManagedConsumerMessage(msg, nil, nil, context.Canceled, true)
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("settlement error=%v, want cancelled handler context", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("settlement error=%v, want bounded request deadline", err)
		}
		if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
			t.Fatalf("settlement took %s, want close timeout bound", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("settlement remained blocked after managed consumer cancellation")
	}
	select {
	case <-entered:
	default:
		t.Fatal("automatic acknowledgement did not reach server")
	}
}
