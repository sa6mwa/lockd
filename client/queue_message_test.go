package client

import (
	"bytes"
	"io"
	"testing"
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
