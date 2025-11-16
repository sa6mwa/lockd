package client

import (
	"io"
	"testing"
	"time"
)

func TestDocumentMutate(t *testing.T) {
	doc := NewDocument("orders", "order-1")
	if err := doc.Mutate(
		"/status=pending",
		"/items/0/sku=ABC",
		"/items/0/qty=1",
		"time:/updated_at=2025-11-10T12:00:00Z",
	); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	if doc.Namespace != "orders" || doc.Key != "order-1" {
		t.Fatalf("unexpected doc identity: %+v", doc)
	}
	if doc.Body["status"] != "pending" {
		t.Fatalf("status not set: %+v", doc.Body)
	}
}

func TestDocumentBytesAndLoadInto(t *testing.T) {
	doc := NewDocument("orders", "order-2")
	if err := doc.Mutate("/status=done", "/amount=42.5"); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	data, err := doc.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	if len(data) == 0 || data[0] != '{' {
		t.Fatalf("unexpected bytes: %s", data)
	}
	var target struct {
		Status string  `json:"status"`
		Amount float64 `json:"amount"`
	}
	if err := doc.LoadInto(&target); err != nil {
		t.Fatalf("load into: %v", err)
	}
	if target.Status != "done" || target.Amount != 42.5 {
		t.Fatalf("unexpected decode: %+v", target)
	}
}

func TestDocumentMutateWithTime(t *testing.T) {
	doc := NewDocument("ns", "k")
	now := time.Date(2025, 11, 11, 12, 0, 0, 0, time.UTC)
	if err := doc.MutateWithTime(now, "time:/processed_at=NOW"); err != nil {
		t.Fatalf("mutate with time: %v", err)
	}
	if doc.Body["processed_at"] != now.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected timestamp: %v", doc.Body["processed_at"])
	}
}

func TestDocumentWriteAndReader(t *testing.T) {
	doc := NewDocument("ns", "stream")
	payload := []byte(`{"status":"streaming"}`)
	if _, err := doc.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	data, err := doc.Bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("unexpected bytes: %s", data)
	}
	r, err := doc.Reader()
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected reader: %s", got)
	}
}
