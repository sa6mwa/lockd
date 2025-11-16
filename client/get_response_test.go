package client

import (
	"bytes"
	"io"
	"testing"
)

func TestGetResponseDocument(t *testing.T) {
	body := []byte(`{"status":"ok","count":5}`)
	resp := &GetResponse{
		Namespace: "default",
		Key:       "orders/42",
		ETag:      "abc123",
		Version:   "7",
		HasState:  true,
		reader:    io.NopCloser(bytes.NewReader(body)),
	}
	doc, err := resp.Document()
	if err != nil {
		t.Fatalf("document: %v", err)
	}
	if doc == nil {
		t.Fatal("expected document")
	}
	if doc.Namespace != "default" || doc.Key != "orders/42" || doc.Version != "7" || doc.ETag != "abc123" {
		t.Fatalf("unexpected doc identity: %+v", doc)
	}
	if doc.Body["status"] != "ok" {
		t.Fatalf("unexpected body: %+v", doc.Body)
	}
	if resp.Reader() != nil {
		t.Fatalf("expected reader to be consumed")
	}
	again, err := resp.Document()
	if err != nil {
		t.Fatalf("second document: %v", err)
	}
	if again != nil {
		t.Fatal("expected nil document after consumption")
	}
}
