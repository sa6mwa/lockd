package storage_test

import (
	"context"
	"errors"
	"testing"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
)

func TestNewTransientErrorWraps(t *testing.T) {
	t.Parallel()

	err := errors.New("boom")
	wrapped := storage.NewTransientError(err)
	if wrapped == nil {
		t.Fatal("expected wrapped error")
	}
	if !errors.Is(wrapped, err) {
		t.Fatal("wrapped error should contain original")
	}
	if !storage.IsTransient(wrapped) {
		t.Fatal("expected IsTransient to detect wrapped error")
	}
	if storage.IsTransient(err) {
		t.Fatal("plain error should not be transient")
	}
}

func TestNewTransientErrorHandlesNil(t *testing.T) {
	t.Parallel()

	if storage.NewTransientError(nil) != nil {
		t.Fatal("nil input should return nil")
	}
}

func TestLoadMetaSummaryFallback(t *testing.T) {
	t.Parallel()

	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	meta := &storage.Meta{
		Version:             7,
		PublishedVersion:    5,
		StateETag:           "state-etag-1",
		StateDescriptor:     []byte("desc-1"),
		StatePlaintextBytes: 123,
	}
	meta.MarkQueryExcluded()

	etag, err := store.StoreMeta(context.Background(), namespaces.Default, "orders/1", meta, "")
	if err != nil {
		t.Fatalf("store meta: %v", err)
	}
	result, err := storage.LoadMetaSummary(context.Background(), store, namespaces.Default, "orders/1")
	if err != nil {
		t.Fatalf("load meta summary: %v", err)
	}
	if result.ETag != etag {
		t.Fatalf("etag mismatch: got %q want %q", result.ETag, etag)
	}
	if result.Meta == nil {
		t.Fatalf("summary missing")
	}
	if result.Meta.Version != 7 {
		t.Fatalf("version mismatch: got %d", result.Meta.Version)
	}
	if result.Meta.PublishedVersion != 5 {
		t.Fatalf("published version mismatch: got %d", result.Meta.PublishedVersion)
	}
	if result.Meta.StateETag != "state-etag-1" {
		t.Fatalf("state etag mismatch: got %q", result.Meta.StateETag)
	}
	if result.Meta.StatePlaintextBytes != 123 {
		t.Fatalf("plaintext bytes mismatch: got %d", result.Meta.StatePlaintextBytes)
	}
	if !result.Meta.QueryExcluded {
		t.Fatalf("expected query excluded")
	}
	if string(result.Meta.StateDescriptor) != "desc-1" {
		t.Fatalf("descriptor mismatch: got %q", string(result.Meta.StateDescriptor))
	}
}
