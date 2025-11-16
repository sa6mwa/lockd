package scan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/lql"
	"pkt.systems/lockd/namespaces"
)

func TestScanAdapterMatchAll(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeMetaOnly(t, store, "default", "empty")
	writeState(t, store, "default", "orders/1", map[string]any{"status": "open"})
	writeState(t, store, "default", "orders/2", map[string]any{"status": "closed"})

	adapter, err := New(Config{Backend: store, MaxDocumentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	result, err := adapter.Query(ctx, search.Request{Namespace: "default", Limit: 10})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(result.Keys))
	}
	if result.Cursor != "" {
		t.Fatalf("expected empty cursor, got %q", result.Cursor)
	}
}

func TestScanAdapterSelector(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{
		"status": "open",
		"amount": 150.5,
	})
	writeState(t, store, "default", "orders/2", map[string]any{
		"status": "closed",
		"amount": 99,
	})
	writeState(t, store, "default", "orders/3", map[string]any{
		"status": "open",
		"amount": 400,
	})

	sel, err := lql.ParseSelectorString(`
and.eq{field=/status,value=open},
and.range{field=/amount,gte=120,lt=200}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}

	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	result, err := adapter.Query(ctx, search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "orders/1" {
		t.Fatalf("unexpected keys %v", result.Keys)
	}
	if result.Cursor != "" {
		t.Fatalf("expected empty cursor, got %q", result.Cursor)
	}
}

func TestScanAdapterCursor(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		writeState(t, store, "default", fmt.Sprintf("key-%02d", i), map[string]any{"status": "open"})
	}
	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	first, err := adapter.Query(ctx, search.Request{Namespace: "default", Limit: 2})
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	if len(first.Keys) != 2 || first.Cursor == "" {
		t.Fatalf("unexpected first result %+v", first)
	}
	second, err := adapter.Query(ctx, search.Request{
		Namespace: "default",
		Limit:     2,
		Cursor:    first.Cursor,
	})
	if err != nil {
		t.Fatalf("second query: %v", err)
	}
	if len(second.Keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(second.Keys))
	}
}

func TestScanAdapterInvalidCursor(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{"status": "open"})
	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.Query(ctx, search.Request{
		Namespace: "default",
		Cursor:    "invalid",
	})
	if !errors.Is(err, search.ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestScanAdapterSkipsReservedAndHidden(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "lockd-diagnostics/123", map[string]any{"internal": true})
	writeState(t, store, "default", "orders/visible", map[string]any{"status": "open"})
	writeState(t, store, "default", "orders/hidden", map[string]any{"status": "open"})
	meta, etag, err := store.LoadMeta(ctx, "default", "orders/hidden")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta.MarkQueryExcluded()
	if _, err := store.StoreMeta(ctx, "default", "orders/hidden", meta, etag); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	result, err := adapter.Query(ctx, search.Request{Namespace: "default", Limit: 10})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "orders/visible" {
		t.Fatalf("expected only visible order, got %+v", result.Keys)
	}
}

func TestScanAdapterCapabilities(t *testing.T) {
	adapter, err := New(Config{Backend: memory.New()})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	caps, err := adapter.Capabilities(context.Background(), namespaces.Default)
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if !caps.Scan || caps.Index {
		t.Fatalf("expected scan-only capabilities, got %+v", caps)
	}
}

func writeState(t *testing.T, store storage.Backend, namespace, key string, doc map[string]any) {
	t.Helper()
	payload, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	res, err := store.WriteState(context.Background(), namespace, key, bytes.NewReader(payload), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	meta := &storage.Meta{
		Version:             1,
		PublishedVersion:    1,
		StateETag:           res.NewETag,
		UpdatedAtUnix:       time.Now().Unix(),
		StatePlaintextBytes: res.BytesWritten,
		StateDescriptor:     append([]byte(nil), res.Descriptor...),
	}
	if _, err := store.StoreMeta(context.Background(), namespace, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
}

func writeMetaOnly(t *testing.T, store storage.Backend, namespace, key string) {
	t.Helper()
	meta := &storage.Meta{
		Version:          1,
		PublishedVersion: 1,
	}
	if _, err := store.StoreMeta(context.Background(), namespace, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
}
