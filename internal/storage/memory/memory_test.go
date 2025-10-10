package memory

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"pkt.systems/lockd/internal/storage"
)

func TestStoreMetaCAS(t *testing.T) {
	store := New()
	ctx := context.Background()

	meta := &storage.Meta{Version: 1}
	if _, err := store.StoreMeta(ctx, "alpha", meta, ""); err != nil {
		t.Fatalf("store meta create: %v", err)
	}
	loaded, etag, err := store.LoadMeta(ctx, "alpha")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if loaded.Version != 1 {
		t.Fatalf("expected version 1, got %d", loaded.Version)
	}
	meta.Version = 2
	if _, err := store.StoreMeta(ctx, "alpha", meta, etag); err != nil {
		t.Fatalf("store meta cas: %v", err)
	}
	if _, err := store.StoreMeta(ctx, "alpha", meta, "wrong"); !errors.Is(err, storage.ErrCASMismatch) {
		t.Fatalf("expected cas mismatch, got %v", err)
	}
	keys, err := store.ListMetaKeys(ctx)
	if err != nil {
		t.Fatalf("list meta keys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "alpha" {
		t.Fatalf("unexpected keys: %v", keys)
	}
}

func TestWriteStateCAS(t *testing.T) {
	store := New()
	ctx := context.Background()

	body := bytes.NewBufferString(`{"cursor":1}`)
	res, err := store.WriteState(ctx, "alpha", body, storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	if res.BytesWritten == 0 || res.NewETag == "" {
		t.Fatalf("expected write metadata, got %+v", res)
	}

	reader, info, err := store.ReadState(ctx, "alpha")
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	defer reader.Close()
	if info.ETag != res.NewETag {
		t.Fatalf("etag mismatch: %s vs %s", info.ETag, res.NewETag)
	}

	newBody := bytes.NewBufferString(`{"cursor":2}`)
	if _, err := store.WriteState(ctx, "alpha", newBody, storage.PutStateOptions{ExpectedETag: "wrong"}); !errors.Is(err, storage.ErrCASMismatch) {
		t.Fatalf("expected cas mismatch, got %v", err)
	}
}
