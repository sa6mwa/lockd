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

func TestListObjectsPrefixAndStartAfter(t *testing.T) {
	store := New()
	ctx := context.Background()
	keys := []string{
		"meta/alpha.pb",
		"q/a/msg/001.pb",
		"q/a/msg/002.pb",
		"q/a/msg/003.pb",
		"q/b/msg/001.pb",
	}
	for _, key := range keys {
		if _, err := store.PutObject(ctx, key, bytes.NewBufferString("body"), storage.PutObjectOptions{}); err != nil {
			t.Fatalf("put object %s: %v", key, err)
		}
	}

	result, err := store.ListObjects(ctx, storage.ListOptions{Prefix: "q/a/msg/", Limit: 2})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}
	if len(result.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(result.Objects))
	}
	if result.Objects[0].Key != "q/a/msg/001.pb" || result.Objects[1].Key != "q/a/msg/002.pb" {
		t.Fatalf("unexpected keys: %#v", result.Objects)
	}
	if !result.Truncated || result.NextStartAfter != "q/a/msg/002.pb" {
		t.Fatalf("unexpected truncation metadata: %+v", result)
	}

	result2, err := store.ListObjects(ctx, storage.ListOptions{Prefix: "q/a/msg/", StartAfter: "q/a/msg/002.pb", Limit: 2})
	if err != nil {
		t.Fatalf("list objects start after: %v", err)
	}
	if len(result2.Objects) != 1 || result2.Objects[0].Key != "q/a/msg/003.pb" {
		t.Fatalf("unexpected keys after start after: %#v", result2.Objects)
	}
	if result2.Truncated {
		t.Fatalf("did not expect truncation after consuming tail: %+v", result2)
	}

	result3, err := store.ListObjects(ctx, storage.ListOptions{Prefix: "q/a/msg/", StartAfter: "meta/alpha.pb", Limit: 2})
	if err != nil {
		t.Fatalf("list objects start after meta: %v", err)
	}
	if len(result3.Objects) == 0 || result3.Objects[0].Key != "q/a/msg/001.pb" {
		t.Fatalf("expected to resume at first queue key, got %#v", result3.Objects)
	}
}
