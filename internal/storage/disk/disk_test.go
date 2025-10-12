package disk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
)

func TestDiskStoreRoundTrip(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "store")
	store, err := New(Config{Root: root, Now: func() time.Time { return time.Unix(1700000000, 0) }})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	key := "orders/v1"

	meta := storage.Meta{Version: 1, UpdatedAtUnix: 1700000000}
	etag, err := store.StoreMeta(ctx, key, &meta, "")
	if err != nil {
		t.Fatalf("store meta: %v", err)
	}
	if etag == "" {
		t.Fatalf("expected etag")
	}

	payload := []byte(`{"hello":"world"}`)
	res, err := store.WriteState(ctx, key, bytes.NewReader(payload), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	if res.BytesWritten != int64(len(payload)) {
		t.Fatalf("bytes written = %d want %d", res.BytesWritten, len(payload))
	}
	if res.NewETag == "" {
		t.Fatalf("expected new etag")
	}

	reader, info, err := store.ReadState(ctx, key)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(payload, body) {
		t.Fatalf("body mismatch")
	}
	if info.Size != int64(len(payload)) {
		t.Fatalf("size = %d want %d", info.Size, len(payload))
	}
	if info.ETag != res.NewETag {
		t.Fatalf("etag = %s want %s", info.ETag, res.NewETag)
	}

	metaLoaded, metaETag, err := store.LoadMeta(ctx, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if metaETag != etag {
		t.Fatalf("meta etag = %s want %s", metaETag, etag)
	}
	if metaLoaded.Version != meta.Version {
		t.Fatalf("version = %d want %d", metaLoaded.Version, meta.Version)
	}

	if err := store.RemoveState(ctx, key, res.NewETag); err != nil {
		t.Fatalf("remove state: %v", err)
	}

	if _, _, err := store.ReadState(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestDiskStoreCAS(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "store")
	store, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	key := "orders"

	if _, err := store.WriteState(ctx, key, bytes.NewReader([]byte("foo")), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write state: %v", err)
	}

	if _, err := store.WriteState(ctx, key, bytes.NewReader([]byte("bar")), storage.PutStateOptions{ExpectedETag: "nope"}); !errors.Is(err, storage.ErrCASMismatch) {
		t.Fatalf("expected cas mismatch, got %v", err)
	}

	if err := store.RemoveState(ctx, key, "mismatch"); !errors.Is(err, storage.ErrCASMismatch) {
		t.Fatalf("expected cas mismatch, got %v", err)
	}
}

func TestDiskRetentionSweep(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "store")
	store, err := New(Config{Root: root, Now: func() time.Time { return time.Unix(2000, 0) }})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	store.retention = time.Second

	ctx := context.Background()
	key := "old-key"

	meta := storage.Meta{Version: 1, UpdatedAtUnix: 1}
	if _, err := store.StoreMeta(ctx, key, &meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	if _, err := store.WriteState(ctx, key, bytes.NewReader([]byte("data")), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write state: %v", err)
	}

	store.sweepOnce()

	if _, _, err := store.LoadMeta(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected meta cleanup, got %v", err)
	}

	if _, _, err := store.ReadState(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected state cleanup, got %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, "state")); err != nil {
		t.Fatalf("state dir: %v", err)
	}
}
