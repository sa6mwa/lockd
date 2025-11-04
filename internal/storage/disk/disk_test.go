package disk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	if err := store.Remove(ctx, key, res.NewETag); err != nil {
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

	if err := store.Remove(ctx, key, "mismatch"); !errors.Is(err, storage.ErrCASMismatch) {
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

func TestDiskStoreConcurrentMeta(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	store1, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("new store1: %v", err)
	}
	defer store1.Close()
	store2, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("new store2: %v", err)
	}
	defer store2.Close()

	ctx := context.Background()
	key := "concurrent-meta"
	initial := storage.Meta{Version: 1, UpdatedAtUnix: time.Now().Unix()}
	etag, err := store1.StoreMeta(ctx, key, &initial, "")
	if err != nil {
		t.Fatalf("store meta: %v", err)
	}

	initial.Version = 2
	newEtag, err := store1.StoreMeta(ctx, key, &initial, etag)
	if err != nil {
		t.Fatalf("sequential update failed: %v", err)
	}
	initial.Version = 3
	if _, err := store1.StoreMeta(ctx, key, &initial, etag); !errors.Is(err, storage.ErrCASMismatch) {
		t.Fatalf("expected cas mismatch on stale etag, got %v", err)
	}
	etag = newEtag

	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)

	updated1 := storage.Meta{Version: 2, UpdatedAtUnix: time.Now().Unix()}
	updated2 := storage.Meta{Version: 3, UpdatedAtUnix: time.Now().Unix()}

	go func() {
		defer wg.Done()
		_, err := store1.StoreMeta(ctx, key, &updated1, etag)
		results <- err
	}()

	go func() {
		defer wg.Done()
		_, err := store2.StoreMeta(ctx, key, &updated2, etag)
		results <- err
	}()

	wg.Wait()
	close(results)

	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, storage.ErrCASMismatch) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if success != 1 {
		t.Fatalf("expected exactly one successful meta update, got %d", success)
	}

	meta, _, err := store1.LoadMeta(ctx, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if meta.Version != 2 && meta.Version != 3 {
		t.Fatalf("unexpected version after concurrent update: %d", meta.Version)
	}
}

func TestDiskStoreConcurrentState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	store1, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("new store1: %v", err)
	}
	defer store1.Close()
	store2, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("new store2: %v", err)
	}
	defer store2.Close()

	ctx := context.Background()
	key := "concurrent-state"

	res, err := store1.WriteState(ctx, key, strings.NewReader("initial"), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write initial state: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)

	go func() {
		defer wg.Done()
		_, err := store1.WriteState(ctx, key, strings.NewReader("alpha"), storage.PutStateOptions{ExpectedETag: res.NewETag})
		results <- err
	}()

	go func() {
		defer wg.Done()
		_, err := store2.WriteState(ctx, key, strings.NewReader("beta"), storage.PutStateOptions{ExpectedETag: res.NewETag})
		results <- err
	}()

	wg.Wait()
	close(results)

	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, storage.ErrCASMismatch) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if success != 1 {
		t.Fatalf("expected exactly one successful state update, got %d", success)
	}

	reader, info, err := store1.ReadState(ctx, key)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "alpha" && string(body) != "beta" {
		t.Fatalf("unexpected final body: %s", string(body))
	}
	if info.ETag == res.NewETag {
		t.Fatalf("etag not updated")
	}
}
