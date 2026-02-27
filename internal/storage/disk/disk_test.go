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
	"pkt.systems/lockd/namespaces"
)

const testNamespace = namespaces.Default

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
	etag, err := store.StoreMeta(ctx, testNamespace, key, &meta, "")
	if err != nil {
		t.Fatalf("store meta: %v", err)
	}
	if etag == "" {
		t.Fatalf("expected etag")
	}

	payload := []byte(`{"hello":"world"}`)
	res, err := store.WriteState(ctx, testNamespace, key, bytes.NewReader(payload), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	if res.BytesWritten != int64(len(payload)) {
		t.Fatalf("bytes written = %d want %d", res.BytesWritten, len(payload))
	}
	if res.NewETag == "" {
		t.Fatalf("expected new etag")
	}

	readRes, err := store.ReadState(ctx, testNamespace, key)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	defer readRes.Reader.Close()
	body, err := io.ReadAll(readRes.Reader)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(payload, body) {
		t.Fatalf("body mismatch")
	}
	if readRes.Info.Size != int64(len(payload)) {
		t.Fatalf("size = %d want %d", readRes.Info.Size, len(payload))
	}
	if readRes.Info.ETag != res.NewETag {
		t.Fatalf("etag = %s want %s", readRes.Info.ETag, res.NewETag)
	}

	metaRes, err := store.LoadMeta(ctx, testNamespace, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if metaRes.ETag != etag {
		t.Fatalf("meta etag = %s want %s", metaRes.ETag, etag)
	}
	if metaRes.Meta.Version != meta.Version {
		t.Fatalf("version = %d want %d", metaRes.Meta.Version, meta.Version)
	}

	if err := store.Remove(ctx, testNamespace, key, res.NewETag); err != nil {
		t.Fatalf("remove state: %v", err)
	}

	if _, err := store.ReadState(ctx, testNamespace, key); !errors.Is(err, storage.ErrNotFound) {
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

	if _, err := store.WriteState(ctx, testNamespace, key, bytes.NewReader([]byte("foo")), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write state: %v", err)
	}

	if _, err := store.WriteState(ctx, testNamespace, key, bytes.NewReader([]byte("bar")), storage.PutStateOptions{ExpectedETag: "nope"}); !errors.Is(err, storage.ErrCASMismatch) {
		t.Fatalf("expected cas mismatch, got %v", err)
	}

	if err := store.Remove(ctx, testNamespace, key, "mismatch"); !errors.Is(err, storage.ErrCASMismatch) {
		t.Fatalf("expected cas mismatch, got %v", err)
	}
}

func TestDiskLoadMetaSummary(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "store")
	store, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	key := "orders/summary"

	meta := storage.Meta{
		Version:             10,
		PublishedVersion:    8,
		StateETag:           "state-etag-a",
		StateDescriptor:     []byte("desc-a"),
		StatePlaintextBytes: 2048,
	}
	meta.MarkQueryExcluded()
	etag, err := store.StoreMeta(ctx, testNamespace, key, &meta, "")
	if err != nil {
		t.Fatalf("store meta: %v", err)
	}

	summary, err := store.LoadMetaSummary(ctx, testNamespace, key)
	if err != nil {
		t.Fatalf("load meta summary: %v", err)
	}
	if summary.ETag != etag {
		t.Fatalf("etag mismatch: got %q want %q", summary.ETag, etag)
	}
	if summary.Meta == nil {
		t.Fatalf("summary missing")
	}
	if !summary.Meta.QueryExcluded {
		t.Fatalf("expected query excluded")
	}
	if summary.Meta.EffectiveVersion() != 8 {
		t.Fatalf("effective version mismatch: got %d", summary.Meta.EffectiveVersion())
	}
	if string(summary.Meta.StateDescriptor) != "desc-a" {
		t.Fatalf("state descriptor mismatch: got %q", string(summary.Meta.StateDescriptor))
	}

	meta.ClearQueryExcluded()
	meta.PublishedVersion = 9
	meta.StateETag = "state-etag-b"
	meta.StateDescriptor = []byte("desc-b")
	meta.StatePlaintextBytes = 4096
	newETag, err := store.StoreMeta(ctx, testNamespace, key, &meta, etag)
	if err != nil {
		t.Fatalf("update meta: %v", err)
	}
	updated, err := store.LoadMetaSummary(ctx, testNamespace, key)
	if err != nil {
		t.Fatalf("load updated summary: %v", err)
	}
	if updated.ETag != newETag {
		t.Fatalf("updated etag mismatch: got %q want %q", updated.ETag, newETag)
	}
	if updated.Meta == nil {
		t.Fatalf("updated summary missing")
	}
	if updated.Meta.QueryExcluded {
		t.Fatalf("did not expect query excluded after clear")
	}
	if updated.Meta.PublishedVersion != 9 {
		t.Fatalf("published version mismatch: got %d", updated.Meta.PublishedVersion)
	}
	if updated.Meta.StateETag != "state-etag-b" {
		t.Fatalf("state etag mismatch: got %q", updated.Meta.StateETag)
	}
	if updated.Meta.StatePlaintextBytes != 4096 {
		t.Fatalf("plaintext bytes mismatch: got %d", updated.Meta.StatePlaintextBytes)
	}
	if string(updated.Meta.StateDescriptor) != "desc-b" {
		t.Fatalf("state descriptor mismatch: got %q", string(updated.Meta.StateDescriptor))
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
	if _, err := store.StoreMeta(ctx, testNamespace, key, &meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	if _, err := store.WriteState(ctx, testNamespace, key, bytes.NewReader([]byte("data")), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write state: %v", err)
	}

	store.sweepOnce()

	if _, err := store.LoadMeta(ctx, testNamespace, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected meta cleanup, got %v", err)
	}

	if _, err := store.ReadState(ctx, testNamespace, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected state cleanup, got %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, testNamespace, "logstore")); err != nil {
		t.Fatalf("logstore dir: %v", err)
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
	etag, err := store1.StoreMeta(ctx, testNamespace, key, &initial, "")
	if err != nil {
		t.Fatalf("store meta: %v", err)
	}

	initial.Version = 2
	newEtag, err := store1.StoreMeta(ctx, testNamespace, key, &initial, etag)
	if err != nil {
		t.Fatalf("sequential update failed: %v", err)
	}
	initial.Version = 3
	if _, err := store1.StoreMeta(ctx, testNamespace, key, &initial, etag); !errors.Is(err, storage.ErrCASMismatch) {
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
		_, err := store1.StoreMeta(ctx, testNamespace, key, &updated1, etag)
		results <- err
	}()

	go func() {
		defer wg.Done()
		_, err := store2.StoreMeta(ctx, testNamespace, key, &updated2, etag)
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

	metaRes, err := store1.LoadMeta(ctx, testNamespace, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta := metaRes.Meta
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

	res, err := store1.WriteState(ctx, testNamespace, key, strings.NewReader("initial"), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write initial state: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)

	go func() {
		defer wg.Done()
		_, err := store1.WriteState(ctx, testNamespace, key, strings.NewReader("alpha"), storage.PutStateOptions{ExpectedETag: res.NewETag})
		results <- err
	}()

	go func() {
		defer wg.Done()
		_, err := store2.WriteState(ctx, testNamespace, key, strings.NewReader("beta"), storage.PutStateOptions{ExpectedETag: res.NewETag})
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

	readRes, err := store1.ReadState(ctx, testNamespace, key)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	defer readRes.Reader.Close()
	body, err := io.ReadAll(readRes.Reader)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "alpha" && string(body) != "beta" {
		t.Fatalf("unexpected final body: %s", string(body))
	}
	if readRes.Info.ETag == res.NewETag {
		t.Fatalf("etag not updated")
	}
}
