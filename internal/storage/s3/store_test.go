package s3

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"pkt.systems/lockd/internal/storage"
)

func TestS3StoreMetaLifecycle(t *testing.T) {
	server, cfg := setupFakeS3(t)
	defer server.Close()

	store, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	meta := &storage.Meta{Version: 1}
	initialETag, err := store.StoreMeta(ctx, "alpha", meta, "")
	if err != nil {
		t.Fatalf("store meta create: %v", err)
	}
	got, gotETag, err := store.LoadMeta(ctx, "alpha")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("expected version 1, got %d", got.Version)
	}
	meta.Version = 2
	newETag, err := store.StoreMeta(ctx, "alpha", meta, gotETag)
	if err != nil {
		t.Fatalf("store meta update: %v", err)
	}
	if _, err := store.StoreMeta(ctx, "alpha", meta, "bogus"); err != storage.ErrCASMismatch {
		t.Fatalf("expected cas mismatch, got %v", err)
	}
	if err := store.DeleteMeta(ctx, "alpha", "wrong"); err != storage.ErrCASMismatch {
		t.Fatalf("expected delete cas mismatch, got %v", err)
	}
	if err := store.DeleteMeta(ctx, "alpha", newETag); err != nil {
		t.Fatalf("delete meta: %v", err)
	}
	if err := store.DeleteMeta(ctx, "alpha", initialETag); err != storage.ErrNotFound {
		t.Fatalf("expected not found on second delete, got %v", err)
	}
}

func TestS3StoreStateLifecycle(t *testing.T) {
	server, cfg := setupFakeS3(t)
	defer server.Close()

	store, err := New(cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ctx := context.Background()
	res, err := store.WriteState(ctx, "stream", bytes.NewReader([]byte(`{"offset":1}`)), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	reader, info, err := store.ReadState(ctx, "stream")
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	data := new(bytes.Buffer)
	if _, err := data.ReadFrom(reader); err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(data.String(), "offset") {
		t.Fatalf("expected body, got %s", data.String())
	}
	_ = reader.Close()
	if info.ETag == "" || info.ETag != res.NewETag {
		t.Fatalf("expected etag match, got %q vs %q", info.ETag, res.NewETag)
	}
	if _, err := store.WriteState(ctx, "stream", bytes.NewReader([]byte(`{"offset":2}`)), storage.PutStateOptions{ExpectedETag: "wrong"}); err != storage.ErrCASMismatch {
		t.Fatalf("expected cas mismatch, got %v", err)
	}
	if err := store.RemoveState(ctx, "stream", "wrong"); err != storage.ErrCASMismatch {
		t.Fatalf("expected remove cas mismatch, got %v", err)
	}
	if err := store.RemoveState(ctx, "stream", res.NewETag); err != nil {
		t.Fatalf("remove state: %v", err)
	}
}

func setupFakeS3(t *testing.T) (*httptest.Server, Config) {
	t.Helper()
	backend := s3mem.New()
	fs := gofakes3.New(backend)
	server := httptest.NewServer(fs.Server())
	bucket := "lockd-test"
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	endpoint := strings.TrimPrefix(server.URL, "http://")
	os.Setenv("AWS_ACCESS_KEY_ID", "test")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	cfg := Config{
		Endpoint:       endpoint,
		Region:         "us-east-1",
		Bucket:         bucket,
		Secure:         false,
		ForcePathStyle: true,
	}
	return server, cfg
}
