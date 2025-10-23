package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	minio "github.com/minio/minio-go/v7"

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
		Insecure:       true,
		ForcePathStyle: true,
	}
	return server, cfg
}

type fakeTimeoutErr struct{}

func (fakeTimeoutErr) Error() string   { return "timeout" }
func (fakeTimeoutErr) Timeout() bool   { return true }
func (fakeTimeoutErr) Temporary() bool { return true }

func TestIsRetryableNetworkErrors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{name: "nil", err: nil, expected: false},
		{name: "context deadline", err: context.DeadlineExceeded, expected: true},
		{name: "net timeout", err: fakeTimeoutErr{}, expected: true},
		{name: "dns temporary", err: &net.DNSError{IsTemporary: true}, expected: true},
		{name: "net op timeout", err: &net.OpError{Err: fakeTimeoutErr{}}, expected: true},
		{name: "connection reset", err: syscall.ECONNRESET, expected: true},
		{name: "connection refused", err: syscall.ECONNREFUSED, expected: true},
		{name: "io EOF", err: io.EOF, expected: true},
		{name: "non retryable", err: errors.New("boom"), expected: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := isRetryable(tc.err)
			if got != tc.expected {
				t.Fatalf("expected %v, got %v for %T", tc.expected, got, tc.err)
			}
		})
	}
}

type stubObject struct {
	readErr   error
	readAtErr error
	seekErr   error
	closed    bool
}

func (s *stubObject) Read(p []byte) (int, error) {
	if s.readErr != nil {
		return 0, s.readErr
	}
	return 0, io.EOF
}

func (s *stubObject) ReadAt(p []byte, offset int64) (int, error) {
	if s.readAtErr != nil {
		return 0, s.readAtErr
	}
	return 0, io.EOF
}

func (s *stubObject) Seek(offset int64, whence int) (int64, error) {
	if s.seekErr != nil {
		return 0, s.seekErr
	}
	return 0, nil
}

func (s *stubObject) Close() error {
	s.closed = true
	return nil
}

func TestNotFoundAwareObjectConverts404(t *testing.T) {
	err404 := minio.ErrorResponse{StatusCode: http.StatusNotFound}
	obj := &stubObject{readErr: err404, readAtErr: err404, seekErr: err404}
	reader := &notFoundAwareObject{object: obj}

	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Read: expected ErrNotFound, got %v", err)
	}
	if _, err := reader.ReadAt(make([]byte, 1), 0); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("ReadAt: expected ErrNotFound, got %v", err)
	}
	if _, err := reader.Seek(0, io.SeekStart); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Seek: expected ErrNotFound, got %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: unexpected error: %v", err)
	}
	if !obj.closed {
		t.Fatal("Close: expected underlying close to be called")
	}
}

func TestNotFoundAwareObjectPassthrough(t *testing.T) {
	expected := errors.New("boom")
	obj := &stubObject{readErr: expected, readAtErr: expected, seekErr: expected}
	reader := &notFoundAwareObject{object: obj}

	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, expected) {
		t.Fatalf("Read: expected %v, got %v", expected, err)
	}
	if _, err := reader.ReadAt(make([]byte, 1), 0); !errors.Is(err, expected) {
		t.Fatalf("ReadAt: expected %v, got %v", expected, err)
	}
	if _, err := reader.Seek(0, io.SeekStart); !errors.Is(err, expected) {
		t.Fatalf("Seek: expected %v, got %v", expected, err)
	}
}
