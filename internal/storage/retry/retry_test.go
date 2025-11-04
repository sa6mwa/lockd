package retry_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/retry"
)

type fakeClock struct {
	sleeps []time.Duration
	now    time.Time
}

func (f *fakeClock) Now() time.Time {
	if f.now.IsZero() {
		f.now = time.Unix(0, 0)
	}
	return f.now
}

func (f *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	f.sleeps = append(f.sleeps, d)
	ch <- f.Now().Add(d)
	return ch
}

func (f *fakeClock) Sleep(d time.Duration) {
	f.sleeps = append(f.sleeps, d)
	f.now = f.now.Add(d)
}

type stubBackend struct {
	loadMetaErrs  []error
	loadMetaCalls int
	hook          func(int)
}

func (s *stubBackend) LoadMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	s.loadMetaCalls++
	if s.hook != nil {
		s.hook(s.loadMetaCalls)
	}
	var err error
	if idx := s.loadMetaCalls - 1; idx < len(s.loadMetaErrs) {
		err = s.loadMetaErrs[idx]
	}
	if err != nil {
		return nil, "", err
	}
	return &storage.Meta{Version: int64(s.loadMetaCalls)}, fmt.Sprintf("etag-%d", s.loadMetaCalls), nil
}

func (s *stubBackend) StoreMeta(context.Context, string, *storage.Meta, string) (string, error) {
	return "", storage.ErrNotImplemented
}

func (s *stubBackend) DeleteMeta(context.Context, string, string) error {
	return storage.ErrNotImplemented
}

func (s *stubBackend) ListMetaKeys(context.Context) ([]string, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubBackend) ReadState(context.Context, string) (io.ReadCloser, *storage.StateInfo, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil, storage.ErrNotImplemented
}

func (s *stubBackend) WriteState(context.Context, string, io.Reader, storage.PutStateOptions) (*storage.PutStateResult, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubBackend) Remove(context.Context, string, string) error {
	return storage.ErrNotImplemented
}

func (s *stubBackend) ListObjects(context.Context, storage.ListOptions) (*storage.ListResult, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubBackend) GetObject(context.Context, string) (io.ReadCloser, *storage.ObjectInfo, error) {
	return nil, nil, storage.ErrNotImplemented
}

func (s *stubBackend) PutObject(context.Context, string, io.Reader, storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubBackend) DeleteObject(context.Context, string, storage.DeleteObjectOptions) error {
	return storage.ErrNotImplemented
}

func (s *stubBackend) Close() error { return nil }

func TestWrapReturnsNilOnNilInner(t *testing.T) {
	t.Parallel()

	if retry.Wrap(nil, loggingutil.NoopLogger(), &fakeClock{}, retry.Config{}) != nil {
		t.Fatal("expected nil backend when inner is nil")
	}
}

func TestLoadMetaRetriesTransientErrors(t *testing.T) {
	t.Parallel()

	back := &stubBackend{
		loadMetaErrs: []error{
			storage.NewTransientError(errors.New("temporary")),
			nil,
		},
	}
	fc := &fakeClock{}
	wrapped := retry.Wrap(back, loggingutil.NoopLogger(), fc, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
		Multiplier:  2,
		MaxDelay:    10 * time.Millisecond,
	})
	meta, etag, err := wrapped.LoadMeta(context.Background(), "key")
	if err != nil {
		t.Fatalf("LoadMeta returned error: %v", err)
	}
	if meta == nil || meta.Version != 2 {
		t.Fatalf("unexpected meta: %#v", meta)
	}
	if etag != "etag-2" {
		t.Fatalf("unexpected etag: %q", etag)
	}
	if back.loadMetaCalls != 2 {
		t.Fatalf("expected 2 attempts, got %d", back.loadMetaCalls)
	}
	if got := len(fc.sleeps); got != 1 {
		t.Fatalf("expected 1 recorded sleep, got %d", got)
	}
	if fc.sleeps[0] != 5*time.Millisecond {
		t.Fatalf("unexpected backoff duration: %v", fc.sleeps[0])
	}
}

func TestLoadMetaStopsOnNonTransientError(t *testing.T) {
	t.Parallel()

	back := &stubBackend{
		loadMetaErrs: []error{
			errors.New("fatal"),
			nil,
		},
	}
	fc := &fakeClock{}
	wrapped := retry.Wrap(back, loggingutil.NoopLogger(), fc, retry.Config{MaxAttempts: 3})
	_, _, err := wrapped.LoadMeta(context.Background(), "key")
	if err == nil || err.Error() != "fatal" {
		t.Fatalf("expected fatal error, got %v", err)
	}
	if back.loadMetaCalls != 1 {
		t.Fatalf("unexpected number of attempts: %d", back.loadMetaCalls)
	}
	if len(fc.sleeps) != 0 {
		t.Fatalf("unexpected sleeps: %+v", fc.sleeps)
	}
}

func TestLoadMetaRespectsContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	back := &stubBackend{
		loadMetaErrs: []error{
			storage.NewTransientError(errors.New("flaky")),
			storage.NewTransientError(errors.New("flaky")),
		},
		hook: func(attempt int) {
			if attempt == 1 {
				cancel()
			}
		},
	}
	fc := &fakeClock{}
	wrapped := retry.Wrap(back, loggingutil.NoopLogger(), fc, retry.Config{MaxAttempts: 5})
	_, _, err := wrapped.LoadMeta(ctx, "key")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancelled error, got %v", err)
	}
	if back.loadMetaCalls != 1 {
		t.Fatalf("expected single attempt, got %d", back.loadMetaCalls)
	}
	if len(fc.sleeps) != 0 {
		t.Fatalf("expected no sleeps when context cancelled, got %v", fc.sleeps)
	}
}
