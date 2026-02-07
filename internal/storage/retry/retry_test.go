package retry_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/retry"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
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

	writeStateErrs   []error
	writeStateCalls  int
	writeStateBodies []string

	putObjectErrs   []error
	putObjectCalls  int
	putObjectBodies []string
}

func (s *stubBackend) LoadMeta(ctx context.Context, namespace, key string) (storage.LoadMetaResult, error) {
	s.loadMetaCalls++
	if s.hook != nil {
		s.hook(s.loadMetaCalls)
	}
	var err error
	if idx := s.loadMetaCalls - 1; idx < len(s.loadMetaErrs) {
		err = s.loadMetaErrs[idx]
	}
	if err != nil {
		return storage.LoadMetaResult{}, err
	}
	return storage.LoadMetaResult{
		Meta: &storage.Meta{Version: int64(s.loadMetaCalls)},
		ETag: fmt.Sprintf("etag-%d", s.loadMetaCalls),
	}, nil
}

func (s *stubBackend) StoreMeta(context.Context, string, string, *storage.Meta, string) (string, error) {
	return "", storage.ErrNotImplemented
}

func (s *stubBackend) DeleteMeta(context.Context, string, string, string) error {
	return storage.ErrNotImplemented
}

func (s *stubBackend) ListMetaKeys(context.Context, string) ([]string, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubBackend) ReadState(context.Context, string, string) (storage.ReadStateResult, error) {
	return storage.ReadStateResult{Reader: io.NopCloser(bytes.NewReader(nil))}, storage.ErrNotImplemented
}

func (s *stubBackend) WriteState(_ context.Context, _ string, _ string, body io.Reader, _ storage.PutStateOptions) (*storage.PutStateResult, error) {
	s.writeStateCalls++
	if body != nil {
		data, err := io.ReadAll(body)
		if err != nil {
			return nil, err
		}
		s.writeStateBodies = append(s.writeStateBodies, string(data))
	} else {
		s.writeStateBodies = append(s.writeStateBodies, "")
	}
	var err error
	if idx := s.writeStateCalls - 1; idx < len(s.writeStateErrs) {
		err = s.writeStateErrs[idx]
	}
	if err != nil {
		return nil, err
	}
	return &storage.PutStateResult{
		BytesWritten: int64(len(s.writeStateBodies[s.writeStateCalls-1])),
		NewETag:      fmt.Sprintf("state-etag-%d", s.writeStateCalls),
	}, nil
}

func (s *stubBackend) Remove(context.Context, string, string, string) error {
	return storage.ErrNotImplemented
}

func (s *stubBackend) ListObjects(context.Context, string, storage.ListOptions) (*storage.ListResult, error) {
	return nil, storage.ErrNotImplemented
}

func (s *stubBackend) GetObject(context.Context, string, string) (storage.GetObjectResult, error) {
	return storage.GetObjectResult{}, storage.ErrNotImplemented
}

func (s *stubBackend) PutObject(_ context.Context, _ string, _ string, body io.Reader, _ storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	s.putObjectCalls++
	if body != nil {
		data, err := io.ReadAll(body)
		if err != nil {
			return nil, err
		}
		s.putObjectBodies = append(s.putObjectBodies, string(data))
	} else {
		s.putObjectBodies = append(s.putObjectBodies, "")
	}
	var err error
	if idx := s.putObjectCalls - 1; idx < len(s.putObjectErrs) {
		err = s.putObjectErrs[idx]
	}
	if err != nil {
		return nil, err
	}
	return &storage.ObjectInfo{
		Key:  "obj",
		ETag: fmt.Sprintf("obj-etag-%d", s.putObjectCalls),
		Size: int64(len(s.putObjectBodies[s.putObjectCalls-1])),
	}, nil
}

func (s *stubBackend) DeleteObject(context.Context, string, string, storage.DeleteObjectOptions) error {
	return storage.ErrNotImplemented
}

func (s *stubBackend) BackendHash(context.Context) (string, error) {
	return "stub-backend", nil
}

func (s *stubBackend) Close() error { return nil }

func TestWrapReturnsNilOnNilInner(t *testing.T) {
	t.Parallel()

	if retry.Wrap(nil, pslog.NoopLogger(), &fakeClock{}, retry.Config{}) != nil {
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
	wrapped := retry.Wrap(back, pslog.NoopLogger(), fc, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
		Multiplier:  2,
		MaxDelay:    10 * time.Millisecond,
	})
	metaRes, err := wrapped.LoadMeta(context.Background(), namespaces.Default, "key")
	if err != nil {
		t.Fatalf("LoadMeta returned error: %v", err)
	}
	if metaRes.Meta == nil || metaRes.Meta.Version != 2 {
		t.Fatalf("unexpected meta: %#v", metaRes.Meta)
	}
	if metaRes.ETag != "etag-2" {
		t.Fatalf("unexpected etag: %q", metaRes.ETag)
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
	wrapped := retry.Wrap(back, pslog.NoopLogger(), fc, retry.Config{MaxAttempts: 3})
	_, err := wrapped.LoadMeta(context.Background(), namespaces.Default, "key")
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
			storage.NewTransientError(errors.New("flaky retry")),
		},
		hook: func(attempt int) {
			if attempt == 1 {
				cancel()
			}
		},
	}
	fc := &fakeClock{}
	wrapped := retry.Wrap(back, pslog.NoopLogger(), fc, retry.Config{MaxAttempts: 5})
	_, err := wrapped.LoadMeta(ctx, namespaces.Default, "key")
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

func TestWriteStateRetriesReplayableBody(t *testing.T) {
	t.Parallel()

	back := &stubBackend{
		writeStateErrs: []error{
			storage.NewTransientError(errors.New("temporary")),
			nil,
		},
	}
	fc := &fakeClock{}
	wrapped := retry.Wrap(back, pslog.NoopLogger(), fc, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
		Multiplier:  2,
		MaxDelay:    10 * time.Millisecond,
	})

	body := bytes.NewReader([]byte(`{"v":1}`))
	res, err := wrapped.WriteState(context.Background(), namespaces.Default, "key", body, storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("WriteState returned error: %v", err)
	}
	if res == nil || res.NewETag == "" {
		t.Fatalf("expected write result, got %#v", res)
	}
	if back.writeStateCalls != 2 {
		t.Fatalf("expected 2 write attempts, got %d", back.writeStateCalls)
	}
	if len(back.writeStateBodies) != 2 || back.writeStateBodies[0] != `{"v":1}` || back.writeStateBodies[1] != `{"v":1}` {
		t.Fatalf("unexpected write payloads: %#v", back.writeStateBodies)
	}
}

func TestWriteStateFailsFastForNonReplayableBody(t *testing.T) {
	t.Parallel()

	back := &stubBackend{
		writeStateErrs: []error{
			storage.NewTransientError(errors.New("temporary")),
			nil,
		},
	}
	fc := &fakeClock{}
	wrapped := retry.Wrap(back, pslog.NoopLogger(), fc, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
	})

	_, err := wrapped.WriteState(context.Background(), namespaces.Default, "key", bytes.NewBufferString(`{"v":1}`), storage.PutStateOptions{})
	if err == nil {
		t.Fatal("expected fail-fast error for non-replayable body")
	}
	if !errors.Is(err, retry.ErrNonReplayableBody) {
		t.Fatalf("expected ErrNonReplayableBody, got %v", err)
	}
	if back.writeStateCalls != 1 {
		t.Fatalf("expected single write attempt, got %d", back.writeStateCalls)
	}
	if len(back.writeStateBodies) != 1 || back.writeStateBodies[0] != `{"v":1}` {
		t.Fatalf("unexpected write payloads: %#v", back.writeStateBodies)
	}
	if len(fc.sleeps) != 0 {
		t.Fatalf("expected no sleep before fail-fast, got %d", len(fc.sleeps))
	}
}

func TestPutObjectRetriesReplayableBody(t *testing.T) {
	t.Parallel()

	back := &stubBackend{
		putObjectErrs: []error{
			storage.NewTransientError(errors.New("temporary")),
			nil,
		},
	}
	fc := &fakeClock{}
	wrapped := retry.Wrap(back, pslog.NoopLogger(), fc, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
	})

	info, err := wrapped.PutObject(context.Background(), namespaces.Default, "obj", bytes.NewReader([]byte("payload")), storage.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObject returned error: %v", err)
	}
	if info == nil || info.ETag == "" {
		t.Fatalf("expected object info, got %#v", info)
	}
	if back.putObjectCalls != 2 {
		t.Fatalf("expected 2 put attempts, got %d", back.putObjectCalls)
	}
	if len(back.putObjectBodies) != 2 || back.putObjectBodies[0] != "payload" || back.putObjectBodies[1] != "payload" {
		t.Fatalf("unexpected put payloads: %#v", back.putObjectBodies)
	}
}

func TestPutObjectFailsFastForNonReplayableBody(t *testing.T) {
	t.Parallel()

	back := &stubBackend{
		putObjectErrs: []error{
			storage.NewTransientError(errors.New("temporary")),
			nil,
		},
	}
	fc := &fakeClock{}
	wrapped := retry.Wrap(back, pslog.NoopLogger(), fc, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   5 * time.Millisecond,
	})

	_, err := wrapped.PutObject(context.Background(), namespaces.Default, "obj", bytes.NewBufferString("payload"), storage.PutObjectOptions{})
	if err == nil {
		t.Fatal("expected fail-fast error for non-replayable body")
	}
	if !errors.Is(err, retry.ErrNonReplayableBody) {
		t.Fatalf("expected ErrNonReplayableBody, got %v", err)
	}
	if back.putObjectCalls != 1 {
		t.Fatalf("expected single put attempt, got %d", back.putObjectCalls)
	}
	if len(back.putObjectBodies) != 1 || back.putObjectBodies[0] != "payload" {
		t.Fatalf("unexpected put payloads: %#v", back.putObjectBodies)
	}
	if len(fc.sleeps) != 0 {
		t.Fatalf("expected no sleep before fail-fast, got %d", len(fc.sleeps))
	}
}

func TestReplayContractTableDriven(t *testing.T) {
	t.Parallel()

	type opCase struct {
		name          string
		op            string
		call          func(storage.Backend) error
		expectedCalls int
		expectErr     error
	}

	newReplayableBody := func() io.Reader { return bytes.NewReader([]byte(`{"v":1}`)) }
	newNonReplayableBody := func() io.Reader { return bytes.NewBufferString(`{"v":1}`) }
	ops := []opCase{
		{
			name: "write_state/replayable_retries",
			op:   "write_state",
			call: func(backend storage.Backend) error {
				_, err := backend.WriteState(context.Background(), namespaces.Default, "key", newReplayableBody(), storage.PutStateOptions{})
				return err
			},
			expectedCalls: 2,
		},
		{
			name: "write_state/non_replayable_fail_fast",
			op:   "write_state",
			call: func(backend storage.Backend) error {
				_, err := backend.WriteState(context.Background(), namespaces.Default, "key", newNonReplayableBody(), storage.PutStateOptions{})
				return err
			},
			expectedCalls: 1,
			expectErr:     retry.ErrNonReplayableBody,
		},
		{
			name: "put_object/replayable_retries",
			op:   "put_object",
			call: func(backend storage.Backend) error {
				_, err := backend.PutObject(context.Background(), namespaces.Default, "obj", newReplayableBody(), storage.PutObjectOptions{})
				return err
			},
			expectedCalls: 2,
		},
		{
			name: "put_object/non_replayable_fail_fast",
			op:   "put_object",
			call: func(backend storage.Backend) error {
				_, err := backend.PutObject(context.Background(), namespaces.Default, "obj", newNonReplayableBody(), storage.PutObjectOptions{})
				return err
			},
			expectedCalls: 1,
			expectErr:     retry.ErrNonReplayableBody,
		},
	}

	for _, tc := range ops {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			back := &stubBackend{
				writeStateErrs: []error{
					storage.NewTransientError(errors.New("temporary")),
					nil,
				},
				putObjectErrs: []error{
					storage.NewTransientError(errors.New("temporary")),
					nil,
				},
			}
			fc := &fakeClock{}
			wrapped := retry.Wrap(back, pslog.NoopLogger(), fc, retry.Config{
				MaxAttempts: 3,
				BaseDelay:   5 * time.Millisecond,
			})
			err := tc.call(wrapped)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected error %v, got %v", tc.expectErr, err)
				}
				if len(fc.sleeps) != 0 {
					t.Fatalf("expected no sleeps for fail-fast, got %v", fc.sleeps)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			switch tc.op {
			case "write_state":
				if back.writeStateCalls != tc.expectedCalls {
					t.Fatalf("write state calls=%d want=%d", back.writeStateCalls, tc.expectedCalls)
				}
			case "put_object":
				if back.putObjectCalls != tc.expectedCalls {
					t.Fatalf("put object calls=%d want=%d", back.putObjectCalls, tc.expectedCalls)
				}
			default:
				t.Fatalf("unknown op type %q", tc.op)
			}
		})
	}
}
