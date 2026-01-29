package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

func TestStopHAUnblocks(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	svc := New(Config{
		Store:      store,
		HAMode:     "failover",
		HALeaseTTL: 500 * time.Millisecond,
		Logger:     pslog.NoopLogger(),
	})

	done := make(chan struct{})
	go func() {
		svc.StopHA()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StopHA did not return")
	}

	// Calling StopHA again should be a no-op.
	svc.StopHA()
}

type casOnceStore struct {
	storage.Backend
	mu       sync.Mutex
	failOnce bool
}

func (s *casOnceStore) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expected string) (string, error) {
	s.mu.Lock()
	if s.failOnce {
		s.failOnce = false
		s.mu.Unlock()
		return "", storage.ErrCASMismatch
	}
	s.mu.Unlock()
	return s.Backend.StoreMeta(ctx, namespace, key, meta, expected)
}

func TestReleaseHARetriesCASMismatch(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &casOnceStore{Backend: mem, failOnce: true}

	svc := New(Config{
		Store:      store,
		HAMode:     "failover",
		HALeaseTTL: 2 * time.Second,
		Logger:     pslog.NoopLogger(),
	})
	svc.haRefresh()
	svc.StopHA()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	svc.ReleaseHA(ctx)

	metaRes, err := store.LoadMeta(ctx, haNamespace, haLeaseKey)
	if err != nil {
		t.Fatalf("load ha lease: %v", err)
	}
	if metaRes.Meta == nil || metaRes.Meta.Lease == nil {
		t.Fatal("expected ha lease meta")
	}
	if metaRes.Meta.Lease.ExpiresAtUnix != 0 {
		t.Fatalf("expected release to expire immediately; got %d", metaRes.Meta.Lease.ExpiresAtUnix)
	}
}
