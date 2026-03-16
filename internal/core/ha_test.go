package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

type trackingSingleWriterBackend struct {
	storage.Backend
	mu      sync.Mutex
	changes []bool
}

func (b *trackingSingleWriterBackend) SetSingleWriter(enabled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.changes = append(b.changes, enabled)
}

func (b *trackingSingleWriterBackend) saw(enabled bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, change := range b.changes {
		if change == enabled {
			return true
		}
	}
	return false
}

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

func TestHANodeIDUsesConfiguredIdentity(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	svc := New(Config{
		Store:      store,
		HAMode:     "failover",
		HALeaseTTL: time.Second,
		HANodeID:   "node-a",
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(svc.StopHA)

	if svc.haNodeID != "node-a" {
		t.Fatalf("expected configured ha node id, got %q", svc.haNodeID)
	}
}

func TestHANodeIDFallsBackToGeneratedIdentity(t *testing.T) {
	store := memory.New()
	t.Cleanup(func() { _ = store.Close() })

	svc := New(Config{
		Store:      store,
		HAMode:     "failover",
		HALeaseTTL: time.Second,
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(svc.StopHA)

	if svc.haNodeID == "" {
		t.Fatal("expected generated ha node id")
	}
}

func TestHASingleModeStartsActiveWithoutLeaseWrites(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &trackingSingleWriterBackend{Backend: mem}

	svc := New(Config{
		Store:    store,
		HAMode:   "single",
		HANodeID: "single-node",
		Logger:   pslog.NoopLogger(),
	})
	t.Cleanup(svc.StopHA)

	if !svc.NodeActive() {
		t.Fatal("expected single mode node to be active")
	}
	if svc.usesHALease() {
		t.Fatal("expected single mode to avoid HA lease")
	}
	if !store.saw(true) {
		t.Fatal("expected single mode to enable single-writer mode")
	}
	_, err := mem.LoadMeta(context.Background(), haNamespace, haLeaseKey)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no activelease record, got %v", err)
	}
}

func TestHAAutoPromotesToFailoverOnPeerDetection(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })

	storeA := &trackingSingleWriterBackend{Backend: mem}
	storeB := &trackingSingleWriterBackend{Backend: mem}

	svcA := New(Config{
		Store:      storeA,
		HAMode:     "auto",
		HALeaseTTL: time.Second,
		HANodeID:   "node-a",
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(svcA.StopHA)

	if svcA.usesHALease() {
		t.Fatal("expected auto mode to start without HA lease")
	}

	svcB := New(Config{
		Store:      storeB,
		HAMode:     "auto",
		HALeaseTTL: time.Second,
		HANodeID:   "node-b",
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(svcB.StopHA)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if svcA.usesHALease() && svcB.usesHALease() && svcA.NodeActive() != svcB.NodeActive() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !svcA.usesHALease() || !svcB.usesHALease() {
		t.Fatalf("expected both auto nodes to promote to failover; a=%v b=%v", svcA.usesHALease(), svcB.usesHALease())
	}
	if svcA.NodeActive() == svcB.NodeActive() {
		t.Fatalf("expected exactly one active node after promotion; a=%v b=%v", svcA.NodeActive(), svcB.NodeActive())
	}

	activeStore := storeA
	passiveStore := storeB
	if svcB.NodeActive() {
		activeStore = storeB
		passiveStore = storeA
	}
	if !activeStore.saw(true) {
		t.Fatal("expected promoted active node to enable single-writer mode")
	}
	if passiveStore.saw(true) {
		t.Fatal("expected passive node to avoid single-writer mode")
	}

	metaRes, err := mem.LoadMeta(context.Background(), haNamespace, haLeaseKey)
	if err != nil {
		t.Fatalf("load activelease: %v", err)
	}
	if metaRes.Meta == nil || metaRes.Meta.Lease == nil {
		t.Fatal("expected activelease after auto promotion")
	}
	owner := metaRes.Meta.Lease.Owner
	if owner != "node-a" && owner != "node-b" {
		t.Fatalf("unexpected activelease owner %q", owner)
	}
}
