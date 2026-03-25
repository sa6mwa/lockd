package core

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
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

type probeSingleWriterBackend struct {
	storage.Backend
	mu             sync.Mutex
	foreignPresent atomic.Bool
	foreignExpiry  atomic.Int64
	probeErr       error
}

func (b *probeSingleWriterBackend) SetSingleWriter(enabled bool) {
	_ = enabled
}

func (b *probeSingleWriterBackend) setForeignPresence(until time.Time) {
	b.foreignExpiry.Store(until.Unix())
	b.foreignPresent.Store(true)
}

func (b *probeSingleWriterBackend) ProbeExclusiveWriter(context.Context) (storage.ExclusiveWriterPresence, error) {
	b.mu.Lock()
	err := b.probeErr
	b.mu.Unlock()
	if err != nil {
		return storage.ExclusiveWriterPresence{}, err
	}
	if !b.foreignPresent.Load() {
		return storage.ExclusiveWriterPresence{}, nil
	}
	expires := b.foreignExpiry.Load()
	if expires <= time.Now().Unix() {
		b.foreignPresent.Store(false)
		return storage.ExclusiveWriterPresence{}, nil
	}
	return storage.ExclusiveWriterPresence{
		Present:       true,
		ExpiresAtUnix: expires,
	}, nil
}

func (b *probeSingleWriterBackend) setProbeError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probeErr = err
}

type failingHAMetaBackend struct {
	storage.Backend
	loadErr error
	listErr error
}

type barrierHAMemberBackend struct {
	storage.Backend

	mu           sync.Mutex
	started      int
	released     bool
	releaseCh    chan struct{}
	storeCalls   int
	listMetaKeys int
}

func newBarrierHAMemberBackend(inner storage.Backend) *barrierHAMemberBackend {
	return &barrierHAMemberBackend{
		Backend:   inner,
		releaseCh: make(chan struct{}),
	}
}

func (b *barrierHAMemberBackend) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	if namespace == haNamespace && strings.HasPrefix(key, haMemberPrefix) && meta != nil {
		if mode, ok := meta.GetAttribute(haMemberModeAttr); ok && strings.EqualFold(strings.TrimSpace(mode), haMemberModeSingle) {
			b.mu.Lock()
			b.storeCalls++
			b.started++
			if b.started == 2 && !b.released {
				close(b.releaseCh)
				b.released = true
			}
			releaseCh := b.releaseCh
			b.mu.Unlock()
			select {
			case <-releaseCh:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
	}
	return b.Backend.StoreMeta(ctx, namespace, key, meta, expectedETag)
}

func (b *barrierHAMemberBackend) StoreCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.storeCalls
}

func (b *failingHAMetaBackend) LoadMeta(ctx context.Context, namespace, key string) (storage.LoadMetaResult, error) {
	if namespace == haNamespace && b.loadErr != nil {
		return storage.LoadMetaResult{}, b.loadErr
	}
	return b.Backend.LoadMeta(ctx, namespace, key)
}

func (b *failingHAMetaBackend) ListMetaKeys(ctx context.Context, namespace string) ([]string, error) {
	if namespace == haNamespace && b.listErr != nil {
		return nil, b.listErr
	}
	return b.Backend.ListMetaKeys(ctx, namespace)
}

func TestHARefreshIntervalUsesSeventyPercentTTL(t *testing.T) {
	t.Parallel()

	if got := haRefreshInterval(10 * time.Second); got != 7*time.Second {
		t.Fatalf("10s ttl interval=%s want %s", got, 7*time.Second)
	}
	if got := haRefreshInterval(5 * time.Second); got != 3500*time.Millisecond {
		t.Fatalf("5s ttl interval=%s want %s", got, 3500*time.Millisecond)
	}
	if got := haRefreshInterval(250 * time.Millisecond); got != 500*time.Millisecond {
		t.Fatalf("250ms ttl interval=%s want %s", got, 500*time.Millisecond)
	}
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
	member := waitForHAMember(t, mem, "single-node")
	mode, ok := member.Attributes[haMemberModeAttr]
	if !ok || mode != haMemberModeSingle {
		t.Fatalf("expected single member mode %q, got %q", haMemberModeSingle, mode)
	}
}

func TestHASingleModeStartsPassiveUntilPresenceRefreshSucceeds(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &failingHAMetaBackend{Backend: mem, loadErr: errors.New("ha member load failed")}

	svc := New(Config{
		Store:    store,
		HAMode:   "single",
		HANodeID: "single-node",
		Logger:   pslog.NoopLogger(),
	})
	t.Cleanup(svc.StopHA)

	if svc.NodeActive() {
		t.Fatal("expected single mode node to stay passive until presence refresh succeeds")
	}
	if err := svc.RequireNodeActive(); err == nil {
		t.Fatal("expected RequireNodeActive to fail while single mode fencing is not established")
	}
}

func TestHASingleModeSerializesConcurrentStartup(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := newBarrierHAMemberBackend(mem)

	var svcA, svcB *Service
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		svcA = New(Config{
			Store:               store,
			HAMode:              "single",
			HASinglePresenceTTL: 5 * time.Second,
			HANodeID:            "single-a",
			Logger:              pslog.NoopLogger(),
		})
	}()
	go func() {
		defer wg.Done()
		svcB = New(Config{
			Store:               store,
			HAMode:              "single",
			HASinglePresenceTTL: 5 * time.Second,
			HANodeID:            "single-b",
			Logger:              pslog.NoopLogger(),
		})
	}()
	wg.Wait()
	t.Cleanup(func() {
		if svcA != nil {
			svcA.StopHA()
		}
		if svcB != nil {
			svcB.StopHA()
		}
	})

	if svcA == nil || svcB == nil {
		t.Fatal("expected both single services to start")
	}
	if store.StoreCalls() < 2 {
		t.Fatalf("expected both single services to publish membership during startup, got %d stores", store.StoreCalls())
	}
	if svcA.NodeActive() == svcB.NodeActive() {
		t.Fatalf("expected exactly one single node active after concurrent startup; a=%v b=%v", svcA.NodeActive(), svcB.NodeActive())
	}
}

func TestHAAutoStartsPassiveUntilRefreshSucceeds(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &failingHAMetaBackend{Backend: mem, loadErr: errors.New("ha member load failed")}

	svc := New(Config{
		Store:      store,
		HAMode:     "auto",
		HALeaseTTL: time.Second,
		HANodeID:   "auto-node",
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(svc.StopHA)

	if svc.NodeActive() {
		t.Fatal("expected auto mode node to stay passive until fencing refresh succeeds")
	}
	if err := svc.RequireNodeActive(); err == nil {
		t.Fatal("expected RequireNodeActive to fail while auto mode fencing is not established")
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
	if !passiveStore.saw(false) {
		t.Fatal("expected passive node to disable single-writer mode after promotion")
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

func TestHAAutoStaysPassiveWhenSingleMemberPresent(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })

	single := New(Config{
		Store:               mem,
		HAMode:              "single",
		HASinglePresenceTTL: 1500 * time.Millisecond,
		HANodeID:            "single-node",
		Logger:              pslog.NoopLogger(),
	})
	t.Cleanup(single.StopHA)

	auto := New(Config{
		Store:      mem,
		HAMode:     "auto",
		HALeaseTTL: time.Second,
		HANodeID:   "auto-node",
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(auto.StopHA)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !auto.NodeActive() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if auto.NodeActive() {
		t.Fatal("expected auto node to remain passive while single member is live")
	}
	if auto.usesHALease() {
		t.Fatal("expected auto node to avoid failover lease when fenced by single member")
	}
	if err := auto.RequireNodeActive(); err == nil {
		t.Fatal("expected auto node active check to fail while single member is live")
	}
	_, err := mem.LoadMeta(context.Background(), haNamespace, haLeaseKey)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no activelease record, got %v", err)
	}
}

func TestHAAutoPrefersLiveSingleMemberOverPassiveAutoPeer(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })

	manual := clock.NewManual(time.Unix(1_700_000_000, 0).UTC())

	single := New(Config{
		Store:               mem,
		HAMode:              "single",
		HASinglePresenceTTL: 5 * time.Second,
		HANodeID:            "single-z",
		Clock:               manual,
		Logger:              pslog.NoopLogger(),
	})
	t.Cleanup(single.StopHA)
	single.StopHA()
	single.singleModeRefresh()

	autoPeer := New(Config{
		Store:      mem,
		HAMode:     "auto",
		HALeaseTTL: 2 * time.Second,
		HANodeID:   "auto-a",
		Clock:      manual,
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(autoPeer.StopHA)
	autoPeer.StopHA()
	autoPeer.autoHARefresh()
	if autoPeer.NodeActive() {
		t.Fatal("expected first auto peer to stay passive while single presence is live")
	}

	autoCandidate := New(Config{
		Store:      mem,
		HAMode:     "auto",
		HALeaseTTL: 2 * time.Second,
		HANodeID:   "auto-b",
		Clock:      manual,
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(autoCandidate.StopHA)
	autoCandidate.StopHA()
	autoCandidate.autoHARefresh()

	if autoCandidate.NodeActive() {
		t.Fatal("expected second auto peer to stay passive while single presence is live")
	}
	if autoCandidate.usesHALease() {
		t.Fatal("expected second auto peer to avoid failover lease while single presence is live")
	}
	if err := autoCandidate.RequireNodeActive(); err == nil {
		t.Fatal("expected second auto peer to reject writes while single presence is live")
	}
}

func TestHAAutoSinglePresenceExpiryPromotesToFailoverAndFencesRejoin(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })

	manual := clock.NewManual(time.Unix(1_700_000_000, 0).UTC())

	single := New(Config{
		Store:               mem,
		HAMode:              "single",
		HASinglePresenceTTL: 5 * time.Second,
		HANodeID:            "single-node",
		Clock:               manual,
		Logger:              pslog.NoopLogger(),
	})
	t.Cleanup(single.StopHA)
	single.StopHA()
	single.singleModeRefresh()

	auto := New(Config{
		Store:      mem,
		HAMode:     "auto",
		HALeaseTTL: 2 * time.Second,
		HANodeID:   "auto-node",
		Clock:      manual,
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(auto.StopHA)
	auto.StopHA()

	auto.autoHARefresh()
	if auto.NodeActive() {
		t.Fatal("expected auto node to stay passive while single presence is live")
	}
	if err := auto.RequireNodeActive(); err == nil {
		t.Fatal("expected single presence to fence auto node")
	} else {
		failure, ok := err.(Failure)
		if !ok {
			t.Fatalf("expected Failure, got %T", err)
		}
		if failure.Code != "node_passive" {
			t.Fatalf("expected node_passive, got %q", failure.Code)
		}
		if failure.RetryAfter < 4 {
			t.Fatalf("expected retry_after to reflect single presence ttl, got %d", failure.RetryAfter)
		}
	}

	manual.Advance(6 * time.Second)
	auto.autoHARefresh()
	if !auto.NodeActive() {
		t.Fatal("expected auto node to become active after single presence expiry")
	}
	if !auto.usesHALease() {
		t.Fatal("expected auto node to promote into failover ownership after single presence expiry")
	}
	if _, _, err := auto.loadHALease(context.Background()); err != nil {
		t.Fatalf("expected promoted auto node to publish activelease, got %v", err)
	}
	if err := auto.RequireNodeActive(); err != nil {
		t.Fatalf("expected promoted auto node to remain writable after expiry, got %v", err)
	}

	rejoin := New(Config{
		Store:               mem,
		HAMode:              "single",
		HASinglePresenceTTL: 5 * time.Second,
		HANodeID:            "single-rejoin",
		Clock:               manual,
		Logger:              pslog.NoopLogger(),
	})
	t.Cleanup(rejoin.StopHA)
	rejoin.StopHA()
	rejoin.singleModeRefresh()

	if rejoin.NodeActive() {
		t.Fatal("expected rejoining single node to stay passive after auto promotion")
	}
	if err := rejoin.RequireNodeActive(); err == nil {
		t.Fatal("expected rejoining single node to reject writes after auto promotion")
	}
}

func TestHAAutoPassiveRetryAfterUsesBackendExclusiveWriterExpiry(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &probeSingleWriterBackend{Backend: mem}
	store.setForeignPresence(time.Now().Add(2 * time.Second))

	auto := New(Config{
		Store:      store,
		HAMode:     "auto",
		HALeaseTTL: time.Second,
		HANodeID:   "auto-node",
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(auto.StopHA)

	auto.autoHARefresh()
	if auto.NodeActive() {
		t.Fatal("expected backend exclusive writer to fence auto node")
	}
	err := auto.RequireNodeActive()
	if err == nil {
		t.Fatal("expected node_passive while backend exclusive writer is present")
	}
	failure, ok := err.(Failure)
	if !ok {
		t.Fatalf("expected Failure, got %T", err)
	}
	if failure.Code != "node_passive" {
		t.Fatalf("expected node_passive, got %q", failure.Code)
	}
	if failure.RetryAfter <= 0 {
		t.Fatalf("expected positive retry_after while backend exclusive writer is present, got %d", failure.RetryAfter)
	}
}

func TestHAAutoPromotesAfterNativeExclusiveWriterClears(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &probeSingleWriterBackend{Backend: mem}
	store.setForeignPresence(time.Now().Add(30 * time.Second))

	auto := New(Config{
		Store:      store,
		HAMode:     "auto",
		HALeaseTTL: 2 * time.Second,
		HANodeID:   "auto-node",
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(auto.StopHA)
	auto.StopHA()

	auto.autoHARefresh()
	if auto.NodeActive() {
		t.Fatal("expected auto node to stay passive while native exclusive writer is present")
	}
	if auto.usesHALease() {
		t.Fatal("expected auto node to remain in auto mode while native exclusive writer is present")
	}

	store.setForeignPresence(time.Time{})
	auto.autoHARefresh()

	if !auto.usesHALease() {
		t.Fatal("expected auto node to promote into failover ownership once native exclusive writer clears")
	}
	if !auto.NodeActive() {
		t.Fatal("expected auto node to become active once native exclusive writer clears")
	}
	if _, _, err := auto.loadHALease(context.Background()); err != nil {
		t.Fatalf("expected promoted auto node to publish activelease after native exclusive writer clears, got %v", err)
	}
}

func TestHAAutoPromotesAfterNativeExclusiveWriterFencesAnAlreadyActiveNode(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &probeSingleWriterBackend{Backend: mem}

	auto := New(Config{
		Store:      store,
		HAMode:     "auto",
		HALeaseTTL: 2 * time.Second,
		HANodeID:   "auto-node",
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(auto.StopHA)
	auto.StopHA()

	auto.autoHARefresh()
	if !auto.NodeActive() {
		t.Fatal("expected auto node to start active when no native exclusive writer is present")
	}
	if auto.usesHALease() {
		t.Fatal("expected auto node to remain in auto mode before any fencing event")
	}

	store.setForeignPresence(time.Now().Add(30 * time.Second))
	auto.autoHARefresh()
	if auto.NodeActive() {
		t.Fatal("expected native exclusive writer to fence an already active auto node")
	}
	if auto.usesHALease() {
		t.Fatal("expected fenced auto node to remain out of failover mode while foreign writer is still present")
	}

	store.setForeignPresence(time.Time{})
	auto.autoHARefresh()

	if !auto.usesHALease() {
		t.Fatal("expected fenced auto node to promote into failover ownership once native exclusive writer clears")
	}
	if !auto.NodeActive() {
		t.Fatal("expected fenced auto node to become active again once native exclusive writer clears")
	}
	if _, _, err := auto.loadHALease(context.Background()); err != nil {
		t.Fatalf("expected promoted auto node to publish activelease after native exclusive writer clears, got %v", err)
	}
}

func TestHAAutoPromotesWhenSingleFenceClearsEarly(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })

	manual := clock.NewManual(time.Unix(1_700_000_000, 0).UTC())

	single := New(Config{
		Store:               mem,
		HAMode:              "single",
		HASinglePresenceTTL: 30 * time.Second,
		HANodeID:            "single-node",
		Clock:               manual,
		Logger:              pslog.NoopLogger(),
	})
	t.Cleanup(single.StopHA)
	single.StopHA()
	single.singleModeRefresh()

	auto := New(Config{
		Store:      mem,
		HAMode:     "auto",
		HALeaseTTL: 2 * time.Second,
		HANodeID:   "auto-node",
		Clock:      manual,
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(auto.StopHA)
	auto.StopHA()

	auto.autoHARefresh()
	if auto.NodeActive() {
		t.Fatal("expected auto node to stay passive while single presence is live")
	}

	single.ReleaseHA(context.Background())
	auto.autoHARefresh()

	if !auto.usesHALease() {
		t.Fatal("expected auto node to promote into failover ownership when single fence clears early")
	}
	if !auto.NodeActive() {
		t.Fatal("expected auto node to become active when single fence clears early")
	}
}

func TestHASingleRejoinHonorsExistingFailoverLease(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })

	manual := clock.NewManual(time.Unix(1_700_000_000, 0).UTC())

	auto := New(Config{
		Store:      mem,
		HAMode:     "failover",
		HALeaseTTL: 5 * time.Second,
		HANodeID:   "auto-node",
		Clock:      manual,
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(auto.StopHA)
	auto.StopHA()
	auto.haRefresh()
	if !auto.NodeActive() {
		t.Fatal("expected failover node to hold active lease")
	}

	single := New(Config{
		Store:               mem,
		HAMode:              "single",
		HASinglePresenceTTL: 30 * time.Second,
		HANodeID:            "single-node",
		Clock:               manual,
		Logger:              pslog.NoopLogger(),
	})
	t.Cleanup(single.StopHA)
	single.StopHA()
	single.singleModeRefresh()

	if single.NodeActive() {
		t.Fatal("expected rejoining single node to stay passive while failover lease is active")
	}
	if err := single.RequireNodeActive(); err == nil {
		t.Fatal("expected rejoining single node to reject writes while failover lease is active")
	}
	keys, err := mem.ListMetaKeys(context.Background(), haNamespace)
	if err != nil {
		t.Fatalf("list ha keys: %v", err)
	}
	for _, key := range keys {
		if key == haMemberKey("single-node") {
			t.Fatalf("expected passive rejoining single node to avoid re-advertising itself, found %q", key)
		}
	}
}

func TestHASingleIgnoresPassiveAutoMember(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })

	manual := clock.NewManual(time.Unix(1_700_000_000, 0).UTC())

	single := New(Config{
		Store:               mem,
		HAMode:              "single",
		HASinglePresenceTTL: 30 * time.Second,
		HANodeID:            "single-node",
		Clock:               manual,
		Logger:              pslog.NoopLogger(),
	})
	t.Cleanup(single.StopHA)
	single.StopHA()

	auto := New(Config{
		Store:      mem,
		HAMode:     "auto",
		HALeaseTTL: 5 * time.Second,
		HANodeID:   "auto-node",
		Clock:      manual,
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(auto.StopHA)
	auto.StopHA()

	auto.autoHARefresh()
	single.singleModeRefresh()

	if !single.NodeActive() {
		t.Fatal("expected single node to remain active while auto peer is only advertising membership")
	}
	if err := single.RequireNodeActive(); err != nil {
		t.Fatalf("expected single node writes to remain allowed, got %v", err)
	}
	if auto.usesHALease() {
		t.Fatal("expected passive auto peer to avoid failover lease")
	}
}

func TestHASingleModeSkipsHAMemberWhenBackendProvidesExclusiveWriterProbe(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &probeSingleWriterBackend{Backend: mem}

	svc := New(Config{
		Store:               store,
		HAMode:              "single",
		HASinglePresenceTTL: time.Second,
		HANodeID:            "single-node",
		Logger:              pslog.NoopLogger(),
	})
	t.Cleanup(svc.StopHA)

	if !svc.NodeActive() {
		t.Fatal("expected single mode node to be active")
	}
	keys, err := mem.ListMetaKeys(context.Background(), haNamespace)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("list ha keys: %v", err)
	}
	for _, key := range keys {
		if strings.HasPrefix(key, haMemberPrefix) {
			t.Fatalf("expected backend-native single-writer presence to avoid HA member writes, found %q", key)
		}
	}
}

func TestHASingleModePreservesPassiveStateOnExclusiveWriterProbeError(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &probeSingleWriterBackend{Backend: mem}
	store.setProbeError(errors.New("probe failed"))

	svc := New(Config{
		Store:               store,
		HAMode:              "single",
		HASinglePresenceTTL: time.Second,
		HANodeID:            "single-node",
		Logger:              pslog.NoopLogger(),
	})
	t.Cleanup(svc.StopHA)
	svc.StopHA()
	svc.setAutoPassiveUntil(time.Now().Add(time.Second).Unix())
	svc.setNodeActive(false, time.Now().Add(time.Second).Unix(), "")
	svc.singleModeRefresh()

	if svc.NodeActive() {
		t.Fatal("expected probe failure to preserve passive state")
	}
	keys, err := mem.ListMetaKeys(context.Background(), haNamespace)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("list ha keys: %v", err)
	}
	for _, key := range keys {
		if strings.HasPrefix(key, haMemberPrefix) {
			t.Fatalf("expected probe failure to avoid HA member writes, found %q", key)
		}
	}
}

func TestHAAutoStaysPassiveWhenBackendExclusiveWriterPresent(t *testing.T) {
	mem := memory.New()
	t.Cleanup(func() { _ = mem.Close() })
	store := &probeSingleWriterBackend{Backend: mem}
	store.setForeignPresence(time.Now().Add(2 * time.Second))

	single := New(Config{
		Store:    store,
		HAMode:   "single",
		HANodeID: "single-node",
		Logger:   pslog.NoopLogger(),
	})
	t.Cleanup(single.StopHA)

	auto := New(Config{
		Store:      store,
		HAMode:     "auto",
		HALeaseTTL: time.Second,
		HANodeID:   "auto-node",
		Logger:     pslog.NoopLogger(),
	})
	t.Cleanup(auto.StopHA)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !auto.NodeActive() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if auto.NodeActive() {
		t.Fatal("expected auto node to remain passive while backend exclusive writer is present")
	}
	if auto.usesHALease() {
		t.Fatal("expected auto node to avoid failover lease when backend exclusive writer is present")
	}
}

func TestHAAutoPromotesAfterAbruptDiskSingleCrash(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	singleStore, err := disk.New(disk.Config{Root: root})
	if err != nil {
		t.Fatalf("new single store: %v", err)
	}
	defer singleStore.Close()
	autoStore, err := disk.New(disk.Config{Root: root})
	if err != nil {
		t.Fatalf("new auto store: %v", err)
	}
	defer autoStore.Close()

	single := New(Config{
		Store:    singleStore,
		HAMode:   "single",
		HANodeID: "single-node",
		Logger:   pslog.NoopLogger(),
	})
	defer single.StopHA()

	auto := New(Config{
		Store:      autoStore,
		HAMode:     "auto",
		HALeaseTTL: 5 * time.Second,
		HANodeID:   "auto-node",
		Logger:     pslog.NoopLogger(),
	})
	defer auto.StopHA()

	passiveDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(passiveDeadline) {
		if !auto.NodeActive() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if auto.NodeActive() {
		t.Fatal("expected auto node to become passive while single disk writer is present")
	}

	if err := singleStore.Abort(); err != nil {
		t.Fatalf("abort single store: %v", err)
	}

	promoteDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(promoteDeadline) {
		if auto.NodeActive() && auto.usesHALease() {
			if _, _, err := auto.loadHALease(context.Background()); err == nil {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("expected auto node to promote after abrupt single-writer crash")
}

func waitForHAMember(t testing.TB, backend storage.Backend, nodeID string) *storage.Meta {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	key := haMemberKey(nodeID)
	for time.Now().Before(deadline) {
		res, err := backend.LoadMeta(context.Background(), haNamespace, key)
		if err == nil && res.Meta != nil && res.Meta.Lease != nil {
			return res.Meta
		}
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("load ha member: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for HA member %q", key)
	return nil
}
