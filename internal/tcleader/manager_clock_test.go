package tcleader

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/pslog"
)

func TestManagerElectionUsesClock(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint},
		LeaseTTL:     5 * time.Second,
		Logger:       pslog.NoopLogger(),
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	ok := manager.tryElect(context.Background(), manager.endpointsSnapshot(), randSource(1))
	if !ok {
		t.Fatalf("expected election to succeed")
	}
	info := manager.Leader(start)
	if !info.IsLeader {
		t.Fatalf("expected leader state")
	}
	if info.ExpiresAt != start.Add(5*time.Second) {
		t.Fatalf("unexpected expiry: %v", info.ExpiresAt)
	}
}

func TestManagerRenewUsesClock(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint},
		LeaseTTL:     8 * time.Second,
		Logger:       pslog.NoopLogger(),
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	_, leaseErr := manager.leaseStore.Acquire(start, manager.selfID, manager.selfEndpoint, 1, manager.leaseTTL)
	if leaseErr != nil {
		t.Fatalf("acquire local lease: %v", leaseErr)
	}
	manager.becomeLeader(1)

	clk.Advance(3 * time.Second)
	ok := manager.renewLeases(context.Background(), manager.endpointsSnapshot())
	if !ok {
		t.Fatalf("expected renew to succeed")
	}

	expected := clk.Now().Add(manager.leaseTTL)
	if manager.expiresAt != expected {
		t.Fatalf("unexpected renewal expiry: %v", manager.expiresAt)
	}
}

func TestManagerObserveLeaderUsesClock(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"

	leaderResp := api.TCLeaderResponse{
		LeaderID:       "leader-1",
		LeaderEndpoint: "http://leader",
		Term:           2,
		ExpiresAtUnix:  start.Add(5 * time.Second).UnixMilli(),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tc/leader" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(leaderResp)
	}))
	t.Cleanup(srv.Close)

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint, srv.URL},
		LeaseTTL:     10 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: time.Second},
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	if ok := manager.observeLeader(context.Background(), manager.endpointsSnapshot()); !ok {
		t.Fatalf("expected leader observation to succeed")
	}

	rec := manager.leaseStore.Leader()
	if rec.LeaderID != leaderResp.LeaderID || rec.Term != leaderResp.Term || !rec.Observed {
		t.Fatalf("unexpected record: %+v", rec)
	}
}

func TestManagerObserveLeaderAdoptsSelfLease(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"
	selfID := "node-1"

	leaderResp := api.TCLeaderResponse{
		LeaderID:       selfID,
		LeaderEndpoint: selfEndpoint,
		Term:           3,
		ExpiresAtUnix:  start.Add(7 * time.Second).UnixMilli(),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tc/leader" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(leaderResp)
	}))
	t.Cleanup(srv.Close)

	manager, err := NewManager(Config{
		SelfID:       selfID,
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint, srv.URL},
		LeaseTTL:     10 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: time.Second},
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	_, leaseErr := manager.leaseStore.Acquire(start, selfID, selfEndpoint, leaderResp.Term, manager.leaseTTL)
	if leaseErr != nil {
		t.Fatalf("acquire local lease: %v", leaseErr)
	}

	if ok := manager.observeLeader(context.Background(), manager.endpointsSnapshot()); !ok {
		t.Fatalf("expected leader observation to succeed")
	}

	info := manager.Leader(start)
	if !info.IsLeader {
		t.Fatalf("expected manager to adopt leader lease")
	}
	if info.Term != leaderResp.Term {
		t.Fatalf("expected term %d, got %d", leaderResp.Term, info.Term)
	}
	expectedExpiry := time.UnixMilli(leaderResp.ExpiresAtUnix)
	if !info.ExpiresAt.Equal(expectedExpiry) {
		t.Fatalf("expected expiry %v, got %v", expectedExpiry, info.ExpiresAt)
	}
	if info.LeaderID != selfID || info.LeaderEndpoint != selfEndpoint {
		t.Fatalf("unexpected leader identity: %+v", info)
	}
}

func TestLeaseRoundContextUsesClock(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint},
		LeaseTTL:     6 * time.Second,
		Logger:       pslog.NoopLogger(),
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	ctx, cancel := manager.leaseRoundContext(context.Background())
	t.Cleanup(cancel)

	select {
	case <-ctx.Done():
		t.Fatal("context cancelled before advance")
	default:
	}

	clk.Advance(10 * time.Second)
	select {
	case <-ctx.Done():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected context to cancel after advance")
	}
}

func TestManagerElectsWithMaxTermPlusOne(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"

	peerA := httptest.NewServer(http.HandlerFunc((&leaseStub{leaderTerm: 3, acquireGranted: true, renewGranted: true}).handler))
	peerB := httptest.NewServer(http.HandlerFunc((&leaseStub{leaderTerm: 7, acquireGranted: true, renewGranted: true}).handler))
	t.Cleanup(peerA.Close)
	t.Cleanup(peerB.Close)

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint, peerA.URL, peerB.URL},
		LeaseTTL:     5 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: time.Second},
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	manager.leaseStore.record = LeaseRecord{
		LeaderID:       "old",
		LeaderEndpoint: "http://old",
		Term:           4,
		ExpiresAt:      start.Add(-time.Second),
	}

	if ok := manager.tryElect(context.Background(), manager.endpointsSnapshot(), randSource(1)); !ok {
		t.Fatalf("expected election to succeed")
	}
	if manager.term != 8 {
		t.Fatalf("expected term 8, got %d", manager.term)
	}
	if !manager.isLeaderState() {
		t.Fatalf("expected leader state")
	}
}

func TestManagerStepsDownOnQuorumLoss(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"

	peer := httptest.NewServer(http.HandlerFunc((&leaseStub{leaderTerm: 0, acquireGranted: false, renewGranted: false}).handler))
	t.Cleanup(peer.Close)

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint, peer.URL},
		LeaseTTL:     6 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: time.Second},
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	if _, leaseErr := manager.leaseStore.Acquire(start, manager.selfID, manager.selfEndpoint, 1, manager.leaseTTL); leaseErr != nil {
		t.Fatalf("acquire local lease: %v", leaseErr)
	}
	manager.becomeLeader(1)

	manager.mu.Lock()
	manager.expiresAt = start.Add(-time.Second)
	manager.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go manager.run(ctx)

	deadline := time.Now().Add(500 * time.Millisecond)
	for manager.isLeaderState() {
		if time.Now().After(deadline) {
			t.Fatal("expected manager to step down on quorum loss")
		}
		time.Sleep(5 * time.Millisecond)
	}

	rec := manager.leaseStore.Leader()
	if rec.LeaderID != "" || rec.LeaderEndpoint != "" {
		t.Fatalf("expected lease cleared on step down, got %+v", rec)
	}
}

func TestManagerSetEndpointsDisableEnable(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint, "http://peer"},
		LeaseTTL:     5 * time.Second,
		Logger:       pslog.NoopLogger(),
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	if !manager.Enabled() {
		t.Fatalf("expected manager enabled")
	}
	if err := manager.SetEndpoints(nil); err != nil {
		t.Fatalf("disable endpoints: %v", err)
	}
	if manager.Enabled() {
		t.Fatalf("expected manager disabled after clearing endpoints")
	}
	if err := manager.SetEndpoints([]string{selfEndpoint}); err != nil {
		t.Fatalf("re-enable endpoints: %v", err)
	}
	if !manager.Enabled() {
		t.Fatalf("expected manager enabled after restoring endpoints")
	}
}

func TestManagerSetEndpointsPreservesLeaderLease(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint},
		LeaseTTL:     5 * time.Second,
		Logger:       pslog.NoopLogger(),
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	if _, leaseErr := manager.leaseStore.Acquire(start, manager.selfID, manager.selfEndpoint, 1, manager.leaseTTL); leaseErr != nil {
		t.Fatalf("acquire local lease: %v", leaseErr)
	}
	manager.becomeLeader(1)
	if !manager.isLeaderState() {
		t.Fatalf("expected leader state before SetEndpoints")
	}

	if err := manager.SetEndpoints([]string{selfEndpoint, "http://peer"}); err != nil {
		t.Fatalf("set endpoints: %v", err)
	}
	if !manager.isLeaderState() {
		t.Fatalf("expected leader state preserved after SetEndpoints")
	}
}

func TestManagerBackoffUsesClock(t *testing.T) {
	start := time.Unix(10, 0).UTC()
	clk := clock.NewManual(start)
	selfEndpoint := "http://self"

	acquireCh := make(chan struct{}, 10)
	peer := httptest.NewServer(http.HandlerFunc((&leaseStub{
		leaderTerm:     0,
		acquireGranted: false,
		renewGranted:   false,
		acquireCh:      acquireCh,
	}).handler))
	t.Cleanup(peer.Close)

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint, peer.URL},
		LeaseTTL:     5 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: time.Second},
		DisableMTLS:  true,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go manager.run(ctx)

	skew := manager.electionSkew(defaultElectionBackoff, manager.endpointsSnapshot())
	if skew > 0 {
		clk.Advance(skew / 2)
		select {
		case <-acquireCh:
			t.Fatal("unexpected election attempt before skew elapsed")
		case <-time.After(200 * time.Millisecond):
		}
		clk.Advance(skew)
	}
	select {
	case <-acquireCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected initial election attempt")
	}

	clk.Advance(defaultElectionBackoff / 2)
	select {
	case <-acquireCh:
		t.Fatal("unexpected retry before backoff elapsed")
	case <-time.After(200 * time.Millisecond):
	}

	clk.Advance(defaultElectionBackoff * 2)
	select {
	case <-acquireCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected retry after backoff elapsed")
	}
}

func randSource(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

type leaseStub struct {
	leaderTerm     uint64
	acquireGranted bool
	renewGranted   bool
	acquireCh      chan<- struct{}
}

func (s *leaseStub) handler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/tc/leader":
		_ = json.NewEncoder(w).Encode(api.TCLeaderResponse{Term: s.leaderTerm})
	case "/v1/tc/lease/acquire":
		if s.acquireCh != nil {
			s.acquireCh <- struct{}{}
		}
		var req api.TCLeaseAcquireRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(api.TCLeaseAcquireResponse{
			Granted:       s.acquireGranted,
			Term:          req.Term,
			ExpiresAtUnix: time.Now().Add(5 * time.Second).UnixMilli(),
		})
	case "/v1/tc/lease/renew":
		var req api.TCLeaseRenewRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(api.TCLeaseRenewResponse{
			Renewed:       s.renewGranted,
			Term:          req.Term,
			ExpiresAtUnix: time.Now().Add(5 * time.Second).UnixMilli(),
		})
	case "/v1/tc/lease/release":
		_ = json.NewEncoder(w).Encode(api.TCLeaseReleaseResponse{Released: true})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}
