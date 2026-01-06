package tcleader

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/pslog"
)

type tcStub struct {
	leaderTerm uint64
}

func (s *tcStub) handler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/tc/leader":
		_ = json.NewEncoder(w).Encode(api.TCLeaderResponse{Term: s.leaderTerm})
	case "/v1/tc/lease/acquire":
		var req api.TCLeaseAcquireRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(api.TCLeaseAcquireResponse{Granted: true, Term: req.Term, ExpiresAtUnix: time.Now().Add(15 * time.Second).UnixMilli()})
	case "/v1/tc/lease/renew":
		var req api.TCLeaseRenewRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(api.TCLeaseRenewResponse{Renewed: true, Term: req.Term, ExpiresAtUnix: time.Now().Add(15 * time.Second).UnixMilli()})
	case "/v1/tc/lease/release":
		_ = json.NewEncoder(w).Encode(api.TCLeaseReleaseResponse{Released: true})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

type blockingLeaseStub struct {
	startCh chan<- struct{}
	allow   <-chan struct{}
}

func (s *blockingLeaseStub) handler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/tc/leader":
		_ = json.NewEncoder(w).Encode(api.TCLeaderResponse{Term: 1})
	case "/v1/tc/lease/acquire":
		if s.startCh != nil {
			s.startCh <- struct{}{}
		}
		if s.allow != nil {
			<-s.allow
		}
		var req api.TCLeaseAcquireRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(api.TCLeaseAcquireResponse{
			Granted:       true,
			Term:          req.Term,
			ExpiresAtUnix: time.Now().Add(15 * time.Second).UnixMilli(),
		})
	case "/v1/tc/lease/renew":
		if s.startCh != nil {
			s.startCh <- struct{}{}
		}
		if s.allow != nil {
			<-s.allow
		}
		var req api.TCLeaseRenewRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(api.TCLeaseRenewResponse{
			Renewed:       true,
			Term:          req.Term,
			ExpiresAtUnix: time.Now().Add(15 * time.Second).UnixMilli(),
		})
	case "/v1/tc/lease/release":
		_ = json.NewEncoder(w).Encode(api.TCLeaseReleaseResponse{Released: true})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestTryElectRequiresSelfGrant(t *testing.T) {
	ctx := context.Background()
	peerA := httptest.NewServer(http.HandlerFunc((&tcStub{}).handler))
	defer peerA.Close()
	peerB := httptest.NewServer(http.HandlerFunc((&tcStub{}).handler))
	defer peerB.Close()

	selfEndpoint := "http://self"
	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint, peerA.URL, peerB.URL},
		LeaseTTL:     10 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: time.Second},
		DisableMTLS:  true,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if manager.leaseStore == nil {
		t.Fatalf("expected lease store")
	}
	_, _ = manager.leaseStore.Acquire(time.Now(), "other", "http://other", 5, time.Minute)

	ok := manager.tryElect(ctx, manager.endpointsSnapshot(), rand.New(rand.NewSource(1)))
	if ok {
		t.Fatalf("expected election to fail without self grant")
	}
	if manager.isLeaderState() {
		t.Fatalf("expected not leader")
	}
}

func TestRenewRequiresSelfGrant(t *testing.T) {
	ctx := context.Background()
	peerA := httptest.NewServer(http.HandlerFunc((&tcStub{}).handler))
	defer peerA.Close()
	peerB := httptest.NewServer(http.HandlerFunc((&tcStub{}).handler))
	defer peerB.Close()

	selfEndpoint := "http://self"
	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint, peerA.URL, peerB.URL},
		LeaseTTL:     10 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: time.Second},
		DisableMTLS:  true,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if manager.leaseStore == nil {
		t.Fatalf("expected lease store")
	}
	_, _ = manager.leaseStore.Acquire(time.Now(), "other", "http://other", 9, time.Minute)
	manager.becomeLeader(10)

	ok := manager.renewLeases(ctx, manager.endpointsSnapshot())
	if ok {
		t.Fatalf("expected renew to fail without self grant")
	}
}

func TestSetEndpointsReleasesLeaderLeasesOnDisable(t *testing.T) {
	const term = 7
	selfEndpoint := "http://self"
	var releaseCountA int32
	var releaseCountB int32
	errCh := make(chan error, 4)
	expectedLeader := ""

	handler := func(counter *int32) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/tc/lease/release" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			var req api.TCLeaseReleaseRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.LeaderID != expectedLeader || req.Term != term {
				errCh <- fmt.Errorf("unexpected release: leader_id=%q term=%d", req.LeaderID, req.Term)
			}
			atomic.AddInt32(counter, 1)
			_ = json.NewEncoder(w).Encode(api.TCLeaseReleaseResponse{Released: true})
		}
	}

	peerA := httptest.NewServer(http.HandlerFunc(handler(&releaseCountA)))
	defer peerA.Close()
	peerB := httptest.NewServer(http.HandlerFunc(handler(&releaseCountB)))
	defer peerB.Close()

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint, peerA.URL, peerB.URL},
		LeaseTTL:     10 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: time.Second},
		DisableMTLS:  true,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if manager.leaseStore == nil {
		t.Fatalf("expected lease store")
	}
	if _, leaseErr := manager.leaseStore.Acquire(time.Now(), manager.selfID, manager.selfEndpoint, term, time.Minute); leaseErr != nil {
		t.Fatalf("acquire local lease: code=%s detail=%s term=%d leader=%s endpoint=%s", leaseErr.Code, leaseErr.Detail, leaseErr.Term, leaseErr.LeaderID, leaseErr.LeaderEndpoint)
	}
	manager.becomeLeader(term)
	expectedLeader = manager.selfID

	if err := manager.SetEndpoints(nil); err != nil {
		t.Fatalf("set endpoints: %v", err)
	}
	if got := atomic.LoadInt32(&releaseCountA); got != 1 {
		t.Fatalf("expected peerA release once, got %d", got)
	}
	if got := atomic.LoadInt32(&releaseCountB); got != 1 {
		t.Fatalf("expected peerB release once, got %d", got)
	}
	select {
	case err := <-errCh:
		t.Fatalf("release request mismatch: %v", err)
	default:
	}
	if manager.isLeaderState() {
		t.Fatalf("expected leader state cleared")
	}
	if rec := manager.leaseStore.Leader(); rec.LeaderID != "" {
		t.Fatalf("expected local lease released, got leader %q term %d", rec.LeaderID, rec.Term)
	}
}

func TestObservedLeaderWait(t *testing.T) {
	selfEndpoint := "http://self"
	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    []string{selfEndpoint},
		LeaseTTL:     9 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: time.Second},
		DisableMTLS:  true,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if manager.leaseStore == nil {
		t.Fatalf("expected lease store")
	}

	now := time.Now()
	if wait := manager.observedLeaderWait(now); wait != 0 {
		t.Fatalf("expected zero wait without leader, got %s", wait)
	}

	manager.leaseStore.record = LeaseRecord{
		LeaderID:       "leader-1",
		LeaderEndpoint: "http://leader",
		Term:           1,
		ExpiresAt:      now.Add(10 * time.Second),
		Observed:       true,
	}
	wait := manager.observedLeaderWait(now)
	if wait <= 0 {
		t.Fatalf("expected wait > 0, got %s", wait)
	}
	maxWait := manager.leaseTTL / 3
	if wait > maxWait {
		t.Fatalf("expected wait <= %s, got %s", maxWait, wait)
	}

	manager.leaseStore.record = LeaseRecord{
		LeaderID:       "leader-1",
		LeaderEndpoint: "http://leader",
		Term:           1,
		ExpiresAt:      now.Add(-time.Second),
		Observed:       true,
	}
	if wait := manager.observedLeaderWait(now); wait != 0 {
		t.Fatalf("expected zero wait for expired leader, got %s", wait)
	}
}

func TestTryElectParallelizesLeaseRequests(t *testing.T) {
	const peers = 4
	startCh := make(chan struct{}, peers)
	allowCh := make(chan struct{})
	stub := &blockingLeaseStub{startCh: startCh, allow: allowCh}

	servers := make([]*httptest.Server, 0, peers)
	endpoints := make([]string, 0, peers+1)
	selfEndpoint := "http://self"
	endpoints = append(endpoints, selfEndpoint)
	for i := 0; i < peers; i++ {
		srv := httptest.NewServer(http.HandlerFunc(stub.handler))
		servers = append(servers, srv)
		endpoints = append(endpoints, srv.URL)
	}
	t.Cleanup(func() {
		for _, srv := range servers {
			srv.Close()
		}
	})

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    endpoints,
		LeaseTTL:     5 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: 2 * time.Second},
		DisableMTLS:  true,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if manager.leaseStore == nil {
		t.Fatalf("expected lease store")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	done := make(chan bool, 1)
	var allowOnce sync.Once
	release := func() { allowOnce.Do(func() { close(allowCh) }) }
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})

	go func() {
		ok := manager.tryElect(ctx, manager.endpointsSnapshot(), rand.New(rand.NewSource(1)))
		done <- ok
	}()

	required := quorumSize(peers+1) - 1
	if required < 1 {
		required = 1
	}
	deadline := time.Now().Add(1 * time.Second)
	for i := 0; i < required; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("expected %d parallel lease requests, got %d", required, i)
		}
		select {
		case <-startCh:
		case <-time.After(remaining):
			t.Fatalf("expected %d parallel lease requests, got %d", required, i)
		}
	}

	release()
	select {
	case ok := <-done:
		if !ok {
			t.Fatalf("expected election to succeed")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("election timed out")
	}
}

func TestRenewLeasesParallelizesRequests(t *testing.T) {
	const peers = 4
	startCh := make(chan struct{}, peers)
	allowCh := make(chan struct{})
	stub := &blockingLeaseStub{startCh: startCh, allow: allowCh}

	servers := make([]*httptest.Server, 0, peers)
	endpoints := make([]string, 0, peers+1)
	selfEndpoint := "http://self"
	endpoints = append(endpoints, selfEndpoint)
	for i := 0; i < peers; i++ {
		srv := httptest.NewServer(http.HandlerFunc(stub.handler))
		servers = append(servers, srv)
		endpoints = append(endpoints, srv.URL)
	}
	t.Cleanup(func() {
		for _, srv := range servers {
			srv.Close()
		}
	})

	manager, err := NewManager(Config{
		SelfEndpoint: selfEndpoint,
		Endpoints:    endpoints,
		LeaseTTL:     5 * time.Second,
		Logger:       pslog.NoopLogger(),
		HTTPClient:   &http.Client{Timeout: 2 * time.Second},
		DisableMTLS:  true,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if manager.leaseStore == nil {
		t.Fatalf("expected lease store")
	}
	_, leaseErr := manager.leaseStore.Acquire(time.Now(), manager.selfID, manager.selfEndpoint, 1, time.Minute)
	if leaseErr != nil {
		t.Fatalf("acquire local lease: code=%s detail=%s term=%d leader=%s endpoint=%s", leaseErr.Code, leaseErr.Detail, leaseErr.Term, leaseErr.LeaderID, leaseErr.LeaderEndpoint)
	}
	manager.becomeLeader(1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	done := make(chan bool, 1)
	var allowOnce sync.Once
	release := func() { allowOnce.Do(func() { close(allowCh) }) }
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})

	go func() {
		ok := manager.renewLeases(ctx, manager.endpointsSnapshot())
		done <- ok
	}()

	required := quorumSize(peers+1) - 1
	if required < 1 {
		required = 1
	}
	deadline := time.Now().Add(1 * time.Second)
	for i := 0; i < required; i++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("expected %d parallel renew requests, got %d", required, i)
		}
		select {
		case <-startCh:
		case <-time.After(remaining):
			t.Fatalf("expected %d parallel renew requests, got %d", required, i)
		}
	}

	release()
	select {
	case ok := <-done:
		if !ok {
			t.Fatalf("expected renew to succeed")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("renew timed out")
	}
}
