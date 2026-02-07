package tcrm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/pslog"
)

func TestReplicatorRegisterSkipsSelfAndForwards(t *testing.T) {
	ctx := context.Background()
	var selfCalls atomic.Int32
	var peerCalls atomic.Int32
	var gotHeader atomic.Bool

	selfServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selfCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer selfServer.Close()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerCalls.Add(1)
		if r.Method == http.MethodGet && r.URL.Path == "/v1/tc/leader" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Header.Get("X-Lockd-TC-Replicate") == "1" {
			gotHeader.Store(true)
		}
		var payload struct {
			BackendHash string `json:"backend_hash"`
			Endpoint    string `json:"endpoint"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		if payload.BackendHash != "hash-1" {
			t.Errorf("backend hash mismatch: %s", payload.BackendHash)
		}
		if payload.Endpoint != "https://rm-1" {
			t.Errorf("endpoint mismatch: %s", payload.Endpoint)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer peerServer.Close()

	store := memory.New()
	clusterStore := tccluster.NewStore(store, pslog.NoopLogger(), clock.Real{})
	if _, err := clusterStore.Announce(ctx, "id-self", selfServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce self: %v", err)
	}
	if _, err := clusterStore.Announce(ctx, "id-peer", peerServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce peer: %v", err)
	}
	rmStore := NewStore(store, pslog.NoopLogger())
	replicator, err := NewReplicator(ReplicatorConfig{
		Store:        rmStore,
		Cluster:      clusterStore,
		SelfEndpoint: selfServer.URL,
		DisableMTLS:  true,
		Logger:       pslog.NoopLogger(),
	})
	if err != nil {
		t.Fatalf("new replicator: %v", err)
	}
	headers := http.Header{
		"X-Lockd-TC-Replicate": []string{"1"},
	}
	if _, err := replicator.Register(ctx, "hash-1", "https://rm-1", headers); err != nil {
		t.Fatalf("register: %v", err)
	}
	if selfCalls.Load() != 0 {
		t.Fatalf("expected self not called, got %d", selfCalls.Load())
	}
	if peerCalls.Load() != 2 {
		t.Fatalf("expected peer called twice, got %d", peerCalls.Load())
	}
	if !gotHeader.Load() {
		t.Fatalf("expected replicate header")
	}
}

func TestReplicatorUnregisterSkipsSelfAndForwards(t *testing.T) {
	ctx := context.Background()
	var selfCalls atomic.Int32
	var peerCalls atomic.Int32

	selfServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selfCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer selfServer.Close()

	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerCalls.Add(1)
		if r.Method == http.MethodGet && r.URL.Path == "/v1/tc/leader" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer peerServer.Close()

	store := memory.New()
	clusterStore := tccluster.NewStore(store, pslog.NoopLogger(), clock.Real{})
	if _, err := clusterStore.Announce(ctx, "id-self", selfServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce self: %v", err)
	}
	if _, err := clusterStore.Announce(ctx, "id-peer", peerServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce peer: %v", err)
	}
	rmStore := NewStore(store, pslog.NoopLogger())
	replicator, err := NewReplicator(ReplicatorConfig{
		Store:        rmStore,
		Cluster:      clusterStore,
		SelfEndpoint: selfServer.URL,
		DisableMTLS:  true,
		Logger:       pslog.NoopLogger(),
	})
	if err != nil {
		t.Fatalf("new replicator: %v", err)
	}
	headers := http.Header{
		"X-Lockd-TC-Replicate": []string{"1"},
	}
	if _, err := replicator.Unregister(ctx, "hash-1", "https://rm-1", headers); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if selfCalls.Load() != 0 {
		t.Fatalf("expected self not called, got %d", selfCalls.Load())
	}
	if peerCalls.Load() != 2 {
		t.Fatalf("expected peer called twice, got %d", peerCalls.Load())
	}
}

func TestReplicatorRegisterReportsReplicationError(t *testing.T) {
	ctx := context.Background()
	var goodCalls atomic.Int32
	var badCalls atomic.Int32

	selfServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("self endpoint should not be called")
	}))
	defer selfServer.Close()

	goodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer goodServer.Close()

	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badCalls.Add(1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer badServer.Close()

	store := memory.New()
	clusterStore := tccluster.NewStore(store, pslog.NoopLogger(), clock.Real{})
	if _, err := clusterStore.Announce(ctx, "id-self", selfServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce self: %v", err)
	}
	if _, err := clusterStore.Announce(ctx, "id-good", goodServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce good: %v", err)
	}
	if _, err := clusterStore.Announce(ctx, "id-bad", badServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce bad: %v", err)
	}
	rmStore := NewStore(store, pslog.NoopLogger())
	replicator, err := NewReplicator(ReplicatorConfig{
		Store:        rmStore,
		Cluster:      clusterStore,
		SelfEndpoint: selfServer.URL,
		DisableMTLS:  true,
		Logger:       pslog.NoopLogger(),
	})
	if err != nil {
		t.Fatalf("new replicator: %v", err)
	}
	_, err = replicator.Register(ctx, "hash-1", "https://rm-1", nil)
	if err == nil {
		t.Fatalf("expected replication error")
	}
	var replErr *ReplicationError
	if !errors.As(err, &replErr) {
		t.Fatalf("expected replication error, got %T", err)
	}
	if replErr.Operation != "register" {
		t.Fatalf("expected register operation, got %q", replErr.Operation)
	}
	if len(replErr.Failures) != 1 || replErr.Failures[0].Endpoint != badServer.URL {
		t.Fatalf("expected failure for %q, got %+v", badServer.URL, replErr.Failures)
	}
	if goodCalls.Load() != 1 {
		t.Fatalf("expected good peer called once, got %d", goodCalls.Load())
	}
	if badCalls.Load() != 1 {
		t.Fatalf("expected bad peer called once, got %d", badCalls.Load())
	}
	endpoints, err := rmStore.Endpoints(ctx, "hash-1")
	if err != nil {
		t.Fatalf("load endpoints: %v", err)
	}
	if len(endpoints) != 0 {
		t.Fatalf("expected local registry empty, got %+v", endpoints)
	}
}

func TestReplicatorUnregisterReportsReplicationError(t *testing.T) {
	ctx := context.Background()
	var goodCalls atomic.Int32
	var badCalls atomic.Int32

	selfServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("self endpoint should not be called")
	}))
	defer selfServer.Close()

	goodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer goodServer.Close()

	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badCalls.Add(1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer badServer.Close()

	store := memory.New()
	clusterStore := tccluster.NewStore(store, pslog.NoopLogger(), clock.Real{})
	if _, err := clusterStore.Announce(ctx, "id-self", selfServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce self: %v", err)
	}
	if _, err := clusterStore.Announce(ctx, "id-good", goodServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce good: %v", err)
	}
	if _, err := clusterStore.Announce(ctx, "id-bad", badServer.URL, time.Minute); err != nil {
		t.Fatalf("cluster announce bad: %v", err)
	}
	rmStore := NewStore(store, pslog.NoopLogger())
	if _, err := rmStore.Register(ctx, "hash-1", "https://rm-1"); err != nil {
		t.Fatalf("register local: %v", err)
	}
	replicator, err := NewReplicator(ReplicatorConfig{
		Store:        rmStore,
		Cluster:      clusterStore,
		SelfEndpoint: selfServer.URL,
		DisableMTLS:  true,
		Logger:       pslog.NoopLogger(),
	})
	if err != nil {
		t.Fatalf("new replicator: %v", err)
	}
	_, err = replicator.Unregister(ctx, "hash-1", "https://rm-1", nil)
	if err == nil {
		t.Fatalf("expected replication error")
	}
	var replErr *ReplicationError
	if !errors.As(err, &replErr) {
		t.Fatalf("expected replication error, got %T", err)
	}
	if replErr.Operation != "unregister" {
		t.Fatalf("expected unregister operation, got %q", replErr.Operation)
	}
	if len(replErr.Failures) != 1 || replErr.Failures[0].Endpoint != badServer.URL {
		t.Fatalf("expected failure for %q, got %+v", badServer.URL, replErr.Failures)
	}
	if goodCalls.Load() != 1 {
		t.Fatalf("expected good peer called once, got %d", goodCalls.Load())
	}
	if badCalls.Load() != 1 {
		t.Fatalf("expected bad peer called once, got %d", badCalls.Load())
	}
	endpoints, err := rmStore.Endpoints(ctx, "hash-1")
	if err != nil {
		t.Fatalf("load endpoints: %v", err)
	}
	if len(endpoints) != 1 || endpoints[0] != "https://rm-1" {
		t.Fatalf("expected local registry unchanged, got %+v", endpoints)
	}
}

func TestReplicatorRegisterRejectsInvalidRMEndpoint(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	rmStore := NewStore(store, pslog.NoopLogger())
	replicator, err := NewReplicator(ReplicatorConfig{
		Store:       rmStore,
		DisableMTLS: true,
		Logger:      pslog.NoopLogger(),
	})
	if err != nil {
		t.Fatalf("new replicator: %v", err)
	}
	_, err = replicator.Register(ctx, "hash-1", "https://rm-1?x=1", nil)
	if err == nil {
		t.Fatal("expected invalid endpoint error")
	}
	if !strings.Contains(err.Error(), "endpoint must not include query or fragment") {
		t.Fatalf("unexpected error: %v", err)
	}
}
