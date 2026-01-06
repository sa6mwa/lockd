package txncoord

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/internal/tcleader"
)

func TestDeciderForwardPreservesPayload(t *testing.T) {
	recv := make(chan api.TxnDecisionRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/txn/decide" {
			http.NotFound(w, r)
			return
		}
		var payload api.TxnDecisionRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}
		recv <- payload
		_ = json.NewEncoder(w).Encode(api.TxnDecisionResponse{State: "commit"})
	}))
	t.Cleanup(server.Close)

	leader := newFollowerManager(t, "http://follower", []string{"http://follower", server.URL}, server.URL, 7)
	decider, err := NewDecider(DeciderConfig{
		Coordinator:    &Coordinator{},
		Leader:         leader,
		ForwardTimeout: 500 * time.Millisecond,
		HTTPClient:     server.Client(),
	})
	if err != nil {
		t.Fatalf("decider: %v", err)
	}

	now := time.Now()
	rec := core.TxnRecord{
		TxnID:         "txn-forward-1",
		State:         core.TxnStateCommit,
		ExpiresAtUnix: now.Add(2 * time.Minute).Unix(),
		TCTerm:        42,
		Participants: []core.TxnParticipant{
			{Namespace: "default", Key: "k1", BackendHash: "hash-a"},
			{Namespace: "default", Key: "k2", BackendHash: "hash-b"},
		},
	}

	state, err := decider.Decide(context.Background(), rec)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if state != core.TxnStateCommit {
		t.Fatalf("expected commit, got %q", state)
	}

	select {
	case got := <-recv:
		if got.TxnID != rec.TxnID {
			t.Fatalf("expected txn_id %q, got %q", rec.TxnID, got.TxnID)
		}
		if got.State != string(rec.State) {
			t.Fatalf("expected state %q, got %q", rec.State, got.State)
		}
		if got.ExpiresAtUnix != rec.ExpiresAtUnix {
			t.Fatalf("expected expires_at %d, got %d", rec.ExpiresAtUnix, got.ExpiresAtUnix)
		}
		if got.TCTerm != rec.TCTerm {
			t.Fatalf("expected tc_term %d, got %d", rec.TCTerm, got.TCTerm)
		}
		if len(got.Participants) != len(rec.Participants) {
			t.Fatalf("expected %d participants, got %d", len(rec.Participants), len(got.Participants))
		}
		for i := range rec.Participants {
			exp := rec.Participants[i]
			if got.Participants[i].Namespace != exp.Namespace || got.Participants[i].Key != exp.Key || got.Participants[i].BackendHash != exp.BackendHash {
				t.Fatalf("participant %d mismatch: %+v != %+v", i, got.Participants[i], exp)
			}
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for forwarded decision")
	}
}

func TestDeciderForwardUnreachableReturnsNotLeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	server.Close()

	leader := newFollowerManager(t, "http://follower", []string{"http://follower", server.URL}, server.URL, 3)
	decider, err := NewDecider(DeciderConfig{
		Coordinator:    &Coordinator{},
		Leader:         leader,
		ForwardTimeout: 200 * time.Millisecond,
		HTTPClient:     &http.Client{Timeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("decider: %v", err)
	}

	_, err = decider.Decide(context.Background(), core.TxnRecord{
		TxnID: "txn-forward-down",
		State: core.TxnStateCommit,
	})
	var failure core.Failure
	if !errors.As(err, &failure) {
		t.Fatalf("expected failure, got %v", err)
	}
	if failure.Code != "tc_not_leader" {
		t.Fatalf("expected tc_not_leader, got %q", failure.Code)
	}
	if failure.LeaderEndpoint != server.URL {
		t.Fatalf("expected leader endpoint %q, got %q", server.URL, failure.LeaderEndpoint)
	}
}

func TestDeciderNoLeaderReturnsUnavailable(t *testing.T) {
	leader, err := tcleader.NewManager(tcleader.Config{
		SelfEndpoint: "http://self",
		Endpoints:    []string{"http://self", "http://peer"},
		LeaseTTL:     2 * time.Second,
		DisableMTLS:  true,
	})
	if err != nil {
		t.Fatalf("leader manager: %v", err)
	}
	decider, err := NewDecider(DeciderConfig{
		Coordinator: &Coordinator{},
		Leader:      leader,
	})
	if err != nil {
		t.Fatalf("decider: %v", err)
	}

	_, err = decider.Decide(context.Background(), core.TxnRecord{
		TxnID: "txn-no-leader",
		State: core.TxnStateCommit,
	})
	var failure core.Failure
	if !errors.As(err, &failure) {
		t.Fatalf("expected failure, got %v", err)
	}
	if failure.Code != "tc_unavailable" {
		t.Fatalf("expected tc_unavailable, got %q", failure.Code)
	}
}

func TestDeciderLeaderStampsTermStable(t *testing.T) {
	mem := memory.New()
	svc := core.New(core.Config{
		Store:            mem,
		BackendHash:      "local-backend",
		DefaultNamespace: "default",
	})
	coord, err := New(Config{Core: svc})
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}

	mgr, err := tcleader.NewManager(tcleader.Config{
		SelfEndpoint: "http://self",
		Endpoints:    []string{"http://self"},
		LeaseTTL:     2 * time.Second,
		DisableMTLS:  true,
	})
	if err != nil {
		t.Fatalf("leader manager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mgr.Start(ctx)
	info := waitForLeaderTerm(t, mgr, 2*time.Second)

	decider, err := NewDecider(DeciderConfig{
		Coordinator: coord,
		Leader:      mgr,
	})
	if err != nil {
		t.Fatalf("decider: %v", err)
	}

	rec := core.TxnRecord{
		TxnID: "txn-term-1",
		State: core.TxnStatePending,
	}
	if _, err := decider.Decide(ctx, rec); err != nil {
		t.Fatalf("decide: %v", err)
	}
	first := loadTxnRecord(t, mem, rec.TxnID)
	if first.TCTerm != info.Term {
		t.Fatalf("expected tc_term %d, got %d", info.Term, first.TCTerm)
	}

	if _, err := decider.Decide(ctx, rec); err != nil {
		t.Fatalf("decide again: %v", err)
	}
	second := loadTxnRecord(t, mem, rec.TxnID)
	if second.TCTerm != first.TCTerm {
		t.Fatalf("expected stable tc_term %d, got %d", first.TCTerm, second.TCTerm)
	}
}

func newFollowerManager(t testing.TB, self string, endpoints []string, leaderEndpoint string, term uint64) *tcleader.Manager {
	t.Helper()
	mgr, err := tcleader.NewManager(tcleader.Config{
		SelfEndpoint: self,
		Endpoints:    endpoints,
		LeaseTTL:     2 * time.Second,
		DisableMTLS:  true,
	})
	if err != nil {
		t.Fatalf("leader manager: %v", err)
	}
	now := time.Now()
	mgr.LeaseStore().Follow(now, "leader-id", leaderEndpoint, term, now.Add(5*time.Second))
	return mgr
}

func waitForLeaderTerm(t testing.TB, mgr *tcleader.Manager, timeout time.Duration) tcleader.LeaderInfo {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info := mgr.Leader(time.Now())
		if info.IsLeader && info.Term > 0 {
			return info
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("leader term not observed before timeout")
	return tcleader.LeaderInfo{}
}

func loadTxnRecord(t testing.TB, backend storage.Backend, txnID string) core.TxnRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	obj, err := backend.GetObject(ctx, ".txns", txnID)
	if err != nil {
		t.Fatalf("load txn record: %v", err)
	}
	defer obj.Reader.Close()
	var rec core.TxnRecord
	if err := json.NewDecoder(obj.Reader).Decode(&rec); err != nil {
		t.Fatalf("decode txn record: %v", err)
	}
	return rec
}
