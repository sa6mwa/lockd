package txncoord

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/storage/memory"
)

type staticEndpointProvider map[string][]string

func (p staticEndpointProvider) Endpoints(ctx context.Context, backendHash string) ([]string, error) {
	list := p[backendHash]
	if len(list) == 0 {
		return nil, nil
	}
	out := make([]string, len(list))
	copy(out, list)
	return out, nil
}

func newTestCoordinator(t testing.TB, provider EndpointProvider) *Coordinator {
	t.Helper()
	svc := core.New(core.Config{
		Store:            memory.New(),
		BackendHash:      "local-backend",
		DefaultNamespace: "default",
	})
	coord, err := New(Config{
		Core:              svc,
		FanoutProvider:    provider,
		FanoutTimeout:     250 * time.Millisecond,
		FanoutMaxAttempts: 1,
		DisableMTLS:       true,
		HTTPClient:        &http.Client{},
	})
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	return coord
}

func closedEndpoint(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return "http://" + addr
}

func newApplyServer(t testing.TB, backendHash string, calls *int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/txn/commit" {
			http.NotFound(w, r)
			return
		}
		var payload api.TxnDecisionRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}
		if payload.TargetBackendHash != backendHash {
			http.Error(w, "backend hash mismatch", http.StatusBadRequest)
			return
		}
		atomic.AddInt32(calls, 1)
		w.WriteHeader(http.StatusOK)
	}))
}

func newConflictServer(t testing.TB) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/txn/commit" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(api.ErrorResponse{
			ErrorCode: "txn_conflict",
			Detail:    "transaction already decided",
		})
	}))
}

func TestCoordinatorFanoutSucceedsWithAnyEndpoint(t *testing.T) {
	backendHash := "remote-backend"
	var calls int32
	okServer := newApplyServer(t, backendHash, &calls)
	t.Cleanup(okServer.Close)

	provider := staticEndpointProvider{
		backendHash: {closedEndpoint(t), okServer.URL},
	}
	coord := newTestCoordinator(t, provider)
	groups := map[string][]core.TxnParticipant{
		backendHash: {{Namespace: "default", Key: "k1", BackendHash: backendHash}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	if err := coord.fanout(ctx, "txn-1", core.TxnStateCommit, 1, groups, time.Now().Add(time.Minute).Unix()); err != nil {
		t.Fatalf("fanout error: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected 1 apply call, got %d", got)
	}
}

func TestCoordinatorFanoutFailsWhenAllEndpointsFail(t *testing.T) {
	backendHash := "remote-backend"
	provider := staticEndpointProvider{
		backendHash: {closedEndpoint(t), closedEndpoint(t)},
	}
	coord := newTestCoordinator(t, provider)
	groups := map[string][]core.TxnParticipant{
		backendHash: {{Namespace: "default", Key: "k1", BackendHash: backendHash}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	err := coord.fanout(ctx, "txn-2", core.TxnStateCommit, 1, groups, time.Now().Add(time.Minute).Unix())
	if err == nil {
		t.Fatalf("expected fanout error")
	}
	var fanoutErr *FanoutError
	if !errors.As(err, &fanoutErr) {
		t.Fatalf("expected FanoutError, got %T", err)
	}
	if len(fanoutErr.Failures) == 0 {
		t.Fatalf("expected failures in FanoutError")
	}
}

func TestCoordinatorFanoutTreatsTxnConflictAsApplied(t *testing.T) {
	backendHash := "remote-backend"
	conflictServer := newConflictServer(t)
	t.Cleanup(conflictServer.Close)

	provider := staticEndpointProvider{
		backendHash: {conflictServer.URL},
	}
	coord := newTestCoordinator(t, provider)
	groups := map[string][]core.TxnParticipant{
		backendHash: {{Namespace: "default", Key: "k1", BackendHash: backendHash}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)

	if err := coord.fanout(ctx, "txn-3", core.TxnStateCommit, 1, groups, time.Now().Add(time.Minute).Unix()); err != nil {
		t.Fatalf("fanout error: %v", err)
	}
}
