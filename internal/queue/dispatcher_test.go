package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

type nextResponse struct {
	candidate *Candidate
	err       error
}

type stubService struct {
	mu        sync.Mutex
	responses []nextResponse
	calls     int
}

func (s *stubService) NextCandidate(ctx context.Context, namespace, queue string, startAfter string, pageSize int) (MessageCandidateResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.responses) == 0 {
		return MessageCandidateResult{}, storage.ErrNotFound
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	if resp.err != nil {
		return MessageCandidateResult{}, resp.err
	}
	if resp.candidate == nil {
		return MessageCandidateResult{}, storage.ErrNotFound
	}
	return MessageCandidateResult{Descriptor: &resp.candidate.Descriptor, NextCursor: resp.candidate.NextCursor}, nil
}

func (s *stubService) EnsureMessageReady(ctx context.Context, namespace, queue, id string) error {
	return nil
}

func TestDispatcherTryWatcherDiscrepancy(t *testing.T) {
	const namespace = "default"
	svc := &stubService{
		responses: []nextResponse{
			{err: storage.ErrNotFound},
		},
	}
	disp := NewDispatcher(svc,
		WithLogger(pslog.NoopLogger()),
		WithResilientPollInterval(5*time.Minute),
	)

	qs, err := disp.queueState(namespace, "orders")
	if err != nil {
		t.Fatalf("queueState: %v", err)
	}
	qs.markNeedsPoll("watch_event")

	cand, err := disp.Try(context.Background(), namespace, "orders")
	if err != nil {
		t.Fatalf("Try returned error: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected nil candidate, got %+v", cand)
	}

	qs.mu.Lock()
	reasons := append([]string(nil), qs.lastPollReasons...)
	qs.mu.Unlock()
	if !containsReason(reasons, "watch_event") {
		t.Fatalf("expected watch_event reason recorded, got %v", reasons)
	}
}
