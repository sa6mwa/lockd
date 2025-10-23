package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/logport"
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

func (s *stubService) NextCandidate(ctx context.Context, queue string, startAfter string, pageSize int) (*MessageDescriptor, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.responses) == 0 {
		return nil, "", storage.ErrNotFound
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	if resp.err != nil {
		return nil, "", resp.err
	}
	if resp.candidate == nil {
		return nil, "", storage.ErrNotFound
	}
	return &resp.candidate.Descriptor, resp.candidate.NextCursor, nil
}

func (s *stubService) EnsureMessageReady(ctx context.Context, queue, id string) error {
	return nil
}

type stubWatchSubscription struct {
	events chan struct{}
}

func (s *stubWatchSubscription) Events() <-chan struct{} { return s.events }

func (s *stubWatchSubscription) Close() error {
	close(s.events)
	return nil
}

type stubWatchFactory struct {
	sub *stubWatchSubscription
}

func (f *stubWatchFactory) Subscribe(queue string) (WatchSubscription, error) {
	return f.sub, nil
}

func makeCandidate(queueName, id string) *Candidate {
	doc := messageDocument{
		Type:              "queue_msg",
		Queue:             queueName,
		ID:                id,
		NotVisibleUntil:   time.Unix(0, 0),
		MaxAttempts:       5,
		VisibilityTimeout: 30,
	}
	desc := MessageDescriptor{
		ID:           id,
		MetadataKey:  "meta/" + id,
		MetadataETag: "etag-" + id,
		Document:     doc,
	}
	return &Candidate{
		Descriptor: desc,
		NextCursor: id + "-next",
	}
}

func TestDispatcherTryWatcherDiscrepancy(t *testing.T) {
	svc := &stubService{
		responses: []nextResponse{
			{err: storage.ErrNotFound},
		},
	}
	disp := NewDispatcher(svc,
		WithLogger(logport.NoopLogger()),
		WithResilientPollInterval(5*time.Minute),
	)

	qs, err := disp.queueState("orders")
	if err != nil {
		t.Fatalf("queueState: %v", err)
	}
	qs.markNeedsPoll("watch_event")

	cand, err := disp.Try(context.Background(), "orders")
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
