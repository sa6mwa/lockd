package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

type dueProvider struct {
	mu        sync.Mutex
	readyAt   time.Time
	candidate *Candidate
}

func (p *dueProvider) NextCandidate(ctx context.Context, namespace, queue, startAfter string, pageSize int) (MessageCandidateResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Now().Before(p.readyAt) {
		return MessageCandidateResult{}, storage.ErrNotFound
	}
	if p.candidate == nil {
		return MessageCandidateResult{}, storage.ErrNotFound
	}
	desc := p.candidate.Descriptor
	return MessageCandidateResult{
		Descriptor: &desc,
		NextCursor: p.candidate.NextCursor,
	}, nil
}

func TestDispatcherNotifyAtWakesDelayedMessage(t *testing.T) {
	t.Parallel()

	readyAt := time.Now().Add(120 * time.Millisecond)
	provider := &dueProvider{
		readyAt: readyAt,
		candidate: &Candidate{
			Descriptor: MessageDescriptor{
				Namespace:    "default",
				ID:           "msg-1",
				MetadataKey:  "q/orders/msg/msg-1.pb",
				MetadataETag: "etag-1",
				Document: messageDocument{
					Queue: "orders",
					ID:    "msg-1",
				},
			},
			NextCursor: "cursor-1",
		},
	}
	disp := NewDispatcher(provider,
		WithLogger(pslog.NoopLogger()),
		WithPollInterval(30*time.Second),
		WithPollJitter(0),
		WithResilientPollInterval(30*time.Second),
	)

	disp.NotifyAt("default", "orders", "msg-1", readyAt)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	cand, err := disp.Wait(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("wait returned error: %v", err)
	}
	if cand == nil {
		t.Fatalf("expected candidate")
	}
	elapsed := time.Since(start)
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected delayed wake, elapsed=%s", elapsed)
	}
	if elapsed > 700*time.Millisecond {
		t.Fatalf("expected notifier wake before fallback poll, elapsed=%s", elapsed)
	}
}

func TestDispatcherCancelNotifyPreventsDelayedWake(t *testing.T) {
	t.Parallel()

	readyAt := time.Now().Add(120 * time.Millisecond)
	provider := &dueProvider{
		readyAt: readyAt,
		candidate: &Candidate{
			Descriptor: MessageDescriptor{
				Namespace:    "default",
				ID:           "msg-2",
				MetadataKey:  "q/orders/msg/msg-2.pb",
				MetadataETag: "etag-2",
				Document: messageDocument{
					Queue: "orders",
					ID:    "msg-2",
				},
			},
			NextCursor: "cursor-2",
		},
	}
	disp := NewDispatcher(provider,
		WithLogger(pslog.NoopLogger()),
		WithPollInterval(30*time.Second),
		WithPollJitter(0),
		WithResilientPollInterval(30*time.Second),
	)

	disp.NotifyAt("default", "orders", "msg-2", readyAt)
	disp.CancelNotify("default", "orders", "msg-2")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := disp.Wait(ctx, "default", "orders")
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context timeout/cancel after canceled notify, got %v", err)
	}
}
