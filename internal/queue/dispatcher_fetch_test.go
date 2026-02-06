package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

type captureCtxKey string

type captureProvider struct {
	nextResult MessageCandidateResult
	nextErr    error

	nextCtxCh  chan context.Context
	readyCtxCh chan context.Context
}

func (p *captureProvider) NextCandidate(ctx context.Context, namespace, queue, startAfter string, pageSize int) (MessageCandidateResult, error) {
	if p.nextCtxCh != nil {
		select {
		case p.nextCtxCh <- ctx:
		default:
		}
	}
	if p.nextErr != nil {
		return MessageCandidateResult{}, p.nextErr
	}
	return p.nextResult, nil
}

func (p *captureProvider) EnsureMessageReady(ctx context.Context, namespace, queue, id string) error {
	if p.readyCtxCh != nil {
		select {
		case p.readyCtxCh <- ctx:
		default:
		}
	}
	return nil
}

func TestDispatcherTryHandlesNilDescriptor(t *testing.T) {
	t.Parallel()

	svc := &captureProvider{
		nextResult: MessageCandidateResult{
			Descriptor: nil,
			NextCursor: "cursor-1",
		},
		readyCtxCh: make(chan context.Context, 1),
	}
	disp := NewDispatcher(svc, WithLogger(pslog.NoopLogger()))

	cand, err := disp.Try(context.Background(), "default", "orders")
	if err != nil {
		t.Fatalf("Try returned error: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected nil candidate, got %+v", cand)
	}
	select {
	case <-svc.readyCtxCh:
		t.Fatal("EnsureMessageReady should not be called for nil descriptor")
	default:
	}
}

func TestDispatcherTryPropagatesContextToFetchAndReadiness(t *testing.T) {
	t.Parallel()

	key := captureCtxKey("ctx_key")
	ctx := context.WithValue(context.Background(), key, "ctx-value")
	svc := &captureProvider{
		nextResult: MessageCandidateResult{
			Descriptor: &MessageDescriptor{
				Namespace:    "default",
				ID:           "msg-1",
				MetadataKey:  "q/orders/msg/msg-1.pb",
				MetadataETag: "etag-1",
				Document: messageDocument{
					Queue: "orders",
					ID:    "msg-1",
				},
			},
			NextCursor: "cursor-2",
		},
		nextCtxCh:  make(chan context.Context, 1),
		readyCtxCh: make(chan context.Context, 1),
	}
	disp := NewDispatcher(svc, WithLogger(pslog.NoopLogger()))

	cand, err := disp.Try(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("Try returned error: %v", err)
	}
	if cand == nil {
		t.Fatal("expected candidate")
	}

	select {
	case got := <-svc.nextCtxCh:
		if got == nil || got.Value(key) != "ctx-value" {
			t.Fatalf("NextCandidate context missing propagated value: %#v", got)
		}
	default:
		t.Fatal("NextCandidate was not called")
	}
	select {
	case got := <-svc.readyCtxCh:
		if got == nil || got.Value(key) != "ctx-value" {
			t.Fatalf("EnsureMessageReady context missing propagated value: %#v", got)
		}
	default:
		t.Fatal("EnsureMessageReady was not called")
	}
}

func TestDispatcherWaitPollPropagatesWaiterContext(t *testing.T) {
	t.Parallel()

	key := captureCtxKey("wait_ctx_key")
	waitCtx, cancel := context.WithTimeout(context.WithValue(context.Background(), key, "wait-value"), 150*time.Millisecond)
	defer cancel()

	svc := &captureProvider{
		nextErr:   storage.ErrNotFound,
		nextCtxCh: make(chan context.Context, 1),
	}
	disp := NewDispatcher(svc,
		WithLogger(pslog.NoopLogger()),
		WithPollInterval(5*time.Millisecond),
		WithPollJitter(0),
	)

	_, err := disp.Wait(waitCtx, "default", "orders")
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected wait context error, got %v", err)
	}
	select {
	case got := <-svc.nextCtxCh:
		if got == nil || got.Value(key) != "wait-value" {
			t.Fatalf("poll fetch context missing waiter value: %#v", got)
		}
	default:
		t.Fatal("expected at least one NextCandidate call")
	}
}
