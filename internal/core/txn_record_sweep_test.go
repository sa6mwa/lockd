package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

type countingBackend struct {
	*memory.Store
	listCalls atomic.Int64
}

func (b *countingBackend) ListObjects(ctx context.Context, namespace string, opts storage.ListOptions) (*storage.ListResult, error) {
	b.listCalls.Add(1)
	return b.Store.ListObjects(ctx, namespace, opts)
}

func waitForListCalls(t testing.TB, counter *atomic.Int64, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter.Load() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected list calls >= %d, got %d", want, counter.Load())
}

func TestMaybeReplayTxnRecordsImmediate(t *testing.T) {
	backend := &countingBackend{Store: memory.New()}
	svc := New(Config{
		Store:             backend,
		TxnReplayInterval: 500 * time.Millisecond,
	})

	svc.maybeReplayTxnRecords(context.Background())
	waitForListCalls(t, &backend.listCalls, 1, time.Second)
}

func TestMaybeReplayTxnRecordsDeferred(t *testing.T) {
	backend := &countingBackend{Store: memory.New()}
	svc := New(Config{
		Store:             backend,
		TxnReplayInterval: 30 * time.Second,
	})

	svc.maybeReplayTxnRecords(context.Background())
	time.Sleep(50 * time.Millisecond)
	if backend.listCalls.Load() != 0 {
		t.Fatalf("expected no list calls for deferred replay, got %d", backend.listCalls.Load())
	}
}

func TestMaybeReplayTxnRecordsThrottle(t *testing.T) {
	backend := &countingBackend{Store: memory.New()}
	svc := New(Config{
		Store:             backend,
		TxnReplayInterval: 500 * time.Millisecond,
	})

	svc.maybeReplayTxnRecords(context.Background())
	waitForListCalls(t, &backend.listCalls, 1, time.Second)

	svc.maybeReplayTxnRecords(context.Background())
	time.Sleep(100 * time.Millisecond)
	if backend.listCalls.Load() > 1 {
		t.Fatalf("expected replay throttled within interval, got %d list calls", backend.listCalls.Load())
	}
}
