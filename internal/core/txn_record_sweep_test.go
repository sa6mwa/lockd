package core

import (
	"context"
	"errors"
	"io"
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

type decisionPathCountingBackend struct {
	*memory.Store
	decisionGetCalls    atomic.Int64
	decisionPutCalls    atomic.Int64
	decisionDeleteCalls atomic.Int64
}

type slowListBackend struct {
	*memory.Store
	delay time.Duration
}

type slowCountingListBackend struct {
	*memory.Store
	delay     time.Duration
	listCalls atomic.Int64
}

func (b *decisionPathCountingBackend) GetObject(ctx context.Context, namespace, key string) (storage.GetObjectResult, error) {
	if namespace == txnDecisionNamespace {
		b.decisionGetCalls.Add(1)
	}
	return b.Store.GetObject(ctx, namespace, key)
}

func (b *decisionPathCountingBackend) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	if namespace == txnDecisionNamespace {
		b.decisionPutCalls.Add(1)
	}
	return b.Store.PutObject(ctx, namespace, key, body, opts)
}

func (b *decisionPathCountingBackend) DeleteObject(ctx context.Context, namespace, key string, opts storage.DeleteObjectOptions) error {
	if namespace == txnDecisionNamespace {
		b.decisionDeleteCalls.Add(1)
	}
	return b.Store.DeleteObject(ctx, namespace, key, opts)
}

func (b *slowListBackend) ListObjects(ctx context.Context, namespace string, opts storage.ListOptions) (*storage.ListResult, error) {
	if b.delay > 0 {
		timer := time.NewTimer(b.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return b.Store.ListObjects(ctx, namespace, opts)
}

func (b *slowCountingListBackend) ListObjects(ctx context.Context, namespace string, opts storage.ListOptions) (*storage.ListResult, error) {
	b.listCalls.Add(1)
	if b.delay > 0 {
		timer := time.NewTimer(b.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
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

func TestSweepTxnRecordsPartialIdleDefersDecisionFinalization(t *testing.T) {
	ctx := context.Background()
	backend := &decisionPathCountingBackend{Store: memory.New()}
	svc := New(Config{
		Store:                backend,
		TxnDecisionRetention: 24 * time.Hour,
	})

	now := time.Now().Unix()
	rec := &TxnRecord{
		TxnID: "idle-defer-finalize",
		State: TxnStateRollback,
		Participants: []TxnParticipant{
			{Namespace: "default", Key: "missing-key"},
		},
		ExpiresAtUnix: now - 1,
		UpdatedAtUnix: now,
		CreatedAtUnix: now,
	}
	if _, err := svc.putTxnRecord(ctx, rec, ""); err != nil {
		t.Fatalf("seed txn record: %v", err)
	}

	budget := newSweepBudget(svc.clock, IdleSweepOptions{
		Now:        svc.clock.Now(),
		MaxOps:     10,
		MaxRuntime: time.Second,
	})
	if err := svc.sweepTxnRecordsPartial(ctx, budget, sweepModeIdle); err != nil {
		t.Fatalf("idle sweep txn records: %v", err)
	}

	if backend.decisionGetCalls.Load() != 0 {
		t.Fatalf("expected idle sweep to avoid decision marker reads, got %d", backend.decisionGetCalls.Load())
	}
	if backend.decisionPutCalls.Load() != 0 {
		t.Fatalf("expected idle sweep to defer decision marker writes, got %d", backend.decisionPutCalls.Load())
	}
	if backend.decisionDeleteCalls.Load() != 0 {
		t.Fatalf("expected idle sweep to avoid decision marker deletes, got %d", backend.decisionDeleteCalls.Load())
	}

	updated, _, err := svc.loadTxnRecord(ctx, rec.TxnID)
	if err != nil {
		t.Fatalf("load updated txn record: %v", err)
	}
	if len(updated.Participants) != 1 || !updated.Participants[0].Applied {
		t.Fatalf("expected participant marked applied without finalization, got %+v", updated.Participants)
	}
}

func TestSweepTxnRecordsPartialReplayFinalizesDecision(t *testing.T) {
	ctx := context.Background()
	backend := &decisionPathCountingBackend{Store: memory.New()}
	svc := New(Config{
		Store:                backend,
		TxnDecisionRetention: 24 * time.Hour,
	})

	now := time.Now().Unix()
	rec := &TxnRecord{
		TxnID: "replay-finalize",
		State: TxnStateRollback,
		Participants: []TxnParticipant{
			{Namespace: "default", Key: "missing-key"},
		},
		ExpiresAtUnix: now - 1,
		UpdatedAtUnix: now,
		CreatedAtUnix: now,
	}
	if _, err := svc.putTxnRecord(ctx, rec, ""); err != nil {
		t.Fatalf("seed txn record: %v", err)
	}

	budget := newSweepBudget(svc.clock, IdleSweepOptions{
		Now:        svc.clock.Now(),
		MaxOps:     10,
		MaxRuntime: time.Second,
	})
	if err := svc.sweepTxnRecordsPartial(ctx, budget, sweepModeReplay); err != nil {
		t.Fatalf("replay sweep txn records: %v", err)
	}

	if backend.decisionPutCalls.Load() == 0 {
		t.Fatalf("expected replay sweep to write decision marker")
	}
	if _, _, err := svc.loadTxnRecord(ctx, rec.TxnID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected replay sweep to remove txn record, got %v", err)
	}
	if _, _, err := svc.loadTxnDecisionMarker(ctx, rec.TxnID); err != nil {
		t.Fatalf("expected replay sweep to persist decision marker, got %v", err)
	}
}

func TestMaybeReplayTxnRecordsInlineRuntimeBounded(t *testing.T) {
	backend := &slowListBackend{
		Store: memory.New(),
		delay: time.Second,
	}
	svc := New(Config{
		Store:             backend,
		TxnReplayInterval: time.Millisecond,
	})

	start := time.Now()
	ran := svc.maybeReplayTxnRecordsInline(context.Background())
	if ran {
		t.Fatalf("expected inline replay to report incomplete run when runtime budget is exhausted")
	}
	elapsed := time.Since(start)
	limit := txnReplayInlineMaxRuntime + 100*time.Millisecond
	if elapsed > limit {
		t.Fatalf("expected inline replay bounded by %s, took %s", limit, elapsed)
	}
}

func TestMaybeReplayTxnRecordsInlineTimeoutAllowsImmediateAsyncReplay(t *testing.T) {
	backend := &slowCountingListBackend{
		Store: memory.New(),
		delay: txnReplayInlineMaxRuntime + 50*time.Millisecond,
	}
	svc := New(Config{
		Store:             backend,
		TxnReplayInterval: time.Millisecond,
	})

	start := time.Now()
	ranInline := svc.maybeReplayTxnRecordsInline(context.Background())
	if ranInline {
		t.Fatalf("expected inline replay to return false after timeout")
	}
	inlineElapsed := time.Since(start)
	inlineLimit := txnReplayInlineMaxRuntime + 100*time.Millisecond
	if inlineElapsed > inlineLimit {
		t.Fatalf("expected inline replay bounded by %s, took %s", inlineLimit, inlineElapsed)
	}

	// Simulate call-site fallback path (Get/Dequeue): inline returns false, then async runs.
	svc.maybeReplayTxnRecords(context.Background())
	waitForListCalls(t, &backend.listCalls, 2, 3*time.Second)
}
