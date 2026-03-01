package core

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

func TestReleaseNoStagingRollsBackAndKeepsState(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	ns, key := "default", "release-no-staging"
	stateRes, err := svc.store.WriteState(ctx, ns, key, bytes.NewBufferString(`{"status":"existing"}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	meta := &storage.Meta{
		StateETag:                 stateRes.NewETag,
		StateDescriptor:           stateRes.Descriptor,
		StatePlaintextBytes:       stateRes.BytesWritten,
		Version:                   1,
		PublishedVersion:          1,
		StagedTxnID:               "",
		StagedVersion:             0,
		StagedStateETag:           "",
		StagedStateDescriptor:     nil,
		StagedStatePlaintextBytes: 0,
		StagedRemove:              false,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    ns,
		Key:          key,
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    ns,
		Key:          key,
		LeaseID:      acq.LeaseID,
		TxnID:        acq.TxnID,
		FencingToken: acq.FencingToken,
		Rollback:     false,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	readRes, err := svc.store.ReadState(ctx, ns, key)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	_ = readRes.Reader.Close()
	if readRes.Info == nil || readRes.Info.ETag != stateRes.NewETag {
		t.Fatalf("expected state etag %q, got %+v", stateRes.NewETag, readRes.Info)
	}

	updatedRes, err := svc.store.LoadMeta(ctx, ns, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	updated := updatedRes.Meta
	if updated.Lease != nil {
		t.Fatalf("expected lease cleared after release")
	}
	if updated.StagedTxnID != "" || updated.StagedRemove {
		t.Fatalf("expected no staging after release, got %+v", updated)
	}

	if _, _, err := svc.loadTxnRecord(ctx, acq.TxnID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no txn record for implicit txn, got %v", err)
	}
}

type releaseCASRetryStore struct {
	storage.Backend

	mu        sync.Mutex
	failNext  bool
	failCount int
}

func (s *releaseCASRetryStore) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	s.mu.Lock()
	shouldFail := s.failNext &&
		namespace == "default" &&
		key == "release-cas-retry" &&
		meta != nil &&
		meta.Lease == nil
	if shouldFail {
		s.failNext = false
		s.failCount++
	}
	s.mu.Unlock()
	if shouldFail {
		return "", storage.ErrCASMismatch
	}
	return s.Backend.StoreMeta(ctx, namespace, key, meta, expectedETag)
}

type releaseNotFoundStore struct {
	storage.Backend

	mu        sync.Mutex
	failNext  bool
	failCount int
}

func (s *releaseNotFoundStore) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	s.mu.Lock()
	shouldFail := s.failNext &&
		namespace == "default" &&
		key == "release-notfound-idempotent" &&
		meta != nil &&
		meta.Lease == nil
	if shouldFail {
		s.failNext = false
		s.failCount++
	}
	s.mu.Unlock()
	if shouldFail {
		_ = s.DeleteMeta(ctx, namespace, key, "")
		return "", storage.ErrNotFound
	}
	return s.Backend.StoreMeta(ctx, namespace, key, meta, expectedETag)
}

func TestReleaseNoStagingRetriesOnMetaCASMismatch(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	store := &releaseCASRetryStore{
		Backend: base,
	}
	svc := New(Config{
		Store:            store,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
	})

	ns, key := "default", "release-cas-retry"
	stateRes, err := svc.store.WriteState(ctx, ns, key, bytes.NewBufferString(`{"status":"existing"}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	meta := &storage.Meta{
		StateETag:                 stateRes.NewETag,
		StateDescriptor:           stateRes.Descriptor,
		StatePlaintextBytes:       stateRes.BytesWritten,
		Version:                   1,
		PublishedVersion:          1,
		StagedTxnID:               "",
		StagedVersion:             0,
		StagedStateETag:           "",
		StagedStateDescriptor:     nil,
		StagedStatePlaintextBytes: 0,
		StagedRemove:              false,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    ns,
		Key:          key,
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	store.mu.Lock()
	store.failNext = true
	store.mu.Unlock()

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    ns,
		Key:          key,
		LeaseID:      acq.LeaseID,
		TxnID:        acq.TxnID,
		FencingToken: acq.FencingToken,
		Rollback:     false,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	updatedRes, err := svc.store.LoadMeta(ctx, ns, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if updatedRes.Meta.Lease != nil {
		t.Fatalf("expected lease cleared after release")
	}
	if store.failCount != 1 {
		t.Fatalf("expected one injected CAS mismatch, got %d", store.failCount)
	}
}

func TestReleaseNoStagingTreatsStoreMetaNotFoundAsIdempotent(t *testing.T) {
	ctx := context.Background()
	base := memory.New()
	store := &releaseNotFoundStore{
		Backend: base,
	}
	svc := New(Config{
		Store:            store,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
	})

	ns, key := "default", "release-notfound-idempotent"
	if _, err := svc.store.StoreMeta(ctx, ns, key, &storage.Meta{}, ""); err != nil {
		t.Fatalf("store initial meta: %v", err)
	}
	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    ns,
		Key:          key,
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	store.mu.Lock()
	store.failNext = true
	store.mu.Unlock()

	res, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    ns,
		Key:          key,
		LeaseID:      acq.LeaseID,
		TxnID:        acq.TxnID,
		FencingToken: acq.FencingToken,
	})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if res == nil || !res.Released {
		t.Fatalf("expected idempotent release success, got %#v", res)
	}

	next, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    ns,
		Key:          key,
		Owner:        "worker-2",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	if next.LeaseID == "" {
		t.Fatalf("expected lease id after reacquire")
	}
	if store.failCount != 1 {
		t.Fatalf("expected one injected not_found, got %d", store.failCount)
	}
}
