package core

import (
	"bytes"
	"context"
	"testing"

	"pkt.systems/lockd/internal/storage"
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

	rec, _, err := svc.loadTxnRecord(ctx, acq.TxnID)
	if err != nil {
		t.Fatalf("load txn record: %v", err)
	}
	if rec.State != TxnStateRollback {
		t.Fatalf("expected rollback decision, got %s", rec.State)
	}
}
