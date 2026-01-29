package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

func newTestService(t testing.TB) *Service {
	t.Helper()
	mem := memory.New()
	return New(Config{
		Store:            mem,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
	})
}

func TestApplyTxnDecisionCommit(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg0"
	ns, key := "default", "commit-key"

	body := bytes.NewBufferString(`{"value":"staged"}`)
	stageRes, err := svc.staging.StageState(ctx, ns, key, txnID, body, storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "lease-1",
			Owner:         "test",
			ExpiresAtUnix: time.Now().Add(5 * time.Minute).Unix(),
			TxnID:         txnID,
		},
		StagedTxnID:           txnID,
		StagedStateETag:       stageRes.NewETag,
		StagedVersion:         1,
		StagedStateDescriptor: stageRes.Descriptor,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	rec := &TxnRecord{
		TxnID:        txnID,
		State:        TxnStateCommit,
		Participants: []TxnParticipant{{Namespace: ns, Key: key}},
	}

	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	updatedRes, err := svc.store.LoadMeta(ctx, ns, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	updated := updatedRes.Meta
	if updated.StagedTxnID != "" || updated.StagedStateETag != "" {
		t.Fatalf("expected staging cleared, got %+v", updated)
	}
	if updated.StateETag == "" {
		t.Fatalf("expected committed state etag")
	}
	if updated.Lease != nil {
		t.Fatalf("expected lease cleared")
	}
}

func TestApplyTxnDecisionCommitIndexesState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := disk.New(disk.Config{
		Root:            root,
		Retention:       0,
		JanitorInterval: 0,
		QueueWatch:      false,
	})
	if err != nil {
		t.Fatalf("disk store: %v", err)
	}
	indexStore := index.NewStore(store, nil)
	indexManager := index.NewManager(indexStore, index.WriterOptions{
		FlushDocs:     1,
		FlushInterval: time.Second,
		Logger:        pslog.NoopLogger(),
	})
	svc := New(Config{
		Store:            store,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
		IndexManager:     indexManager,
		Logger:           pslog.NoopLogger(),
	})

	txnID := "c5v9d0sl70b3m3q8ndg1"
	ns, key := "default", "commit-indexed"

	body := bytes.NewBufferString(`{"value":"indexed"}`)
	stageRes, err := svc.staging.StageState(ctx, ns, key, txnID, body, storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "lease-1",
			Owner:         "test",
			ExpiresAtUnix: time.Now().Add(5 * time.Minute).Unix(),
			TxnID:         txnID,
		},
		StagedTxnID:           txnID,
		StagedStateETag:       stageRes.NewETag,
		StagedVersion:         1,
		StagedStateDescriptor: stageRes.Descriptor,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	rec := &TxnRecord{
		TxnID:        txnID,
		State:        TxnStateCommit,
		Participants: []TxnParticipant{{Namespace: ns, Key: key}},
	}

	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := indexManager.WaitForReadable(waitCtx, ns); err != nil {
		t.Fatalf("index wait: %v", err)
	}
	manifestRes, err := indexStore.LoadManifest(ctx, ns)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifestRes.Manifest == nil {
		t.Fatalf("expected manifest")
	}
	foundSegment := false
	for _, shard := range manifestRes.Manifest.Shards {
		if shard == nil {
			continue
		}
		if len(shard.Segments) > 0 {
			foundSegment = true
			break
		}
	}
	if !foundSegment {
		t.Fatalf("expected index manifest segments")
	}
}

func TestApplyTxnDecisionSkipsOtherBackend(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg2"
	ns := "default"
	localKey := "commit-local"
	remoteKey := "commit-remote"

	stage := func(key string) (*storage.PutStateResult, *storage.Meta) {
		body := bytes.NewBufferString(`{"value":"staged"}`)
		stageRes, err := svc.staging.StageState(ctx, ns, key, txnID, body, storage.PutStateOptions{})
		if err != nil {
			t.Fatalf("stage state %s: %v", key, err)
		}
		meta := &storage.Meta{
			Lease: &storage.Lease{
				ID:            "lease-" + key,
				Owner:         "test",
				ExpiresAtUnix: time.Now().Add(5 * time.Minute).Unix(),
				TxnID:         txnID,
			},
			StagedTxnID:           txnID,
			StagedStateETag:       stageRes.NewETag,
			StagedVersion:         1,
			StagedStateDescriptor: stageRes.Descriptor,
		}
		if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
			t.Fatalf("store meta %s: %v", key, err)
		}
		return stageRes, meta
	}

	_, _ = stage(localKey)
	_, _ = stage(remoteKey)

	rec := &TxnRecord{
		TxnID: txnID,
		State: TxnStateCommit,
		Participants: []TxnParticipant{
			{Namespace: ns, Key: localKey, BackendHash: svc.BackendHash()},
			{Namespace: ns, Key: remoteKey, BackendHash: "remote-backend"},
		},
	}

	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	localMetaRes, err := svc.store.LoadMeta(ctx, ns, localKey)
	if err != nil {
		t.Fatalf("load local meta: %v", err)
	}
	localMeta := localMetaRes.Meta
	if localMeta.StagedTxnID != "" || localMeta.StagedStateETag != "" {
		t.Fatalf("expected local staging cleared, got %+v", localMeta)
	}
	if localMeta.StateETag == "" {
		t.Fatalf("expected local committed state etag")
	}

	remoteMetaRes, err := svc.store.LoadMeta(ctx, ns, remoteKey)
	if err != nil {
		t.Fatalf("load remote meta: %v", err)
	}
	remoteMeta := remoteMetaRes.Meta
	if remoteMeta.StagedTxnID == "" || remoteMeta.StagedStateETag == "" {
		t.Fatalf("expected remote staging to remain, got %+v", remoteMeta)
	}
}

func TestApplyTxnDecisionCommitMissingStagingRecovers(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg9"
	ns, key := "default", "commit-missing-staging"

	stageRes, err := svc.staging.StageState(ctx, ns, key, txnID, bytes.NewBufferString(`{"v":1}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}
	promoted, err := svc.staging.PromoteStagedState(ctx, ns, key, txnID, storage.PromoteStagedOptions{})
	if err != nil {
		t.Fatalf("promote staged state: %v", err)
	}

	meta := &storage.Meta{
		StagedTxnID:               txnID,
		StagedStateETag:           stageRes.NewETag,
		StagedStateDescriptor:     stageRes.Descriptor,
		StagedStatePlaintextBytes: stageRes.BytesWritten,
		StagedVersion:             1,
		StateETag:                 "stale-etag",
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	rec := &TxnRecord{TxnID: txnID, State: TxnStateCommit, Participants: []TxnParticipant{{Namespace: ns, Key: key}}}
	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	updatedRes, err := svc.store.LoadMeta(ctx, ns, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	updated := updatedRes.Meta
	if updated.StagedTxnID != "" || updated.StagedStateETag != "" {
		t.Fatalf("expected staging cleared, got %+v", updated)
	}
	if updated.StateETag != promoted.NewETag {
		t.Fatalf("expected state etag %q, got %q", promoted.NewETag, updated.StateETag)
	}
	if updated.Version != 1 {
		t.Fatalf("expected version 1, got %d", updated.Version)
	}
}

func TestApplyTxnDecisionCommitSkipsPromotionWhenHeadExists(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndga"
	ns, key := "default", "commit-skip-promote"

	body := bytes.NewBufferString(`{"value":"staged"}`)
	stageRes, err := svc.staging.StageState(ctx, ns, key, txnID, body, storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}

	headRes, err := svc.store.WriteState(ctx, ns, key, bytes.NewBufferString(`{"value":"staged"}`), storage.PutStateOptions{IfNotExists: true})
	if err != nil {
		t.Fatalf("write head state: %v", err)
	}

	meta := &storage.Meta{
		StagedTxnID:               txnID,
		StagedStateETag:           stageRes.NewETag,
		StagedStateDescriptor:     stageRes.Descriptor,
		StagedStatePlaintextBytes: stageRes.BytesWritten,
		StagedVersion:             1,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	rec := &TxnRecord{TxnID: txnID, State: TxnStateCommit, Participants: []TxnParticipant{{Namespace: ns, Key: key}}}
	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	updatedRes, err := svc.store.LoadMeta(ctx, ns, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	updated := updatedRes.Meta
	if updated.StagedTxnID != "" || updated.StagedStateETag != "" {
		t.Fatalf("expected staging cleared, got %+v", updated)
	}
	if updated.StateETag != headRes.NewETag {
		t.Fatalf("expected head etag %q, got %q", headRes.NewETag, updated.StateETag)
	}
	if updated.Version != 1 {
		t.Fatalf("expected version 1, got %d", updated.Version)
	}

	stateRes, err := svc.store.ReadState(ctx, ns, key)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if stateRes.Reader != nil {
		_ = stateRes.Reader.Close()
	}
	if stateRes.Info == nil || stateRes.Info.ETag != headRes.NewETag {
		t.Fatalf("expected state etag %q, got %+v", headRes.NewETag, stateRes.Info)
	}

	if _, err := svc.staging.LoadStagedState(ctx, ns, key, txnID); err == nil || !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected staged state removed, got %v", err)
	}
}

func TestApplyTxnDecisionRollback(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg1"
	ns, key := "default", "rollback-key"

	body := bytes.NewBufferString(`{"value":"staged"}`)
	stageRes, err := svc.staging.StageState(ctx, ns, key, txnID, body, storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "lease-1",
			Owner:         "test",
			ExpiresAtUnix: time.Now().Add(5 * time.Minute).Unix(),
			TxnID:         txnID,
		},
		StagedTxnID:           txnID,
		StagedStateETag:       stageRes.NewETag,
		StagedVersion:         1,
		StagedStateDescriptor: stageRes.Descriptor,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	rec := &TxnRecord{
		TxnID:        txnID,
		State:        TxnStateRollback,
		Participants: []TxnParticipant{{Namespace: ns, Key: key}},
	}

	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	updatedRes, err := svc.store.LoadMeta(ctx, ns, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	updated := updatedRes.Meta
	if updated.StagedTxnID != "" || updated.StagedStateETag != "" {
		t.Fatalf("expected staging cleared, got %+v", updated)
	}
	if updated.StateETag != "" {
		t.Fatalf("expected state to remain uncommitted after rollback")
	}
	if updated.Lease != nil {
		t.Fatalf("expected lease cleared")
	}
}

func TestApplyTxnDecisionCommitStagedRemoveDeletesState(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg6"
	ns, key := "default", "commit-remove-key"

	stateRes, err := svc.store.WriteState(ctx, ns, key, bytes.NewBufferString(`{"status":"remove-me"}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "lease-1",
			Owner:         "test",
			ExpiresAtUnix: time.Now().Add(5 * time.Minute).Unix(),
			TxnID:         txnID,
		},
		StateETag:             stateRes.NewETag,
		StateDescriptor:       stateRes.Descriptor,
		StatePlaintextBytes:   stateRes.BytesWritten,
		Version:               1,
		PublishedVersion:      1,
		StagedTxnID:           txnID,
		StagedRemove:          true,
		StagedVersion:         2,
		StagedStateETag:       "",
		StagedStateDescriptor: nil,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	rec := &TxnRecord{
		TxnID:        txnID,
		State:        TxnStateCommit,
		Participants: []TxnParticipant{{Namespace: ns, Key: key}},
	}
	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	if _, err := svc.store.ReadState(ctx, ns, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected state removed, got %v", err)
	}
	updatedRes, err := svc.store.LoadMeta(ctx, ns, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	updated := updatedRes.Meta
	if updated.StateETag != "" {
		t.Fatalf("expected state etag cleared, got %q", updated.StateETag)
	}
	if updated.Version != meta.StagedVersion || updated.PublishedVersion != meta.StagedVersion {
		t.Fatalf("expected version %d, got %d/%d", meta.StagedVersion, updated.Version, updated.PublishedVersion)
	}
	if updated.StagedRemove || updated.StagedTxnID != "" {
		t.Fatalf("expected staged remove cleared, got %+v", updated)
	}
	if updated.Lease != nil {
		t.Fatalf("expected lease cleared")
	}
}

func TestApplyTxnDecisionRollbackStagedRemoveKeepsState(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg7"
	ns, key := "default", "rollback-remove-key"

	stateRes, err := svc.store.WriteState(ctx, ns, key, bytes.NewBufferString(`{"status":"keep-me"}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "lease-1",
			Owner:         "test",
			ExpiresAtUnix: time.Now().Add(5 * time.Minute).Unix(),
			TxnID:         txnID,
		},
		StateETag:             stateRes.NewETag,
		StateDescriptor:       stateRes.Descriptor,
		StatePlaintextBytes:   stateRes.BytesWritten,
		Version:               1,
		PublishedVersion:      1,
		StagedTxnID:           txnID,
		StagedRemove:          true,
		StagedVersion:         2,
		StagedStateETag:       "",
		StagedStateDescriptor: nil,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	rec := &TxnRecord{
		TxnID:        txnID,
		State:        TxnStateRollback,
		Participants: []TxnParticipant{{Namespace: ns, Key: key}},
	}
	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
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
	if updated.Version != meta.Version || updated.PublishedVersion != meta.PublishedVersion {
		t.Fatalf("expected version %d, got %d/%d", meta.Version, updated.Version, updated.PublishedVersion)
	}
	if updated.StagedRemove || updated.StagedTxnID != "" {
		t.Fatalf("expected staged remove cleared, got %+v", updated)
	}
	if updated.Lease != nil {
		t.Fatalf("expected lease cleared")
	}
}

func TestApplyTxnDecisionSkipsOtherStagedTxn(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	otherTxn := "c5v9d0sl70b3m3q8ndg8"
	thisTxn := "c5v9d0sl70b3m3q8ndg9"
	ns, key := "default", "skip-other-staged"

	stageRes, err := svc.staging.StageState(ctx, ns, key, otherTxn, bytes.NewBufferString(`{"value":"other"}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "lease-1",
			Owner:         "test",
			ExpiresAtUnix: time.Now().Add(5 * time.Minute).Unix(),
			TxnID:         otherTxn,
		},
		StagedTxnID:           otherTxn,
		StagedStateETag:       stageRes.NewETag,
		StagedVersion:         1,
		StagedStateDescriptor: stageRes.Descriptor,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	rec := &TxnRecord{
		TxnID:        thisTxn,
		State:        TxnStateCommit,
		Participants: []TxnParticipant{{Namespace: ns, Key: key}},
	}
	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	updatedRes, err := svc.store.LoadMeta(ctx, ns, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	updated := updatedRes.Meta
	if updated.StagedTxnID != otherTxn || updated.StagedStateETag != stageRes.NewETag {
		t.Fatalf("expected staged txn %s to remain, got %+v", otherTxn, updated)
	}
	if updated.Lease == nil || updated.Lease.TxnID != otherTxn {
		t.Fatalf("expected lease txn %s to remain, got %+v", otherTxn, updated.Lease)
	}
}

func TestSweepTxnRecordsRollsBackExpiredPending(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	now := time.Now()
	txnID := "c5v9d0sl70b3m3q8ndg2"
	ns, key := "default", "sweep-rollback"

	body := bytes.NewBufferString(`{"value":"staged"}`)
	stageRes, err := svc.staging.StageState(ctx, ns, key, txnID, body, storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "lease-1",
			Owner:         "test",
			ExpiresAtUnix: now.Add(-time.Minute).Unix(),
			TxnID:         txnID,
		},
		StagedTxnID:           txnID,
		StagedStateETag:       stageRes.NewETag,
		StagedVersion:         1,
		StagedStateDescriptor: stageRes.Descriptor,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	rec := TxnRecord{
		TxnID:         txnID,
		State:         TxnStatePending,
		Participants:  []TxnParticipant{{Namespace: ns, Key: key}},
		ExpiresAtUnix: now.Add(-30 * time.Second).Unix(),
		CreatedAtUnix: now.Add(-time.Minute).Unix(),
		UpdatedAtUnix: now.Add(-time.Minute).Unix(),
	}
	buf := &bytes.Buffer{}
	_ = json.NewEncoder(buf).Encode(rec)
	if _, err := svc.store.PutObject(ctx, txnNamespace, txnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{ContentType: storage.ContentTypeJSON}); err != nil {
		t.Fatalf("put txn record: %v", err)
	}

	if err := svc.SweepTxnRecords(ctx, now); err != nil {
		t.Fatalf("sweep txn records: %v", err)
	}

	updatedRes, err := svc.store.LoadMeta(ctx, ns, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	updated := updatedRes.Meta
	if updated.StagedTxnID != "" || updated.StagedStateETag != "" {
		t.Fatalf("expected staging cleared after sweep, got %+v", updated)
	}
	if updated.Lease != nil {
		t.Fatalf("expected lease cleared after sweep")
	}
	if _, err := svc.store.GetObject(ctx, txnNamespace, txnID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected txn record to be deleted after sweep, got %v", err)
	}
}

type failingPromoteStaging struct {
	base storage.StagingBackend
	err  error
}

func (f failingPromoteStaging) StageState(ctx context.Context, ns, key, txnID string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	return f.base.StageState(ctx, ns, key, txnID, body, opts)
}
func (f failingPromoteStaging) LoadStagedState(ctx context.Context, ns, key, txnID string) (storage.ReadStateResult, error) {
	return f.base.LoadStagedState(ctx, ns, key, txnID)
}
func (f failingPromoteStaging) PromoteStagedState(ctx context.Context, ns, key, txnID string, opts storage.PromoteStagedOptions) (*storage.PutStateResult, error) {
	return nil, f.err
}
func (f failingPromoteStaging) DiscardStagedState(ctx context.Context, ns, key, txnID string, opts storage.DiscardStagedOptions) error {
	return f.base.DiscardStagedState(ctx, ns, key, txnID, opts)
}
func (f failingPromoteStaging) ListStagedState(ctx context.Context, ns string, opts storage.ListStagedOptions) (*storage.ListResult, error) {
	return f.base.ListStagedState(ctx, ns, opts)
}

func TestTxnDecisionCounters(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	// Successful apply increments applied counter.
	txnID := "c5v9d0sl70b3m3q8ndg3"
	stageRes, err := svc.staging.StageState(ctx, "default", "counter-ok", txnID, bytes.NewBufferString(`{"v":1}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}
	meta := &storage.Meta{
		Lease:                 &storage.Lease{ID: "lease-1", Owner: "test", ExpiresAtUnix: time.Now().Add(time.Minute).Unix(), TxnID: txnID},
		StagedTxnID:           txnID,
		StagedStateETag:       stageRes.NewETag,
		StagedVersion:         1,
		StagedStateDescriptor: stageRes.Descriptor,
	}
	if _, err := svc.store.StoreMeta(ctx, "default", "counter-ok", meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	rec := &TxnRecord{TxnID: txnID, State: TxnStateCommit, Participants: []TxnParticipant{{Namespace: "default", Key: "counter-ok"}}}
	if err := svc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply decision: %v", err)
	}
	applied, failed := svc.TxnDecisionCounters()
	if applied != 1 || failed != 0 {
		t.Fatalf("unexpected counters after success: applied=%d failed=%d", applied, failed)
	}

	// Failed promote increments failed counter and returns an error.
	failing := failingPromoteStaging{base: svc.staging, err: errors.New("promote-fail")}
	svc.staging = failing
	txnID2 := "c5v9d0sl70b3m3q8ndg4"
	stageRes2, err := failing.base.StageState(ctx, "default", "counter-fail", txnID2, bytes.NewBufferString(`{"v":2}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state (fail case): %v", err)
	}
	meta2 := &storage.Meta{
		Lease:                 &storage.Lease{ID: "lease-2", Owner: "test", ExpiresAtUnix: time.Now().Add(time.Minute).Unix(), TxnID: txnID2},
		StagedTxnID:           txnID2,
		StagedStateETag:       stageRes2.NewETag,
		StagedVersion:         1,
		StagedStateDescriptor: stageRes2.Descriptor,
	}
	if _, err := svc.store.StoreMeta(ctx, "default", "counter-fail", meta2, ""); err != nil {
		t.Fatalf("store meta (fail case): %v", err)
	}
	rec2 := &TxnRecord{TxnID: txnID2, State: TxnStateCommit, Participants: []TxnParticipant{{Namespace: "default", Key: "counter-fail"}}}
	if err := svc.applyTxnDecision(ctx, rec2); err == nil {
		t.Fatalf("expected apply decision to return error on failing promote")
	}
	applied, failed = svc.TxnDecisionCounters()
	if applied != 1 || failed != 1 {
		t.Fatalf("unexpected counters after failure: applied=%d failed=%d", applied, failed)
	}
}

func TestCommitTxnDeletesRecordAfterApply(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndga"
	ns, key := "default", "commit-cleanup"

	stageRes, err := svc.staging.StageState(ctx, ns, key, txnID, bytes.NewBufferString(`{"v":1}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}
	meta := &storage.Meta{
		Lease:                 &storage.Lease{ID: "lease-1", Owner: "test", ExpiresAtUnix: time.Now().Add(time.Minute).Unix(), TxnID: txnID},
		StagedTxnID:           txnID,
		StagedStateETag:       stageRes.NewETag,
		StagedVersion:         1,
		StagedStateDescriptor: stageRes.Descriptor,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	state, err := svc.CommitTxn(ctx, TxnRecord{
		TxnID:        txnID,
		Participants: []TxnParticipant{{Namespace: ns, Key: key}},
	})
	if err != nil {
		t.Fatalf("commit txn: %v", err)
	}
	if state != TxnStateCommit {
		t.Fatalf("expected commit state, got %s", state)
	}
	if _, err := svc.store.GetObject(ctx, txnNamespace, txnID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected txn record to be deleted after apply, got %v", err)
	}
}

func TestCommitTxnRetainsRecordOnApplyFailure(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg5"
	ns, key := "default", "commit-fail"

	stageRes, err := svc.staging.StageState(ctx, ns, key, txnID, bytes.NewBufferString(`{"v":1}`), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("stage state: %v", err)
	}
	meta := &storage.Meta{
		Lease:                 &storage.Lease{ID: "lease-1", Owner: "test", ExpiresAtUnix: time.Now().Add(time.Minute).Unix(), TxnID: txnID},
		StagedTxnID:           txnID,
		StagedStateETag:       stageRes.NewETag,
		StagedVersion:         1,
		StagedStateDescriptor: stageRes.Descriptor,
	}
	if _, err := svc.store.StoreMeta(ctx, ns, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	svc.staging = failingPromoteStaging{base: svc.staging, err: errors.New("promote-fail")}
	rec := TxnRecord{TxnID: txnID, Participants: []TxnParticipant{{Namespace: ns, Key: key}}}
	state, err := svc.CommitTxn(ctx, rec)
	if err == nil {
		t.Fatalf("expected commit error on failed apply")
	}
	if state != TxnStateCommit {
		t.Fatalf("expected commit state, got %s", state)
	}

	if _, err := svc.store.GetObject(ctx, txnNamespace, txnID); err != nil {
		t.Fatalf("expected txn record to remain after apply failure: %v", err)
	}
}

func TestTxnDecisionMarkerConflict(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndgb"
	marker := txnDecisionMarker{
		TxnID:         txnID,
		State:         TxnStateCommit,
		TCTerm:        4,
		ExpiresAtUnix: time.Now().Add(time.Minute).Unix(),
		UpdatedAtUnix: time.Now().Unix(),
	}
	if _, err := svc.putTxnDecisionMarker(ctx, &marker, ""); err != nil {
		t.Fatalf("put marker: %v", err)
	}

	_, err := svc.RollbackTxn(ctx, TxnRecord{
		TxnID:  txnID,
		State:  TxnStateRollback,
		TCTerm: 4,
	})
	if err == nil {
		t.Fatalf("expected txn_conflict on rollback with marker")
	}
	assertFailureCode(t, err, "txn_conflict")
}

func TestTxnDecisionRejectsStaleTerm(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg6"
	if _, err := svc.DecideTxn(ctx, TxnRecord{TxnID: txnID, State: TxnStatePending, TCTerm: 2}); err != nil {
		t.Fatalf("decide pending: %v", err)
	}
	if _, err := svc.CommitTxn(ctx, TxnRecord{TxnID: txnID, TCTerm: 1}); err == nil {
		t.Fatalf("expected stale term error")
	} else {
		assertFailureCode(t, err, "tc_term_stale")
	}
}

func TestTxnDecisionRejectsConflictForSameTerm(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg7"
	if _, err := svc.DecideTxn(ctx, TxnRecord{TxnID: txnID, State: TxnStateCommit, TCTerm: 3}); err != nil {
		t.Fatalf("decide commit: %v", err)
	}
	if _, err := svc.RollbackTxn(ctx, TxnRecord{TxnID: txnID, TCTerm: 3}); err == nil {
		t.Fatalf("expected conflict error")
	} else {
		assertFailureCode(t, err, "txn_conflict")
	}
}

func TestTxnDecisionRequiresTermWhenRecorded(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := "c5v9d0sl70b3m3q8ndg8"
	if _, err := svc.DecideTxn(ctx, TxnRecord{TxnID: txnID, State: TxnStatePending, TCTerm: 4}); err != nil {
		t.Fatalf("decide pending: %v", err)
	}
	if _, err := svc.CommitTxn(ctx, TxnRecord{TxnID: txnID}); err == nil {
		t.Fatalf("expected term required error")
	} else {
		assertFailureCode(t, err, "tc_term_required")
	}
}

func assertFailureCode(t *testing.T, err error, code string) {
	t.Helper()
	var failure Failure
	if !errors.As(err, &failure) {
		t.Fatalf("expected core failure, got %T (%v)", err, err)
	}
	if failure.Code != code {
		t.Fatalf("expected code %q, got %q", code, failure.Code)
	}
}
