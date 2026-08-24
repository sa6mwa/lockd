package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/rs/xid"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

type pendingEmptyDecisionTCDecider struct {
	last TxnRecord
}

type promotionOrderingBackend struct {
	storage.Backend
	txnID                      string
	sawExplicitLeaseWithoutTxn bool
}

func (b *promotionOrderingBackend) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	if meta != nil && meta.Lease != nil && meta.Lease.TxnID == b.txnID && meta.Lease.TxnExplicit {
		obj, err := b.GetObject(ctx, txnNamespace, b.txnID)
		if err != nil {
			b.sawExplicitLeaseWithoutTxn = true
		} else {
			_ = obj.Reader.Close()
		}
	}
	return b.Backend.StoreMeta(ctx, namespace, key, meta, expectedETag)
}

func (d *pendingEmptyDecisionTCDecider) Enlist(context.Context, TxnRecord) error {
	return nil
}

func (d *pendingEmptyDecisionTCDecider) Decide(_ context.Context, rec TxnRecord) (TxnState, error) {
	d.last = rec
	if rec.State == "" {
		return TxnStatePending, nil
	}
	return rec.State, nil
}

func TestServerMintedTxnRegistersImplicitParticipant(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          "implicit-txn",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	_, err = svc.Update(ctx, UpdateCommand{
		Namespace:     "default",
		Key:           "implicit-txn",
		LeaseID:       acq.LeaseID,
		FencingToken:  acq.FencingToken,
		TxnID:         acq.TxnID,
		Body:          strings.NewReader(`{"value":"implicit"}`),
		CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	rec, _, err := svc.loadImplicitTxnRecord(ctx, acq.TxnID)
	if err != nil {
		t.Fatalf("load implicit txn record: %v", err)
	}
	if rec == nil || !rec.Implicit || len(rec.Participants) != 1 {
		t.Fatalf("expected one implicit participant, got %+v", rec)
	}
}

func TestServerMintedTxnDefersPublicationUntilAllParticipantsRelease(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	leaseA, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "implicit-xa-a", Owner: "worker-a", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	leaseB, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "implicit-xa-b", Owner: "worker-b", TTLSeconds: 30, BlockSeconds: apiBlockNoWait, TxnID: leaseA.TxnID,
	})
	if err != nil {
		t.Fatalf("acquire second lease: %v", err)
	}

	metaA, err := svc.store.LoadMeta(ctx, "default", "implicit-xa-a")
	if err != nil {
		t.Fatalf("load first meta: %v", err)
	}
	if metaA.Meta.Lease == nil || !metaA.Meta.Lease.TxnExplicit {
		t.Fatalf("first lease was not promoted to XA: %+v", metaA.Meta.Lease)
	}
	for _, update := range []struct {
		key   string
		lease *AcquireResult
		body  string
	}{
		{key: "implicit-xa-a", lease: leaseA, body: `{"value":"a"}`},
		{key: "implicit-xa-b", lease: leaseB, body: `{"value":"b"}`},
	} {
		if _, err := svc.Update(ctx, UpdateCommand{
			Namespace: update.lease.Namespace, Key: update.key, LeaseID: update.lease.LeaseID,
			FencingToken: update.lease.FencingToken, TxnID: update.lease.TxnID,
			Body: strings.NewReader(update.body), CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
		}); err != nil {
			t.Fatalf("stage %s: %v", update.key, err)
		}
	}
	hidden := true
	if _, err := svc.Metadata(ctx, MetadataCommand{
		Namespace: "default", Key: "implicit-xa-a", LeaseID: leaseA.LeaseID,
		FencingToken: leaseA.FencingToken, TxnID: leaseA.TxnID,
		Mutation: MetadataMutation{QueryHidden: &hidden},
	}); err != nil {
		t.Fatalf("stage metadata: %v", err)
	}
	if _, err := svc.Attach(ctx, AttachCommand{
		Namespace: "default", Key: "implicit-xa-b", LeaseID: leaseB.LeaseID,
		FencingToken: leaseB.FencingToken, TxnID: leaseB.TxnID,
		Name: "payload.txt", Body: bytes.NewBufferString("payload"),
	}); err != nil {
		t.Fatalf("stage attachment: %v", err)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: "default", Key: "implicit-xa-a", LeaseID: leaseA.LeaseID, FencingToken: leaseA.FencingToken, TxnID: leaseA.TxnID,
		// This is the cached pre-promotion metadata returned by the first
		// acquire. Release must reload it instead of treating it as a
		// standalone transaction and publishing its staged state.
		KnownMeta: leaseA.Meta, KnownMetaETag: leaseA.MetaETag,
	}); err != nil {
		t.Fatalf("release first lease: %v", err)
	}
	for _, key := range []string{"implicit-xa-a", "implicit-xa-b"} {
		got, err := svc.Get(ctx, GetCommand{Namespace: "default", Key: key, Public: true})
		if err != nil {
			t.Fatalf("public get %s after first release: %v", key, err)
		}
		if got.Reader != nil {
			_ = got.Reader.Close()
		}
		if !got.NoContent {
			t.Fatalf("public state for %s became visible before all releases", key)
		}
	}
	metaA, err = svc.store.LoadMeta(ctx, "default", "implicit-xa-a")
	if err != nil {
		t.Fatalf("load first meta after partial release: %v", err)
	}
	if metaA.Meta.QueryExcluded() {
		t.Fatal("staged metadata became visible before all releases")
	}
	metaB, err := svc.store.LoadMeta(ctx, "default", "implicit-xa-b")
	if err != nil {
		t.Fatalf("load second meta after partial release: %v", err)
	}
	if len(metaB.Meta.Attachments) != 0 {
		t.Fatal("staged attachment became visible before all releases")
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: "default", Key: "implicit-xa-b", LeaseID: leaseB.LeaseID, FencingToken: leaseB.FencingToken, TxnID: leaseB.TxnID,
	}); err != nil {
		t.Fatalf("release second lease: %v", err)
	}
	for _, key := range []string{"implicit-xa-a", "implicit-xa-b"} {
		got, err := svc.Get(ctx, GetCommand{Namespace: "default", Key: key, Public: true})
		if err != nil {
			t.Fatalf("public get %s after commit: %v", key, err)
		}
		if got.NoContent || got.Reader == nil {
			t.Fatalf("public state for %s missing after all releases", key)
		}
		_ = got.Reader.Close()
	}
	metaA, err = svc.store.LoadMeta(ctx, "default", "implicit-xa-a")
	if err != nil {
		t.Fatalf("load first meta after commit: %v", err)
	}
	if !metaA.Meta.QueryExcluded() {
		t.Fatal("staged metadata missing after commit")
	}
	metaB, err = svc.store.LoadMeta(ctx, "default", "implicit-xa-b")
	if err != nil {
		t.Fatalf("load second meta after commit: %v", err)
	}
	if len(metaB.Meta.Attachments) != 1 || metaB.Meta.Attachments[0].Name != "payload.txt" {
		t.Fatalf("staged attachment missing after commit: %+v", metaB.Meta.Attachments)
	}
}

func TestUpdateExplicitTxnRegistersTxnRecord(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := xid.New().String()
	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          "explicit-txn",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        txnID,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	_, err = svc.Update(ctx, UpdateCommand{
		Namespace:     "default",
		Key:           "explicit-txn",
		LeaseID:       acq.LeaseID,
		FencingToken:  acq.FencingToken,
		TxnID:         txnID,
		Body:          strings.NewReader(`{"value":"explicit"}`),
		CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	rec, _, err := svc.loadTxnRecord(ctx, txnID)
	if err != nil {
		t.Fatalf("load txn record: %v", err)
	}
	if rec == nil || rec.TxnID != txnID || len(rec.Participants) == 0 {
		t.Fatalf("expected txn record for explicit txn, got %+v", rec)
	}
}

func TestExplicitTxnReleaseUsesRequestedDecision(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	txnID := xid.New().String()
	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "explicit-release", Owner: "worker", TTLSeconds: 30,
		BlockSeconds: apiBlockNoWait, TxnID: txnID,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := svc.Update(ctx, UpdateCommand{
		Namespace: "default", Key: "explicit-release", LeaseID: acq.LeaseID,
		FencingToken: acq.FencingToken, TxnID: txnID, Body: strings.NewReader(`{"value":"committed"}`),
		CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	}); err != nil {
		t.Fatalf("stage update: %v", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: "default", Key: "explicit-release", LeaseID: acq.LeaseID,
		FencingToken: acq.FencingToken, TxnID: txnID,
	}); err != nil {
		t.Fatalf("release explicit transaction: %v", err)
	}

	got, err := svc.Get(ctx, GetCommand{Namespace: "default", Key: "explicit-release", Public: true})
	if err != nil {
		t.Fatalf("get committed state: %v", err)
	}
	if got.NoContent || got.Reader == nil {
		t.Fatal("expected committed state after release")
	}
	if err := got.Reader.Close(); err != nil {
		t.Fatalf("close committed state: %v", err)
	}
}

func TestReleaseAfterFinalizedImplicitXADecisionPreservesOutcome(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	decider := &pendingEmptyDecisionTCDecider{}
	svc.SetTCDecider(decider)

	leaseA, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "finalized-implicit-a", Owner: "worker-a", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire first implicit lease: %v", err)
	}
	leaseB, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "finalized-implicit-b", Owner: "worker-b", TTLSeconds: 30,
		BlockSeconds: apiBlockNoWait, TxnID: leaseA.TxnID,
	})
	if err != nil {
		t.Fatalf("acquire second implicit participant: %v", err)
	}
	rec, _, err := svc.loadTxnRecord(ctx, leaseA.TxnID)
	if err != nil {
		t.Fatalf("load promoted transaction: %v", err)
	}
	if rec == nil || !rec.Implicit {
		t.Fatalf("expected promoted implicit transaction, got %+v", rec)
	}
	if _, err := svc.CommitTxn(ctx, *rec); err != nil {
		t.Fatalf("externally decide implicit transaction: %v", err)
	}
	if _, _, err := svc.loadTxnRecord(ctx, leaseA.TxnID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected finalized transaction record to be removed, got %v", err)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: "default", Key: "finalized-implicit-b", LeaseID: leaseB.LeaseID,
		FencingToken: leaseB.FencingToken, TxnID: leaseB.TxnID,
		KnownMeta: leaseB.Meta, KnownMetaETag: leaseB.MetaETag,
	}); err != nil {
		t.Fatalf("release with stale finalized implicit-XA lease: %v", err)
	}
	if decider.last.State != TxnStateRollback {
		t.Fatalf("expected stale release to preserve rollback outcome, got %q", decider.last.State)
	}
}

func TestAcquireRejectsDecidedExplicitTxnWithoutPersistingLease(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	txnID := xid.New().String()
	if _, err := svc.putTxnRecord(ctx, &TxnRecord{
		TxnID: txnID,
		State: TxnStateCommit,
	}, ""); err != nil {
		t.Fatalf("store decided transaction: %v", err)
	}

	_, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "decided-txn-lease", Owner: "worker", TTLSeconds: 30,
		BlockSeconds: apiBlockNoWait, TxnID: txnID,
	})
	var failure Failure
	if !errors.As(err, &failure) || failure.Code != "txn_decided" {
		t.Fatalf("acquire error=%v, want txn_decided failure", err)
	}
	if _, err := svc.store.LoadMeta(ctx, "default", "decided-txn-lease"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("decided transaction acquire left metadata behind: %v", err)
	}

	if _, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "decided-txn-lease", Owner: "worker", TTLSeconds: 30,
		BlockSeconds: apiBlockNoWait,
	}); err != nil {
		t.Fatalf("fresh acquire after rejected transaction: %v", err)
	}
}

func TestPromoteImplicitTxnCreatesCoordinatorBeforeMarkingLeaseExplicit(t *testing.T) {
	ctx := context.Background()
	store := &promotionOrderingBackend{Backend: memory.New()}
	svc := New(Config{
		Store:            store,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
	})

	lease, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "promotion-order", Owner: "worker", TTLSeconds: 30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire implicit lease: %v", err)
	}
	store.txnID = lease.TxnID

	if _, err := svc.promoteImplicitTxn(ctx, lease.TxnID); err != nil {
		t.Fatalf("promote implicit transaction: %v", err)
	}
	if store.sawExplicitLeaseWithoutTxn {
		t.Fatal("promotion marked a lease explicit before installing its coordinator record")
	}
	rec, _, err := svc.loadTxnRecord(ctx, lease.TxnID)
	if err != nil {
		t.Fatalf("load promoted transaction: %v", err)
	}
	if rec == nil || !rec.Implicit || rec.State != TxnStatePending {
		t.Fatalf("unexpected promoted transaction: %+v", rec)
	}
}
