package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
	"pkt.systems/lockd/internal/storage/memory"
)

type pendingEmptyDecisionTCDecider struct {
	last TxnRecord
}

type failingEnlistTCDecider struct {
	err error
}

type promotionOrderingBackend struct {
	storage.Backend
	txnID                      string
	sawExplicitLeaseWithoutTxn bool
}

type promotionBarrierBackend struct {
	storage.Backend
	txnID   string
	started chan struct{}
	resume  chan struct{}
	once    sync.Once
}

type promotionCASConflictBackend struct {
	storage.Backend
	txnID string
	key   string
	once  sync.Once
}

type failingImplicitEnrollmentBackend struct {
	storage.Backend
	fail bool
}

func (b *failingImplicitEnrollmentBackend) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	if b.fail && namespace == implicitTxnNamespace {
		return nil, errors.New("implicit transaction record unavailable")
	}
	return b.Backend.PutObject(ctx, namespace, key, body, opts)
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

func (b *promotionBarrierBackend) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	if meta != nil && meta.Lease != nil && meta.Lease.TxnID == b.txnID && meta.Lease.TxnExplicit {
		b.once.Do(func() { close(b.started) })
		select {
		case <-b.resume:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return b.Backend.StoreMeta(ctx, namespace, key, meta, expectedETag)
}

func (b *promotionCASConflictBackend) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	if key == b.key && meta != nil && meta.Lease != nil && meta.Lease.TxnID == b.txnID {
		forced := false
		b.once.Do(func() { forced = true })
		if forced {
			return "", storage.ErrCASMismatch
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

func (d failingEnlistTCDecider) Enlist(context.Context, TxnRecord) error {
	return d.err
}

func (failingEnlistTCDecider) Decide(_ context.Context, rec TxnRecord) (TxnState, error) {
	return rec.State, nil
}

func TestExplicitAcquireDiskDoesNotRereadPendingTxnRecord(t *testing.T) {
	store, err := disk.New(disk.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	svc := New(Config{
		Store:            store,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
	})
	txnID := xid.New().String()
	res, err := svc.Acquire(context.Background(), AcquireCommand{
		Key:          "explicit-disk-pending-txn",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: 0,
		TxnID:        txnID,
	})
	if err != nil {
		t.Fatalf("explicit acquire: %v", err)
	}
	if res.TxnID != txnID {
		t.Fatalf("transaction id = %q, want %q", res.TxnID, txnID)
	}

	rec, _, err := svc.loadTxnRecord(context.Background(), txnID)
	if err != nil {
		t.Fatalf("load transaction record: %v", err)
	}
	if len(rec.Participants) != 1 || rec.Participants[0].Key != "explicit-disk-pending-txn" {
		t.Fatalf("transaction participants = %+v, want acquired key", rec.Participants)
	}
}

func TestImplicitPromotionDiskKeepsFirstReleasePrivate(t *testing.T) {
	store, err := disk.New(disk.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	svc := New(Config{Store: store, BackendHash: "test-backend", DefaultNamespace: "default"})
	leaseA, err := svc.Acquire(ctx, AcquireCommand{
		Key: "implicit-disk-a", Owner: "worker-a", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	leaseB, err := svc.Acquire(ctx, AcquireCommand{
		Key: "implicit-disk-b", Owner: "worker-b", TTLSeconds: 30, BlockSeconds: apiBlockNoWait, TxnID: leaseA.TxnID,
	})
	if err != nil {
		t.Fatalf("acquire promoted participant: %v", err)
	}
	for _, lease := range []*AcquireResult{leaseA, leaseB} {
		if _, err := svc.Update(ctx, UpdateCommand{
			Namespace: lease.Namespace, Key: lease.Key, LeaseID: lease.LeaseID, FencingToken: lease.FencingToken, TxnID: lease.TxnID,
			Body: strings.NewReader(`{"value":"staged"}`), CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
		}); err != nil {
			t.Fatalf("stage %s: %v", lease.Key, err)
		}
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: leaseA.Namespace, Key: leaseA.Key, LeaseID: leaseA.LeaseID, FencingToken: leaseA.FencingToken, TxnID: leaseA.TxnID,
	}); err != nil {
		t.Fatalf("release first participant: %v", err)
	}
	for _, lease := range []*AcquireResult{leaseA, leaseB} {
		got, err := svc.Get(ctx, GetCommand{Namespace: lease.Namespace, Key: lease.Key, Public: true})
		if err != nil {
			t.Fatalf("public get %s after first release: %v", lease.Key, err)
		}
		if got.Reader != nil {
			_ = got.Reader.Close()
		}
		if !got.NoContent {
			t.Fatalf("first release published staged state for %s", lease.Key)
		}
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: leaseB.Namespace, Key: leaseB.Key, LeaseID: leaseB.LeaseID, FencingToken: leaseB.FencingToken, TxnID: leaseB.TxnID,
	}); err != nil {
		t.Fatalf("release final participant: %v", err)
	}
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

func TestAcquireConflictAfterImplicitPromotionLeavesSeedReleasable(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	seed, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "implicit-seed", Owner: "seed", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire seed lease: %v", err)
	}
	if _, err := svc.Update(ctx, UpdateCommand{
		Namespace: seed.Namespace, Key: seed.Key, LeaseID: seed.LeaseID, FencingToken: seed.FencingToken, TxnID: seed.TxnID,
		Body: strings.NewReader(`{"value":"seed"}`), CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	}); err != nil {
		t.Fatalf("stage seed state: %v", err)
	}

	occupied, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "occupied", Owner: "other", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire occupied lease: %v", err)
	}
	defer func() {
		_, _ = svc.Release(ctx, ReleaseCommand{
			Namespace: occupied.Namespace, Key: occupied.Key, LeaseID: occupied.LeaseID, FencingToken: occupied.FencingToken, TxnID: occupied.TxnID,
		})
	}()

	_, err = svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "occupied", Owner: "reuse", TTLSeconds: 30, BlockSeconds: apiBlockNoWait, TxnID: seed.TxnID,
	})
	var failure Failure
	if !errors.As(err, &failure) || failure.Code != "waiting" {
		t.Fatalf("conflicting follow-up acquire error=%v, want waiting", err)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: seed.Namespace, Key: seed.Key, LeaseID: seed.LeaseID, FencingToken: seed.FencingToken, TxnID: seed.TxnID,
	}); err != nil {
		t.Fatalf("release seed after rejected promotion: %v", err)
	}
	state, err := svc.Get(ctx, GetCommand{Namespace: seed.Namespace, Key: seed.Key, Public: true})
	if err != nil {
		t.Fatalf("get released seed state: %v", err)
	}
	if state.NoContent {
		t.Fatal("rejected promotion discarded seed state")
	}
	defer state.Reader.Close()
	var got map[string]string
	if err := json.NewDecoder(state.Reader).Decode(&got); err != nil {
		t.Fatalf("decode released seed state: %v", err)
	}
	if got["value"] != "seed" {
		t.Fatalf("released seed state=%v", got)
	}
}

func TestAcquireCASConflictAfterImplicitPromotionRestoresSeed(t *testing.T) {
	ctx := context.Background()
	store := &promotionCASConflictBackend{Backend: memory.New(), key: "promotion-cas-conflict"}
	svc := New(Config{Store: store, BackendHash: "test-backend", DefaultNamespace: "default"})

	seed, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "implicit-cas-seed", Owner: "seed", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire seed lease: %v", err)
	}
	store.txnID = seed.TxnID
	if _, err := svc.Update(ctx, UpdateCommand{
		Namespace: seed.Namespace, Key: seed.Key, LeaseID: seed.LeaseID, FencingToken: seed.FencingToken, TxnID: seed.TxnID,
		Body: strings.NewReader(`{"value":"seed"}`), CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	}); err != nil {
		t.Fatalf("stage seed state: %v", err)
	}

	_, err = svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: store.key, Owner: "reuse", TTLSeconds: 30, BlockSeconds: apiBlockNoWait, IfNotExists: true, TxnID: seed.TxnID,
	})
	var failure Failure
	if !errors.As(err, &failure) || failure.Code != "already_exists" {
		t.Fatalf("CAS-conflicting follow-up acquire error=%v, want already_exists", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: seed.Namespace, Key: seed.Key, LeaseID: seed.LeaseID, FencingToken: seed.FencingToken, TxnID: seed.TxnID,
	}); err != nil {
		t.Fatalf("release seed after CAS-conflicting promotion: %v", err)
	}
}

func TestImplicitPromotionEnlistFailureRestoresSeedLease(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	svc.SetTCDecider(failingEnlistTCDecider{err: errors.New("tc unavailable")})

	seed, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "enlist-failure-seed", Owner: "seed", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire seed lease: %v", err)
	}
	if _, err := svc.Update(ctx, UpdateCommand{
		Namespace: seed.Namespace, Key: seed.Key, LeaseID: seed.LeaseID, FencingToken: seed.FencingToken, TxnID: seed.TxnID,
		Body: strings.NewReader(`{"value":"seed"}`), CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	}); err != nil {
		t.Fatalf("stage seed state: %v", err)
	}

	_, err = svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "enlist-failure-second", Owner: "reuse", TTLSeconds: 30, BlockSeconds: apiBlockNoWait, TxnID: seed.TxnID,
	})
	if err == nil || !strings.Contains(err.Error(), "tc unavailable") {
		t.Fatalf("second acquire error=%v, want TC enlist failure", err)
	}
	if _, _, err := svc.loadTxnRecord(ctx, seed.TxnID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("failed promotion left coordinator record: %v", err)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: seed.Namespace, Key: seed.Key, LeaseID: seed.LeaseID, FencingToken: seed.FencingToken, TxnID: seed.TxnID,
	}); err != nil {
		t.Fatalf("release seed after failed promotion: %v", err)
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

func TestReleaseWaitsForImplicitPromotionEnrollment(t *testing.T) {
	ctx := context.Background()
	store := &promotionBarrierBackend{
		Backend: memory.New(),
		started: make(chan struct{}),
		resume:  make(chan struct{}),
	}
	svc := New(Config{Store: store, BackendHash: "test-backend", DefaultNamespace: "default"})

	leaseA, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "promotion-race-a", Owner: "worker-a", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire seed lease: %v", err)
	}
	store.txnID = leaseA.TxnID
	if _, err := svc.Update(ctx, UpdateCommand{
		Namespace: leaseA.Namespace, Key: leaseA.Key, LeaseID: leaseA.LeaseID, FencingToken: leaseA.FencingToken, TxnID: leaseA.TxnID,
		Body: strings.NewReader(`{"value":"a"}`), CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	}); err != nil {
		t.Fatalf("stage seed state: %v", err)
	}

	leaseBCh := make(chan *AcquireResult, 1)
	errCh := make(chan error, 1)
	go func() {
		leaseB, acquireErr := svc.Acquire(ctx, AcquireCommand{
			Namespace: "default", Key: "promotion-race-b", Owner: "worker-b", TTLSeconds: 30, BlockSeconds: apiBlockNoWait, TxnID: leaseA.TxnID,
		})
		if acquireErr != nil {
			errCh <- acquireErr
			return
		}
		leaseBCh <- leaseB
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("promotion did not reach first explicit lease")
	}

	_, err = svc.Release(ctx, ReleaseCommand{
		Namespace: leaseA.Namespace, Key: leaseA.Key, LeaseID: leaseA.LeaseID, FencingToken: leaseA.FencingToken, TxnID: leaseA.TxnID,
	})
	var failure Failure
	if !errors.As(err, &failure) || failure.Code != "txn_pending" {
		t.Fatalf("release during promotion error=%v, want txn_pending", err)
	}
	got, err := svc.Get(ctx, GetCommand{Namespace: leaseA.Namespace, Key: leaseA.Key, Public: true})
	if err != nil {
		t.Fatalf("public get during promotion: %v", err)
	}
	if got.Reader != nil {
		_ = got.Reader.Close()
	}
	if !got.NoContent {
		t.Fatal("release during promotion published staged state")
	}

	close(store.resume)
	var leaseB *AcquireResult
	select {
	case err := <-errCh:
		t.Fatalf("acquire second lease: %v", err)
	case leaseB = <-leaseBCh:
	case <-time.After(time.Second):
		t.Fatal("second acquire did not complete after promotion unblocked")
	}
	if _, err := svc.Update(ctx, UpdateCommand{
		Namespace: leaseB.Namespace, Key: leaseB.Key, LeaseID: leaseB.LeaseID, FencingToken: leaseB.FencingToken, TxnID: leaseB.TxnID,
		Body: strings.NewReader(`{"value":"b"}`), CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	}); err != nil {
		t.Fatalf("stage second state: %v", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: leaseA.Namespace, Key: leaseA.Key, LeaseID: leaseA.LeaseID, FencingToken: leaseA.FencingToken, TxnID: leaseA.TxnID,
	}); err != nil {
		t.Fatalf("release seed lease after enrollment: %v", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: leaseB.Namespace, Key: leaseB.Key, LeaseID: leaseB.LeaseID, FencingToken: leaseB.FencingToken, TxnID: leaseB.TxnID,
	}); err != nil {
		t.Fatalf("release second lease: %v", err)
	}
	for _, key := range []string{leaseA.Key, leaseB.Key} {
		state, getErr := svc.Get(ctx, GetCommand{Namespace: "default", Key: key, Public: true})
		if getErr != nil {
			t.Fatalf("public get %s after commit: %v", key, getErr)
		}
		if state.NoContent || state.Reader == nil {
			t.Fatalf("public state %s missing after commit", key)
		}
		_ = state.Reader.Close()
	}
}

func TestAcquireCompensatesFailedImplicitEnrollment(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		seedMeta *storage.Meta
	}{
		{name: "new key"},
		{name: "existing metadata", seedMeta: &storage.Meta{Attributes: map[string]string{"preserve": "yes"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &failingImplicitEnrollmentBackend{Backend: memory.New(), fail: true}
			svc := New(Config{Store: store, BackendHash: "test-backend", DefaultNamespace: "default"})
			key := "failed-implicit-enrollment"
			if tc.seedMeta != nil {
				if _, err := store.StoreMeta(ctx, "default", key, tc.seedMeta, ""); err != nil {
					t.Fatalf("seed metadata: %v", err)
				}
			}

			if _, err := svc.Acquire(ctx, AcquireCommand{
				Namespace: "default", Key: key, Owner: "worker", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
			}); err == nil {
				t.Fatal("expected implicit enrollment failure")
			}

			metaRes, err := store.LoadMeta(ctx, "default", key)
			if tc.seedMeta == nil {
				if !errors.Is(err, storage.ErrNotFound) {
					t.Fatalf("failed acquire left metadata behind: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("load restored metadata: %v", err)
				}
				if metaRes.Meta.Lease != nil || metaRes.Meta.Attributes["preserve"] != "yes" {
					t.Fatalf("failed acquire did not restore metadata: %+v", metaRes.Meta)
				}
			}

			store.fail = false
			if _, err := svc.Acquire(ctx, AcquireCommand{
				Namespace: "default", Key: key, Owner: "worker", TTLSeconds: 30, BlockSeconds: apiBlockNoWait,
			}); err != nil {
				t.Fatalf("fresh acquire after enrollment failure: %v", err)
			}
		})
	}
}

func TestPromoteImplicitTxnPrunesExpiredSeedParticipant(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	svc := newTestServiceWithClock(t, clk)

	seed, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "expired-seed", Owner: "seed-worker", TTLSeconds: 1, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire seed lease: %v", err)
	}
	clk.Advance(2 * time.Second)

	participant, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "promoted-after-expiry", Owner: "next-worker", TTLSeconds: 30,
		BlockSeconds: apiBlockNoWait, TxnID: seed.TxnID,
	})
	if err != nil {
		t.Fatalf("acquire using expired seed xid: %v", err)
	}
	if _, _, err := svc.loadImplicitTxnRecord(ctx, seed.TxnID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expired seed record was retained: %v", err)
	}

	if _, err := svc.Update(ctx, UpdateCommand{
		Namespace: "default", Key: "promoted-after-expiry", LeaseID: participant.LeaseID,
		FencingToken: participant.FencingToken, TxnID: participant.TxnID,
		Body:          strings.NewReader(`{"value":"published"}`),
		CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	}); err != nil {
		t.Fatalf("stage participant state: %v", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: "default", Key: "promoted-after-expiry", LeaseID: participant.LeaseID,
		FencingToken: participant.FencingToken, TxnID: participant.TxnID,
	}); err != nil {
		t.Fatalf("release participant: %v", err)
	}
	got, err := svc.Get(ctx, GetCommand{Namespace: "default", Key: "promoted-after-expiry", Public: true})
	if err != nil {
		t.Fatalf("get published state: %v", err)
	}
	if got.NoContent || got.Reader == nil {
		t.Fatal("state remained unpublished after stale seed pruning")
	}
	_ = got.Reader.Close()
}

func TestExpiredImplicitLeaseCleanupRemovesSeedParticipant(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	svc := newTestServiceWithClock(t, clk)

	seed, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "cleanup-expired-seed", Owner: "seed-worker", TTLSeconds: 1, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire seed lease: %v", err)
	}
	clk.Advance(2 * time.Second)
	if _, err := svc.Get(ctx, GetCommand{
		Namespace: "default", Key: "cleanup-expired-seed", LeaseID: seed.LeaseID,
		FencingToken: seed.FencingToken,
	}); err == nil {
		t.Fatal("expected expired lease get to fail")
	}
	if _, _, err := svc.loadImplicitTxnRecord(ctx, seed.TxnID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expired lease cleanup retained implicit seed: %v", err)
	}
}

func TestExpiredPromotedImplicitLeaseCleanupUnblocksRemainingParticipant(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	svc := newTestServiceWithClock(t, clk)

	seed, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "promoted-expired-seed", Owner: "seed-worker", TTLSeconds: 1, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire seed lease: %v", err)
	}
	participant, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "promoted-survivor", Owner: "survivor-worker", TTLSeconds: 30, BlockSeconds: apiBlockNoWait, TxnID: seed.TxnID,
	})
	if err != nil {
		t.Fatalf("acquire promoted participant: %v", err)
	}
	if _, err := svc.Update(ctx, UpdateCommand{
		Namespace: participant.Namespace, Key: participant.Key, LeaseID: participant.LeaseID, FencingToken: participant.FencingToken, TxnID: participant.TxnID,
		Body: strings.NewReader(`{"value":"committed by survivor"}`), CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	}); err != nil {
		t.Fatalf("stage surviving participant state: %v", err)
	}

	clk.Advance(2 * time.Second)
	if _, err := svc.Get(ctx, GetCommand{
		Namespace: seed.Namespace, Key: seed.Key, LeaseID: seed.LeaseID, FencingToken: seed.FencingToken,
	}); err == nil {
		t.Fatal("expected expired seed get to fail")
	}
	rec, _, err := svc.loadTxnRecord(ctx, seed.TxnID)
	if err != nil {
		t.Fatalf("load promoted transaction after expiry cleanup: %v", err)
	}
	if len(rec.Participants) != 1 || rec.Participants[0].Key != participant.Key {
		t.Fatalf("participants after expired seed cleanup=%+v, want only %q", rec.Participants, participant.Key)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: participant.Namespace, Key: participant.Key, LeaseID: participant.LeaseID, FencingToken: participant.FencingToken, TxnID: participant.TxnID,
	}); err != nil {
		t.Fatalf("release surviving participant: %v", err)
	}
	state, err := svc.Get(ctx, GetCommand{Namespace: participant.Namespace, Key: participant.Key, Public: true})
	if err != nil {
		t.Fatalf("get surviving participant state: %v", err)
	}
	if state.NoContent || state.Reader == nil {
		t.Fatal("surviving participant state remained unpublished")
	}
	defer state.Reader.Close()
	var document map[string]string
	if err := json.NewDecoder(state.Reader).Decode(&document); err != nil {
		t.Fatalf("decode surviving participant state: %v", err)
	}
	if document["value"] != "committed by survivor" {
		t.Fatalf("surviving participant document=%v", document)
	}
}

func TestExpiredPromotedImplicitRollbackVotePreventsCommit(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	svc := newTestServiceWithClock(t, clk)

	rollbackVoter, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "promoted-expired-rollback", Owner: "rollback-worker", TTLSeconds: 1, BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire rollback voter: %v", err)
	}
	participant, err := svc.Acquire(ctx, AcquireCommand{
		Namespace: "default", Key: "promoted-rollback-survivor", Owner: "survivor-worker", TTLSeconds: 30, BlockSeconds: apiBlockNoWait, TxnID: rollbackVoter.TxnID,
	})
	if err != nil {
		t.Fatalf("acquire promoted participant: %v", err)
	}
	if _, err := svc.Update(ctx, UpdateCommand{
		Namespace: participant.Namespace, Key: participant.Key, LeaseID: participant.LeaseID, FencingToken: participant.FencingToken, TxnID: participant.TxnID,
		Body: strings.NewReader(`{"value":"must not commit"}`), CompactWriter: func(w io.Writer, r io.Reader, _ int64) error { _, err := io.Copy(w, r); return err },
	}); err != nil {
		t.Fatalf("stage surviving participant state: %v", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: rollbackVoter.Namespace, Key: rollbackVoter.Key, LeaseID: rollbackVoter.LeaseID, FencingToken: rollbackVoter.FencingToken, TxnID: rollbackVoter.TxnID, Rollback: true,
	}); err != nil {
		t.Fatalf("record rollback vote: %v", err)
	}

	clk.Advance(2 * time.Second)
	if _, err := svc.Get(ctx, GetCommand{
		Namespace: rollbackVoter.Namespace, Key: rollbackVoter.Key, LeaseID: rollbackVoter.LeaseID, FencingToken: rollbackVoter.FencingToken,
	}); err == nil {
		t.Fatal("expected expired rollback voter get to fail")
	}
	rec, _, err := svc.loadTxnRecord(ctx, rollbackVoter.TxnID)
	if err != nil {
		t.Fatalf("load promoted transaction after expiry cleanup: %v", err)
	}
	if len(rec.Participants) != 2 {
		t.Fatalf("participants after rollback voter expiry=%+v, want both participants", rec.Participants)
	}
	rollbackParticipant := TxnParticipant{Namespace: rollbackVoter.Namespace, Key: rollbackVoter.Key, BackendHash: svc.backendHash}
	idx := participantIndex(rec.Participants, rollbackParticipant)
	if idx < 0 || !rec.Participants[idx].Prepared || rec.Participants[idx].Outcome != TxnStateRollback {
		t.Fatalf("rollback vote after expiry=%+v, want prepared rollback voter", rec.Participants)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace: participant.Namespace, Key: participant.Key, LeaseID: participant.LeaseID, FencingToken: participant.FencingToken, TxnID: participant.TxnID,
	}); err != nil {
		t.Fatalf("release surviving participant: %v", err)
	}
	state, err := svc.Get(ctx, GetCommand{Namespace: participant.Namespace, Key: participant.Key, Public: true})
	if err != nil {
		t.Fatalf("get surviving participant state: %v", err)
	}
	if state.Reader != nil {
		defer state.Reader.Close()
	}
	if !state.NoContent {
		t.Fatal("surviving participant state was committed despite rollback vote")
	}
}
