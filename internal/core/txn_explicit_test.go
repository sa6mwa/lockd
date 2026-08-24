package core

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/rs/xid"
)

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
