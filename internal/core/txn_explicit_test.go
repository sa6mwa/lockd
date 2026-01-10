package core

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/rs/xid"
	"pkt.systems/lockd/internal/storage"
)

func TestUpdateImplicitTxnSkipsTxnRecord(t *testing.T) {
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

	if _, _, err := svc.loadTxnRecord(ctx, acq.TxnID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no txn record for implicit txn, got %v", err)
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
