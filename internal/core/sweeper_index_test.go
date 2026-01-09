package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

func newTestServiceWithClock(t testing.TB, clk clock.Clock) *Service {
	t.Helper()
	mem := memory.New()
	return New(Config{
		Store:                mem,
		BackendHash:          "test-backend",
		DefaultNamespace:     "default",
		Clock:                clk,
		TxnDecisionRetention: 0,
	})
}

func TestLoadTxnDecisionMarkerExpiredDeletes(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	svc := newTestServiceWithClock(t, clk)

	rec := &TxnRecord{
		TxnID:         "txn-expired",
		State:         TxnStateCommit,
		ExpiresAtUnix: start.Add(-time.Minute).Unix(),
	}
	if err := svc.writeDecisionMarker(ctx, rec); err != nil {
		t.Fatalf("write decision marker: %v", err)
	}

	markerPrefix := txnDecisionMarkerPrefix + "/"
	entries, err := svc.store.ListObjects(ctx, txnDecisionNamespace, storage.ListOptions{Prefix: markerPrefix})
	if err != nil {
		t.Fatalf("list markers: %v", err)
	}
	if len(entries.Objects) != 1 {
		t.Fatalf("expected 1 marker, got %d", len(entries.Objects))
	}

	bucket := sweepBucketFromUnix(rec.ExpiresAtUnix)
	indexPrefix := txnDecisionIndexKey(bucket, "")
	if !strings.HasSuffix(indexPrefix, "/") {
		indexPrefix += "/"
	}
	entries, err = svc.store.ListObjects(ctx, txnDecisionNamespace, storage.ListOptions{Prefix: indexPrefix})
	if err != nil {
		t.Fatalf("list index entries: %v", err)
	}
	if len(entries.Objects) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(entries.Objects))
	}

	_, _, err = svc.loadTxnDecisionMarker(ctx, rec.TxnID)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected expired marker to be deleted, got %v", err)
	}

	entries, err = svc.store.ListObjects(ctx, txnDecisionNamespace, storage.ListOptions{Prefix: markerPrefix})
	if err != nil {
		t.Fatalf("list markers after delete: %v", err)
	}
	if len(entries.Objects) != 0 {
		t.Fatalf("expected no markers after delete, got %d", len(entries.Objects))
	}

	entries, err = svc.store.ListObjects(ctx, txnDecisionNamespace, storage.ListOptions{Prefix: indexPrefix})
	if err != nil {
		t.Fatalf("list index after delete: %v", err)
	}
	if len(entries.Objects) != 0 {
		t.Fatalf("expected no index entries after delete, got %d", len(entries.Objects))
	}
}

func TestClearExpiredLeaseRemovesIndexEntry(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	svc := newTestServiceWithClock(t, clk)

	namespace := "default"
	key := "lease-key"
	expiresAt := start.Add(10 * time.Minute).Unix()
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "lease-1",
			Owner:         "tester",
			ExpiresAtUnix: expiresAt,
		},
	}
	if _, err := svc.store.StoreMeta(ctx, namespace, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	if err := svc.updateLeaseIndex(ctx, namespace, key, 0, expiresAt); err != nil {
		t.Fatalf("update lease index: %v", err)
	}

	bucket := sweepBucketFromUnix(expiresAt)
	indexPrefix := leaseIndexKey(bucket, "")
	if !strings.HasSuffix(indexPrefix, "/") {
		indexPrefix += "/"
	}
	entries, err := svc.store.ListObjects(ctx, namespace, storage.ListOptions{Prefix: indexPrefix})
	if err != nil {
		t.Fatalf("list lease index entries: %v", err)
	}
	if len(entries.Objects) != 1 {
		t.Fatalf("expected 1 lease index entry, got %d", len(entries.Objects))
	}

	clk.Advance(20 * time.Minute)
	metaLoaded, metaETag, err := svc.ensureMeta(ctx, namespace, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if _, _, err := svc.clearExpiredLease(ctx, namespace, key, metaLoaded, metaETag, clk.Now(), sweepModeTransparent, false); err != nil {
		t.Fatalf("clear expired lease: %v", err)
	}

	entries, err = svc.store.ListObjects(ctx, namespace, storage.ListOptions{Prefix: indexPrefix})
	if err != nil {
		t.Fatalf("list lease index after clear: %v", err)
	}
	if len(entries.Objects) != 0 {
		t.Fatalf("expected no lease index entries after clear, got %d", len(entries.Objects))
	}
}
