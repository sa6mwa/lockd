package core

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

func TestValidateLeaseTreatsEqualExpiryAsExpired(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0).UTC()
	meta := &storage.Meta{
		Version:   3,
		StateETag: "etag-3",
		Lease: &storage.Lease{
			ID:            "lease-1",
			FencingToken:  7,
			TxnID:         "txn-1",
			ExpiresAtUnix: now.Unix(),
		},
	}
	err := validateLease(meta, "lease-1", 7, "txn-1", now)
	if err == nil {
		t.Fatalf("expected lease_expired at expiry boundary")
	}
	assertFailureCode(t, err, "lease_expired")
}

func TestUpdateReturnsLeaseExpiredAtExpiryBoundary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	manual := clock.NewManual(time.Unix(1_700_000_000, 0).UTC())
	svc := New(Config{
		Store:            memory.New(),
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
		Clock:            manual,
	})

	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          "expiry-boundary-update",
		Owner:        "worker",
		TTLSeconds:   1,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	manual.Advance(time.Second)

	res, err := svc.Update(ctx, UpdateCommand{
		Namespace:    "default",
		Key:          "expiry-boundary-update",
		LeaseID:      acq.LeaseID,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
		Body:         strings.NewReader(`{"v":1}`),
		CompactWriter: func(w io.Writer, r io.Reader, _ int64) error {
			_, copyErr := io.Copy(w, r)
			return copyErr
		},
	})
	if err == nil {
		t.Fatalf("expected update failure when lease expires at boundary; got result %+v", res)
	}
	if res != nil {
		t.Fatalf("expected nil update result on lease_expired, got %+v", res)
	}
	assertFailureCode(t, err, "lease_expired")
}
