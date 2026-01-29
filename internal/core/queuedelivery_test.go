package core

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

func TestAcquireLeaseForQueueKeyBlocksActiveLease(t *testing.T) {
	ctx := context.Background()
	coreSvc, _ := newQueueCoreForTest(t)

	messageKey, err := queue.MessageLeaseKey("default", "q1", "msg-1")
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	_, err = coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", messageKey),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	_, err = coreSvc.acquireLeaseForKey(ctx, acquireParams{
		Namespace: "default",
		Key:       messageKey,
		Owner:     "worker2",
		TTL:       30 * time.Second,
	})
	if err == nil {
		t.Fatalf("expected waiting failure")
	}
	failure, ok := err.(Failure)
	if !ok {
		t.Fatalf("expected Failure, got %T", err)
	}
	if failure.Code != "waiting" {
		t.Fatalf("expected waiting code, got %q", failure.Code)
	}
	if failure.RetryAfter <= 0 {
		t.Fatalf("expected retry_after > 0, got %d", failure.RetryAfter)
	}
}

func TestAcquireLeaseForQueueKeyAllowsExpiredLease(t *testing.T) {
	ctx := context.Background()
	coreSvc, _ := newQueueCoreForTest(t)

	messageKey, err := queue.MessageLeaseKey("default", "q1", "msg-2")
	if err != nil {
		t.Fatalf("message lease key: %v", err)
	}
	relKey := relativeKey("default", messageKey)
	now := time.Now().Unix()
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "old",
			Owner:         "worker",
			ExpiresAtUnix: now - 10,
		},
		UpdatedAtUnix: now - 10,
	}
	if _, err := coreSvc.store.StoreMeta(ctx, "default", relKey, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	outcome, err := coreSvc.acquireLeaseForKey(ctx, acquireParams{
		Namespace: "default",
		Key:       messageKey,
		Owner:     "worker2",
		TTL:       30 * time.Second,
	})
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if outcome == nil || outcome.Response.LeaseID == "" {
		t.Fatalf("expected new lease")
	}
}
