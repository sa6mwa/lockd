package queue

import (
	"context"
	"path"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

func TestReadyCacheInflightSkipsWhileInflight(t *testing.T) {
	cache := newReadyCache(&Service{}, "default", "q")
	now := time.Now().UTC()

	desc := MessageDescriptor{
		Namespace:    "default",
		ID:           "msg-1",
		MetadataKey:  "q/q/msg/msg-1.pb",
		MetadataETag: "etag-1",
		Document: messageDocument{
			ID:              "msg-1",
			Queue:           "q",
			NotVisibleUntil: now.Add(-time.Second),
		},
	}
	cache.upsert(desc)
	if popped := cache.popReady(now); popped == nil || popped.ID != "msg-1" {
		t.Fatalf("expected popReady to return descriptor, got %#v", popped)
	}
	if _, ok := cache.entry("msg-1"); ok {
		t.Fatalf("expected entry removed after popReady")
	}

	desc.Document.NotVisibleUntil = now.Add(-time.Second)
	ready, err := cache.handleDescriptor(context.TODO(), desc, now.Add(500*time.Millisecond))
	if err != nil {
		t.Fatalf("handleDescriptor error: %v", err)
	}
	if ready {
		t.Fatalf("expected inflight descriptor to be skipped")
	}
	if _, ok := cache.entry("msg-1"); ok {
		t.Fatalf("expected entry to remain absent while inflight")
	}

	desc.MetadataETag = "etag-2"
	desc.Document.NotVisibleUntil = now.Add(-time.Second)
	ready, err = cache.handleDescriptor(context.TODO(), desc, now.Add(time.Second))
	if err != nil {
		t.Fatalf("handleDescriptor error on new etag: %v", err)
	}
	if ready {
		t.Fatalf("expected inflight descriptor to remain skipped even with new etag")
	}
	if _, ok := cache.entry("msg-1"); ok {
		t.Fatalf("expected entry to remain absent while inflight")
	}
}

func TestReadyCacheIgnoresLeaseMetaForDelivery(t *testing.T) {
	now := time.Now().UTC()
	store := memory.NewWithConfig(memory.Config{QueueWatch: false})
	svc := &Service{
		store: store,
		clk:   clock.Real{},
		cfg: Config{
			DefaultVisibilityTimeout: 30 * time.Second,
		},
	}
	cache := newReadyCache(svc, "default", "q")

	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "lease-1",
			ExpiresAtUnix: now.Add(30 * time.Second).Unix(),
			FencingToken:  1,
			Owner:         "worker",
		},
	}
	if _, err := store.StoreMeta(context.Background(), "default", path.Join(queueBasePath("q"), "msg", "msg-1"), meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	desc := MessageDescriptor{
		Namespace:    "default",
		ID:           "msg-1",
		MetadataKey:  "q/q/msg/msg-1.pb",
		MetadataETag: "etag-1",
		Document: messageDocument{
			ID:              "msg-1",
			Queue:           "q",
			NotVisibleUntil: now.Add(-time.Second),
		},
	}

	ready, err := cache.handleDescriptor(context.Background(), desc, now)
	if err != nil {
		t.Fatalf("handleDescriptor error: %v", err)
	}
	if ready {
		// ok
	} else {
		t.Fatalf("expected message to be ready despite lease meta")
	}
	if popped := cache.popReady(now); popped == nil || popped.ID != "msg-1" {
		t.Fatalf("expected popReady to return descriptor, got %#v", popped)
	}
}

func TestReadyCacheClearsInflightOnLeaseReset(t *testing.T) {
	svc := &Service{}
	cache := svc.readyCacheForQueue("default", "q")
	now := time.Now().UTC()

	desc := MessageDescriptor{
		Namespace:    "default",
		ID:           "msg-1",
		MetadataKey:  "q/q/msg/msg-1.pb",
		MetadataETag: "etag-1",
		Document: messageDocument{
			ID:              "msg-1",
			Queue:           "q",
			NotVisibleUntil: now.Add(-time.Second),
			LeaseID:         "lease-1",
			LeaseFencingToken: 1,
			LeaseTxnID:      "txn-1",
		},
	}
	cache.upsert(desc)
	if popped := cache.popReady(now); popped == nil || popped.ID != "msg-1" {
		t.Fatalf("expected popReady to return descriptor, got %#v", popped)
	}

	desc.MetadataETag = "etag-2"
	desc.Document.LeaseID = ""
	desc.Document.LeaseFencingToken = 0
	desc.Document.LeaseTxnID = ""
	svc.notifyCacheUpdate("default", "q", desc)

	ready, err := cache.handleDescriptor(context.Background(), desc, now.Add(500*time.Millisecond))
	if err != nil {
		t.Fatalf("handleDescriptor error: %v", err)
	}
	if !ready {
		t.Fatalf("expected descriptor to be accepted after lease reset")
	}
}
