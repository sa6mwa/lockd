package queue

import (
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage/memory"
)

func TestIsQueueStateKey(t *testing.T) {
	cases := []struct {
		key     string
		isState bool
	}{
		{"default/q/orders/state/msg-123", true},
		{"default/q/orders/msg/msg-123", false},
		{"default/q//state/msg-123", false},
		{"state/msg-123", false},
		{"default/q/orders/state", false},
	}
	for _, tc := range cases {
		if got := IsQueueStateKey(tc.key); got != tc.isState {
			t.Fatalf("IsQueueStateKey(%q)=%v want %v", tc.key, got, tc.isState)
		}
	}
}

func TestUpdateReadyCacheSeedsDescriptor(t *testing.T) {
	store := memory.NewWithConfig(memory.Config{QueueWatch: false})
	svc, err := New(store, clock.Real{}, Config{})
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	doc := MessageDocument{
		Queue:           "jobs",
		ID:              "msg-1",
		NotVisibleUntil: time.Now().Add(30 * time.Second).UTC(),
	}
	svc.UpdateReadyCache("default", "jobs", &doc, "etag-1")
	cache := svc.readyCacheForQueue("default", "jobs")
	entry, ok := cache.entry("msg-1")
	if !ok {
		t.Fatalf("expected cache entry")
	}
	if entry.descriptor.MetadataETag != "etag-1" {
		t.Fatalf("expected etag %q, got %q", "etag-1", entry.descriptor.MetadataETag)
	}
	if !entry.due.Equal(doc.NotVisibleUntil) {
		t.Fatalf("expected due %s, got %s", doc.NotVisibleUntil, entry.due)
	}
}
