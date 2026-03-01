package queue

import (
	"bytes"
	"context"
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

func TestPeekCandidateDoesNotConsume(t *testing.T) {
	store := memory.NewWithConfig(memory.Config{QueueWatch: false})
	svc, err := New(store, clock.Real{}, Config{})
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	ctx := context.Background()
	msg, err := svc.Enqueue(ctx, "default", "jobs", bytes.NewReader([]byte("payload")), EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	peek1, err := svc.PeekCandidate(ctx, "default", "jobs", "", 1)
	if err != nil {
		t.Fatalf("peek candidate: %v", err)
	}
	if peek1.Descriptor == nil || peek1.Descriptor.Document.ID != msg.ID {
		t.Fatalf("unexpected first peek result: %+v", peek1.Descriptor)
	}

	peek2, err := svc.PeekCandidate(ctx, "default", "jobs", "", 1)
	if err != nil {
		t.Fatalf("peek candidate second call: %v", err)
	}
	if peek2.Descriptor == nil || peek2.Descriptor.Document.ID != msg.ID {
		t.Fatalf("unexpected second peek result: %+v", peek2.Descriptor)
	}

	next, err := svc.NextCandidate(ctx, "default", "jobs", "", 1)
	if err != nil {
		t.Fatalf("next candidate after peek: %v", err)
	}
	if next.Descriptor == nil || next.Descriptor.Document.ID != msg.ID {
		t.Fatalf("expected next candidate %q, got %+v", msg.ID, next.Descriptor)
	}
}
