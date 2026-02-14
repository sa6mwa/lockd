package queue

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	lockdproto "pkt.systems/lockd/internal/proto"
)

func TestMarshalMessageDocumentWritesZeroFailureAttempts(t *testing.T) {
	doc := &messageDocument{
		Type:               "queue.message",
		Queue:              "jobs",
		ID:                 "m1",
		Attempts:           4,
		FailureAttempts:    0,
		MaxAttempts:        1,
		EnqueuedAt:         time.Unix(1700000000, 0).UTC(),
		UpdatedAt:          time.Unix(1700000001, 0).UTC(),
		NotVisibleUntil:    time.Unix(1700000002, 0).UTC(),
		VisibilityTimeout:  30,
		PayloadBytes:       12,
		PayloadContentType: "application/json",
	}

	raw, err := marshalMessageDocument(doc)
	if err != nil {
		t.Fatalf("marshalMessageDocument: %v", err)
	}

	var stored lockdproto.QueueMessageMeta
	if err := proto.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if stored.FailureAttempts == nil {
		t.Fatalf("expected failure_attempts field to be present")
	}
	if got := int(stored.GetFailureAttempts()); got != 0 {
		t.Fatalf("expected failure_attempts=0, got %d", got)
	}

	roundTrip, err := unmarshalMessageDocument(raw)
	if err != nil {
		t.Fatalf("unmarshalMessageDocument: %v", err)
	}
	if got := roundTrip.FailureAttempts; got != 0 {
		t.Fatalf("expected failure_attempts to remain 0, got %d", got)
	}
}

func TestUnmarshalMessageDocumentBackfillsFailureAttemptsForLegacyMeta(t *testing.T) {
	legacy := &lockdproto.QueueMessageMeta{
		Type:                "queue.message",
		Queue:               "jobs",
		Id:                  "m2",
		Attempts:            3,
		MaxAttempts:         5,
		EnqueuedAtUnix:      time.Unix(1700000010, 0).UTC().Unix(),
		UpdatedAtUnix:       time.Unix(1700000011, 0).UTC().Unix(),
		NotVisibleUntilUnix: time.Unix(1700000012, 0).UTC().Unix(),
	}

	raw, err := proto.Marshal(legacy)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	doc, err := unmarshalMessageDocument(raw)
	if err != nil {
		t.Fatalf("unmarshalMessageDocument: %v", err)
	}
	if got := doc.FailureAttempts; got != doc.Attempts {
		t.Fatalf("expected failure_attempts to backfill from attempts=%d, got %d", doc.Attempts, got)
	}
}
