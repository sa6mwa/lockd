package core

import "testing"

import "pkt.systems/lockd/internal/queue"

func TestValidateQueueMessageLeaseIfPresent(t *testing.T) {
	t.Run("nil_doc", func(t *testing.T) {
		if err := validateQueueMessageLeaseIfPresent(nil, "lease", 1, "txn"); err == nil {
			t.Fatal("expected error for nil doc")
		}
	})

	t.Run("empty_doc_skips", func(t *testing.T) {
		doc := &queue.MessageDocument{}
		if err := validateQueueMessageLeaseIfPresent(doc, "lease", 1, "txn"); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("partial_doc_errors", func(t *testing.T) {
		doc := &queue.MessageDocument{LeaseID: "lease"}
		if err := validateQueueMessageLeaseIfPresent(doc, "lease", 1, "txn"); err == nil {
			t.Fatal("expected error for missing fencing token")
		}
	})

	t.Run("match", func(t *testing.T) {
		doc := &queue.MessageDocument{LeaseID: "lease", LeaseFencingToken: 9, LeaseTxnID: "txn"}
		if err := validateQueueMessageLeaseIfPresent(doc, "lease", 9, "txn"); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		doc := &queue.MessageDocument{LeaseID: "lease", LeaseFencingToken: 9, LeaseTxnID: "txn"}
		if err := validateQueueMessageLeaseIfPresent(doc, "lease", 8, "txn"); err == nil {
			t.Fatal("expected error for fencing mismatch")
		}
	})
}
