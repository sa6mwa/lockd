package core

import (
	"strings"
	"testing"

	indexer "pkt.systems/lockd/internal/search/index"
)

func TestCacheStagedIndexDocEvictsSupersededEntry(t *testing.T) {
	svc := &Service{indexManager: &indexer.Manager{}}
	const (
		namespace = "default"
		key       = "orders"
		txnID     = "txn-1"
	)

	svc.cacheStagedIndexDoc(namespace, key, txnID, "etag-1", 128, indexer.Document{Key: key})
	svc.cacheStagedIndexDoc(namespace, key, txnID, "etag-2", 128, indexer.Document{Key: key})

	if _, ok := svc.stagedIndexDoc(namespace, key, txnID, "etag-1"); ok {
		t.Fatalf("expected superseded staged document entry to be evicted")
	}
	if _, ok := svc.stagedIndexDoc(namespace, key, txnID, "etag-2"); !ok {
		t.Fatalf("expected latest staged document entry to remain")
	}
	if got := countStagedIndexDocEntriesForTxnForTest(svc, namespace, key, txnID); got != 1 {
		t.Fatalf("expected exactly one staged document cache entry, got %d", got)
	}
	headRaw, ok := svc.stagedIndexDocHeads.Load(stagedIndexHeadKey(namespace, key, txnID))
	if !ok {
		t.Fatalf("expected staged head entry")
	}
	head, _ := headRaw.(string)
	if head != "etag-2" {
		t.Fatalf("expected latest staged head etag=etag-2, got %q", head)
	}

	svc.deleteStagedIndexDoc(namespace, key, txnID, "etag-2")
	if got := countStagedIndexDocEntriesForTxnForTest(svc, namespace, key, txnID); got != 0 {
		t.Fatalf("expected staged document cache empty after delete, got %d", got)
	}
	if _, ok := svc.stagedIndexDocHeads.Load(stagedIndexHeadKey(namespace, key, txnID)); ok {
		t.Fatalf("expected staged head entry removed after delete")
	}
}

func countStagedIndexDocEntriesForTxnForTest(s *Service, namespace, key, txnID string) int {
	if s == nil {
		return 0
	}
	prefix := namespace + "\x00" + key + "\x00" + txnID + "\x00"
	count := 0
	s.stagedIndexDocs.Range(func(rawKey, _ any) bool {
		cacheKey, _ := rawKey.(string)
		if strings.HasPrefix(cacheKey, prefix) {
			count++
		}
		return true
	})
	return count
}
