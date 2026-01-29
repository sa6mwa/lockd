package disk

import (
	"testing"

	"pkt.systems/lockd/internal/storage"
)

func TestApplyPendingRespectsCommitOrder(t *testing.T) {
	ns := &logNamespace{
		metaIndex:   make(map[string]*recordRef),
		stateIndex:  make(map[string]*recordRef),
		objectIndex: make(map[string]*recordRef),
	}
	seg := &logSegment{name: "seg-1"}

	groupA := storage.NewCommitGroup()
	groupB := storage.NewCommitGroup()

	refA1 := &recordRef{
		recType:       logRecordMetaPut,
		key:           "a",
		meta:          recordMeta{gen: 1},
		segment:       seg,
		payloadOffset: 10,
		payloadLen:    2,
	}
	refB := &recordRef{
		recType:       logRecordMetaPut,
		key:           "b",
		meta:          recordMeta{gen: 1},
		segment:       seg,
		payloadOffset: 20,
		payloadLen:    2,
	}
	refA2 := &recordRef{
		recType:       logRecordMetaPut,
		key:           "c",
		meta:          recordMeta{gen: 1},
		segment:       seg,
		payloadOffset: 30,
		payloadLen:    2,
	}
	seg.size = recordEndOffset(refA2)

	ns.queuePendingLocked(refA1, groupA)
	ns.queuePendingLocked(refB, groupB)
	ns.queuePendingLocked(refA2, groupA)

	ns.applyPendingLocked()
	if len(ns.metaIndex) != 0 {
		t.Fatalf("expected no applied records before commit, got %d", len(ns.metaIndex))
	}

	ns.markCommittedLocked(groupA)
	ns.applyPendingLocked()
	if _, ok := ns.metaIndex["a"]; !ok {
		t.Fatalf("expected group A head record applied")
	}
	if _, ok := ns.metaIndex["b"]; ok {
		t.Fatalf("expected group B record to remain pending")
	}
	if _, ok := ns.metaIndex["c"]; !ok {
		t.Fatalf("expected trailing group A record applied")
	}
	if got := seg.readOffset; got != recordEndOffset(refA1) {
		t.Fatalf("expected readOffset %d, got %d", recordEndOffset(refA1), got)
	}

	ns.markCommittedLocked(groupB)
	ns.applyPendingLocked()
	if _, ok := ns.metaIndex["b"]; !ok {
		t.Fatalf("expected group B record applied after commit")
	}
	if _, ok := ns.metaIndex["c"]; !ok {
		t.Fatalf("expected trailing group A record applied after group B commit")
	}
	if got := seg.readOffset; got != recordEndOffset(refA2) {
		t.Fatalf("expected readOffset %d, got %d", recordEndOffset(refA2), got)
	}
	if len(ns.pending) != 0 {
		t.Fatalf("expected pending queue drained, got %d", len(ns.pending))
	}
	if len(ns.pendingCounts) != 0 {
		t.Fatalf("expected pendingCounts cleared, got %d", len(ns.pendingCounts))
	}
	if len(ns.committed) != 0 {
		t.Fatalf("expected committed cleared, got %d", len(ns.committed))
	}
}

func TestPendingMapsTrackLatestAndClear(t *testing.T) {
	ns := &logNamespace{
		metaIndex:   make(map[string]*recordRef),
		stateIndex:  make(map[string]*recordRef),
		objectIndex: make(map[string]*recordRef),
	}
	seg := &logSegment{name: "seg-1"}
	group := storage.NewCommitGroup()

	refMeta1 := &recordRef{
		recType:       logRecordMetaPut,
		key:           "meta-key",
		meta:          recordMeta{gen: 1, etag: "m1"},
		segment:       seg,
		payloadOffset: 10,
		payloadLen:    2,
	}
	refMeta2 := &recordRef{
		recType:       logRecordMetaPut,
		key:           "meta-key",
		meta:          recordMeta{gen: 2, etag: "m2"},
		segment:       seg,
		payloadOffset: 20,
		payloadLen:    2,
	}
	refState := &recordRef{
		recType:       logRecordStatePut,
		key:           "state-key",
		meta:          recordMeta{gen: 1, etag: "s1"},
		segment:       seg,
		payloadOffset: 30,
		payloadLen:    2,
	}
	refObj := &recordRef{
		recType:       logRecordObjectPut,
		key:           "object-key",
		meta:          recordMeta{gen: 1, etag: "o1"},
		segment:       seg,
		payloadOffset: 40,
		payloadLen:    2,
	}
	seg.size = recordEndOffset(refObj)

	ns.mu.Lock()
	ns.queuePendingLocked(refMeta1, group)
	ns.queuePendingLocked(refState, group)
	ns.queuePendingLocked(refObj, group)
	ns.queuePendingLocked(refMeta2, group)
	if ns.pendingMeta["meta-key"] == nil || ns.pendingMeta["meta-key"].ref != refMeta2 {
		ns.mu.Unlock()
		t.Fatalf("expected latest meta pending ref")
	}
	if ns.pendingState["state-key"] == nil || ns.pendingState["state-key"].ref != refState {
		ns.mu.Unlock()
		t.Fatalf("expected state pending ref")
	}
	if ns.pendingObject["object-key"] == nil || ns.pendingObject["object-key"].ref != refObj {
		ns.mu.Unlock()
		t.Fatalf("expected object pending ref")
	}

	ns.markCommittedLocked(group)
	ns.applyPendingLocked()
	if len(ns.pendingMeta) != 0 || len(ns.pendingState) != 0 || len(ns.pendingObject) != 0 {
		ns.mu.Unlock()
		t.Fatalf("expected pending maps cleared after commit")
	}
	ns.mu.Unlock()
}
