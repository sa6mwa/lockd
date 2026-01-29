package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

func TestApplyTxnDecisionQueueMessageCommit(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-commit"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.TxnID == "" {
		t.Fatalf("expected txn id")
	}
	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}
	if err := qsvc.RefreshReadyCache(ctx, "default", queueName); err != nil {
		t.Fatalf("refresh cache: %v", err)
	}
	if _, err := qsvc.NextCandidate(ctx, "default", queueName, "", 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no candidate before decision, got %v", err)
	}

	if err := coreSvc.applyTxnDecisionQueueMessage(ctx, "default", queueName, msg.ID, acq.TxnID, true); err != nil {
		t.Fatalf("apply commit: %v", err)
	}
	if _, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected message deleted, got %v", err)
	}
	if err := qsvc.RefreshReadyCache(ctx, "default", queueName); err != nil {
		t.Fatalf("refresh cache after commit: %v", err)
	}
	if _, err := qsvc.NextCandidate(ctx, "default", queueName, "", 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no candidate after commit, got %v", err)
	}

	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	metaRes, err := coreSvc.store.LoadMeta(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta := metaRes.Meta
	if meta.Lease != nil {
		t.Fatalf("expected lease cleared after commit")
	}
}

func TestApplyTxnDecisionQueueMessageRollback(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-rollback"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.TxnID == "" {
		t.Fatalf("expected txn id")
	}
	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}
	if err := qsvc.RefreshReadyCache(ctx, "default", queueName); err != nil {
		t.Fatalf("refresh cache: %v", err)
	}
	if _, err := qsvc.NextCandidate(ctx, "default", queueName, "", 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no candidate before decision, got %v", err)
	}

	if err := coreSvc.applyTxnDecisionQueueMessage(ctx, "default", queueName, msg.ID, acq.TxnID, false); err != nil {
		t.Fatalf("apply rollback: %v", err)
	}
	if err := qsvc.RefreshReadyCache(ctx, "default", queueName); err != nil {
		t.Fatalf("refresh cache after rollback: %v", err)
	}
	candidateRes, err := qsvc.NextCandidate(ctx, "default", queueName, "", 1)
	if err != nil {
		t.Fatalf("expected candidate after rollback, got %v", err)
	}
	desc := candidateRes.Descriptor
	if desc.ID != msg.ID {
		t.Fatalf("expected message %s after rollback, got %s", msg.ID, desc.ID)
	}

	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	metaRes, err := coreSvc.store.LoadMeta(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta := metaRes.Meta
	if meta.Lease != nil {
		t.Fatalf("expected lease cleared after rollback")
	}
}

func TestMaybeApplyDecisionMarkerForLeaseQueueMessage(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-marker-rollback"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        xid.New().String(),
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.TxnID == "" {
		t.Fatalf("expected txn id")
	}
	if _, err := qsvc.EnsureStateExists(ctx, "default", queueName, msg.ID); err != nil {
		t.Fatalf("ensure state exists: %v", err)
	}
	stateLease, err := queue.StateLeaseKey("default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("state lease key: %v", err)
	}
	if _, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", stateLease),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        acq.TxnID,
	}); err != nil {
		t.Fatalf("acquire state lease: %v", err)
	}

	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}

	now := time.Now().Unix()
	marker := txnDecisionMarker{
		TxnID:         acq.TxnID,
		State:         TxnStateRollback,
		ExpiresAtUnix: now + 3600,
		UpdatedAtUnix: now,
	}
	if _, err := coreSvc.putTxnDecisionMarker(ctx, &marker, ""); err != nil {
		t.Fatalf("put decision marker: %v", err)
	}

	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	applied, err := coreSvc.maybeApplyDecisionMarkerForLease(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("apply decision marker: %v", err)
	}
	if !applied {
		t.Fatalf("expected decision marker to apply")
	}

	metaRes, err := coreSvc.store.LoadMeta(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if metaRes.Meta.Lease != nil {
		t.Fatalf("expected lease cleared after marker apply")
	}
	docRes, err = qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message after marker: %v", err)
	}
	if docRes.Document.LeaseTxnID != "" {
		t.Fatalf("expected message lease cleared after marker apply")
	}
	if _, err := qsvc.LoadState(ctx, "default", queueName, msg.ID); err != nil {
		t.Fatalf("expected state retained after rollback marker, got %v", err)
	}
	stateMetaKey := relativeKey("default", stateLease)
	stateMetaRes, err := coreSvc.store.LoadMeta(ctx, "default", stateMetaKey)
	if err != nil {
		t.Fatalf("load state meta: %v", err)
	}
	if stateMetaRes.Meta != nil && stateMetaRes.Meta.Lease != nil {
		t.Fatalf("expected state lease cleared after marker apply")
	}
	if err := qsvc.RefreshReadyCache(ctx, "default", queueName); err != nil {
		t.Fatalf("refresh cache after marker: %v", err)
	}
	cand, err := qsvc.NextCandidate(ctx, "default", queueName, "", 1)
	if err != nil {
		t.Fatalf("next candidate after marker: %v", err)
	}
	if cand.Descriptor == nil || cand.Descriptor.ID != msg.ID {
		t.Fatalf("expected message ready after marker apply")
	}
}

func TestMaybeApplyDecisionMarkerForLeaseQueueMessageCommit(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-marker-commit"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        xid.New().String(),
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.TxnID == "" {
		t.Fatalf("expected txn id")
	}
	if _, err := qsvc.EnsureStateExists(ctx, "default", queueName, msg.ID); err != nil {
		t.Fatalf("ensure state exists: %v", err)
	}
	stateLease, err := queue.StateLeaseKey("default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("state lease key: %v", err)
	}
	if _, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", stateLease),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        acq.TxnID,
	}); err != nil {
		t.Fatalf("acquire state lease: %v", err)
	}

	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}

	now := time.Now().Unix()
	marker := txnDecisionMarker{
		TxnID:         acq.TxnID,
		State:         TxnStateCommit,
		ExpiresAtUnix: now + 3600,
		UpdatedAtUnix: now,
	}
	if _, err := coreSvc.putTxnDecisionMarker(ctx, &marker, ""); err != nil {
		t.Fatalf("put decision marker: %v", err)
	}

	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	applied, err := coreSvc.maybeApplyDecisionMarkerForLease(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("apply decision marker: %v", err)
	}
	if !applied {
		t.Fatalf("expected decision marker to apply")
	}

	metaRes, err := coreSvc.store.LoadMeta(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if metaRes.Meta.Lease != nil {
		t.Fatalf("expected lease cleared after marker apply")
	}
	if _, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected message deleted after commit marker, got %v", err)
	}
	if _, err := qsvc.LoadState(ctx, "default", queueName, msg.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected state deleted after commit marker, got %v", err)
	}
	stateMetaKey := relativeKey("default", stateLease)
	stateMetaRes, err := coreSvc.store.LoadMeta(ctx, "default", stateMetaKey)
	if err != nil {
		t.Fatalf("load state meta: %v", err)
	}
	if stateMetaRes.Meta != nil && stateMetaRes.Meta.Lease != nil {
		t.Fatalf("expected state lease cleared after marker apply")
	}
}

func TestQueueDecisionWorklistRollback(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-worklist"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}
	if err := qsvc.RefreshReadyCache(ctx, "default", queueName); err != nil {
		t.Fatalf("refresh cache: %v", err)
	}
	if _, err := qsvc.NextCandidate(ctx, "default", queueName, "", 1); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no candidate before decision, got %v", err)
	}

	rec := &TxnRecord{
		TxnID: acq.TxnID,
		State: TxnStateRollback,
		Participants: []TxnParticipant{
			{Namespace: "default", Key: relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))},
		},
	}
	coreSvc.recordQueueDecisionWorklist(ctx, rec)

	applied, err := coreSvc.maybeApplyQueueDecisionWorklist(ctx, "default", queueName)
	if err != nil {
		t.Fatalf("apply worklist: %v", err)
	}
	if applied == 0 {
		t.Fatalf("expected worklist apply")
	}
	if err := qsvc.RefreshReadyCache(ctx, "default", queueName); err != nil {
		t.Fatalf("refresh cache after apply: %v", err)
	}
	cand, err := qsvc.NextCandidate(ctx, "default", queueName, "", 1)
	if err != nil {
		t.Fatalf("expected message visible after rollback, got %v", err)
	}
	if cand.Descriptor == nil || cand.Descriptor.ID != msg.ID {
		t.Fatalf("expected message visible after rollback, got %+v", cand.Descriptor)
	}
}

func TestApplyTxnDecisionQueueMessageRollbackMissingMeta(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-rollback-missing-meta"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        xid.New().String(),
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}

	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	if err := coreSvc.store.DeleteMeta(ctx, "default", metaKey, ""); err != nil {
		t.Fatalf("delete meta: %v", err)
	}

	if err := coreSvc.applyTxnDecisionQueueMessage(ctx, "default", queueName, msg.ID, acq.TxnID, false); err != nil {
		t.Fatalf("apply rollback with missing meta: %v", err)
	}
	docRes, err = qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("reload message: %v", err)
	}
	doc = docRes.Document
	if doc.LeaseID != "" || doc.LeaseTxnID != "" || doc.LeaseFencingToken != 0 {
		t.Fatalf("expected lease cleared from message doc after rollback")
	}
	if doc.NotVisibleUntil.After(time.Now().Add(1 * time.Second)) {
		t.Fatalf("expected message visible after rollback, got not_visible_until=%s", doc.NotVisibleUntil)
	}
}

func TestApplyTxnDecisionQueueMessageRollbackMissingLeaseInfo(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-rollback-missing-lease"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        xid.New().String(),
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	docETag, err = qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second)
	if err != nil {
		t.Fatalf("increment attempts: %v", err)
	}

	doc.LeaseID = ""
	doc.LeaseFencingToken = 0
	doc.LeaseTxnID = ""
	if _, err := qsvc.SaveMessageDocument(ctx, "default", queueName, doc.ID, doc, docETag); err != nil {
		t.Fatalf("clear lease fields: %v", err)
	}

	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	if err := coreSvc.store.DeleteMeta(ctx, "default", metaKey, ""); err != nil {
		t.Fatalf("delete meta: %v", err)
	}

	if err := coreSvc.applyTxnDecisionQueueMessage(ctx, "default", queueName, msg.ID, acq.TxnID, false); err != nil {
		t.Fatalf("apply rollback with missing lease: %v", err)
	}
	docRes, err = qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("reload message: %v", err)
	}
	doc = docRes.Document
	if doc.NotVisibleUntil.After(time.Now().Add(1 * time.Second)) {
		t.Fatalf("expected message visible after rollback, got not_visible_until=%s", doc.NotVisibleUntil)
	}
}

func TestApplyTxnDecisionQueueMessageRejectsOtherTxn(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-skip"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.TxnID == "" {
		t.Fatalf("expected txn id")
	}

	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}

	err = coreSvc.applyTxnDecisionQueueMessage(ctx, "default", queueName, msg.ID, acq.TxnID+"-other", true)
	if err == nil {
		t.Fatalf("expected lease mismatch error")
	}
	fail := Failure{}
	if !errors.As(err, &fail) || fail.Code != queueMessageLeaseMismatchCode {
		t.Fatalf("expected %s, got %v", queueMessageLeaseMismatchCode, err)
	}

	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	metaRes, err := coreSvc.store.LoadMeta(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta := metaRes.Meta
	if meta.Lease == nil || meta.Lease.TxnID != acq.TxnID {
		t.Fatalf("expected lease txn %s to remain, got %+v", acq.TxnID, meta.Lease)
	}
	if _, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID); err != nil {
		t.Fatalf("expected message to remain, got %v", err)
	}
}

func TestApplyTxnDecisionQueueMessageReplayToleratesLeaseMismatch(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-replay-tolerate"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.SaveMessageDocument(ctx, "default", queueName, doc.ID, doc, docETag); err != nil {
		t.Fatalf("save message: %v", err)
	}

	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	metaRes, err := coreSvc.store.LoadMeta(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	newLease := &storage.Lease{
		ID:            "lease-2",
		Owner:         "worker-2",
		ExpiresAtUnix: time.Now().Add(1 * time.Minute).Unix(),
		FencingToken:  acq.FencingToken + 1,
		TxnID:         "txn-2",
	}
	meta := metaRes.Meta
	meta.Lease = newLease
	if _, err := coreSvc.store.StoreMeta(ctx, "default", metaKey, meta, metaRes.ETag); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	docRes, err = qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message for new lease: %v", err)
	}
	doc = docRes.Document
	docETag = docRes.ETag
	doc.LeaseID = newLease.ID
	doc.LeaseFencingToken = newLease.FencingToken
	doc.LeaseTxnID = newLease.TxnID
	doc.UpdatedAt = time.Now()
	if _, err := qsvc.SaveMessageDocument(ctx, "default", queueName, doc.ID, doc, docETag); err != nil {
		t.Fatalf("save message new lease: %v", err)
	}

	rec := &TxnRecord{
		TxnID: acq.TxnID,
		State: TxnStateRollback,
		Participants: []TxnParticipant{
			{Namespace: "default", Key: metaKey},
		},
	}
	if err := coreSvc.applyTxnDecisionWithOptions(ctx, rec, txnApplyOptions{tolerateQueueLeaseMismatch: true}); err != nil {
		t.Fatalf("apply decision: %v", err)
	}

	metaRes, err = coreSvc.store.LoadMeta(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("reload meta: %v", err)
	}
	if metaRes.Meta.Lease == nil || metaRes.Meta.Lease.TxnID != newLease.TxnID {
		t.Fatalf("expected lease to remain on new txn, got %+v", metaRes.Meta.Lease)
	}
}

func TestApplyTxnDecisionQueueMessageCommitStateful(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-stateful-commit"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	txnID := acq.TxnID
	if txnID == "" {
		t.Fatalf("expected txn id")
	}
	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}
	if _, err := qsvc.EnsureStateExists(ctx, "default", queueName, msg.ID); err != nil {
		t.Fatalf("ensure state: %v", err)
	}
	stateKey, err := queue.StateLeaseKey("default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("state lease key: %v", err)
	}
	stateAcq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", stateKey),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        txnID,
	})
	if err != nil {
		t.Fatalf("acquire state: %v", err)
	}
	if stateAcq.TxnID != txnID {
		t.Fatalf("expected state txn %s, got %s", txnID, stateAcq.TxnID)
	}

	rec := &TxnRecord{
		TxnID: txnID,
		State: TxnStateCommit,
		Participants: []TxnParticipant{
			{Namespace: "default", Key: relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))},
			{Namespace: "default", Key: relativeKey("default", stateKey)},
		},
	}
	if err := coreSvc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply commit: %v", err)
	}

	if _, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected message deleted, got %v", err)
	}
	if _, err := qsvc.LoadState(ctx, "default", queueName, msg.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected state deleted, got %v", err)
	}
	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	metaRes, err := coreSvc.store.LoadMeta(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("load message meta: %v", err)
	}
	meta := metaRes.Meta
	if meta.Lease != nil {
		t.Fatalf("expected message lease cleared")
	}
	stateRel := relativeKey("default", stateKey)
	stateMetaRes, err := coreSvc.store.LoadMeta(ctx, "default", stateRel)
	if err != nil {
		t.Fatalf("load state meta: %v", err)
	}
	stateMeta := stateMetaRes.Meta
	if stateMeta.Lease != nil {
		t.Fatalf("expected state lease cleared")
	}
}

func TestApplyTxnDecisionQueueMessageRollbackStateful(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	queueName := "txn-stateful-rollback"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	txnID := acq.TxnID
	if txnID == "" {
		t.Fatalf("expected txn id")
	}
	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}
	if _, err := qsvc.EnsureStateExists(ctx, "default", queueName, msg.ID); err != nil {
		t.Fatalf("ensure state: %v", err)
	}
	stateKey, err := queue.StateLeaseKey("default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("state lease key: %v", err)
	}
	stateAcq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", stateKey),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        txnID,
	})
	if err != nil {
		t.Fatalf("acquire state: %v", err)
	}
	if stateAcq.TxnID != txnID {
		t.Fatalf("expected state txn %s, got %s", txnID, stateAcq.TxnID)
	}

	rec := &TxnRecord{
		TxnID: txnID,
		State: TxnStateRollback,
		Participants: []TxnParticipant{
			{Namespace: "default", Key: relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))},
			{Namespace: "default", Key: relativeKey("default", stateKey)},
		},
	}
	if err := coreSvc.applyTxnDecision(ctx, rec); err != nil {
		t.Fatalf("apply rollback: %v", err)
	}

	if _, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID); err != nil {
		t.Fatalf("expected message to remain, got %v", err)
	}
	if _, err := qsvc.LoadState(ctx, "default", queueName, msg.ID); err != nil {
		t.Fatalf("expected state to remain, got %v", err)
	}
	if err := qsvc.RefreshReadyCache(ctx, "default", queueName); err != nil {
		t.Fatalf("refresh cache: %v", err)
	}
	candidateRes, err := qsvc.NextCandidate(ctx, "default", queueName, "", 1)
	if err != nil {
		t.Fatalf("expected candidate after rollback, got %v", err)
	}
	desc := candidateRes.Descriptor
	if desc.ID != msg.ID {
		t.Fatalf("expected message %s after rollback, got %s", msg.ID, desc.ID)
	}

	metaKey := relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID))
	metaRes, err := coreSvc.store.LoadMeta(ctx, "default", metaKey)
	if err != nil {
		t.Fatalf("load message meta: %v", err)
	}
	meta := metaRes.Meta
	if meta.Lease != nil {
		t.Fatalf("expected message lease cleared")
	}
	stateRel := relativeKey("default", stateKey)
	stateMetaRes, err := coreSvc.store.LoadMeta(ctx, "default", stateRel)
	if err != nil {
		t.Fatalf("load state meta: %v", err)
	}
	stateMeta := stateMetaRes.Meta
	if stateMeta.Lease != nil {
		t.Fatalf("expected state lease cleared")
	}
}

type queueDispatcherSpy struct {
	mu    sync.Mutex
	calls []queueDispatchCall
}

type queueDispatchCall struct {
	namespace string
	queue     string
}

func (s *queueDispatcherSpy) Notify(namespace, queue string) {
	s.mu.Lock()
	s.calls = append(s.calls, queueDispatchCall{namespace: namespace, queue: queue})
	s.mu.Unlock()
}

func (s *queueDispatcherSpy) Try(ctx context.Context, namespace, queue string) (*queue.Candidate, error) {
	return nil, nil
}

func (s *queueDispatcherSpy) Wait(ctx context.Context, namespace, queue string) (*queue.Candidate, error) {
	return nil, nil
}

func (s *queueDispatcherSpy) HasActiveWatcher(namespace, queue string) bool {
	return false
}

func (s *queueDispatcherSpy) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *queueDispatcherSpy) Last() (queueDispatchCall, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		return queueDispatchCall{}, false
	}
	return s.calls[len(s.calls)-1], true
}

func TestApplyTxnDecisionQueueMessageCommitNotifiesDispatcher(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)
	spy := &queueDispatcherSpy{}
	coreSvc.queueDispatcher = spy

	queueName := "txn-commit-notify"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.TxnID == "" {
		t.Fatalf("expected txn id")
	}
	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}

	if err := coreSvc.applyTxnDecisionQueueMessage(ctx, "default", queueName, msg.ID, acq.TxnID, true); err != nil {
		t.Fatalf("apply commit: %v", err)
	}
	if spy.Count() != 1 {
		t.Fatalf("expected dispatcher notify once, got %d", spy.Count())
	}
	if last, ok := spy.Last(); !ok || last.namespace != "default" || last.queue != queueName {
		t.Fatalf("unexpected dispatcher notify %+v", last)
	}
}

func TestApplyTxnDecisionQueueMessageRollbackNotifiesDispatcher(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)
	spy := &queueDispatcherSpy{}
	coreSvc.queueDispatcher = spy

	queueName := "txn-rollback-notify"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.TxnID == "" {
		t.Fatalf("expected txn id")
	}
	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}

	if err := coreSvc.applyTxnDecisionQueueMessage(ctx, "default", queueName, msg.ID, acq.TxnID, false); err != nil {
		t.Fatalf("apply rollback: %v", err)
	}
	if spy.Count() != 1 {
		t.Fatalf("expected dispatcher notify once, got %d", spy.Count())
	}
	if last, ok := spy.Last(); !ok || last.namespace != "default" || last.queue != queueName {
		t.Fatalf("unexpected dispatcher notify %+v", last)
	}
}

func TestApplyTxnDecisionQueueMessageRejectsOtherTxnDoesNotNotify(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)
	spy := &queueDispatcherSpy{}
	coreSvc.queueDispatcher = spy

	queueName := "txn-skip-notify"
	msg, err := qsvc.Enqueue(ctx, "default", queueName, strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", queueName, msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.TxnID == "" {
		t.Fatalf("expected txn id")
	}

	docRes, err := qsvc.GetMessage(ctx, "default", queueName, msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	docETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	if _, err := qsvc.IncrementAttempts(ctx, "default", queueName, doc, docETag, 30*time.Second); err != nil {
		t.Fatalf("increment attempts: %v", err)
	}

	err = coreSvc.applyTxnDecisionQueueMessage(ctx, "default", queueName, msg.ID, acq.TxnID+"-other", true)
	if err == nil {
		t.Fatalf("expected lease mismatch error")
	}
	fail := Failure{}
	if !errors.As(err, &fail) || fail.Code != queueMessageLeaseMismatchCode {
		t.Fatalf("expected %s, got %v", queueMessageLeaseMismatchCode, err)
	}
	if spy.Count() != 0 {
		t.Fatalf("expected no dispatcher notify, got %d", spy.Count())
	}
}
