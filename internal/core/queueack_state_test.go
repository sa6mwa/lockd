package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

// Covers stateful ack/nack/extend paths including fencing and CAS surface.
func TestQueueAckStatefulFlow(t *testing.T) {
	ctx := context.Background()
	store := memory.NewWithConfig(memory.Config{QueueWatch: false})
	qsvc, _ := queue.New(store, clock.Real{}, queue.Config{})
	svc := New(Config{
		Store:            store,
		QueueService:     qsvc,
		DefaultNamespace: "default",
		DefaultTTL:       10 * time.Second,
		MaxTTL:           1 * time.Minute,
		Logger:           pslog.NoopLogger(),
	})

	// Enqueue and ensure state.
	msg, err := qsvc.Enqueue(ctx, "default", "jobs", strings.NewReader("x"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := qsvc.EnsureStateExists(ctx, "default", "jobs", msg.ID); err != nil {
		t.Fatalf("ensure state: %v", err)
	}

	// Acquire message and state leases via core Acquire (mimic delivery).
	msgKey, _ := queue.MessageLeaseKey("default", "jobs", msg.ID)
	stateKey, _ := queue.StateLeaseKey("default", "jobs", msg.ID)
	acqMsg, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: relativeKey("default", msgKey), Owner: "worker", TTLSeconds: 5})
	if err != nil {
		t.Fatalf("acquire msg: %v", err)
	}
	acqState, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: relativeKey("default", stateKey), Owner: "worker", TTLSeconds: 5})
	if err != nil {
		t.Fatalf("acquire state: %v", err)
	}

	// Nack (stateful) and ensure leases are cleared and meta_etag changes.
	docRes, _ := qsvc.GetMessage(ctx, "default", "jobs", msg.ID)
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acqMsg.LeaseID
	doc.LeaseFencingToken = acqMsg.FencingToken
	doc.LeaseTxnID = acqMsg.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "jobs", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}
	resNack, err := svc.Nack(ctx, QueueNackCommand{
		Namespace:         "default",
		Queue:             "jobs",
		MessageID:         msg.ID,
		MetaETag:          metaETag,
		Stateful:          true,
		LeaseID:           acqMsg.LeaseID,
		StateLeaseID:      acqState.LeaseID,
		FencingToken:      acqMsg.FencingToken,
		StateFencingToken: acqState.FencingToken,
		TxnID:             acqMsg.TxnID,
		Delay:             0,
	})
	if err != nil || !resNack.Requeued {
		t.Fatalf("nack stateful: %v", err)
	}
	metaAfter, _, _ := svc.ensureMeta(ctx, "default", msgKey)
	if metaAfter.Lease != nil {
		t.Fatalf("expected msg lease cleared after nack")
	}
	stateMetaAfter, _, _ := svc.ensureMeta(ctx, "default", stateKey)
	if stateMetaAfter.Lease != nil {
		t.Fatalf("expected state lease cleared after nack")
	}

	// Reacquire and extend stateful delivery.
	acqMsg2, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: relativeKey("default", msgKey), Owner: "worker", TTLSeconds: 5})
	if err != nil {
		t.Fatalf("re-acquire msg: %v", err)
	}
	acqState2, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: relativeKey("default", stateKey), Owner: "worker", TTLSeconds: 5})
	if err != nil {
		t.Fatalf("re-acquire state: %v", err)
	}
	docRes, err = qsvc.GetMessage(ctx, "default", "jobs", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc = docRes.Document
	metaETag = docRes.ETag
	doc.LeaseID = acqMsg2.LeaseID
	doc.LeaseFencingToken = acqMsg2.FencingToken
	doc.LeaseTxnID = acqMsg2.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "jobs", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}
	ext, err := svc.Extend(ctx, QueueExtendCommand{
		Namespace:         "default",
		Queue:             "jobs",
		MessageID:         doc.ID,
		MetaETag:          metaETag,
		Stateful:          true,
		LeaseID:           acqMsg2.LeaseID,
		StateLeaseID:      acqState2.LeaseID,
		FencingToken:      acqMsg2.FencingToken,
		StateFencingToken: acqState2.FencingToken,
		TxnID:             acqMsg2.TxnID,
		Visibility:        15 * time.Second,
	})
	if err != nil {
		t.Fatalf("extend stateful: %v", err)
	}
	if ext.StateLeaseExpiresAtUnix <= acqState2.ExpiresAt {
		t.Fatalf("expected state lease extended")
	}
	rec, _, err := svc.loadTxnRecord(ctx, acqMsg2.TxnID)
	if err != nil {
		t.Fatalf("load txn record: %v", err)
	}
	if rec == nil {
		t.Fatalf("expected txn record for %s", acqMsg2.TxnID)
	}
	if rec.ExpiresAtUnix < ext.LeaseExpiresAtUnix {
		t.Fatalf("expected txn expiry >= lease expiry, got %d want >= %d", rec.ExpiresAtUnix, ext.LeaseExpiresAtUnix)
	}

	// Ack stateful delivery.
	resAck, err := svc.Ack(ctx, QueueAckCommand{
		Namespace:         "default",
		Queue:             "jobs",
		MessageID:         doc.ID,
		MetaETag:          ext.MetaETag,
		LeaseID:           acqMsg2.LeaseID,
		StateLeaseID:      acqState2.LeaseID,
		FencingToken:      acqMsg2.FencingToken,
		StateFencingToken: acqState2.FencingToken,
		TxnID:             acqMsg2.TxnID,
		Stateful:          true,
	})
	if err != nil || !resAck.Acked {
		t.Fatalf("ack stateful: %v", err)
	}
	metaFinal, _, _ := svc.ensureMeta(ctx, "default", msgKey)
	if metaFinal.Lease != nil {
		t.Fatalf("expected msg lease cleared after ack")
	}
	stateMetaFinal, _, _ := svc.ensureMeta(ctx, "default", stateKey)
	if stateMetaFinal.Lease != nil {
		t.Fatalf("expected state lease cleared after ack")
	}
}
