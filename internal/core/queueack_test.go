package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

type stubTCDecider struct {
	calls int
	last  TxnRecord
	state TxnState
	err   error
}

func (s *stubTCDecider) Enlist(context.Context, TxnRecord) error {
	return nil
}

func (s *stubTCDecider) Decide(_ context.Context, rec TxnRecord) (TxnState, error) {
	s.calls++
	s.last = rec
	if s.err != nil {
		return "", s.err
	}
	if s.state == "" {
		s.state = rec.State
	}
	return s.state, nil
}

func newQueueCoreForTest(t *testing.T) (*Service, *queue.Service) {
	t.Helper()
	store := memory.NewWithConfig(memory.Config{QueueWatch: false})
	qsvc, err := queue.New(store, clock.Real{}, queue.Config{})
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	coreSvc := New(Config{
		Store:            store,
		QueueService:     qsvc,
		DefaultNamespace: "default",
		DefaultTTL:       30 * time.Second,
		MaxTTL:           2 * time.Minute,
		Logger:           pslog.NoopLogger(),
	})
	return coreSvc, qsvc
}

func TestQueueAckReleasesLease(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q1", strings.NewReader("hi"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Acquire delivery lease.
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q1", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q1", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q1", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}
	res, err := coreSvc.Ack(ctx, QueueAckCommand{
		Namespace:    "default",
		Queue:        "q1",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		TxnID:        acq.TxnID,
		FencingToken: acq.FencingToken,
		Stateful:     false,
	})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !res.Acked {
		t.Fatalf("expected acked")
	}
	if res.TxnID != acq.TxnID {
		t.Fatalf("expected txn_id %s, got %s", acq.TxnID, res.TxnID)
	}
	meta, _, err := coreSvc.ensureMeta(ctx, "default", msgLeaseKey(t, "default", "q1", doc.ID))
	if err != nil {
		t.Fatalf("ensureMeta: %v", err)
	}
	if meta.Lease != nil {
		t.Fatalf("expected lease cleared after ack")
	}
}

func TestQueueAckAllowsDocLeaseWhenMetaMissing(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q0", strings.NewReader("hi"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q0", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	leaseID := xid.New().String()
	txnID := xid.New().String()
	doc.LeaseID = leaseID
	doc.LeaseFencingToken = 1
	doc.LeaseTxnID = txnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q0", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	res, err := coreSvc.Ack(ctx, QueueAckCommand{
		Namespace:    "default",
		Queue:        "q0",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      leaseID,
		TxnID:        txnID,
		FencingToken: doc.LeaseFencingToken,
		Stateful:     false,
	})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !res.Acked {
		t.Fatalf("expected acked")
	}
	if _, err := qsvc.GetMessage(ctx, "default", "q0", msg.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected message removed, got %v", err)
	}
	relKey := relativeKey("default", msgLeaseKey(t, "default", "q0", doc.ID))
	if _, err := coreSvc.store.LoadMeta(ctx, "default", relKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no lease meta, got %v", err)
	}
}

func TestQueueAckRetriesOnCASMismatch(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q-retry", strings.NewReader("hello"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q-retry", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q-retry", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	doc.NotVisibleUntil = time.Now().UTC().Add(30 * time.Second)
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q-retry", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}
	staleMetaETag := metaETag
	doc.Attributes = map[string]any{"retry": true}
	_, err = qsvc.SaveMessageDocument(ctx, "default", "q-retry", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message retry: %v", err)
	}

	res, err := coreSvc.Ack(ctx, QueueAckCommand{
		Namespace:    "default",
		Queue:        "q-retry",
		MessageID:    msg.ID,
		MetaETag:     staleMetaETag,
		LeaseID:      acq.LeaseID,
		TxnID:        acq.TxnID,
		FencingToken: acq.FencingToken,
		Stateful:     false,
	})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !res.Acked {
		t.Fatalf("expected acked after cas retry")
	}
	if _, err := qsvc.GetMessage(ctx, "default", "q-retry", msg.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected message removed after ack, got %v", err)
	}
}

func TestQueueNackClearsLease(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q2", strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q2", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q2", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q2", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}
	res, err := coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q2",
		MessageID:    doc.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Stateful:     false,
		Delay:        0,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
	})
	if err != nil {
		t.Fatalf("nack: %v", err)
	}
	if !res.Requeued {
		t.Fatalf("expected requeued")
	}
	if res.TxnID != acq.TxnID {
		t.Fatalf("expected txn_id %s, got %s", acq.TxnID, res.TxnID)
	}
	meta, _, err := coreSvc.ensureMeta(ctx, "default", msgLeaseKey(t, "default", "q2", doc.ID))
	if err != nil {
		t.Fatalf("ensureMeta: %v", err)
	}
	if meta.Lease != nil {
		t.Fatalf("expected lease cleared after nack")
	}
}

func TestQueueNackFailureIntentIncrementsFailureAttempts(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q2-failure", strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q2-failure", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q2-failure", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q2-failure", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	_, err = coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q2-failure",
		MessageID:    doc.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Stateful:     false,
		Delay:        0,
		Intent:       QueueNackIntentFailure,
		LastError:    map[string]any{"reason": "boom"},
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
	})
	if err != nil {
		t.Fatalf("nack failure intent: %v", err)
	}

	updated, err := qsvc.GetMessage(ctx, "default", "q2-failure", msg.ID)
	if err != nil {
		t.Fatalf("get updated message: %v", err)
	}
	if updated.Document.FailureAttempts != 1 {
		t.Fatalf("expected failure_attempts=1, got %d", updated.Document.FailureAttempts)
	}
	if updated.Document.LastError == nil {
		t.Fatalf("expected last_error to be recorded for failure intent")
	}
}

func TestQueueNackDeferIntentDoesNotIncrementFailureAttempts(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q2-defer", strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q2-defer", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q2-defer", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q2-defer", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	_, err = coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q2-defer",
		MessageID:    doc.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Stateful:     false,
		Delay:        0,
		Intent:       QueueNackIntentDefer,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
	})
	if err != nil {
		t.Fatalf("nack defer intent: %v", err)
	}

	updated, err := qsvc.GetMessage(ctx, "default", "q2-defer", msg.ID)
	if err != nil {
		t.Fatalf("get updated message: %v", err)
	}
	if updated.Document.FailureAttempts != 0 {
		t.Fatalf("expected failure_attempts=0 for defer intent, got %d", updated.Document.FailureAttempts)
	}
	if updated.Document.LastError != nil {
		t.Fatalf("expected last_error cleared for defer intent, got %#v", updated.Document.LastError)
	}
}

func TestQueueNackRejectsDeferLastError(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q2-defer-last-error", strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q2-defer-last-error", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q2-defer-last-error", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q2-defer-last-error", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	_, err = coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q2-defer-last-error",
		MessageID:    doc.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Delay:        0,
		Intent:       QueueNackIntentDefer,
		LastError:    map[string]any{"reason": "ignored"},
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
	})
	if err == nil {
		t.Fatalf("expected invalid_nack_last_error")
	}
	fail := Failure{}
	if !errors.As(err, &fail) || fail.Code != "invalid_nack_last_error" {
		t.Fatalf("expected invalid_nack_last_error, got %v", err)
	}
}

func TestQueueNackRejectsInvalidIntent(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q2-invalid-intent", strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q2-invalid-intent", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q2-invalid-intent", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q2-invalid-intent", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	_, err = coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q2-invalid-intent",
		MessageID:    doc.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Intent:       QueueNackIntent("maybe-later"),
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
	})
	if err == nil {
		t.Fatalf("expected invalid intent error")
	}
	fail := Failure{}
	if !errors.As(err, &fail) || fail.Code != "invalid_nack_intent" {
		t.Fatalf("expected invalid_nack_intent, got %v", err)
	}
}

func TestQueueNackNormalizesIntent(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q2-normalized-intent", strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q2-normalized-intent", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q2-normalized-intent", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q2-normalized-intent", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	_, err = coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q2-normalized-intent",
		MessageID:    doc.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Intent:       QueueNackIntent("  DeFeR "),
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
	})
	if err != nil {
		t.Fatalf("nack normalized defer intent: %v", err)
	}
}

func TestQueueExtendRenewsLease(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q3", strings.NewReader("x"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q3", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   5,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q3", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q3", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}
	extension := 10 * time.Second
	res, err := coreSvc.Extend(ctx, QueueExtendCommand{
		Namespace:    "default",
		Queue:        "q3",
		MessageID:    doc.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Visibility:   extension,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
	})
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	if res.LeaseExpiresAtUnix <= acq.ExpiresAt {
		t.Fatalf("expected lease expiry to increase")
	}
	if res.TxnID != acq.TxnID {
		t.Fatalf("expected txn_id %s, got %s", acq.TxnID, res.TxnID)
	}
}

func TestQueueAckRejectsMessageLeaseMismatch(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q4", strings.NewReader("hi"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q4", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q4", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = "other-lease"
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q4", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	_, err = coreSvc.Ack(ctx, QueueAckCommand{
		Namespace:    "default",
		Queue:        "q4",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		TxnID:        acq.TxnID,
		FencingToken: acq.FencingToken,
	})
	if err == nil {
		t.Fatalf("expected lease mismatch error")
	}
	fail := Failure{}
	if !errors.As(err, &fail) || fail.Code != queueMessageLeaseMismatchCode {
		t.Fatalf("expected %s, got %v", queueMessageLeaseMismatchCode, err)
	}
}

func TestQueueAckUsesTCDecider(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q7", strings.NewReader("hi"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	txnID := xid.New().String()
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q7", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        txnID,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q7", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q7", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	decider := &stubTCDecider{
		err: Failure{Code: "tc_not_leader", Detail: "tc leader unavailable", HTTPStatus: 409},
	}
	coreSvc.SetTCDecider(decider)

	_, err = coreSvc.Ack(ctx, QueueAckCommand{
		Namespace:    "default",
		Queue:        "q7",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		TxnID:        txnID,
		FencingToken: acq.FencingToken,
		Stateful:     false,
	})
	if err == nil {
		t.Fatalf("expected ack error")
	}
	var fail Failure
	if !errors.As(err, &fail) || fail.Code != "tc_not_leader" {
		t.Fatalf("expected tc_not_leader, got %v", err)
	}
	if decider.calls != 1 {
		t.Fatalf("expected 1 decide call, got %d", decider.calls)
	}
	if decider.last.State != TxnStateCommit {
		t.Fatalf("expected commit state, got %s", decider.last.State)
	}
	if len(decider.last.Participants) != 1 {
		t.Fatalf("expected 1 participant, got %d", len(decider.last.Participants))
	}
	expectedKey := relativeKey("default", msgLeaseKey(t, "default", "q7", msg.ID))
	if decider.last.Participants[0].Key != expectedKey {
		t.Fatalf("participant key mismatch: %s", decider.last.Participants[0].Key)
	}
	if _, err := qsvc.GetMessage(ctx, "default", "q7", msg.ID); err != nil {
		t.Fatalf("expected message to remain, got %v", err)
	}
}

func TestQueueNackUsesTCDecider(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q8", strings.NewReader("hi"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	txnID := xid.New().String()
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q8", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        txnID,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q8", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q8", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	decider := &stubTCDecider{
		err: Failure{Code: "tc_not_leader", Detail: "tc leader unavailable", HTTPStatus: 409},
	}
	coreSvc.SetTCDecider(decider)

	_, err = coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q8",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		TxnID:        txnID,
		FencingToken: acq.FencingToken,
		Delay:        0,
	})
	if err == nil {
		t.Fatalf("expected nack error")
	}
	var fail Failure
	if !errors.As(err, &fail) || fail.Code != "tc_not_leader" {
		t.Fatalf("expected tc_not_leader, got %v", err)
	}
	if decider.calls != 1 {
		t.Fatalf("expected 1 decide call, got %d", decider.calls)
	}
	if decider.last.State != TxnStateRollback {
		t.Fatalf("expected rollback state, got %s", decider.last.State)
	}
	if len(decider.last.Participants) != 1 {
		t.Fatalf("expected 1 participant, got %d", len(decider.last.Participants))
	}
	expectedKey := relativeKey("default", msgLeaseKey(t, "default", "q8", msg.ID))
	if decider.last.Participants[0].Key != expectedKey {
		t.Fatalf("participant key mismatch: %s", decider.last.Participants[0].Key)
	}
	if _, err := qsvc.GetMessage(ctx, "default", "q8", msg.ID); err != nil {
		t.Fatalf("expected message to remain, got %v", err)
	}
}

func TestQueueNackTCDeferDelayClearsStaleLastError(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q8-tc-defer-delay", strings.NewReader("hi"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	txnID := xid.New().String()
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q8-tc-defer-delay", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        txnID,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q8-tc-defer-delay", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	doc.LastError = map[string]any{"reason": "previous_failure"}
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q8-tc-defer-delay", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	decider := &stubTCDecider{}
	coreSvc.SetTCDecider(decider)

	res, err := coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q8-tc-defer-delay",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		TxnID:        txnID,
		FencingToken: acq.FencingToken,
		Delay:        2 * time.Second,
		Intent:       QueueNackIntentDefer,
	})
	if err != nil {
		t.Fatalf("nack defer delay: %v", err)
	}
	if !res.Requeued {
		t.Fatalf("expected requeued")
	}
	if decider.calls != 1 {
		t.Fatalf("expected 1 decide call, got %d", decider.calls)
	}

	updated, err := qsvc.GetMessage(ctx, "default", "q8-tc-defer-delay", msg.ID)
	if err != nil {
		t.Fatalf("get updated message: %v", err)
	}
	if updated.Document.FailureAttempts != 0 {
		t.Fatalf("expected failure_attempts=0, got %d", updated.Document.FailureAttempts)
	}
	if updated.Document.LastError != nil {
		t.Fatalf("expected stale last_error cleared, got %#v", updated.Document.LastError)
	}
	if !updated.Document.NotVisibleUntil.After(time.Now().UTC()) {
		t.Fatalf("expected deferred visibility in future, got %v", updated.Document.NotVisibleUntil)
	}
}

func TestQueueNackTCDeferZeroDelayClearsStaleLastError(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q8-tc-defer-zero", strings.NewReader("hi"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	txnID := xid.New().String()
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q8-tc-defer-zero", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
		TxnID:        txnID,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q8-tc-defer-zero", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = acq.LeaseID
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	doc.LastError = map[string]any{"reason": "previous_failure"}
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q8-tc-defer-zero", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	decider := &stubTCDecider{}
	coreSvc.SetTCDecider(decider)

	res, err := coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q8-tc-defer-zero",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		TxnID:        txnID,
		FencingToken: acq.FencingToken,
		Delay:        0,
		Intent:       QueueNackIntentDefer,
	})
	if err != nil {
		t.Fatalf("nack defer zero delay: %v", err)
	}
	if !res.Requeued {
		t.Fatalf("expected requeued")
	}
	if res.MetaETag == "" || res.MetaETag == metaETag {
		t.Fatalf("expected metadata etag to change, old=%q new=%q", metaETag, res.MetaETag)
	}
	if decider.calls != 1 {
		t.Fatalf("expected 1 decide call, got %d", decider.calls)
	}

	updated, err := qsvc.GetMessage(ctx, "default", "q8-tc-defer-zero", msg.ID)
	if err != nil {
		t.Fatalf("get updated message: %v", err)
	}
	if updated.Document.FailureAttempts != 0 {
		t.Fatalf("expected failure_attempts=0, got %d", updated.Document.FailureAttempts)
	}
	if updated.Document.LastError != nil {
		t.Fatalf("expected stale last_error cleared, got %#v", updated.Document.LastError)
	}
}

func TestQueueNackRejectsMessageLeaseMismatch(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q5", strings.NewReader("payload"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q5", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q5", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = "other-lease"
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q5", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	_, err = coreSvc.Nack(ctx, QueueNackCommand{
		Namespace:    "default",
		Queue:        "q5",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
		Delay:        0,
	})
	if err == nil {
		t.Fatalf("expected lease mismatch error")
	}
	fail := Failure{}
	if !errors.As(err, &fail) || fail.Code != queueMessageLeaseMismatchCode {
		t.Fatalf("expected %s, got %v", queueMessageLeaseMismatchCode, err)
	}
}

func TestQueueExtendRejectsMessageLeaseMismatch(t *testing.T) {
	ctx := context.Background()
	coreSvc, qsvc := newQueueCoreForTest(t)

	msg, err := qsvc.Enqueue(ctx, "default", "q6", strings.NewReader("x"), queue.EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	acq, err := coreSvc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          relativeKey("default", msgLeaseKey(t, "default", "q6", msg.ID)),
		Owner:        "worker",
		TTLSeconds:   5,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	docRes, err := qsvc.GetMessage(ctx, "default", "q6", msg.ID)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	doc := docRes.Document
	metaETag := docRes.ETag
	doc.LeaseID = "other-lease"
	doc.LeaseFencingToken = acq.FencingToken
	doc.LeaseTxnID = acq.TxnID
	metaETag, err = qsvc.SaveMessageDocument(ctx, "default", "q6", doc.ID, doc, metaETag)
	if err != nil {
		t.Fatalf("save message: %v", err)
	}

	_, err = coreSvc.Extend(ctx, QueueExtendCommand{
		Namespace:    "default",
		Queue:        "q6",
		MessageID:    msg.ID,
		MetaETag:     metaETag,
		LeaseID:      acq.LeaseID,
		Visibility:   10 * time.Second,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
	})
	if err == nil {
		t.Fatalf("expected lease mismatch error")
	}
	fail := Failure{}
	if !errors.As(err, &fail) || fail.Code != queueMessageLeaseMismatchCode {
		t.Fatalf("expected %s, got %v", queueMessageLeaseMismatchCode, err)
	}
}

// helpers
const apiBlockNoWait = int64(-1)

func msgLeaseKey(t *testing.T, namespace, queueName, id string) string {
	t.Helper()
	key, err := queue.MessageLeaseKey(namespace, queueName, id)
	if err != nil {
		t.Fatalf("MessageLeaseKey: %v", err)
	}
	return key
}
