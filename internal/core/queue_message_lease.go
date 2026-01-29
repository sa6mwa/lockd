package core

import (
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

const queueMessageLeaseMismatchCode = "queue_message_lease_mismatch"

var (
	queueLeaseStatsEnable       = os.Getenv("LOCKD_QUEUE_LEASE_STATS") == "1"
	queueLeaseMissing           atomic.Int64
	queueLeaseMismatch          atomic.Int64
	queueLeaseTxnMismatch       atomic.Int64
	queueLeaseVisibilityExpired atomic.Int64

	queueLeaseAckNoRecord       atomic.Int64
	queueLeaseAckRecordMatch    atomic.Int64
	queueLeaseAckRecordMismatch atomic.Int64
	queueLeaseAckMetaMismatch   atomic.Int64

	queueLeaseRecordOps           atomic.Uint64
	queueLeaseRecords             sync.Map
	queueLeaseRedeliveredInflight atomic.Int64
)

// QueueLeaseStats captures queue lease mismatch counters.
type QueueLeaseStats struct {
	MissingLease        int64
	LeaseMismatch       int64
	TxnMismatch         int64
	VisibilityExpired   int64
	AckNoRecord         int64
	AckRecordMatch      int64
	AckRecordMismatch   int64
	AckMetaMismatch     int64
	RedeliveredInflight int64
}

// QueueLeaseStatsSnapshot returns the current lease mismatch counters.
func QueueLeaseStatsSnapshot() QueueLeaseStats {
	return QueueLeaseStats{
		MissingLease:        queueLeaseMissing.Load(),
		LeaseMismatch:       queueLeaseMismatch.Load(),
		TxnMismatch:         queueLeaseTxnMismatch.Load(),
		VisibilityExpired:   queueLeaseVisibilityExpired.Load(),
		AckNoRecord:         queueLeaseAckNoRecord.Load(),
		AckRecordMatch:      queueLeaseAckRecordMatch.Load(),
		AckRecordMismatch:   queueLeaseAckRecordMismatch.Load(),
		AckMetaMismatch:     queueLeaseAckMetaMismatch.Load(),
		RedeliveredInflight: queueLeaseRedeliveredInflight.Load(),
	}
}

// ResetQueueLeaseStats clears the queue lease mismatch counters.
func ResetQueueLeaseStats() {
	queueLeaseMissing.Store(0)
	queueLeaseMismatch.Store(0)
	queueLeaseTxnMismatch.Store(0)
	queueLeaseVisibilityExpired.Store(0)
	queueLeaseAckNoRecord.Store(0)
	queueLeaseAckRecordMatch.Store(0)
	queueLeaseAckRecordMismatch.Store(0)
	queueLeaseAckMetaMismatch.Store(0)
	queueLeaseRecordOps.Store(0)
	queueLeaseRedeliveredInflight.Store(0)
}

func noteQueueLeaseStat(detail string) {
	if !queueLeaseStatsEnable {
		return
	}
	lower := strings.ToLower(detail)
	switch {
	case strings.Contains(lower, "missing"):
		queueLeaseMissing.Add(1)
	case strings.Contains(lower, "txn"):
		queueLeaseTxnMismatch.Add(1)
	case strings.Contains(lower, "visibility"):
		queueLeaseVisibilityExpired.Add(1)
	default:
		queueLeaseMismatch.Add(1)
	}
}

type queueLeaseRecord struct {
	leaseID         string
	fencingToken    int64
	txnID           string
	metaETag        string
	leaseMetaETag   string
	leaseMetaSet    bool
	leaseMeta       storage.Meta
	updatedAt       time.Time
	notVisibleUntil time.Time
}

func queueLeaseRecordKey(namespace, queue, id string) string {
	return namespace + "\x1f" + queue + "\x1f" + id
}

func recordQueueLeaseDelivery(namespace, queue, id, leaseID string, fencingToken int64, txnID string, metaETag string, notVisibleUntil time.Time, now time.Time, leaseMeta *storage.Meta, leaseMetaETag string) (bool, queueLeaseRecord) {
	var previous queueLeaseRecord
	redelivered := false
	if existing, ok := queueLeaseRecords.Load(queueLeaseRecordKey(namespace, queue, id)); ok {
		if rec, ok := existing.(queueLeaseRecord); ok {
			previous = rec
			if now.Before(rec.notVisibleUntil) && (rec.leaseID != leaseID || rec.fencingToken != fencingToken || rec.metaETag != metaETag) {
				redelivered = true
				if queueLeaseStatsEnable {
					queueLeaseRedeliveredInflight.Add(1)
				}
			}
		}
	}
	record := queueLeaseRecord{
		leaseID:         leaseID,
		fencingToken:    fencingToken,
		txnID:           txnID,
		metaETag:        metaETag,
		updatedAt:       now,
		notVisibleUntil: notVisibleUntil,
	}
	if leaseMeta != nil && leaseMetaETag != "" {
		record.leaseMetaSet = true
		record.leaseMetaETag = leaseMetaETag
		record.leaseMeta = cloneMeta(*leaseMeta)
	}
	queueLeaseRecords.Store(queueLeaseRecordKey(namespace, queue, id), record)
	if queueLeaseRecordOps.Add(1)%1024 == 0 {
		cutoff := now.Add(-2 * time.Minute)
		queueLeaseRecords.Range(func(key, value any) bool {
			rec, ok := value.(queueLeaseRecord)
			if !ok || rec.updatedAt.Before(cutoff) {
				queueLeaseRecords.Delete(key)
			}
			return true
		})
	}
	return redelivered, previous
}

func queueLeaseRecordSnapshot(namespace, queue, id string) (queueLeaseRecord, bool) {
	value, ok := queueLeaseRecords.Load(queueLeaseRecordKey(namespace, queue, id))
	if !ok {
		return queueLeaseRecord{}, false
	}
	rec, ok := value.(queueLeaseRecord)
	if !ok {
		return queueLeaseRecord{}, false
	}
	return rec, true
}

func queueLeaseRecordDocument(namespace, queueName, id string, rec queueLeaseRecord) *queue.MessageDocument {
	return &queue.MessageDocument{
		Namespace:         namespace,
		Queue:             queueName,
		ID:                id,
		UpdatedAt:         rec.updatedAt,
		NotVisibleUntil:   rec.notVisibleUntil,
		LeaseID:           rec.leaseID,
		LeaseFencingToken: rec.fencingToken,
		LeaseTxnID:        rec.txnID,
	}
}

func noteQueueAckLeaseMismatch(namespace, queue, id, ackLeaseID string, ackFencing int64, ackTxnID string, ackMetaETag string) {
	if !queueLeaseStatsEnable {
		return
	}
	key := queueLeaseRecordKey(namespace, queue, id)
	value, ok := queueLeaseRecords.Load(key)
	if !ok {
		queueLeaseAckNoRecord.Add(1)
		return
	}
	rec, ok := value.(queueLeaseRecord)
	if !ok {
		queueLeaseAckNoRecord.Add(1)
		return
	}
	ackMatches := ackLeaseID == rec.leaseID && ackFencing == rec.fencingToken
	if ackTxnID == "" {
		ackMatches = ackMatches && rec.txnID == ""
	} else {
		ackMatches = ackMatches && rec.txnID == ackTxnID
	}
	if ackMatches {
		queueLeaseAckRecordMatch.Add(1)
	} else {
		queueLeaseAckRecordMismatch.Add(1)
	}
	if ackMetaETag != "" && rec.metaETag != "" && ackMetaETag != rec.metaETag {
		queueLeaseAckMetaMismatch.Add(1)
	}
}

func queueMessageLeaseFailure(detail string) Failure {
	noteQueueLeaseStat(detail)
	return Failure{
		Code:       queueMessageLeaseMismatchCode,
		Detail:     detail,
		HTTPStatus: 409,
	}
}

func validateQueueMessageLease(doc *queue.MessageDocument, leaseID string, fencingToken int64, txnID string) error {
	if doc == nil {
		return queueMessageLeaseFailure("queue message lease missing")
	}
	if doc.LeaseID == "" || doc.LeaseFencingToken == 0 {
		return queueMessageLeaseFailure("queue message lease missing")
	}
	if doc.LeaseID != leaseID || doc.LeaseFencingToken != fencingToken {
		return queueMessageLeaseFailure("queue message lease mismatch")
	}
	if txnID == "" {
		if doc.LeaseTxnID != "" {
			return queueMessageLeaseFailure("queue message lease mismatch")
		}
	} else if doc.LeaseTxnID != txnID {
		return queueMessageLeaseFailure("queue message lease mismatch")
	}
	return nil
}

// validateQueueMessageLeaseIfPresent enforces lease checks when lease data is present
// on the message document. If the document lacks any lease fields, validation is skipped
// and callers should rely on meta lease checks.
func validateQueueMessageLeaseIfPresent(doc *queue.MessageDocument, leaseID string, fencingToken int64, txnID string) error {
	if doc == nil {
		return queueMessageLeaseFailure("queue message lease missing")
	}
	if doc.LeaseID == "" && doc.LeaseFencingToken == 0 && doc.LeaseTxnID == "" {
		return nil
	}
	return validateQueueMessageLease(doc, leaseID, fencingToken, txnID)
}

func queueMessageLeaseMatches(doc *queue.MessageDocument, leaseID string, fencingToken int64, txnID string, now time.Time) bool {
	if doc == nil {
		return false
	}
	if doc.LeaseID == "" && doc.LeaseFencingToken == 0 && doc.LeaseTxnID == "" {
		return false
	}
	if doc.LeaseID != leaseID || doc.LeaseFencingToken != fencingToken {
		return false
	}
	if txnID == "" {
		if doc.LeaseTxnID != "" {
			return false
		}
	} else if doc.LeaseTxnID != txnID {
		return false
	}
	if !doc.NotVisibleUntil.IsZero() && !doc.NotVisibleUntil.After(now) {
		noteQueueLeaseStat("queue message lease visibility expired")
		return false
	}
	return true
}

func isQueueMessageLeaseMismatch(err error) bool {
	var failure Failure
	if errors.As(err, &failure) {
		return failure.Code == queueMessageLeaseMismatchCode
	}
	return false
}
