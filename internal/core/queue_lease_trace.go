package core

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

var (
	queueLeaseTraceEnable  = os.Getenv("LOCKD_QUEUE_LEASE_TRACE") == "1"
	queueLeaseTraceStderr  = os.Getenv("LOCKD_QUEUE_LEASE_TRACE_STDERR") == "1"
	queueLeaseTraceSample  = loadQueueLeaseTraceSample()
	queueLeaseTraceCounter atomic.Uint64
)

func loadQueueLeaseTraceSample() uint64 {
	raw := strings.TrimSpace(os.Getenv("LOCKD_QUEUE_LEASE_TRACE_SAMPLE"))
	if raw == "" {
		return 1
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 1
	}
	return value
}

func queueLeaseTraceEnabled() bool {
	return queueLeaseTraceEnable
}

func shouldTraceQueueLease() bool {
	if !queueLeaseTraceEnable {
		return false
	}
	if queueLeaseTraceSample <= 1 {
		return true
	}
	return queueLeaseTraceCounter.Add(1)%queueLeaseTraceSample == 0
}

func traceQueueLease(logger pslog.Logger, event string, fields ...any) {
	if !shouldTraceQueueLease() {
		return
	}
	if logger != nil {
		logger.Trace(event, fields...)
	}
	if queueLeaseTraceStderr {
		queueLeaseTracef(event, fields...)
	}
}

func queueLeaseTracef(event string, fields ...any) {
	fmt.Fprint(os.Stderr, event)
	for i := 0; i+1 < len(fields); i += 2 {
		fmt.Fprintf(os.Stderr, " %v=%v", fields[i], fields[i+1])
	}
	if len(fields)%2 == 1 {
		fmt.Fprintf(os.Stderr, " %v", fields[len(fields)-1])
	}
	fmt.Fprintln(os.Stderr)
}

func traceQueueLeaseDocMismatch(logger pslog.Logger, event, namespace, queueName, messageID, leaseID string, fencingToken int64, txnID, metaETag string, doc *queue.MessageDocument, docETag string) {
	if !queueLeaseTraceEnabled() {
		return
	}
	fields := []any{
		"namespace", namespace,
		"queue", queueName,
		"message_id", messageID,
		"lease_id", leaseID,
		"fencing_token", fencingToken,
		"txn_id", txnID,
		"meta_etag", metaETag,
		"doc_lease_id", doc.LeaseID,
		"doc_fencing_token", doc.LeaseFencingToken,
		"doc_txn_id", doc.LeaseTxnID,
		"doc_not_visible_until", doc.NotVisibleUntil.Unix(),
		"doc_meta_etag", docETag,
	}
	if rec, ok := queueLeaseRecordSnapshot(namespace, queueName, messageID); ok {
		fields = append(fields,
			"record_lease_id", rec.leaseID,
			"record_fencing_token", rec.fencingToken,
			"record_txn_id", rec.txnID,
			"record_meta_etag", rec.metaETag,
			"record_not_visible_until", rec.notVisibleUntil.Unix(),
			"record_updated_at", rec.updatedAt.Unix(),
		)
	}
	traceQueueLease(logger, event, fields...)
}

func traceQueueLeaseMetaMismatch(logger pslog.Logger, event, namespace, queueName, messageID, leaseID string, fencingToken int64, txnID, metaETag string, meta *storage.Meta, doc *queue.MessageDocument, docETag string, now time.Time, docLeaseMatch bool) {
	if !queueLeaseTraceEnabled() {
		return
	}
	metaLeaseID := ""
	metaLeaseTxn := ""
	metaLeaseFencing := int64(0)
	metaLeaseExpires := int64(0)
	metaLeaseOwner := ""
	if meta != nil && meta.Lease != nil {
		metaLeaseID = meta.Lease.ID
		metaLeaseTxn = meta.Lease.TxnID
		metaLeaseFencing = meta.Lease.FencingToken
		metaLeaseExpires = meta.Lease.ExpiresAtUnix
		metaLeaseOwner = meta.Lease.Owner
	}
	fields := []any{
		"namespace", namespace,
		"queue", queueName,
		"message_id", messageID,
		"lease_id", leaseID,
		"fencing_token", fencingToken,
		"txn_id", txnID,
		"meta_etag", metaETag,
		"doc_lease_match", docLeaseMatch,
		"doc_lease_id", doc.LeaseID,
		"doc_fencing_token", doc.LeaseFencingToken,
		"doc_txn_id", doc.LeaseTxnID,
		"doc_not_visible_until", doc.NotVisibleUntil.Unix(),
		"doc_meta_etag", docETag,
		"meta_lease_id", metaLeaseID,
		"meta_fencing_token", metaLeaseFencing,
		"meta_txn_id", metaLeaseTxn,
		"meta_lease_owner", metaLeaseOwner,
		"meta_expires_at", metaLeaseExpires,
		"now_unix", now.Unix(),
	}
	if rec, ok := queueLeaseRecordSnapshot(namespace, queueName, messageID); ok {
		fields = append(fields,
			"record_lease_id", rec.leaseID,
			"record_fencing_token", rec.fencingToken,
			"record_txn_id", rec.txnID,
			"record_meta_etag", rec.metaETag,
			"record_not_visible_until", rec.notVisibleUntil.Unix(),
			"record_updated_at", rec.updatedAt.Unix(),
		)
	}
	traceQueueLease(logger, event, fields...)
}
