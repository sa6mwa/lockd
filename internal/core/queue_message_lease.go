package core

import (
	"errors"

	"pkt.systems/lockd/internal/queue"
)

const queueMessageLeaseMismatchCode = "queue_message_lease_mismatch"

func queueMessageLeaseFailure(detail string) Failure {
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
	if doc.LeaseID == "" || doc.LeaseTxnID == "" || doc.LeaseFencingToken == 0 {
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

func isQueueMessageLeaseMismatch(err error) bool {
	var failure Failure
	if errors.As(err, &failure) {
		return failure.Code == queueMessageLeaseMismatchCode
	}
	return false
}
