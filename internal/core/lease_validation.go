package core

import (
	"net/http"
	"time"

	"pkt.systems/lockd/internal/storage"
)

func validateLease(meta *storage.Meta, leaseID string, fencingToken int64, txnID string, now time.Time) error {
	if meta.Lease == nil || meta.Lease.ID != leaseID {
		return Failure{
			HTTPStatus: http.StatusForbidden,
			Code:       "lease_required",
			Detail:     "active lease required",
			Version:    meta.Version,
			ETag:       meta.StateETag,
		}
	}
	if meta.Lease.ExpiresAtUnix < now.Unix() {
		return Failure{
			HTTPStatus: http.StatusForbidden,
			Code:       "lease_expired",
			Detail:     "lease expired",
			Version:    meta.Version,
			ETag:       meta.StateETag,
		}
	}
	if meta.Lease.FencingToken != fencingToken {
		return Failure{
			HTTPStatus: http.StatusForbidden,
			Code:       "fencing_mismatch",
			Detail:     "fencing token mismatch",
			Version:    meta.Version,
			ETag:       meta.StateETag,
		}
	}
	if meta.Lease.TxnID != "" && txnID == "" {
		return Failure{
			HTTPStatus: http.StatusBadRequest,
			Code:       "missing_txn",
			Detail:     "transaction id required",
			Version:    meta.Version,
			ETag:       meta.StateETag,
		}
	}
	if meta.Lease.TxnID != "" && txnID != "" && meta.Lease.TxnID != txnID {
		return Failure{
			HTTPStatus: http.StatusConflict,
			Code:       "txn_mismatch",
			Detail:     "transaction mismatch",
			Version:    meta.Version,
			ETag:       meta.StateETag,
		}
	}
	return nil
}
