package core

import "context"

// TCDecider drives implicit XA decisions through a TC leader.
type TCDecider interface {
	Enlist(ctx context.Context, rec TxnRecord) error
	Decide(ctx context.Context, rec TxnRecord) (TxnState, error)
}
