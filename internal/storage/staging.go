package storage

import (
	"context"
	"io"
)

// StagingBackend exposes transactional state primitives that allow callers to
// stage writes under a transaction id and later commit or discard them. The
// default backends do not yet implement this; server code should feature-detect
// support via type assertions and fall back or error accordingly.
//
// The intent is:
//  1. StageState writes the pending payload to a backend-specific staging
//     location that is isolated per txn_id (e.g. /<ns>/<key>/.staging/<txn>).
//  2. PromoteStagedState swaps the staged payload into the committed head using
//     CAS against the current head etag when provided.
//  3. DiscardStagedState removes staged payloads without touching the head.
//  4. ListStagedState lets sweepers enumerate orphaned or expired staging
//     entries for cleanup.
//
// Backends with rename support may implement Promote as an atomic rename; those
// without should copy + CAS swap to preserve correctness.
type StagingBackend interface {
	// StageState uploads a staged payload bound to txnID.
	StageState(ctx context.Context, namespace, key, txnID string, body io.Reader, opts PutStateOptions) (*PutStateResult, error)
	// LoadStagedState streams a previously staged payload.
	LoadStagedState(ctx context.Context, namespace, key, txnID string) (ReadStateResult, error)
	// PromoteStagedState atomically replaces the committed head with the staged
	// payload. When ExpectedHeadETag is set, the backend must enforce CAS.
	PromoteStagedState(ctx context.Context, namespace, key, txnID string, opts PromoteStagedOptions) (*PutStateResult, error)
	// DiscardStagedState deletes the staged payload. ExpectedETag is optional and
	// allows defensive CAS on the staged object itself.
	DiscardStagedState(ctx context.Context, namespace, key, txnID string, opts DiscardStagedOptions) error
	// ListStagedState enumerates staged entries, primarily for sweeper cleanup.
	ListStagedState(ctx context.Context, namespace string, opts ListStagedOptions) (*ListResult, error)
}

// PromoteStagedOptions governs the CAS semantics for promoting staged data.
type PromoteStagedOptions struct {
	ExpectedHeadETag string
}

// DiscardStagedOptions controls conditional deletes of staged payloads.
type DiscardStagedOptions struct {
	ExpectedETag   string
	IgnoreNotFound bool
}

// ListStagedOptions scopes staged listings for sweepers.
type ListStagedOptions struct {
	TxnPrefix  string // optional: filter txn ids by prefix
	StartAfter string
	Limit      int
}
