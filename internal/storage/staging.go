package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
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

const stagingMarker = "/.staging/"

// StagedObjectLister is the minimal object-listing surface required to
// discover staged entries.
type StagedObjectLister interface {
	ListObjects(ctx context.Context, namespace string, opts ListOptions) (*ListResult, error)
}

// IsStagingObjectKey reports whether key belongs to a staging subtree.
func IsStagingObjectKey(key string) bool {
	if strings.HasPrefix(key, ".staging/") {
		return true
	}
	return strings.Contains(key, stagingMarker)
}

// ListStagedStateFromObjects lists staged state objects by scanning namespace
// objects and selecting keys that match "<key>/.staging/<txn>" (or root-level
// ".staging/<txn>"). The scan excludes nested staging objects such as staged
// attachments under ".staging/<txn>/attachments/...".
func ListStagedStateFromObjects(ctx context.Context, lister StagedObjectLister, namespace string, opts ListStagedOptions) (*ListResult, error) {
	if lister == nil {
		return nil, fmt.Errorf("storage: staged state listing requires object lister")
	}
	txnPrefix := strings.TrimPrefix(strings.TrimSpace(opts.TxnPrefix), "/")
	scan := ListOptions{StartAfter: opts.StartAfter}
	matches := make([]ObjectInfo, 0, max(0, opts.Limit))
	for {
		list, err := lister.ListObjects(ctx, namespace, scan)
		if err != nil {
			return nil, err
		}
		for i, obj := range list.Objects {
			txnID, ok := stagedStateTxnID(obj.Key)
			if !ok {
				continue
			}
			if txnPrefix != "" && !strings.HasPrefix(txnID, txnPrefix) {
				continue
			}
			matches = append(matches, obj)
			if opts.Limit > 0 && len(matches) >= opts.Limit {
				hasMore, err := hasMoreStagedState(ctx, lister, namespace, scan, list, i+1, txnPrefix)
				if err != nil {
					return nil, err
				}
				if hasMore {
					return &ListResult{
						Objects:        matches,
						Truncated:      true,
						NextStartAfter: matches[len(matches)-1].Key,
					}, nil
				}
				return &ListResult{Objects: matches}, nil
			}
		}
		if !list.Truncated || list.NextStartAfter == "" {
			return &ListResult{Objects: matches}, nil
		}
		scan.StartAfter = list.NextStartAfter
	}
}

func hasMoreStagedState(ctx context.Context, lister StagedObjectLister, namespace string, scan ListOptions, current *ListResult, nextIndex int, txnPrefix string) (bool, error) {
	if current != nil {
		for i := nextIndex; i < len(current.Objects); i++ {
			txnID, ok := stagedStateTxnID(current.Objects[i].Key)
			if !ok {
				continue
			}
			if txnPrefix != "" && !strings.HasPrefix(txnID, txnPrefix) {
				continue
			}
			return true, nil
		}
		if !current.Truncated || current.NextStartAfter == "" {
			return false, nil
		}
		scan.StartAfter = current.NextStartAfter
	}
	for {
		list, err := lister.ListObjects(ctx, namespace, scan)
		if err != nil {
			return false, err
		}
		for _, obj := range list.Objects {
			txnID, ok := stagedStateTxnID(obj.Key)
			if !ok {
				continue
			}
			if txnPrefix != "" && !strings.HasPrefix(txnID, txnPrefix) {
				continue
			}
			return true, nil
		}
		if !list.Truncated || list.NextStartAfter == "" {
			return false, nil
		}
		scan.StartAfter = list.NextStartAfter
	}
}

func stagedStateTxnID(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	if strings.HasPrefix(key, ".staging/") {
		txnID := strings.TrimPrefix(key, ".staging/")
		if txnID == "" || strings.Contains(txnID, "/") {
			return "", false
		}
		return txnID, true
	}
	idx := strings.Index(key, stagingMarker)
	if idx < 0 {
		return "", false
	}
	txnID := key[idx+len(stagingMarker):]
	if txnID == "" || strings.Contains(txnID, "/") {
		return "", false
	}
	return txnID, true
}
