package storage

import (
	"context"
	"fmt"
	"io"
	"path"
)

// defaultStagingBackend reuses the existing Backend surface by storing staged
// payloads under a per-key .staging/<txn-id> suffix. It is a portability layer
// so staging works across all backends without custom implementations.
type defaultStagingBackend struct {
	backend Backend
}

// NewDefaultStagingBackend wraps a Backend with staging semantics.
func NewDefaultStagingBackend(b Backend) StagingBackend { return &defaultStagingBackend{backend: b} }

// EnsureStaging returns a StagingBackend, wrapping b when it doesn't already
// implement the interface.
func EnsureStaging(b Backend) StagingBackend {
	if sb, ok := b.(StagingBackend); ok {
		return sb
	}
	return NewDefaultStagingBackend(b)
}

func (d *defaultStagingBackend) stagingKey(key, txnID string) string {
	return path.Join(key, ".staging", txnID)
}

func (d *defaultStagingBackend) StageState(ctx context.Context, namespace, key, txnID string, body io.Reader, opts PutStateOptions) (*PutStateResult, error) {
	return d.backend.WriteState(ctx, namespace, d.stagingKey(key, txnID), body, opts)
}

func (d *defaultStagingBackend) LoadStagedState(ctx context.Context, namespace, key, txnID string) (ReadStateResult, error) {
	return d.backend.ReadState(ctx, namespace, d.stagingKey(key, txnID))
}

func (d *defaultStagingBackend) PromoteStagedState(ctx context.Context, namespace, key, txnID string, opts PromoteStagedOptions) (*PutStateResult, error) {
	state, err := d.backend.ReadState(ctx, namespace, d.stagingKey(key, txnID))
	if err != nil {
		return nil, err
	}
	defer state.Reader.Close()
	// Preserve the plaintext size hint so backends can keep size metadata
	// accurate while re-encrypting the final object.
	propagatedCtx := ContextWithStatePlaintextSize(ctx, state.Info.Size)
	res, err := d.backend.WriteState(propagatedCtx, namespace, key, state.Reader, PutStateOptions{
		ExpectedETag: opts.ExpectedHeadETag,
		IfNotExists:  opts.ExpectedHeadETag == "",
	})
	if err != nil {
		return nil, err
	}
	_ = d.backend.Remove(ctx, namespace, d.stagingKey(key, txnID), "")
	return res, nil
}

func (d *defaultStagingBackend) DiscardStagedState(ctx context.Context, namespace, key, txnID string, opts DiscardStagedOptions) error {
	return d.backend.Remove(ctx, namespace, d.stagingKey(key, txnID), opts.ExpectedETag)
}

func (d *defaultStagingBackend) ListStagedState(ctx context.Context, namespace string, opts ListStagedOptions) (*ListResult, error) {
	prefix := ".staging/"
	if opts.TxnPrefix != "" {
		prefix = path.Join(prefix, opts.TxnPrefix)
	}
	return d.backend.ListObjects(ctx, namespace, ListOptions{Prefix: prefix, StartAfter: opts.StartAfter, Limit: opts.Limit})
}

// ErrNoStaging signals that the backend cannot support staging operations.
var ErrNoStaging = fmt.Errorf("storage: staging unsupported")
