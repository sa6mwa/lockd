package storage

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound indicates the requested key or resource is missing.
var (
	ErrNotFound       = errors.New("storage: not found")
	ErrCASMismatch    = errors.New("storage: cas mismatch")
	ErrNotImplemented = errors.New("storage: not implemented")
)

// Meta encapsulates per-key metadata persisted by backends.
type Meta struct {
	Lease         *Lease `json:"lease,omitempty"`
	Version       int64  `json:"version"`
	StateETag     string `json:"state_etag,omitempty"`
	UpdatedAtUnix int64  `json:"updated_at_unix,omitempty"`
}

// Lease captures the server-side view of an active lease.
type Lease struct {
	ID            string `json:"lease_id"`
	Owner         string `json:"owner"`
	ExpiresAtUnix int64  `json:"expires_at_unix"`
}

// StateInfo provides metadata about a stored state blob.
type StateInfo struct {
	Size       int64
	ETag       string
	Version    int64
	ModifiedAt int64
}

// PutStateOptions controls the behaviour of state writes.
type PutStateOptions struct {
	// ExpectedETag enables CAS semantics. When empty, no CAS is enforced.
	ExpectedETag string
	// TempSuffix allows callers to hint the suffix used for temp objects.
	TempSuffix string
}

// PutStateResult describes the outcome of PutState.
type PutStateResult struct {
	BytesWritten int64
	NewETag      string
}

// Backend defines the storage contract expected by the server.
type Backend interface {
	// LoadMeta returns the current meta document and its opaque ETag.
	LoadMeta(ctx context.Context, key string) (*Meta, string, error)
	// StoreMeta atomically writes meta if the existing ETag matches. Use empty
	// expectedETag to create brand new entries.
	StoreMeta(ctx context.Context, key string, meta *Meta, expectedETag string) (newETag string, err error)
	// DeleteMeta removes metadata entirely, used when cleaning keys.
	DeleteMeta(ctx context.Context, key string, expectedETag string) error
	// ListMetaKeys enumerates all known metadata keys.
	ListMetaKeys(ctx context.Context) ([]string, error)

	// ReadState streams the JSON state blob with metadata.
	ReadState(ctx context.Context, key string) (io.ReadCloser, *StateInfo, error)
	// WriteState uploads a new state blob with optional CAS on the previous ETag.
	WriteState(ctx context.Context, key string, body io.Reader, opts PutStateOptions) (*PutStateResult, error)
	// RemoveState deletes stored state if present.
	RemoveState(ctx context.Context, key string, expectedETag string) error

	// Close releases backend resources.
	Close() error
}
