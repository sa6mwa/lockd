package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

// Content type constants used for metadata and payload blobs across backends.
const (
	ContentTypeJSON                 = "application/json"
	ContentTypeJSONEncrypted        = "application/vnd.lockd+json-encrypted"
	ContentTypeProtobuf             = "application/x-protobuf"
	ContentTypeProtobufEncrypted    = "application/vnd.lockd+protobuf-encrypted"
	ContentTypeOctetStream          = "application/octet-stream"
	ContentTypeOctetStreamEncrypted = "application/vnd.lockd.octet-stream+encrypted"
)

// ErrNotFound indicates the requested key or resource is missing.
var (
	ErrNotFound       = errors.New("storage: not found")
	ErrCASMismatch    = errors.New("storage: cas mismatch")
	ErrNotImplemented = errors.New("storage: not implemented")
)

// Meta encapsulates per-key metadata persisted by backends.
type Meta struct {
	Lease               *Lease `json:"lease,omitempty"`
	Version             int64  `json:"version"`
	StateETag           string `json:"state_etag,omitempty"`
	UpdatedAtUnix       int64  `json:"updated_at_unix,omitempty"`
	FencingToken        int64  `json:"fencing_token,omitempty"`
	StateDescriptor     []byte `json:"state_descriptor,omitempty"`
	StatePlaintextBytes int64  `json:"state_plaintext_bytes,omitempty"`
}

// Lease captures the server-side view of an active lease.
type Lease struct {
	ID            string `json:"lease_id"`
	Owner         string `json:"owner"`
	ExpiresAtUnix int64  `json:"expires_at_unix"`
	FencingToken  int64  `json:"fencing_token,omitempty"`
}

// StateInfo provides metadata about a stored state blob.
type StateInfo struct {
	Size       int64
	CipherSize int64
	ETag       string
	Version    int64
	ModifiedAt int64
	Descriptor []byte
}

// PutStateOptions controls the behaviour of state writes.
type PutStateOptions struct {
	// ExpectedETag enables CAS semantics. When empty, no CAS is enforced.
	ExpectedETag string
	// TempSuffix allows callers to hint the suffix used for temp objects.
	TempSuffix string
	Descriptor []byte
}

// PutStateResult describes the outcome of PutState.
type PutStateResult struct {
	BytesWritten int64
	NewETag      string
	Descriptor   []byte
}

// Backend defines the storage contract expected by the server.
type Backend interface {
	// LoadMeta returns the current meta document and its opaque ETag.
	LoadMeta(ctx context.Context, namespace, key string) (*Meta, string, error)
	// StoreMeta atomically writes meta if the existing ETag matches. Use empty
	// expectedETag to create brand new entries.
	StoreMeta(ctx context.Context, namespace, key string, meta *Meta, expectedETag string) (newETag string, err error)
	// DeleteMeta removes metadata entirely, used when cleaning keys.
	DeleteMeta(ctx context.Context, namespace, key string, expectedETag string) error
	// ListMetaKeys enumerates all known metadata keys.
	ListMetaKeys(ctx context.Context, namespace string) ([]string, error)

	// ReadState streams the JSON state blob with metadata.
	ReadState(ctx context.Context, namespace, key string) (io.ReadCloser, *StateInfo, error)
	// WriteState uploads a new state blob with optional CAS on the previous ETag.
	WriteState(ctx context.Context, namespace, key string, body io.Reader, opts PutStateOptions) (*PutStateResult, error)
	// Remove deletes stored state if present.
	Remove(ctx context.Context, namespace, key string, expectedETag string) error

	// ListObjects enumerates objects under the supplied prefix in ascending
	// lexical order within the namespace. Results are limited by opts.Limit when >0 and resume from
	// opts.StartAfter when provided.
	ListObjects(ctx context.Context, namespace string, opts ListOptions) (*ListResult, error)
	// GetObject fetches the raw bytes for key and returns a reader alongside
	// metadata. Callers must close the returned reader.
	GetObject(ctx context.Context, namespace, key string) (io.ReadCloser, *ObjectInfo, error)
	// PutObject writes a blob to the provided key, applying conditional
	// semantics when opts.ExpectedETag or opts.IfNotExists are set.
	PutObject(ctx context.Context, namespace, key string, body io.Reader, opts PutObjectOptions) (*ObjectInfo, error)
	// DeleteObject removes the object identified by key, optionally enforcing a
	// matching ETag when opts.ExpectedETag is set.
	DeleteObject(ctx context.Context, namespace, key string, opts DeleteObjectOptions) error

	// Close releases backend resources.
	Close() error
}

type transientError struct {
	err error
}

func (t transientError) Error() string { return t.err.Error() }
func (t transientError) Unwrap() error { return t.err }

// NewTransientError marks err as retryable.
func NewTransientError(err error) error {
	if err == nil {
		return nil
	}
	return transientError{err: err}
}

// IsTransient reports whether err was marked as retryable.
func IsTransient(err error) bool {
	var te transientError
	return errors.As(err, &te)
}

// ObjectInfo captures metadata exposed by object-oriented backends.
type ObjectInfo struct {
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
	ContentType  string
	Descriptor   []byte
}

// PutObjectOptions controls conditional semantics and metadata for PutObject.
type PutObjectOptions struct {
	ExpectedETag string
	IfNotExists  bool
	ContentType  string
	Descriptor   []byte
}

// DeleteObjectOptions controls conditional semantics for DeleteObject.
type DeleteObjectOptions struct {
	ExpectedETag   string
	IgnoreNotFound bool
}

// ListOptions guides ListObjects traversal.
type ListOptions struct {
	Prefix     string
	StartAfter string
	Limit      int
}

// ListResult captures the outcome of a ListObjects call.
type ListResult struct {
	Objects        []ObjectInfo
	NextStartAfter string
	Truncated      bool
}

// QueueChangeSubscription receives notifications when queue objects change.
type QueueChangeSubscription interface {
	Events() <-chan struct{}
	Close() error
}

// QueueChangeFeed indicates the backend can emit change notifications for queue prefixes.
type QueueChangeFeed interface {
	SubscribeQueueChanges(namespace, queue string) (QueueChangeSubscription, error)
}

// QueueWatchStatusProvider reports whether filesystem-level queue change
// notifications are active (e.g. inotify/fsnotify) and why they may be
// unavailable.
type QueueWatchStatusProvider interface {
	QueueWatchStatus() (enabled bool, mode string, reason string)
}
