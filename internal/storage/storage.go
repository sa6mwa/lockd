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
	ContentTypeTextEncrypted        = "application/vnd.lockd.text+encrypted"
)

// ErrNotFound indicates the requested key or resource is missing.
var (
	ErrNotFound       = errors.New("storage: not found")
	ErrCASMismatch    = errors.New("storage: cas mismatch")
	ErrNotImplemented = errors.New("storage: not implemented")
)

// Meta encapsulates per-key metadata persisted by backends.
type Meta struct {
	Lease               *Lease            `json:"lease,omitempty"`
	Version             int64             `json:"version"`
	PublishedVersion    int64             `json:"published_version,omitempty"`
	StateETag           string            `json:"state_etag,omitempty"`
	UpdatedAtUnix       int64             `json:"updated_at_unix,omitempty"`
	FencingToken        int64             `json:"fencing_token,omitempty"`
	StateDescriptor     []byte            `json:"state_descriptor,omitempty"`
	StatePlaintextBytes int64             `json:"state_plaintext_bytes,omitempty"`
	Attributes          map[string]string `json:"attributes,omitempty"`
	Attachments         []Attachment      `json:"attachments,omitempty"`
	// Staged* capture pending transactional changes that will be committed or
	// rolled back by Release.
	StagedTxnID               string             `json:"staged_txn_id,omitempty"`
	StagedVersion             int64              `json:"staged_version,omitempty"`
	StagedStateETag           string             `json:"staged_state_etag,omitempty"`
	StagedStateDescriptor     []byte             `json:"staged_state_descriptor,omitempty"`
	StagedStatePlaintextBytes int64              `json:"staged_state_plaintext_bytes,omitempty"`
	StagedAttributes          map[string]string  `json:"staged_attributes,omitempty"`
	StagedRemove              bool               `json:"staged_remove,omitempty"`
	StagedAttachments         []StagedAttachment `json:"staged_attachments,omitempty"`
	StagedAttachmentDeletes   []string           `json:"staged_attachment_deletes,omitempty"`
	StagedAttachmentsClear    bool               `json:"staged_attachments_clear,omitempty"`
}

// MetaRecord pairs metadata with its ETag for backends that persist them together.
type MetaRecord struct {
	ETag string
	Meta *Meta
}

// Lease captures the server-side view of an active lease.
type Lease struct {
	ID            string `json:"lease_id"`
	Owner         string `json:"owner"`
	ExpiresAtUnix int64  `json:"expires_at_unix"`
	FencingToken  int64  `json:"fencing_token,omitempty"`
	TxnID         string `json:"txn_id,omitempty"`
	TxnExplicit   bool   `json:"txn_explicit,omitempty"`
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

// Attachment captures metadata about a stored attachment object.
type Attachment struct {
	ID              string `json:"id,omitempty"`
	Name            string `json:"name,omitempty"`
	Size            int64  `json:"size,omitempty"`
	PlaintextBytes  int64  `json:"plaintext_bytes,omitempty"`
	PlaintextSHA256 string `json:"plaintext_sha256,omitempty"`
	ContentType     string `json:"content_type,omitempty"`
	Descriptor      []byte `json:"descriptor,omitempty"`
	CreatedAtUnix   int64  `json:"created_at_unix,omitempty"`
	UpdatedAtUnix   int64  `json:"updated_at_unix,omitempty"`
}

// StagedAttachment describes a pending attachment payload staged for a txn.
type StagedAttachment struct {
	ID               string `json:"id,omitempty"`
	Name             string `json:"name,omitempty"`
	Size             int64  `json:"size,omitempty"`
	PlaintextBytes   int64  `json:"plaintext_bytes,omitempty"`
	PlaintextSHA256  string `json:"plaintext_sha256,omitempty"`
	ContentType      string `json:"content_type,omitempty"`
	StagedDescriptor []byte `json:"staged_descriptor,omitempty"`
	CreatedAtUnix    int64  `json:"created_at_unix,omitempty"`
	UpdatedAtUnix    int64  `json:"updated_at_unix,omitempty"`
}

// PutStateOptions controls the behaviour of state writes.
type PutStateOptions struct {
	// ExpectedETag enables CAS semantics. When empty, no CAS is enforced.
	ExpectedETag string
	// TempSuffix allows callers to hint the suffix used for temp objects.
	TempSuffix string
	Descriptor []byte
	// IfNotExists enforces creation-only semantics when true. Ignored when
	// ExpectedETag is provided.
	IfNotExists bool
}

// PutStateResult describes the outcome of PutState.
type PutStateResult struct {
	BytesWritten int64
	NewETag      string
	Descriptor   []byte
}

// LoadMetaResult captures the metadata payload and its ETag.
type LoadMetaResult struct {
	Meta *Meta
	ETag string
}

// ReadStateResult captures a state reader with its metadata.
type ReadStateResult struct {
	Reader io.ReadCloser
	Info   *StateInfo
}

// Backend defines the storage contract expected by the server.
type Backend interface {
	// LoadMeta returns the current meta document and its opaque ETag.
	LoadMeta(ctx context.Context, namespace, key string) (LoadMetaResult, error)
	// StoreMeta atomically writes meta if the existing ETag matches. Use empty
	// expectedETag to create brand new entries.
	StoreMeta(ctx context.Context, namespace, key string, meta *Meta, expectedETag string) (newETag string, err error)
	// DeleteMeta removes metadata entirely, used when cleaning keys.
	DeleteMeta(ctx context.Context, namespace, key string, expectedETag string) error
	// ListMetaKeys enumerates all known metadata keys.
	ListMetaKeys(ctx context.Context, namespace string) ([]string, error)

	// ReadState streams the JSON state blob with metadata.
	ReadState(ctx context.Context, namespace, key string) (ReadStateResult, error)
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
	GetObject(ctx context.Context, namespace, key string) (GetObjectResult, error)
	// PutObject writes a blob to the provided key, applying conditional
	// semantics when opts.ExpectedETag or opts.IfNotExists are set.
	PutObject(ctx context.Context, namespace, key string, body io.Reader, opts PutObjectOptions) (*ObjectInfo, error)
	// DeleteObject removes the object identified by key, optionally enforcing a
	// matching ETag when opts.ExpectedETag is set.
	DeleteObject(ctx context.Context, namespace, key string, opts DeleteObjectOptions) error

	// BackendHash returns the stable identity hash for this backend.
	BackendHash(ctx context.Context) (string, error)

	// Close releases backend resources.
	Close() error
}

// NamespaceLister reports all namespaces stored in the backend, when supported.
type NamespaceLister interface {
	ListNamespaces(ctx context.Context) ([]string, error)
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

// CopyObjectOptions controls conditional semantics for object copy operations.
type CopyObjectOptions struct {
	ExpectedETag string
	IfNotExists  bool
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
	QueueWatchStatus() QueueWatchStatus
}

// QueueWatchStatus reports whether queue change notifications are active.
type QueueWatchStatus struct {
	Enabled bool
	Mode    string
	Reason  string
}

// SingleWriterControl allows callers to enable single-writer optimizations when
// the backend is exclusively owned by one server.
type SingleWriterControl interface {
	SetSingleWriter(enabled bool)
}

// ConcurrentWriteSupport reports whether the backend supports multiple writers
// against the same backend root without risking corruption.
type ConcurrentWriteSupport interface {
	SupportsConcurrentWrites() bool
}

// GetObjectResult captures an object reader with its metadata.
type GetObjectResult struct {
	Reader io.ReadCloser
	Info   *ObjectInfo
}

// ObjectCopier indicates the backend can copy objects server-side.
type ObjectCopier interface {
	CopyObject(ctx context.Context, namespace, srcKey, dstKey string, opts CopyObjectOptions) (*ObjectInfo, error)
}

// IndexerDefaultsProvider allows storage backends to tune writer flush behaviour
// based on underlying consistency and latency characteristics.
type IndexerDefaultsProvider interface {
	IndexerFlushDefaults() (flushDocs int, flushInterval time.Duration)
}
