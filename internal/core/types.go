package core

import (
	"context"
	"io"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
)

// AcquireCommand requests an exclusive lease on a key.
type AcquireCommand struct {
	Namespace        string
	Key              string
	Owner            string
	TTLSeconds       int64
	BlockSeconds     int64
	Idempotency      string
	ClientHint       string // optional client identity
	ForceQueryHidden bool   // mark meta as query-hidden when acquiring (for queue state keys)
}

// AcquireResult describes the lease returned by an AcquireCommand.
type AcquireResult struct {
	Namespace     string
	LeaseID       string
	Key           string
	Owner         string
	ExpiresAt     int64
	Version       int64
	StateETag     string
	FencingToken  int64
	RetryAfter    int64
	CorrelationID string
	GeneratedKey  bool
	MetaETag      string
	Meta          *storage.Meta
}

// KeepAliveCommand refreshes an active lease.
type KeepAliveCommand struct {
	Namespace     string
	Key           string
	LeaseID       string
	TTLSeconds    int64
	FencingToken  int64
	ClientHint    string
	Correlation   string
	KnownMeta     *storage.Meta
	KnownMetaETag string
}

// KeepAliveResult reports the refreshed lease state after a keepalive.
type KeepAliveResult struct {
	ExpiresAt    int64
	FencingToken int64
	MetaETag     string
	Meta         *storage.Meta
}

// ReleaseCommand relinquishes a lease.
type ReleaseCommand struct {
	Namespace     string
	Key           string
	LeaseID       string
	FencingToken  int64
	KnownMeta     *storage.Meta
	KnownMetaETag string
}

// ReleaseResult indicates whether the lease was released and metadata cleared.
type ReleaseResult struct {
	Released    bool
	MetaETag    string
	MetaCleared bool
}

// DescribeCommand fetches current lease/meta info.
type DescribeCommand struct {
	Namespace string
	Key       string
}

// DescribeResult returns the current lock state and metadata for a key.
type DescribeResult struct {
	Namespace string
	Key       string
	Owner     string
	LeaseID   string
	ExpiresAt int64
	Version   int64
	StateETag string
	UpdatedAt int64
	Metadata  map[string]any
	Fencing   int64
	Meta      *storage.Meta
}

// TTLConfig encodes default and max TTL constraints.
type TTLConfig struct {
	Default time.Duration
	Max     time.Duration
}

// GetCommand fetches state for a key, optionally requiring a lease.
type GetCommand struct {
	Namespace    string
	Key          string
	LeaseID      string
	FencingToken int64
	Public       bool
}

// GetResult contains the state reader and metadata returned from a GetCommand.
type GetResult struct {
	Meta             *storage.Meta
	Info             *storage.StateInfo
	Reader           io.ReadCloser
	PublishedVersion int64
	Public           bool
	NoContent        bool
}

// MetadataCommand mutates metadata using optimistic concurrency or lease checks.
type MetadataCommand struct {
	Namespace     string
	Key           string
	LeaseID       string
	FencingToken  int64
	Mutation      MetadataMutation
	KnownMeta     *storage.Meta
	KnownMetaETag string
	IfVersion     int64
	IfVersionSet  bool
	IfStateETag   string
}

// MetadataResult captures the new metadata state after mutation.
type MetadataResult struct {
	Version  int64
	Meta     *storage.Meta
	MetaETag string
}

// RemoveCommand deletes state and metadata when allowed by the provided guards.
type RemoveCommand struct {
	Namespace     string
	Key           string
	LeaseID       string
	FencingToken  int64
	KnownMeta     *storage.Meta
	KnownMetaETag string
	IfStateETag   string
	IfVersion     int64
	IfVersionSet  bool
}

// RemoveResult reports whether removal succeeded and the resulting metadata.
type RemoveResult struct {
	Removed    bool
	NewVersion int64
	Meta       *storage.Meta
	MetaETag   string
}

// QueryCommand drives namespace-scoped selector queries.
type QueryCommand struct {
	Namespace string
	Selector  api.Selector
	Limit     int
	Cursor    string
	Fields    map[string]any
	Engine    search.EngineHint
	Return    api.QueryReturn
	Refresh   RefreshMode
	Keys      []string // optional preselected keys to stream
}

// QueryResult contains keys and cursor information returned from a QueryCommand.
type QueryResult struct {
	Namespace string
	Keys      []string
	Cursor    string
	IndexSeq  uint64
	Metadata  map[string]string
}

// DocumentSink receives streaming documents for query return=documents.
type DocumentSink interface {
	OnDocument(ctx context.Context, namespace, key string, version int64, reader io.Reader) error
}

// apiQueryReturnDocuments is a small adapter constant to avoid importing api into core/query.
const apiQueryReturnDocuments = "documents"

// RefreshMode controls how search refresh semantics are applied to queries.
type RefreshMode string

const (
	// RefreshImmediate uses backend defaults for index refresh.
	RefreshImmediate RefreshMode = ""
	// RefreshWaitFor waits until indexes are refreshed before returning results.
	RefreshWaitFor RefreshMode = "wait_for"
)
