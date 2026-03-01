package api

// AcquireRequest models the JSON payload for POST /v1/acquire.
type AcquireRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Key identifies the lock/state key within the namespace.
	Key string `json:"key"`
	// TTLSeconds is a duration in seconds.
	TTLSeconds int64 `json:"ttl_seconds"`
	// Owner identifies the caller or worker owning the lease or queue delivery.
	Owner string `json:"owner"`
	// BlockSecs controls acquire wait behavior in seconds (-1 no-wait, 0 indefinite, >0 bounded wait).
	BlockSecs int64 `json:"block_seconds"`
	// IfNotExists enforces create-only semantics: fail when the key already exists.
	IfNotExists bool `json:"if_not_exists,omitempty"`
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id,omitempty"`
}

const (
	// BlockNoWait instructs the server to fail acquire requests immediately when a lease is held.
	BlockNoWait int64 = -1
)

// AcquireResponse is returned when a lease is granted.
type AcquireResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace"`
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string `json:"lease_id"`
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id,omitempty"`
	// Key identifies the lock/state key within the namespace.
	Key string `json:"key"`
	// Owner identifies the caller or worker owning the lease or queue delivery.
	Owner string `json:"owner"`
	// ExpiresAt is the lease expiry time as a Unix timestamp in seconds.
	ExpiresAt int64 `json:"expires_at_unix"`
	// Version is the lockd monotonic version for the target object.
	Version int64 `json:"version"`
	// StateETag is the state entity tag used for CAS semantics.
	StateETag string `json:"state_etag,omitempty"`
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken int64 `json:"fencing_token,omitempty"`
	// RetryAfter is the server hint (seconds) before retrying a contended acquire.
	RetryAfter int64 `json:"retry_after_seconds,omitempty"`
	// CorrelationID links related operations across request/response logs.
	CorrelationID string `json:"correlation_id,omitempty"`
}

// UpdateResponse captures metadata returned by POST /v1/update.
type UpdateResponse struct {
	// NewVersion is the updated monotonic version after a successful mutation.
	NewVersion int64 `json:"new_version"`
	// NewStateETag is the updated state entity tag after a successful mutation.
	NewStateETag string `json:"new_state_etag"`
	// Bytes is the number of state bytes written by the update.
	Bytes int64 `json:"bytes"`
	// Metadata carries metadata values returned by the server for this object.
	Metadata MetadataAttributes `json:"metadata,omitempty"`
}

// KeepAliveRequest represents POST /v1/keepalive.
type KeepAliveRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Key identifies the lock/state key within the namespace.
	Key string `json:"key"`
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string `json:"lease_id"`
	// TTLSeconds is a duration in seconds.
	TTLSeconds int64 `json:"ttl_seconds"`
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id,omitempty"`
}

// KeepAliveResponse acknowledges keepalive.
type KeepAliveResponse struct {
	// ExpiresAt is the refreshed lease expiry time as a Unix timestamp in seconds.
	ExpiresAt int64 `json:"expires_at_unix"`
}

// ReleaseRequest represents POST /v1/release.
type ReleaseRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Key identifies the lock/state key within the namespace.
	Key string `json:"key"`
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string `json:"lease_id"`
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id"`
	// Rollback requests rollback semantics instead of commit for lease release.
	Rollback bool `json:"rollback,omitempty"`
}

// ReleaseResponse indicates release status.
type ReleaseResponse struct {
	// Released reports whether the lease or TC lock release succeeded.
	Released bool `json:"released"`
}

// RemoveResponse is emitted by POST /v1/remove.
type RemoveResponse struct {
	// Removed reports whether the target state document was removed.
	Removed bool `json:"removed"`
	// NewVersion is the updated monotonic version after a successful mutation.
	NewVersion int64 `json:"new_version,omitempty"`
}

// DescribeResponse is returned from GET /v1/describe.
type DescribeResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace"`
	// Key identifies the lock/state key within the namespace.
	Key string `json:"key"`
	// Owner identifies the caller or worker owning the lease or queue delivery.
	Owner string `json:"owner,omitempty"`
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string `json:"lease_id,omitempty"`
	// ExpiresAt is the lease expiry time as a Unix timestamp in seconds when a lease is present.
	ExpiresAt int64 `json:"expires_at_unix,omitempty"`
	// Version is the lockd monotonic version for the target object.
	Version int64 `json:"version"`
	// StateETag is the state entity tag used for CAS semantics.
	StateETag string `json:"state_etag,omitempty"`
	// UpdatedAt is the last committed state update time as a Unix timestamp in seconds.
	UpdatedAt int64 `json:"updated_at_unix,omitempty"`
	// Metadata carries metadata values returned by the server for this object.
	Metadata MetadataAttributes `json:"metadata,omitempty"`
}

// ErrorResponse is the canonical error envelope for API errors.
type ErrorResponse struct {
	// ErrorCode is the stable lockd error identifier.
	ErrorCode string `json:"error"`
	// Detail provides human-readable diagnostic context for the error.
	Detail string `json:"detail,omitempty"`
	// LeaderEndpoint identifies the active leader endpoint to retry against when applicable.
	LeaderEndpoint string `json:"leader_endpoint,omitempty"`
	// CurrentVersion returns the server's current version for conflict diagnostics.
	CurrentVersion int64 `json:"current_version,omitempty"`
	// CurrentETag returns the server's current ETag for conflict diagnostics.
	CurrentETag string `json:"current_etag,omitempty"`
	// RetryAfterSeconds is the server-provided retry hint in seconds.
	RetryAfterSeconds int64 `json:"retry_after_seconds,omitempty"`
}

// MetadataAttributes captures lock metadata exposed to clients.
type MetadataAttributes struct {
	// QueryHidden controls whether the key is hidden from selector/query results.
	QueryHidden *bool `json:"query_hidden,omitempty"`
}

// MetadataUpdateResponse acknowledges metadata mutations.
type MetadataUpdateResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Key identifies the lock/state key within the namespace.
	Key string `json:"key,omitempty"`
	// Version is the lockd monotonic version for the target object.
	Version int64 `json:"version"`
	// Metadata carries metadata values returned by the server for this object.
	Metadata MetadataAttributes `json:"metadata,omitempty"`
}

// NamespaceQueryConfig describes the query engine preferences for a namespace.
type NamespaceQueryConfig struct {
	// PreferredEngine selects the primary query execution strategy for the namespace.
	PreferredEngine string `json:"preferred_engine"`
	// FallbackEngine selects the fallback strategy when the preferred engine cannot satisfy the request.
	FallbackEngine string `json:"fallback_engine"`
}

// NamespaceConfigRequest mutates namespace-level settings.
type NamespaceConfigRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Query contains namespace query-engine configuration settings.
	Query *NamespaceQueryConfig `json:"query,omitempty"`
}

// NamespaceConfigResponse surfaces namespace-level settings.
type NamespaceConfigResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace"`
	// Query contains namespace query-engine configuration settings.
	Query NamespaceQueryConfig `json:"query"`
}

// EnqueueRequest represents POST /v1/queue/enqueue.
type EnqueueRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue,omitempty"`
	// DelaySeconds postpones message visibility by the specified number of seconds.
	DelaySeconds int64 `json:"delay_seconds,omitempty"`
	// VisibilityTimeoutSeconds controls how long a leased message remains invisible, in seconds.
	VisibilityTimeoutSeconds int64 `json:"visibility_timeout_seconds,omitempty"`
	// TTLSeconds is a duration in seconds.
	TTLSeconds int64 `json:"ttl_seconds,omitempty"`
	// MaxAttempts caps how many failed attempts are allowed before terminal handling.
	MaxAttempts int `json:"max_attempts,omitempty"`
	// Attributes carries arbitrary JSON metadata associated with the message or request.
	Attributes map[string]any `json:"attributes,omitempty"`
	// PayloadContentType is the media type for the queue payload.
	PayloadContentType string `json:"payload_content_type,omitempty"`
}

// EnqueueResponse surfaces details of the enqueued message.
type EnqueueResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue"`
	// MessageID uniquely identifies a queue message.
	MessageID string `json:"message_id"`
	// Attempts is the number of delivery attempts observed so far.
	Attempts int `json:"attempts"`
	// MaxAttempts caps how many failed attempts are allowed before terminal handling.
	MaxAttempts int `json:"max_attempts"`
	// FailureAttempts is the number of failed attempts observed so far.
	FailureAttempts int `json:"failure_attempts,omitempty"`
	// NotVisibleUntilUnix is when the message becomes visible for delivery again, as Unix seconds.
	NotVisibleUntilUnix int64 `json:"not_visible_until_unix"`
	// VisibilityTimeoutSeconds controls how long a leased message remains invisible, in seconds.
	VisibilityTimeoutSeconds int64 `json:"visibility_timeout_seconds"`
	// PayloadBytes is the payload size in bytes.
	PayloadBytes int64 `json:"payload_bytes"`
	// CorrelationID links related operations across request/response logs.
	CorrelationID string `json:"correlation_id,omitempty"`
}

// DequeueRequest drives POST /v1/queue/dequeue and /v1/queue/subscribe.
type DequeueRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue,omitempty"`
	// Owner identifies the caller or worker owning the lease or queue delivery.
	Owner string `json:"owner,omitempty"`
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id,omitempty"`
	// VisibilityTimeoutSeconds controls how long a leased message remains invisible, in seconds.
	VisibilityTimeoutSeconds int64 `json:"visibility_timeout_seconds,omitempty"`
	// WaitSeconds controls long-poll wait time in seconds for dequeue/subscribe calls.
	WaitSeconds int64 `json:"wait_seconds,omitempty"`
	// PageSize limits the number of items returned in one response.
	PageSize int `json:"page_size,omitempty"`
	// StartAfter is the server-issued cursor for continuing from a prior position.
	StartAfter string `json:"start_after,omitempty"`
}

// QueueWatchRequest drives POST /v1/queue/watch.
type QueueWatchRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue,omitempty"`
}

// QueueStatsRequest drives POST /v1/queue/stats.
type QueueStatsRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue,omitempty"`
}

// QueueStatsResponse captures side-effect-free runtime and head snapshot metrics for one queue.
type QueueStatsResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue"`
	// WaitingConsumers is the number of active consumers currently blocked waiting for work on this queue.
	WaitingConsumers int `json:"waiting_consumers"`
	// PendingCandidates is the number of ready candidates currently buffered for this queue by the dispatcher.
	PendingCandidates int `json:"pending_candidates"`
	// TotalConsumers is the number of active consumers across all queues for this server process.
	TotalConsumers int `json:"total_consumers"`
	// HasActiveWatcher indicates whether the dispatcher currently maintains a live watcher for this queue.
	HasActiveWatcher bool `json:"has_active_watcher"`
	// Available reports whether a visible head message is currently available.
	Available bool `json:"available"`
	// HeadMessageID is the current visible head message identifier when available.
	HeadMessageID string `json:"head_message_id,omitempty"`
	// HeadEnqueuedAtUnix is when the visible head message was originally enqueued, as Unix seconds.
	HeadEnqueuedAtUnix int64 `json:"head_enqueued_at_unix,omitempty"`
	// HeadNotVisibleUntilUnix is when the head message became visible, as Unix seconds.
	HeadNotVisibleUntilUnix int64 `json:"head_not_visible_until_unix,omitempty"`
	// HeadAgeSeconds is the age of the visible head message in seconds.
	HeadAgeSeconds int64 `json:"head_age_seconds,omitempty"`
	// CorrelationID links related operations across request/response logs.
	CorrelationID string `json:"correlation_id,omitempty"`
}

// QueueWatchEvent is emitted over SSE by /v1/queue/watch when queue visibility changes.
type QueueWatchEvent struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue"`
	// Available reports whether at least one visible message is currently available.
	Available bool `json:"available"`
	// HeadMessageID is the current visible head message identifier when available.
	HeadMessageID string `json:"head_message_id,omitempty"`
	// ChangedAtUnix is when the server observed this queue state, as Unix seconds.
	ChangedAtUnix int64 `json:"changed_at_unix"`
	// CorrelationID links related operations across request/response logs.
	CorrelationID string `json:"correlation_id,omitempty"`
}

// Message carries message metadata and payload delivery info.
type Message struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue"`
	// MessageID uniquely identifies a queue message.
	MessageID string `json:"message_id"`
	// Attempts is the number of delivery attempts observed so far.
	Attempts int `json:"attempts"`
	// MaxAttempts caps how many failed attempts are allowed before terminal handling.
	MaxAttempts int `json:"max_attempts"`
	// FailureAttempts is the number of failed attempts observed so far.
	FailureAttempts int `json:"failure_attempts,omitempty"`
	// NotVisibleUntilUnix is when the message becomes visible again, as a Unix timestamp in seconds.
	NotVisibleUntilUnix int64 `json:"not_visible_until_unix"`
	// VisibilityTimeoutSeconds controls how long a leased message remains invisible, in seconds.
	VisibilityTimeoutSeconds int64 `json:"visibility_timeout_seconds"`
	// Attributes carries arbitrary JSON metadata associated with the message or request.
	Attributes map[string]any `json:"attributes,omitempty"`
	// PayloadContentType is the media type for the queue payload.
	PayloadContentType string `json:"payload_content_type,omitempty"`
	// PayloadBytes is the payload size in bytes.
	PayloadBytes int64 `json:"payload_bytes"`
	// CorrelationID links related operations across request/response logs.
	CorrelationID string `json:"correlation_id,omitempty"`
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string `json:"lease_id"`
	// LeaseExpiresAtUnix is when the message lease expires, as a Unix timestamp in seconds.
	LeaseExpiresAtUnix int64 `json:"lease_expires_at_unix"`
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken int64 `json:"fencing_token"`
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id,omitempty"`
	// MetaETag is the queue metadata entity tag used for CAS semantics.
	MetaETag string `json:"meta_etag"`
	// StateETag is the state entity tag used for CAS semantics.
	StateETag string `json:"state_etag,omitempty"`
	// StateLeaseID identifies the associated workflow state lease.
	StateLeaseID string `json:"state_lease_id,omitempty"`
	// StateLeaseExpiresAtUnix is when the associated state lease expires, as a Unix timestamp in seconds.
	StateLeaseExpiresAtUnix int64 `json:"state_lease_expires_at_unix,omitempty"`
	// StateFencingToken is the fencing token for the associated workflow state lease.
	StateFencingToken int64 `json:"state_fencing_token,omitempty"`
	// StateTxnID is the transaction identifier attached to the state lease when present.
	StateTxnID string `json:"state_txn_id,omitempty"`
}

// DequeueResponse delivers a message and associated cursors.
type DequeueResponse struct {
	// Message carries the dequeued queue message envelope when available.
	Message *Message `json:"message,omitempty"`
	// NextCursor is the server-issued cursor for subsequent pagination.
	NextCursor string `json:"next_cursor,omitempty"`
}

// AckRequest acknowledges a processed message.
type AckRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue"`
	// MessageID uniquely identifies a queue message.
	MessageID string `json:"message_id"`
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string `json:"lease_id"`
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id,omitempty"`
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken int64 `json:"fencing_token"`
	// MetaETag is the queue metadata entity tag used for CAS semantics.
	MetaETag string `json:"meta_etag"`
	// StateETag is the state entity tag used for CAS semantics.
	StateETag string `json:"state_etag,omitempty"`
	// StateLeaseID identifies the associated workflow state lease.
	StateLeaseID string `json:"state_lease_id,omitempty"`
	// StateFencingToken is the fencing token for the associated workflow state lease.
	StateFencingToken int64 `json:"state_fencing_token,omitempty"`
}

// AckResponse reports acknowledgement status.
type AckResponse struct {
	// Acked maps to the "acked" JSON field.
	Acked bool `json:"acked"`
	// CorrelationID links related operations across request/response logs.
	CorrelationID string `json:"correlation_id,omitempty"`
}

// NackIntent describes how a negative acknowledgement should be accounted.
type NackIntent string

const (
	// NackIntentFailure records a failed processing attempt.
	NackIntentFailure NackIntent = "failure"
	// NackIntentDefer requeues intentionally without consuming failure budget.
	NackIntentDefer NackIntent = "defer"
)

// NackRequest re-queues a message with optional delay.
type NackRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue"`
	// MessageID uniquely identifies a queue message.
	MessageID string `json:"message_id"`
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string `json:"lease_id"`
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id,omitempty"`
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken int64 `json:"fencing_token"`
	// MetaETag is the queue metadata entity tag used for CAS semantics.
	MetaETag string `json:"meta_etag"`
	// StateETag is the state entity tag used for CAS semantics.
	StateETag string `json:"state_etag,omitempty"`
	// DelaySeconds postpones message visibility by the specified number of seconds.
	DelaySeconds int64 `json:"delay_seconds,omitempty"`
	// Intent controls whether the requeue counts against max_attempts.
	Intent NackIntent `json:"intent,omitempty"`
	// LastError carries optional error metadata persisted for intent=failure.
	LastError any `json:"last_error,omitempty"`
	// StateLeaseID identifies the associated workflow state lease.
	StateLeaseID string `json:"state_lease_id,omitempty"`
	// StateFencingToken is the fencing token for the associated workflow state lease.
	StateFencingToken int64 `json:"state_fencing_token,omitempty"`
}

// NackResponse confirms nack handling.
type NackResponse struct {
	// Requeued maps to the "requeued" JSON field.
	Requeued bool `json:"requeued"`
	// MetaETag is the queue metadata entity tag used for CAS semantics.
	MetaETag string `json:"meta_etag"`
	// CorrelationID links related operations across request/response logs.
	CorrelationID string `json:"correlation_id,omitempty"`
}

// ExtendRequest keeps a lease alive and extends visibility.
type ExtendRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Queue is the queue name targeted by the operation.
	Queue string `json:"queue"`
	// MessageID uniquely identifies a queue message.
	MessageID string `json:"message_id"`
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string `json:"lease_id"`
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id,omitempty"`
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken int64 `json:"fencing_token"`
	// MetaETag is the queue metadata entity tag used for CAS semantics.
	MetaETag string `json:"meta_etag"`
	// ExtendBySeconds is the requested visibility extension duration in seconds.
	ExtendBySeconds int64 `json:"extend_by_seconds,omitempty"`
	// StateLeaseID identifies the associated workflow state lease.
	StateLeaseID string `json:"state_lease_id,omitempty"`
	// StateFencingToken is the fencing token for the associated workflow state lease.
	StateFencingToken int64 `json:"state_fencing_token,omitempty"`
}

// ExtendResponse captures the refreshed visibility/lease info.
type ExtendResponse struct {
	// LeaseExpiresAtUnix is the refreshed message lease expiry as a Unix timestamp in seconds.
	LeaseExpiresAtUnix int64 `json:"lease_expires_at_unix"`
	// VisibilityTimeoutSeconds controls how long a leased message remains invisible, in seconds.
	VisibilityTimeoutSeconds int64 `json:"visibility_timeout_seconds"`
	// MetaETag is the queue metadata entity tag used for CAS semantics.
	MetaETag string `json:"meta_etag"`
	// StateLeaseExpiresAtUnix is the refreshed state lease expiry as a Unix timestamp in seconds when state is attached.
	StateLeaseExpiresAtUnix int64 `json:"state_lease_expires_at_unix,omitempty"`
	// CorrelationID links related operations across request/response logs.
	CorrelationID string `json:"correlation_id,omitempty"`
}
