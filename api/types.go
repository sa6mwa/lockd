package api

// AcquireRequest models the JSON payload for POST /v1/acquire.
type AcquireRequest struct {
	Key         string `json:"key"`
	TTLSeconds  int64  `json:"ttl_seconds"`
	Owner       string `json:"owner"`
	BlockSecs   int64  `json:"block_seconds"`
	Idempotency string `json:"-"`
}

const (
	// BlockNoWait instructs the server to fail acquire requests immediately when a lease is held.
	BlockNoWait int64 = -1
)

// AcquireResponse is returned when a lease is granted.
type AcquireResponse struct {
	LeaseID       string `json:"lease_id"`
	Key           string `json:"key"`
	Owner         string `json:"owner"`
	ExpiresAt     int64  `json:"expires_at_unix"`
	Version       int64  `json:"version"`
	StateETag     string `json:"state_etag,omitempty"`
	FencingToken  int64  `json:"fencing_token,omitempty"`
	RetryAfter    int64  `json:"retry_after_seconds,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// KeepAliveRequest represents POST /v1/keepalive.
type KeepAliveRequest struct {
	Key        string `json:"key"`
	LeaseID    string `json:"lease_id"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

// KeepAliveResponse acknowledges keepalive.
type KeepAliveResponse struct {
	ExpiresAt int64 `json:"expires_at_unix"`
}

// ReleaseRequest represents POST /v1/release.
type ReleaseRequest struct {
	Key     string `json:"key"`
	LeaseID string `json:"lease_id"`
}

// ReleaseResponse indicates release status.
type ReleaseResponse struct {
	Released bool `json:"released"`
}

// RemoveStateResponse is emitted by POST /v1/remove-state.
type RemoveStateResponse struct {
	Removed    bool  `json:"removed"`
	NewVersion int64 `json:"new_version,omitempty"`
}

// DescribeResponse is returned from GET /v1/describe.
type DescribeResponse struct {
	Key       string `json:"key"`
	Owner     string `json:"owner,omitempty"`
	LeaseID   string `json:"lease_id,omitempty"`
	ExpiresAt int64  `json:"expires_at_unix,omitempty"`
	Version   int64  `json:"version"`
	StateETag string `json:"state_etag,omitempty"`
	UpdatedAt int64  `json:"updated_at_unix,omitempty"`
}

// ErrorResponse is the canonical error envelope for API errors.
type ErrorResponse struct {
	ErrorCode         string `json:"error"`
	Detail            string `json:"detail,omitempty"`
	CurrentVersion    int64  `json:"current_version,omitempty"`
	CurrentETag       string `json:"current_etag,omitempty"`
	RetryAfterSeconds int64  `json:"retry_after_seconds,omitempty"`
}

// EnqueueRequest represents POST /v1/queue/enqueue.
type EnqueueRequest struct {
	Queue                    string         `json:"queue,omitempty"`
	DelaySeconds             int64          `json:"delay_seconds,omitempty"`
	VisibilityTimeoutSeconds int64          `json:"visibility_timeout_seconds,omitempty"`
	TTLSeconds               int64          `json:"ttl_seconds,omitempty"`
	MaxAttempts              int            `json:"max_attempts,omitempty"`
	Attributes               map[string]any `json:"attributes,omitempty"`
	PayloadContentType       string         `json:"payload_content_type,omitempty"`
	IdempotencyKey           string         `json:"-"`
}

// EnqueueResponse surfaces details of the enqueued message.
type EnqueueResponse struct {
	Queue                    string `json:"queue"`
	MessageID                string `json:"message_id"`
	Attempts                 int    `json:"attempts"`
	MaxAttempts              int    `json:"max_attempts"`
	NotVisibleUntilUnix      int64  `json:"not_visible_until_unix"`
	VisibilityTimeoutSeconds int64  `json:"visibility_timeout_seconds"`
	PayloadBytes             int64  `json:"payload_bytes"`
	CorrelationID            string `json:"correlation_id,omitempty"`
}

// DequeueRequest drives POST /v1/queue/dequeue and /v1/queue/subscribe.
type DequeueRequest struct {
	Queue                    string `json:"queue,omitempty"`
	Owner                    string `json:"owner,omitempty"`
	VisibilityTimeoutSeconds int64  `json:"visibility_timeout_seconds,omitempty"`
	WaitSeconds              int64  `json:"wait_seconds,omitempty"`
	PageSize                 int    `json:"page_size,omitempty"`
	StartAfter               string `json:"start_after,omitempty"`
}

// Message carries message metadata and payload delivery info.
type Message struct {
	Queue                    string         `json:"queue"`
	MessageID                string         `json:"message_id"`
	Attempts                 int            `json:"attempts"`
	MaxAttempts              int            `json:"max_attempts"`
	NotVisibleUntilUnix      int64          `json:"not_visible_until_unix"`
	VisibilityTimeoutSeconds int64          `json:"visibility_timeout_seconds"`
	Attributes               map[string]any `json:"attributes,omitempty"`
	PayloadContentType       string         `json:"payload_content_type,omitempty"`
	PayloadBytes             int64          `json:"payload_bytes"`
	CorrelationID            string         `json:"correlation_id,omitempty"`
	LeaseID                  string         `json:"lease_id"`
	LeaseExpiresAtUnix       int64          `json:"lease_expires_at_unix"`
	FencingToken             int64          `json:"fencing_token"`
	MetaETag                 string         `json:"meta_etag"`
	StateETag                string         `json:"state_etag,omitempty"`
	StateLeaseID             string         `json:"state_lease_id,omitempty"`
	StateLeaseExpiresAtUnix  int64          `json:"state_lease_expires_at_unix,omitempty"`
	StateFencingToken        int64          `json:"state_fencing_token,omitempty"`
}

// DequeueResponse delivers a message and associated cursors.
type DequeueResponse struct {
	Message    *Message `json:"message,omitempty"`
	NextCursor string   `json:"next_cursor,omitempty"`
}

// AckRequest acknowledges a processed message.
type AckRequest struct {
	Queue             string `json:"queue"`
	MessageID         string `json:"message_id"`
	LeaseID           string `json:"lease_id"`
	FencingToken      int64  `json:"fencing_token"`
	MetaETag          string `json:"meta_etag"`
	StateETag         string `json:"state_etag,omitempty"`
	StateLeaseID      string `json:"state_lease_id,omitempty"`
	StateFencingToken int64  `json:"state_fencing_token,omitempty"`
}

// AckResponse reports acknowledgement status.
type AckResponse struct {
	Acked         bool   `json:"acked"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// NackRequest re-queues a message with optional delay.
type NackRequest struct {
	Queue             string `json:"queue"`
	MessageID         string `json:"message_id"`
	LeaseID           string `json:"lease_id"`
	FencingToken      int64  `json:"fencing_token"`
	MetaETag          string `json:"meta_etag"`
	DelaySeconds      int64  `json:"delay_seconds,omitempty"`
	LastError         any    `json:"last_error,omitempty"`
	StateLeaseID      string `json:"state_lease_id,omitempty"`
	StateFencingToken int64  `json:"state_fencing_token,omitempty"`
}

// NackResponse confirms nack handling.
type NackResponse struct {
	Requeued      bool   `json:"requeued"`
	MetaETag      string `json:"meta_etag"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// ExtendRequest keeps a lease alive and extends visibility.
type ExtendRequest struct {
	Queue             string `json:"queue"`
	MessageID         string `json:"message_id"`
	LeaseID           string `json:"lease_id"`
	FencingToken      int64  `json:"fencing_token"`
	MetaETag          string `json:"meta_etag"`
	ExtendBySeconds   int64  `json:"extend_by_seconds,omitempty"`
	StateLeaseID      string `json:"state_lease_id,omitempty"`
	StateFencingToken int64  `json:"state_fencing_token,omitempty"`
}

// ExtendResponse captures the refreshed visibility/lease info.
type ExtendResponse struct {
	LeaseExpiresAtUnix       int64  `json:"lease_expires_at_unix"`
	VisibilityTimeoutSeconds int64  `json:"visibility_timeout_seconds"`
	MetaETag                 string `json:"meta_etag"`
	StateLeaseExpiresAtUnix  int64  `json:"state_lease_expires_at_unix,omitempty"`
	CorrelationID            string `json:"correlation_id,omitempty"`
}
