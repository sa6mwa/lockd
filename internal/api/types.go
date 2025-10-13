package api

// AcquireRequest models the JSON payload for POST /v1/acquire.
type AcquireRequest struct {
	Key         string `json:"key"`
	TTLSeconds  int64  `json:"ttl_seconds"`
	Owner       string `json:"owner"`
	BlockSecs   int64  `json:"block_seconds"`
	Idempotency string `json:"-"`
}

// AcquireResponse is returned when a lease is granted.
type AcquireResponse struct {
	LeaseID    string `json:"lease_id"`
	Key        string `json:"key"`
	Owner      string `json:"owner"`
	ExpiresAt  int64  `json:"expires_at_unix"`
	Version    int64  `json:"version"`
	StateETag  string `json:"state_etag,omitempty"`
	FencingToken int64  `json:"fencing_token,omitempty"`
	RetryAfter int64  `json:"retry_after_seconds,omitempty"`
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
