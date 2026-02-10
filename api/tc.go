package api

// TCLeaseAcquireRequest requests a quorum lease on a TC peer.
type TCLeaseAcquireRequest struct {
	// CandidateID identifies the node requesting TC leadership.
	CandidateID string `json:"candidate_id"`
	// CandidateEndpoint is the network endpoint for the leadership candidate.
	CandidateEndpoint string `json:"candidate_endpoint"`
	// Term is the leader-election term associated with this operation.
	Term uint64 `json:"term"`
	// TTLMillis is the requested TC lease duration in milliseconds.
	TTLMillis int64 `json:"ttl_ms"`
}

// TCLeaseAcquireResponse reports the outcome of a lease acquire request.
type TCLeaseAcquireResponse struct {
	// Granted reports whether the lease acquire request succeeded.
	Granted bool `json:"granted"`
	// LeaderID identifies the current TC leader node.
	LeaderID string `json:"leader_id,omitempty"`
	// LeaderEndpoint identifies the active leader endpoint to retry against when applicable.
	LeaderEndpoint string `json:"leader_endpoint,omitempty"`
	// Term is the leader-election term associated with this operation.
	Term uint64 `json:"term,omitempty"`
	// ExpiresAtUnix is the TC lease expiry as a Unix timestamp in seconds when granted.
	ExpiresAtUnix int64 `json:"expires_at,omitempty"`
}

// TCLeaseRenewRequest renews an existing TC lease.
type TCLeaseRenewRequest struct {
	// LeaderID identifies the current TC leader node.
	LeaderID string `json:"leader_id"`
	// Term is the leader-election term associated with this operation.
	Term uint64 `json:"term"`
	// TTLMillis is the requested renewal duration in milliseconds.
	TTLMillis int64 `json:"ttl_ms"`
}

// TCLeaseRenewResponse reports the outcome of a lease renewal.
type TCLeaseRenewResponse struct {
	// Renewed reports whether the lease renew request succeeded.
	Renewed bool `json:"renewed"`
	// LeaderID identifies the current TC leader node.
	LeaderID string `json:"leader_id,omitempty"`
	// LeaderEndpoint identifies the active leader endpoint to retry against when applicable.
	LeaderEndpoint string `json:"leader_endpoint,omitempty"`
	// Term is the leader-election term associated with this operation.
	Term uint64 `json:"term,omitempty"`
	// ExpiresAtUnix is the renewed TC lease expiry as a Unix timestamp in seconds.
	ExpiresAtUnix int64 `json:"expires_at,omitempty"`
}

// TCLeaseReleaseRequest releases an existing TC lease.
type TCLeaseReleaseRequest struct {
	// LeaderID identifies the current TC leader node.
	LeaderID string `json:"leader_id"`
	// Term is the leader-election term associated with this operation.
	Term uint64 `json:"term"`
}

// TCLeaseReleaseResponse reports the outcome of a lease release.
type TCLeaseReleaseResponse struct {
	// Released reports whether the lease or TC lock release succeeded.
	Released bool `json:"released"`
}

// TCLeaderResponse reports the current leader state of a TC peer.
type TCLeaderResponse struct {
	// LeaderID identifies the current TC leader node.
	LeaderID string `json:"leader_id,omitempty"`
	// LeaderEndpoint identifies the active leader endpoint to retry against when applicable.
	LeaderEndpoint string `json:"leader_endpoint,omitempty"`
	// Term is the leader-election term associated with this operation.
	Term uint64 `json:"term,omitempty"`
	// ExpiresAtUnix is the active leader lease expiry as a Unix timestamp in seconds.
	ExpiresAtUnix int64 `json:"expires_at,omitempty"`
}

// TCClusterAnnounceRequest refreshes the caller's membership lease.
type TCClusterAnnounceRequest struct {
	// SelfEndpoint is the announcing node endpoint for cluster membership.
	SelfEndpoint string `json:"self_endpoint"`
}

// TCClusterAnnounceResponse reports the active cluster membership list.
type TCClusterAnnounceResponse struct {
	// Endpoints lists active endpoints known for this cluster or backend.
	Endpoints []string `json:"endpoints"`
	// UpdatedAtUnix is when the cluster membership set was last updated, as a Unix timestamp in seconds.
	UpdatedAtUnix int64 `json:"updated_at_unix,omitempty"`
	// ExpiresAtUnix is when this node's membership lease expires, as a Unix timestamp in seconds.
	ExpiresAtUnix int64 `json:"expires_at_unix,omitempty"`
}

// TCClusterLeaveRequest deletes the caller's membership lease.
type TCClusterLeaveRequest struct{}

// TCClusterLeaveResponse reports the active cluster membership list after removal.
type TCClusterLeaveResponse struct {
	// Endpoints lists active endpoints known for this cluster or backend.
	Endpoints []string `json:"endpoints"`
	// UpdatedAtUnix is when the cluster membership set was last updated, as a Unix timestamp in seconds.
	UpdatedAtUnix int64 `json:"updated_at_unix,omitempty"`
}

// TCClusterListResponse reports the current active cluster membership list.
type TCClusterListResponse struct {
	// Endpoints lists active endpoints known for this cluster or backend.
	Endpoints []string `json:"endpoints"`
	// UpdatedAtUnix is when the cluster membership set was last updated, as a Unix timestamp in seconds.
	UpdatedAtUnix int64 `json:"updated_at_unix,omitempty"`
}

// TCRMRegisterRequest registers an RM endpoint for a backend hash.
type TCRMRegisterRequest struct {
	// BackendHash identifies a storage backend island for RM routing.
	BackendHash string `json:"backend_hash"`
	// Endpoint is a registered RM or cluster endpoint.
	Endpoint string `json:"endpoint"`
}

// TCRMRegisterResponse reports the registry entry after registration.
type TCRMRegisterResponse struct {
	// BackendHash identifies a storage backend island for RM routing.
	BackendHash string `json:"backend_hash"`
	// Endpoints lists active endpoints known for this cluster or backend.
	Endpoints []string `json:"endpoints"`
	// UpdatedAtUnix is when the backend registry entry was last updated, as a Unix timestamp in seconds.
	UpdatedAtUnix int64 `json:"updated_at_unix,omitempty"`
}

// TCRMUnregisterRequest removes an RM endpoint for a backend hash.
type TCRMUnregisterRequest struct {
	// BackendHash identifies a storage backend island for RM routing.
	BackendHash string `json:"backend_hash"`
	// Endpoint is a registered RM or cluster endpoint.
	Endpoint string `json:"endpoint"`
}

// TCRMUnregisterResponse reports the registry entry after removal.
type TCRMUnregisterResponse struct {
	// BackendHash identifies a storage backend island for RM routing.
	BackendHash string `json:"backend_hash"`
	// Endpoints lists active endpoints known for this cluster or backend.
	Endpoints []string `json:"endpoints"`
	// UpdatedAtUnix is when the backend registry entry was last updated, as a Unix timestamp in seconds.
	UpdatedAtUnix int64 `json:"updated_at_unix,omitempty"`
}

// TCRMBackend describes RM endpoints for a backend hash.
type TCRMBackend struct {
	// BackendHash identifies a storage backend island for RM routing.
	BackendHash string `json:"backend_hash"`
	// Endpoints lists active endpoints known for this cluster or backend.
	Endpoints []string `json:"endpoints"`
	// UpdatedAtUnix is when this backend mapping was last updated, as a Unix timestamp in seconds.
	UpdatedAtUnix int64 `json:"updated_at_unix,omitempty"`
}

// TCRMListResponse reports the RM registry contents.
type TCRMListResponse struct {
	// Backends enumerates backend-to-endpoint mappings.
	Backends []TCRMBackend `json:"backends"`
	// UpdatedAtUnix is when the RM backend registry was last updated, as a Unix timestamp in seconds.
	UpdatedAtUnix int64 `json:"updated_at_unix,omitempty"`
}
