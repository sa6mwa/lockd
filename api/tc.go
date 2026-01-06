package api

// TCLeaseAcquireRequest requests a quorum lease on a TC peer.
type TCLeaseAcquireRequest struct {
	CandidateID       string `json:"candidate_id"`
	CandidateEndpoint string `json:"candidate_endpoint"`
	Term              uint64 `json:"term"`
	TTLMillis         int64  `json:"ttl_ms"`
}

// TCLeaseAcquireResponse reports the outcome of a lease acquire request.
type TCLeaseAcquireResponse struct {
	Granted        bool   `json:"granted"`
	LeaderID       string `json:"leader_id,omitempty"`
	LeaderEndpoint string `json:"leader_endpoint,omitempty"`
	Term           uint64 `json:"term,omitempty"`
	ExpiresAtUnix  int64  `json:"expires_at,omitempty"`
}

// TCLeaseRenewRequest renews an existing TC lease.
type TCLeaseRenewRequest struct {
	LeaderID  string `json:"leader_id"`
	Term      uint64 `json:"term"`
	TTLMillis int64  `json:"ttl_ms"`
}

// TCLeaseRenewResponse reports the outcome of a lease renewal.
type TCLeaseRenewResponse struct {
	Renewed        bool   `json:"renewed"`
	LeaderID       string `json:"leader_id,omitempty"`
	LeaderEndpoint string `json:"leader_endpoint,omitempty"`
	Term           uint64 `json:"term,omitempty"`
	ExpiresAtUnix  int64  `json:"expires_at,omitempty"`
}

// TCLeaseReleaseRequest releases an existing TC lease.
type TCLeaseReleaseRequest struct {
	LeaderID string `json:"leader_id"`
	Term     uint64 `json:"term"`
}

// TCLeaseReleaseResponse reports the outcome of a lease release.
type TCLeaseReleaseResponse struct {
	Released bool `json:"released"`
}

// TCLeaderResponse reports the current leader state of a TC peer.
type TCLeaderResponse struct {
	LeaderID       string `json:"leader_id,omitempty"`
	LeaderEndpoint string `json:"leader_endpoint,omitempty"`
	Term           uint64 `json:"term,omitempty"`
	ExpiresAtUnix  int64  `json:"expires_at,omitempty"`
}

// TCClusterAnnounceRequest refreshes the caller's membership lease.
type TCClusterAnnounceRequest struct {
	SelfEndpoint string `json:"self_endpoint"`
}

// TCClusterAnnounceResponse reports the active cluster membership list.
type TCClusterAnnounceResponse struct {
	Endpoints     []string `json:"endpoints"`
	UpdatedAtUnix int64    `json:"updated_at_unix,omitempty"`
	ExpiresAtUnix int64    `json:"expires_at_unix,omitempty"`
}

// TCClusterLeaveRequest deletes the caller's membership lease.
type TCClusterLeaveRequest struct{}

// TCClusterLeaveResponse reports the active cluster membership list after removal.
type TCClusterLeaveResponse struct {
	Endpoints     []string `json:"endpoints"`
	UpdatedAtUnix int64    `json:"updated_at_unix,omitempty"`
}

// TCClusterListResponse reports the current active cluster membership list.
type TCClusterListResponse struct {
	Endpoints     []string `json:"endpoints"`
	UpdatedAtUnix int64    `json:"updated_at_unix,omitempty"`
}

// TCRMRegisterRequest registers an RM endpoint for a backend hash.
type TCRMRegisterRequest struct {
	BackendHash string `json:"backend_hash"`
	Endpoint    string `json:"endpoint"`
}

// TCRMRegisterResponse reports the registry entry after registration.
type TCRMRegisterResponse struct {
	BackendHash   string   `json:"backend_hash"`
	Endpoints     []string `json:"endpoints"`
	UpdatedAtUnix int64    `json:"updated_at_unix,omitempty"`
}

// TCRMUnregisterRequest removes an RM endpoint for a backend hash.
type TCRMUnregisterRequest struct {
	BackendHash string `json:"backend_hash"`
	Endpoint    string `json:"endpoint"`
}

// TCRMUnregisterResponse reports the registry entry after removal.
type TCRMUnregisterResponse struct {
	BackendHash   string   `json:"backend_hash"`
	Endpoints     []string `json:"endpoints"`
	UpdatedAtUnix int64    `json:"updated_at_unix,omitempty"`
}

// TCRMBackend describes RM endpoints for a backend hash.
type TCRMBackend struct {
	BackendHash   string   `json:"backend_hash"`
	Endpoints     []string `json:"endpoints"`
	UpdatedAtUnix int64    `json:"updated_at_unix,omitempty"`
}

// TCRMListResponse reports the RM registry contents.
type TCRMListResponse struct {
	Backends      []TCRMBackend `json:"backends"`
	UpdatedAtUnix int64         `json:"updated_at_unix,omitempty"`
}
