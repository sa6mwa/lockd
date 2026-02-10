package api

// TxnReplayRequest drives POST /v1/txn/replay.
type TxnReplayRequest struct {
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id"`
}

// TxnReplayResponse reports the resulting decision.
type TxnReplayResponse struct {
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id"`
	// State carries the transaction decision state (for example pending, commit, rollback).
	State string `json:"state"`
}

// TxnParticipant identifies a namespaced key in a transaction decision request.
type TxnParticipant struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace"`
	// Key identifies the lock/state key within the namespace.
	Key string `json:"key"`
	// BackendHash identifies the storage backend island for this participant.
	BackendHash string `json:"backend_hash,omitempty"`
}

// TxnDecisionRequest drives /v1/txn/decide (TC) and /v1/txn/commit|rollback (RM apply).
// TC callers provide the decision and optional participant list; participants are
// merged into an existing record when present.
type TxnDecisionRequest struct {
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id"`
	State string `json:"state"` // pending|commit|rollback
	// Participants enumerates transaction participants that should be tracked for the decision.
	Participants []TxnParticipant `json:"participants,omitempty"`
	// ExpiresAtUnix is a Unix timestamp in seconds indicating when the lease or record expires.
	ExpiresAtUnix int64 `json:"expires_at_unix,omitempty"`
	// TCTerm is the current TC leader term for this decision.
	TCTerm uint64 `json:"tc_term,omitempty"`
	// TargetBackendHash scopes RM apply requests to a specific backend island.
	TargetBackendHash string `json:"target_backend_hash,omitempty"`
}

// TxnDecisionResponse echoes the recorded decision.
type TxnDecisionResponse struct {
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string `json:"txn_id"`
	// State carries the transaction decision state (for example pending, commit, rollback).
	State string `json:"state"`
}
