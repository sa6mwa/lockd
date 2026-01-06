package tcleader

import (
	"fmt"
	"sync"
	"time"
)

// LeaseRecord captures the current leader lease on a TC peer.
type LeaseRecord struct {
	LeaderID       string
	LeaderEndpoint string
	Term           uint64
	ExpiresAt      time.Time
	Observed       bool
	ObservedTerm   uint64
}

// LeaseStore manages the lease record for a TC peer.
type LeaseStore struct {
	mu     sync.Mutex
	record LeaseRecord
}

// LeaseError reports a lease conflict or term mismatch.
type LeaseError struct {
	Code           string
	Detail         string
	LeaderID       string
	LeaderEndpoint string
	Term           uint64
}

func (e *LeaseError) Error() string {
	if e == nil {
		return ""
	}
	if e.Detail != "" {
		return e.Detail
	}
	return fmt.Sprintf("tc lease error: %s", e.Code)
}

// Acquire attempts to acquire a lease for the candidate. ttl must be > 0.
func (s *LeaseStore) Acquire(now time.Time, candidateID, candidateEndpoint string, term uint64, ttl time.Duration) (LeaseRecord, *LeaseError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := normalizeLeaseRecord(now, s.record)
	s.record = rec
	if term < rec.Term {
		return rec, newLeaseError("tc_term_stale", "term is lower than current term", rec)
	}
	if ttl <= 0 {
		return rec, &LeaseError{Code: "tc_invalid_ttl", Detail: "ttl must be > 0"}
	}
	expired := rec.ExpiresAt.IsZero() || !rec.ExpiresAt.After(now)
	if !expired && rec.LeaderID != "" && rec.LeaderID != candidateID {
		return rec, newLeaseError("tc_lease_active", "another leader holds an active lease", rec)
	}
	if expired {
		if term <= rec.Term {
			return rec, newLeaseError("tc_term_stale", "term is lower than current term", rec)
		}
		rec.LeaderID = candidateID
		rec.LeaderEndpoint = candidateEndpoint
		rec.Term = term
		rec.ExpiresAt = now.Add(ttl)
		rec.Observed = false
		s.record = rec
		return rec, nil
	}
	if rec.LeaderID == candidateID && rec.Term == term {
		rec.ExpiresAt = now.Add(ttl)
		// Preserve observation state; acquire is a vote unless renewed.
		s.record = rec
		return rec, nil
	}
	return rec, newLeaseError("tc_lease_active", "another leader holds an active lease", rec)
}

// Renew extends an existing lease for the leader. ttl must be > 0.
func (s *LeaseStore) Renew(now time.Time, leaderID string, term uint64, ttl time.Duration) (LeaseRecord, *LeaseError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := normalizeLeaseRecord(now, s.record)
	s.record = rec
	if term < rec.Term {
		return rec, newLeaseError("tc_term_stale", "term is lower than current term", rec)
	}
	if ttl <= 0 {
		return rec, &LeaseError{Code: "tc_invalid_ttl", Detail: "ttl must be > 0"}
	}
	if rec.LeaderID != leaderID || rec.Term != term || rec.ExpiresAt.IsZero() || !rec.ExpiresAt.After(now) {
		return rec, newLeaseError("tc_lease_active", "lease is not held by this leader", rec)
	}
	rec.ExpiresAt = now.Add(ttl)
	rec.Observed = true
	if rec.Term > rec.ObservedTerm {
		rec.ObservedTerm = rec.Term
	}
	s.record = rec
	return rec, nil
}

// Release clears the lease when held by the specified leader.
func (s *LeaseStore) Release(now time.Time, leaderID string, term uint64) (LeaseRecord, *LeaseError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := normalizeLeaseRecord(now, s.record)
	s.record = rec
	if term < rec.Term {
		return rec, newLeaseError("tc_term_stale", "term is lower than current term", rec)
	}
	if rec.LeaderID != leaderID || rec.Term != term {
		return rec, newLeaseError("tc_lease_active", "lease is not held by this leader", rec)
	}
	if rec.Observed {
		rec.LeaderID = ""
		rec.LeaderEndpoint = ""
		rec.ExpiresAt = now
		rec.Observed = false
		if rec.Term > rec.ObservedTerm {
			rec.ObservedTerm = rec.Term
		}
	} else {
		rec.LeaderID = ""
		rec.LeaderEndpoint = ""
		rec.ExpiresAt = time.Time{}
		rec.Observed = false
		rec.Term = rec.ObservedTerm
	}
	s.record = rec
	return rec, nil
}

// Follow records an observed leader lease if it is newer than the current record.
func (s *LeaseStore) Follow(now time.Time, leaderID, leaderEndpoint string, term uint64, expiresAt time.Time) (LeaseRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := normalizeLeaseRecord(now, s.record)
	s.record = rec
	if leaderID == "" || leaderEndpoint == "" || term == 0 {
		return rec, false
	}
	if expiresAt.IsZero() || !expiresAt.After(now) {
		return rec, false
	}
	if term < rec.Term {
		return rec, false
	}
	if term == rec.Term && rec.LeaderID != "" && rec.LeaderID != leaderID && rec.ExpiresAt.After(now) {
		return rec, false
	}
	rec.LeaderID = leaderID
	rec.LeaderEndpoint = leaderEndpoint
	rec.Term = term
	rec.ExpiresAt = expiresAt
	rec.Observed = true
	if rec.Term > rec.ObservedTerm {
		rec.ObservedTerm = rec.Term
	}
	s.record = rec
	return rec, true
}

// Snapshot returns the current record without applying expiration logic.
func (s *LeaseStore) Snapshot() LeaseRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.record
}

// Leader returns the current leader record, including expired leases.
func (s *LeaseStore) Leader() LeaseRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.record
}

func newLeaseError(code, detail string, rec LeaseRecord) *LeaseError {
	return &LeaseError{
		Code:           code,
		Detail:         detail,
		LeaderID:       rec.LeaderID,
		LeaderEndpoint: rec.LeaderEndpoint,
		Term:           rec.Term,
	}
}

func normalizeLeaseRecord(now time.Time, rec LeaseRecord) LeaseRecord {
	if rec.Observed && rec.Term > rec.ObservedTerm {
		rec.ObservedTerm = rec.Term
	}
	if rec.Term < rec.ObservedTerm {
		rec.Term = rec.ObservedTerm
	}
	expired := rec.ExpiresAt.IsZero() || !rec.ExpiresAt.After(now)
	if !rec.Observed && expired {
		if rec.LeaderID != "" {
			rec.LeaderID = ""
			rec.LeaderEndpoint = ""
			rec.ExpiresAt = time.Time{}
		}
		rec.Term = rec.ObservedTerm
	}
	return rec
}
