package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

const (
	txnNamespace            = ".txns"
	txnDecisionNamespace    = ".txn-decisions"
	txnDecisionMarkerPrefix = "m"
	txnDecisionIndexPrefix  = "e"
	txnDecisionBucketsKey   = "buckets.json"
)

var errTxnApplySkipped = errors.New("txn apply skipped")

type txnDecisionMarker struct {
	TxnID         string   `json:"txn_id"`
	State         TxnState `json:"state"`
	TCTerm        uint64   `json:"tc_term,omitempty"`
	ExpiresAtUnix int64    `json:"expires_at_unix,omitempty"`
	UpdatedAtUnix int64    `json:"updated_at_unix,omitempty"`
}

func txnDecisionMarkerKey(txnID string) string {
	return path.Join(txnDecisionMarkerPrefix, txnID)
}

func txnDecisionIndexKey(bucket, txnID string) string {
	return path.Join(txnDecisionIndexPrefix, bucket, txnID)
}

func (s *Service) loadTxnDecisionMarker(ctx context.Context, txnID string) (*txnDecisionMarker, string, error) {
	return s.loadTxnDecisionMarkerWithMode(ctx, txnID, sweepModeTransparent)
}

func (s *Service) loadTxnDecisionMarkerWithMode(ctx context.Context, txnID string, mode sweepMode) (*txnDecisionMarker, string, error) {
	obj, err := s.store.GetObject(ctx, txnDecisionNamespace, txnDecisionMarkerKey(txnID))
	if err != nil {
		return nil, "", err
	}
	defer obj.Reader.Close()
	var marker txnDecisionMarker
	if err := json.NewDecoder(obj.Reader).Decode(&marker); err != nil {
		return nil, "", err
	}
	if marker.TxnID == "" {
		marker.TxnID = txnID
	}
	if marker.ExpiresAtUnix > 0 && marker.ExpiresAtUnix <= s.clock.Now().Unix() {
		err := s.deleteTxnDecisionMarker(ctx, &marker)
		if s.txnMetrics != nil {
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				s.txnMetrics.recordDecisionMarkerSweep(ctx, mode, 0, 1)
			} else {
				s.txnMetrics.recordDecisionMarkerSweep(ctx, mode, 1, 0)
			}
		}
		return nil, "", storage.ErrNotFound
	}
	return &marker, obj.Info.ETag, nil
}

func (s *Service) putTxnDecisionMarker(ctx context.Context, marker *txnDecisionMarker, expectedETag string) (string, error) {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(marker); err != nil {
		return "", err
	}
	info, err := s.store.PutObject(ctx, txnDecisionNamespace, txnDecisionMarkerKey(marker.TxnID), bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{
		ExpectedETag: expectedETag,
		ContentType:  storage.ContentTypeJSON,
	})
	if err != nil {
		return "", err
	}
	return info.ETag, nil
}

// TxnState captures the decision state of a transaction record.
type TxnState string

const (
	// TxnStatePending indicates the transaction decision is pending.
	TxnStatePending TxnState = "pending"
	// TxnStateCommit indicates the transaction committed.
	TxnStateCommit TxnState = "commit"
	// TxnStateRollback indicates the transaction rolled back.
	TxnStateRollback TxnState = "rollback"
)

// TxnParticipant lists a key enrolled in a transaction.
type TxnParticipant struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	// BackendHash identifies the storage backend island for this participant.
	BackendHash string `json:"backend_hash,omitempty"`
	// Applied tracks whether the txn decision has been applied for this participant.
	Applied bool `json:"applied,omitempty"`
}

// TxnRecord is the durable coordination record for a local transaction.
type TxnRecord struct {
	TxnID         string           `json:"txn_id"`
	State         TxnState         `json:"state"`
	Participants  []TxnParticipant `json:"participants,omitempty"`
	ExpiresAtUnix int64            `json:"expires_at_unix,omitempty"`
	TCTerm        uint64           `json:"tc_term,omitempty"`
	UpdatedAtUnix int64            `json:"updated_at_unix,omitempty"`
	CreatedAtUnix int64            `json:"created_at_unix,omitempty"`
}

func (s *Service) loadTxnRecord(ctx context.Context, txnID string) (*TxnRecord, string, error) {
	obj, err := s.store.GetObject(ctx, txnNamespace, txnID)
	if err != nil {
		return nil, "", err
	}
	defer obj.Reader.Close()
	var rec TxnRecord
	if err := json.NewDecoder(obj.Reader).Decode(&rec); err != nil {
		return nil, "", err
	}
	if rec.TxnID == "" {
		rec.TxnID = txnID
	}
	return &rec, obj.Info.ETag, nil
}

func txnExplicit(meta *storage.Meta) bool {
	return meta != nil && meta.Lease != nil && meta.Lease.TxnExplicit
}

func (s *Service) putTxnRecord(ctx context.Context, rec *TxnRecord, expectedETag string) (string, error) {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(rec); err != nil {
		return "", err
	}
	info, err := s.store.PutObject(ctx, txnNamespace, rec.TxnID, bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{
		ExpectedETag: expectedETag,
		ContentType:  storage.ContentTypeJSON,
	})
	if err != nil {
		return "", err
	}
	return info.ETag, nil
}

func participantMatches(a, b TxnParticipant) bool {
	if a.Namespace != b.Namespace || a.Key != b.Key {
		return false
	}
	if a.BackendHash == "" || b.BackendHash == "" {
		return true
	}
	return a.BackendHash == b.BackendHash
}

func participantIndex(list []TxnParticipant, p TxnParticipant) int {
	for i, existing := range list {
		if participantMatches(existing, p) {
			return i
		}
	}
	return -1
}

func sortParticipants(list []TxnParticipant) {
	sort.Slice(list, func(i, j int) bool {
		if list[i].BackendHash == list[j].BackendHash {
			if list[i].Namespace == list[j].Namespace {
				return list[i].Key < list[j].Key
			}
			return list[i].Namespace < list[j].Namespace
		}
		return list[i].BackendHash < list[j].BackendHash
	})
}

// mergeParticipants appends any new participants from src into dst (sorted, deduped).
func mergeParticipants(dst []TxnParticipant, src []TxnParticipant) []TxnParticipant {
	for _, p := range src {
		if idx := participantIndex(dst, p); idx == -1 {
			dst = append(dst, p)
		} else {
			if dst[idx].BackendHash == "" && p.BackendHash != "" {
				dst[idx].BackendHash = p.BackendHash
			}
			if p.Applied && !dst[idx].Applied {
				dst[idx].Applied = true
			}
		}
	}
	sortParticipants(dst)
	return dst
}

func (s *Service) normalizeParticipants(list []TxnParticipant) []TxnParticipant {
	if s == nil || s.backendHash == "" || len(list) == 0 {
		return list
	}
	out := make([]TxnParticipant, len(list))
	copy(out, list)
	for i := range out {
		if strings.TrimSpace(out[i].BackendHash) == "" {
			out[i].BackendHash = s.backendHash
		}
	}
	return out
}

func allParticipantsApplied(list []TxnParticipant) bool {
	for _, p := range list {
		if !p.Applied {
			return false
		}
	}
	return true
}

func markAppliedParticipants(rec *TxnRecord, applied []TxnParticipant) bool {
	if rec == nil || len(applied) == 0 {
		return false
	}
	changed := false
	for i := range rec.Participants {
		if rec.Participants[i].Applied {
			continue
		}
		for _, p := range applied {
			if participantMatches(rec.Participants[i], p) {
				rec.Participants[i].Applied = true
				changed = true
				break
			}
		}
	}
	return changed
}

func (s *Service) resolveDecisionFromMarker(ctx context.Context, rec TxnRecord) (*TxnRecord, error) {
	if rec.State == "" || rec.State == TxnStatePending {
		return nil, nil
	}
	marker, _, err := s.loadTxnDecisionMarker(ctx, rec.TxnID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
			return nil, nil
		}
		return nil, err
	}
	if marker.State == "" {
		return nil, nil
	}
	if marker.TCTerm != 0 && rec.TCTerm == 0 {
		return nil, Failure{Code: "tc_term_required", Detail: "tc_term required for this transaction", HTTPStatus: http.StatusBadRequest}
	}
	if rec.TCTerm != 0 && marker.TCTerm != 0 && rec.TCTerm < marker.TCTerm {
		return nil, Failure{Code: "tc_term_stale", Detail: "tc_term is lower than recorded term", HTTPStatus: http.StatusConflict}
	}
	if marker.State != rec.State {
		if rec.TCTerm != 0 || marker.TCTerm != 0 {
			return nil, Failure{Code: "txn_conflict", Detail: "transaction already decided", HTTPStatus: http.StatusConflict}
		}
	}
	return &TxnRecord{
		TxnID:         rec.TxnID,
		State:         marker.State,
		ExpiresAtUnix: marker.ExpiresAtUnix,
		TCTerm:        marker.TCTerm,
	}, nil
}

func (s *Service) writeDecisionMarker(ctx context.Context, rec *TxnRecord) error {
	if s == nil || rec == nil {
		return nil
	}
	if rec.State == "" || rec.State == TxnStatePending {
		return nil
	}
	now := s.clock.Now().Unix()
	marker := txnDecisionMarker{
		TxnID:         rec.TxnID,
		State:         rec.State,
		TCTerm:        rec.TCTerm,
		ExpiresAtUnix: rec.ExpiresAtUnix,
		UpdatedAtUnix: now,
	}
	if s.txnDecisionRetention > 0 {
		retentionExpiry := now + int64(s.txnDecisionRetention/time.Second)
		if marker.ExpiresAtUnix < retentionExpiry {
			marker.ExpiresAtUnix = retentionExpiry
		}
	}
	for {
		existing, etag, err := s.loadTxnDecisionMarker(ctx, rec.TxnID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				etag = ""
			} else if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			} else {
				return err
			}
		}
		if existing != nil {
			oldExpires := existing.ExpiresAtUnix
			if existing.State != "" && existing.State != marker.State {
				if rec.TCTerm != 0 || existing.TCTerm != 0 {
					return Failure{Code: "txn_conflict", Detail: "transaction already decided", HTTPStatus: http.StatusConflict}
				}
				return nil
			}
			if existing.TCTerm > marker.TCTerm {
				marker.TCTerm = existing.TCTerm
			}
			if existing.ExpiresAtUnix > marker.ExpiresAtUnix {
				marker.ExpiresAtUnix = existing.ExpiresAtUnix
			}
			if _, err := s.putTxnDecisionMarker(ctx, &marker, etag); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				if errors.Is(err, storage.ErrNotImplemented) {
					return nil
				}
				return err
			}
			if err := s.updateDecisionIndex(ctx, marker.TxnID, oldExpires, marker.ExpiresAtUnix); err != nil && s.logger != nil {
				s.logger.Warn("txn.decision.index.update_failed", "txn_id", marker.TxnID, "error", err)
			}
			return nil
		}
		if _, err := s.putTxnDecisionMarker(ctx, &marker, etag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
		if err := s.updateDecisionIndex(ctx, marker.TxnID, 0, marker.ExpiresAtUnix); err != nil && s.logger != nil {
			s.logger.Warn("txn.decision.index.update_failed", "txn_id", marker.TxnID, "error", err)
		}
		return nil
	}
}

func (s *Service) normalizeTxnRecord(rec TxnRecord) TxnRecord {
	rec.Participants = s.normalizeParticipants(rec.Participants)
	return rec
}

// registerTxnParticipant ensures the participant is recorded under the txn id,
// extending the TTL when provided.
func (s *Service) registerTxnParticipant(ctx context.Context, txnID, namespace, key string, leaseExpires int64) (*TxnRecord, string, error) {
	p := TxnParticipant{Namespace: namespace, Key: key, BackendHash: s.backendHash}
	for {
		rec, etag, err := s.loadTxnRecord(ctx, txnID)
		if err != nil {
			if errors.Is(err, storage.ErrNotImplemented) {
				return &TxnRecord{TxnID: txnID, State: TxnStatePending, Participants: []TxnParticipant{p}}, "", nil
			}
			if !errors.Is(err, storage.ErrNotFound) {
				return nil, "", err
			}
			rec = nil
		}
		now := s.clock.Now().Unix()
		if rec == nil {
			rec = &TxnRecord{
				TxnID:         txnID,
				State:         TxnStatePending,
				Participants:  []TxnParticipant{p},
				CreatedAtUnix: now,
			}
		} else if idx := participantIndex(rec.Participants, p); idx == -1 {
			rec.Participants = append(rec.Participants, p)
		} else if rec.Participants[idx].BackendHash == "" && p.BackendHash != "" {
			rec.Participants[idx].BackendHash = p.BackendHash
		}
		if leaseExpires > rec.ExpiresAtUnix {
			rec.ExpiresAtUnix = leaseExpires
		}
		if rec.ExpiresAtUnix == 0 && s.defaultTTL.Default > 0 {
			rec.ExpiresAtUnix = now + int64(s.defaultTTL.Default/time.Second)
		}
		rec.UpdatedAtUnix = now
		sortParticipants(rec.Participants)
		newETag, err := s.putTxnRecord(ctx, rec, etag)
		if err != nil {
			if errors.Is(err, storage.ErrNotImplemented) {
				return rec, "", nil
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, "", err
		}
		return rec, newETag, nil
	}
}

func (s *Service) enlistTxnParticipant(ctx context.Context, txnID, namespace, key string, leaseExpires int64) (*TxnRecord, string, error) {
	rec, etag, err := s.registerTxnParticipant(ctx, txnID, namespace, key, leaseExpires)
	if err != nil {
		return rec, etag, err
	}
	if s.tcDecider == nil {
		return rec, etag, nil
	}
	expiresAt := leaseExpires
	if rec != nil && rec.ExpiresAtUnix > expiresAt {
		expiresAt = rec.ExpiresAtUnix
	}
	enlist := TxnRecord{
		TxnID:         txnID,
		State:         TxnStatePending,
		ExpiresAtUnix: expiresAt,
		Participants: []TxnParticipant{{
			Namespace:   namespace,
			Key:         key,
			BackendHash: s.backendHash,
		}},
	}
	if err := s.tcDecider.Enlist(ctx, enlist); err != nil {
		return rec, etag, err
	}
	return rec, etag, nil
}

// decideTxn records the transaction decision using CAS to avoid races.
func (s *Service) decideTxn(ctx context.Context, txnID string, decision TxnState) (*TxnRecord, error) {
	for {
		plan := s.newWritePlan(ctx)
		commitCtx := plan.Context()
		rec, etag, err := s.loadTxnRecord(commitCtx, txnID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			if errors.Is(err, storage.ErrNotImplemented) {
				return &TxnRecord{TxnID: txnID, State: decision}, nil
			}
			return nil, plan.Wait(err)
		}
		now := s.clock.Now().Unix()
		if rec == nil {
			rec = &TxnRecord{
				TxnID:         txnID,
				State:         decision,
				CreatedAtUnix: now,
			}
		}
		if rec.State != TxnStatePending && rec.State != decision {
			// Decision already made; surface the record.
			return rec, nil
		}
		rec.State = decision
		if rec.ExpiresAtUnix == 0 && s.defaultTTL.Default > 0 {
			rec.ExpiresAtUnix = now + int64(s.defaultTTL.Default/time.Second)
		}
		rec.UpdatedAtUnix = now
		if _, err := s.putTxnRecord(commitCtx, rec, etag); err != nil {
			if errors.Is(err, storage.ErrNotImplemented) {
				if waitErr := plan.Wait(nil); waitErr != nil {
					return nil, waitErr
				}
				return rec, nil
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				if waitErr := plan.Wait(nil); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			return nil, plan.Wait(err)
		}
		if waitErr := plan.Wait(nil); waitErr != nil {
			return nil, waitErr
		}
		return rec, nil
	}
}

// decideTxnWithMerge sets the decision, merges participants/expiry, and persists.
func (s *Service) decideTxnWithMerge(ctx context.Context, rec TxnRecord) (*TxnRecord, error) {
	start := s.clock.Now()
	recordDecision := func(state TxnState) {
		if s.txnMetrics == nil {
			return
		}
		s.txnMetrics.recordDecision(ctx, state, s.clock.Now().Sub(start))
	}
	recordQueueDecision := func(rec *TxnRecord) {
		if rec == nil || rec.State == "" || rec.State == TxnStatePending {
			return
		}
		s.recordQueueDecisionWorklist(ctx, rec)
	}
	for {
		plan := s.newWritePlan(ctx)
		commitCtx := plan.Context()
		existing, etag, err := s.loadTxnRecord(commitCtx, rec.TxnID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			if errors.Is(err, storage.ErrNotImplemented) {
				existing = &TxnRecord{TxnID: rec.TxnID}
			} else {
				return nil, plan.Wait(err)
			}
		}
		now := s.clock.Now().Unix()
		if existing == nil {
			if resolved, err := s.resolveDecisionFromMarker(commitCtx, rec); err != nil {
				return resolved, plan.Wait(err)
			} else if resolved != nil {
				recordDecision(resolved.State)
				recordQueueDecision(resolved)
				if waitErr := plan.Wait(nil); waitErr != nil {
					return nil, waitErr
				}
				return resolved, nil
			}
			existing = &TxnRecord{
				TxnID:         rec.TxnID,
				CreatedAtUnix: now,
			}
		}
		existing.Participants = mergeParticipants(existing.Participants, rec.Participants)
		if rec.ExpiresAtUnix > existing.ExpiresAtUnix {
			existing.ExpiresAtUnix = rec.ExpiresAtUnix
		}
		if existing.ExpiresAtUnix == 0 && s.defaultTTL.Default > 0 {
			existing.ExpiresAtUnix = now + int64(s.defaultTTL.Default/time.Second)
		}
		if existing.State == "" {
			existing.State = TxnStatePending
		}
		if existing.TCTerm != 0 && rec.TCTerm == 0 {
			return existing, Failure{Code: "tc_term_required", Detail: "tc_term required for this transaction", HTTPStatus: http.StatusBadRequest}
		}
		if rec.TCTerm != 0 && existing.TCTerm != 0 && rec.TCTerm < existing.TCTerm {
			return existing, Failure{Code: "tc_term_stale", Detail: "tc_term is lower than recorded term", HTTPStatus: http.StatusConflict}
		}
		if existing.State != TxnStatePending && existing.State != rec.State {
			if rec.TCTerm != 0 || existing.TCTerm != 0 {
				return existing, plan.Wait(Failure{Code: "txn_conflict", Detail: "transaction already decided", HTTPStatus: http.StatusConflict})
			}
			recordDecision(existing.State)
			recordQueueDecision(existing)
			if waitErr := plan.Wait(nil); waitErr != nil {
				return nil, waitErr
			}
			return existing, nil
		}
		if rec.TCTerm != 0 {
			existing.TCTerm = rec.TCTerm
		}
		existing.State = rec.State
		existing.UpdatedAtUnix = now
		if _, err := s.putTxnRecord(commitCtx, existing, etag); err != nil {
			if errors.Is(err, storage.ErrNotImplemented) {
				recordDecision(existing.State)
				recordQueueDecision(existing)
				if waitErr := plan.Wait(nil); waitErr != nil {
					return nil, waitErr
				}
				return existing, nil
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				if waitErr := plan.Wait(nil); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			return nil, plan.Wait(err)
		}
		recordDecision(existing.State)
		recordQueueDecision(existing)
		if waitErr := plan.Wait(nil); waitErr != nil {
			return nil, waitErr
		}
		return existing, nil
	}
}

// DecideTxn records a transaction decision without applying it.
func (s *Service) DecideTxn(ctx context.Context, rec TxnRecord) (*TxnRecord, error) {
	if rec.State == "" {
		rec.State = TxnStatePending
	}
	stopWatchdog := startTxnWatchdog(s.logger, "txn.decide.watchdog", txnDecisionSlowThreshold,
		"txn_id", rec.TxnID,
		"state", rec.State,
		"participants", len(rec.Participants),
	)
	defer stopWatchdog()
	rec = s.normalizeTxnRecord(rec)
	return s.decideTxnWithMerge(ctx, rec)
}

// DecideTxnViaTC records a transaction decision via the configured TC decider.
func (s *Service) DecideTxnViaTC(ctx context.Context, rec TxnRecord) (TxnState, error) {
	if s == nil || s.tcDecider == nil {
		return "", Failure{Code: "txn_coordinator_unavailable", Detail: "txn coordinator unavailable", HTTPStatus: http.StatusServiceUnavailable}
	}
	return s.tcDecider.Decide(ctx, rec)
}

// PrepareTxn records a pending decision and participants (no fan-out).
func (s *Service) PrepareTxn(ctx context.Context, rec TxnRecord) (*TxnRecord, error) {
	rec.State = TxnStatePending
	stopWatchdog := startTxnWatchdog(s.logger, "txn.prepare.watchdog", txnDecisionSlowThreshold,
		"txn_id", rec.TxnID,
		"participants", len(rec.Participants),
	)
	defer stopWatchdog()
	rec = s.normalizeTxnRecord(rec)
	return s.decideTxnWithMerge(ctx, rec)
}

// CommitTxn records a commit decision, merges participants/expiry, and applies.
func (s *Service) CommitTxn(ctx context.Context, rec TxnRecord) (TxnState, error) {
	rec.State = TxnStateCommit
	stopWatchdog := startTxnWatchdog(s.logger, "txn.commit.watchdog", txnDecisionSlowThreshold,
		"txn_id", rec.TxnID,
		"participants", len(rec.Participants),
	)
	defer stopWatchdog()
	rec = s.normalizeTxnRecord(rec)
	decided, err := s.decideTxnWithMerge(ctx, rec)
	if err != nil {
		return "", err
	}
	if decided.State == TxnStateCommit {
		if err := s.applyTxnDecision(ctx, decided); err != nil {
			return decided.State, err
		}
	}
	return decided.State, nil
}

// RollbackTxn records a rollback decision, merges participants/expiry, and applies.
func (s *Service) RollbackTxn(ctx context.Context, rec TxnRecord) (TxnState, error) {
	rec.State = TxnStateRollback
	stopWatchdog := startTxnWatchdog(s.logger, "txn.rollback.watchdog", txnDecisionSlowThreshold,
		"txn_id", rec.TxnID,
		"participants", len(rec.Participants),
	)
	defer stopWatchdog()
	rec = s.normalizeTxnRecord(rec)
	decided, err := s.decideTxnWithMerge(ctx, rec)
	if err != nil {
		return "", err
	}
	if decided.State == TxnStateRollback {
		if err := s.applyTxnDecision(ctx, decided); err != nil {
			return decided.State, err
		}
	}
	return decided.State, nil
}

// ApplyTxnDecision loads and applies a recorded decision, if any.
func (s *Service) ApplyTxnDecision(ctx context.Context, txnID string) (TxnState, error) {
	rec, _, err := s.loadTxnRecord(ctx, txnID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			marker, _, markerErr := s.loadTxnDecisionMarker(ctx, txnID)
			if markerErr == nil && marker != nil && marker.State != "" && marker.State != TxnStatePending {
				return marker.State, nil
			}
			if markerErr != nil && !errors.Is(markerErr, storage.ErrNotFound) && !errors.Is(markerErr, storage.ErrNotImplemented) {
				return "", markerErr
			}
		}
		return "", err
	}
	if rec.TxnID == "" {
		rec.TxnID = strings.TrimSpace(txnID)
	}
	if rec.State == "" {
		rec.State = TxnStatePending
	}
	if rec.State == TxnStatePending {
		return rec.State, nil
	}
	if err := s.applyTxnDecision(ctx, rec); err != nil {
		return rec.State, err
	}
	return rec.State, nil
}

// ApplyTxnDecisionRecord applies a decision using the provided transaction record.
func (s *Service) ApplyTxnDecisionRecord(ctx context.Context, rec *TxnRecord) (TxnState, error) {
	if rec == nil {
		return "", Failure{Code: "txn_not_found", Detail: "transaction not found", HTTPStatus: http.StatusNotFound}
	}
	if rec.State == "" {
		rec.State = TxnStatePending
	}
	if rec.State == TxnStatePending {
		return rec.State, nil
	}
	if err := s.applyTxnDecision(ctx, rec); err != nil {
		return rec.State, err
	}
	return rec.State, nil
}

// applyTxnDecision applies the recorded decision to all participants.
type txnApplyOptions struct {
	tolerateQueueLeaseMismatch bool
}

type txnApplyHint struct {
	namespace string
	key       string
	meta      *storage.Meta
	metaETag  string
}

type txnApplyHintKey struct{}

func withTxnApplyHint(ctx context.Context, namespace, key string, meta *storage.Meta, metaETag string) context.Context {
	if ctx == nil {
		return nil
	}
	hint := &txnApplyHint{
		namespace: namespace,
		key:       key,
		meta:      meta,
		metaETag:  metaETag,
	}
	return context.WithValue(ctx, txnApplyHintKey{}, hint)
}

func applyHintFromContext(ctx context.Context) *txnApplyHint {
	if ctx == nil {
		return nil
	}
	if hint, ok := ctx.Value(txnApplyHintKey{}).(*txnApplyHint); ok {
		return hint
	}
	return nil
}

// MarkTxnParticipantsApplied records applied participants and deletes the txn record once all are applied.
func (s *Service) MarkTxnParticipantsApplied(ctx context.Context, txnID string, applied []TxnParticipant) error {
	if s == nil {
		return nil
	}
	txnID = strings.TrimSpace(txnID)
	if txnID == "" {
		return nil
	}
	for {
		plan := s.newWritePlan(ctx)
		commitCtx := plan.Context()
		rec, etag, err := s.loadTxnRecord(commitCtx, txnID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return plan.Wait(err)
		}
		changed := markAppliedParticipants(rec, applied)
		allApplied := allParticipantsApplied(rec.Participants)
		if !changed && !allApplied {
			return plan.Wait(nil)
		}
		now := s.clock.Now().Unix()
		rec.UpdatedAtUnix = now
		if allApplied && rec.State != TxnStatePending {
			if err := s.writeDecisionMarker(commitCtx, rec); err != nil {
				return plan.Wait(err)
			}
			if err := s.store.DeleteObject(commitCtx, txnNamespace, rec.TxnID, storage.DeleteObjectOptions{}); err != nil {
				if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
					return plan.Wait(nil)
				}
				return plan.Wait(err)
			}
			return plan.Wait(nil)
		}
		if _, err := s.putTxnRecord(commitCtx, rec, etag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				if waitErr := plan.Wait(nil); waitErr != nil {
					return waitErr
				}
				continue
			}
			if errors.Is(err, storage.ErrNotImplemented) {
				return plan.Wait(nil)
			}
			return plan.Wait(err)
		}
		return plan.Wait(nil)
	}
}

func (s *Service) applyTxnDecision(ctx context.Context, rec *TxnRecord) error {
	return s.applyTxnDecisionWithOptions(ctx, rec, txnApplyOptions{})
}

func (s *Service) applyTxnDecisionWithOptions(ctx context.Context, rec *TxnRecord, opts txnApplyOptions) error {
	if rec == nil {
		return nil
	}
	stopWatchdog := startTxnWatchdog(s.logger, "txn.apply.watchdog", txnApplySlowThreshold,
		"txn_id", rec.TxnID,
		"state", rec.State,
		"participants", len(rec.Participants),
	)
	defer stopWatchdog()
	commit := rec.State == TxnStateCommit
	var firstErr error
	var failedCount int
	var appliedCount int
	var alreadyApplied int
	var retryCount int
	appliedParticipants := make([]TxnParticipant, 0, len(rec.Participants))
	start := s.clock.Now()
	localHash := s.backendHash
	applyHint := applyHintFromContext(ctx)
	for _, p := range rec.Participants {
		if p.Applied {
			alreadyApplied++
			continue
		}
		if localHash == "" && p.BackendHash != "" {
			if s.logger != nil {
				s.logger.Debug("txn.apply.skip.backend_missing",
					"namespace", p.Namespace,
					"key", p.Key,
					"txn_id", rec.TxnID,
					"decision", rec.State,
					"backend_hash", p.BackendHash)
			}
			continue
		}
		if localHash != "" && p.BackendHash != "" && p.BackendHash != localHash {
			if s.logger != nil {
				s.logger.Debug("txn.apply.skip.backend_mismatch",
					"namespace", p.Namespace,
					"key", p.Key,
					"txn_id", rec.TxnID,
					"decision", rec.State,
					"backend_hash", p.BackendHash,
					"local_backend_hash", localHash)
			}
			continue
		}
		if applyHint != nil && applyHint.namespace == p.Namespace && applyHint.key == p.Key {
			err := s.applyTxnDecisionForMeta(ctx, p.Namespace, p.Key, rec.TxnID, commit, applyHint.meta, applyHint.metaETag)
			if err != nil {
				if opts.tolerateQueueLeaseMismatch && isQueueMessageLeaseMismatch(err) {
					if s.logger != nil {
						s.logger.Debug("txn.apply.skip.queue_lease_mismatch",
							"namespace", p.Namespace,
							"key", p.Key,
							"txn_id", rec.TxnID,
							"commit", commit)
					}
				} else {
					s.txnDecisionFailed.Add(1)
					failedCount++
					if firstErr == nil {
						firstErr = err
					}
					if s.logger != nil {
						s.logger.Warn("txn.apply.failed",
							"namespace", p.Namespace,
							"key", p.Key,
							"txn_id", rec.TxnID,
							"decision", rec.State,
							"error", err)
					}
				}
			} else {
				s.txnDecisionApplied.Add(1)
				appliedCount++
				appliedParticipants = append(appliedParticipants, p)
			}
			continue
		}
		retries, err := s.applyTxnDecisionWithRetry(ctx, p, rec.TxnID, commit, opts)
		retryCount += retries
		if errors.Is(err, errTxnApplySkipped) {
			continue
		}
		if err != nil {
			s.txnDecisionFailed.Add(1)
			failedCount++
			if firstErr == nil {
				firstErr = err
			}
			if s.logger != nil {
				s.logger.Warn("txn.apply.failed",
					"namespace", p.Namespace,
					"key", p.Key,
					"txn_id", rec.TxnID,
					"decision", rec.State,
					"error", err)
			}
			continue
		}
		s.txnDecisionApplied.Add(1)
		appliedCount++
		appliedParticipants = append(appliedParticipants, p)
	}
	duration := s.clock.Now().Sub(start)
	if s.logger != nil {
		fields := []any{
			"txn_id", rec.TxnID,
			"state", rec.State,
			"participants", len(rec.Participants),
			"applied", appliedCount,
			"already_applied", alreadyApplied,
			"failed", failedCount,
			"retries", retryCount,
			"duration_ms", duration.Milliseconds(),
		}
		switch {
		case firstErr != nil:
			s.logger.Warn("txn.apply.complete", append(fields, "error", firstErr)...)
		case duration > txnApplySlowThreshold:
			s.logger.Warn("txn.apply.slow", fields...)
		default:
			s.logger.Debug("txn.apply.complete", fields...)
		}
	}
	if s.txnMetrics != nil {
		s.txnMetrics.recordApply(ctx, rec.State, appliedCount, failedCount, retryCount, duration)
	}
	if firstErr != nil {
		if err := s.MarkTxnParticipantsApplied(ctx, rec.TxnID, appliedParticipants); err != nil && s.logger != nil {
			s.logger.Warn("txn.apply.mark_applied.failed", "txn_id", rec.TxnID, "state", rec.State, "error", err)
		}
		return fmt.Errorf("txn apply failed for %d/%d participants: %w", failedCount, len(rec.Participants), firstErr)
	}
	if err := s.MarkTxnParticipantsApplied(ctx, rec.TxnID, appliedParticipants); err != nil {
		if s.logger != nil {
			s.logger.Warn("txn.apply.mark_applied.failed", "txn_id", rec.TxnID, "state", rec.State, "error", err)
		}
		return err
	}
	return nil
}

const (
	txnApplyRetryAttempts    = 3
	txnApplyRetryStartDelay  = 50 * time.Millisecond
	txnApplyRetryMaxDelay    = 500 * time.Millisecond
	txnDecisionSlowThreshold = 10 * time.Second
	txnApplySlowThreshold    = 10 * time.Second
	txnReplaySlowThreshold   = 10 * time.Second
	txnSweepSlowThreshold    = 10 * time.Second
)

func (s *Service) applyTxnDecisionWithRetry(ctx context.Context, p TxnParticipant, txnID string, commit bool, opts txnApplyOptions) (int, error) {
	var lastErr error
	retries := 0
	delay := txnApplyRetryStartDelay
	for attempt := 0; attempt < txnApplyRetryAttempts; attempt++ {
		if ctx.Err() != nil {
			if lastErr != nil {
				return retries, lastErr
			}
			return retries, ctx.Err()
		}
		err := s.applyTxnDecisionToKey(ctx, p.Namespace, p.Key, txnID, commit, opts.tolerateQueueLeaseMismatch)
		if err == nil {
			return retries, nil
		}
		if opts.tolerateQueueLeaseMismatch && isQueueMessageLeaseMismatch(err) {
			if s.logger != nil {
				s.logger.Debug("txn.apply.skip.queue_lease_mismatch",
					"namespace", p.Namespace,
					"key", p.Key,
					"txn_id", txnID,
					"commit", commit)
			}
			return retries, errTxnApplySkipped
		}
		lastErr = err
		if attempt == txnApplyRetryAttempts-1 {
			break
		}
		retries++
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return retries, lastErr
		case <-timer.C:
		}
		delay *= 2
		if delay > txnApplyRetryMaxDelay {
			delay = txnApplyRetryMaxDelay
		}
	}
	return retries, lastErr
}

// applyTxnDecisionToKey commits or rolls back a single participant, clearing the lease.
func (s *Service) applyTxnDecisionForMeta(ctx context.Context, namespace, key, txnID string, commit bool, meta *storage.Meta, metaETag string) error {
	var promoted *storage.PutStateResult
	var promotedTxnID string
	var promotedStagedETag string
	var promotedAttachments map[string]storage.Attachment
	var promotedAttachmentsTxn string
	queueIndexInsert := func(plan *writePlan, namespace, key string) {
		if s.indexManager == nil || plan == nil {
			return
		}
		namespace = strings.TrimSpace(namespace)
		key = strings.TrimSpace(key)
		if namespace == "" || key == "" {
			return
		}
		plan.AddFinalizer(func() error {
			docMeta := buildIndexDocumentMeta(meta)
			s.scheduleIndexAsync(func() {
				stateRes, err := s.store.ReadState(storage.ContextWithCommitGroup(ctx, nil), namespace, key)
				if err != nil {
					return
				}
				defer stateRes.Reader.Close()
				doc, derr := buildDocumentFromJSON(key, stateRes.Reader)
				if derr != nil {
					return
				}
				if docMeta != nil {
					doc.Meta = docMeta
				}
				_ = s.indexManager.Insert(namespace, doc)
			})
			return nil
		})
	}
	queueVisibilityUpdate := func(plan *writePlan, namespace, key string, meta *storage.Meta) {
		if s.indexManager == nil || plan == nil || meta == nil {
			return
		}
		visible := meta.PublishedVersion > 0 && !meta.QueryExcluded()
		plan.AddFinalizer(func() error {
			s.scheduleIndexAsync(func() {
				if err := s.indexManager.UpdateVisibility(namespace, key, visible); err != nil {
					if s.logger != nil {
						s.logger.Warn("index.visibility.update_failed", "namespace", namespace, "key", key, "error", err)
					}
				}
			})
			return nil
		})
	}
	for {
		plan := s.newWritePlan(ctx)
		commitCtx := plan.Context()
		waitCommit := func(err error) error {
			return plan.Wait(err)
		}
		if meta == nil {
			var err error
			var metaRes storage.LoadMetaResult
			metaRes, err = s.store.LoadMeta(commitCtx, namespace, key)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					if s.logger != nil {
						s.logger.Debug("txn.apply.meta.missing",
							"namespace", namespace,
							"key", key,
							"txn_id", txnID,
							"commit", commit)
					}
					return waitCommit(nil)
				}
				return waitCommit(err)
			}
			meta = metaRes.Meta
			metaETag = metaRes.ETag
		}
		if meta.StagedTxnID != "" && meta.StagedTxnID != txnID {
			if s.logger != nil {
				s.logger.Debug("txn.apply.skip.staged_other",
					"namespace", namespace,
					"key", key,
					"txn_id", txnID,
					"staged_txn_id", meta.StagedTxnID)
			}
			return waitCommit(nil)
		}
		if meta.StagedTxnID == "" && meta.Lease != nil && meta.Lease.TxnID != txnID {
			if s.logger != nil {
				s.logger.Debug("txn.apply.skip.leased_other",
					"namespace", namespace,
					"key", key,
					"txn_id", txnID,
					"lease_txn_id", meta.Lease.TxnID)
			}
			return waitCommit(nil)
		}
		if s.logger != nil {
			s.logger.Debug("txn.apply.meta",
				"namespace", namespace,
				"key", key,
				"txn_id", txnID,
				"commit", commit,
				"staged_txn_id", meta.StagedTxnID,
				"staged_state_etag", meta.StagedStateETag,
				"state_etag", meta.StateETag,
				"staged_remove", meta.StagedRemove)
		}
		now := s.clock.Now()
		if !commit {
			oldExpires := int64(0)
			if meta.Lease != nil {
				oldExpires = meta.Lease.ExpiresAtUnix
			}
			if meta.StagedStateETag != "" {
				_ = s.staging.DiscardStagedState(commitCtx, namespace, key, meta.StagedTxnID, storage.DiscardStagedOptions{IgnoreNotFound: true})
			}
			if len(meta.StagedAttachments) > 0 {
				discardStagedAttachments(commitCtx, s.store, namespace, key, meta.StagedTxnID, meta.StagedAttachments)
			}
			meta.StagedTxnID = ""
			meta.StagedVersion = 0
			meta.StagedStateETag = ""
			meta.StagedStateDescriptor = nil
			meta.StagedStatePlaintextBytes = 0
			meta.StagedAttributes = nil
			meta.StagedRemove = false
			meta.StagedAttachments = nil
			meta.StagedAttachmentDeletes = nil
			meta.StagedAttachmentsClear = false
			meta.Lease = nil
			meta.UpdatedAtUnix = now.Unix()
			if _, err := s.store.StoreMeta(commitCtx, namespace, key, meta, metaETag); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					if waitErr := waitCommit(nil); waitErr != nil {
						return waitErr
					}
					meta = nil
					continue
				}
				return waitCommit(err)
			}
			if err := s.updateLeaseIndex(commitCtx, namespace, key, oldExpires, 0); err != nil && s.logger != nil {
				s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", key, "error", err)
			}
			return waitCommit(nil)
		}

		if meta.StagedRemove {
			_ = s.staging.DiscardStagedState(commitCtx, namespace, key, meta.StagedTxnID, storage.DiscardStagedOptions{IgnoreNotFound: true})
			if len(meta.StagedAttachments) > 0 {
				discardStagedAttachments(commitCtx, s.store, namespace, key, meta.StagedTxnID, meta.StagedAttachments)
			}
			if len(meta.Attachments) > 0 {
				for _, att := range meta.Attachments {
					if att.ID == "" {
						continue
					}
					if err := s.store.DeleteObject(commitCtx, namespace, storage.AttachmentObjectKey(key, att.ID), storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil && !errors.Is(err, storage.ErrNotFound) {
						return waitCommit(err)
					}
				}
			}
			meta.Attachments = nil
			meta.StagedAttachments = nil
			meta.StagedAttachmentDeletes = nil
			meta.StagedAttachmentsClear = false
			if err := s.store.Remove(commitCtx, namespace, key, meta.StateETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
				if errors.Is(err, storage.ErrCASMismatch) {
					stateRes, readErr := s.store.ReadState(commitCtx, namespace, key)
					if readErr != nil {
						if errors.Is(readErr, storage.ErrNotFound) {
							// state already removed; continue with meta cleanup
						} else {
							return waitCommit(readErr)
						}
					} else {
						info := stateRes.Info
						_ = stateRes.Reader.Close()
						if info != nil && info.ETag != "" {
							if err := s.store.Remove(commitCtx, namespace, key, info.ETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
								if errors.Is(err, storage.ErrCASMismatch) {
									if waitErr := waitCommit(nil); waitErr != nil {
										return waitErr
									}
									meta = nil
									continue
								}
								return waitCommit(err)
							}
						}
					}
				}
				return waitCommit(err)
			}
			meta.StateETag = ""
			meta.StateDescriptor = nil
			meta.StatePlaintextBytes = 0
			meta.Version = pickStagedVersion(meta)
			meta.PublishedVersion = meta.Version
		} else if meta.StagedStateETag != "" {
			skipPromote := false
			if stateRes, rerr := s.store.ReadState(commitCtx, namespace, key); rerr == nil {
				info := stateRes.Info
				if info == nil {
					info = &storage.StateInfo{}
				}
				if info.ETag != "" && info.ETag != meta.StateETag {
					if s.logger != nil {
						s.logger.Debug("txn.apply.promote.skip.head_mismatch",
							"namespace", namespace,
							"key", key,
							"txn_id", txnID,
							"expected_etag", meta.StateETag,
							"head_etag", info.ETag)
					}
					meta.StateETag = info.ETag
					meta.StateDescriptor = info.Descriptor
					meta.StatePlaintextBytes = info.Size
					meta.Version = pickStagedVersion(meta)
					meta.PublishedVersion = meta.Version
					queueIndexInsert(plan, namespace, key)
					_ = s.staging.DiscardStagedState(commitCtx, namespace, key, meta.StagedTxnID, storage.DiscardStagedOptions{IgnoreNotFound: true})
					skipPromote = true
				} else {
					_ = stateRes.Reader.Close()
				}
			} else if !errors.Is(rerr, storage.ErrNotFound) {
				return waitCommit(rerr)
			}

			if !skipPromote {
				if promoted != nil && promotedTxnID == meta.StagedTxnID && promotedStagedETag == meta.StagedStateETag {
					meta.StateETag = promoted.NewETag
					meta.StateDescriptor = promoted.Descriptor
					meta.StatePlaintextBytes = promoted.BytesWritten
					meta.Version = pickStagedVersion(meta)
					meta.PublishedVersion = meta.Version
				} else {
					useLegacy := false
					if s.crypto != nil && s.crypto.Enabled() && len(meta.StagedStateDescriptor) > 0 {
						objectCtx := storage.StateObjectContext(path.Join(namespace, key))
						if _, derr := s.crypto.MaterialFromDescriptor(objectCtx, meta.StagedStateDescriptor); derr != nil {
							useLegacy = true
						}
					}
					var (
						result *storage.PutStateResult
						err    error
					)
					if useLegacy {
						legacy := storage.NewDefaultStagingBackend(s.store)
						result, err = legacy.PromoteStagedState(commitCtx, namespace, key, meta.StagedTxnID, storage.PromoteStagedOptions{ExpectedHeadETag: meta.StateETag})
					} else {
						promoteCtx := storage.ContextWithStateObjectContext(commitCtx, storage.StateObjectContext(path.Join(namespace, key)))
						if len(meta.StagedStateDescriptor) > 0 {
							promoteCtx = storage.ContextWithStateDescriptor(promoteCtx, meta.StagedStateDescriptor)
						}
						if meta.StagedStatePlaintextBytes > 0 {
							promoteCtx = storage.ContextWithStatePlaintextSize(promoteCtx, meta.StagedStatePlaintextBytes)
						}
						result, err = s.staging.PromoteStagedState(promoteCtx, namespace, key, meta.StagedTxnID, storage.PromoteStagedOptions{ExpectedHeadETag: meta.StateETag})
					}
					if err != nil {
						if errors.Is(err, storage.ErrCASMismatch) {
							if s.logger != nil {
								s.logger.Debug("txn.apply.promote.cas_mismatch",
									"namespace", namespace,
									"key", key,
									"txn_id", txnID,
									"expected_etag", meta.StateETag)
							}
							if waitErr := waitCommit(nil); waitErr != nil {
								return waitErr
							}
							meta = nil
							continue
						}
						if errors.Is(err, storage.ErrNotFound) {
							if s.logger != nil {
								s.logger.Warn("txn.apply.promote.missing_staging",
									"namespace", namespace,
									"key", key,
									"txn_id", txnID)
							}
							stateRes, readErr := s.store.ReadState(commitCtx, namespace, key)
							if readErr != nil {
								return waitCommit(readErr)
							}
							reader := stateRes.Reader
							info := stateRes.Info
							_ = reader.Close()
							meta.StateETag = info.ETag
							meta.StateDescriptor = info.Descriptor
							meta.StatePlaintextBytes = info.Size
							meta.Version = pickStagedVersion(meta)
							meta.PublishedVersion = meta.Version
						} else {
							if s.logger != nil {
								s.logger.Warn("txn.apply.promote.error",
									"namespace", namespace,
									"key", key,
									"txn_id", txnID,
									"error", err)
							}
							return waitCommit(err)
						}
					} else {
						promoted = result
						promotedTxnID = meta.StagedTxnID
						promotedStagedETag = meta.StagedStateETag
						if s.logger != nil {
							s.logger.Debug("txn.apply.promote.success",
								"namespace", namespace,
								"key", key,
								"txn_id", txnID,
								"new_etag", result.NewETag)
						}
						meta.StateETag = result.NewETag
						meta.StateDescriptor = result.Descriptor
						meta.StatePlaintextBytes = result.BytesWritten
						meta.Version = pickStagedVersion(meta)
						meta.PublishedVersion = meta.Version
					}
				}
				queueIndexInsert(plan, namespace, key)
			}
		}

		attachmentsChanged := meta.StagedAttachmentsClear || len(meta.StagedAttachmentDeletes) > 0 || len(meta.StagedAttachments) > 0
		if attachmentsChanged {
			attachments := make(map[string]storage.Attachment)
			if !meta.StagedAttachmentsClear {
				for _, att := range meta.Attachments {
					if att.Name == "" || att.ID == "" {
						continue
					}
					attachments[att.Name] = att
				}
			} else {
				for _, att := range meta.Attachments {
					if att.ID == "" {
						continue
					}
					if err := s.store.DeleteObject(commitCtx, namespace, storage.AttachmentObjectKey(key, att.ID), storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil && !errors.Is(err, storage.ErrNotFound) {
						return waitCommit(err)
					}
				}
			}
			for _, name := range meta.StagedAttachmentDeletes {
				name = strings.TrimSpace(name)
				if name == "" {
					continue
				}
				if att, ok := attachments[name]; ok {
					if err := s.store.DeleteObject(commitCtx, namespace, storage.AttachmentObjectKey(key, att.ID), storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil && !errors.Is(err, storage.ErrNotFound) {
						return waitCommit(err)
					}
					delete(attachments, name)
				}
			}
			if len(meta.StagedAttachments) > 0 {
				if promotedAttachmentsTxn != meta.StagedTxnID {
					promotedAttachments = make(map[string]storage.Attachment, len(meta.StagedAttachments))
					promotedAttachmentsTxn = meta.StagedTxnID
				}
				for _, staged := range meta.StagedAttachments {
					if staged.ID == "" || staged.Name == "" {
						continue
					}
					promoted, ok := promotedAttachments[staged.ID]
					if !ok {
						result, err := s.promoteStagedAttachment(commitCtx, namespace, key, meta.StagedTxnID, staged)
						if err != nil {
							return waitCommit(err)
						}
						promoted = *result
						promotedAttachments[staged.ID] = promoted
					}
					plaintextBytes := staged.PlaintextBytes
					if plaintextBytes == 0 {
						plaintextBytes = staged.Size
					}
					promoted.ID = staged.ID
					promoted.Name = staged.Name
					promoted.Size = plaintextBytes
					promoted.PlaintextBytes = plaintextBytes
					promoted.ContentType = staged.ContentType
					promoted.CreatedAtUnix = staged.CreatedAtUnix
					promoted.UpdatedAtUnix = staged.UpdatedAtUnix
					attachments[promoted.Name] = promoted
				}
			}
			meta.Attachments = attachmentsFromMap(attachments)
			if meta.StagedStateETag == "" && !meta.StagedRemove {
				meta.Version = pickStagedVersion(meta)
				meta.PublishedVersion = meta.Version
			}
		}

		if len(meta.StagedAttributes) > 0 {
			if meta.Attributes == nil {
				meta.Attributes = make(map[string]string, len(meta.StagedAttributes))
			}
			for k, v := range meta.StagedAttributes {
				meta.Attributes[k] = v
			}
		}

		oldExpires := int64(0)
		if meta.Lease != nil {
			oldExpires = meta.Lease.ExpiresAtUnix
		}
		meta.StagedTxnID = ""
		meta.StagedVersion = 0
		meta.StagedStateETag = ""
		meta.StagedStateDescriptor = nil
		meta.StagedStatePlaintextBytes = 0
		meta.StagedAttributes = nil
		meta.StagedRemove = false
		meta.StagedAttachments = nil
		meta.StagedAttachmentDeletes = nil
		meta.StagedAttachmentsClear = false
		meta.Lease = nil
		meta.UpdatedAtUnix = now.Unix()

		if _, err := s.store.StoreMeta(commitCtx, namespace, key, meta, metaETag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				if waitErr := waitCommit(nil); waitErr != nil {
					return waitErr
				}
				meta = nil
				continue
			}
			return waitCommit(err)
		}
		queueVisibilityUpdate(plan, namespace, key, meta)
		if err := s.updateLeaseIndex(commitCtx, namespace, key, oldExpires, 0); err != nil && s.logger != nil {
			s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", key, "error", err)
		}
		return waitCommit(nil)
	}
}

func (s *Service) applyTxnDecisionToKey(ctx context.Context, namespace, key, txnID string, commit bool, tolerateQueueLeaseMismatch bool) error {
	if parts, ok := queue.ParseStateLeaseKey(key); ok {
		return s.applyTxnDecisionQueueState(ctx, namespace, parts.Queue, parts.ID, txnID, commit)
	}
	if parts, ok := queue.ParseMessageLeaseKey(key); ok {
		return s.applyTxnDecisionQueueMessage(ctx, namespace, parts.Queue, parts.ID, txnID, commit)
	}
	return s.applyTxnDecisionForMeta(ctx, namespace, key, txnID, commit, nil, "")
}

// ReplayTxn replays a single transaction record by id, applying any committed or
// rollback decisions. Pending transactions are left untouched unless expired,
// in which case they are rolled back. Returns the resulting state.
func (s *Service) ReplayTxn(ctx context.Context, txnID string) (TxnState, error) {
	stopWatchdog := startTxnWatchdog(s.logger, "txn.replay.watchdog", txnReplaySlowThreshold,
		"txn_id", txnID,
	)
	defer stopWatchdog()
	start := s.clock.Now()
	logReplay := func(state TxnState, err error) {
		if s.logger == nil {
			return
		}
		duration := s.clock.Now().Sub(start)
		fields := []any{
			"txn_id", txnID,
			"state", state,
			"duration_ms", duration.Milliseconds(),
		}
		switch {
		case err != nil:
			s.logger.Warn("txn.replay.complete", append(fields, "error", err)...)
		case duration > txnReplaySlowThreshold:
			s.logger.Warn("txn.replay.slow", fields...)
		default:
			s.logger.Debug("txn.replay.complete", fields...)
		}
		if s.txnMetrics != nil {
			s.txnMetrics.recordReplay(ctx, state, duration)
		}
	}
	for {
		rec, etag, err := s.loadTxnRecord(ctx, txnID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				marker, _, markerErr := s.loadTxnDecisionMarker(ctx, txnID)
				if markerErr == nil && marker != nil && marker.State != "" && marker.State != TxnStatePending {
					logReplay(marker.State, nil)
					return marker.State, nil
				}
				if markerErr != nil && !errors.Is(markerErr, storage.ErrNotFound) && !errors.Is(markerErr, storage.ErrNotImplemented) {
					logReplay("", markerErr)
					return "", markerErr
				}
			}
			logReplay("", err)
			return "", err
		}
		if rec.TxnID == "" {
			rec.TxnID = strings.TrimSpace(txnID)
		}
		if rec.State == "" {
			rec.State = TxnStatePending
		}
		now := s.clock.Now().Unix()
		if rec.State == TxnStatePending && rec.ExpiresAtUnix > 0 && rec.ExpiresAtUnix <= now {
			rec.State = TxnStateRollback
			if s.txnDecisionRetention > 0 {
				rec.ExpiresAtUnix = now + int64(s.txnDecisionRetention/time.Second)
			}
			rec.UpdatedAtUnix = now
			if _, err := s.putTxnRecord(ctx, rec, etag); err != nil && !errors.Is(err, storage.ErrNotImplemented) {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				return "", err
			}
		}
		if rec.State == TxnStatePending {
			logReplay(rec.State, nil)
			return rec.State, nil
		}
		if err := s.applyTxnDecisionWithOptions(ctx, rec, txnApplyOptions{tolerateQueueLeaseMismatch: true}); err != nil {
			logReplay(rec.State, err)
			return rec.State, err
		}
		logReplay(rec.State, nil)
		return rec.State, nil
	}
}

// SweepTxnRecords replays committed/rolled back transactions and rolls back expired pending ones.
func (s *Service) SweepTxnRecords(ctx context.Context, now time.Time) error {
	start := s.clock.Now()
	stopWatchdog := startTxnWatchdog(s.logger, "txn.sweep.watchdog", txnSweepSlowThreshold)
	defer stopWatchdog()
	var (
		totalCount        int
		pendingSkipped    int
		pendingExpired    int
		applySuccess      int
		applyFailed       int
		loadErrors        int
		cleanupDeleted    int
		cleanupFailed     int
		rollbackCASFailed int
		markerDeleted     int
		markerFailed      int
	)
	startAfter := ""
	for {
		list, err := s.store.ListObjects(ctx, txnNamespace, storage.ListOptions{StartAfter: startAfter, Limit: 128})
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil
			}
			if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
		for _, obj := range list.Objects {
			totalCount++
			rec, etag, err := s.loadTxnRecord(ctx, obj.Key)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				if errors.Is(err, storage.ErrNotImplemented) {
					return nil
				}
				if s.logger != nil {
					s.logger.Warn("txn.load.error", "txn_id", obj.Key, "error", err)
				}
				loadErrors++
				continue
			}
			if rec.TxnID == "" {
				rec.TxnID = strings.TrimSpace(obj.Key)
			}
			if rec.State == "" {
				rec.State = TxnStatePending
			}
			// Convert expired pending txns into rollbacks.
			if rec.State == TxnStatePending && rec.ExpiresAtUnix > 0 && rec.ExpiresAtUnix <= now.Unix() {
				rec.State = TxnStateRollback
				pendingExpired++
				nowUnix := now.Unix()
				if s.txnDecisionRetention > 0 {
					rec.ExpiresAtUnix = nowUnix + int64(s.txnDecisionRetention/time.Second)
				}
				rec.UpdatedAtUnix = nowUnix
				if _, err := s.putTxnRecord(ctx, rec, etag); err != nil {
					if errors.Is(err, storage.ErrNotImplemented) {
						// proceed without persisting updated state
					} else if errors.Is(err, storage.ErrCASMismatch) {
						continue
					}
					if s.logger != nil {
						s.logger.Warn("txn.rollback.cas_failed", "txn_id", rec.TxnID, "error", err)
					}
					rollbackCASFailed++
					continue
				}
			}
			if rec.State == TxnStatePending {
				pendingSkipped++
				continue
			}
			if allParticipantsApplied(rec.Participants) {
				if err := s.writeDecisionMarker(ctx, rec); err != nil {
					if s.logger != nil {
						s.logger.Warn("txn.decision.marker.write_failed", "txn_id", rec.TxnID, "state", rec.State, "error", err)
					}
					applyFailed++
					continue
				}
				if err := s.store.DeleteObject(ctx, txnNamespace, rec.TxnID, storage.DeleteObjectOptions{}); err != nil {
					if !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, storage.ErrNotImplemented) {
						if s.logger != nil {
							s.logger.Debug("txn.cleanup.failed", "txn_id", rec.TxnID, "error", err)
						}
						cleanupFailed++
					}
				} else {
					cleanupDeleted++
				}
				continue
			}
			if s.logger != nil {
				s.logger.Debug("txn.sweeper.replay",
					"txn_id", rec.TxnID,
					"state", rec.State,
					"participants", len(rec.Participants))
			}
			if err := s.applyTxnDecisionWithOptions(ctx, rec, txnApplyOptions{tolerateQueueLeaseMismatch: true}); err != nil {
				if s.logger != nil {
					s.logger.Warn("txn.apply.error", "txn_id", rec.TxnID, "state", rec.State, "error", err)
				}
				applyFailed++
				continue
			}
			applySuccess++
			if s.txnDecisionRetention > 0 && rec.ExpiresAtUnix > 0 && rec.ExpiresAtUnix <= now.Unix() {
				if err := s.store.DeleteObject(ctx, txnNamespace, rec.TxnID, storage.DeleteObjectOptions{}); err != nil {
					if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
						continue
					}
					if s.logger != nil {
						s.logger.Debug("txn.cleanup.failed", "txn_id", rec.TxnID, "error", err)
					}
					cleanupFailed++
				} else {
					cleanupDeleted++
				}
			}
		}
		if !list.Truncated {
			if deleted, failed, err := s.sweepTxnDecisionMarkers(ctx, now); err != nil {
				if s.logger != nil {
					s.logger.Warn("txn.decision.marker.sweep_failed", "error", err)
				}
				markerFailed += failed
			} else {
				markerDeleted += deleted
				markerFailed += failed
			}
			if s.logger != nil {
				duration := s.clock.Now().Sub(start)
				fields := []any{
					"records", totalCount,
					"pending_skipped", pendingSkipped,
					"pending_expired", pendingExpired,
					"applied", applySuccess,
					"apply_failed", applyFailed,
					"load_errors", loadErrors,
					"rollback_cas_failed", rollbackCASFailed,
					"cleanup_deleted", cleanupDeleted,
					"cleanup_failed", cleanupFailed,
					"decision_marker_deleted", markerDeleted,
					"decision_marker_failed", markerFailed,
					"duration_ms", duration.Milliseconds(),
				}
				work := applySuccess + applyFailed + pendingExpired + cleanupDeleted + cleanupFailed + loadErrors + rollbackCASFailed + markerDeleted + markerFailed
				switch {
				case duration > txnSweepSlowThreshold:
					s.logger.Warn("txn.sweeper.summary", fields...)
				case work > 0:
					s.logger.Debug("txn.sweeper.summary", fields...)
				default:
					s.logger.Debug("txn.sweeper.summary", fields...)
				}
			}
			if s.txnMetrics != nil {
				duration := s.clock.Now().Sub(start)
				s.txnMetrics.recordSweep(ctx, sweepModeManual, duration, applySuccess, applyFailed, pendingExpired, cleanupDeleted, cleanupFailed, loadErrors, rollbackCASFailed)
			}
			return nil
		}
		startAfter = list.NextStartAfter
	}
}

func (s *Service) sweepTxnDecisionMarkers(ctx context.Context, now time.Time) (int, int, error) {
	if s == nil {
		return 0, 0, nil
	}
	budget := newSweepBudget(s.clock, IdleSweepOptions{
		Now:        now,
		MaxOps:     128,
		MaxRuntime: 2 * time.Second,
	})
	deleted, failed, err := s.sweepDecisionIndex(ctx, budget, sweepModeManual)
	if err != nil {
		return deleted, failed, err
	}
	return deleted, failed, nil
}
