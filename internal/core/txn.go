package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

const txnNamespace = ".txns"

// TxnState captures the decision state of a transaction record.
type TxnState string

const (
	TxnStatePending  TxnState = "pending"
	TxnStateCommit   TxnState = "commit"
	TxnStateRollback TxnState = "rollback"
)

// TxnParticipant lists a key enrolled in a transaction.
type TxnParticipant struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	// BackendHash identifies the storage backend island for this participant.
	BackendHash string `json:"backend_hash,omitempty"`
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

func participantIndex(list []TxnParticipant, p TxnParticipant) int {
	for i, existing := range list {
		if existing.Namespace != p.Namespace || existing.Key != p.Key {
			continue
		}
		if existing.BackendHash == p.BackendHash || existing.BackendHash == "" || p.BackendHash == "" {
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
		} else if dst[idx].BackendHash == "" && p.BackendHash != "" {
			dst[idx].BackendHash = p.BackendHash
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
		rec, etag, err := s.loadTxnRecord(ctx, txnID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			if errors.Is(err, storage.ErrNotImplemented) {
				return &TxnRecord{TxnID: txnID, State: decision}, nil
			}
			return nil, err
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
		if _, err := s.putTxnRecord(ctx, rec, etag); err != nil {
			if errors.Is(err, storage.ErrNotImplemented) {
				return rec, nil
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, err
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
	for {
		existing, etag, err := s.loadTxnRecord(ctx, rec.TxnID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			if errors.Is(err, storage.ErrNotImplemented) {
				existing = &TxnRecord{TxnID: rec.TxnID}
			} else {
				return nil, err
			}
		}
		now := s.clock.Now().Unix()
		if existing == nil {
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
				return existing, Failure{Code: "txn_conflict", Detail: "transaction already decided", HTTPStatus: http.StatusConflict}
			}
			recordDecision(existing.State)
			return existing, nil
		}
		if rec.TCTerm != 0 {
			existing.TCTerm = rec.TCTerm
		}
		existing.State = rec.State
		existing.UpdatedAtUnix = now
		if _, err := s.putTxnRecord(ctx, existing, etag); err != nil {
			if errors.Is(err, storage.ErrNotImplemented) {
				recordDecision(existing.State)
				return existing, nil
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, err
		}
		recordDecision(existing.State)
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
	var retryCount int
	start := s.clock.Now()
	localHash := s.backendHash
	applyHint := applyHintFromContext(ctx)
	for _, p := range rec.Participants {
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
			}
			continue
		}
		retries, err := s.applyTxnDecisionWithRetry(ctx, p, rec.TxnID, commit, opts)
		retryCount += retries
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
	}
	duration := s.clock.Now().Sub(start)
	if s.logger != nil {
		fields := []any{
			"txn_id", rec.TxnID,
			"state", rec.State,
			"participants", len(rec.Participants),
			"applied", appliedCount,
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
			s.logger.Info("txn.apply.complete", fields...)
		}
	}
	if s.txnMetrics != nil {
		s.txnMetrics.recordApply(ctx, rec.State, appliedCount, failedCount, retryCount, duration)
	}
	if firstErr != nil {
		return fmt.Errorf("txn apply failed for %d/%d participants: %w", failedCount, len(rec.Participants), firstErr)
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
		err := s.applyTxnDecisionToKey(ctx, p.Namespace, p.Key, txnID, commit)
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
			return retries, nil
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
	for {
		if meta == nil {
			var err error
			var metaRes storage.LoadMetaResult
			metaRes, err = s.store.LoadMeta(ctx, namespace, key)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					if s.logger != nil {
						s.logger.Debug("txn.apply.meta.missing",
							"namespace", namespace,
							"key", key,
							"txn_id", txnID,
							"commit", commit)
					}
					return nil
				}
				return err
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
			return nil
		}
		if meta.StagedTxnID == "" && meta.Lease != nil && meta.Lease.TxnID != txnID {
			if s.logger != nil {
				s.logger.Debug("txn.apply.skip.leased_other",
					"namespace", namespace,
					"key", key,
					"txn_id", txnID,
					"lease_txn_id", meta.Lease.TxnID)
			}
			return nil
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
			if meta.StagedStateETag != "" {
				_ = s.staging.DiscardStagedState(ctx, namespace, key, meta.StagedTxnID, storage.DiscardStagedOptions{IgnoreNotFound: true})
			}
			if len(meta.StagedAttachments) > 0 {
				discardStagedAttachments(ctx, s.store, namespace, key, meta.StagedTxnID, meta.StagedAttachments)
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
			if _, err := s.store.StoreMeta(ctx, namespace, key, meta, metaETag); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					meta = nil
					continue
				}
				return err
			}
			return nil
		}

		if meta.StagedRemove {
			_ = s.staging.DiscardStagedState(ctx, namespace, key, meta.StagedTxnID, storage.DiscardStagedOptions{IgnoreNotFound: true})
			if len(meta.StagedAttachments) > 0 {
				discardStagedAttachments(ctx, s.store, namespace, key, meta.StagedTxnID, meta.StagedAttachments)
			}
			if len(meta.Attachments) > 0 {
				for _, att := range meta.Attachments {
					if att.ID == "" {
						continue
					}
					if err := s.store.DeleteObject(ctx, namespace, storage.AttachmentObjectKey(key, att.ID), storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil && !errors.Is(err, storage.ErrNotFound) {
						return err
					}
				}
			}
			meta.Attachments = nil
			meta.StagedAttachments = nil
			meta.StagedAttachmentDeletes = nil
			meta.StagedAttachmentsClear = false
			if err := s.store.Remove(ctx, namespace, key, meta.StateETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
				if errors.Is(err, storage.ErrCASMismatch) {
					stateRes, readErr := s.store.ReadState(ctx, namespace, key)
					if readErr != nil {
						if errors.Is(readErr, storage.ErrNotFound) {
							// state already removed; continue with meta cleanup
						} else {
							return readErr
						}
					} else {
						info := stateRes.Info
						_ = stateRes.Reader.Close()
						if info != nil && info.ETag != "" {
							if err := s.store.Remove(ctx, namespace, key, info.ETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
								if errors.Is(err, storage.ErrCASMismatch) {
									meta = nil
									continue
								}
								return err
							}
						}
					}
				}
				return err
			}
			meta.StateETag = ""
			meta.StateDescriptor = nil
			meta.StatePlaintextBytes = 0
			meta.Version = pickStagedVersion(meta)
			meta.PublishedVersion = meta.Version
		} else if meta.StagedStateETag != "" {
			skipPromote := false
			if stateRes, rerr := s.store.ReadState(ctx, namespace, key); rerr == nil {
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
					if s.indexManager != nil {
						func() {
							defer stateRes.Reader.Close()
							if doc, derr := buildDocumentFromJSON(key, stateRes.Reader); derr == nil {
								_ = s.indexManager.Insert(namespace, doc)
							}
						}()
					} else {
						_ = stateRes.Reader.Close()
					}
					_ = s.staging.DiscardStagedState(ctx, namespace, key, meta.StagedTxnID, storage.DiscardStagedOptions{IgnoreNotFound: true})
					skipPromote = true
				} else {
					_ = stateRes.Reader.Close()
				}
			} else if !errors.Is(rerr, storage.ErrNotFound) {
				return rerr
			}

			if !skipPromote {
				if promoted != nil && promotedTxnID == meta.StagedTxnID && promotedStagedETag == meta.StagedStateETag {
					meta.StateETag = promoted.NewETag
					meta.StateDescriptor = promoted.Descriptor
					meta.StatePlaintextBytes = promoted.BytesWritten
					meta.Version = pickStagedVersion(meta)
					meta.PublishedVersion = meta.Version
				} else {
					result, err := s.staging.PromoteStagedState(ctx, namespace, key, meta.StagedTxnID, storage.PromoteStagedOptions{ExpectedHeadETag: meta.StateETag})
					if err != nil {
						if errors.Is(err, storage.ErrCASMismatch) {
							if s.logger != nil {
								s.logger.Debug("txn.apply.promote.cas_mismatch",
									"namespace", namespace,
									"key", key,
									"txn_id", txnID,
									"expected_etag", meta.StateETag)
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
							stateRes, readErr := s.store.ReadState(ctx, namespace, key)
							if readErr != nil {
								return readErr
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
							return err
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
				if s.indexManager != nil {
					if stateRes, rerr := s.store.ReadState(ctx, namespace, key); rerr == nil {
						func() {
							defer stateRes.Reader.Close()
							if doc, derr := buildDocumentFromJSON(key, stateRes.Reader); derr == nil {
								_ = s.indexManager.Insert(namespace, doc)
							}
						}()
					}
				}
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
					if err := s.store.DeleteObject(ctx, namespace, storage.AttachmentObjectKey(key, att.ID), storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil && !errors.Is(err, storage.ErrNotFound) {
						return err
					}
				}
			}
			for _, name := range meta.StagedAttachmentDeletes {
				name = strings.TrimSpace(name)
				if name == "" {
					continue
				}
				if att, ok := attachments[name]; ok {
					if err := s.store.DeleteObject(ctx, namespace, storage.AttachmentObjectKey(key, att.ID), storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil && !errors.Is(err, storage.ErrNotFound) {
						return err
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
						result, err := s.promoteStagedAttachment(ctx, namespace, key, meta.StagedTxnID, staged)
						if err != nil {
							return err
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

		if _, err := s.store.StoreMeta(ctx, namespace, key, meta, metaETag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				meta = nil
				continue
			}
			return err
		}
		return nil
	}
}

func (s *Service) applyTxnDecisionToKey(ctx context.Context, namespace, key, txnID string, commit bool) error {
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
			s.logger.Info("txn.replay.complete", fields...)
		}
		if s.txnMetrics != nil {
			s.txnMetrics.recordReplay(ctx, state, duration)
		}
	}
	for {
		rec, etag, err := s.loadTxnRecord(ctx, txnID)
		if err != nil {
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
					"duration_ms", duration.Milliseconds(),
				}
				work := applySuccess + applyFailed + pendingExpired + cleanupDeleted + cleanupFailed + loadErrors + rollbackCASFailed
				switch {
				case duration > txnSweepSlowThreshold:
					s.logger.Warn("txn.sweeper.summary", fields...)
				case work > 0:
					s.logger.Info("txn.sweeper.summary", fields...)
				default:
					s.logger.Debug("txn.sweeper.summary", fields...)
				}
			}
			if s.txnMetrics != nil {
				duration := s.clock.Now().Sub(start)
				s.txnMetrics.recordSweep(ctx, duration, applySuccess, applyFailed, pendingExpired, cleanupDeleted, cleanupFailed, loadErrors, rollbackCASFailed)
			}
			return nil
		}
		startAfter = list.NextStartAfter
	}
}
