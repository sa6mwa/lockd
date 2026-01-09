package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

// Acquire obtains an exclusive lease using transport-neutral inputs/outputs.
func (s *Service) Acquire(ctx context.Context, cmd AcquireCommand) (res *AcquireResult, err error) {
	start := s.clock.Now()
	namespaceLabel := "unknown"
	kindLabel := "lock"
	defer func() {
		if s.leaseMetrics == nil {
			return
		}
		duration := s.clock.Now().Sub(start)
		s.leaseMetrics.recordAcquire(ctx, namespaceLabel, kindLabel, duration, err)
	}()
	if err := s.maybeThrottleLock(); err != nil {
		return nil, err
	}
	if err := s.applyShutdownGuard("lock_acquire"); err != nil {
		return nil, err
	}
	finishLock := s.beginLockOp()
	defer finishLock()

	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = s.logger
	}

	// Resolve namespace & defaults.
	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: 400}
	}
	namespaceLabel = namespace
	s.observeNamespace(namespace)

	key := strings.TrimSpace(cmd.Key)
	autoKey := false
	if key == "" {
		key, err = s.generateUniqueKey(ctx, namespace)
		if err != nil {
			return nil, fmt.Errorf("generate key: %w", err)
		}
		autoKey = true
	}
	if cmd.Owner == "" {
		return nil, Failure{Code: "missing_owner", Detail: "owner is required", HTTPStatus: 400}
	}

	ttl := s.resolveTTL(cmd.TTLSeconds)
	if ttl <= 0 {
		return nil, Failure{Code: "invalid_ttl", Detail: "ttl_seconds must be positive", HTTPStatus: 400}
	}

	storageKey, err := s.namespacedKey(namespace, key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: 400}
	}
	keyComponent := relativeKey(namespace, storageKey)
	kindLabel = leaseKindLabel(keyComponent)
	block, waitForever := s.resolveBlock(cmd.BlockSeconds)

	// Correlation setup
	if !correlation.Has(ctx) {
		ctx = correlation.Set(ctx, correlation.Generate())
	}
	correlationID := correlation.ID(ctx)

	// Shutdown gate
	state := s.currentShutdownState()
	if state.Draining {
		retry := durationToSeconds(state.Remaining)
		logger.Info("lease.acquire.reject_shutdown",
			"namespace", namespace,
			"key", key,
			"owner", cmd.Owner,
			"remaining_seconds", retry,
		)
		return nil, Failure{
			Code:       "shutdown_draining",
			Detail:     "server is draining existing leases",
			RetryAfter: retry,
			HTTPStatus: 503,
		}
	}

	logger.Info("lease.acquire.begin",
		"namespace", namespace,
		"key", key,
		"owner", cmd.Owner,
		"ttl_seconds", ttl.Seconds(),
		"block_seconds", cmd.BlockSeconds,
		"generated_key", autoKey,
	)

	var deadline time.Time
	if !waitForever && block > 0 {
		deadline = s.clock.Now().Add(block)
	}

	txnID := strings.TrimSpace(cmd.TxnID)
	if txnID != "" {
		if _, err := xid.FromString(txnID); err != nil {
			return nil, Failure{Code: "invalid_txn", Detail: "txn_id must be a valid xid", HTTPStatus: http.StatusBadRequest}
		}
	}
	leaseID := xid.New().String()
	if txnID == "" {
		txnID = xid.New().String()
	}
	backoff := newAcquireBackoff()
	for {
		now := s.clock.Now()
		meta, metaETag, err := s.ensureMeta(ctx, namespace, storageKey)
		if err != nil {
			return nil, err
		}
		if cmd.ForceQueryHidden && !meta.HasQueryHiddenPreference() {
			meta.SetQueryHidden(true)
		}
		var creationMu *sync.Mutex
		if metaETag == "" {
			creationMu = s.creationMutex(storageKey)
			creationMu.Lock()
		}
		oldExpires := int64(0)
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			oldExpires = meta.Lease.ExpiresAtUnix
			meta.Lease = nil
		}
		// If the lease is gone but staging remains, roll it back before granting
		// a new lease to avoid resurfacing stale staged payloads after restarts.
		if meta.Lease == nil && meta.StagedTxnID != "" {
			s.discardStagedArtifacts(ctx, namespace, keyComponent, meta)
			clearStagingFields(meta)
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix > now.Unix() {
			if creationMu != nil {
				creationMu.Unlock()
			}
			if block > 0 && (waitForever || now.Before(deadline)) {
				leaseExpiry := time.Unix(meta.Lease.ExpiresAtUnix, 0)
				limit := leaseExpiry.Sub(now)
				if limit <= 0 {
					limit = acquireBackoffStart
				}
				if !waitForever && !deadline.IsZero() {
					remaining := deadline.Sub(now)
					if remaining > 0 && (limit <= 0 || remaining < limit) {
						limit = remaining
					}
				}
				sleep := backoff.Next(limit)
				s.clock.Sleep(sleep)
				continue
			}
			retryDur := maxDuration(time.Until(time.Unix(meta.Lease.ExpiresAtUnix, 0)), 0)
			retry := maxInt64(int64(math.Ceil(retryDur.Seconds())), 1)
			return nil, Failure{
				Code:       "waiting",
				Detail:     "lease already held",
				RetryAfter: retry,
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: 409,
			}
		}

		expiresAt := now.Add(ttl).Unix()
		newFencing := meta.FencingToken + 1
		meta.FencingToken = newFencing
		meta.Lease = &storage.Lease{
			ID:            leaseID,
			Owner:         cmd.Owner,
			ExpiresAtUnix: expiresAt,
			FencingToken:  newFencing,
			TxnID:         txnID,
		}
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := s.store.StoreMeta(ctx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if creationMu != nil {
				creationMu.Unlock()
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, fmt.Errorf("store meta: %w", err)
		}
		if err := s.updateLeaseIndex(ctx, namespace, keyComponent, oldExpires, meta.Lease.ExpiresAtUnix); err != nil && s.logger != nil {
			s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", keyComponent, "error", err)
		}
		if creationMu != nil {
			creationMu.Unlock()
		}
		metaETag = newMetaETag

		res = &AcquireResult{
			Namespace:     namespace,
			LeaseID:       leaseID,
			TxnID:         txnID,
			Key:           key,
			Owner:         cmd.Owner,
			ExpiresAt:     expiresAt,
			Version:       meta.Version,
			StateETag:     meta.StateETag,
			FencingToken:  newFencing,
			CorrelationID: correlationID,
			GeneratedKey:  autoKey,
			MetaETag:      metaETag,
			Meta:          meta,
		}
		if s.leaseMetrics != nil {
			s.leaseMetrics.addActive(kindLabel, 1)
		}
		logger.Info("lease.acquire.success",
			"namespace", namespace,
			"key", key,
			"owner", cmd.Owner,
			"lease_id", leaseID,
			"txn_id", txnID,
			"fencing_token", newFencing,
			"expires_at", expiresAt,
		)
		return res, nil
	}
}

// KeepAlive refreshes a lease TTL.
func (s *Service) KeepAlive(ctx context.Context, cmd KeepAliveCommand) (res *KeepAliveResult, err error) {
	start := s.clock.Now()
	namespaceLabel := "unknown"
	kindLabel := "lock"
	defer func() {
		if s.leaseMetrics == nil {
			return
		}
		duration := s.clock.Now().Sub(start)
		s.leaseMetrics.recordKeepAlive(ctx, namespaceLabel, kindLabel, duration, err)
	}()
	if err := s.maybeThrottleLock(); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	namespaceLabel = namespace
	s.observeNamespace(namespace)
	if strings.TrimSpace(cmd.Key) == "" {
		return nil, Failure{Code: "missing_key", Detail: "key is required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
	}
	ttl := s.resolveTTL(cmd.TTLSeconds)
	if ttl <= 0 {
		return nil, Failure{Code: "invalid_ttl", Detail: "ttl_seconds must be positive", HTTPStatus: http.StatusBadRequest}
	}

	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)
	kindLabel = leaseKindLabel(keyComponent)

	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = s.logger
	}

	requestTxn := strings.TrimSpace(cmd.TxnID)
	for {
		now := s.clock.Now()
		meta, metaETag, err := s.loadMetaMaybeCached(ctx, namespace, storageKey, cmd.KnownMeta, cmd.KnownMetaETag)
		if err != nil {
			return nil, err
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, requestTxn, now)
			if _, _, err := s.clearExpiredLease(ctx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				return nil, err
			}
			return nil, leaseErr
		}
		metaTxn := ""
		if meta.Lease != nil {
			metaTxn = meta.Lease.TxnID
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, requestTxn, now); err != nil {
			return nil, err
		}
		oldExpires := meta.Lease.ExpiresAtUnix
		meta.Lease.ExpiresAtUnix = now.Add(ttl).Unix()
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := s.store.StoreMeta(ctx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, fmt.Errorf("store meta: %w", err)
		}
		logger.Debug("keepalive.success",
			"namespace", namespace,
			"key", keyComponent,
			"lease_id", cmd.LeaseID,
			"txn_id", metaTxn,
			"expires_at", meta.Lease.ExpiresAtUnix,
			"fencing", meta.Lease.FencingToken,
		)
		if err := s.updateLeaseIndex(ctx, namespace, keyComponent, oldExpires, meta.Lease.ExpiresAtUnix); err != nil && s.logger != nil {
			s.logger.Warn("lease.index.update_failed", "namespace", namespace, "key", keyComponent, "error", err)
		}
		res = &KeepAliveResult{
			ExpiresAt:    meta.Lease.ExpiresAtUnix,
			FencingToken: meta.Lease.FencingToken,
			MetaETag:     newMetaETag,
			Meta:         meta,
		}
		return res, nil
	}
}

// Release relinquishes a lease.
func (s *Service) Release(ctx context.Context, cmd ReleaseCommand) (res *ReleaseResult, err error) {
	start := s.clock.Now()
	namespaceLabel := "unknown"
	kindLabel := "lock"
	defer func() {
		if s.leaseMetrics == nil {
			return
		}
		duration := s.clock.Now().Sub(start)
		s.leaseMetrics.recordRelease(ctx, namespaceLabel, kindLabel, duration, err)
	}()
	if err := s.maybeThrottleLock(); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	namespaceLabel = namespace
	if strings.TrimSpace(cmd.Key) == "" {
		return nil, Failure{Code: "missing_key", Detail: "key is required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.TxnID) == "" {
		return nil, Failure{Code: "missing_txn", Detail: "txn_id required", HTTPStatus: http.StatusBadRequest}
	}
	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)
	kindLabel = leaseKindLabel(keyComponent)

	for {
		now := s.clock.Now()
		meta, metaETag, err := s.loadMetaMaybeCached(ctx, namespace, storageKey, cmd.KnownMeta, cmd.KnownMetaETag)
		if err != nil {
			return nil, err
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			if _, _, err := s.clearExpiredLease(ctx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				return nil, err
			}
			return &ReleaseResult{Released: true, MetaCleared: true}, nil
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now); err != nil {
			// Idempotent: mismatched/expired/fencing issues are treated as already released.
			var failure Failure
			if errors.As(err, &failure) {
				switch failure.Code {
				case "lease_required", "lease_expired", "fencing_mismatch":
					return &ReleaseResult{Released: true, MetaCleared: true}, nil
				}
			}
			return nil, err
		}

		if meta.StagedTxnID != "" && meta.StagedTxnID != cmd.TxnID {
			return nil, Failure{
				Code:       "txn_mismatch",
				Detail:     "different transaction already staged",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}

		commit := !cmd.Rollback
		if meta.StagedTxnID == "" && meta.StagedStateETag == "" && !meta.StagedRemove && len(meta.StagedAttributes) == 0 && len(meta.StagedAttachments) == 0 && len(meta.StagedAttachmentDeletes) == 0 && !meta.StagedAttachmentsClear {
			commit = false // nothing staged; just clear lease
		}

		decision := TxnStateCommit
		if !commit {
			decision = TxnStateRollback
		}
		if s.tcDecider != nil {
			rec := TxnRecord{
				TxnID:         cmd.TxnID,
				State:         decision,
				ExpiresAtUnix: meta.Lease.ExpiresAtUnix,
				Participants: []TxnParticipant{{
					Namespace:   namespace,
					Key:         keyComponent,
					BackendHash: s.backendHash,
				}},
			}
			applyCtx := withTxnApplyHint(ctx, namespace, keyComponent, meta, metaETag)
			state, err := s.tcDecider.Decide(applyCtx, rec)
			if err != nil {
				return nil, err
			}
			if state == TxnStatePending {
				return nil, Failure{Code: "txn_pending", Detail: "transaction decision not recorded", HTTPStatus: http.StatusConflict}
			}
			if s.leaseMetrics != nil {
				s.leaseMetrics.addActive(kindLabel, -1)
			}
			return &ReleaseResult{
				Released:    true,
				MetaCleared: true,
			}, nil
		}
		if _, _, err := s.registerTxnParticipant(ctx, cmd.TxnID, namespace, keyComponent, meta.Lease.ExpiresAtUnix); err != nil {
			return nil, fmt.Errorf("register txn participant: %w", err)
		}
		rec, err := s.decideTxn(ctx, cmd.TxnID, decision)
		if err != nil {
			return nil, fmt.Errorf("txn decide: %w", err)
		}
		// Ensure the current participant is recorded even if the txn record pre-existed.
		currentParticipant := TxnParticipant{Namespace: namespace, Key: keyComponent, BackendHash: s.backendHash}
		if idx := participantIndex(rec.Participants, currentParticipant); idx == -1 {
			rec.Participants = append(rec.Participants, currentParticipant)
			sortParticipants(rec.Participants)
			_, _ = s.putTxnRecord(ctx, rec, "")
		} else if rec.Participants[idx].BackendHash == "" && s.backendHash != "" {
			rec.Participants[idx].BackendHash = s.backendHash
			sortParticipants(rec.Participants)
			_, _ = s.putTxnRecord(ctx, rec, "")
		}
		decisionCommit := rec.State == TxnStateCommit
		// Apply to the current participant using the already loaded meta to avoid extra reads.
		if err := s.applyTxnDecisionForMeta(ctx, namespace, keyComponent, rec.TxnID, decisionCommit, meta, metaETag); err != nil {
			return nil, err
		}
		// Fan-out to remaining participants.
		for _, p := range rec.Participants {
			if p.BackendHash != "" && s.backendHash != "" && p.BackendHash != s.backendHash {
				continue
			}
			if p.Namespace == namespace && p.Key == keyComponent && (p.BackendHash == "" || p.BackendHash == s.backendHash) {
				continue
			}
			if err := s.applyTxnDecisionToKey(ctx, p.Namespace, p.Key, rec.TxnID, decisionCommit); err != nil && s.logger != nil {
				s.logger.Warn("txn.apply.failed",
					"namespace", p.Namespace,
					"key", p.Key,
					"txn_id", rec.TxnID,
					"decision", rec.State,
					"error", err)
			}
		}
		if s.leaseMetrics != nil {
			s.leaseMetrics.addActive(kindLabel, -1)
		}
		return &ReleaseResult{
			Released:    true,
			MetaCleared: true,
		}, nil
	}
}

// --- helpers copied/trimmed from httpapi handler ---

func (s *Service) resolveNamespace(ns string) (string, error) {
	if strings.TrimSpace(ns) == "" {
		ns = s.defaultNamespace
	}
	normalized, err := namespaces.Normalize(ns, s.defaultNamespace)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(normalized, ".") {
		return "", fmt.Errorf("namespace %q is reserved", normalized)
	}
	return normalized, nil
}

func (s *Service) observeNamespace(ns string) {
	if s.namespaceTracker != nil {
		s.namespaceTracker.Observe(ns)
	}
}

func (s *Service) resolveTTL(ttlSeconds int64) time.Duration {
	if ttlSeconds <= 0 {
		return s.defaultTTL.Default
	}
	ttl := time.Duration(ttlSeconds) * time.Second
	if s.defaultTTL.Max > 0 && ttl > s.defaultTTL.Max {
		return s.defaultTTL.Max
	}
	return ttl
}

func (s *Service) resolveBlock(blockSeconds int64) (time.Duration, bool) {
	switch blockSeconds {
	case api.BlockNoWait:
		return 0, false
	case 0:
		if s.acquireBlock <= 0 {
			return 0, true
		}
		return s.acquireBlock, true
	default:
		if blockSeconds < 0 {
			return s.acquireBlock, false
		}
		return time.Duration(blockSeconds) * time.Second, false
	}
}

func (s *Service) namespacedKey(namespace, key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("key required")
	}
	key = strings.TrimPrefix(key, "/")
	normalized, err := namespaces.NormalizeKey(key)
	if err != nil {
		return "", err
	}
	if namespace == "" {
		return normalized, nil
	}
	return namespace + "/" + normalized, nil
}

func relativeKey(namespace, namespaced string) string {
	if namespace == "" {
		return namespaced
	}
	prefix := namespace + "/"
	return strings.TrimPrefix(namespaced, prefix)
}

func (s *Service) creationMutex(key string) *sync.Mutex {
	// Basic per-key creation mutex; we don't persist across restarts and that's fine.
	if s == nil {
		return &sync.Mutex{}
	}
	if s.createLocks == nil {
		s.createLocks = &sync.Map{}
	}
	mu, _ := s.createLocks.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Service) ensureMeta(ctx context.Context, namespace, key string) (*storage.Meta, string, error) {
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = s.logger
	}
	relKey := relativeKey(namespace, key)
	res, err := s.store.LoadMeta(ctx, namespace, relKey)
	if err == nil {
		return res.Meta, res.ETag, nil
	}
	if errors.Is(err, storage.ErrNotFound) {
		return &storage.Meta{}, "", nil
	}
	return nil, "", fmt.Errorf("load meta: %w", err)
}

func (s *Service) loadMetaMaybeCached(ctx context.Context, namespace, storageKey string, cached *storage.Meta, cachedETag string) (*storage.Meta, string, error) {
	if cached != nil {
		clone := cloneMeta(*cached)
		return &clone, cachedETag, nil
	}
	return s.ensureMeta(ctx, namespace, storageKey)
}

func cloneMeta(meta storage.Meta) storage.Meta {
	clone := meta
	if meta.Lease != nil {
		leaseCopy := *meta.Lease
		clone.Lease = &leaseCopy
	}
	if len(meta.StateDescriptor) > 0 {
		clone.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
	}
	if len(meta.Attachments) > 0 {
		clone.Attachments = make([]storage.Attachment, len(meta.Attachments))
		for i, att := range meta.Attachments {
			clone.Attachments[i] = cloneAttachment(att)
		}
	}
	if len(meta.StagedAttachments) > 0 {
		clone.StagedAttachments = make([]storage.StagedAttachment, len(meta.StagedAttachments))
		for i, att := range meta.StagedAttachments {
			clone.StagedAttachments[i] = cloneStagedAttachment(att)
		}
	}
	if len(meta.StagedAttachmentDeletes) > 0 {
		clone.StagedAttachmentDeletes = append([]string(nil), meta.StagedAttachmentDeletes...)
	}
	return clone
}

func cloneAttachment(att storage.Attachment) storage.Attachment {
	clone := att
	if len(att.Descriptor) > 0 {
		clone.Descriptor = append([]byte(nil), att.Descriptor...)
	}
	return clone
}

func cloneStagedAttachment(att storage.StagedAttachment) storage.StagedAttachment {
	clone := att
	if len(att.StagedDescriptor) > 0 {
		clone.StagedDescriptor = append([]byte(nil), att.StagedDescriptor...)
	}
	return clone
}

func (s *Service) maybeThrottleLock() error {
	if s.qrf == nil {
		return nil
	}
	decision := s.qrf.Decide(qrf.KindLock)
	if !decision.Throttle {
		return nil
	}
	retry := durationToSeconds(decision.RetryAfter)
	if retry <= 0 {
		retry = 1
	}
	return Failure{
		Code:       "throttled",
		Detail:     "perimeter defence engaged",
		RetryAfter: retry,
		HTTPStatus: 429,
	}
}

func (s *Service) maybeThrottleQueue(kind qrf.Kind) error {
	if s.qrf == nil {
		return nil
	}
	decision := s.qrf.Decide(kind)
	if !decision.Throttle {
		return nil
	}
	retry := durationToSeconds(decision.RetryAfter)
	if retry <= 0 {
		retry = 1
	}
	return Failure{
		Code:       "throttled",
		Detail:     "perimeter defence engaged",
		RetryAfter: retry,
		HTTPStatus: 429,
	}
}

func (s *Service) beginLockOp() func() {
	if s.lsf == nil {
		return func() {}
	}
	return s.lsf.BeginLockOp()
}

func (s *Service) generateUniqueKey(ctx context.Context, namespace string) (string, error) {
	const maxAttempts = 5
	var err error
	for range maxAttempts {
		candidate := uuidv7.NewString()
		if _, err = s.namespacedKey(namespace, candidate); err != nil {
			return "", err
		}
		_, err = s.store.LoadMeta(ctx, namespace, candidate)
		if errors.Is(err, storage.ErrNotFound) {
			return candidate, nil
		}
		if err != nil {
			return "", fmt.Errorf("load meta: %w", err)
		}
	}
	return "", fmt.Errorf("unable to allocate unique key after %d attempts", maxAttempts)
}

func (s *Service) currentShutdownState() ShutdownState {
	if s.shutdownState == nil {
		return ShutdownState{}
	}
	return s.shutdownState()
}

// --- small helpers ---

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func durationToSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(d.Seconds()))
}

func pickStagedVersion(meta *storage.Meta) int64 {
	if meta == nil {
		return 0
	}
	if meta.StagedVersion > 0 {
		return meta.StagedVersion
	}
	return meta.Version + 1
}

// acquire backoff (simplified copy)
const (
	acquireBackoffStart      = 500 * time.Millisecond
	acquireBackoffMax        = 5 * time.Second
	acquireBackoffMin        = 250 * time.Millisecond
	acquireBackoffMultiplier = 1.3
	acquireBackoffJitter     = 100 * time.Millisecond
)

type acquireBackoff struct {
	next time.Duration
}

func newAcquireBackoff() *acquireBackoff {
	return &acquireBackoff{next: acquireBackoffStart}
}

func (b *acquireBackoff) Next(limit time.Duration) time.Duration {
	sleep := b.next
	if limit > 0 && sleep > limit {
		sleep = limit
	}
	b.next = time.Duration(float64(b.next)*acquireBackoffMultiplier + float64(acquireBackoffJitter))
	if b.next > acquireBackoffMax {
		b.next = acquireBackoffMax
	}
	if b.next < acquireBackoffMin {
		b.next = acquireBackoffMin
	}
	return sleep
}
