package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

func normalizeETag(etag string) string {
	return strings.Trim(strings.TrimSpace(etag), "\"")
}

func normalizeExpectedSHA256(raw string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return "", nil
	}
	if len(value) != 64 {
		return "", fmt.Errorf("expected sha256 must be 64 hex characters")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return "", fmt.Errorf("expected sha256 must be valid hex")
	}
	return hex.EncodeToString(decoded), nil
}

// UpdateCommand captures state write parameters.
type UpdateCommand struct {
	Namespace        string
	Key              string
	LeaseID          string
	FencingToken     int64
	TxnID            string
	IfVersion        int64
	IfVersionSet     bool
	IfStateETag      string
	ExpectedSHA256   string
	ExpectedBytes    int64
	ExpectedBytesSet bool
	Body             io.Reader
	CompactWriter    func(io.Writer, io.Reader, int64) error
	MaxBytes         int64
	SpoolThreshold   int64
	KnownMeta        *storage.Meta
	KnownMetaETag    string
}

// UpdateResult describes the new state metadata after a successful update.
type UpdateResult struct {
	NewVersion   int64
	NewStateETag string
	Bytes        int64
	Meta         *storage.Meta
	MetaETag     string
	Metadata     map[string]any
	Namespace    string
	Key          string
}

// Update streams new state into storage with lease/CAS enforcement.
func (s *Service) Update(ctx context.Context, cmd UpdateCommand) (*UpdateResult, error) {
	if err := s.maybeThrottleLock(ctx); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.Key) == "" {
		return nil, Failure{Code: "missing_key", Detail: "key query required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "X-Lease-ID required", HTTPStatus: http.StatusBadRequest}
	}
	if cmd.Body == nil {
		return nil, Failure{Code: "missing_body", Detail: "state body required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.TxnID) == "" {
		return nil, Failure{Code: "missing_txn", Detail: "X-Txn-ID required", HTTPStatus: http.StatusBadRequest}
	}
	expectedSHA256, err := normalizeExpectedSHA256(cmd.ExpectedSHA256)
	if err != nil {
		return nil, Failure{Code: "invalid_expected_sha256", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if cmd.ExpectedBytesSet && cmd.ExpectedBytes < 0 {
		return nil, Failure{Code: "invalid_expected_bytes", Detail: "expected bytes must be >= 0", HTTPStatus: http.StatusBadRequest}
	}

	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)
	knownMeta := cmd.KnownMeta
	knownMetaETag := cmd.KnownMetaETag

	// Enforce optional payload size limit
	limited := cmd.Body
	if cmd.MaxBytes > 0 {
		limited = io.LimitReader(cmd.Body, cmd.MaxBytes+1)
	}

	// Compact and spill to disk if needed
	counting := &countingReader{r: limited, hash: sha256.New()}
	spool := newPayloadSpool(cmd.SpoolThreshold)
	defer spool.Close()
	if err := cmd.CompactWriter(spool, counting, cmd.MaxBytes); err != nil {
		var fail Failure
		if errors.As(err, &fail) {
			return nil, fail
		}
		return nil, httpErrorFrom(err)
	}
	actualBytes := counting.Count()
	if cmd.ExpectedBytesSet && actualBytes != cmd.ExpectedBytes {
		return nil, Failure{
			Code:       "expected_bytes_mismatch",
			Detail:     fmt.Sprintf("payload bytes mismatch: expected=%d actual=%d", cmd.ExpectedBytes, actualBytes),
			HTTPStatus: http.StatusConflict,
		}
	}
	if expectedSHA256 != "" {
		actual := counting.SumHex()
		if actual != expectedSHA256 {
			return nil, Failure{
				Code:       "expected_sha256_mismatch",
				Detail:     fmt.Sprintf("payload sha256 mismatch: expected=%s actual=%s", expectedSHA256, actual),
				HTTPStatus: http.StatusConflict,
			}
		}
	}

	for {
		plan := s.newWritePlan(ctx)
		commitCtx := plan.Context()
		now := s.clock.Now()
		meta, metaETag, err := s.loadMetaMaybeCached(commitCtx, namespace, storageKey, knownMeta, knownMetaETag)
		if err != nil {
			return nil, plan.Wait(err)
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now)
			if _, _, err := s.clearExpiredLease(commitCtx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					if waitErr := plan.Wait(nil); waitErr != nil {
						return nil, waitErr
					}
					knownMeta = nil
					knownMetaETag = ""
					continue
				}
				return nil, plan.Wait(err)
			}
			return nil, plan.Wait(leaseErr)
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now); err != nil {
			return nil, plan.Wait(err)
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
		currentVersion := meta.Version
		if meta.StagedTxnID == cmd.TxnID && meta.StagedVersion > 0 {
			currentVersion = meta.StagedVersion
		}
		if cmd.IfVersionSet && currentVersion != cmd.IfVersion {
			return nil, Failure{
				Code:       "version_conflict",
				Detail:     "state version mismatch",
				Version:    currentVersion,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}
		currentETag := meta.StateETag
		if meta.StagedTxnID == cmd.TxnID && meta.StagedStateETag != "" {
			currentETag = meta.StagedStateETag
		}
		if cmd.IfStateETag != "" && normalizeETag(currentETag) != normalizeETag(cmd.IfStateETag) {
			return nil, Failure{
				Code:       "etag_mismatch",
				Detail:     "current state etag does not match",
				Version:    currentVersion,
				ETag:       currentETag,
				HTTPStatus: http.StatusConflict,
			}
		}

		// Mint staged encryption material using the committed key context so the
		// ciphertext is already valid for promotion.
		stageCtx := storage.ContextWithStateObjectContext(commitCtx, storage.StateObjectContext(path.Join(namespace, keyComponent)))
		reader, err := spool.Reader()
		if err != nil {
			return nil, plan.Wait(fmt.Errorf("spool reader: %w", err))
		}
		result, err := s.staging.StageState(stageCtx, namespace, keyComponent, cmd.TxnID, reader, storage.PutStateOptions{})
		if err != nil {
			return nil, plan.Wait(fmt.Errorf("stage state: %w", err))
		}

		meta.StagedTxnID = cmd.TxnID
		meta.StagedStateETag = result.NewETag
		meta.StagedStateDescriptor = result.Descriptor
		meta.StagedStatePlaintextBytes = result.BytesWritten
		meta.StagedRemove = false
		if meta.StagedVersion == 0 {
			meta.StagedVersion = meta.Version + 1
		}
		meta.UpdatedAtUnix = now.Unix()
		if meta.FencingToken == 0 {
			meta.FencingToken = cmd.FencingToken
		}

		newMetaETag, err := s.store.StoreMeta(commitCtx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				if waitErr := plan.Wait(nil); waitErr != nil {
					return nil, waitErr
				}
				knownMeta = nil
				knownMetaETag = ""
				continue
			}
			return nil, plan.Wait(fmt.Errorf("store meta: %w", err))
		}
		if txnExplicit(meta) {
			if _, _, err := s.enlistTxnParticipant(commitCtx, cmd.TxnID, namespace, keyComponent, meta.Lease.ExpiresAtUnix); err != nil {
				return nil, plan.Wait(fmt.Errorf("register txn participant: %w", err))
			}
		}
		if waitErr := plan.Wait(nil); waitErr != nil {
			return nil, waitErr
		}
		return &UpdateResult{
			NewVersion:   meta.StagedVersion,
			NewStateETag: meta.StagedStateETag,
			Bytes:        result.BytesWritten,
			Meta:         meta,
			MetaETag:     newMetaETag,
			Metadata:     map[string]any{},
			Namespace:    namespace,
			Key:          cmd.Key,
		}, nil
	}
}

// Remove deletes state and meta if lease matches.
func (s *Service) Remove(ctx context.Context, cmd RemoveCommand) (*RemoveResult, error) {
	if err := s.maybeThrottleLock(ctx); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.Key) == "" {
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.TxnID) == "" {
		return nil, Failure{Code: "missing_txn", Detail: "X-Txn-ID required", HTTPStatus: http.StatusBadRequest}
	}
	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)
	knownMeta := cmd.KnownMeta
	knownMetaETag := cmd.KnownMetaETag

	for {
		plan := s.newWritePlan(ctx)
		commitCtx := plan.Context()
		now := s.clock.Now()
		meta, metaETag, err := s.loadMetaMaybeCached(commitCtx, namespace, storageKey, knownMeta, knownMetaETag)
		if err != nil {
			return nil, plan.Wait(err)
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now)
			if _, _, err := s.clearExpiredLease(commitCtx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					if waitErr := plan.Wait(nil); waitErr != nil {
						return nil, waitErr
					}
					knownMeta = nil
					knownMetaETag = ""
					continue
				}
				return nil, plan.Wait(err)
			}
			return nil, plan.Wait(leaseErr)
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now); err != nil {
			return nil, plan.Wait(err)
		}
		currentVersion := meta.Version
		if meta.StagedTxnID == cmd.TxnID && meta.StagedVersion > 0 {
			currentVersion = meta.StagedVersion
		}
		if cmd.IfVersionSet && currentVersion != cmd.IfVersion {
			return nil, Failure{
				Code:       "version_conflict",
				Detail:     "state version mismatch",
				Version:    currentVersion,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}
		currentETag := meta.StateETag
		if meta.StagedTxnID == cmd.TxnID && meta.StagedStateETag != "" {
			currentETag = meta.StagedStateETag
		}
		if cmd.IfStateETag != "" && normalizeETag(currentETag) != normalizeETag(cmd.IfStateETag) {
			return nil, Failure{
				Code:       "etag_mismatch",
				Detail:     "state etag mismatch",
				Version:    currentVersion,
				ETag:       currentETag,
				HTTPStatus: http.StatusConflict,
			}
		}

		hasStagedState := meta.StagedTxnID == cmd.TxnID && (meta.StagedStateETag != "" || meta.StagedRemove)
		hasCommittedState := meta.StateETag != ""
		if !hasStagedState && !hasCommittedState {
			// Nothing to remove; no version bump.
			return &RemoveResult{
				Removed:    false,
				NewVersion: meta.Version,
				Meta:       meta,
				MetaETag:   metaETag,
			}, nil
		}

		_ = s.staging.DiscardStagedState(commitCtx, namespace, keyComponent, cmd.TxnID, storage.DiscardStagedOptions{IgnoreNotFound: true})
		meta.StagedTxnID = cmd.TxnID
		meta.StagedRemove = true
		meta.StagedStateETag = ""
		meta.StagedStateDescriptor = nil
		meta.StagedStatePlaintextBytes = 0
		meta.StagedAttributes = nil

		versionBase := meta.Version
		if meta.StagedTxnID == cmd.TxnID && meta.StagedVersion > 0 {
			versionBase = meta.StagedVersion
		}
		meta.StagedVersion = versionBase + 1
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := s.store.StoreMeta(commitCtx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				if waitErr := plan.Wait(nil); waitErr != nil {
					return nil, waitErr
				}
				knownMeta = nil
				knownMetaETag = ""
				continue
			}
			return nil, plan.Wait(fmt.Errorf("store meta: %w", err))
		}
		if txnExplicit(meta) {
			if _, _, err := s.enlistTxnParticipant(commitCtx, cmd.TxnID, namespace, keyComponent, meta.Lease.ExpiresAtUnix); err != nil {
				return nil, plan.Wait(fmt.Errorf("register txn participant: %w", err))
			}
		}
		if waitErr := plan.Wait(nil); waitErr != nil {
			return nil, waitErr
		}
		return &RemoveResult{
			Removed:    true,
			NewVersion: meta.StagedVersion,
			Meta:       meta,
			MetaETag:   newMetaETag,
		}, nil
	}
}

// Metadata mutates metadata attributes guarded by fencing.
func (s *Service) Metadata(ctx context.Context, cmd MetadataCommand) (*MetadataResult, error) {
	if err := s.maybeThrottleLock(ctx); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.Key) == "" {
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
	}
	if cmd.Mutation.empty() {
		return nil, Failure{Code: "invalid_body", Detail: "no metadata fields provided", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.TxnID) == "" {
		return nil, Failure{Code: "missing_txn", Detail: "X-Txn-ID required", HTTPStatus: http.StatusBadRequest}
	}

	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)
	knownMeta := cmd.KnownMeta
	knownMetaETag := cmd.KnownMetaETag

	for {
		plan := s.newWritePlan(ctx)
		commitCtx := plan.Context()
		now := s.clock.Now()
		meta, metaETag, err := s.loadMetaMaybeCached(commitCtx, namespace, storageKey, knownMeta, knownMetaETag)
		if err != nil {
			return nil, plan.Wait(err)
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now)
			if _, _, err := s.clearExpiredLease(commitCtx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					if waitErr := plan.Wait(nil); waitErr != nil {
						return nil, waitErr
					}
					knownMeta = nil
					knownMetaETag = ""
					continue
				}
				return nil, plan.Wait(err)
			}
			return nil, plan.Wait(leaseErr)
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now); err != nil {
			return nil, plan.Wait(err)
		}
		currentVersion := meta.Version
		if meta.StagedTxnID == cmd.TxnID && meta.StagedVersion > 0 {
			currentVersion = meta.StagedVersion
		}
		if cmd.IfVersionSet && currentVersion != cmd.IfVersion {
			return nil, Failure{
				Code:       "version_conflict",
				Detail:     "state version mismatch",
				Version:    currentVersion,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}
		currentETag := meta.StateETag
		if meta.StagedTxnID == cmd.TxnID && meta.StagedStateETag != "" {
			currentETag = meta.StagedStateETag
		}
		if cmd.IfStateETag != "" && normalizeETag(currentETag) != normalizeETag(cmd.IfStateETag) {
			return nil, Failure{
				Code:       "etag_mismatch",
				Detail:     "state etag mismatch",
				Version:    currentVersion,
				ETag:       currentETag,
				HTTPStatus: http.StatusConflict,
			}
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
		if meta.StagedAttributes == nil {
			meta.StagedAttributes = make(map[string]string)
		}
		meta.StagedTxnID = cmd.TxnID
		cmd.Mutation.apply(meta, true /* staged */)
		if meta.StagedVersion == 0 {
			meta.StagedVersion = meta.Version + 1
		}
		meta.UpdatedAtUnix = now.Unix()
		newMetaETag, err := s.store.StoreMeta(commitCtx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				if waitErr := plan.Wait(nil); waitErr != nil {
					return nil, waitErr
				}
				knownMeta = nil
				knownMetaETag = ""
				continue
			}
			return nil, plan.Wait(fmt.Errorf("store meta: %w", err))
		}
		if txnExplicit(meta) {
			if _, _, err := s.enlistTxnParticipant(commitCtx, cmd.TxnID, namespace, keyComponent, meta.Lease.ExpiresAtUnix); err != nil {
				return nil, plan.Wait(fmt.Errorf("register txn participant: %w", err))
			}
		}
		if waitErr := plan.Wait(nil); waitErr != nil {
			return nil, waitErr
		}
		logger := pslog.LoggerFromContext(ctx)
		if logger == nil {
			logger = s.logger
		}
		if logger != nil {
			logger.Debug("metadata.success",
				"namespace", namespace,
				"key", keyComponent,
				"lease_id", cmd.LeaseID,
				"txn_id", cmd.TxnID,
				"version", meta.StagedVersion,
			)
		}
		return &MetadataResult{
			Version:  meta.StagedVersion,
			Meta:     meta,
			MetaETag: newMetaETag,
		}, nil
	}
}

// --- helpers for spooling / error mapping ---

type payloadSpool struct {
	buf       []byte
	file      *os.File
	threshold int64
}

func newPayloadSpool(threshold int64) *payloadSpool {
	if threshold <= 0 {
		threshold = 4 << 20
	}
	return &payloadSpool{threshold: threshold}
}

func (p *payloadSpool) Write(data []byte) (int, error) {
	if p.file != nil {
		return p.file.Write(data)
	}
	if int64(len(p.buf)+len(data)) <= p.threshold {
		p.buf = append(p.buf, data...)
		return len(data), nil
	}
	f, err := os.CreateTemp("", "lockd-spool-*.tmp")
	if err != nil {
		return 0, err
	}
	if len(p.buf) > 0 {
		if _, err := f.Write(p.buf); err != nil {
			f.Close()
			os.Remove(f.Name())
			return 0, err
		}
	}
	p.file = f
	p.buf = nil
	return f.Write(data)
}

func (p *payloadSpool) Reader() (io.ReadSeeker, error) {
	if p.file != nil {
		if _, err := p.file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return p.file, nil
	}
	return bytes.NewReader(p.buf), nil
}

func (p *payloadSpool) Close() error {
	if p.file != nil {
		name := p.file.Name()
		err := p.file.Close()
		_ = os.Remove(name)
		p.file = nil
		return err
	}
	p.buf = nil
	return nil
}

func httpErrorFrom(err error) Failure {
	return Failure{Code: "invalid_body", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
}
