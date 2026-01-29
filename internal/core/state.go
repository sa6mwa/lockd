package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"pkt.systems/lockd/internal/storage"
)

// Describe returns metadata for a key.
func (s *Service) Describe(ctx context.Context, cmd DescribeCommand) (*DescribeResult, error) {
	if err := s.ensureNodeActive(); err != nil {
		return nil, err
	}
	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	s.observeNamespace(namespace)
	if cmd.Key == "" {
		return nil, Failure{Code: "missing_key", Detail: "key query required", HTTPStatus: http.StatusBadRequest}
	}
	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	meta, _, err := s.ensureMeta(ctx, namespace, storageKey)
	if err != nil {
		return nil, err
	}
	resp := &DescribeResult{
		Namespace: namespace,
		Key:       cmd.Key,
		Version:   meta.Version,
		StateETag: meta.StateETag,
		UpdatedAt: meta.UpdatedAtUnix,
		Fencing:   meta.FencingToken,
		Meta:      meta,
	}
	if meta.Lease != nil {
		resp.LeaseID = meta.Lease.ID
		resp.Owner = meta.Lease.Owner
		resp.ExpiresAt = meta.Lease.ExpiresAtUnix
	}
	return resp, nil
}

// Get streams state for a key. Public reads skip lease validation but require published state.
func (s *Service) Get(ctx context.Context, cmd GetCommand) (*GetResult, error) {
	if err := s.maybeThrottleLock(ctx); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

	if !s.maybeReplayTxnRecordsInline(ctx) {
		s.maybeReplayTxnRecords(ctx)
	}

	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	s.observeNamespace(namespace)
	if cmd.Key == "" {
		return nil, Failure{Code: "missing_params", Detail: "key query required", HTTPStatus: http.StatusBadRequest}
	}
	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	meta, metaETag, err := s.ensureMeta(ctx, namespace, storageKey)
	if err != nil {
		return nil, err
	}
	relKey := relativeKey(namespace, storageKey)
	publishedVersion := meta.PublishedVersion
	if publishedVersion == 0 {
		publishedVersion = meta.Version
	}
	expectState := meta.StateETag != ""
	publicRead := cmd.Public && cmd.LeaseID == ""
	txnID := ""
	if meta.Lease != nil {
		txnID = meta.Lease.TxnID
	}
	if publicRead {
		if !expectState || publishedVersion == 0 {
			return &GetResult{NoContent: true, Public: true, Meta: meta, PublishedVersion: publishedVersion}, nil
		}
		if publishedVersion < meta.Version {
			return nil, Failure{
				Code:       "state_not_published",
				Detail:     "state update not published yet",
				HTTPStatus: http.StatusServiceUnavailable,
			}
		}
	} else {
		if cmd.LeaseID == "" {
			return nil, Failure{Code: "missing_params", Detail: "lease_id required (or use public=1)", HTTPStatus: http.StatusBadRequest}
		}
		now := s.clock.Now()
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, txnID, now)
			if _, _, err := s.clearExpiredLease(ctx, namespace, relKey, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					return nil, leaseErr
				}
				return nil, err
			}
			return nil, leaseErr
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, txnID, now); err != nil {
			return nil, err
		}
	}
	if meta.StagedTxnID != "" && meta.StagedTxnID == txnID {
		if meta.StagedRemove {
			return &GetResult{NoContent: true, Public: publicRead, Meta: meta, PublishedVersion: publishedVersion}, nil
		}
		if s.staging == nil {
			return nil, Failure{Code: "staging_unavailable", Detail: "staging backend missing", HTTPStatus: http.StatusServiceUnavailable}
		}
		stagedCtx := ctx
		if s.crypto == nil || !s.crypto.Enabled() || len(meta.StagedStateDescriptor) == 0 {
			stagedCtx = storage.ContextWithStateObjectContext(ctx, storage.StateObjectContext(path.Join(namespace, relKey)))
		} else if _, err := s.crypto.MaterialFromDescriptor(storage.StateObjectContext(path.Join(namespace, relKey)), meta.StagedStateDescriptor); err == nil {
			stagedCtx = storage.ContextWithStateObjectContext(ctx, storage.StateObjectContext(path.Join(namespace, relKey)))
		}
		stateRes, err := s.staging.LoadStagedState(stagedCtx, namespace, relKey, meta.StagedTxnID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, Failure{Code: "staged_state_missing", Detail: "staged state missing", HTTPStatus: http.StatusNotFound}
			}
			return nil, fmt.Errorf("load staged state: %w", err)
		}
		reader := stateRes.Reader
		info := stateRes.Info
		if info == nil {
			info = &storage.StateInfo{}
		}
		if info.ETag == "" {
			info.ETag = meta.StagedStateETag
		}
		if info.Version == 0 {
			info.Version = meta.StagedVersion
		}
		if info.Size == 0 {
			info.Size = meta.StagedStatePlaintextBytes
		}
		if len(info.Descriptor) == 0 {
			info.Descriptor = meta.StagedStateDescriptor
		}
		return &GetResult{
			Meta:             meta,
			Info:             info,
			Reader:           reader,
			PublishedVersion: publishedVersion,
			Public:           publicRead,
		}, nil
	}

	stateCtx := ctx
	if len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	reader, info, err := s.readStateWithWarmup(stateCtx, namespace, storageKey, meta.StateETag, meta.StatePlaintextBytes, expectState)
	if errors.Is(err, storage.ErrNotFound) {
		return &GetResult{NoContent: true, Public: publicRead, Meta: meta, PublishedVersion: publishedVersion}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	return &GetResult{
		Meta:             meta,
		Info:             info,
		Reader:           reader,
		PublishedVersion: publishedVersion,
		Public:           publicRead,
	}, nil
}

func (s *Service) readStateWithWarmup(ctx context.Context, namespace, key string, stateETag string, stateSize int64, expectState bool) (io.ReadCloser, *storage.StateInfo, error) {
	relKey := relativeKey(namespace, key)
	cacheKey := ""
	if s.stateCache != nil && stateETag != "" {
		cacheKey = namespace + "|" + relKey + "|" + stateETag
		if data, ok := s.stateCache.get(cacheKey); ok {
			info := &storage.StateInfo{
				Size: int64(len(data)),
				ETag: stateETag,
			}
			return io.NopCloser(bytes.NewReader(data)), info, nil
		}
	}
	result, err := s.store.ReadState(ctx, namespace, relKey)
	reader := result.Reader
	info := result.Info
	if err == nil || !expectState || !errors.Is(err, storage.ErrNotFound) {
		return s.maybeCacheState(reader, info, cacheKey, stateETag, stateSize, err)
	}
	attemptLimit := s.stateWarmup.Attempts
	delay := s.stateWarmup.Initial
	maxDelay := s.stateWarmup.Max
	for attempt := 1; attempt <= attemptLimit; attempt++ {
		if delay > 0 {
			if waitErr := s.waitWithContext(ctx, delay); waitErr != nil {
				return nil, nil, waitErr
			}
			delay = nextWarmupDelay(delay, maxDelay)
		}
		result, err = s.store.ReadState(ctx, namespace, relKey)
		reader = result.Reader
		info = result.Info
		if err == nil || !errors.Is(err, storage.ErrNotFound) {
			return s.maybeCacheState(reader, info, cacheKey, stateETag, stateSize, err)
		}
	}
	return reader, info, err
}

func (s *Service) maybeCacheState(reader io.ReadCloser, info *storage.StateInfo, cacheKey, stateETag string, stateSize int64, err error) (io.ReadCloser, *storage.StateInfo, error) {
	if err != nil || reader == nil || s.stateCache == nil || cacheKey == "" {
		return reader, info, err
	}
	size := stateSize
	if size <= 0 && info != nil {
		size = info.Size
	}
	if size <= 0 || size > s.stateCache.maxBytes {
		return reader, info, err
	}
	data, readErr := io.ReadAll(reader)
	reader.Close()
	if readErr != nil {
		return nil, nil, readErr
	}
	s.stateCache.put(cacheKey, data)
	outInfo := &storage.StateInfo{Size: int64(len(data)), ETag: stateETag}
	if info != nil {
		cloned := *info
		cloned.Size = int64(len(data))
		cloned.ETag = stateETag
		outInfo = &cloned
	}
	return io.NopCloser(bytes.NewReader(data)), outInfo, nil
}

func (s *Service) waitWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextWarmupDelay(current, max time.Duration) time.Duration {
	if current <= 0 {
		return 0
	}
	next := current * 2
	if max > 0 && next > max {
		next = max
	}
	return next
}
