package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"pkt.systems/lockd/internal/storage"
)

// Describe returns metadata for a key.
func (s *Service) Describe(ctx context.Context, cmd DescribeCommand) (*DescribeResult, error) {
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
	if err := s.maybeThrottleLock(); err != nil {
		return nil, err
	}
	finish := s.beginLockOp()
	defer finish()

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

	meta, _, err := s.ensureMeta(ctx, namespace, storageKey)
	if err != nil {
		return nil, err
	}
	publishedVersion := meta.PublishedVersion
	if publishedVersion == 0 {
		publishedVersion = meta.Version
	}
	expectState := meta.StateETag != ""
	publicRead := cmd.Public && cmd.LeaseID == ""
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
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, s.clock.Now()); err != nil {
			return nil, err
		}
	}

	stateCtx := ctx
	if len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	reader, info, err := s.readStateWithWarmup(stateCtx, namespace, storageKey, expectState)
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

func (s *Service) readStateWithWarmup(ctx context.Context, namespace, key string, expectState bool) (io.ReadCloser, *storage.StateInfo, error) {
	relKey := relativeKey(namespace, key)
	reader, info, err := s.store.ReadState(ctx, namespace, relKey)
	if err == nil || !expectState || !errors.Is(err, storage.ErrNotFound) {
		return reader, info, err
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
		reader, info, err = s.store.ReadState(ctx, namespace, relKey)
		if err == nil || !errors.Is(err, storage.ErrNotFound) {
			return reader, info, err
		}
	}
	return reader, info, err
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
