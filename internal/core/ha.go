package core

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"pkt.systems/lockd/internal/storage"
)

const (
	haNamespace = ".ha"
	haLeaseKey  = "activelease"
)

func (s *Service) startHA() {
	if s == nil || s.haMode != "failover" || s.haLeaseTTL <= 0 || s.store == nil {
		return
	}
	if s.haStop != nil {
		return
	}
	s.haStop = make(chan struct{})
	s.haDone = make(chan struct{})
	s.haRefresh()
	go s.haLoop()
}

// StopHA stops the HA lease refresh loop.
func (s *Service) StopHA() {
	if s == nil || s.haStop == nil {
		return
	}
	close(s.haStop)
	if s.haDone != nil {
		<-s.haDone
		s.haDone = nil
	}
	s.haStop = nil
}

// ReleaseHA releases the failover lease held by this node, if present.
func (s *Service) ReleaseHA(ctx context.Context) {
	if s == nil || s.store == nil || s.haMode != "failover" {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for attempt := 0; attempt < 2; attempt++ {
		meta, etag, err := s.loadHALease(ctx)
		if err != nil || meta == nil || meta.Lease == nil {
			return
		}
		if strings.TrimSpace(meta.Lease.Owner) != s.haNodeID {
			return
		}
		now := s.clock.Now().Unix()
		release := cloneMeta(*meta)
		release.Version = nextVersion(meta.Version)
		release.UpdatedAtUnix = now
		release.Lease = &storage.Lease{
			ID:            s.haNodeID,
			Owner:         s.haNodeID,
			ExpiresAtUnix: 0,
		}
		if err := s.writeHALease(ctx, &release, etag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) && attempt == 0 {
				continue
			}
			s.logger.Warn("ha.lease.release_failed", "error", err)
			return
		}
		s.setNodeActive(false, now, s.haNodeID)
		return
	}
}

func (s *Service) haLoop() {
	defer close(s.haDone)
	interval := s.haLeaseTTL / 2
	if interval < 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.haRefresh()
		select {
		case <-ticker.C:
		case <-s.haStop:
			return
		}
	}
}

// RequireNodeActive returns an error when this node is not active in HA mode.
func (s *Service) RequireNodeActive() error {
	return s.ensureNodeActive()
}

// NodeActive reports whether this node currently holds the failover lease without
// attempting to refresh it.
func (s *Service) NodeActive() bool {
	if s == nil || s.haMode != "failover" {
		return true
	}
	if !s.haActive.Load() {
		return false
	}
	if expires := s.haLeaseExpires.Load(); expires > 0 {
		now := s.clock.Now().Unix()
		if expires <= now {
			return false
		}
	}
	return true
}

func (s *Service) ensureNodeActive() error {
	if s == nil || s.haMode != "failover" {
		return nil
	}
	if s.NodeActive() {
		return nil
	}
	s.haRefresh()
	if s.NodeActive() {
		return nil
	}
	retryAfter := int64(1)
	if expires := s.haLeaseExpires.Load(); expires > 0 {
		now := s.clock.Now().Unix()
		if expires > now {
			retryAfter = expires - now
		}
	}
	return Failure{
		Code:       "node_passive",
		Detail:     "server is passive for this backend; retry another node",
		RetryAfter: retryAfter,
		HTTPStatus: http.StatusServiceUnavailable,
	}
}

func (s *Service) haRefresh() {
	if s == nil || s.store == nil {
		return
	}
	s.haRefreshes.Add(1)
	timeout := minDuration(2*time.Second, s.haLeaseTTL)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	now := s.clock.Now()
	expiresAt := now.Add(s.haLeaseTTL)
	expiresUnix := expiresAt.Unix()

	meta, etag, err := s.loadHALease(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			err = s.writeHALease(ctx, &storage.Meta{
				Version:       1,
				UpdatedAtUnix: now.Unix(),
				Lease: &storage.Lease{
					ID:            s.haNodeID,
					Owner:         s.haNodeID,
					ExpiresAtUnix: expiresUnix,
				},
			}, "")
			if err == nil {
				s.setNodeActive(true, expiresUnix, s.haNodeID)
				return
			}
			s.haErrors.Add(1)
			if errors.Is(err, storage.ErrCASMismatch) {
				if s.refreshHALeaseOnMismatch(ctx, now.Unix()) {
					return
				}
				s.setNodeActive(false, 0, "")
				return
			}
			if s.haActive.Load() && s.haLeaseExpires.Load() > now.Unix() {
				s.logger.Warn("ha.lease.claim_failed", "error", err, "action", "keep_active")
				return
			}
			s.setNodeActive(false, 0, "")
			s.logger.Warn("ha.lease.claim_failed", "error", err)
			return
		}
		s.haErrors.Add(1)
		if s.haActive.Load() && s.haLeaseExpires.Load() > now.Unix() {
			s.logger.Warn("ha.lease.read_failed", "error", err, "action", "keep_active")
			return
		}
		s.setNodeActive(false, 0, "")
		s.logger.Warn("ha.lease.read_failed", "error", err)
		return
	}

	owner := ""
	if meta.Lease != nil {
		owner = strings.TrimSpace(meta.Lease.Owner)
	}
	if meta.Lease == nil || meta.Lease.ExpiresAtUnix <= now.Unix() {
		prevExpires := int64(0)
		if meta.Lease != nil {
			prevExpires = meta.Lease.ExpiresAtUnix
		}
		s.haLeaseExpires.Store(prevExpires)
		claimValue := cloneMeta(*meta)
		claimValue.Version = nextVersion(meta.Version)
		claimValue.UpdatedAtUnix = now.Unix()
		claimValue.Lease = &storage.Lease{
			ID:            s.haNodeID,
			Owner:         s.haNodeID,
			ExpiresAtUnix: expiresUnix,
		}
		if err := s.writeHALease(ctx, &claimValue, etag); err != nil {
			s.haErrors.Add(1)
			if errors.Is(err, storage.ErrCASMismatch) {
				if s.refreshHALeaseOnMismatch(ctx, now.Unix()) {
					return
				}
				s.setNodeActive(false, prevExpires, owner)
				return
			}
			if s.haActive.Load() && prevExpires > now.Unix() {
				s.logger.Warn("ha.lease.claim_failed", "error", err, "action", "keep_active")
				return
			}
			s.setNodeActive(false, prevExpires, owner)
			s.logger.Warn("ha.lease.claim_failed", "error", err)
			return
		}
		s.setNodeActive(true, expiresUnix, s.haNodeID)
		return
	}

	s.haLeaseExpires.Store(meta.Lease.ExpiresAtUnix)
	if owner == s.haNodeID {
		renewValue := cloneMeta(*meta)
		renewValue.Version = nextVersion(meta.Version)
		renewValue.UpdatedAtUnix = now.Unix()
		renewValue.Lease = &storage.Lease{
			ID:            s.haNodeID,
			Owner:         s.haNodeID,
			ExpiresAtUnix: expiresUnix,
		}
		if err := s.writeHALease(ctx, &renewValue, etag); err != nil {
			s.haErrors.Add(1)
			if errors.Is(err, storage.ErrCASMismatch) {
				if s.refreshHALeaseOnMismatch(ctx, now.Unix()) {
					return
				}
			}
			if s.haActive.Load() && meta.Lease.ExpiresAtUnix > now.Unix() {
				s.logger.Warn("ha.lease.renew_failed", "error", err, "action", "keep_active")
				return
			}
			s.setNodeActive(false, meta.Lease.ExpiresAtUnix, owner)
			s.logger.Warn("ha.lease.renew_failed", "error", err)
			return
		}
		s.setNodeActive(true, expiresUnix, owner)
		return
	}

	s.setNodeActive(false, meta.Lease.ExpiresAtUnix, owner)
}

func (s *Service) refreshHALeaseOnMismatch(ctx context.Context, nowUnix int64) bool {
	if s == nil || s.store == nil {
		return false
	}
	meta, _, err := s.loadHALease(ctx)
	if err != nil || meta == nil || meta.Lease == nil {
		return false
	}
	owner := strings.TrimSpace(meta.Lease.Owner)
	expires := meta.Lease.ExpiresAtUnix
	if owner == s.haNodeID && expires > nowUnix {
		s.setNodeActive(true, expires, owner)
		return true
	}
	s.setNodeActive(false, expires, owner)
	return true
}

func (s *Service) loadHALease(ctx context.Context) (*storage.Meta, string, error) {
	res, err := s.store.LoadMeta(ctx, haNamespace, haLeaseKey)
	if err != nil {
		return nil, "", err
	}
	if res.Meta == nil {
		return nil, res.ETag, storage.ErrNotFound
	}
	return res.Meta, res.ETag, nil
}

func (s *Service) writeHALease(ctx context.Context, meta *storage.Meta, expected string) error {
	_, err := s.store.StoreMeta(ctx, haNamespace, haLeaseKey, meta, expected)
	return err
}

func (s *Service) setNodeActive(active bool, expiresUnix int64, owner string) {
	s.haLeaseExpires.Store(expiresUnix)
	if s.haActive.Load() == active {
		return
	}
	s.haActive.Store(active)
	s.haTransitions.Add(1)
	if ctrl, ok := s.store.(storage.SingleWriterControl); ok {
		ctrl.SetSingleWriter(active)
	}
	s.logger.Info("node.state.changed",
		"active", active,
		"owner", owner,
		"expires_at_unix", expiresUnix,
	)
}

func nextVersion(version int64) int64 {
	if version <= 0 {
		return 1
	}
	return version + 1
}
