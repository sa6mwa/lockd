package core

import (
	"context"
	"errors"
	"strings"
	"time"

	"pkt.systems/lockd/internal/storage"
)

const haMemberPrefix = "members/"

func haMemberKey(nodeID string) string {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return ""
	}
	return haMemberPrefix + nodeID
}

func (s *Service) startAutoHA() {
	if s == nil || s.store == nil || !strings.EqualFold(s.haMode, "auto") || s.haLeaseTTL <= 0 {
		return
	}
	if s.haAutoStop != nil {
		return
	}
	s.haAutoStop = make(chan struct{})
	s.haAutoDone = make(chan struct{})
	s.autoHARefresh()
	go s.autoHALoop()
}

func (s *Service) autoHALoop() {
	defer close(s.haAutoDone)
	interval := s.haLeaseTTL / 2
	if interval < 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if s.usesHALease() {
			return
		}
		s.autoHARefresh()
		select {
		case <-ticker.C:
		case <-s.haAutoStop:
			return
		}
	}
}

func (s *Service) autoHARefresh() {
	if s == nil || s.store == nil || !strings.EqualFold(s.haMode, "auto") || s.haLeaseTTL <= 0 || s.usesHALease() {
		return
	}
	timeout := minDuration(2*time.Second, s.haLeaseTTL)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	now := s.clock.Now()
	if err := s.writeHAMember(ctx, now.Unix(), now.Add(s.haLeaseTTL).Unix()); err != nil {
		s.logger.Warn("ha.auto.heartbeat_failed", "error", err)
		return
	}
	present, reason, err := s.autoHAPeerPresent(ctx, now.Unix())
	if err != nil {
		s.logger.Warn("ha.auto.scan_failed", "error", err)
		return
	}
	if present {
		s.promoteAutoToFailover(reason)
	}
}

func (s *Service) promoteAutoToFailover(reason string) {
	if s == nil || !strings.EqualFold(s.haMode, "auto") {
		return
	}
	if !s.haUsesLease.CompareAndSwap(false, true) {
		return
	}
	s.logger.Info("ha.auto.promote", "reason", reason, "node_id", s.haNodeID)
	s.setNodeActive(false, 0, "")
	s.startHA()
}

func (s *Service) writeHAMember(ctx context.Context, nowUnix, expiresUnix int64) error {
	if s == nil || s.store == nil {
		return nil
	}
	key := haMemberKey(s.haNodeID)
	if key == "" {
		return nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		res, err := s.store.LoadMeta(ctx, haNamespace, key)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		version := int64(1)
		expected := ""
		if err == nil && res.Meta != nil {
			version = nextVersion(res.Meta.Version)
			expected = res.ETag
		}
		_, err = s.store.StoreMeta(ctx, haNamespace, key, &storage.Meta{
			Version:       version,
			UpdatedAtUnix: nowUnix,
			Lease: &storage.Lease{
				ID:            s.haNodeID,
				Owner:         s.haNodeID,
				ExpiresAtUnix: expiresUnix,
			},
		}, expected)
		if err == nil {
			return nil
		}
		if errors.Is(err, storage.ErrCASMismatch) && attempt == 0 {
			continue
		}
		return err
	}
	return nil
}

func (s *Service) autoHAPeerPresent(ctx context.Context, nowUnix int64) (bool, string, error) {
	if s == nil || s.store == nil {
		return false, "", nil
	}
	keys, err := s.store.ListMetaKeys(ctx, haNamespace)
	if err != nil {
		return false, "", err
	}
	self := haMemberKey(s.haNodeID)
	for _, key := range keys {
		switch {
		case key == haLeaseKey:
			meta, _, err := s.loadHALease(ctx)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				return false, "", err
			}
			if meta != nil && meta.Lease != nil && meta.Lease.ExpiresAtUnix > nowUnix {
				owner := strings.TrimSpace(meta.Lease.Owner)
				if owner != "" && owner != s.haNodeID {
					return true, "activelease:" + owner, nil
				}
			}
		case strings.HasPrefix(key, haMemberPrefix):
			if key == self {
				continue
			}
			res, err := s.store.LoadMeta(ctx, haNamespace, key)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				return false, "", err
			}
			if res.Meta != nil && res.Meta.Lease != nil && res.Meta.Lease.ExpiresAtUnix > nowUnix {
				return true, "member:" + strings.TrimPrefix(key, haMemberPrefix), nil
			}
		}
	}
	return false, "", nil
}

func (s *Service) releaseHAMember(ctx context.Context) {
	if s == nil || s.store == nil || !strings.EqualFold(s.haMode, "auto") {
		return
	}
	key := haMemberKey(s.haNodeID)
	if key == "" {
		return
	}
	res, err := s.store.LoadMeta(ctx, haNamespace, key)
	if err != nil || res.Meta == nil {
		return
	}
	if err := s.store.DeleteMeta(ctx, haNamespace, key, res.ETag); err != nil && !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, storage.ErrCASMismatch) {
		s.logger.Warn("ha.auto.member.release_failed", "key", key, "error", err)
	}
}
