package core

import (
	"context"
	"errors"
	"strings"
	"time"

	"pkt.systems/lockd/internal/storage"
)

const haMemberPrefix = "members/"
const haMemberModeAttr = "ha_mode"

const (
	haMemberModeAuto   = "auto"
	haMemberModeSingle = "single"
)

type haPeerPresence struct {
	nodeID      string
	mode        string
	reason      string
	expiresUnix int64
}

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
	interval := haRefreshInterval(s.haLeaseTTL)
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
	prevPassiveUntil := s.haPassiveUntil.Load()
	if err := s.writeHAMember(ctx, now.Unix(), now.Add(s.haLeaseTTL).Unix(), haMemberModeAuto); err != nil {
		s.logger.Warn("ha.auto.heartbeat_failed", "error", err)
		return
	}
	if probe, ok := s.store.(storage.ExclusiveWriterProbe); ok {
		status, err := probe.ProbeExclusiveWriter(ctx)
		if err != nil && !errors.Is(err, storage.ErrNotImplemented) {
			s.logger.Warn("ha.auto.exclusive_writer_probe_failed", "error", err)
			return
		}
		if err == nil && status.Present && status.ExpiresAtUnix > now.Unix() {
			s.setAutoPassiveUntil(status.ExpiresAtUnix)
			return
		}
	}
	peer, err := s.autoHAPeerPresent(ctx, now.Unix())
	if err != nil {
		s.logger.Warn("ha.auto.scan_failed", "error", err)
		return
	}
	if peer.mode == haMemberModeSingle {
		s.setAutoPassiveUntil(peer.expiresUnix)
		return
	}
	if prevPassiveUntil > 0 {
		s.setAutoPassiveUntil(0)
		s.promoteAutoToFailover("single_fence_cleared")
		return
	}
	s.setAutoPassiveUntil(0)
	s.setNodeActive(true, 0, s.haNodeID)
	if peer.reason != "" {
		s.promoteAutoToFailover(peer.reason)
	}
}

func (s *Service) promoteAutoToFailover(reason string) {
	if s == nil || !strings.EqualFold(s.haMode, "auto") {
		return
	}
	if !s.haUsesLease.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	s.releaseHAMember(ctx)
	cancel()
	s.logger.Info("ha.auto.promote", "reason", reason, "node_id", s.haNodeID)
	s.setNodeActive(false, 0, "")
	s.startHA()
}

func (s *Service) writeHAMember(ctx context.Context, nowUnix, expiresUnix int64, mode string) error {
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
			Attributes: map[string]string{
				haMemberModeAttr: mode,
			},
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

func (s *Service) autoHAPeerPresent(ctx context.Context, nowUnix int64) (haPeerPresence, error) {
	if s == nil || s.store == nil {
		return haPeerPresence{}, nil
	}
	keys, err := s.store.ListMetaKeys(ctx, haNamespace)
	if err != nil {
		return haPeerPresence{}, err
	}
	self := haMemberKey(s.haNodeID)
	var autoPeer haPeerPresence
	var singlePeer haPeerPresence
	for _, key := range keys {
		switch {
		case key == haLeaseKey:
			meta, _, err := s.loadHALease(ctx)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				return haPeerPresence{}, err
			}
			if meta != nil && meta.Lease != nil && meta.Lease.ExpiresAtUnix > nowUnix {
				owner := strings.TrimSpace(meta.Lease.Owner)
				if owner != "" && owner != s.haNodeID {
					return haPeerPresence{
						nodeID:      owner,
						mode:        haMemberModeAuto,
						reason:      "activelease:" + owner,
						expiresUnix: meta.Lease.ExpiresAtUnix,
					}, nil
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
				return haPeerPresence{}, err
			}
			if res.Meta != nil && res.Meta.Lease != nil && res.Meta.Lease.ExpiresAtUnix > nowUnix {
				mode := haMemberModeAuto
				if got, ok := res.Meta.GetAttribute(haMemberModeAttr); ok && strings.EqualFold(strings.TrimSpace(got), haMemberModeSingle) {
					mode = haMemberModeSingle
				}
				nodeID := strings.TrimSpace(strings.TrimPrefix(key, haMemberPrefix))
				if leaseOwner := strings.TrimSpace(res.Meta.Lease.Owner); leaseOwner != "" {
					nodeID = leaseOwner
				}
				peer := haPeerPresence{
					nodeID:      nodeID,
					mode:        mode,
					reason:      "member:" + strings.TrimPrefix(key, haMemberPrefix),
					expiresUnix: res.Meta.Lease.ExpiresAtUnix,
				}
				if mode == haMemberModeSingle {
					if singlePeer.reason == "" || compareHANodeID(peer.nodeID, singlePeer.nodeID) < 0 {
						singlePeer = peer
					}
					continue
				}
				if autoPeer.reason == "" {
					autoPeer = peer
				}
			}
		}
	}
	if singlePeer.reason != "" {
		return singlePeer, nil
	}
	return autoPeer, nil
}

func compareHANodeID(a, b string) int {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	switch {
	case a == b:
		return 0
	case a == "":
		return 1
	case b == "":
		return -1
	case a < b:
		return -1
	default:
		return 1
	}
}

func (s *Service) releaseHAMember(ctx context.Context) {
	if s == nil || s.store == nil || (!strings.EqualFold(s.haMode, "auto") && !strings.EqualFold(s.haMode, "single")) {
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
		s.logger.Warn("ha.member.release_failed", "key", key, "error", err)
	}
}

func (s *Service) startSinglePresence() {
	if s == nil || s.store == nil || !strings.EqualFold(s.haMode, "single") {
		return
	}
	if s.haSingleStop != nil {
		return
	}
	s.haSingleStop = make(chan struct{})
	s.haSingleDone = make(chan struct{})
	s.singleModeRefresh()
	go s.singlePresenceLoop()
}

func (s *Service) singlePresenceLoop() {
	defer close(s.haSingleDone)
	interval := s.haSingleTTL * 7 / 10
	if interval <= 0 || (s.haLeaseTTL > 0 && haRefreshInterval(s.haLeaseTTL) < interval) {
		interval = haRefreshInterval(s.haLeaseTTL)
	}
	if interval < 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.singleModeRefresh()
		select {
		case <-ticker.C:
		case <-s.haSingleStop:
			return
		}
	}
}

func (s *Service) singleModeRefresh() {
	if s == nil || s.store == nil || !strings.EqualFold(s.haMode, "single") {
		return
	}
	timeoutBase := s.haSingleTTL
	if timeoutBase <= 0 {
		timeoutBase = s.haLeaseTTL
	}
	if timeoutBase <= 0 {
		timeoutBase = 2 * time.Second
	}
	timeout := minDuration(2*time.Second, timeoutBase)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	now := s.clock.Now()
	nativeProbeHealthy := false
	keepHAMember := false

	if probe, ok := s.store.(storage.ExclusiveWriterProbe); ok {
		status, err := probe.ProbeExclusiveWriter(ctx)
		if err != nil && !errors.Is(err, storage.ErrNotImplemented) {
			s.logger.Warn("ha.single.exclusive_writer_probe_failed", "error", err)
			return
		} else if err == nil {
			nativeProbeHealthy = true
		}
		if err == nil && status.Present && status.ExpiresAtUnix > now.Unix() {
			s.setAutoPassiveUntil(status.ExpiresAtUnix)
			s.releaseHAMember(ctx)
			s.setNodeActive(false, status.ExpiresAtUnix, "")
			return
		}
	}

	peer, err := s.autoHAPeerPresent(ctx, now.Unix())
	if err != nil {
		s.logger.Warn("ha.single.scan_failed", "error", err)
		return
	}
	if peer.reason != "" && (peer.mode == haMemberModeSingle || strings.HasPrefix(peer.reason, "activelease:")) {
		s.setAutoPassiveUntil(peer.expiresUnix)
		s.releaseHAMember(ctx)
		s.setNodeActive(false, peer.expiresUnix, "")
		return
	}

	serializationTTL := s.haSingleTTL
	if nativeProbeHealthy {
		serializationTTL = maxDuration(serializationTTL, maxDuration(timeoutBase, 5*time.Second))
	} else if serializationTTL > 0 {
		keepHAMember = true
	}
	if serializationTTL > 0 {
		if err := s.writeHAMember(ctx, now.Unix(), now.Add(serializationTTL).Unix(), haMemberModeSingle); err != nil {
			s.logger.Warn("ha.single.presence_failed", "error", err)
			return
		}
		peer, err = s.autoHAPeerPresent(ctx, now.Unix())
		if err != nil {
			s.logger.Warn("ha.single.scan_failed", "error", err)
			if !keepHAMember {
				s.releaseHAMember(ctx)
			}
			return
		}
		if strings.HasPrefix(peer.reason, "activelease:") || (peer.mode == haMemberModeSingle && compareHANodeID(peer.nodeID, s.haNodeID) < 0) {
			s.setAutoPassiveUntil(peer.expiresUnix)
			s.releaseHAMember(ctx)
			s.setNodeActive(false, peer.expiresUnix, "")
			return
		}
	}
	if !keepHAMember {
		s.releaseHAMember(ctx)
	}
	s.setAutoPassiveUntil(0)
	s.setNodeActive(true, 0, s.haNodeID)
}

func (s *Service) setAutoPassiveUntil(expiresUnix int64) {
	if s == nil {
		return
	}
	s.haPassiveUntil.Store(expiresUnix)
}
