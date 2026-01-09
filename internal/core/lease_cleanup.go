package core

import (
	"context"
	"errors"
	"time"

	"pkt.systems/lockd/internal/storage"
)

func (s *Service) clearExpiredLease(ctx context.Context, namespace, key string, meta *storage.Meta, metaETag string, now time.Time, cleanupStaging bool) (bool, string, error) {
	if s == nil || meta == nil || meta.Lease == nil {
		return false, metaETag, nil
	}
	if meta.Lease.ExpiresAtUnix > now.Unix() {
		return false, metaETag, nil
	}
	oldExpires := meta.Lease.ExpiresAtUnix
	meta.Lease = nil
	if cleanupStaging {
		s.discardStagedArtifacts(ctx, namespace, key, meta)
		clearStagingFields(meta)
	}
	meta.UpdatedAtUnix = now.Unix()
	newETag, err := s.store.StoreMeta(ctx, namespace, key, meta, metaETag)
	if err != nil {
		return false, metaETag, err
	}
	if err := s.updateLeaseIndex(ctx, namespace, key, oldExpires, 0); err != nil {
		if s.logger != nil && !errors.Is(err, storage.ErrNotImplemented) {
			s.logger.Warn("lease.index.clear_failed", "namespace", namespace, "key", key, "error", err)
		}
	}
	return true, newETag, nil
}

func (s *Service) discardStagedArtifacts(ctx context.Context, namespace, key string, meta *storage.Meta) {
	if meta == nil {
		return
	}
	if meta.StagedStateETag != "" {
		_ = s.staging.DiscardStagedState(ctx, namespace, key, meta.StagedTxnID, storage.DiscardStagedOptions{IgnoreNotFound: true})
	}
	if len(meta.StagedAttachments) > 0 {
		discardStagedAttachments(ctx, s.store, namespace, key, meta.StagedTxnID, meta.StagedAttachments)
	}
}

func clearStagingFields(meta *storage.Meta) {
	if meta == nil {
		return
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
}
