package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
)

const attachmentDefaultContentType = storage.ContentTypeOctetStream

// Attach stages an attachment payload for a lease-bound key.
func (s *Service) Attach(ctx context.Context, cmd AttachCommand) (res *AttachResult, err error) {
	start := s.clock.Now()
	namespaceLabel := "unknown"
	var attachBytes int64
	defer func() {
		if s.attachmentMetrics == nil {
			return
		}
		duration := s.clock.Now().Sub(start)
		s.attachmentMetrics.recordAttach(ctx, namespaceLabel, attachBytes, duration, err)
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
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.TxnID) == "" {
		return nil, Failure{Code: "missing_txn", Detail: "txn_id required", HTTPStatus: http.StatusBadRequest}
	}
	if cmd.Body == nil {
		return nil, Failure{Code: "missing_body", Detail: "attachment body required", HTTPStatus: http.StatusBadRequest}
	}
	name := strings.TrimSpace(cmd.Name)
	if name == "" {
		return nil, Failure{Code: "missing_attachment", Detail: "attachment name required", HTTPStatus: http.StatusBadRequest}
	}

	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)

	maxBytes := s.attachmentMaxBytes
	if cmd.MaxBytesSet {
		maxBytes = cmd.MaxBytes
	}
	if maxBytes < 0 {
		return nil, Failure{Code: "invalid_max_bytes", Detail: "max_bytes must be >= 0", HTTPStatus: http.StatusBadRequest}
	}

	contentType := strings.TrimSpace(cmd.ContentType)
	if contentType == "" {
		contentType = attachmentDefaultContentType
	}

	var staged storage.StagedAttachment
	var stagedKey string
	var stagedPrepared bool
	var stagedBody io.Reader
	var stagedDescriptor []byte
	var stagedSize int64
	var createdAt int64
	var attachmentID string
	var attempt int
	for {
		attempt++
		now := s.clock.Now()
		meta, metaETag, err := s.ensureMeta(ctx, namespace, storageKey)
		if err != nil {
			return nil, err
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now)
			if _, _, err := s.clearExpiredLease(ctx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				return nil, err
			}
			if stagedPrepared {
				_ = s.deleteAttachmentObject(ctx, namespace, stagedKey)
			}
			return nil, leaseErr
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now); err != nil {
			if stagedPrepared {
				_ = s.deleteAttachmentObject(ctx, namespace, stagedKey)
			}
			return nil, err
		}
		if meta.StagedTxnID != "" && meta.StagedTxnID != cmd.TxnID {
			if stagedPrepared {
				_ = s.deleteAttachmentObject(ctx, namespace, stagedKey)
			}
			return nil, Failure{
				Code:       "txn_mismatch",
				Detail:     "different transaction already staged",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				HTTPStatus: http.StatusConflict,
			}
		}
		if !stagedPrepared {
			view := buildAttachmentView(meta, true, cmd.TxnID)
			entry, ok := view.byName[name]
			var committed *storage.Attachment
			if !ok {
				committed = findCommittedAttachment(meta, name)
			}
			if ok && cmd.PreventOverwrite {
				return &AttachResult{Attachment: entry.info, Noop: true, Version: view.currentVersion(meta)}, nil
			}
			if !ok && committed != nil && cmd.PreventOverwrite {
				return &AttachResult{Attachment: attachmentInfoFromAttachment(*committed), Noop: true, Version: view.currentVersion(meta)}, nil
			}
			if ok {
				attachmentID = entry.info.ID
				if entry.info.CreatedAtUnix > 0 {
					createdAt = entry.info.CreatedAtUnix
				}
			} else if committed != nil {
				attachmentID = committed.ID
				if committed.CreatedAtUnix > 0 {
					createdAt = committed.CreatedAtUnix
				}
			}
			if attachmentID == "" {
				attachmentID = uuidv7.NewString()
			}
			if createdAt == 0 {
				createdAt = now.Unix()
			}
			updatedAt := now.Unix()

			stagedKey = storage.StagedAttachmentObjectKey(keyComponent, cmd.TxnID, attachmentID)
			payload, err := s.prepareAttachmentPayload(ctx, namespace, stagedKey, cmd.Body, maxBytes, contentType)
			if err != nil {
				return nil, err
			}
			stagedBody = payload.reader
			stagedDescriptor = payload.descriptor

			if err := s.putAttachmentObject(ctx, namespace, stagedKey, stagedBody, payload.storageContentType, stagedDescriptor); err != nil {
				return nil, err
			}
			if payload.counter != nil {
				stagedSize = payload.counter.Count()
			}
			attachBytes = stagedSize
			if maxBytes > 0 && stagedSize > maxBytes {
				_ = s.deleteAttachmentObject(ctx, namespace, stagedKey)
				return nil, Failure{Code: "attachment_too_large", Detail: "attachment exceeds max_bytes", HTTPStatus: http.StatusRequestEntityTooLarge}
			}
			staged = storage.StagedAttachment{
				ID:               attachmentID,
				Name:             name,
				Size:             stagedSize,
				PlaintextBytes:   stagedSize,
				ContentType:      contentType,
				StagedDescriptor: stagedDescriptor,
				CreatedAtUnix:    createdAt,
				UpdatedAtUnix:    updatedAt,
			}
			stagedPrepared = true
		}

		meta.StagedTxnID = cmd.TxnID
		if meta.StagedVersion == 0 {
			meta.StagedVersion = meta.Version + 1
		}
		meta.UpdatedAtUnix = now.Unix()
		if meta.FencingToken == 0 {
			meta.FencingToken = cmd.FencingToken
		}

		removeAttachmentDelete(meta, name)
		upsertStagedAttachment(meta, staged)

		newMetaETag, err := s.store.StoreMeta(ctx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				if attempt < 3 {
					continue
				}
			}
			_ = s.deleteAttachmentObject(ctx, namespace, stagedKey)
			return nil, fmt.Errorf("store meta: %w", err)
		}
		if meta.Lease != nil {
			if _, _, err := s.enlistTxnParticipant(ctx, cmd.TxnID, namespace, keyComponent, meta.Lease.ExpiresAtUnix); err != nil {
				_ = s.deleteAttachmentObject(ctx, namespace, stagedKey)
				return nil, fmt.Errorf("register txn participant: %w", err)
			}
		}
		return &AttachResult{
			Attachment: attachmentInfoFromStaged(staged),
			Noop:       false,
			Version:    meta.StagedVersion,
			Meta:       meta,
			MetaETag:   newMetaETag,
		}, nil
	}
}

// ListAttachments enumerates attachments for a key.
func (s *Service) ListAttachments(ctx context.Context, cmd ListAttachmentsCommand) (res *ListAttachmentsResult, err error) {
	start := s.clock.Now()
	namespaceLabel := "unknown"
	var itemCount int
	defer func() {
		if s.attachmentMetrics == nil {
			return
		}
		duration := s.clock.Now().Sub(start)
		s.attachmentMetrics.recordList(ctx, namespaceLabel, itemCount, duration, err)
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
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: http.StatusBadRequest}
	}
	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)

	meta, metaETag, err := s.ensureMeta(ctx, namespace, storageKey)
	if err != nil {
		return nil, err
	}
	publishedVersion := meta.PublishedVersion
	if publishedVersion == 0 {
		publishedVersion = meta.Version
	}
	publicRead := cmd.Public && cmd.LeaseID == ""
	if publicRead {
		if publishedVersion < meta.Version {
			return nil, Failure{Code: "state_not_published", Detail: "state update not published yet", HTTPStatus: http.StatusServiceUnavailable}
		}
	} else {
		if strings.TrimSpace(cmd.LeaseID) == "" {
			return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
		}
		if strings.TrimSpace(cmd.TxnID) == "" {
			return nil, Failure{Code: "missing_txn", Detail: "txn_id required", HTTPStatus: http.StatusBadRequest}
		}
		now := s.clock.Now()
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now)
			if _, _, err := s.clearExpiredLease(ctx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					return nil, leaseErr
				}
				return nil, err
			}
			return nil, leaseErr
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now); err != nil {
			return nil, err
		}
		if meta.StagedRemove && meta.StagedTxnID == cmd.TxnID {
			return &ListAttachmentsResult{Attachments: nil}, nil
		}
	}
	view := buildAttachmentView(meta, !publicRead, cmd.TxnID)
	attachments := view.list()
	itemCount = len(attachments)
	return &ListAttachmentsResult{Attachments: attachments}, nil
}

// RetrieveAttachment streams a single attachment payload.
func (s *Service) RetrieveAttachment(ctx context.Context, cmd RetrieveAttachmentCommand) (res *RetrieveAttachmentResult, err error) {
	start := s.clock.Now()
	namespaceLabel := "unknown"
	var retrieveBytes int64
	defer func() {
		if s.attachmentMetrics == nil {
			return
		}
		duration := s.clock.Now().Sub(start)
		s.attachmentMetrics.recordRetrieve(ctx, namespaceLabel, retrieveBytes, duration, err)
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
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: http.StatusBadRequest}
	}
	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)
	selector, err := normalizeAttachmentSelector(cmd.Selector)
	if err != nil {
		return nil, err
	}

	meta, metaETag, err := s.ensureMeta(ctx, namespace, storageKey)
	if err != nil {
		return nil, err
	}
	publishedVersion := meta.PublishedVersion
	if publishedVersion == 0 {
		publishedVersion = meta.Version
	}
	publicRead := cmd.Public && cmd.LeaseID == ""
	if publicRead {
		if publishedVersion < meta.Version {
			return nil, Failure{Code: "state_not_published", Detail: "state update not published yet", HTTPStatus: http.StatusServiceUnavailable}
		}
	} else {
		if strings.TrimSpace(cmd.LeaseID) == "" {
			return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
		}
		if strings.TrimSpace(cmd.TxnID) == "" {
			return nil, Failure{Code: "missing_txn", Detail: "txn_id required", HTTPStatus: http.StatusBadRequest}
		}
		now := s.clock.Now()
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now)
			if _, _, err := s.clearExpiredLease(ctx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					return nil, leaseErr
				}
				return nil, err
			}
			return nil, leaseErr
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now); err != nil {
			return nil, err
		}
		if meta.StagedRemove && meta.StagedTxnID == cmd.TxnID {
			return nil, Failure{Code: "attachment_not_found", Detail: "attachment not found", HTTPStatus: http.StatusNotFound}
		}
	}

	view := buildAttachmentView(meta, !publicRead, cmd.TxnID)
	entry, ok := view.lookup(selector)
	if !ok {
		return nil, Failure{Code: "attachment_not_found", Detail: "attachment not found", HTTPStatus: http.StatusNotFound}
	}
	objectKey := ""
	descriptor := []byte(nil)
	if entry.staged {
		if meta.StagedTxnID == "" {
			return nil, Failure{Code: "attachment_not_found", Detail: "attachment not found", HTTPStatus: http.StatusNotFound}
		}
		objectKey = storage.StagedAttachmentObjectKey(keyComponent, meta.StagedTxnID, entry.info.ID)
		descriptor = entry.stagedDescriptor
	} else {
		objectKey = storage.AttachmentObjectKey(keyComponent, entry.info.ID)
		descriptor = entry.descriptor
	}
	reader, err := s.readAttachmentObject(ctx, namespace, objectKey, descriptor)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, Failure{Code: "attachment_not_found", Detail: "attachment not found", HTTPStatus: http.StatusNotFound}
		}
		return nil, err
	}
	retrieveBytes = entry.info.Size
	return &RetrieveAttachmentResult{Attachment: entry.info, Reader: reader}, nil
}

// DeleteAttachment stages removal of a single attachment.
func (s *Service) DeleteAttachment(ctx context.Context, cmd DeleteAttachmentCommand) (res *DeleteAttachmentResult, err error) {
	start := s.clock.Now()
	namespaceLabel := "unknown"
	defer func() {
		if s.attachmentMetrics == nil {
			return
		}
		duration := s.clock.Now().Sub(start)
		s.attachmentMetrics.recordDelete(ctx, namespaceLabel, duration, err)
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
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.LeaseID) == "" {
		return nil, Failure{Code: "missing_lease", Detail: "lease_id required", HTTPStatus: http.StatusBadRequest}
	}
	if strings.TrimSpace(cmd.TxnID) == "" {
		return nil, Failure{Code: "missing_txn", Detail: "txn_id required", HTTPStatus: http.StatusBadRequest}
	}
	selector, err := normalizeAttachmentSelector(cmd.Selector)
	if err != nil {
		return nil, err
	}

	storageKey, err := s.namespacedKey(namespace, cmd.Key)
	if err != nil {
		return nil, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	keyComponent := relativeKey(namespace, storageKey)

	for {
		now := s.clock.Now()
		meta, metaETag, err := s.ensureMeta(ctx, namespace, storageKey)
		if err != nil {
			return nil, err
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now)
			if _, _, err := s.clearExpiredLease(ctx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				return nil, err
			}
			return nil, leaseErr
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now); err != nil {
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
		if meta.StagedRemove && meta.StagedTxnID == cmd.TxnID {
			return &DeleteAttachmentResult{Deleted: false, Version: meta.StagedVersion}, nil
		}
		view := buildAttachmentView(meta, true, cmd.TxnID)
		entry, ok := view.lookup(selector)
		if !ok {
			return &DeleteAttachmentResult{Deleted: false, Version: view.currentVersion(meta)}, nil
		}
		if entry.staged {
			removeStagedAttachment(meta, entry.info.Name, entry.info.ID)
			_ = s.deleteAttachmentObject(ctx, namespace, storage.StagedAttachmentObjectKey(keyComponent, cmd.TxnID, entry.info.ID))
			if committed := findCommittedAttachment(meta, entry.info.Name); committed != nil && !meta.StagedAttachmentsClear {
				addAttachmentDelete(meta, entry.info.Name)
			}
		} else {
			if !meta.StagedAttachmentsClear {
				addAttachmentDelete(meta, entry.info.Name)
			}
		}
		meta.StagedTxnID = cmd.TxnID
		if meta.StagedVersion == 0 {
			meta.StagedVersion = meta.Version + 1
		}
		meta.UpdatedAtUnix = now.Unix()
		if meta.FencingToken == 0 {
			meta.FencingToken = cmd.FencingToken
		}

		newMetaETag, err := s.store.StoreMeta(ctx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, fmt.Errorf("store meta: %w", err)
		}
		if meta.Lease != nil {
			if _, _, err := s.enlistTxnParticipant(ctx, cmd.TxnID, namespace, keyComponent, meta.Lease.ExpiresAtUnix); err != nil {
				return nil, fmt.Errorf("register txn participant: %w", err)
			}
		}
		return &DeleteAttachmentResult{Deleted: true, Version: meta.StagedVersion, Meta: meta, MetaETag: newMetaETag}, nil
	}
}

// DeleteAllAttachments stages removal of all attachments for a key.
func (s *Service) DeleteAllAttachments(ctx context.Context, cmd DeleteAllAttachmentsCommand) (res *DeleteAllAttachmentsResult, err error) {
	start := s.clock.Now()
	namespaceLabel := "unknown"
	deletedCount := 0
	defer func() {
		if s.attachmentMetrics == nil {
			return
		}
		duration := s.clock.Now().Sub(start)
		s.attachmentMetrics.recordDeleteAll(ctx, namespaceLabel, deletedCount, duration, err)
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
		return nil, Failure{Code: "missing_key", Detail: "key required", HTTPStatus: http.StatusBadRequest}
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

	for {
		now := s.clock.Now()
		meta, metaETag, err := s.ensureMeta(ctx, namespace, storageKey)
		if err != nil {
			return nil, err
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			leaseErr := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now)
			if _, _, err := s.clearExpiredLease(ctx, namespace, keyComponent, meta, metaETag, now, sweepModeTransparent, true); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				return nil, err
			}
			return nil, leaseErr
		}
		if err := validateLease(meta, cmd.LeaseID, cmd.FencingToken, cmd.TxnID, now); err != nil {
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
		if meta.StagedRemove && meta.StagedTxnID == cmd.TxnID {
			return &DeleteAllAttachmentsResult{Deleted: 0, Version: meta.StagedVersion}, nil
		}
		view := buildAttachmentView(meta, true, cmd.TxnID)
		deletedCount = len(view.byName)
		if len(meta.StagedAttachments) > 0 {
			discardStagedAttachments(ctx, s.store, namespace, keyComponent, cmd.TxnID, meta.StagedAttachments)
		}
		meta.StagedAttachmentsClear = true
		meta.StagedAttachmentDeletes = nil
		meta.StagedAttachments = nil
		meta.StagedTxnID = cmd.TxnID
		if meta.StagedVersion == 0 {
			meta.StagedVersion = meta.Version + 1
		}
		meta.UpdatedAtUnix = now.Unix()
		if meta.FencingToken == 0 {
			meta.FencingToken = cmd.FencingToken
		}

		newMetaETag, err := s.store.StoreMeta(ctx, namespace, keyComponent, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return nil, fmt.Errorf("store meta: %w", err)
		}
		if meta.Lease != nil {
			if _, _, err := s.enlistTxnParticipant(ctx, cmd.TxnID, namespace, keyComponent, meta.Lease.ExpiresAtUnix); err != nil {
				return nil, fmt.Errorf("register txn participant: %w", err)
			}
		}
		return &DeleteAllAttachmentsResult{Deleted: deletedCount, Version: meta.StagedVersion, Meta: meta, MetaETag: newMetaETag}, nil
	}
}

type attachmentView struct {
	byName map[string]*attachmentViewEntry
	byID   map[string]*attachmentViewEntry
}

type attachmentViewEntry struct {
	info             AttachmentInfo
	descriptor       []byte
	stagedDescriptor []byte
	staged           bool
}

func buildAttachmentView(meta *storage.Meta, includeStaged bool, txnID string) attachmentView {
	view := attachmentView{
		byName: make(map[string]*attachmentViewEntry),
		byID:   make(map[string]*attachmentViewEntry),
	}
	if meta == nil {
		return view
	}
	includeCommitted := true
	if includeStaged && meta.StagedTxnID != "" && meta.StagedTxnID == txnID && meta.StagedAttachmentsClear {
		includeCommitted = false
	}
	if includeCommitted {
		for _, att := range meta.Attachments {
			if att.Name == "" || att.ID == "" {
				continue
			}
			entry := &attachmentViewEntry{
				info:       attachmentInfoFromAttachment(att),
				descriptor: append([]byte(nil), att.Descriptor...),
			}
			view.byName[att.Name] = entry
			view.byID[att.ID] = entry
		}
	}
	if includeStaged && meta.StagedTxnID != "" && meta.StagedTxnID == txnID {
		if meta.StagedAttachmentsClear {
			view.byName = make(map[string]*attachmentViewEntry)
			view.byID = make(map[string]*attachmentViewEntry)
		}
		for _, name := range meta.StagedAttachmentDeletes {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			entry := view.byName[name]
			if entry != nil {
				delete(view.byName, name)
				if entry.info.ID != "" {
					delete(view.byID, entry.info.ID)
				}
			}
		}
		for _, att := range meta.StagedAttachments {
			if att.Name == "" || att.ID == "" {
				continue
			}
			entry := &attachmentViewEntry{
				info:             attachmentInfoFromStaged(att),
				stagedDescriptor: append([]byte(nil), att.StagedDescriptor...),
				staged:           true,
			}
			view.byName[att.Name] = entry
			view.byID[att.ID] = entry
		}
	}
	return view
}

func (v attachmentView) list() []AttachmentInfo {
	if len(v.byName) == 0 {
		return nil
	}
	out := make([]AttachmentInfo, 0, len(v.byName))
	for _, entry := range v.byName {
		out = append(out, entry.info)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func (v attachmentView) lookup(selector AttachmentSelector) (*attachmentViewEntry, bool) {
	if selector.ID != "" {
		entry := v.byID[selector.ID]
		if entry == nil {
			return nil, false
		}
		if selector.Name != "" && entry.info.Name != selector.Name {
			return nil, false
		}
		return entry, true
	}
	if selector.Name != "" {
		entry := v.byName[selector.Name]
		if entry == nil {
			return nil, false
		}
		return entry, true
	}
	return nil, false
}

func (v attachmentView) currentVersion(meta *storage.Meta) int64 {
	if meta == nil {
		return 0
	}
	if meta.StagedVersion > 0 {
		return meta.StagedVersion
	}
	return meta.Version
}

func normalizeAttachmentSelector(sel AttachmentSelector) (AttachmentSelector, error) {
	sel.ID = strings.TrimSpace(sel.ID)
	sel.Name = strings.TrimSpace(sel.Name)
	if sel.ID == "" && sel.Name == "" {
		return sel, Failure{Code: "missing_attachment", Detail: "attachment id or name required", HTTPStatus: http.StatusBadRequest}
	}
	return sel, nil
}

func attachmentInfoFromAttachment(att storage.Attachment) AttachmentInfo {
	size := att.PlaintextBytes
	if size == 0 {
		size = att.Size
	}
	return AttachmentInfo{
		ID:            att.ID,
		Name:          att.Name,
		Size:          size,
		ContentType:   att.ContentType,
		CreatedAtUnix: att.CreatedAtUnix,
		UpdatedAtUnix: att.UpdatedAtUnix,
	}
}

func attachmentInfoFromStaged(att storage.StagedAttachment) AttachmentInfo {
	size := att.PlaintextBytes
	if size == 0 {
		size = att.Size
	}
	return AttachmentInfo{
		ID:            att.ID,
		Name:          att.Name,
		Size:          size,
		ContentType:   att.ContentType,
		CreatedAtUnix: att.CreatedAtUnix,
		UpdatedAtUnix: att.UpdatedAtUnix,
	}
}

func findCommittedAttachment(meta *storage.Meta, name string) *storage.Attachment {
	if meta == nil {
		return nil
	}
	for i := range meta.Attachments {
		if meta.Attachments[i].Name == name {
			return &meta.Attachments[i]
		}
	}
	return nil
}

func upsertStagedAttachment(meta *storage.Meta, att storage.StagedAttachment) {
	if meta == nil {
		return
	}
	name := strings.TrimSpace(att.Name)
	if name == "" {
		return
	}
	filtered := meta.StagedAttachments[:0]
	for _, existing := range meta.StagedAttachments {
		if existing.Name == name || (att.ID != "" && existing.ID == att.ID) {
			continue
		}
		filtered = append(filtered, existing)
	}
	meta.StagedAttachments = append(filtered, att)
}

func removeStagedAttachment(meta *storage.Meta, name string, id string) {
	if meta == nil {
		return
	}
	name = strings.TrimSpace(name)
	id = strings.TrimSpace(id)
	filtered := meta.StagedAttachments[:0]
	for _, existing := range meta.StagedAttachments {
		if (name != "" && existing.Name == name) || (id != "" && existing.ID == id) {
			continue
		}
		filtered = append(filtered, existing)
	}
	meta.StagedAttachments = filtered
}

func addAttachmentDelete(meta *storage.Meta, name string) {
	if meta == nil {
		return
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	for _, existing := range meta.StagedAttachmentDeletes {
		if existing == name {
			return
		}
	}
	meta.StagedAttachmentDeletes = append(meta.StagedAttachmentDeletes, name)
}

func removeAttachmentDelete(meta *storage.Meta, name string) {
	if meta == nil || len(meta.StagedAttachmentDeletes) == 0 {
		return
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	filtered := meta.StagedAttachmentDeletes[:0]
	for _, existing := range meta.StagedAttachmentDeletes {
		if existing == name {
			continue
		}
		filtered = append(filtered, existing)
	}
	meta.StagedAttachmentDeletes = filtered
}

type attachmentPayload struct {
	reader             io.Reader
	counter            *countingReader
	descriptor         []byte
	storageContentType string
}

func (s *Service) prepareAttachmentPayload(ctx context.Context, namespace, objectKey string, body io.Reader, maxBytes int64, contentType string) (attachmentPayload, error) {
	counting := &countingReader{r: body}
	reader := io.Reader(counting)
	if maxBytes > 0 {
		limit := maxBytes + 1
		if limit <= 0 {
			limit = maxBytes
		}
		reader = io.LimitReader(counting, limit)
	}
	payload := attachmentPayload{reader: reader, counter: counting, storageContentType: contentType}
	if s.crypto == nil || !s.crypto.Enabled() {
		payload.reader = reader
		return payload, nil
	}
	objectCtx := storage.AttachmentObjectContext(path.Join(namespace, objectKey))
	minted, err := s.crypto.MintMaterial(objectCtx)
	if err != nil {
		return payload, err
	}
	payload.descriptor = append([]byte(nil), minted.Descriptor...)
	payload.storageContentType = storage.ContentTypeOctetStreamEncrypted
	pr, pw := io.Pipe()
	encWriter, err := s.crypto.EncryptWriterForMaterial(pw, minted.Material)
	if err != nil {
		pw.Close()
		return payload, err
	}
	go func() {
		if _, err := io.Copy(encWriter, reader); err != nil {
			encWriter.Close()
			pw.CloseWithError(err)
			return
		}
		if err := encWriter.Close(); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()
	payload.reader = pr
	return payload, nil
}

func (s *Service) putAttachmentObject(ctx context.Context, namespace, objectKey string, body io.Reader, contentType string, descriptor []byte) error {
	_, err := s.store.PutObject(ctx, namespace, objectKey, body, storage.PutObjectOptions{
		ContentType: contentType,
		Descriptor:  descriptor,
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) readAttachmentObject(ctx context.Context, namespace, objectKey string, descriptor []byte) (io.ReadCloser, error) {
	obj, err := s.store.GetObject(ctx, namespace, objectKey)
	if err != nil {
		return nil, err
	}
	reader := obj.Reader
	info := obj.Info
	if s.crypto == nil || !s.crypto.Enabled() {
		return reader, nil
	}
	if len(descriptor) == 0 && info != nil {
		descriptor = append([]byte(nil), info.Descriptor...)
	}
	if len(descriptor) == 0 {
		reader.Close()
		return nil, fmt.Errorf("attachment: missing descriptor for %s/%s", namespace, objectKey)
	}
	mat, err := s.crypto.MaterialFromDescriptor(storage.AttachmentObjectContext(path.Join(namespace, objectKey)), descriptor)
	if err != nil {
		reader.Close()
		return nil, err
	}
	decReader, err := s.crypto.DecryptReaderForMaterial(reader, mat)
	if err != nil {
		reader.Close()
		return nil, err
	}
	return decReader, nil
}

func (s *Service) deleteAttachmentObject(ctx context.Context, namespace, objectKey string) error {
	if objectKey == "" {
		return nil
	}
	err := s.store.DeleteObject(ctx, namespace, objectKey, storage.DeleteObjectOptions{IgnoreNotFound: true})
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	return err
}

func discardStagedAttachments(ctx context.Context, store storage.Backend, namespace, key, txnID string, staged []storage.StagedAttachment) {
	if store == nil {
		return
	}
	for _, att := range staged {
		if att.ID == "" {
			continue
		}
		objectKey := storage.StagedAttachmentObjectKey(key, txnID, att.ID)
		_ = store.DeleteObject(ctx, namespace, objectKey, storage.DeleteObjectOptions{IgnoreNotFound: true})
	}
}

func attachmentsFromMap(input map[string]storage.Attachment) []storage.Attachment {
	if len(input) == 0 {
		return nil
	}
	names := make([]string, 0, len(input))
	for name := range input {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]storage.Attachment, 0, len(names))
	for _, name := range names {
		out = append(out, input[name])
	}
	return out
}

func (s *Service) promoteStagedAttachment(ctx context.Context, namespace, key, txnID string, staged storage.StagedAttachment) (*storage.Attachment, error) {
	if staged.ID == "" {
		return nil, fmt.Errorf("attachment: missing id")
	}
	stagedKey := storage.StagedAttachmentObjectKey(key, txnID, staged.ID)
	reader, err := s.readAttachmentObject(ctx, namespace, stagedKey, staged.StagedDescriptor)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	contentType := strings.TrimSpace(staged.ContentType)
	if contentType == "" {
		contentType = attachmentDefaultContentType
	}
	commitKey := storage.AttachmentObjectKey(key, staged.ID)
	payload, err := s.prepareAttachmentPayload(ctx, namespace, commitKey, reader, 0, contentType)
	if err != nil {
		return nil, err
	}
	if err := s.putAttachmentObject(ctx, namespace, commitKey, payload.reader, payload.storageContentType, payload.descriptor); err != nil {
		return nil, err
	}
	size := staged.PlaintextBytes
	if size == 0 {
		size = staged.Size
	}
	if size == 0 && payload.counter != nil {
		size = payload.counter.Count()
	}
	if err := s.deleteAttachmentObject(ctx, namespace, stagedKey); err != nil {
		return nil, err
	}
	return &storage.Attachment{
		ID:             staged.ID,
		Name:           staged.Name,
		Size:           size,
		PlaintextBytes: size,
		ContentType:    contentType,
		Descriptor:     payload.descriptor,
		CreatedAtUnix:  staged.CreatedAtUnix,
		UpdatedAtUnix:  staged.UpdatedAtUnix,
	}, nil
}

type countingReader struct {
	r     io.Reader
	count int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.count += int64(n)
	return n, err
}

func (c *countingReader) Count() int64 {
	return c.count
}
