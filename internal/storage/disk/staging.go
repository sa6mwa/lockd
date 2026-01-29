package disk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"pkt.systems/lockd/internal/storage"
)

func (s *Store) stagingKey(key, txnID string) string {
	key = strings.TrimSuffix(key, "/")
	if key == "" {
		return ".staging/" + txnID
	}
	return key + "/.staging/" + txnID
}

type lockedStateKey struct {
	key    string
	global *sync.Mutex
	local  *sync.Mutex
	file   *fileLock
}

func (s *Store) lockStateKeys(namespace string, keys []string) ([]lockedStateKey, func() error, error) {
	unique := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		unique[key] = struct{}{}
	}
	if len(unique) == 0 {
		return nil, func() error { return nil }, nil
	}
	ordered := make([]string, 0, len(unique))
	for key := range unique {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	locks := make([]lockedStateKey, 0, len(ordered))
	unlock := func() error {
		var unlockErr error
		for i := len(locks) - 1; i >= 0; i-- {
			lock := locks[i]
			if lock.file != nil {
				if err := lock.file.Unlock(); err != nil && unlockErr == nil {
					unlockErr = err
				}
			}
			if lock.local != nil {
				lock.local.Unlock()
			}
			if lock.global != nil {
				lock.global.Unlock()
			}
		}
		return unlockErr
	}
	for _, key := range ordered {
		g := globalKeyMutex(namespace, key)
		g.Lock()
		l := s.keyLock(namespace, key)
		l.Lock()
		f, err := s.acquireFileLock(namespace, key)
		if err != nil {
			_ = unlock()
			return nil, nil, err
		}
		locks = append(locks, lockedStateKey{
			key:    key,
			global: g,
			local:  l,
			file:   f,
		})
	}
	return locks, unlock, nil
}

// StageState writes state into a per-transaction staging key.
func (s *Store) StageState(ctx context.Context, namespace, key, txnID string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	return s.WriteState(ctx, namespace, s.stagingKey(key, txnID), body, opts)
}

// LoadStagedState reads staged state for a transaction.
func (s *Store) LoadStagedState(ctx context.Context, namespace, key, txnID string) (storage.ReadStateResult, error) {
	return s.ReadState(ctx, namespace, s.stagingKey(key, txnID))
}

// PromoteStagedState moves staged state into the committed key.
func (s *Store) PromoteStagedState(ctx context.Context, namespace, key, txnID string, opts storage.PromoteStagedOptions) (result *storage.PutStateResult, err error) {
	logger, verbose := s.loggers(ctx)
	verbose.Trace("disk.promote_staged.begin", "namespace", namespace, "key", key, "txn_id", txnID)
	stagedKey := s.stagingKey(key, txnID)

	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.promote_staged.encode_error", "key", key, "error", err)
		return nil, err
	}
	nsStaged, cleanStaged, err := s.normalizeKey(namespace, stagedKey)
	if err != nil {
		logger.Debug("disk.promote_staged.encode_error", "key", stagedKey, "error", err)
		return nil, err
	}
	if nsStaged != ns {
		return nil, fmt.Errorf("disk: staged namespace mismatch")
	}

	_, unlock, err := s.lockStateKeys(ns, []string{clean, cleanStaged})
	if err != nil {
		logger.Debug("disk.promote_staged.lock_error", "key", key, "error", err)
		return nil, err
	}
	defer func() {
		if unlockErr := unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.promote_staged.namespace_error", "key", key, "error", err)
		return nil, err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.promote_staged.refresh_error", "key", key, "error", err)
		return nil, err
	}

	expectedETag := opts.ExpectedHeadETag
	ifNotExists := expectedETag == ""

	ln.mu.Lock()
	current := ln.stateIndex[clean]
	pending := ln.pendingState[clean]
	ln.mu.Unlock()
	if pending == nil && (expectedETag != "" || ifNotExists) && current == nil {
		if err := ln.refreshForce(); err != nil {
			logger.Debug("disk.promote_staged.refresh_force_error", "key", key, "error", err)
			return nil, err
		}
		ln.mu.Lock()
		current = ln.stateIndex[clean]
		pending = ln.pendingState[clean]
		ln.mu.Unlock()
	}
	if pending != nil && (expectedETag != "" || ifNotExists) {
		if !pendingGroupMatch(ctx, pending) {
			if err := waitForPendingCommit(ctx, ln, pending); err != nil {
				logger.Debug("disk.promote_staged.pending_wait_error", "key", key, "error", err)
				return nil, err
			}
			ln.mu.Lock()
			current = ln.stateIndex[clean]
			pending = ln.pendingState[clean]
			ln.mu.Unlock()
			if pending != nil && !pendingGroupMatch(ctx, pending) {
				logger.Debug("disk.promote_staged.cas_pending", "key", key)
				return nil, storage.ErrCASMismatch
			}
		}
		if expectedETag != "" {
			if pending != nil && pending.ref.meta.etag != expectedETag {
				expectedETag = pending.ref.meta.etag
			}
			if isDeleteRecord(pending.ref.recType) || pending.ref.meta.etag != expectedETag {
				logger.Debug("disk.promote_staged.cas_pending_mismatch", "key", key, "expected_etag", expectedETag)
				return nil, storage.ErrCASMismatch
			}
			current = pending.ref
		}
		if ifNotExists && pending != nil {
			logger.Debug("disk.promote_staged.if_not_exists_pending", "key", key)
			return nil, storage.ErrCASMismatch
		}
	}
	if expectedETag != "" {
		if current == nil {
			logger.Debug("disk.promote_staged.cas_not_found", "key", key, "expected_etag", expectedETag)
			return nil, storage.ErrNotFound
		}
		if current.meta.etag != expectedETag {
			logger.Debug("disk.promote_staged.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", current.meta.etag)
			return nil, storage.ErrCASMismatch
		}
	}
	if ifNotExists && current != nil {
		logger.Debug("disk.promote_staged.if_not_exists_conflict", "key", key)
		return nil, storage.ErrCASMismatch
	}

	ln.mu.Lock()
	staged := ln.stateIndex[cleanStaged]
	stagedPending := ln.pendingState[cleanStaged]
	ln.mu.Unlock()
	if staged == nil && stagedPending == nil {
		if err := ln.refreshForce(); err != nil {
			logger.Debug("disk.promote_staged.refresh_force_error", "key", stagedKey, "error", err)
			return nil, err
		}
		ln.mu.Lock()
		staged = ln.stateIndex[cleanStaged]
		stagedPending = ln.pendingState[cleanStaged]
		ln.mu.Unlock()
	}
	if stagedPending != nil {
		if !pendingGroupMatch(ctx, stagedPending) {
			if err := waitForPendingCommit(ctx, ln, stagedPending); err != nil {
				logger.Debug("disk.promote_staged.pending_wait_error", "key", stagedKey, "error", err)
				return nil, err
			}
			ln.mu.Lock()
			staged = ln.stateIndex[cleanStaged]
			stagedPending = ln.pendingState[cleanStaged]
			ln.mu.Unlock()
			if stagedPending != nil && !pendingGroupMatch(ctx, stagedPending) {
				logger.Debug("disk.promote_staged.pending_mismatch", "key", stagedKey)
				return nil, storage.ErrCASMismatch
			}
		}
		if stagedPending != nil {
			staged = stagedPending.ref
		}
	}
	if staged == nil || isDeleteRecord(staged.recType) {
		logger.Debug("disk.promote_staged.missing_staged", "key", stagedKey)
		return nil, storage.ErrNotFound
	}

	linkPath := ""
	linkOffset := int64(0)
	linkLen := int64(0)
	if staged.link != nil {
		linkPath = staged.link.path
		linkOffset = staged.link.offset
		linkLen = staged.link.length
	} else if staged.segment != nil {
		linkPath = staged.segment.path
		linkOffset = staged.payloadOffset
		linkLen = staged.payloadLen
	}
	if linkPath == "" {
		return nil, fmt.Errorf("disk: staged payload location missing")
	}
	segmentName := filepath.Base(linkPath)
	linkPayload, err := encodeStateLinkPayload(stateLinkPayload{
		segment: segmentName,
		offset:  linkOffset,
		length:  linkLen,
	})
	if err != nil {
		return nil, err
	}

	gen := uint64(1)
	if current != nil {
		gen = current.meta.gen + 1
	}
	meta := recordMeta{
		gen:           gen,
		modifiedAt:    s.now().Unix(),
		etag:          staged.meta.etag,
		descriptor:    append([]byte(nil), staged.meta.descriptor...),
		plaintextSize: staged.meta.plaintextSize,
		cipherSize:    staged.meta.cipherSize,
	}
	ref, err := ln.appendRecord(ctx, logRecordStateLink, clean, meta, bytes.NewReader(linkPayload))
	if err != nil {
		logger.Debug("disk.promote_staged.append_error", "key", key, "error", err)
		return nil, err
	}
	ref.link = &recordLink{
		path:   filepath.Join(ln.segmentsDir, segmentName),
		offset: linkOffset,
		length: linkLen,
	}

	deleteMeta := recordMeta{
		gen:        staged.meta.gen + 1,
		modifiedAt: s.now().Unix(),
	}
	if _, err := ln.appendRecord(ctx, logRecordStateDelete, cleanStaged, deleteMeta, nil); err != nil {
		logger.Debug("disk.promote_staged.cleanup_error", "key", stagedKey, "error", err)
		return nil, err
	}

	result = &storage.PutStateResult{
		BytesWritten: ref.meta.plaintextSize,
		NewETag:      ref.meta.etag,
	}
	if len(ref.meta.descriptor) > 0 {
		result.Descriptor = append([]byte(nil), ref.meta.descriptor...)
	}
	verbose.Trace("disk.promote_staged.success", "namespace", namespace, "key", key, "staged", stagedKey, "etag", result.NewETag)
	return result, nil
}

// DiscardStagedState deletes staged state for a transaction.
func (s *Store) DiscardStagedState(ctx context.Context, namespace, key, txnID string, opts storage.DiscardStagedOptions) error {
	return s.Remove(ctx, namespace, s.stagingKey(key, txnID), opts.ExpectedETag)
}

// ListStagedState lists staged state keys for a namespace.
func (s *Store) ListStagedState(ctx context.Context, namespace string, opts storage.ListStagedOptions) (*storage.ListResult, error) {
	prefix := ".staging/"
	if opts.TxnPrefix != "" {
		trimmed := strings.TrimPrefix(opts.TxnPrefix, "/")
		if trimmed != "" {
			prefix += trimmed
		}
	}
	return s.ListObjects(ctx, namespace, storage.ListOptions{Prefix: prefix, StartAfter: opts.StartAfter, Limit: opts.Limit})
}
