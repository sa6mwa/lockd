package memory

import (
	"context"
	"io"
	"strings"
	"time"

	"pkt.systems/lockd/internal/storage"
)

func (s *Store) stagingKey(key, txnID string) string {
	key = strings.TrimSuffix(key, "/")
	if key == "" {
		return ".staging/" + txnID
	}
	return key + "/.staging/" + txnID
}

// StageState writes state into a per-transaction staging key.
func (s *Store) StageState(ctx context.Context, namespace, key, txnID string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	return s.WriteState(ctx, namespace, s.stagingKey(key, txnID), body, opts)
}

// LoadStagedState reads staged state for a transaction.
func (s *Store) LoadStagedState(ctx context.Context, namespace, key, txnID string) (storage.ReadStateResult, error) {
	return s.ReadState(ctx, namespace, s.stagingKey(key, txnID))
}

// PromoteStagedState copies staged state into the committed key without re-encrypting.
func (s *Store) PromoteStagedState(ctx context.Context, namespace, key, txnID string, opts storage.PromoteStagedOptions) (*storage.PutStateResult, error) {
	stagedKey := s.stagingKey(key, txnID)
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return nil, err
	}
	stagedStorageKey, err := canonicalKey(namespace, stagedKey)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if opts.ExpectedHeadETag != "" {
		current, ok := s.state[storageKey]
		if !ok {
			return nil, storage.ErrNotFound
		}
		if current.etag != opts.ExpectedHeadETag {
			return nil, storage.ErrCASMismatch
		}
	} else if _, exists := s.state[storageKey]; exists {
		return nil, storage.ErrCASMismatch
	}

	staged, ok := s.state[stagedStorageKey]
	if !ok {
		return nil, storage.ErrNotFound
	}
	descriptor := append([]byte(nil), staged.descriptor...)
	payload := append([]byte(nil), staged.payload...)
	etag := staged.etag
	plaintext := staged.plaintext
	if plaintext == 0 {
		plaintext = int64(len(payload))
	}
	s.state[storageKey] = &stateEntry{
		payload:    payload,
		etag:       etag,
		updated:    time.Now().UTC(),
		descriptor: descriptor,
		plaintext:  plaintext,
	}
	delete(s.state, stagedStorageKey)

	result := &storage.PutStateResult{
		BytesWritten: plaintext,
		NewETag:      etag,
	}
	if len(descriptor) > 0 {
		result.Descriptor = append([]byte(nil), descriptor...)
	}
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
