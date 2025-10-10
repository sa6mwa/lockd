package memory

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"

	"pkt.systems/lockd/internal/storage"
)

// Store implements storage.Backend in-memory; intended for tests and local dev.
type Store struct {
	mu    sync.RWMutex
	metas map[string]*metaEntry
	state map[string]*stateEntry
}

type metaEntry struct {
	data *storage.Meta
	etag string
}

type stateEntry struct {
	payload []byte
	etag    string
	updated time.Time
}

// New returns a ready to use in-memory store.
func New() *Store {
	return &Store{
		metas: make(map[string]*metaEntry),
		state: make(map[string]*stateEntry),
	}
}

func (s *Store) Close() error { return nil }

func (s *Store) LoadMeta(_ context.Context, key string) (*storage.Meta, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.metas[key]
	if !ok {
		return nil, "", storage.ErrNotFound
	}
	clone := *entry.data
	if entry.data.Lease != nil {
		leaseCopy := *entry.data.Lease
		clone.Lease = &leaseCopy
	}
	return &clone, entry.etag, nil
}

func (s *Store) StoreMeta(_ context.Context, key string, meta *storage.Meta, expectedETag string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if expectedETag != "" {
		entry, ok := s.metas[key]
		if !ok {
			return "", storage.ErrNotFound
		}
		if entry.etag != expectedETag {
			return "", storage.ErrCASMismatch
		}
	}
	etag := uuid.NewString()
	clone := *meta
	if meta.Lease != nil {
		leaseCopy := *meta.Lease
		clone.Lease = &leaseCopy
	}
	s.metas[key] = &metaEntry{
		data: &clone,
		etag: etag,
	}
	return etag, nil
}

func (s *Store) DeleteMeta(_ context.Context, key string, expectedETag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if expectedETag != "" {
		entry, ok := s.metas[key]
		if !ok {
			return storage.ErrNotFound
		}
		if entry.etag != expectedETag {
			return storage.ErrCASMismatch
		}
	}
	delete(s.metas, key)
	return nil
}

func (s *Store) ListMetaKeys(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.metas))
	for key := range s.metas {
		keys = append(keys, key)
	}
	return keys, nil
}

func (s *Store) ReadState(_ context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.state[key]
	if !ok {
		return nil, nil, storage.ErrNotFound
	}
	reader := io.NopCloser(bytes.NewReader(entry.payload))
	info := &storage.StateInfo{
		Size:       int64(len(entry.payload)),
		ETag:       entry.etag,
		ModifiedAt: entry.updated.Unix(),
	}
	return reader, info, nil
}

func (s *Store) WriteState(_ context.Context, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	payload, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if opts.ExpectedETag != "" {
		entry, ok := s.state[key]
		if !ok {
			return nil, storage.ErrNotFound
		}
		if entry.etag != opts.ExpectedETag {
			return nil, storage.ErrCASMismatch
		}
	}
	etag := uuid.NewString()
	s.state[key] = &stateEntry{
		payload: payload,
		etag:    etag,
		updated: time.Now().UTC(),
	}
	return &storage.PutStateResult{
		BytesWritten: int64(len(payload)),
		NewETag:      etag,
	}, nil
}

func (s *Store) RemoveState(_ context.Context, key string, expectedETag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if expectedETag != "" {
		entry, ok := s.state[key]
		if !ok {
			return storage.ErrNotFound
		}
		if entry.etag != expectedETag {
			return storage.ErrCASMismatch
		}
	}
	delete(s.state, key)
	return nil
}
