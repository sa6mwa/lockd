package pebblestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"

	"pkt.systems/lockd/internal/storage"
)

// Store implements storage.Backend backed by Pebble.
type Store struct {
	db    *pebble.DB
	locks sync.Map
}

func (s *Store) keyLock(key string) *sync.Mutex {
	mu, _ := s.locks.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Store) loadMetaValue(key string) (*metaRecord, error) {
	val, closer, err := s.db.Get(metaKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var rec metaRecord
	if err := json.Unmarshal(val, &rec); err != nil {
		return nil, fmt.Errorf("pebble: decode meta: %w", err)
	}
	return &rec, nil
}

type metaRecord struct {
	ETag string       `json:"etag"`
	Meta storage.Meta `json:"meta"`
}

// Open initialises a Pebble store at the provided path.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("pebble: path required")
	}
	db, err := pebble.Open(filepath.Clean(path), &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("pebble: open: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) LoadMeta(_ context.Context, key string) (*storage.Meta, string, error) {
	rec, err := s.loadMetaValue(key)
	if err != nil {
		return nil, "", err
	}
	return &rec.Meta, rec.ETag, nil
}

func (s *Store) StoreMeta(ctx context.Context, key string, meta *storage.Meta, expectedETag string) (string, error) {
	mu := s.keyLock(key)
	mu.Lock()
	defer mu.Unlock()

	current, err := s.loadMetaValue(key)
	currentETag := ""
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return "", err
	}
	if current != nil {
		currentETag = current.ETag
	}
	if expectedETag != "" {
		if errors.Is(err, storage.ErrNotFound) {
			return "", storage.ErrNotFound
		}
		if currentETag != expectedETag {
			return "", storage.ErrCASMismatch
		}
	}
	rec := metaRecord{
		ETag: uuid.NewString(),
		Meta: *meta,
	}
	payload, err := json.Marshal(&rec)
	if err != nil {
		return "", err
	}
	if err := s.db.Set(metaKey(key), payload, pebble.Sync); err != nil {
		return "", err
	}
	return rec.ETag, nil
}

func (s *Store) DeleteMeta(ctx context.Context, key string, expectedETag string) error {
	mu := s.keyLock(key)
	mu.Lock()
	defer mu.Unlock()

	if expectedETag != "" {
		current, err := s.loadMetaValue(key)
		if err != nil {
			return err
		}
		if current.ETag != expectedETag {
			return storage.ErrCASMismatch
		}
	}
	if err := s.db.Delete(metaKey(key), pebble.Sync); err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return storage.ErrNotFound
		}
		return err
	}
	return nil
}

func (s *Store) ListMetaKeys(_ context.Context) ([]string, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("meta/")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, []byte("meta/")) {
			break
		}
		keys = append(keys, string(key[len("meta/"):]))
	}
	return keys, iter.Error()
}

func (s *Store) ReadState(_ context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	val, closer, err := s.db.Get(stateKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	data := append([]byte(nil), val...)
	closer.Close()
	etag := hashBytes(data)
	info := &storage.StateInfo{
		Size: int64(len(data)),
		ETag: etag,
	}
	return io.NopCloser(bytes.NewReader(data)), info, nil
}

func (s *Store) WriteState(ctx context.Context, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	if opts.ExpectedETag != "" {
		current, _, err := s.ReadState(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, storage.ErrNotFound
			}
			return nil, err
		}
		currentData, _ := io.ReadAll(current)
		current.Close()
		if hashBytes(currentData) != opts.ExpectedETag {
			return nil, storage.ErrCASMismatch
		}
	}
	if err := s.db.Set(stateKey(key), data, pebble.Sync); err != nil {
		return nil, err
	}
	return &storage.PutStateResult{
		BytesWritten: int64(len(data)),
		NewETag:      hashBytes(data),
	}, nil
}

func (s *Store) RemoveState(ctx context.Context, key string, expectedETag string) error {
	if expectedETag != "" {
		reader, _, err := s.ReadState(ctx, key)
		if err != nil {
			return err
		}
		data, _ := io.ReadAll(reader)
		reader.Close()
		if hashBytes(data) != expectedETag {
			return storage.ErrCASMismatch
		}
	}
	if err := s.db.Delete(stateKey(key), pebble.Sync); err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return storage.ErrNotFound
		}
		return err
	}
	return nil
}

func metaKey(key string) []byte {
	return []byte("meta/" + key)
}

func stateKey(key string) []byte {
	return []byte("state/" + key)
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
