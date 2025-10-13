package disk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"pkt.systems/lockd/internal/storage"
)

// Config captures the tunables for the disk backend.
type Config struct {
	Root            string
	Retention       time.Duration
	JanitorInterval time.Duration
	Now             func() time.Time
}

// Store implements storage.Backend backed by the local filesystem.
type Store struct {
	root            string
	metaDir         string
	stateDir        string
	tmpDir          string
	lockDir         string
	retention       time.Duration
	janitorInterval time.Duration
	now             func() time.Time

	locks sync.Map

	stopJanitor chan struct{}
	doneJanitor chan struct{}
}

var globalLocks sync.Map

func globalKeyMutex(encoded string) *sync.Mutex {
	mu, _ := globalLocks.LoadOrStore(encoded, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

type fileLock struct {
	file *os.File
}

func (f *fileLock) Unlock() error {
	if f.file == nil {
		return nil
	}
	if err := unlockFile(f.file); err != nil {
		f.file.Close()
		return err
	}
	return f.file.Close()
}

// New initialises a disk-backed store rooted at cfg.Root.
func New(cfg Config) (*Store, error) {
	if cfg.Root == "" {
		return nil, fmt.Errorf("disk: root path required")
	}
	if cfg.Retention < 0 {
		return nil, fmt.Errorf("disk: retention must be >= 0")
	}
	if cfg.JanitorInterval < 0 {
		return nil, fmt.Errorf("disk: janitor interval must be >= 0")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	root := filepath.Clean(cfg.Root)
	metaDir := filepath.Join(root, "meta")
	stateDir := filepath.Join(root, "state")
	tmpDir := filepath.Join(root, "tmp")
	for _, dir := range []string{metaDir, stateDir, tmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("disk: prepare directory %q: %w", dir, err)
		}
	}
	lockDir := filepath.Join(root, "locks")
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		return nil, fmt.Errorf("disk: prepare lock directory %q: %w", lockDir, err)
	}

	s := &Store{
		root:            root,
		metaDir:         metaDir,
		stateDir:        stateDir,
		tmpDir:          tmpDir,
		lockDir:         lockDir,
		retention:       cfg.Retention,
		janitorInterval: cfg.JanitorInterval,
		now:             cfg.Now,
	}
	if s.janitorInterval <= 0 {
		s.janitorInterval = time.Hour
	}

	if s.retention > 0 {
		s.stopJanitor = make(chan struct{})
		s.doneJanitor = make(chan struct{})
		go s.janitorLoop()
	}

	return s, nil
}

func (s *Store) keyLock(key string) *sync.Mutex {
	mu, _ := s.locks.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Store) acquireFileLock(key string) (*fileLock, error) {
	encoded, err := s.encodeKey(key)
	if err != nil {
		return nil, err
	}
	lockPath := filepath.Join(s.lockDir, encoded+".lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("disk: open lock: %w", err)
	}
	if err := lockFile(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("disk: lock key: %w", err)
	}
	return &fileLock{file: f}, nil
}

// Close shuts down the backend and waits for the janitor to finish.
func (s *Store) Close() error {
	if s.stopJanitor != nil {
		close(s.stopJanitor)
		<-s.doneJanitor
	}
	return nil
}

func (s *Store) encodeKey(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("disk: key required")
	}
	encoded := url.PathEscape(key)
	if strings.Contains(encoded, "..") {
		return "", fmt.Errorf("disk: invalid key %q", key)
	}
	return encoded, nil
}

func (s *Store) metaPath(encoded string) string {
	return filepath.Join(s.metaDir, encoded+".json")
}

func (s *Store) stateDirPath(encoded string) string {
	return filepath.Join(s.stateDir, encoded)
}

func (s *Store) stateDataPath(encoded string) string {
	return filepath.Join(s.stateDirPath(encoded), "data")
}

func (s *Store) stateInfoPath(encoded string) string {
	return filepath.Join(s.stateDirPath(encoded), "info.json")
}

type metaRecord struct {
	ETag string       `json:"etag"`
	Meta storage.Meta `json:"meta"`
}

type stateRecord struct {
	ETag            string `json:"etag"`
	Size            int64  `json:"size"`
	ModifiedAtUnix  int64  `json:"modified_at_unix"`
	LastAccessUnix  int64  `json:"last_access_unix,omitempty"`
	RetainedUntil   int64  `json:"retained_until_unix,omitempty"`
	CompressionHint string `json:"compression,omitempty"`
}

func (s *Store) LoadMeta(_ context.Context, key string) (*storage.Meta, string, error) {
	encoded, err := s.encodeKey(key)
	if err != nil {
		return nil, "", err
	}
	rec, err := s.readMetaRecord(encoded)
	if err != nil {
		return nil, "", err
	}
	return &rec.Meta, rec.ETag, nil
}

func (s *Store) StoreMeta(_ context.Context, key string, meta *storage.Meta, expectedETag string) (etag string, err error) {
	if meta == nil {
		return "", fmt.Errorf("disk: meta nil")
	}
	encoded, err := s.encodeKey(key)
	if err != nil {
		return "", err
	}

	glob := globalKeyMutex(encoded)
	glob.Lock()
	defer glob.Unlock()

	mu := s.keyLock(key)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(key)
	if err != nil {
		return "", err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	current, err := s.readMetaRecord(encoded)
	exists := err == nil
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return "", err
	}
	if expectedETag != "" {
		if errors.Is(err, storage.ErrNotFound) {
			return "", storage.ErrNotFound
		}
		if current.ETag != expectedETag {
			return "", storage.ErrCASMismatch
		}
	} else if exists {
		return "", storage.ErrCASMismatch
	}

	newRec := metaRecord{
		ETag: uuid.NewString(),
		Meta: *meta,
	}
	if err := s.writeJSONAtomic(s.metaPath(encoded), newRec, "meta"); err != nil {
		return "", err
	}
	return newRec.ETag, nil
}

func (s *Store) DeleteMeta(_ context.Context, key string, expectedETag string) (err error) {
	encoded, err := s.encodeKey(key)
	if err != nil {
		return err
	}
	glob := globalKeyMutex(encoded)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(key)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(key)
	if err != nil {
		return err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	if expectedETag != "" {
		rec, err := s.readMetaRecord(encoded)
		if err != nil {
			return err
		}
		if rec.ETag != expectedETag {
			return storage.ErrCASMismatch
		}
	}
	if err := os.Remove(s.metaPath(encoded)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return storage.ErrNotFound
		}
		return err
	}
	return nil
}

func (s *Store) ListMetaKeys(_ context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.metaDir)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		encoded := strings.TrimSuffix(name, ".json")
		key, err := url.PathUnescape(encoded)
		if err != nil {
			continue
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (s *Store) ReadState(_ context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	encoded, err := s.encodeKey(key)
	if err != nil {
		return nil, nil, err
	}
	info, err := s.readStateRecord(encoded)
	if err != nil {
		return nil, nil, err
	}
	file, err := os.Open(s.stateDataPath(encoded))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, storage.ErrNotFound
		}
		return nil, nil, err
	}
	return file, &storage.StateInfo{
		Size:       info.Size,
		ETag:       info.ETag,
		ModifiedAt: info.ModifiedAtUnix,
	}, nil
}

func (s *Store) WriteState(_ context.Context, key string, body io.Reader, opts storage.PutStateOptions) (result *storage.PutStateResult, err error) {
	encoded, err := s.encodeKey(key)
	if err != nil {
		return nil, err
	}
	glob := globalKeyMutex(encoded)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(key)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(key)
	if err != nil {
		return nil, err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	if err := os.MkdirAll(s.stateDirPath(encoded), 0o755); err != nil {
		return nil, fmt.Errorf("disk: ensure state dir: %w", err)
	}

	tempFile, err := os.CreateTemp(s.tmpDir, "lockd-state-*")
	if err != nil {
		return nil, err
	}
	moved := false
	defer func() {
		_ = tempFile.Close()
		if !moved {
			_ = os.Remove(tempFile.Name())
		}
	}()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(tempFile, hasher), body)
	if err != nil {
		return nil, err
	}
	if err := tempFile.Sync(); err != nil {
		return nil, err
	}
	if err := tempFile.Close(); err != nil {
		return nil, err
	}

	newETag := hex.EncodeToString(hasher.Sum(nil))
	if opts.ExpectedETag != "" {
		info, err := s.readStateRecord(encoded)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, storage.ErrNotFound
			}
			return nil, err
		}
		if info.ETag != opts.ExpectedETag {
			return nil, storage.ErrCASMismatch
		}
	}

	dataPath := s.stateDataPath(encoded)
	if err := os.Rename(tempFile.Name(), dataPath); err != nil {
		if removeErr := os.Remove(dataPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return nil, err
		}
		if err2 := os.Rename(tempFile.Name(), dataPath); err2 != nil {
			return nil, err2
		}
	}
	moved = true
	_ = syncDir(filepath.Dir(dataPath))

	info := stateRecord{
		ETag:           newETag,
		Size:           written,
		ModifiedAtUnix: s.now().Unix(),
	}
	if err := s.writeJSONAtomic(s.stateInfoPath(encoded), info, "state-info"); err != nil {
		return nil, err
	}

	return &storage.PutStateResult{
		BytesWritten: written,
		NewETag:      newETag,
	}, nil
}

func (s *Store) RemoveState(_ context.Context, key string, expectedETag string) (err error) {
	encoded, err := s.encodeKey(key)
	if err != nil {
		return err
	}
	glob := globalKeyMutex(encoded)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(key)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(key)
	if err != nil {
		return err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	if expectedETag != "" {
		info, err := s.readStateRecord(encoded)
		if err != nil {
			return err
		}
		if info.ETag != expectedETag {
			return storage.ErrCASMismatch
		}
	}

	dataPath := s.stateDataPath(encoded)
	infoPath := s.stateInfoPath(encoded)
	errData := os.Remove(dataPath)
	errInfo := os.Remove(infoPath)
	if errData != nil && !errors.Is(errData, os.ErrNotExist) {
		return errData
	}
	if errInfo != nil && !errors.Is(errInfo, os.ErrNotExist) {
		return errInfo
	}
	_ = os.Remove(s.stateDirPath(encoded))
	if errors.Is(errData, os.ErrNotExist) && errors.Is(errInfo, os.ErrNotExist) {
		return storage.ErrNotFound
	}
	return nil
}

// SweepOnceForTests exposes the retention sweeper for testing.
func (s *Store) SweepOnceForTests() {
	s.sweepOnce()
}

func (s *Store) readMetaRecord(encoded string) (*metaRecord, error) {
	data, err := os.ReadFile(s.metaPath(encoded))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	var rec metaRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("disk: decode meta: %w", err)
	}
	return &rec, nil
}

func (s *Store) readStateRecord(encoded string) (*stateRecord, error) {
	data, err := os.ReadFile(s.stateInfoPath(encoded))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	var rec stateRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("disk: decode state info: %w", err)
	}
	return &rec, nil
}

func (s *Store) writeJSONAtomic(dest string, v any, prefix string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.tmpDir, "lockd-"+prefix+"-*")
	if err != nil {
		return err
	}
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "")
	if err := enc.Encode(v); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), dest); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	_ = syncDir(filepath.Dir(dest))
	return nil
}

func (s *Store) janitorLoop() {
	ticker := time.NewTicker(s.janitorInterval)
	defer ticker.Stop()
	defer close(s.doneJanitor)
	for {
		select {
		case <-ticker.C:
			s.sweepOnce()
		case <-s.stopJanitor:
			return
		}
	}
}

func (s *Store) sweepOnce() {
	if s.retention <= 0 {
		return
	}
	now := s.now()
	entries, err := os.ReadDir(s.metaDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		encoded := strings.TrimSuffix(name, ".json")
		rec, err := s.readMetaRecord(encoded)
		if err != nil {
			continue
		}
		if rec.Meta.UpdatedAtUnix == 0 {
			continue
		}
		age := now.Sub(time.Unix(rec.Meta.UpdatedAtUnix, 0))
		if age <= s.retention {
			continue
		}
		key, err := url.PathUnescape(encoded)
		if err != nil {
			continue
		}
		// best-effort removal
		_ = s.DeleteMeta(context.Background(), key, rec.ETag)
		_ = s.RemoveState(context.Background(), key, "")
	}
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
