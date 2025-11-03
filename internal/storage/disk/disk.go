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
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

// Config captures the tunables for the disk backend.
type Config struct {
	Root            string
	Retention       time.Duration
	JanitorInterval time.Duration
	Now             func() time.Time
	QueueWatch      bool
	Crypto          *storage.Crypto
}

// Store implements storage.Backend backed by the local filesystem.
type Store struct {
	root            string
	metaDir         string
	stateDir        string
	tmpDir          string
	lockDir         string
	objectDir       string
	retention       time.Duration
	janitorInterval time.Duration
	now             func() time.Time
	crypto          *storage.Crypto

	locks sync.Map

	stopJanitor chan struct{}
	doneJanitor chan struct{}

	queueWatchEnabled bool
	queueWatchMode    string
	queueWatchReason  string
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
	objectDir := filepath.Join(root, "objects")
	for _, dir := range []string{metaDir, stateDir, tmpDir, objectDir} {
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
		objectDir:       objectDir,
		retention:       cfg.Retention,
		janitorInterval: cfg.JanitorInterval,
		now:             cfg.Now,
		crypto:          cfg.Crypto,
	}
	s.queueWatchMode = "polling"
	s.queueWatchReason = "config_disabled"
	if cfg.QueueWatch {
		if queueWatchSupported(root) {
			s.queueWatchEnabled = true
			s.queueWatchMode = "fsnotify"
			s.queueWatchReason = "filesystem_watch_enabled"
		} else {
			s.queueWatchReason = "filesystem_not_supported"
		}
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

// QueueWatchStatus reports whether fsnotify-based queue change notifications are active.
func (s *Store) QueueWatchStatus() (bool, string, string) {
	return s.queueWatchEnabled, s.queueWatchMode, s.queueWatchReason
}

func (s *Store) keyLock(key string) *sync.Mutex {
	mu, _ := s.locks.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Store) acquireFileLock(key string) (*fileLock, error) {
	lockPath, err := s.lockFilePath(key)
	if err != nil {
		return nil, err
	}
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

func (s *Store) loggers(ctx context.Context) (pslog.Logger, pslog.Logger) {
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = loggingutil.NoopLogger()
	}
	logger = logger.With("storage_backend", "disk")
	return logger, logger
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
	return filepath.Join(s.metaDir, encoded+".pb")
}

func (s *Store) lockFilePath(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("disk: key required for lock path")
	}
	clean := path.Clean("/" + key)
	if clean == "/" {
		return "", fmt.Errorf("disk: invalid lock key %q", key)
	}
	if strings.Contains(clean, "..") {
		return "", fmt.Errorf("disk: invalid lock key %q", key)
	}
	relative := strings.TrimPrefix(clean, "/")
	if relative == "" {
		return "", fmt.Errorf("disk: invalid lock key %q", key)
	}
	segments := strings.Split(relative, "/")
	lockDir := filepath.Join(append([]string{s.lockDir}, segments[:len(segments)-1]...)...)
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		return "", fmt.Errorf("disk: prepare lock directory %q: %w", lockDir, err)
	}
	filename := segments[len(segments)-1] + ".lock"
	return filepath.Join(lockDir, filename), nil
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

func (s *Store) objectDataPath(key string) (string, error) {
	normalized, err := s.normalizeObjectKey(key)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.objectDir, filepath.FromSlash(normalized)), nil
}

func (s *Store) objectInfoPath(key string) (string, error) {
	dataPath, err := s.objectDataPath(key)
	if err != nil {
		return "", err
	}
	return dataPath + ".info.json", nil
}

func (s *Store) normalizeObjectKey(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("disk: object key required")
	}
	clean := path.Clean("/" + key)
	if clean == "/" || clean == "." {
		return "", fmt.Errorf("disk: invalid object key %q", key)
	}
	clean = strings.TrimPrefix(clean, "/")
	if clean == "" || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("disk: invalid object key %q", key)
	}
	return clean, nil
}

func (s *Store) keyFromObjectPath(objectPath string) (string, error) {
	rel, err := filepath.Rel(s.objectDir, objectPath)
	if err != nil {
		return "", fmt.Errorf("disk: compute relative path: %w", err)
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("disk: object path outside root: %q", objectPath)
	}
	rel = filepath.ToSlash(rel)
	if rel == "" {
		return "", fmt.Errorf("disk: empty object key for path %q", objectPath)
	}
	return rel, nil
}

type metaRecord struct {
	ETag string
	Meta *storage.Meta
}

type stateRecord struct {
	ETag            string `json:"etag"`
	Size            int64  `json:"size"`
	CipherSize      int64  `json:"cipher_size,omitempty"`
	ModifiedAtUnix  int64  `json:"modified_at_unix"`
	LastAccessUnix  int64  `json:"last_access_unix,omitempty"`
	RetainedUntil   int64  `json:"retained_until_unix,omitempty"`
	CompressionHint string `json:"compression,omitempty"`
	Descriptor      []byte `json:"descriptor,omitempty"`
}

type objectInfoRecord struct {
	ETag          string `json:"etag"`
	ContentType   string `json:"content_type,omitempty"`
	UpdatedAtUnix int64  `json:"updated_at_unix,omitempty"`
	Descriptor    []byte `json:"descriptor,omitempty"`
}

// LoadMeta reads the per-key metadata protobuf and returns it with the stored ETag.
func (s *Store) LoadMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.load_meta.begin", "key", key)

	encoded, err := s.encodeKey(key)
	if err != nil {
		logger.Debug("disk.load_meta.encode_error", "key", key, "error", err)
		return nil, "", err
	}
	rec, err := s.readMetaRecord(encoded)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			verbose.Debug("disk.load_meta.not_found", "key", key, "elapsed", time.Since(start))
		} else {
			logger.Debug("disk.load_meta.error", "key", key, "error", err, "elapsed", time.Since(start))
		}
		return nil, "", err
	}
	meta := *rec.Meta
	if rec.Meta.Lease != nil {
		leaseCopy := *rec.Meta.Lease
		meta.Lease = &leaseCopy
	}
	leaseOwner := ""
	leaseExpires := int64(0)
	if meta.Lease != nil {
		leaseOwner = meta.Lease.Owner
		leaseExpires = meta.Lease.ExpiresAtUnix
	}
	verbose.Debug("disk.load_meta.success",
		"key", key,
		"version", meta.Version,
		"state_etag", meta.StateETag,
		"lease_owner", leaseOwner,
		"lease_expires_at", leaseExpires,
		"meta_etag", rec.ETag,
		"elapsed", time.Since(start),
	)
	return &meta, rec.ETag, nil
}

// StoreMeta persists metadata with conditional semantics when expectedETag is provided.
func (s *Store) StoreMeta(ctx context.Context, key string, meta *storage.Meta, expectedETag string) (etag string, err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()

	if meta == nil {
		err := fmt.Errorf("disk: meta nil")
		logger.Debug("disk.store_meta.invalid_meta", "key", key, "error", err)
		return "", err
	}
	leaseOwner := ""
	leaseExpires := int64(0)
	if meta.Lease != nil {
		leaseOwner = meta.Lease.Owner
		leaseExpires = meta.Lease.ExpiresAtUnix
	}
	verbose.Trace("disk.store_meta.begin",
		"key", key,
		"expected_etag", expectedETag,
		"version", meta.Version,
		"state_etag", meta.StateETag,
		"lease_owner", leaseOwner,
		"lease_expires_at", leaseExpires,
	)

	encoded, err := s.encodeKey(key)
	if err != nil {
		logger.Debug("disk.store_meta.encode_error", "key", key, "error", err)
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
		logger.Debug("disk.store_meta.filelock_error", "key", key, "error", err)
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
		logger.Debug("disk.store_meta.read_error", "key", key, "error", err)
		return "", err
	}
	if expectedETag != "" {
		if errors.Is(err, storage.ErrNotFound) {
			logger.Debug("disk.store_meta.cas_not_found", "key", key, "expected_etag", expectedETag)
			return "", storage.ErrNotFound
		}
		if current.ETag != expectedETag {
			logger.Debug("disk.store_meta.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", current.ETag)
			return "", storage.ErrCASMismatch
		}
	} else if exists {
		logger.Debug("disk.store_meta.cas_exists", "key", key, "current_etag", current.ETag)
		return "", storage.ErrCASMismatch
	}

	copyMeta := *meta
	newRec := metaRecord{
		ETag: uuidv7.NewString(),
		Meta: &copyMeta,
	}
	payload, err := storage.MarshalMetaRecord(newRec.ETag, newRec.Meta, s.crypto)
	if err != nil {
		logger.Debug("disk.store_meta.encode_error", "key", key, "error", err)
		return "", err
	}
	if err := s.writeBytesAtomic(s.metaPath(encoded), payload, "meta"); err != nil {
		logger.Debug("disk.store_meta.write_error", "key", key, "error", err)
		return "", err
	}
	verbose.Debug("disk.store_meta.success",
		"key", key,
		"new_etag", newRec.ETag,
		"elapsed", time.Since(start),
	)
	return newRec.ETag, nil
}

// DeleteMeta removes the metadata document on disk, honouring an expected ETag when supplied.
func (s *Store) DeleteMeta(ctx context.Context, key string, expectedETag string) (err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.delete_meta.begin", "key", key, "expected_etag", expectedETag)

	encoded, err := s.encodeKey(key)
	if err != nil {
		logger.Debug("disk.delete_meta.encode_error", "key", key, "error", err)
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
		logger.Debug("disk.delete_meta.filelock_error", "key", key, "error", err)
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
			logger.Debug("disk.delete_meta.read_error", "key", key, "error", err)
			return err
		}
		if rec.ETag != expectedETag {
			logger.Debug("disk.delete_meta.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", rec.ETag)
			return storage.ErrCASMismatch
		}
	}
	if err := os.Remove(s.metaPath(encoded)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Debug("disk.delete_meta.not_found", "key", key)
			return storage.ErrNotFound
		}
		logger.Debug("disk.delete_meta.remove_error", "key", key, "error", err)
		return err
	}
	verbose.Debug("disk.delete_meta.success", "key", key, "elapsed", time.Since(start))
	return nil
}

// ListMetaKeys scans the metadata directory and returns all known keys.
func (s *Store) ListMetaKeys(ctx context.Context) ([]string, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.list_meta_keys.begin")

	entries, err := os.ReadDir(s.metaDir)
	if err != nil {
		logger.Debug("disk.list_meta_keys.error", "error", err)
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".pb") {
			continue
		}
		encoded := strings.TrimSuffix(name, ".pb")
		key, err := url.PathUnescape(encoded)
		if err != nil {
			logger.Debug("disk.list_meta_keys.decode_error", "encoded", encoded, "error", err)
			continue
		}
		keys = append(keys, key)
	}
	verbose.Debug("disk.list_meta_keys.success", "count", len(keys), "elapsed", time.Since(start))
	return keys, nil
}

// ReadState opens the immutable JSON state file for key and returns its metadata.
func (s *Store) ReadState(ctx context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.read_state.begin", "key", key)

	encoded, err := s.encodeKey(key)
	if err != nil {
		logger.Debug("disk.read_state.encode_error", "key", key, "error", err)
		return nil, nil, err
	}
	info, err := s.readStateRecord(encoded)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			verbose.Debug("disk.read_state.not_found", "key", key, "elapsed", time.Since(start))
		} else {
			logger.Debug("disk.read_state.info_error", "key", key, "error", err)
		}
		return nil, nil, err
	}
	file, err := os.Open(s.stateDataPath(encoded))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			verbose.Debug("disk.read_state.not_found", "key", key, "elapsed", time.Since(start))
			return nil, nil, storage.ErrNotFound
		}
		logger.Debug("disk.read_state.open_error", "key", key, "error", err)
		return nil, nil, err
	}
	encrypted := s.crypto != nil && s.crypto.Enabled()
	var descriptor []byte
	if descFromCtx, ok := storage.StateDescriptorFromContext(ctx); ok && len(descFromCtx) > 0 {
		descriptor = append([]byte(nil), descFromCtx...)
	} else if len(info.Descriptor) > 0 {
		descriptor = append([]byte(nil), info.Descriptor...)
	}
	var reader io.ReadCloser = file
	if encrypted {
		if len(descriptor) == 0 {
			file.Close()
			logger.Debug("disk.read_state.missing_descriptor", "key", key)
			return nil, nil, fmt.Errorf("disk: missing state descriptor for %q", key)
		}
		mat, err := s.crypto.MaterialFromDescriptor(storage.StateObjectContext(key), descriptor)
		if err != nil {
			file.Close()
			logger.Debug("disk.read_state.material_error", "key", key, "error", err)
			return nil, nil, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(file, mat)
		if err != nil {
			file.Close()
			logger.Debug("disk.read_state.decrypt_error", "key", key, "error", err)
			return nil, nil, err
		}
		reader = decReader
	}
	verbose.Debug("disk.read_state.success",
		"key", key,
		"etag", info.ETag,
		"size", info.Size,
		"cipher_size", info.CipherSize,
		"elapsed", time.Since(start),
	)
	stateInfo := &storage.StateInfo{
		Size:       info.Size,
		CipherSize: info.CipherSize,
		ETag:       info.ETag,
		ModifiedAt: info.ModifiedAtUnix,
	}
	if plain, ok := storage.StatePlaintextSizeFromContext(ctx); ok && plain > 0 {
		stateInfo.Size = plain
	}
	if len(descriptor) > 0 {
		stateInfo.Descriptor = append([]byte(nil), descriptor...)
	}
	return reader, stateInfo, nil
}

// WriteState stages and atomically replaces the JSON state file, returning the new ETag.
func (s *Store) WriteState(ctx context.Context, key string, body io.Reader, opts storage.PutStateOptions) (result *storage.PutStateResult, err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.write_state.begin", "key", key, "expected_etag", opts.ExpectedETag)

	encoded, err := s.encodeKey(key)
	if err != nil {
		logger.Debug("disk.write_state.encode_error", "key", key, "error", err)
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
		logger.Debug("disk.write_state.filelock_error", "key", key, "error", err)
		return nil, err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	if err := os.MkdirAll(s.stateDirPath(encoded), 0o755); err != nil {
		err = fmt.Errorf("disk: ensure state dir: %w", err)
		logger.Debug("disk.write_state.ensure_dir_error", "key", key, "error", err)
		return nil, err
	}

	tempFile, err := os.CreateTemp(s.tmpDir, "lockd-state-*")
	if err != nil {
		logger.Debug("disk.write_state.tempfile_error", "key", key, "error", err)
		return nil, err
	}
	moved := false
	defer func() {
		_ = tempFile.Close()
		if !moved {
			_ = os.Remove(tempFile.Name())
		}
	}()

	encrypted := s.crypto != nil && s.crypto.Enabled()
	var descriptor []byte
	hasher := sha256.New()
	destWriter := io.Writer(tempFile)
	closeDest := func() error { return nil }
	if encrypted {
		descFromCtx, ok := storage.StateDescriptorFromContext(ctx)
		var mat kryptograf.Material
		if ok && len(descFromCtx) > 0 {
			descriptor = append([]byte(nil), descFromCtx...)
			mat, err = s.crypto.MaterialFromDescriptor(storage.StateObjectContext(key), descriptor)
			if err != nil {
				logger.Debug("disk.write_state.material_error", "key", key, "error", err)
				return nil, err
			}
		} else {
			var descBytes []byte
			mat, descBytes, err = s.crypto.MintMaterial(storage.StateObjectContext(key))
			if err != nil {
				logger.Debug("disk.write_state.mint_descriptor_error", "key", key, "error", err)
				return nil, err
			}
			descriptor = append([]byte(nil), descBytes...)
		}
		encWriter, err := s.crypto.EncryptWriterForMaterial(tempFile, mat)
		if err != nil {
			logger.Debug("disk.write_state.encrypt_writer_error", "key", key, "error", err)
			return nil, err
		}
		destWriter = encWriter
		closeDest = encWriter.Close
	}
	multi := io.MultiWriter(destWriter, hasher)
	written, err := io.Copy(multi, body)
	if err != nil {
		logger.Debug("disk.write_state.copy_error", "key", key, "error", err)
		return nil, err
	}
	if err := closeDest(); err != nil {
		logger.Debug("disk.write_state.dest_close_error", "key", key, "error", err)
		return nil, err
	}
	var cipherSize int64 = written
	if encrypted {
		if fileInfo, statErr := os.Stat(tempFile.Name()); statErr == nil {
			cipherSize = fileInfo.Size()
		}
		if f, openErr := os.OpenFile(tempFile.Name(), os.O_RDONLY, 0); openErr == nil {
			_ = f.Sync()
			_ = f.Close()
		}
	} else {
		if err := tempFile.Sync(); err != nil {
			logger.Debug("disk.write_state.sync_error", "key", key, "error", err)
			return nil, err
		}
		if err := tempFile.Close(); err != nil {
			logger.Debug("disk.write_state.close_error", "key", key, "error", err)
			return nil, err
		}
	}

	newETag := hex.EncodeToString(hasher.Sum(nil))
	if opts.ExpectedETag != "" {
		info, err := s.readStateRecord(encoded)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				logger.Debug("disk.write_state.cas_not_found", "key", key, "expected_etag", opts.ExpectedETag)
				return nil, storage.ErrNotFound
			}
			logger.Debug("disk.write_state.info_error", "key", key, "error", err)
			return nil, err
		}
		if info.ETag != opts.ExpectedETag {
			logger.Debug("disk.write_state.cas_mismatch", "key", key, "expected_etag", opts.ExpectedETag, "current_etag", info.ETag)
			return nil, storage.ErrCASMismatch
		}
	}

	dataPath := s.stateDataPath(encoded)
	if err := os.Rename(tempFile.Name(), dataPath); err != nil {
		if removeErr := os.Remove(dataPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			logger.Debug("disk.write_state.rename_remove_error", "key", key, "error", removeErr)
			return nil, err
		}
		if err2 := os.Rename(tempFile.Name(), dataPath); err2 != nil {
			logger.Debug("disk.write_state.rename_error", "key", key, "error", err2)
			return nil, err2
		}
	}
	moved = true
	_ = syncDir(filepath.Dir(dataPath))

	info := stateRecord{
		ETag:           newETag,
		Size:           written,
		CipherSize:     cipherSize,
		ModifiedAtUnix: s.now().Unix(),
		Descriptor:     append([]byte(nil), descriptor...),
	}
	if err := s.writeJSONAtomic(s.stateInfoPath(encoded), info, "state-info"); err != nil {
		logger.Debug("disk.write_state.write_info_error", "key", key, "error", err)
		return nil, err
	}

	result = &storage.PutStateResult{
		BytesWritten: written,
		NewETag:      newETag,
	}
	if len(descriptor) > 0 {
		result.Descriptor = append([]byte(nil), descriptor...)
	}
	verbose.Debug("disk.write_state.success",
		"key", key,
		"bytes", written,
		"new_etag", newETag,
		"elapsed", time.Since(start),
	)
	return result, nil
}

// RemoveState deletes the JSON state for key, respecting an expected ETag when supplied.
func (s *Store) RemoveState(ctx context.Context, key string, expectedETag string) (err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.remove_state.begin", "key", key, "expected_etag", expectedETag)

	encoded, err := s.encodeKey(key)
	if err != nil {
		logger.Debug("disk.remove_state.encode_error", "key", key, "error", err)
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
		logger.Debug("disk.remove_state.filelock_error", "key", key, "error", err)
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
			logger.Debug("disk.remove_state.info_error", "key", key, "error", err)
			return err
		}
		if info.ETag != expectedETag {
			logger.Debug("disk.remove_state.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", info.ETag)
			return storage.ErrCASMismatch
		}
	}

	dataPath := s.stateDataPath(encoded)
	infoPath := s.stateInfoPath(encoded)
	errData := os.Remove(dataPath)
	errInfo := os.Remove(infoPath)
	if errData != nil && !errors.Is(errData, os.ErrNotExist) {
		logger.Debug("disk.remove_state.remove_data_error", "key", key, "error", errData)
		return errData
	}
	if errInfo != nil && !errors.Is(errInfo, os.ErrNotExist) {
		logger.Debug("disk.remove_state.remove_info_error", "key", key, "error", errInfo)
		return errInfo
	}
	_ = os.Remove(s.stateDirPath(encoded))
	if errors.Is(errData, os.ErrNotExist) && errors.Is(errInfo, os.ErrNotExist) {
		verbose.Debug("disk.remove_state.not_found", "key", key, "elapsed", time.Since(start))
		return storage.ErrNotFound
	}
	verbose.Debug("disk.remove_state.success", "key", key, "elapsed", time.Since(start))
	return nil
}

func (s *Store) loadObjectInfo(key string) (*storage.ObjectInfo, error) {
	dataPath, err := s.objectDataPath(key)
	if err != nil {
		return nil, err
	}
	infoPath, err := s.objectInfoPath(key)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(dataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("disk: stat object %q: %w", key, err)
	}
	payload, err := os.ReadFile(infoPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("disk: missing object metadata for %q", key)
		}
		return nil, fmt.Errorf("disk: read object metadata for %q: %w", key, err)
	}
	var rec objectInfoRecord
	if err := json.Unmarshal(payload, &rec); err != nil {
		return nil, fmt.Errorf("disk: decode object metadata for %q: %w", key, err)
	}
	if rec.ETag == "" {
		return nil, fmt.Errorf("disk: object %q missing etag", key)
	}
	info := &storage.ObjectInfo{
		Key:          key,
		ETag:         rec.ETag,
		Size:         fi.Size(),
		LastModified: fi.ModTime(),
		ContentType:  rec.ContentType,
		Descriptor:   append([]byte(nil), rec.Descriptor...),
	}
	return info, nil
}

func (s *Store) storeObjectInfo(key string, rec objectInfoRecord) error {
	infoPath, err := s.objectInfoPath(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(infoPath), 0o755); err != nil {
		return fmt.Errorf("disk: prepare object metadata dir for %q: %w", key, err)
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("disk: encode object metadata for %q: %w", key, err)
	}
	tmp, err := os.CreateTemp(s.tmpDir, "objectinfo-*")
	if err != nil {
		return fmt.Errorf("disk: create temp metadata file for %q: %w", key, err)
	}
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return fmt.Errorf("disk: write metadata for %q: %w", key, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return fmt.Errorf("disk: sync metadata for %q: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("disk: close metadata for %q: %w", key, err)
	}
	if err := os.Rename(tmp.Name(), infoPath); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("disk: rename metadata for %q: %w", key, err)
	}
	return nil
}

// ListObjects enumerates on-disk objects using lexical ordering of keys.
func (s *Store) ListObjects(ctx context.Context, opts storage.ListOptions) (*storage.ListResult, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.list_objects.begin", "prefix", opts.Prefix, "start_after", opts.StartAfter, "limit", opts.Limit)

	keys := make([]string, 0, 64)
	err := filepath.WalkDir(s.objectDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".info.json") {
			return nil
		}
		key, err := s.keyFromObjectPath(path)
		if err != nil {
			return err
		}
		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			return nil
		}
		if opts.StartAfter != "" && key <= opts.StartAfter {
			return nil
		}
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		logger.Debug("disk.list_objects.walk_error", "error", err)
		return nil, fmt.Errorf("disk: list objects: %w", err)
	}
	sort.Strings(keys)
	limit := len(keys)
	if opts.Limit > 0 && opts.Limit < limit {
		limit = opts.Limit
	}
	result := &storage.ListResult{
		Objects: make([]storage.ObjectInfo, 0, limit),
	}
	for i := 0; i < limit; i++ {
		info, err := s.loadObjectInfo(keys[i])
		if err != nil {
			logger.Debug("disk.list_objects.load_error", "key", keys[i], "error", err)
			return nil, err
		}
		result.Objects = append(result.Objects, *info)
	}
	if limit > 0 && limit < len(keys) {
		result.Truncated = true
		result.NextStartAfter = keys[limit-1]
	}
	verbose.Debug("disk.list_objects.success",
		"prefix", opts.Prefix,
		"start_after", opts.StartAfter,
		"limit", opts.Limit,
		"count", len(result.Objects),
		"truncated", result.Truncated,
		"elapsed", time.Since(start),
	)
	return result, nil
}

// GetObject streams the object payload for key.
func (s *Store) GetObject(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	logger, verbose := s.loggers(ctx)
	verbose.Trace("disk.get_object.begin", "key", key)
	dataPath, err := s.objectDataPath(key)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(dataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			verbose.Debug("disk.get_object.not_found", "key", key)
			return nil, nil, storage.ErrNotFound
		}
		logger.Debug("disk.get_object.open_error", "key", key, "error", err)
		return nil, nil, fmt.Errorf("disk: open object %q: %w", key, err)
	}
	info, err := s.loadObjectInfo(key)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if plain, ok := storage.ObjectPlaintextSizeFromContext(ctx); ok && plain > 0 {
		info.Size = plain
	}
	verbose.Debug("disk.get_object.success", "key", key, "etag", info.ETag, "size", info.Size)
	return f, info, nil
}

// PutObject writes an object to disk with optional conditional semantics.
func (s *Store) PutObject(ctx context.Context, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	logger, verbose := s.loggers(ctx)
	verbose.Trace("disk.put_object.begin", "key", key, "expected_etag", opts.ExpectedETag, "if_not_exists", opts.IfNotExists)
	dataPath, err := s.objectDataPath(key)
	if err != nil {
		return nil, err
	}
	var current *storage.ObjectInfo
	if opts.IfNotExists || opts.ExpectedETag != "" {
		info, err := s.loadObjectInfo(key)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			logger.Debug("disk.put_object.load_error", "key", key, "error", err)
			return nil, err
		}
		if err == nil {
			current = info
		}
		if opts.IfNotExists && current != nil {
			verbose.Debug("disk.put_object.exists", "key", key)
			return nil, storage.ErrCASMismatch
		}
		if opts.ExpectedETag != "" {
			if current == nil {
				verbose.Debug("disk.put_object.cas_missing", "key", key, "expected_etag", opts.ExpectedETag)
				return nil, storage.ErrNotFound
			}
			if current.ETag != opts.ExpectedETag {
				verbose.Debug("disk.put_object.cas_mismatch", "key", key, "expected_etag", opts.ExpectedETag, "current_etag", current.ETag)
				return nil, storage.ErrCASMismatch
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(dataPath), 0o755); err != nil {
		return nil, fmt.Errorf("disk: prepare object directory for %q: %w", key, err)
	}
	tmp, err := os.CreateTemp(s.tmpDir, "object-*")
	if err != nil {
		return nil, fmt.Errorf("disk: create temp object for %q: %w", key, err)
	}
	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmp, hasher), body)
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("disk: write object %q: %w", key, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("disk: sync object %q: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("disk: close object %q: %w", key, err)
	}
	newETag := hex.EncodeToString(hasher.Sum(nil))
	if err := os.Rename(tmp.Name(), dataPath); err != nil {
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("disk: rename object %q: %w", key, err)
	}
	now := s.now()
	if err := s.storeObjectInfo(key, objectInfoRecord{
		ETag:          newETag,
		ContentType:   opts.ContentType,
		UpdatedAtUnix: now.Unix(),
		Descriptor:    append([]byte(nil), opts.Descriptor...),
	}); err != nil {
		return nil, err
	}
	info := &storage.ObjectInfo{
		Key:          key,
		ETag:         newETag,
		Size:         written,
		LastModified: now,
		ContentType:  opts.ContentType,
		Descriptor:   append([]byte(nil), opts.Descriptor...),
	}
	if fi, err := os.Stat(dataPath); err == nil {
		info.Size = fi.Size()
		info.LastModified = fi.ModTime()
	} else {
		logger.Debug("disk.put_object.stat_error", "key", key, "error", err)
	}
	verbose.Debug("disk.put_object.success", "key", key, "size", info.Size, "etag", info.ETag)
	return info, nil
}

// DeleteObject removes an object from disk applying optional CAS semantics.
func (s *Store) DeleteObject(ctx context.Context, key string, opts storage.DeleteObjectOptions) error {
	logger, verbose := s.loggers(ctx)
	verbose.Trace("disk.delete_object.begin", "key", key, "expected_etag", opts.ExpectedETag, "ignore_not_found", opts.IgnoreNotFound)
	dataPath, err := s.objectDataPath(key)
	if err != nil {
		return err
	}
	info, err := s.loadObjectInfo(key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			if opts.IgnoreNotFound {
				return nil
			}
			return storage.ErrNotFound
		}
		return err
	}
	if opts.ExpectedETag != "" && info.ETag != opts.ExpectedETag {
		verbose.Debug("disk.delete_object.cas_mismatch", "key", key, "expected_etag", opts.ExpectedETag, "current_etag", info.ETag)
		return storage.ErrCASMismatch
	}
	if err := os.Remove(dataPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Debug("disk.delete_object.remove_error", "key", key, "error", err)
		return fmt.Errorf("disk: remove object %q: %w", key, err)
	}
	infoPath, err := s.objectInfoPath(key)
	if err != nil {
		return err
	}
	if err := os.Remove(infoPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Debug("disk.delete_object.remove_info_error", "key", key, "error", err)
		return fmt.Errorf("disk: remove object metadata %q: %w", key, err)
	}
	verbose.Debug("disk.delete_object.success", "key", key)

	dir := filepath.Dir(dataPath)
	for dir != s.objectDir && dir != "." {
		if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOTEMPTY) {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
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
	etag, meta, err := storage.UnmarshalMetaRecord(data, s.crypto)
	if err != nil {
		return nil, fmt.Errorf("disk: decode meta: %w", err)
	}
	if meta == nil {
		meta = &storage.Meta{}
	}
	return &metaRecord{
		ETag: etag,
		Meta: meta,
	}, nil
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

func (s *Store) writeBytesAtomic(dest string, payload []byte, prefix string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.tmpDir, "lockd-"+prefix+"-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
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
	return nil
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
		if !strings.HasSuffix(name, ".pb") {
			continue
		}
		encoded := strings.TrimSuffix(name, ".pb")
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
