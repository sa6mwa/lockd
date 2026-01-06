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
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
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

// DefaultNamespaceConfig returns the preferred namespace settings for disk storage.
func (s *Store) DefaultNamespaceConfig() namespaces.Config {
	cfg := namespaces.DefaultConfig()
	cfg.Query.Preferred = search.EngineIndex
	cfg.Query.Fallback = namespaces.FallbackNone
	return cfg
}

// IndexerFlushDefaults suggests disk-friendly indexer tuning.
func (s *Store) IndexerFlushDefaults() (int, time.Duration) {
	return 1000, 10 * time.Second
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
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("disk: prepare root directory %q: %w", root, err)
	}

	s := &Store{
		root:            root,
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
func (s *Store) QueueWatchStatus() storage.QueueWatchStatus {
	return storage.QueueWatchStatus{
		Enabled: s.queueWatchEnabled,
		Mode:    s.queueWatchMode,
		Reason:  s.queueWatchReason,
	}
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

// BackendHash returns the stable identity hash for this backend.
func (s *Store) BackendHash(ctx context.Context) (string, error) {
	root := s.root
	if abs, err := filepath.Abs(root); err == nil {
		root = abs
	}
	desc := fmt.Sprintf("disk|%s", root)
	result, err := storage.ResolveBackendHash(ctx, s, desc)
	return result.Hash, err
}

func (s *Store) loggers(ctx context.Context) (pslog.Logger, pslog.Logger) {
	logger := pslog.LoggerFromContext(ctx)
	logger = logger.With("storage_backend", "disk")
	return logger, logger
}

func (s *Store) namespacePath(namespace string, elems ...string) string {
	parts := append([]string{s.root, namespace}, elems...)
	return filepath.Join(parts...)
}

func (s *Store) namespaceMetaDir(namespace string) string {
	return s.namespacePath(namespace, "meta")
}

func (s *Store) namespaceStateDir(namespace string) string {
	return s.namespacePath(namespace, "state")
}

func (s *Store) namespaceObjectsDir(namespace string) string {
	return s.namespacePath(namespace, "objects")
}

func (s *Store) namespaceLocksDir(namespace string) string {
	return s.namespacePath(namespace, "locks")
}

func (s *Store) namespaceTmpDir(namespace string) string {
	return s.namespacePath(namespace, "tmp")
}

func (s *Store) encodeKey(namespace, key string) (string, string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return "", "", fmt.Errorf("disk: namespace required")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", fmt.Errorf("disk: key required")
	}
	clean := path.Clean("/" + key)
	if clean == "/" {
		return "", "", fmt.Errorf("disk: invalid key %q", key)
	}
	clean = strings.TrimPrefix(clean, "/")
	if clean == "" || strings.HasPrefix(clean, "../") {
		return "", "", fmt.Errorf("disk: invalid key %q", key)
	}
	encoded := url.PathEscape(clean)
	return namespace, encoded, nil
}

func (s *Store) metaPath(namespace, encoded string) string {
	return filepath.Join(s.namespaceMetaDir(namespace), encoded+".pb")
}

func (s *Store) lockFilePath(key string) (string, error) {
	parts, err := storage.SplitNamespacedKey(key)
	if err != nil {
		return "", err
	}
	ns := parts.Namespace
	segments := parts.Segments
	if len(segments) == 0 {
		return "", fmt.Errorf("disk: invalid lock key %q", key)
	}
	encoded := encodePathSegments(segments)
	lockDir := filepath.Join(append([]string{s.namespaceLocksDir(ns)}, encoded[:len(encoded)-1]...)...)
	if err := os.MkdirAll(lockDir, 0o755); err != nil {
		return "", fmt.Errorf("disk: prepare lock directory %q: %w", lockDir, err)
	}
	filename := encoded[len(encoded)-1] + ".lock"
	return filepath.Join(lockDir, filename), nil
}

func (s *Store) stateDirPath(namespace, encoded string) string {
	return filepath.Join(s.namespaceStateDir(namespace), encoded)
}

func (s *Store) stateDataPath(namespace, encoded string) string {
	return filepath.Join(s.stateDirPath(namespace, encoded), "data")
}

func (s *Store) stateInfoPath(namespace, encoded string) string {
	return filepath.Join(s.stateDirPath(namespace, encoded), "info.json")
}

func encodePathSegments(segments []string) []string {
	encoded := make([]string, len(segments))
	for i, segment := range segments {
		encoded[i] = url.PathEscape(segment)
	}
	return encoded
}

func decodePathSegments(segments []string) ([]string, error) {
	decoded := make([]string, len(segments))
	for i, segment := range segments {
		val, err := url.PathUnescape(segment)
		if err != nil {
			return nil, err
		}
		decoded[i] = val
	}
	return decoded, nil
}

func (s *Store) objectDataPath(namespace, key string) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return "", fmt.Errorf("disk: namespace required")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("disk: object key required")
	}
	clean := path.Clean("/" + key)
	if clean == "/" {
		return "", fmt.Errorf("disk: invalid object key %q", key)
	}
	clean = strings.TrimPrefix(clean, "/")
	if strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("disk: invalid object key %q", key)
	}
	segments := strings.Split(clean, "/")
	encoded := encodePathSegments(segments)
	dataPath := filepath.Join(append([]string{s.namespaceObjectsDir(namespace)}, encoded...)...)
	return dataPath, nil
}

func (s *Store) objectInfoPath(namespace, key string) (string, error) {
	dataPath, err := s.objectDataPath(namespace, key)
	if err != nil {
		return "", err
	}
	return dataPath + ".info.json", nil
}

func (s *Store) keyFromObjectPath(namespace string, objectPath string) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return "", fmt.Errorf("disk: namespace required")
	}
	clean := filepath.Clean(objectPath)
	expectedRoot := s.namespaceObjectsDir(namespace)
	if !strings.HasPrefix(clean, expectedRoot) {
		return "", fmt.Errorf("disk: object path outside namespace %q: %s", namespace, objectPath)
	}
	rel, err := filepath.Rel(expectedRoot, clean)
	if err != nil {
		return "", fmt.Errorf("disk: relative path: %w", err)
	}
	rel = filepath.ToSlash(rel)
	if rel == "" || rel == "." {
		return "", fmt.Errorf("disk: unexpected object directory %q", objectPath)
	}
	parts := strings.Split(rel, "/")
	decoded, err := decodePathSegments(parts)
	if err != nil {
		return "", err
	}
	return path.Join(decoded...), nil
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
func (s *Store) LoadMeta(ctx context.Context, namespace, key string) (storage.LoadMetaResult, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.load_meta.begin", "key", key)

	ns, encoded, err := s.encodeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.load_meta.encode_error", "key", key, "error", err)
		return storage.LoadMetaResult{}, err
	}
	rec, err := s.readMetaRecord(ns, encoded)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			verbose.Debug("disk.load_meta.not_found", "key", key, "elapsed", time.Since(start))
		} else {
			logger.Debug("disk.load_meta.error", "key", key, "error", err, "elapsed", time.Since(start))
		}
		return storage.LoadMetaResult{}, err
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
	return storage.LoadMetaResult{Meta: &meta, ETag: rec.ETag}, nil
}

// StoreMeta persists metadata with conditional semantics when expectedETag is provided.
func (s *Store) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (etag string, err error) {
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

	ns, encoded, err := s.encodeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.store_meta.encode_error", "key", key, "error", err)
		return "", err
	}

	storageKey := path.Join(ns, key)
	glob := globalKeyMutex(storageKey)
	glob.Lock()
	defer glob.Unlock()

	mu := s.keyLock(storageKey)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(storageKey)
	if err != nil {
		logger.Debug("disk.store_meta.filelock_error", "key", key, "error", err)
		return "", err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	current, err := s.readMetaRecord(namespace, encoded)
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
	metaFile := s.metaPath(namespace, encoded)
	if err := os.MkdirAll(filepath.Dir(metaFile), 0o755); err != nil {
		logger.Debug("disk.store_meta.mkdir_error", "key", key, "dir", filepath.Dir(metaFile), "error", err)
		return "", err
	}
	if err := s.writeBytesAtomic(namespace, metaFile, payload, "meta"); err != nil {
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
func (s *Store) DeleteMeta(ctx context.Context, namespace, key string, expectedETag string) (err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.delete_meta.begin", "key", key, "expected_etag", expectedETag)

	ns, encoded, err := s.encodeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.delete_meta.encode_error", "key", key, "error", err)
		return err
	}
	storageKey := path.Join(ns, key)
	glob := globalKeyMutex(storageKey)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(storageKey)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(storageKey)
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
		rec, err := s.readMetaRecord(ns, encoded)
		if err != nil {
			logger.Debug("disk.delete_meta.read_error", "key", key, "error", err)
			return err
		}
		if rec.ETag != expectedETag {
			logger.Debug("disk.delete_meta.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", rec.ETag)
			return storage.ErrCASMismatch
		}
	}
	metaFile := s.metaPath(ns, encoded)
	if err := os.Remove(metaFile); err != nil {
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

// ListMetaKeys scans the metadata directory for the provided namespace and returns keys relative to that namespace.
func (s *Store) ListMetaKeys(ctx context.Context, namespace string) ([]string, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.list_meta_keys.begin")
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("disk: namespace required")
	}
	metaDir := s.namespaceMetaDir(namespace)
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		logger.Debug("disk.list_meta_keys.error", "namespace", namespace, "error", err)
		return nil, err
	}
	var keys []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".pb") {
			continue
		}
		encoded := strings.TrimSuffix(name, ".pb")
		decoded, err := url.PathUnescape(encoded)
		if err != nil {
			logger.Debug("disk.list_meta_keys.decode_error", "namespace", namespace, "encoded", encoded, "error", err)
			continue
		}
		keys = append(keys, decoded)
	}
	verbose.Debug("disk.list_meta_keys.success", "namespace", namespace, "count", len(keys), "elapsed", time.Since(start))
	return keys, nil
}

// ReadState opens the immutable JSON state file for key and returns its metadata.
func (s *Store) ReadState(ctx context.Context, namespace, key string) (storage.ReadStateResult, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.read_state.begin", "key", key)

	_, encoded, err := s.encodeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.read_state.encode_error", "key", key, "error", err)
		return storage.ReadStateResult{}, err
	}
	info, err := s.readStateRecord(namespace, encoded)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			verbose.Debug("disk.read_state.not_found", "key", key, "elapsed", time.Since(start))
		} else {
			logger.Debug("disk.read_state.info_error", "key", key, "error", err)
		}
		return storage.ReadStateResult{}, err
	}
	file, err := os.Open(s.stateDataPath(namespace, encoded))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			verbose.Debug("disk.read_state.not_found", "key", key, "elapsed", time.Since(start))
			return storage.ReadStateResult{}, storage.ErrNotFound
		}
		logger.Debug("disk.read_state.open_error", "key", key, "error", err)
		return storage.ReadStateResult{}, err
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
			return storage.ReadStateResult{}, fmt.Errorf("disk: missing state descriptor for %q", key)
		}
		storageKey := path.Join(namespace, key)
		mat, err := s.crypto.MaterialFromDescriptor(storage.StateObjectContext(storageKey), descriptor)
		if err != nil {
			file.Close()
			logger.Debug("disk.read_state.material_error", "key", key, "error", err)
			return storage.ReadStateResult{}, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(file, mat)
		if err != nil {
			file.Close()
			logger.Debug("disk.read_state.decrypt_error", "key", key, "error", err)
			return storage.ReadStateResult{}, err
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
	return storage.ReadStateResult{Reader: reader, Info: stateInfo}, nil
}

// WriteState stages and atomically replaces the JSON state file, returning the new ETag.
func (s *Store) WriteState(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutStateOptions) (result *storage.PutStateResult, err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.write_state.begin", "key", key, "expected_etag", opts.ExpectedETag)

	ns, encoded, err := s.encodeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.write_state.encode_error", "key", key, "error", err)
		return nil, err
	}
	storageKey := path.Join(ns, key)
	glob := globalKeyMutex(storageKey)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(storageKey)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(storageKey)
	if err != nil {
		logger.Debug("disk.write_state.filelock_error", "key", key, "error", err)
		return nil, err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	stateDir := s.stateDirPath(ns, encoded)
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		err = fmt.Errorf("disk: ensure state dir: %w", err)
		logger.Debug("disk.write_state.ensure_dir_error", "key", key, "error", err)
		return nil, err
	}
	tmpDir := s.namespaceTmpDir(ns)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		logger.Debug("disk.write_state.tmpdir_error", "key", key, "error", err)
		return nil, err
	}

	tempFile, err := os.CreateTemp(tmpDir, "lockd-state-*")
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
			mat, err = s.crypto.MaterialFromDescriptor(storage.StateObjectContext(storageKey), descriptor)
			if err != nil {
				logger.Debug("disk.write_state.material_error", "key", key, "error", err)
				return nil, err
			}
		} else {
			var minted storage.MaterialResult
			minted, err = s.crypto.MintMaterial(storage.StateObjectContext(storageKey))
			if err != nil {
				logger.Debug("disk.write_state.mint_descriptor_error", "key", key, "error", err)
				return nil, err
			}
			descriptor = append([]byte(nil), minted.Descriptor...)
			mat = minted.Material
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
		info, err := s.readStateRecord(ns, encoded)
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
	} else if opts.IfNotExists {
		if _, err := s.readStateRecord(ns, encoded); err == nil {
			logger.Debug("disk.write_state.if_not_exists_conflict", "key", key)
			return nil, storage.ErrCASMismatch
		}
	}

	dataPath := s.stateDataPath(ns, encoded)
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
	if err := s.writeJSONAtomic(ns, s.stateInfoPath(ns, encoded), info, "state-info"); err != nil {
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

// Remove deletes the JSON state for key, respecting an expected ETag when supplied.
func (s *Store) Remove(ctx context.Context, namespace, key string, expectedETag string) (err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.remove_state.begin", "key", key, "expected_etag", expectedETag)

	ns, encoded, err := s.encodeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.remove_state.encode_error", "key", key, "error", err)
		return err
	}
	storageKey := path.Join(ns, key)
	glob := globalKeyMutex(storageKey)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(storageKey)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(storageKey)
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
		info, err := s.readStateRecord(ns, encoded)
		if err != nil {
			logger.Debug("disk.remove_state.info_error", "key", key, "error", err)
			return err
		}
		if info.ETag != expectedETag {
			logger.Debug("disk.remove_state.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", info.ETag)
			return storage.ErrCASMismatch
		}
	}

	dataPath := s.stateDataPath(ns, encoded)
	infoPath := s.stateInfoPath(ns, encoded)
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
	_ = os.Remove(s.stateDirPath(ns, encoded))
	if errors.Is(errData, os.ErrNotExist) && errors.Is(errInfo, os.ErrNotExist) {
		verbose.Debug("disk.remove_state.not_found", "key", key, "elapsed", time.Since(start))
		return storage.ErrNotFound
	}
	verbose.Debug("disk.remove_state.success", "key", key, "elapsed", time.Since(start))
	return nil
}

func (s *Store) loadObjectInfo(namespace, key string) (*storage.ObjectInfo, error) {
	dataPath, err := s.objectDataPath(namespace, key)
	if err != nil {
		return nil, err
	}
	infoPath, err := s.objectInfoPath(namespace, key)
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
			return nil, storage.ErrNotFound
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

func (s *Store) storeObjectInfo(namespace, key string, rec objectInfoRecord) error {
	infoPath, err := s.objectInfoPath(namespace, key)
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
	tmpDir := s.namespaceTmpDir(namespace)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return fmt.Errorf("disk: prepare tmp dir for %q: %w", key, err)
	}
	tmp, err := os.CreateTemp(tmpDir, "objectinfo-*")
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
func (s *Store) ListObjects(ctx context.Context, namespace string, opts storage.ListOptions) (*storage.ListResult, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("disk: namespace required")
	}
	verbose.Trace("disk.list_objects.begin",
		"namespace", namespace,
		"prefix", opts.Prefix,
		"start_after", opts.StartAfter,
		"limit", opts.Limit,
	)

	objectsDir := s.namespaceObjectsDir(namespace)
	if _, err := os.Stat(objectsDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &storage.ListResult{}, nil
		}
		logger.Debug("disk.list_objects.stat_error", "namespace", namespace, "error", err)
		return nil, fmt.Errorf("disk: list objects: %w", err)
	}

	keys := make([]string, 0, 64)
	err := filepath.WalkDir(objectsDir, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".info.json") {
			return nil
		}
		key, err := s.keyFromObjectPath(namespace, p)
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
		logger.Debug("disk.list_objects.walk_error", "namespace", namespace, "error", err)
		return nil, fmt.Errorf("disk: list objects: %w", err)
	}

	sort.Strings(keys)
	capHint := len(keys)
	if opts.Limit > 0 && opts.Limit < capHint {
		capHint = opts.Limit
	}
	result := &storage.ListResult{
		Objects: make([]storage.ObjectInfo, 0, capHint),
	}
	maxItems := opts.Limit
	if maxItems <= 0 {
		maxItems = len(keys)
	}
	lastReturned := ""
	for _, key := range keys {
		if maxItems > 0 && len(result.Objects) >= maxItems {
			result.Truncated = true
			result.NextStartAfter = lastReturned
			break
		}
		info, err := s.loadObjectInfo(namespace, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			logger.Debug("disk.list_objects.load_error", "namespace", namespace, "key", key, "error", err)
			return nil, err
		}
		result.Objects = append(result.Objects, *info)
		lastReturned = key
	}
	verbose.Debug("disk.list_objects.success",
		"namespace", namespace,
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
func (s *Store) GetObject(ctx context.Context, namespace, key string) (storage.GetObjectResult, error) {
	logger, verbose := s.loggers(ctx)
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return storage.GetObjectResult{}, fmt.Errorf("disk: namespace required")
	}
	verbose.Trace("disk.get_object.begin", "namespace", namespace, "key", key)
	dataPath, err := s.objectDataPath(namespace, key)
	if err != nil {
		return storage.GetObjectResult{}, err
	}
	f, err := os.Open(dataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			verbose.Debug("disk.get_object.not_found", "key", key)
			return storage.GetObjectResult{}, storage.ErrNotFound
		}
		logger.Debug("disk.get_object.open_error", "namespace", namespace, "key", key, "error", err)
		return storage.GetObjectResult{}, fmt.Errorf("disk: open object %q: %w", key, err)
	}
	info, err := s.loadObjectInfo(namespace, key)
	if err != nil {
		f.Close()
		return storage.GetObjectResult{}, err
	}
	if plain, ok := storage.ObjectPlaintextSizeFromContext(ctx); ok && plain > 0 {
		info.Size = plain
	}
	verbose.Debug("disk.get_object.success", "namespace", namespace, "key", key, "etag", info.ETag, "size", info.Size)
	return storage.GetObjectResult{Reader: f, Info: info}, nil
}

// PutObject writes an object to disk with optional conditional semantics.
func (s *Store) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	logger, verbose := s.loggers(ctx)
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("disk: namespace required")
	}
	verbose.Trace("disk.put_object.begin", "namespace", namespace, "key", key, "expected_etag", opts.ExpectedETag, "if_not_exists", opts.IfNotExists)
	dataPath, err := s.objectDataPath(namespace, key)
	if err != nil {
		return nil, err
	}
	var current *storage.ObjectInfo
	if opts.IfNotExists || opts.ExpectedETag != "" {
		info, err := s.loadObjectInfo(namespace, key)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			logger.Debug("disk.put_object.load_error", "namespace", namespace, "key", key, "error", err)
			return nil, err
		}
		if err == nil {
			current = info
		}
		if opts.IfNotExists && current != nil {
			verbose.Debug("disk.put_object.exists", "namespace", namespace, "key", key)
			return nil, storage.ErrCASMismatch
		}
		if opts.ExpectedETag != "" {
			if current == nil {
				verbose.Debug("disk.put_object.cas_missing", "namespace", namespace, "key", key, "expected_etag", opts.ExpectedETag)
				return nil, storage.ErrNotFound
			}
			if current.ETag != opts.ExpectedETag {
				verbose.Debug("disk.put_object.cas_mismatch", "namespace", namespace, "key", key, "expected_etag", opts.ExpectedETag, "current_etag", current.ETag)
				return nil, storage.ErrCASMismatch
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(dataPath), 0o755); err != nil {
		return nil, fmt.Errorf("disk: prepare object directory for %q: %w", key, err)
	}
	tmpDir := s.namespaceTmpDir(namespace)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("disk: prepare tmp dir for %q: %w", key, err)
	}
	tmp, err := os.CreateTemp(tmpDir, "object-*")
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
	if err := s.storeObjectInfo(namespace, key, objectInfoRecord{
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
		logger.Debug("disk.put_object.stat_error", "namespace", namespace, "key", key, "error", err)
	}
	verbose.Debug("disk.put_object.success", "namespace", namespace, "key", key, "size", info.Size, "etag", info.ETag)
	return info, nil
}

// DeleteObject removes an object from disk applying optional CAS semantics.
func (s *Store) DeleteObject(ctx context.Context, namespace, key string, opts storage.DeleteObjectOptions) error {
	logger, verbose := s.loggers(ctx)
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return fmt.Errorf("disk: namespace required")
	}
	verbose.Trace("disk.delete_object.begin", "namespace", namespace, "key", key, "expected_etag", opts.ExpectedETag, "ignore_not_found", opts.IgnoreNotFound)
	dataPath, err := s.objectDataPath(namespace, key)
	if err != nil {
		return err
	}
	info, err := s.loadObjectInfo(namespace, key)
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
		logger.Debug("disk.delete_object.remove_error", "namespace", namespace, "key", key, "error", err)
		return fmt.Errorf("disk: remove object %q: %w", key, err)
	}
	infoPath, err := s.objectInfoPath(namespace, key)
	if err != nil {
		return err
	}
	if err := os.Remove(infoPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Debug("disk.delete_object.remove_info_error", "namespace", namespace, "key", key, "error", err)
		return fmt.Errorf("disk: remove object metadata %q: %w", key, err)
	}
	verbose.Debug("disk.delete_object.success", "namespace", namespace, "key", key)

	base := s.namespaceObjectsDir(namespace)
	dir := filepath.Dir(dataPath)
	for dir != base && dir != "." {
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

func (s *Store) readMetaRecord(namespace, encoded string) (*metaRecord, error) {
	data, err := os.ReadFile(s.metaPath(namespace, encoded))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	record, err := storage.UnmarshalMetaRecord(data, s.crypto)
	if err != nil {
		return nil, fmt.Errorf("disk: decode meta: %w", err)
	}
	meta := record.Meta
	if meta == nil {
		meta = &storage.Meta{}
	}
	return &metaRecord{
		ETag: record.ETag,
		Meta: meta,
	}, nil
}

func (s *Store) readStateRecord(namespace, encoded string) (*stateRecord, error) {
	data, err := os.ReadFile(s.stateInfoPath(namespace, encoded))
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

func (s *Store) writeBytesAtomic(namespace, dest string, payload []byte, prefix string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmpDir := s.namespaceTmpDir(namespace)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(tmpDir, "lockd-"+prefix+"-*")
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

func (s *Store) writeJSONAtomic(namespace, dest string, v any, prefix string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmpDir := s.namespaceTmpDir(namespace)
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(tmpDir, "lockd-"+prefix+"-*")
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
	nsEntries, err := os.ReadDir(s.root)
	if err != nil {
		return
	}
	ctx := context.Background()
	for _, nsEntry := range nsEntries {
		if !nsEntry.IsDir() {
			continue
		}
		namespace := nsEntry.Name()
		metaDir := s.namespaceMetaDir(namespace)
		metaEntries, err := os.ReadDir(metaDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			continue
		}
		for _, entry := range metaEntries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if !strings.HasSuffix(name, ".pb") {
				continue
			}
			encoded := strings.TrimSuffix(name, ".pb")
			rec, err := s.readMetaRecord(namespace, encoded)
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
			decoded, err := url.PathUnescape(encoded)
			if err != nil {
				continue
			}
			key := decoded
			_ = s.DeleteMeta(ctx, namespace, key, rec.ETag)
			_ = s.Remove(ctx, namespace, key, "")
		}
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
