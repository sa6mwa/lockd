package disk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

// Config captures the tunables for the disk backend.
type Config struct {
	Root                 string
	Retention            time.Duration
	JanitorInterval      time.Duration
	Now                  func() time.Time
	QueueWatch           bool
	LockFileCacheSize    int
	Crypto               *storage.Crypto
	LogstoreCommitMaxOps int
	LogstoreSegmentSize  int64
}

// Store implements storage.Backend backed by the local filesystem.
type Store struct {
	root            string
	retention       time.Duration
	janitorInterval time.Duration
	now             func() time.Time
	crypto          *storage.Crypto
	logstore        *logStore
	lockCache       *lockFileCache
	singleWriter    atomic.Bool

	locks    lockStripes
	lockDirs sync.Map

	stopJanitor chan struct{}
	doneJanitor chan struct{}

	queueWatchEnabled bool
	queueWatchMode    string
	queueWatchReason  string
}

// FsyncStats returns aggregated fsync batch stats for diagnostics.
func (s *Store) FsyncStats() FsyncBatchStats {
	if s == nil || s.logstore == nil {
		return FsyncBatchStats{}
	}
	return s.logstore.FsyncStats()
}

// SupportsConcurrentWrites reports whether the disk backend can safely handle
// multiple concurrent writers. The logstore is single-writer only.
func (s *Store) SupportsConcurrentWrites() bool {
	return false
}

// SetSingleWriter enables single-writer optimizations when the backend is
// exclusively owned by this process.
func (s *Store) SetSingleWriter(enabled bool) {
	if s == nil {
		return
	}
	if s.singleWriter.Load() == enabled {
		return
	}
	s.singleWriter.Store(enabled)
	if s.logstore != nil {
		s.logstore.setSingleWriter(enabled)
	}
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

const (
	defaultLockFileCacheSize = 2048
	lockStripeCount          = 4096
	lockStripeMask           = lockStripeCount - 1
)

type lockStripes struct {
	stripes [lockStripeCount]sync.Mutex
}

func (l *lockStripes) lock(namespace, key string) *sync.Mutex {
	idx := lockStripeIndex(namespace, key) & lockStripeMask
	return &l.stripes[int(idx)]
}

func lockStripeIndex(namespace, key string) uint64 {
	const (
		fnvOffset64 = 14695981039346656037
		fnvPrime64  = 1099511628211
	)
	hash := uint64(fnvOffset64)
	for i := 0; i < len(namespace); i++ {
		hash ^= uint64(namespace[i])
		hash *= fnvPrime64
	}
	hash ^= 0xff
	hash *= fnvPrime64
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= fnvPrime64
	}
	return hash
}

var globalLocks lockStripes

func globalKeyMutex(namespace, key string) *sync.Mutex {
	return globalLocks.lock(namespace, key)
}

type fileLock struct {
	file  *os.File
	cache *lockFileCache
	entry *lockFileEntry
}

func (f *fileLock) Unlock() error {
	if f.file == nil {
		return nil
	}
	if err := unlockFile(f.file); err != nil {
		if f.cache != nil && f.entry != nil {
			f.cache.discard(f.entry)
		} else {
			_ = f.file.Close()
		}
		return err
	}
	if f.cache != nil && f.entry != nil {
		f.cache.release(f.entry)
		return nil
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
	cacheSize := cfg.LockFileCacheSize
	if cacheSize == 0 {
		cacheSize = defaultLockFileCacheSize
	} else if cacheSize < 0 {
		cacheSize = 0
	}
	s.lockCache = newLockFileCache(cacheSize)
	s.logstore = newLogStore(root, s.now, s.crypto, cfg.LogstoreCommitMaxOps, cfg.LogstoreSegmentSize, isNFS(root))
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

func (s *Store) queueNotifyPath(namespace, queue string) (string, error) {
	namespace = strings.TrimSpace(namespace)
	queue = strings.TrimSpace(queue)
	if namespace == "" {
		return "", fmt.Errorf("disk: queue namespace required")
	}
	if queue == "" {
		return "", fmt.Errorf("disk: queue name required")
	}
	dir := filepath.Join(s.root, namespace, "logstore", "queue-notify")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("disk: prepare queue notify dir: %w", err)
	}
	return filepath.Join(dir, queue+".notify"), nil
}

func (s *Store) touchQueueNotify(namespace, queue string) {
	if !s.queueWatchEnabled {
		return
	}
	path, err := s.queueNotifyPath(namespace, queue)
	if err != nil {
		return
	}
	now := time.Now()
	if err := os.Chtimes(path, now, now); err == nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	_ = f.Close()
}

func queueNameFromObjectKey(key string) (string, bool) {
	key = strings.TrimPrefix(strings.TrimSpace(key), "/")
	parts := strings.Split(key, "/")
	if len(parts) < 2 {
		return "", false
	}
	if parts[0] != "q" {
		return "", false
	}
	if parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

func (s *Store) keyLock(namespace, key string) *sync.Mutex {
	return s.locks.lock(namespace, key)
}

func (s *Store) acquireFileLock(namespace, key string) (*fileLock, error) {
	if s.singleWriter.Load() {
		return &fileLock{}, nil
	}
	lockPath, err := s.lockFilePath(namespace, key)
	if err != nil {
		return nil, err
	}
	if s.lockCache != nil {
		entry, err := s.lockCache.acquire(lockPath)
		if err != nil {
			return nil, fmt.Errorf("disk: open lock: %w", err)
		}
		if err := lockFile(entry.file); err != nil {
			s.lockCache.discard(entry)
			return nil, fmt.Errorf("disk: lock key: %w", err)
		}
		return &fileLock{file: entry.file, cache: s.lockCache, entry: entry}, nil
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
	if s.lockCache != nil {
		s.lockCache.close()
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
	result, err := storage.ResolveBackendHash(ctx, s, desc, s.crypto)
	return result.Hash, err
}

func (s *Store) loggers(ctx context.Context) (pslog.Logger, pslog.Logger) {
	logger := pslog.LoggerFromContext(ctx)
	return logger, logger
}

func (s *Store) namespacePath(namespace string, elems ...string) string {
	parts := append([]string{s.root, namespace}, elems...)
	return filepath.Join(parts...)
}

func (s *Store) namespaceLocksDir(namespace string) string {
	return s.namespacePath(namespace, "locks")
}

func (s *Store) normalizeKey(namespace, key string) (string, string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return "", "", fmt.Errorf("disk: namespace required")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", fmt.Errorf("disk: key required")
	}
	clean := key
	if strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") || strings.Contains(key, "//") || strings.Contains(key, "..") {
		clean = path.Clean("/" + key)
		if clean == "/" {
			return "", "", fmt.Errorf("disk: invalid key %q", key)
		}
		clean = strings.TrimPrefix(clean, "/")
		if clean == "" || strings.HasPrefix(clean, "../") {
			return "", "", fmt.Errorf("disk: invalid key %q", key)
		}
	}
	return namespace, clean, nil
}

func joinNamespaceKey(namespace, key string) string {
	if namespace == "" {
		return key
	}
	if key == "" {
		return namespace
	}
	if namespace[len(namespace)-1] == '/' {
		return namespace + key
	}
	return namespace + "/" + key
}

func (s *Store) lockFilePath(namespace, key string) (string, error) {
	if namespace == "" {
		return "", fmt.Errorf("disk: namespace required")
	}
	if key == "" {
		return "", fmt.Errorf("disk: key required")
	}
	segments := strings.Split(key, "/")
	if len(segments) == 0 {
		return "", fmt.Errorf("disk: invalid lock key %q/%q", namespace, key)
	}
	encoded := encodePathSegments(segments)
	lockDir := filepath.Join(append([]string{s.namespaceLocksDir(namespace)}, encoded[:len(encoded)-1]...)...)
	if _, ok := s.lockDirs.Load(lockDir); !ok {
		if err := os.MkdirAll(lockDir, 0o755); err != nil {
			return "", fmt.Errorf("disk: prepare lock directory %q: %w", lockDir, err)
		}
		s.lockDirs.Store(lockDir, struct{}{})
	}
	filename := encoded[len(encoded)-1] + ".lock"
	return filepath.Join(lockDir, filename), nil
}

func encodePathSegments(segments []string) []string {
	encoded := make([]string, len(segments))
	for i, segment := range segments {
		encoded[i] = url.PathEscape(segment)
	}
	return encoded
}

// LoadMeta reads the per-key metadata protobuf and returns it with the stored ETag.
func (s *Store) LoadMeta(ctx context.Context, namespace, key string) (storage.LoadMetaResult, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.load_meta.begin", "key", key)

	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.load_meta.encode_error", "key", key, "error", err)
		return storage.LoadMetaResult{}, err
	}
	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.load_meta.namespace_error", "key", key, "error", err)
		return storage.LoadMetaResult{}, err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.load_meta.refresh_error", "key", key, "error", err)
		return storage.LoadMetaResult{}, err
	}
	ln.mu.Lock()
	ref := ln.metaIndex[clean]
	pending := ln.pendingMeta[clean]
	var cached *storage.Meta
	var cachedETag string
	if pending == nil && ref != nil && ref.cachedMeta != nil {
		metaCopy := *ref.cachedMeta
		if metaCopy.Lease != nil {
			leaseCopy := *metaCopy.Lease
			metaCopy.Lease = &leaseCopy
		}
		cached = &metaCopy
		cachedETag = ref.meta.etag
	}
	ln.mu.Unlock()
	if cached != nil {
		verbose.Debug("disk.load_meta.cache_hit", "key", key, "elapsed", time.Since(start))
		return storage.LoadMetaResult{Meta: cached, ETag: cachedETag}, nil
	}
	if pending != nil && !pendingGroupMatch(ctx, pending) {
		if err := waitForPendingCommit(ctx, ln, pending); err != nil {
			logger.Debug("disk.load_meta.pending_wait_error", "key", key, "error", err)
			return storage.LoadMetaResult{}, err
		}
		ln.mu.Lock()
		ref = ln.metaIndex[clean]
		pending = ln.pendingMeta[clean]
		cached = nil
		cachedETag = ""
		if pending == nil && ref != nil && ref.cachedMeta != nil {
			metaCopy := *ref.cachedMeta
			if metaCopy.Lease != nil {
				leaseCopy := *metaCopy.Lease
				metaCopy.Lease = &leaseCopy
			}
			cached = &metaCopy
			cachedETag = ref.meta.etag
		}
		ln.mu.Unlock()
		if cached != nil {
			verbose.Debug("disk.load_meta.cache_hit", "key", key, "elapsed", time.Since(start))
			return storage.LoadMetaResult{Meta: cached, ETag: cachedETag}, nil
		}
		if pending != nil && !pendingGroupMatch(ctx, pending) {
			logger.Debug("disk.load_meta.pending_mismatch", "key", key)
			return storage.LoadMetaResult{}, storage.ErrCASMismatch
		}
	}
	if ref == nil {
		verbose.Debug("disk.load_meta.not_found", "key", key, "elapsed", time.Since(start))
		return storage.LoadMetaResult{}, storage.ErrNotFound
	}
	payload, err := ln.readPayload(ref)
	if err != nil {
		logger.Debug("disk.load_meta.payload_error", "key", key, "error", err)
		return storage.LoadMetaResult{}, err
	}
	record, err := storage.UnmarshalMetaRecord(payload, s.crypto)
	if err != nil {
		logger.Debug("disk.load_meta.decode_error", "key", key, "error", err)
		if refreshErr := ln.refreshForce(); refreshErr != nil {
			logger.Debug("disk.load_meta.refresh_force_error", "key", key, "error", refreshErr)
			return storage.LoadMetaResult{}, err
		}
		ln.mu.Lock()
		ref = ln.metaIndex[clean]
		ln.mu.Unlock()
		if ref == nil {
			verbose.Debug("disk.load_meta.not_found", "key", key, "elapsed", time.Since(start))
			return storage.LoadMetaResult{}, storage.ErrNotFound
		}
		payload, payloadErr := ln.readPayload(ref)
		if payloadErr != nil {
			logger.Debug("disk.load_meta.payload_error", "key", key, "error", payloadErr)
			return storage.LoadMetaResult{}, payloadErr
		}
		record, err = storage.UnmarshalMetaRecord(payload, s.crypto)
		if err != nil {
			logger.Debug("disk.load_meta.decode_error", "key", key, "error", err)
			return storage.LoadMetaResult{}, err
		}
	}
	meta := *record.Meta
	if record.Meta.Lease != nil {
		leaseCopy := *record.Meta.Lease
		meta.Lease = &leaseCopy
	}
	cachedMeta := meta
	ln.mu.Lock()
	if ref != nil && ln.metaIndex[clean] == ref {
		ref.cachedMeta = &cachedMeta
	}
	ln.mu.Unlock()
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
		"meta_etag", record.ETag,
		"elapsed", time.Since(start),
	)
	return storage.LoadMetaResult{Meta: &meta, ETag: record.ETag}, nil
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

	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.store_meta.encode_error", "key", key, "error", err)
		return "", err
	}

	glob := globalKeyMutex(ns, clean)
	glob.Lock()
	defer glob.Unlock()

	mu := s.keyLock(ns, clean)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(ns, clean)
	if err != nil {
		logger.Debug("disk.store_meta.filelock_error", "key", key, "error", err)
		return "", err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.store_meta.namespace_error", "key", key, "error", err)
		return "", err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.store_meta.refresh_error", "key", key, "error", err)
		return "", err
	}

	ln.mu.Lock()
	current := ln.metaIndex[clean]
	pending := ln.pendingMeta[clean]
	ln.mu.Unlock()
	if pending == nil && expectedETag != "" && current == nil {
		if err := ln.refreshForce(); err != nil {
			logger.Debug("disk.store_meta.refresh_force_error", "key", key, "error", err)
			return "", err
		}
		ln.mu.Lock()
		current = ln.metaIndex[clean]
		pending = ln.pendingMeta[clean]
		ln.mu.Unlock()
	}
	if pending != nil && expectedETag != "" {
		if !pendingGroupMatch(ctx, pending) {
			if err := waitForPendingCommit(ctx, ln, pending); err != nil {
				logger.Debug("disk.store_meta.pending_wait_error", "key", key, "error", err)
				return "", err
			}
			ln.mu.Lock()
			pending = ln.pendingMeta[clean]
			ln.mu.Unlock()
			if pending != nil && !pendingGroupMatch(ctx, pending) {
				logger.Debug("disk.store_meta.cas_pending", "key", key)
				return "", storage.ErrCASMismatch
			}
		}
		if pending != nil && pending.ref.meta.etag != expectedETag {
			expectedETag = pending.ref.meta.etag
		}
		if isDeleteRecord(pending.ref.recType) || pending.ref.meta.etag != expectedETag {
			logger.Debug("disk.store_meta.cas_pending_mismatch", "key", key, "expected_etag", expectedETag)
			return "", storage.ErrCASMismatch
		}
		current = pending.ref
	}
	if expectedETag != "" {
		if current == nil {
			logger.Debug("disk.store_meta.cas_not_found", "key", key, "expected_etag", expectedETag)
			return "", storage.ErrNotFound
		}
		if current.meta.etag != expectedETag {
			logger.Debug("disk.store_meta.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", current.meta.etag)
			return "", storage.ErrCASMismatch
		}
	} else if current != nil {
		logger.Debug("disk.store_meta.cas_exists", "key", key, "current_etag", current.meta.etag)
		return "", storage.ErrCASMismatch
	}

	copyMeta := *meta
	if meta.Lease != nil {
		leaseCopy := *meta.Lease
		copyMeta.Lease = &leaseCopy
	}
	newETag := uuidv7.NewString()
	payload, err := storage.MarshalMetaRecord(newETag, &copyMeta, s.crypto)
	if err != nil {
		logger.Debug("disk.store_meta.encode_error", "key", key, "error", err)
		return "", err
	}
	gen := uint64(1)
	if current != nil {
		gen = current.meta.gen + 1
	}
	ref, err := ln.appendRecord(ctx, logRecordMetaPut, clean, recordMeta{
		gen:        gen,
		modifiedAt: s.now().Unix(),
		etag:       newETag,
	}, bytes.NewReader(payload))
	if err != nil {
		logger.Debug("disk.store_meta.append_error", "key", key, "error", err)
		return "", err
	}
	if ref != nil {
		cached := copyMeta
		ln.mu.Lock()
		ref.cachedMeta = &cached
		ln.mu.Unlock()
	}

	verbose.Debug("disk.store_meta.success",
		"key", key,
		"state_etag", meta.StateETag,
		"lease_owner", leaseOwner,
		"lease_expires_at", leaseExpires,
		"new_etag", newETag,
		"elapsed", time.Since(start),
	)
	return newETag, nil
}

// DeleteMeta removes the metadata document on disk, honouring an expected ETag when supplied.
func (s *Store) DeleteMeta(ctx context.Context, namespace, key string, expectedETag string) (err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.delete_meta.begin", "key", key, "expected_etag", expectedETag)

	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.delete_meta.encode_error", "key", key, "error", err)
		return err
	}
	glob := globalKeyMutex(ns, clean)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(ns, clean)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(ns, clean)
	if err != nil {
		logger.Debug("disk.delete_meta.filelock_error", "key", key, "error", err)
		return err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.delete_meta.namespace_error", "key", key, "error", err)
		return err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.delete_meta.refresh_error", "key", key, "error", err)
		return err
	}

	ln.mu.Lock()
	current := ln.metaIndex[clean]
	pending := ln.pendingMeta[clean]
	ln.mu.Unlock()
	if pending == nil && current == nil && expectedETag != "" {
		if err := ln.refreshForce(); err != nil {
			logger.Debug("disk.delete_meta.refresh_force_error", "key", key, "error", err)
			return err
		}
		ln.mu.Lock()
		current = ln.metaIndex[clean]
		pending = ln.pendingMeta[clean]
		ln.mu.Unlock()
	}
	if pending != nil && expectedETag != "" {
		if !pendingGroupMatch(ctx, pending) {
			if err := waitForPendingCommit(ctx, ln, pending); err != nil {
				logger.Debug("disk.delete_meta.pending_wait_error", "key", key, "error", err)
				return err
			}
			ln.mu.Lock()
			pending = ln.pendingMeta[clean]
			ln.mu.Unlock()
			if pending != nil && !pendingGroupMatch(ctx, pending) {
				logger.Debug("disk.delete_meta.cas_pending", "key", key)
				return storage.ErrCASMismatch
			}
		}
		if pending != nil && pending.ref.meta.etag != expectedETag {
			expectedETag = pending.ref.meta.etag
		}
		if isDeleteRecord(pending.ref.recType) || pending.ref.meta.etag != expectedETag {
			logger.Debug("disk.delete_meta.cas_pending_mismatch", "key", key, "expected_etag", expectedETag)
			return storage.ErrCASMismatch
		}
		current = pending.ref
	}
	if current == nil {
		logger.Debug("disk.delete_meta.not_found", "key", key)
		return storage.ErrNotFound
	}
	if expectedETag != "" && current.meta.etag != expectedETag {
		logger.Debug("disk.delete_meta.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", current.meta.etag)
		return storage.ErrCASMismatch
	}
	gen := current.meta.gen + 1
	if _, err := ln.appendRecord(ctx, logRecordMetaDelete, clean, recordMeta{
		gen:        gen,
		modifiedAt: s.now().Unix(),
	}, nil); err != nil {
		logger.Debug("disk.delete_meta.append_error", "key", key, "error", err)
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
	ln, err := s.logstore.namespace(namespace)
	if err != nil {
		logger.Debug("disk.list_meta_keys.namespace_error", "namespace", namespace, "error", err)
		return nil, err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.list_meta_keys.refresh_error", "namespace", namespace, "error", err)
		return nil, err
	}
	ln.mu.Lock()
	keys := make([]string, 0, len(ln.metaIndex))
	for key := range ln.metaIndex {
		keys = append(keys, key)
	}
	ln.mu.Unlock()
	sort.Strings(keys)
	verbose.Debug("disk.list_meta_keys.success", "namespace", namespace, "count", len(keys), "elapsed", time.Since(start))
	return keys, nil
}

// ListNamespaces enumerates namespace roots on disk.
func (s *Store) ListNamespaces(ctx context.Context) ([]string, error) {
	if s == nil {
		return nil, storage.ErrNotImplemented
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// ReadState opens the immutable JSON state file for key and returns its metadata.
func (s *Store) ReadState(ctx context.Context, namespace, key string) (storage.ReadStateResult, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.read_state.begin", "key", key)

	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.read_state.encode_error", "key", key, "error", err)
		return storage.ReadStateResult{}, err
	}
	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.read_state.namespace_error", "key", key, "error", err)
		return storage.ReadStateResult{}, err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.read_state.refresh_error", "key", key, "error", err)
		return storage.ReadStateResult{}, err
	}
	ln.mu.Lock()
	ref := ln.stateIndex[clean]
	pending := ln.pendingState[clean]
	ln.mu.Unlock()
	if pending != nil && !pendingGroupMatch(ctx, pending) {
		if err := waitForPendingCommit(ctx, ln, pending); err != nil {
			logger.Debug("disk.read_state.pending_wait_error", "key", key, "error", err)
			return storage.ReadStateResult{}, err
		}
		ln.mu.Lock()
		ref = ln.stateIndex[clean]
		pending = ln.pendingState[clean]
		ln.mu.Unlock()
		if pending != nil && !pendingGroupMatch(ctx, pending) {
			logger.Debug("disk.read_state.pending_mismatch", "key", key)
			return storage.ReadStateResult{}, storage.ErrCASMismatch
		}
	}
	if ref == nil {
		verbose.Debug("disk.read_state.not_found", "key", key, "elapsed", time.Since(start))
		return storage.ReadStateResult{}, storage.ErrNotFound
	}
	reader, err := ln.openPayloadReader(ref)
	if err != nil {
		logger.Debug("disk.read_state.open_error", "key", key, "error", err)
		return storage.ReadStateResult{}, err
	}
	encrypted := s.crypto != nil && s.crypto.Enabled()
	descriptor := ref.meta.descriptor
	if descFromCtx, ok := storage.StateDescriptorFromContext(ctx); ok && len(descFromCtx) > 0 {
		descriptor = descFromCtx
	}
	outReader := reader
	if encrypted {
		if len(descriptor) == 0 {
			reader.Close()
			logger.Debug("disk.read_state.missing_descriptor", "key", key)
			return storage.ReadStateResult{}, fmt.Errorf("disk: missing state descriptor for %q", key)
		}
		storageKey := joinNamespaceKey(ns, clean)
		objectCtx := storage.StateObjectContextFromContext(ctx, storage.StateObjectContext(storageKey))
		mat, err := s.crypto.MaterialFromDescriptor(objectCtx, descriptor)
		if err != nil {
			reader.Close()
			logger.Debug("disk.read_state.material_error", "key", key, "error", err)
			return storage.ReadStateResult{}, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(reader, mat)
		if err != nil {
			reader.Close()
			logger.Debug("disk.read_state.decrypt_error", "key", key, "error", err)
			return storage.ReadStateResult{}, err
		}
		outReader = decReader
	}
	stateInfo := &storage.StateInfo{
		Size:       ref.meta.plaintextSize,
		CipherSize: ref.meta.cipherSize,
		ETag:       ref.meta.etag,
		ModifiedAt: ref.meta.modifiedAt,
	}
	if plain, ok := storage.StatePlaintextSizeFromContext(ctx); ok && plain > 0 {
		stateInfo.Size = plain
	}
	if len(descriptor) > 0 {
		stateInfo.Descriptor = append([]byte(nil), descriptor...)
	}
	verbose.Debug("disk.read_state.success",
		"key", key,
		"etag", ref.meta.etag,
		"size", stateInfo.Size,
		"cipher_size", stateInfo.CipherSize,
		"elapsed", time.Since(start),
	)
	return storage.ReadStateResult{Reader: outReader, Info: stateInfo}, nil
}

// WriteState stages and atomically replaces the JSON state file, returning the new ETag.
func (s *Store) WriteState(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutStateOptions) (result *storage.PutStateResult, err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.write_state.begin", "key", key, "expected_etag", opts.ExpectedETag)
	expectedETag := opts.ExpectedETag

	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.write_state.encode_error", "key", key, "error", err)
		return nil, err
	}
	glob := globalKeyMutex(ns, clean)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(ns, clean)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(ns, clean)
	if err != nil {
		logger.Debug("disk.write_state.filelock_error", "key", key, "error", err)
		return nil, err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.write_state.namespace_error", "key", key, "error", err)
		return nil, err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.write_state.refresh_error", "key", key, "error", err)
		return nil, err
	}

	ln.mu.Lock()
	current := ln.stateIndex[clean]
	pending := ln.pendingState[clean]
	ln.mu.Unlock()
	if pending == nil && (expectedETag != "" || opts.IfNotExists) && current == nil {
		if err := ln.refreshForce(); err != nil {
			logger.Debug("disk.write_state.refresh_force_error", "key", key, "error", err)
			return nil, err
		}
		ln.mu.Lock()
		current = ln.stateIndex[clean]
		pending = ln.pendingState[clean]
		ln.mu.Unlock()
	}
	if pending != nil && (expectedETag != "" || opts.IfNotExists) {
		if !pendingGroupMatch(ctx, pending) {
			if err := waitForPendingCommit(ctx, ln, pending); err != nil {
				logger.Debug("disk.write_state.pending_wait_error", "key", key, "error", err)
				return nil, err
			}
			ln.mu.Lock()
			current = ln.stateIndex[clean]
			pending = ln.pendingState[clean]
			ln.mu.Unlock()
			if pending != nil && !pendingGroupMatch(ctx, pending) {
				logger.Debug("disk.write_state.cas_pending", "key", key)
				return nil, storage.ErrCASMismatch
			}
		}
		if expectedETag != "" {
			if pending != nil && pending.ref.meta.etag != expectedETag {
				expectedETag = pending.ref.meta.etag
			}
			if isDeleteRecord(pending.ref.recType) || pending.ref.meta.etag != expectedETag {
				logger.Debug("disk.write_state.cas_pending_mismatch", "key", key, "expected_etag", expectedETag)
				return nil, storage.ErrCASMismatch
			}
			current = pending.ref
		}
		if opts.IfNotExists && pending != nil {
			logger.Debug("disk.write_state.if_not_exists_pending", "key", key)
			return nil, storage.ErrCASMismatch
		}
	}
	if expectedETag != "" {
		if current == nil {
			logger.Debug("disk.write_state.cas_not_found", "key", key, "expected_etag", expectedETag)
			return nil, storage.ErrNotFound
		}
		if current.meta.etag != expectedETag {
			logger.Debug("disk.write_state.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", current.meta.etag)
			return nil, storage.ErrCASMismatch
		}
	}
	if opts.IfNotExists && current != nil {
		logger.Debug("disk.write_state.if_not_exists_conflict", "key", key)
		return nil, storage.ErrCASMismatch
	}

	gen := uint64(1)
	if current != nil {
		gen = current.meta.gen + 1
	}
	ref, err := ln.appendRecord(ctx, logRecordStatePut, clean, recordMeta{gen: gen, modifiedAt: s.now().Unix()}, body)
	if err != nil {
		logger.Debug("disk.write_state.append_error", "key", key, "error", err)
		return nil, err
	}

	result = &storage.PutStateResult{
		BytesWritten: ref.meta.plaintextSize,
		NewETag:      ref.meta.etag,
	}
	if len(ref.meta.descriptor) > 0 {
		result.Descriptor = append([]byte(nil), ref.meta.descriptor...)
	}
	verbose.Debug("disk.write_state.success",
		"key", key,
		"bytes", ref.meta.plaintextSize,
		"new_etag", ref.meta.etag,
		"elapsed", time.Since(start),
	)
	return result, nil
}

// Remove deletes the JSON state for key, respecting an expected ETag when supplied.
func (s *Store) Remove(ctx context.Context, namespace, key string, expectedETag string) (err error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.remove_state.begin", "key", key, "expected_etag", expectedETag)

	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		logger.Debug("disk.remove_state.encode_error", "key", key, "error", err)
		return err
	}
	glob := globalKeyMutex(ns, clean)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(ns, clean)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(ns, clean)
	if err != nil {
		logger.Debug("disk.remove_state.filelock_error", "key", key, "error", err)
		return err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.remove_state.namespace_error", "key", key, "error", err)
		return err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.remove_state.refresh_error", "key", key, "error", err)
		return err
	}

	ln.mu.Lock()
	current := ln.stateIndex[clean]
	pending := ln.pendingState[clean]
	ln.mu.Unlock()
	if pending == nil && current == nil && expectedETag != "" {
		if err := ln.refreshForce(); err != nil {
			logger.Debug("disk.remove_state.refresh_force_error", "key", key, "error", err)
			return err
		}
		ln.mu.Lock()
		current = ln.stateIndex[clean]
		pending = ln.pendingState[clean]
		ln.mu.Unlock()
	}
	if pending != nil && expectedETag != "" {
		if !pendingGroupMatch(ctx, pending) {
			if err := waitForPendingCommit(ctx, ln, pending); err != nil {
				logger.Debug("disk.remove_state.pending_wait_error", "key", key, "error", err)
				return err
			}
			ln.mu.Lock()
			pending = ln.pendingState[clean]
			ln.mu.Unlock()
			if pending != nil && !pendingGroupMatch(ctx, pending) {
				logger.Debug("disk.remove_state.cas_pending", "key", key)
				return storage.ErrCASMismatch
			}
		}
		if pending != nil && pending.ref.meta.etag != expectedETag {
			expectedETag = pending.ref.meta.etag
		}
		if isDeleteRecord(pending.ref.recType) || pending.ref.meta.etag != expectedETag {
			logger.Debug("disk.remove_state.cas_pending_mismatch", "key", key, "expected_etag", expectedETag)
			return storage.ErrCASMismatch
		}
		current = pending.ref
	}
	if current == nil {
		logger.Debug("disk.remove_state.not_found", "key", key)
		return storage.ErrNotFound
	}
	if expectedETag != "" && current.meta.etag != expectedETag {
		logger.Debug("disk.remove_state.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", current.meta.etag)
		return storage.ErrCASMismatch
	}
	gen := current.meta.gen + 1
	if _, err := ln.appendRecord(ctx, logRecordStateDelete, clean, recordMeta{
		gen:        gen,
		modifiedAt: s.now().Unix(),
	}, nil); err != nil {
		logger.Debug("disk.remove_state.append_error", "key", key, "error", err)
		return err
	}
	verbose.Debug("disk.remove_state.success", "key", key, "elapsed", time.Since(start))
	return nil
}

// ListObjects enumerates on-disk objects using lexical ordering of keys.
func (s *Store) ListObjects(ctx context.Context, namespace string, opts storage.ListOptions) (*storage.ListResult, error) {
	logger, verbose := s.loggers(ctx)
	start := time.Now()
	verbose.Trace("disk.list_objects.begin",
		"namespace", namespace,
		"prefix", opts.Prefix,
		"start_after", opts.StartAfter,
		"limit", opts.Limit,
	)
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("disk: namespace required")
	}
	ln, err := s.logstore.namespace(namespace)
	if err != nil {
		logger.Debug("disk.list_objects.namespace_error", "namespace", namespace, "error", err)
		return nil, err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.list_objects.refresh_error", "namespace", namespace, "error", err)
		return nil, err
	}
	prefix := strings.TrimPrefix(strings.TrimSpace(opts.Prefix), "/")
	startAfter := strings.TrimPrefix(strings.TrimSpace(opts.StartAfter), "/")
	result := &storage.ListResult{}
	count := 0

	ln.mu.Lock()
	defer ln.mu.Unlock()
	for _, key := range ln.sortedObjects {
		if startAfter != "" && key <= startAfter {
			continue
		}
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		ref := ln.objectIndex[key]
		if ref == nil {
			continue
		}
		info := storage.ObjectInfo{
			Key:          key,
			ETag:         ref.meta.etag,
			Size:         ref.meta.plaintextSize,
			LastModified: time.Unix(ref.meta.modifiedAt, 0).UTC(),
			ContentType:  ref.meta.contentType,
			Descriptor:   append([]byte(nil), ref.meta.descriptor...),
		}
		if plain, ok := storage.ObjectPlaintextSizeFromContext(ctx); ok && plain > 0 {
			info.Size = plain
		}
		result.Objects = append(result.Objects, info)
		count++
		if opts.Limit > 0 && count >= opts.Limit {
			result.Truncated = true
			result.NextStartAfter = key
			break
		}
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
	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		return storage.GetObjectResult{}, err
	}
	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.get_object.namespace_error", "namespace", namespace, "key", key, "error", err)
		return storage.GetObjectResult{}, err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.get_object.refresh_error", "namespace", namespace, "key", key, "error", err)
		return storage.GetObjectResult{}, err
	}
	ln.mu.Lock()
	ref := ln.objectIndex[clean]
	pending := ln.pendingObject[clean]
	ln.mu.Unlock()
	if pending != nil && !pendingGroupMatch(ctx, pending) {
		if err := waitForPendingCommit(ctx, ln, pending); err != nil {
			logger.Debug("disk.get_object.pending_wait_error", "namespace", namespace, "key", key, "error", err)
			return storage.GetObjectResult{}, err
		}
		ln.mu.Lock()
		ref = ln.objectIndex[clean]
		pending = ln.pendingObject[clean]
		ln.mu.Unlock()
		if pending != nil && !pendingGroupMatch(ctx, pending) {
			logger.Debug("disk.get_object.pending_mismatch", "namespace", namespace, "key", key)
			return storage.GetObjectResult{}, storage.ErrCASMismatch
		}
	}
	if ref == nil {
		verbose.Debug("disk.get_object.not_found", "key", key)
		return storage.GetObjectResult{}, storage.ErrNotFound
	}
	reader, err := ln.openPayloadReader(ref)
	if err != nil {
		logger.Debug("disk.get_object.open_error", "namespace", namespace, "key", key, "error", err)
		return storage.GetObjectResult{}, err
	}
	info := &storage.ObjectInfo{
		Key:          clean,
		ETag:         ref.meta.etag,
		Size:         ref.meta.plaintextSize,
		LastModified: time.Unix(ref.meta.modifiedAt, 0).UTC(),
		ContentType:  ref.meta.contentType,
		Descriptor:   append([]byte(nil), ref.meta.descriptor...),
	}
	if plain, ok := storage.ObjectPlaintextSizeFromContext(ctx); ok && plain > 0 {
		info.Size = plain
	}
	verbose.Debug("disk.get_object.success", "namespace", namespace, "key", key, "etag", info.ETag, "size", info.Size)
	return storage.GetObjectResult{Reader: reader, Info: info}, nil
}

// PutObject writes an object to disk with optional conditional semantics.
func (s *Store) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	logger, verbose := s.loggers(ctx)
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("disk: namespace required")
	}
	verbose.Trace("disk.put_object.begin", "namespace", namespace, "key", key, "expected_etag", opts.ExpectedETag, "if_not_exists", opts.IfNotExists)
	expectedETag := opts.ExpectedETag

	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		return nil, err
	}
	glob := globalKeyMutex(ns, clean)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(ns, clean)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(ns, clean)
	if err != nil {
		logger.Debug("disk.put_object.filelock_error", "key", key, "error", err)
		return nil, err
	}
	defer func() {
		if unlockErr := fl.Unlock(); unlockErr != nil && err == nil {
			err = unlockErr
		}
	}()

	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.put_object.namespace_error", "namespace", namespace, "key", key, "error", err)
		return nil, err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.put_object.refresh_error", "namespace", namespace, "key", key, "error", err)
		return nil, err
	}

	ln.mu.Lock()
	current := ln.objectIndex[clean]
	pending := ln.pendingObject[clean]
	ln.mu.Unlock()
	if pending == nil && (expectedETag != "" || opts.IfNotExists) && current == nil {
		if err := ln.refreshForce(); err != nil {
			logger.Debug("disk.put_object.refresh_force_error", "namespace", namespace, "key", key, "error", err)
			return nil, err
		}
		ln.mu.Lock()
		current = ln.objectIndex[clean]
		pending = ln.pendingObject[clean]
		ln.mu.Unlock()
	}
	if pending != nil && (expectedETag != "" || opts.IfNotExists) {
		if !pendingGroupMatch(ctx, pending) {
			if err := waitForPendingCommit(ctx, ln, pending); err != nil {
				verbose.Debug("disk.put_object.pending_wait_error", "namespace", namespace, "key", key, "error", err)
				return nil, err
			}
			ln.mu.Lock()
			current = ln.objectIndex[clean]
			pending = ln.pendingObject[clean]
			ln.mu.Unlock()
			if pending != nil && !pendingGroupMatch(ctx, pending) {
				verbose.Debug("disk.put_object.cas_pending", "namespace", namespace, "key", key)
				return nil, storage.ErrCASMismatch
			}
		}
		if expectedETag != "" {
			if pending != nil && pending.ref.meta.etag != expectedETag {
				expectedETag = pending.ref.meta.etag
			}
			if isDeleteRecord(pending.ref.recType) || pending.ref.meta.etag != expectedETag {
				verbose.Debug("disk.put_object.cas_pending_mismatch", "namespace", namespace, "key", key, "expected_etag", expectedETag)
				return nil, storage.ErrCASMismatch
			}
			current = pending.ref
		}
		if opts.IfNotExists {
			verbose.Debug("disk.put_object.exists_pending", "namespace", namespace, "key", key)
			return nil, storage.ErrCASMismatch
		}
	}
	if expectedETag != "" {
		if current == nil {
			verbose.Debug("disk.put_object.cas_missing", "namespace", namespace, "key", key, "expected_etag", expectedETag)
			return nil, storage.ErrNotFound
		}
		if current.meta.etag != expectedETag {
			verbose.Debug("disk.put_object.cas_mismatch", "namespace", namespace, "key", key, "expected_etag", expectedETag, "current_etag", current.meta.etag)
			return nil, storage.ErrCASMismatch
		}
	}
	if opts.IfNotExists && current != nil {
		verbose.Debug("disk.put_object.exists", "namespace", namespace, "key", key)
		return nil, storage.ErrCASMismatch
	}

	gen := uint64(1)
	if current != nil {
		gen = current.meta.gen + 1
	}
	meta := recordMeta{
		gen:         gen,
		modifiedAt:  s.now().Unix(),
		contentType: opts.ContentType,
		descriptor:  append([]byte(nil), opts.Descriptor...),
	}
	ref, err := ln.appendRecord(ctx, logRecordObjectPut, clean, meta, body)
	if err != nil {
		logger.Debug("disk.put_object.append_error", "namespace", namespace, "key", key, "error", err)
		return nil, err
	}
	info := &storage.ObjectInfo{
		Key:          clean,
		ETag:         ref.meta.etag,
		Size:         ref.meta.plaintextSize,
		LastModified: time.Unix(ref.meta.modifiedAt, 0).UTC(),
		ContentType:  ref.meta.contentType,
		Descriptor:   append([]byte(nil), ref.meta.descriptor...),
	}
	if plain, ok := storage.ObjectPlaintextSizeFromContext(ctx); ok && plain > 0 {
		info.Size = plain
	}
	verbose.Debug("disk.put_object.success", "namespace", namespace, "key", key, "size", info.Size, "etag", info.ETag)
	if queue, ok := queueNameFromObjectKey(clean); ok {
		s.touchQueueNotify(ns, queue)
	}
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
	expectedETag := opts.ExpectedETag

	ns, clean, err := s.normalizeKey(namespace, key)
	if err != nil {
		return err
	}
	glob := globalKeyMutex(ns, clean)
	glob.Lock()
	defer glob.Unlock()
	mu := s.keyLock(ns, clean)
	mu.Lock()
	defer mu.Unlock()

	fl, err := s.acquireFileLock(ns, clean)
	if err != nil {
		logger.Debug("disk.delete_object.filelock_error", "key", key, "error", err)
		return err
	}
	defer func() {
		_ = fl.Unlock()
	}()

	ln, err := s.logstore.namespace(ns)
	if err != nil {
		logger.Debug("disk.delete_object.namespace_error", "namespace", namespace, "key", key, "error", err)
		return err
	}
	if err := ln.refresh(); err != nil {
		logger.Debug("disk.delete_object.refresh_error", "namespace", namespace, "key", key, "error", err)
		return err
	}

	ln.mu.Lock()
	current := ln.objectIndex[clean]
	pending := ln.pendingObject[clean]
	ln.mu.Unlock()
	if pending == nil && current == nil && expectedETag != "" {
		if err := ln.refreshForce(); err != nil {
			logger.Debug("disk.delete_object.refresh_force_error", "namespace", namespace, "key", key, "error", err)
			return err
		}
		ln.mu.Lock()
		current = ln.objectIndex[clean]
		pending = ln.pendingObject[clean]
		ln.mu.Unlock()
	}
	if pending != nil && expectedETag != "" {
		if !pendingGroupMatch(ctx, pending) {
			if err := waitForPendingCommit(ctx, ln, pending); err != nil {
				verbose.Debug("disk.delete_object.pending_wait_error", "key", key, "error", err)
				return err
			}
			ln.mu.Lock()
			pending = ln.pendingObject[clean]
			ln.mu.Unlock()
			if pending != nil && !pendingGroupMatch(ctx, pending) {
				verbose.Debug("disk.delete_object.cas_pending", "key", key)
				return storage.ErrCASMismatch
			}
		}
		if pending != nil && pending.ref.meta.etag != expectedETag {
			expectedETag = pending.ref.meta.etag
		}
		if isDeleteRecord(pending.ref.recType) || pending.ref.meta.etag != expectedETag {
			verbose.Debug("disk.delete_object.cas_pending_mismatch", "key", key, "expected_etag", expectedETag)
			return storage.ErrCASMismatch
		}
		current = pending.ref
	}
	if current == nil {
		if opts.IgnoreNotFound {
			return nil
		}
		logger.Debug("disk.delete_object.not_found", "key", key)
		return storage.ErrNotFound
	}
	if expectedETag != "" && current.meta.etag != expectedETag {
		verbose.Debug("disk.delete_object.cas_mismatch", "key", key, "expected_etag", expectedETag, "current_etag", current.meta.etag)
		return storage.ErrCASMismatch
	}
	gen := current.meta.gen + 1
	if _, err := ln.appendRecord(ctx, logRecordObjectDelete, clean, recordMeta{
		gen:        gen,
		modifiedAt: s.now().Unix(),
	}, nil); err != nil {
		logger.Debug("disk.delete_object.append_error", "namespace", namespace, "key", key, "error", err)
		return err
	}
	verbose.Debug("disk.delete_object.success", "key", key)
	if queue, ok := queueNameFromObjectKey(clean); ok {
		s.touchQueueNotify(ns, queue)
	}
	return nil
}

// SweepOnceForTests exposes the retention sweeper for testing.
func (s *Store) SweepOnceForTests() {
	s.sweepOnce()
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
		ln, err := s.logstore.namespace(namespace)
		if err != nil {
			continue
		}
		if err := ln.refresh(); err != nil {
			continue
		}
		ln.mu.Lock()
		refs := make([]*recordRef, 0, len(ln.metaIndex))
		for _, ref := range ln.metaIndex {
			refs = append(refs, ref)
		}
		ln.mu.Unlock()
		for _, ref := range refs {
			payload, err := ln.readPayload(ref)
			if err != nil {
				continue
			}
			record, err := storage.UnmarshalMetaRecord(payload, s.crypto)
			if err != nil {
				continue
			}
			if record.Meta.UpdatedAtUnix == 0 {
				continue
			}
			age := now.Sub(time.Unix(record.Meta.UpdatedAtUnix, 0))
			if age <= s.retention {
				continue
			}
			key := ref.key
			_ = s.DeleteMeta(ctx, namespace, key, record.ETag)
			_ = s.Remove(ctx, namespace, key, "")
		}
	}
}

func isDeleteRecord(recType logRecordType) bool {
	switch recType {
	case logRecordMetaDelete, logRecordStateDelete, logRecordObjectDelete:
		return true
	default:
		return false
	}
}

func pendingGroupMatch(ctx context.Context, pending *pendingRef) bool {
	if pending == nil || pending.group == nil {
		return false
	}
	group, ok := storage.CommitGroupFromContext(ctx)
	if !ok || group == nil {
		return false
	}
	return group == pending.group
}

func waitForPendingCommit(ctx context.Context, ln *logNamespace, pending *pendingRef) error {
	if pending == nil || pending.group == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if pending.done != nil {
		select {
		case <-pending.done:
			if pending.err != nil {
				return pending.err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		if err := pending.group.Wait(); err != nil {
			return err
		}
	}
	if ln == nil {
		return nil
	}
	return ln.refreshForce()
}
