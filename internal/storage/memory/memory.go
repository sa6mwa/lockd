package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
)

// Config configures the in-memory store behaviour.
type Config struct {
	QueueWatch bool
	Crypto     *storage.Crypto
}

// Store implements storage.Backend in-memory; intended for tests and local dev.
type Store struct {
	mu    sync.RWMutex
	metas map[string]*metaEntry
	state map[string]*stateEntry
	objs  map[string]*objectEntry

	sortedKeys []string
	keysDirty  bool

	queueWatchEnabled bool
	queueWatchers     map[string]map[*memoryQueueSubscription]struct{}
	queueWatchMu      sync.Mutex

	crypto *storage.Crypto
}

// DefaultNamespaceConfig returns the default namespace settings for the in-memory backend.
func (s *Store) DefaultNamespaceConfig() namespaces.Config {
	cfg := namespaces.DefaultConfig()
	cfg.Query.Preferred = search.EngineScan
	cfg.Query.Fallback = namespaces.FallbackNone
	return cfg
}

func canonicalKey(namespace, key string) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return "", fmt.Errorf("memory: namespace required")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("memory: key required")
	}
	return namespace + "/" + key, nil
}

type metaEntry struct {
	data *storage.Meta
	etag string
}

type stateEntry struct {
	payload    []byte
	etag       string
	updated    time.Time
	descriptor []byte
	plaintext  int64
}

type objectEntry struct {
	payload     []byte
	etag        string
	contentType string
	updated     time.Time
	descriptor  []byte
}

// New returns a ready to use in-memory store with queue change notifications enabled.
func New() *Store {
	return NewWithConfig(Config{QueueWatch: true})
}

// NewWithConfig returns a ready to use in-memory store wired according to cfg.
func NewWithConfig(cfg Config) *Store {
	store := &Store{
		metas:     make(map[string]*metaEntry),
		state:     make(map[string]*stateEntry),
		objs:      make(map[string]*objectEntry),
		keysDirty: true,
		crypto:    cfg.Crypto,
	}
	if cfg.QueueWatch {
		store.queueWatchEnabled = true
		store.queueWatchers = make(map[string]map[*memoryQueueSubscription]struct{})
	}
	return store
}

// Close satisfies storage.Backend but requires no action for the in-memory store.
func (s *Store) Close() error {
	if !s.queueWatchEnabled {
		return nil
	}
	s.queueWatchMu.Lock()
	var subs []*memoryQueueSubscription
	for _, watchers := range s.queueWatchers {
		for sub := range watchers {
			subs = append(subs, sub)
		}
	}
	s.queueWatchers = make(map[string]map[*memoryQueueSubscription]struct{})
	s.queueWatchMu.Unlock()
	for _, sub := range subs {
		sub.close()
	}
	return nil
}

// BackendHash returns the stable identity hash for this backend.
func (s *Store) BackendHash(ctx context.Context) (string, error) {
	result, err := storage.ResolveBackendHash(ctx, s, "")
	return result.Hash, err
}

// LoadMeta returns a copy of the metadata stored for key.
func (s *Store) LoadMeta(_ context.Context, namespace, key string) (storage.LoadMetaResult, error) {
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return storage.LoadMetaResult{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.metas[storageKey]
	if !ok {
		return storage.LoadMetaResult{}, storage.ErrNotFound
	}
	clone := *entry.data
	if entry.data.Lease != nil {
		leaseCopy := *entry.data.Lease
		clone.Lease = &leaseCopy
	}
	if len(entry.data.StateDescriptor) > 0 {
		clone.StateDescriptor = append([]byte(nil), entry.data.StateDescriptor...)
	}
	return storage.LoadMetaResult{Meta: &clone, ETag: entry.etag}, nil
}

// StoreMeta writes metadata for key, enforcing CAS when expectedETag is provided.
func (s *Store) StoreMeta(_ context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.metas[storageKey]
	if expectedETag != "" {
		if !exists {
			return "", storage.ErrNotFound
		}
		if entry.etag != expectedETag {
			return "", storage.ErrCASMismatch
		}
	} else if exists {
		return "", storage.ErrCASMismatch
	}
	etag := uuidv7.NewString()
	clone := *meta
	if meta.Lease != nil {
		leaseCopy := *meta.Lease
		clone.Lease = &leaseCopy
	}
	if len(meta.StateDescriptor) > 0 {
		clone.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
	}
	s.metas[storageKey] = &metaEntry{
		data: &clone,
		etag: etag,
	}
	return etag, nil
}

// DeleteMeta removes metadata for key, respecting the expected ETag when present.
func (s *Store) DeleteMeta(_ context.Context, namespace, key string, expectedETag string) error {
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if expectedETag != "" {
		entry, ok := s.metas[storageKey]
		if !ok {
			return storage.ErrNotFound
		}
		if entry.etag != expectedETag {
			return storage.ErrCASMismatch
		}
	}
	delete(s.metas, storageKey)
	return nil
}

// ListMetaKeys enumerates the in-memory metadata keys.
func (s *Store) ListMetaKeys(_ context.Context, namespace string) ([]string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("memory: namespace required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := namespace + "/"
	keys := make([]string, 0, len(s.metas))
	for key := range s.metas {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, strings.TrimPrefix(key, prefix))
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// ReadState returns a streaming reader for the stored state blob.
func (s *Store) ReadState(ctx context.Context, namespace, key string) (storage.ReadStateResult, error) {
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return storage.ReadStateResult{}, err
	}
	s.mu.RLock()
	entry, ok := s.state[storageKey]
	if !ok {
		s.mu.RUnlock()
		return storage.ReadStateResult{}, storage.ErrNotFound
	}
	payload := append([]byte(nil), entry.payload...)
	descriptor := []byte{}
	if len(entry.descriptor) > 0 {
		descriptor = append([]byte(nil), entry.descriptor...)
	}
	plainSize := entry.plaintext
	if plainSize == 0 {
		plainSize = int64(len(payload))
	}
	etag := entry.etag
	updated := entry.updated
	s.mu.RUnlock()

	var reader io.ReadCloser
	if s.crypto != nil && s.crypto.Enabled() {
		if ctxDesc, ok := storage.StateDescriptorFromContext(ctx); ok && len(ctxDesc) > 0 {
			descriptor = append([]byte(nil), ctxDesc...)
		} else if len(descriptor) == 0 {
			if metaEntry, ok := s.metas[storageKey]; ok && metaEntry.data != nil && len(metaEntry.data.StateDescriptor) > 0 {
				descriptor = append([]byte(nil), metaEntry.data.StateDescriptor...)
			}
		}
		if len(descriptor) == 0 {
			return storage.ReadStateResult{}, fmt.Errorf("memory: missing state descriptor for %q", key)
		}
		mat, err := s.crypto.MaterialFromDescriptor(storage.StateObjectContext(storageKey), descriptor)
		if err != nil {
			return storage.ReadStateResult{}, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(bytes.NewReader(payload), mat)
		if err != nil {
			return storage.ReadStateResult{}, err
		}
		reader = decReader
	} else {
		reader = io.NopCloser(bytes.NewReader(payload))
	}
	info := &storage.StateInfo{
		Size:       plainSize,
		CipherSize: int64(len(payload)),
		ETag:       etag,
		ModifiedAt: updated.Unix(),
	}
	if plain, ok := storage.StatePlaintextSizeFromContext(ctx); ok && plain > 0 {
		info.Size = plain
	}
	if len(descriptor) > 0 {
		info.Descriptor = append([]byte(nil), descriptor...)
	}
	return storage.ReadStateResult{Reader: reader, Info: info}, nil
}

// WriteState replaces the state blob for key and returns the new ETag.
func (s *Store) WriteState(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return nil, err
	}
	plaintext, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if opts.ExpectedETag != "" {
		entry, ok := s.state[storageKey]
		if !ok {
			return nil, storage.ErrNotFound
		}
		if entry.etag != opts.ExpectedETag {
			return nil, storage.ErrCASMismatch
		}
	} else if opts.IfNotExists {
		if _, exists := s.state[storageKey]; exists {
			return nil, storage.ErrCASMismatch
		}
	}
	var ciphertext []byte
	var descriptor []byte
	if s.crypto != nil && s.crypto.Enabled() {
		descFromCtx, ok := storage.StateDescriptorFromContext(ctx)
		var mat kryptograf.Material
		if ok && len(descFromCtx) > 0 {
			descriptor = append([]byte(nil), descFromCtx...)
			mat, err = s.crypto.MaterialFromDescriptor(storage.StateObjectContext(storageKey), descriptor)
			if err != nil {
				return nil, err
			}
		} else {
			var minted storage.MaterialResult
			minted, err = s.crypto.MintMaterial(storage.StateObjectContext(storageKey))
			if err != nil {
				return nil, err
			}
			descriptor = append([]byte(nil), minted.Descriptor...)
			mat = minted.Material
		}
		var buf bytes.Buffer
		writer, err := s.crypto.EncryptWriterForMaterial(&buf, mat)
		if err != nil {
			return nil, err
		}
		if _, err := writer.Write(plaintext); err != nil {
			writer.Close()
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
		ciphertext = buf.Bytes()
	} else {
		ciphertext = append([]byte(nil), plaintext...)
	}
	etag := uuidv7.NewString()
	s.state[storageKey] = &stateEntry{
		payload:    ciphertext,
		etag:       etag,
		updated:    time.Now().UTC(),
		descriptor: append([]byte(nil), descriptor...),
		plaintext:  int64(len(plaintext)),
	}
	result := &storage.PutStateResult{
		BytesWritten: int64(len(plaintext)),
		NewETag:      etag,
	}
	if len(descriptor) > 0 {
		result.Descriptor = append([]byte(nil), descriptor...)
	}
	return result, nil
}

// Remove deletes the state blob for key, applying CAS when expectedETag is set.
func (s *Store) Remove(_ context.Context, namespace, key string, expectedETag string) error {
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if expectedETag != "" {
		entry, ok := s.state[storageKey]
		if !ok {
			return storage.ErrNotFound
		}
		if entry.etag != expectedETag {
			return storage.ErrCASMismatch
		}
	}
	delete(s.state, storageKey)
	return nil
}

// ListObjects returns in-memory objects sorted lexicographically.
func (s *Store) ListObjects(_ context.Context, namespace string, opts storage.ListOptions) (*storage.ListResult, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("memory: namespace required")
	}
	prefix := namespace + "/"
	canonicalPrefix := prefix + strings.TrimPrefix(opts.Prefix, "/")
	if opts.Prefix == "" {
		canonicalPrefix = prefix
	}
	s.mu.Lock()
	if s.keysDirty {
		s.sortedKeys = s.sortedKeys[:0]
		for key := range s.objs {
			s.sortedKeys = append(s.sortedKeys, key)
		}
		sort.Strings(s.sortedKeys)
		s.keysDirty = false
	}
	keys := append([]string(nil), s.sortedKeys...)
	s.mu.Unlock()

	result := &storage.ListResult{}
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	startAfter := ""
	if opts.StartAfter != "" {
		startAfter = prefix + opts.StartAfter
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if startAfter != "" && key <= startAfter {
			continue
		}
		if opts.Prefix != "" && !strings.HasPrefix(key, canonicalPrefix) {
			continue
		}
		entry, ok := s.objs[key]
		if !ok {
			continue
		}
		relative := strings.TrimPrefix(key, prefix)
		result.Objects = append(result.Objects, storage.ObjectInfo{
			Key:          relative,
			ETag:         entry.etag,
			Size:         int64(len(entry.payload)),
			LastModified: entry.updated,
			ContentType:  entry.contentType,
			Descriptor:   append([]byte(nil), entry.descriptor...),
		})
		count++
		if opts.Limit > 0 && count >= opts.Limit {
			result.Truncated = true
			result.NextStartAfter = relative
			break
		}
	}
	return result, nil
}

// GetObject returns the payload for key if present.
func (s *Store) GetObject(ctx context.Context, namespace, key string) (storage.GetObjectResult, error) {
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return storage.GetObjectResult{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.objs[storageKey]
	if !ok {
		return storage.GetObjectResult{}, storage.ErrNotFound
	}
	info := &storage.ObjectInfo{
		Key:          key,
		ETag:         entry.etag,
		Size:         int64(len(entry.payload)),
		LastModified: entry.updated,
		ContentType:  entry.contentType,
		Descriptor:   append([]byte(nil), entry.descriptor...),
	}
	if plain, ok := storage.ObjectPlaintextSizeFromContext(ctx); ok && plain > 0 {
		info.Size = plain
	}
	return storage.GetObjectResult{Reader: io.NopCloser(bytes.NewReader(entry.payload)), Info: info}, nil
}

// PutObject stores or replaces the object for key depending on opts.
func (s *Store) PutObject(_ context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return nil, err
	}
	payload, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	entry, exists := s.objs[storageKey]
	switch {
	case opts.IfNotExists && exists:
		s.mu.Unlock()
		return nil, storage.ErrCASMismatch
	case opts.ExpectedETag != "":
		if !exists {
			s.mu.Unlock()
			return nil, storage.ErrNotFound
		}
		if entry.etag != opts.ExpectedETag {
			s.mu.Unlock()
			return nil, storage.ErrCASMismatch
		}
	}
	etag := uuidv7.NewString()
	now := time.Now().UTC()
	s.objs[storageKey] = &objectEntry{
		payload:     payload,
		etag:        etag,
		contentType: opts.ContentType,
		updated:     now,
		descriptor:  append([]byte(nil), opts.Descriptor...),
	}
	if !exists {
		if s.keysDirty {
			// will rebuild on next read
		} else {
			s.insertKeyLocked(storageKey)
		}
	}
	queue, queueOK := queuePathFromObjectKey(storageKey)
	s.mu.Unlock()

	if queueOK {
		s.notifyQueue(queue)
	}
	return &storage.ObjectInfo{
		Key:          key,
		ETag:         etag,
		Size:         int64(len(payload)),
		LastModified: now,
		ContentType:  opts.ContentType,
		Descriptor:   append([]byte(nil), opts.Descriptor...),
	}, nil
}

// DeleteObject removes the object for key with optional CAS.
func (s *Store) DeleteObject(_ context.Context, namespace, key string, opts storage.DeleteObjectOptions) error {
	storageKey, err := canonicalKey(namespace, key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	entry, exists := s.objs[storageKey]
	if !exists {
		s.mu.Unlock()
		if opts.IgnoreNotFound {
			return nil
		}
		return storage.ErrNotFound
	}
	if opts.ExpectedETag != "" && entry.etag != opts.ExpectedETag {
		s.mu.Unlock()
		return storage.ErrCASMismatch
	}
	delete(s.objs, storageKey)
	if s.keysDirty {
		// defer rebuild
	} else {
		s.removeKeyLocked(storageKey)
	}
	queue, queueOK := queuePathFromObjectKey(storageKey)
	s.mu.Unlock()

	if queueOK {
		s.notifyQueue(queue)
	}
	return nil
}

// SubscribeQueueChanges implements storage.QueueChangeFeed for the in-memory backend.
func (s *Store) SubscribeQueueChanges(namespace, queue string) (storage.QueueChangeSubscription, error) {
	if !s.queueWatchEnabled {
		return nil, storage.ErrNotImplemented
	}
	storageQueue, err := canonicalKey(namespace, path.Join("q", queue))
	if err != nil {
		return nil, err
	}
	sub := &memoryQueueSubscription{
		store:  s,
		queue:  storageQueue,
		events: make(chan struct{}, 1),
	}
	s.queueWatchMu.Lock()
	if s.queueWatchers == nil {
		s.queueWatchers = make(map[string]map[*memoryQueueSubscription]struct{})
	}
	watchers := s.queueWatchers[storageQueue]
	if watchers == nil {
		watchers = make(map[*memoryQueueSubscription]struct{})
		s.queueWatchers[storageQueue] = watchers
	}
	watchers[sub] = struct{}{}
	s.queueWatchMu.Unlock()
	return sub, nil
}

// QueueWatchStatus reports whether in-memory queue notifications are active.
func (s *Store) QueueWatchStatus() storage.QueueWatchStatus {
	if !s.queueWatchEnabled {
		return storage.QueueWatchStatus{
			Enabled: false,
			Mode:    "disabled",
			Reason:  "memory_queue_watch_disabled",
		}
	}
	return storage.QueueWatchStatus{
		Enabled: true,
		Mode:    "inprocess",
		Reason:  "memory_queue_watch_enabled",
	}
}

func (s *Store) notifyQueue(queue string) {
	if !s.queueWatchEnabled {
		return
	}
	s.queueWatchMu.Lock()
	var subs []*memoryQueueSubscription
	for sub := range s.queueWatchers[queue] {
		subs = append(subs, sub)
	}
	s.queueWatchMu.Unlock()
	for _, sub := range subs {
		sub.signal()
	}
}

func (s *Store) removeSubscription(queue string, sub *memoryQueueSubscription) {
	s.queueWatchMu.Lock()
	if watchers, ok := s.queueWatchers[queue]; ok {
		delete(watchers, sub)
		if len(watchers) == 0 {
			delete(s.queueWatchers, queue)
		}
	}
	s.queueWatchMu.Unlock()
}

func (s *Store) insertKeyLocked(key string) {
	idx := sort.SearchStrings(s.sortedKeys, key)
	if idx < len(s.sortedKeys) && s.sortedKeys[idx] == key {
		return
	}
	s.sortedKeys = append(s.sortedKeys, "")
	copy(s.sortedKeys[idx+1:], s.sortedKeys[idx:])
	s.sortedKeys[idx] = key
}

func (s *Store) removeKeyLocked(key string) {
	idx := sort.SearchStrings(s.sortedKeys, key)
	if idx < len(s.sortedKeys) && s.sortedKeys[idx] == key {
		s.sortedKeys = append(s.sortedKeys[:idx], s.sortedKeys[idx+1:]...)
	}
}

func queuePathFromObjectKey(key string) (string, bool) {
	key = strings.TrimPrefix(strings.TrimSpace(key), "/")
	if key == "" {
		return "", false
	}
	parts := strings.Split(key, "/")
	if len(parts) < 4 {
		return "", false
	}
	if parts[1] != "q" {
		return "", false
	}
	namespace := strings.TrimSpace(parts[0])
	queue := strings.TrimSpace(parts[2])
	if namespace == "" || queue == "" {
		return "", false
	}
	return strings.Join(parts[:3], "/"), true
}

type memoryQueueSubscription struct {
	store  *Store
	queue  string
	events chan struct{}
	closed uint32
}

func (s *memoryQueueSubscription) Events() <-chan struct{} {
	return s.events
}

func (s *memoryQueueSubscription) Close() error {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return nil
	}
	s.store.removeSubscription(s.queue, s)
	close(s.events)
	return nil
}

func (s *memoryQueueSubscription) signal() {
	if atomic.LoadUint32(&s.closed) == 1 {
		return
	}
	select {
	case s.events <- struct{}{}:
	default:
	}
}

func (s *memoryQueueSubscription) close() {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return
	}
	close(s.events)
}
