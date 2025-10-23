package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
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

// LoadMeta returns a copy of the metadata stored for key.
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
	if len(entry.data.StateDescriptor) > 0 {
		clone.StateDescriptor = append([]byte(nil), entry.data.StateDescriptor...)
	}
	return &clone, entry.etag, nil
}

// StoreMeta writes metadata for key, enforcing CAS when expectedETag is provided.
func (s *Store) StoreMeta(_ context.Context, key string, meta *storage.Meta, expectedETag string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.metas[key]
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
	s.metas[key] = &metaEntry{
		data: &clone,
		etag: etag,
	}
	return etag, nil
}

// DeleteMeta removes metadata for key, respecting the expected ETag when present.
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

// ListMetaKeys enumerates the in-memory metadata keys.
func (s *Store) ListMetaKeys(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.metas))
	for key := range s.metas {
		keys = append(keys, key)
	}
	return keys, nil
}

// ReadState returns a streaming reader for the stored state blob.
func (s *Store) ReadState(ctx context.Context, key string) (io.ReadCloser, *storage.StateInfo, error) {
	s.mu.RLock()
	entry, ok := s.state[key]
	if !ok {
		s.mu.RUnlock()
		return nil, nil, storage.ErrNotFound
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
			if metaEntry, ok := s.metas[key]; ok && metaEntry.data != nil && len(metaEntry.data.StateDescriptor) > 0 {
				descriptor = append([]byte(nil), metaEntry.data.StateDescriptor...)
			}
		}
		if len(descriptor) == 0 {
			return nil, nil, fmt.Errorf("memory: missing state descriptor for %q", key)
		}
		mat, err := s.crypto.MaterialFromDescriptor(storage.StateObjectContext(key), descriptor)
		if err != nil {
			return nil, nil, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(bytes.NewReader(payload), mat)
		if err != nil {
			return nil, nil, err
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
	return reader, info, nil
}

// WriteState replaces the state blob for key and returns the new ETag.
func (s *Store) WriteState(ctx context.Context, key string, body io.Reader, opts storage.PutStateOptions) (*storage.PutStateResult, error) {
	plaintext, err := io.ReadAll(body)
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
	var ciphertext []byte
	var descriptor []byte
	if s.crypto != nil && s.crypto.Enabled() {
		var descBytes []byte
		descFromCtx, ok := storage.StateDescriptorFromContext(ctx)
		var mat kryptograf.Material
		if ok && len(descFromCtx) > 0 {
			descriptor = append([]byte(nil), descFromCtx...)
			mat, err = s.crypto.MaterialFromDescriptor(storage.StateObjectContext(key), descriptor)
			if err != nil {
				return nil, err
			}
		} else {
			mat, descBytes, err = s.crypto.MintMaterial(storage.StateObjectContext(key))
			if err != nil {
				return nil, err
			}
			descriptor = append([]byte(nil), descBytes...)
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
	s.state[key] = &stateEntry{
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

// RemoveState deletes the state blob for key, applying CAS when expectedETag is set.
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

// ListObjects returns in-memory objects sorted lexicographically.
func (s *Store) ListObjects(_ context.Context, opts storage.ListOptions) (*storage.ListResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keysDirty {
		s.sortedKeys = s.sortedKeys[:0]
		for key := range s.objs {
			s.sortedKeys = append(s.sortedKeys, key)
		}
		sort.Strings(s.sortedKeys)
		s.keysDirty = false
	}
	keys := s.sortedKeys
	startIdx := 0
	if opts.StartAfter != "" {
		startIdx = sort.Search(len(keys), func(i int) bool { return keys[i] > opts.StartAfter })
	}
	limit := len(keys)
	if opts.Limit > 0 && opts.Limit < limit {
		limit = opts.Limit
	}
	result := &storage.ListResult{
		Objects: make([]storage.ObjectInfo, 0, limit),
	}
	seenPrefix := false
	added := 0
	for idx := startIdx; idx < len(keys); idx++ {
		key := keys[idx]
		if opts.Prefix != "" && !strings.HasPrefix(key, opts.Prefix) {
			if seenPrefix {
				break
			}
			continue
		}
		if opts.Prefix != "" {
			seenPrefix = true
		}
		entry := s.objs[key]
		result.Objects = append(result.Objects, storage.ObjectInfo{
			Key:          key,
			ETag:         entry.etag,
			Size:         int64(len(entry.payload)),
			LastModified: entry.updated,
			ContentType:  entry.contentType,
		})
		added++
		if opts.Limit > 0 && added >= opts.Limit {
			if idx+1 < len(keys) {
				result.Truncated = true
				result.NextStartAfter = key
			}
			return result, nil
		}
	}
	if opts.Limit > 0 && added >= opts.Limit {
		return result, nil
	}
	if opts.Limit <= 0 && opts.Prefix == "" && startIdx+added < len(keys) {
		if added > 0 {
			result.Truncated = true
			result.NextStartAfter = result.Objects[len(result.Objects)-1].Key
		}
	}
	return result, nil
}

// GetObject returns the payload for key if present.
func (s *Store) GetObject(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.objs[key]
	if !ok {
		return nil, nil, storage.ErrNotFound
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
	return io.NopCloser(bytes.NewReader(entry.payload)), info, nil
}

// PutObject stores or replaces the object for key depending on opts.
func (s *Store) PutObject(_ context.Context, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	payload, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	entry, exists := s.objs[key]
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
	s.objs[key] = &objectEntry{
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
			s.insertKeyLocked(key)
		}
	}
	queue, queueOK := queueNameFromObjectKey(key)
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
func (s *Store) DeleteObject(_ context.Context, key string, opts storage.DeleteObjectOptions) error {
	s.mu.Lock()
	entry, exists := s.objs[key]
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
	delete(s.objs, key)
	if s.keysDirty {
		// defer rebuild
	} else {
		s.removeKeyLocked(key)
	}
	queue, queueOK := queueNameFromObjectKey(key)
	s.mu.Unlock()

	if queueOK {
		s.notifyQueue(queue)
	}
	return nil
}

// SubscribeQueueChanges implements storage.QueueChangeFeed for the in-memory backend.
func (s *Store) SubscribeQueueChanges(queue string) (storage.QueueChangeSubscription, error) {
	if !s.queueWatchEnabled {
		return nil, storage.ErrNotImplemented
	}
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return nil, fmt.Errorf("memory: queue name required")
	}
	sub := &memoryQueueSubscription{
		store:  s,
		queue:  queue,
		events: make(chan struct{}, 1),
	}
	s.queueWatchMu.Lock()
	if s.queueWatchers == nil {
		s.queueWatchers = make(map[string]map[*memoryQueueSubscription]struct{})
	}
	watchers := s.queueWatchers[queue]
	if watchers == nil {
		watchers = make(map[*memoryQueueSubscription]struct{})
		s.queueWatchers[queue] = watchers
	}
	watchers[sub] = struct{}{}
	s.queueWatchMu.Unlock()
	return sub, nil
}

// QueueWatchStatus reports whether in-memory queue notifications are active.
func (s *Store) QueueWatchStatus() (bool, string, string) {
	if !s.queueWatchEnabled {
		return false, "disabled", "memory_queue_watch_disabled"
	}
	return true, "inprocess", "memory_queue_watch_enabled"
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

func queueNameFromObjectKey(key string) (string, bool) {
	if !strings.HasPrefix(key, "q/") {
		return "", false
	}
	parts := strings.Split(key, "/")
	if len(parts) < 3 {
		return "", false
	}
	queue := strings.TrimSpace(parts[1])
	if queue == "" {
		return "", false
	}
	return queue, true
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
