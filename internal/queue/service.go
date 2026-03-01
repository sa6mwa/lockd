package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
)

var (
	// ErrInvalidQueue is returned when the requested queue name contains invalid characters.
	ErrInvalidQueue = errors.New("queue: invalid queue name")
)

// Config governs queue behaviour defaults.
type Config struct {
	DefaultVisibilityTimeout time.Duration
	DefaultMaxAttempts       int
	DefaultTTL               time.Duration
	QueuePollInterval        time.Duration
	Crypto                   *storage.Crypto
}

// Service implements queue primitives atop the storage backend.
type Service struct {
	store  storage.Backend
	clk    clock.Clock
	cfg    Config
	ready  sync.Map // map[string]*readyCache
	crypto *storage.Crypto

	payloadDeleteOnce sync.Once
	payloadDeleteCh   chan payloadDeleteRequest
}

// New constructs a queue Service.
func New(store storage.Backend, clk clock.Clock, cfg Config) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("queue: backend required")
	}
	if clk == nil {
		clk = clock.Real{}
	}
	if cfg.DefaultVisibilityTimeout <= 0 {
		cfg.DefaultVisibilityTimeout = 30 * time.Second
	}
	if cfg.DefaultMaxAttempts <= 0 {
		cfg.DefaultMaxAttempts = 5
	}
	if cfg.QueuePollInterval <= 0 {
		cfg.QueuePollInterval = 3 * time.Second
	}
	return &Service{
		store:  store,
		clk:    clk,
		cfg:    cfg,
		crypto: cfg.Crypto,
	}, nil
}

type payloadDeleteRequest struct {
	namespace string
	key       string
}

func (s *Service) startPayloadDeleteWorker() {
	s.payloadDeleteOnce.Do(func() {
		s.payloadDeleteCh = make(chan payloadDeleteRequest, 1024)
		go func(ch <-chan payloadDeleteRequest) {
			for req := range ch {
				_ = s.store.DeleteObject(context.Background(), req.namespace, req.key, storage.DeleteObjectOptions{IgnoreNotFound: true})
			}
		}(s.payloadDeleteCh)
	})
}

func (s *Service) enqueuePayloadDelete(namespace, key string) bool {
	s.startPayloadDeleteWorker()
	select {
	case s.payloadDeleteCh <- payloadDeleteRequest{namespace: namespace, key: key}:
		return true
	default:
		return false
	}
}

func (s *Service) readyCacheForQueue(namespace, queue string) *readyCache {
	key := readyCacheKey(namespace, queue)
	if value, ok := s.ready.Load(key); ok {
		return value.(*readyCache)
	}
	cache := newReadyCache(s, namespace, queue)
	actual, loaded := s.ready.LoadOrStore(key, cache)
	if loaded {
		return actual.(*readyCache)
	}
	return cache
}

func (s *Service) notifyCacheUpdate(namespace, queue string, desc MessageDescriptor) {
	key := readyCacheKey(namespace, queue)
	if value, ok := s.ready.Load(key); ok {
		cache := value.(*readyCache)
		if desc.Document.LeaseID == "" && desc.Document.LeaseFencingToken == 0 && desc.Document.LeaseTxnID == "" {
			cache.clearInflight(desc.Document.ID)
		}
		cache.upsert(desc)
	}
}

// UpdateReadyCache seeds the ready cache with a fresh descriptor.
func (s *Service) UpdateReadyCache(namespace, queue string, doc *MessageDocument, etag string) {
	if s == nil || doc == nil || etag == "" {
		return
	}
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return
	}
	if doc.Queue == "" {
		doc.Queue = name
	}
	if doc.ID == "" {
		return
	}
	desc := MessageDescriptor{
		Namespace:    ns,
		ID:           doc.ID,
		MetadataKey:  messageMetaPath(name, doc.ID),
		MetadataETag: etag,
		Document:     *doc,
	}
	cache := s.readyCacheForQueue(ns, name)
	if desc.Document.LeaseID == "" && desc.Document.LeaseFencingToken == 0 && desc.Document.LeaseTxnID == "" {
		cache.clearInflight(desc.Document.ID)
	}
	cache.upsert(desc)
}

func (s *Service) removeFromReadyCache(namespace, queue, id string) {
	key := readyCacheKey(namespace, queue)
	if value, ok := s.ready.Load(key); ok {
		value.(*readyCache).remove(id)
	}
}

func (s *Service) invalidateQueueCache(namespace, queue string) {
	key := readyCacheKey(namespace, queue)
	if value, ok := s.ready.Load(key); ok {
		value.(*readyCache).invalidate()
	}
}

// EnqueueOptions captures optional parameters for Enqueue.
type EnqueueOptions struct {
	Delay       time.Duration
	Visibility  time.Duration
	TTL         time.Duration
	MaxAttempts int
	Attributes  map[string]any
	ContentType string
}

// Message describes an enqueued message.
type Message struct {
	Namespace        string
	Queue            string
	ID               string
	EnqueuedAt       time.Time
	NotVisibleUntil  time.Time
	Visibility       time.Duration
	MaxAttempts      int
	Attempts         int
	FailureAttempts  int
	TTLDeadline      time.Time
	PayloadBytes     int64
	PayloadContent   string
	MetadataObject   string
	PayloadObject    string
	MetadataRevision string
	CorrelationID    string
}

// MessageDescriptor provides a lightweight summary from listing.
type MessageDescriptor struct {
	Namespace    string
	ID           string
	MetadataKey  string
	MetadataETag string
	Document     messageDocument
}

// MessageListResult captures message listing output.
type MessageListResult struct {
	Descriptors    []MessageDescriptor
	NextStartAfter string
	Truncated      bool
}

// MessageCandidateResult captures the next ready candidate and cursor.
type MessageCandidateResult struct {
	Descriptor *MessageDescriptor
	NextCursor string
}

// MessageResult captures a message document and its ETag.
type MessageResult struct {
	Document *MessageDocument
	ETag     string
}

// PayloadResult captures a payload reader with metadata.
type PayloadResult struct {
	Reader io.ReadCloser
	Info   *storage.ObjectInfo
}

// StateResult captures a queue state document and its ETag.
type StateResult struct {
	Document *StateDocument
	ETag     string
}

// StateDocument captures metadata about queue state blobs.
type StateDocument struct {
	Type          string    `json:"type"`
	Namespace     string    `json:"namespace"`
	Queue         string    `json:"queue"`
	MessageID     string    `json:"message_id"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	CorrelationID string    `json:"correlation_id,omitempty"`
}

// MessageDocument captures per-message metadata stored alongside payloads.
type MessageDocument struct {
	Type               string         `json:"type"`
	Namespace          string         `json:"namespace,omitempty"`
	Queue              string         `json:"queue"`
	ID                 string         `json:"id"`
	EnqueuedAt         time.Time      `json:"enqueued_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
	Attempts           int            `json:"attempts"`
	FailureAttempts    int            `json:"failure_attempts,omitempty"`
	NotVisibleUntil    time.Time      `json:"not_visible_until"`
	MaxAttempts        int            `json:"max_attempts"`
	Attributes         map[string]any `json:"attributes,omitempty"`
	LastError          any            `json:"last_error,omitempty"`
	PayloadBytes       int64          `json:"payload_bytes,omitempty"`
	PayloadContentType string         `json:"payload_content_type,omitempty"`
	VisibilityTimeout  int64          `json:"visibility_timeout_seconds,omitempty"`
	ExpiresAt          *time.Time     `json:"expires_at,omitempty"`
	CorrelationID      string         `json:"correlation_id,omitempty"`
	LeaseID            string         `json:"lease_id,omitempty"`
	LeaseFencingToken  int64          `json:"lease_fencing_token,omitempty"`
	LeaseTxnID         string         `json:"lease_txn_id,omitempty"`
	PayloadDescriptor  []byte         `json:"payload_descriptor,omitempty"`
	MetaDescriptor     []byte         `json:"meta_descriptor,omitempty"`
}

type messageDocument = MessageDocument

type stateDocument = StateDocument

// Enqueue inserts a new message into queue and returns its metadata.
func (s *Service) Enqueue(ctx context.Context, namespace, queue string, payload io.Reader, opts EnqueueOptions) (*Message, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return nil, err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return nil, err
	}
	id := uuidv7.NewString()

	metaKey := messageMetaPath(name, id)
	payloadKey := messagePayloadPath(name, id)

	if payload == nil {
		payload = bytes.NewReader(nil)
	}
	payloadContentType := contentTypeOrDefault(opts.ContentType)
	reader, payloadDescriptor, counting, err := s.preparePayloadReader(ctx, ns, payloadKey, payload)
	if err != nil {
		return nil, err
	}
	if closer, ok := reader.(io.ReadCloser); ok && closer != nil {
		defer closer.Close()
	}
	payloadOpts := storage.PutObjectOptions{
		IfNotExists: true,
		ContentType: payloadContentType,
		Descriptor:  payloadDescriptor,
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payloadOpts.ContentType = storage.ContentTypeOctetStreamEncrypted
	}
	payloadInfo, err := s.store.PutObject(ctx, ns, payloadKey, reader, payloadOpts)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return nil, fmt.Errorf("queue: payload already exists: %w", err)
		}
		return nil, err
	}
	payloadBytes := counting.Count()
	if payloadInfo != nil {
		if payloadInfo.ContentType != "" && opts.ContentType == "" {
			opts.ContentType = payloadInfo.ContentType
		}
	}

	now := s.clk.Now().UTC()
	delay := max(opts.Delay, 0)
	visibility := opts.Visibility
	if visibility <= 0 {
		visibility = s.cfg.DefaultVisibilityTimeout
	}
	notVisibleUntil := now.Add(delay)

	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = s.cfg.DefaultMaxAttempts
	}

	var ttlDeadline *time.Time
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = s.cfg.DefaultTTL
	}
	if ttl > 0 {
		deadline := now.Add(ttl)
		ttlDeadline = &deadline
	}

	corr := correlation.ID(ctx)
	if corr == "" {
		corr = correlation.Generate()
	}

	doc := messageDocument{
		Type:               "queue_msg",
		Namespace:          ns,
		Queue:              name,
		ID:                 id,
		EnqueuedAt:         now,
		UpdatedAt:          now,
		Attempts:           0,
		NotVisibleUntil:    notVisibleUntil,
		MaxAttempts:        maxAttempts,
		Attributes:         cloneAttributes(opts.Attributes),
		PayloadBytes:       payloadBytes,
		PayloadContentType: payloadContentType,
		VisibilityTimeout:  int64(visibility / time.Second),
		ExpiresAt:          ttlDeadline,
		CorrelationID:      corr,
	}
	if len(payloadDescriptor) > 0 {
		doc.PayloadDescriptor = append([]byte(nil), payloadDescriptor...)
	}

	metaPayload, metaDescriptor, err := s.encodeMessageMeta(ns, metaKey, &doc)
	if err != nil {
		return nil, err
	}
	metaOpts := storage.PutObjectOptions{
		IfNotExists: true,
		ContentType: storage.ContentTypeProtobuf,
		Descriptor:  metaDescriptor,
	}
	if s.crypto != nil && s.crypto.Enabled() {
		metaOpts.ContentType = storage.ContentTypeProtobufEncrypted
	}
	metaInfo, err := s.store.PutObject(ctx, ns, metaKey, bytes.NewReader(metaPayload), metaOpts)
	if err != nil {
		_ = s.store.DeleteObject(ctx, ns, payloadKey, storage.DeleteObjectOptions{})
		return nil, err
	}
	if metaInfo != nil && metaInfo.ETag != "" {
		desc := MessageDescriptor{
			Namespace:    ns,
			ID:           doc.ID,
			MetadataKey:  metaKey,
			MetadataETag: metaInfo.ETag,
			Document:     doc,
		}
		s.notifyCacheUpdate(ns, name, desc)
	} else {
		s.invalidateQueueCache(ns, name)
	}

	result := &Message{
		Namespace:       ns,
		Queue:           name,
		ID:              id,
		EnqueuedAt:      now,
		NotVisibleUntil: notVisibleUntil,
		Visibility:      visibility,
		MaxAttempts:     maxAttempts,
		Attempts:        0,
		FailureAttempts: 0,
		PayloadBytes:    payloadBytes,
		PayloadContent:  opts.ContentType,
		MetadataObject:  metaKey,
		PayloadObject:   payloadKey,
		CorrelationID:   corr,
	}
	if ttlDeadline != nil {
		result.TTLDeadline = *ttlDeadline
	}
	return result, nil
}

type countingReader struct {
	r     io.Reader
	count int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.count += int64(n)
	return n, err
}

func (c *countingReader) Count() int64 {
	return c.count
}

func (s *Service) preparePayloadReader(ctx context.Context, namespace, payloadPath string, payload io.Reader) (io.Reader, []byte, *countingReader, error) {
	counting := &countingReader{r: payload}
	if s.crypto == nil || !s.crypto.Enabled() {
		return counting, nil, counting, nil
	}
	minted, err := s.crypto.MintMaterial(storage.QueuePayloadContext(namespace, payloadPath))
	if err != nil {
		return nil, nil, nil, err
	}
	mat := minted.Material
	descBytes := minted.Descriptor
	pr, pw := io.Pipe()
	encWriter, err := s.crypto.EncryptWriterForMaterial(pw, mat)
	if err != nil {
		pw.Close()
		return nil, nil, nil, err
	}
	startEncryptedPipe(ctx, counting, encWriter, pw)
	return pr, append([]byte(nil), descBytes...), counting, nil
}

func (s *Service) encodeMessageMeta(namespace, metaPath string, doc *messageDocument) ([]byte, []byte, error) {
	payload, err := marshalMessageDocument(doc)
	if err != nil {
		return nil, nil, err
	}
	if s.crypto == nil || !s.crypto.Enabled() {
		doc.MetaDescriptor = nil
		return payload, nil, nil
	}
	descriptor := append([]byte(nil), doc.MetaDescriptor...)
	var mat kryptograf.Material
	if len(descriptor) > 0 {
		mat, err = s.crypto.MaterialFromDescriptor(storage.QueueMetaContext(namespace, metaPath), descriptor)
		if err != nil {
			descriptor = nil
		}
	}
	if len(descriptor) == 0 {
		var minted storage.MaterialResult
		minted, err = s.crypto.MintMaterial(storage.QueueMetaContext(namespace, metaPath))
		if err != nil {
			return nil, nil, err
		}
		descriptor = append([]byte(nil), minted.Descriptor...)
		mat = minted.Material
	}
	var buf bytes.Buffer
	writer, err := s.crypto.EncryptWriterForMaterial(&buf, mat)
	if err != nil {
		return nil, nil, err
	}
	if _, err := writer.Write(payload); err != nil {
		writer.Close()
		return nil, nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, nil, err
	}
	doc.MetaDescriptor = descriptor
	return buf.Bytes(), descriptor, nil
}

func sanitizeQueueName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", ErrInvalidQueue
	}
	normalized, err := namespaces.NormalizeComponent(name)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidQueue, err)
	}
	return normalized, nil
}

func sanitizeNamespace(namespace string) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = namespaces.Default
	}
	normalized, err := namespaces.Normalize(namespace, namespace)
	if err != nil {
		return "", err
	}
	return normalized, nil
}

func readyCacheKey(namespace, queue string) string {
	return namespace + "/" + queue
}

func queueBasePath(queue string) string {
	return path.Join("q", queue)
}

func messageMetaPath(queue, id string) string {
	return path.Join(queueBasePath(queue), "msg", id+metaFileExtension)
}

func messagePayloadPath(queue, id string) string {
	return path.Join(queueBasePath(queue), "msg", id+".bin")
}

// EnsureMessageReady verifies that the message payload and its metadata exist before delivery.
func (s *Service) EnsureMessageReady(ctx context.Context, namespace, queue, id string) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return err
	}
	key := messagePayloadPath(name, id)
	obj, err := s.store.GetObject(ctx, ns, key)
	if err != nil {
		return err
	}
	if obj.Reader != nil {
		_ = obj.Reader.Close()
	}
	return nil
}

func messageStatePath(queue, id string) string {
	return path.Join(queueBasePath(queue), "state", id+".json")
}

func messageDLQMetaPath(queue, id string) string {
	return path.Join(queueBasePath(queue), "dlq", "msg", id+metaFileExtension)
}

func messageDLQStatePath(queue, id string) string {
	return path.Join(queueBasePath(queue), "dlq", "state", id+".json")
}

func messageDLQPayloadPath(queue, id string) string {
	return path.Join(queueBasePath(queue), "dlq", "msg", id+".bin")
}

func fullObjectKey(namespace, relPath string) string {
	rel := strings.TrimPrefix(relPath, "/")
	if namespace == "" {
		return rel
	}
	return namespace + "/" + rel
}

// MessageLeaseKey returns the logical lock key used for message leasing.
func MessageLeaseKey(namespace, queue, id string) (string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return "", err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return "", err
	}
	return path.Join(ns, queueBasePath(name), "msg", id), nil
}

// StateLeaseKey returns the logical lock key used for workflow state leasing.
func StateLeaseKey(namespace, queue, id string) (string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return "", err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return "", err
	}
	return path.Join(ns, queueBasePath(name), "state", id), nil
}

// IsQueueStateKey reports whether the logical key belongs to a queue state lease.
func IsQueueStateKey(key string) bool {
	clean := path.Clean(key)
	if clean == "" || clean == "." {
		return false
	}
	segments := strings.Split(clean, "/")
	if len(segments) < 4 {
		return false
	}
	idx := len(segments) - 4
	if segments[idx] != "q" {
		return false
	}
	return segments[idx+2] == "state"
}

func cloneAttributes(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	maps.Copy(dst, src)
	return dst
}

func contentTypeOrDefault(contentType string) string {
	if strings.TrimSpace(contentType) == "" {
		return storage.ContentTypeOctetStream
	}
	return contentType
}

// ListMessages enumerates message metadata objects in lexical order.
func (s *Service) ListMessages(ctx context.Context, namespace, queue string, startAfter string, limit int) (MessageListResult, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return MessageListResult{}, err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return MessageListResult{}, err
	}
	prefix := path.Join(queueBasePath(name), "msg") + "/"
	opts := storage.ListOptions{
		Prefix: prefix,
		Limit:  limit,
	}
	if startAfter != "" {
		opts.StartAfter = startAfter
	}
	result, err := s.store.ListObjects(ctx, ns, opts)
	if err != nil {
		return MessageListResult{}, err
	}
	descriptors := make([]MessageDescriptor, 0, len(result.Objects))
	for _, obj := range result.Objects {
		if !strings.HasSuffix(obj.Key, metaFileExtension) {
			continue
		}
		id := strings.TrimSuffix(path.Base(obj.Key), metaFileExtension)
		docPtr, etag, err := s.loadMessageDocument(ctx, ns, name, obj.Key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return MessageListResult{}, err
		}
		if docPtr.Queue != name || docPtr.ID != id {
			continue
		}
		descriptor := MessageDescriptor{
			Namespace:    ns,
			ID:           docPtr.ID,
			MetadataKey:  obj.Key,
			MetadataETag: etag,
			Document:     *docPtr,
		}
		descriptors = append(descriptors, descriptor)
	}
	next := result.NextStartAfter
	return MessageListResult{Descriptors: descriptors, NextStartAfter: next, Truncated: result.Truncated}, nil
}

// NextCandidate returns the next visible message ready for processing.
func (s *Service) NextCandidate(ctx context.Context, namespace, queue string, startAfter string, pageSize int) (MessageCandidateResult, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return MessageCandidateResult{}, err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return MessageCandidateResult{}, err
	}
	cache := s.readyCacheForQueue(ns, name)
	if startAfter != "" {
		cache.resetWithCursor(startAfter)
	}
	now := s.clk.Now().UTC()
	desc, err := cache.next(ctx, pageSize, now)
	if err != nil {
		return MessageCandidateResult{}, err
	}
	if desc == nil {
		return MessageCandidateResult{}, storage.ErrNotFound
	}
	desc.Namespace = ns
	return MessageCandidateResult{Descriptor: desc, NextCursor: ""}, nil
}

// PeekCandidate returns the next visible message without consuming it.
func (s *Service) PeekCandidate(ctx context.Context, namespace, queue string, startAfter string, pageSize int) (MessageCandidateResult, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return MessageCandidateResult{}, err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return MessageCandidateResult{}, err
	}
	cache := s.readyCacheForQueue(ns, name)
	if startAfter != "" {
		cache.resetWithCursor(startAfter)
	}
	now := s.clk.Now().UTC()
	desc, err := cache.peek(ctx, pageSize, now)
	if err != nil {
		return MessageCandidateResult{}, err
	}
	if desc == nil {
		return MessageCandidateResult{}, storage.ErrNotFound
	}
	desc.Namespace = ns
	return MessageCandidateResult{Descriptor: desc, NextCursor: ""}, nil
}

// RefreshReadyCache syncs the ready cache for the provided queue immediately.
func (s *Service) RefreshReadyCache(ctx context.Context, namespace, queue string) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return err
	}
	cache := s.readyCacheForQueue(ns, name)
	_, err = cache.refresh(ctx, defaultCachePageSize, s.clk.Now().UTC(), false)
	return err
}

func (s *Service) listMessageObjects(ctx context.Context, namespace, queue string, startAfter string, limit int) ([]storage.ObjectInfo, string, bool, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return nil, "", false, err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return nil, "", false, err
	}
	prefix := path.Join(queueBasePath(name), "msg") + "/"
	opts := storage.ListOptions{
		Prefix: prefix,
		Limit:  limit,
	}
	if startAfter != "" {
		opts.StartAfter = startAfter
	}
	result, err := s.store.ListObjects(ctx, ns, opts)
	if err != nil {
		return nil, "", false, err
	}
	return result.Objects, result.NextStartAfter, result.Truncated, nil
}

func (s *Service) loadDescriptor(ctx context.Context, namespace, queue, metadataKey string) (*MessageDescriptor, error) {
	doc, etag, err := s.loadMessageDocument(ctx, namespace, queue, metadataKey)
	if err != nil {
		return nil, err
	}
	return &MessageDescriptor{
		Namespace:    namespace,
		ID:           doc.ID,
		MetadataKey:  metadataKey,
		MetadataETag: etag,
		Document:     *doc,
	}, nil
}

// loadMessageDocument fetches and decodes the message metadata.
func (s *Service) loadMessageDocument(ctx context.Context, namespace, queue, metadataKey string) (*messageDocument, string, error) {
	obj, err := s.store.GetObject(ctx, namespace, metadataKey)
	if err != nil {
		return nil, "", err
	}
	reader := obj.Reader
	info := obj.Info
	displayKey := fullObjectKey(namespace, metadataKey)
	var (
		data       []byte
		descriptor []byte
	)
	encrypted := s.crypto != nil && s.crypto.Enabled()
	if info != nil && len(info.Descriptor) > 0 {
		descriptor = append([]byte(nil), info.Descriptor...)
	}
	if encrypted {
		if len(descriptor) == 0 {
			reader.Close()
			return nil, "", fmt.Errorf("queue: missing metadata descriptor for %s", displayKey)
		}
		mat, err := s.crypto.MaterialFromDescriptor(storage.QueueMetaContext(namespace, metadataKey), descriptor)
		if err != nil {
			reader.Close()
			return nil, "", err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(reader, mat)
		if err != nil {
			reader.Close()
			return nil, "", err
		}
		defer decReader.Close()
		data, err = io.ReadAll(decReader)
		if err != nil {
			return nil, "", fmt.Errorf("queue: read message %q: %w", displayKey, err)
		}
	} else {
		defer reader.Close()
		var err error
		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, "", fmt.Errorf("queue: read message %q: %w", displayKey, err)
		}
	}
	doc, err := unmarshalMessageDocument(data)
	if err != nil {
		return nil, "", err
	}
	etag := ""
	if info != nil {
		etag = info.ETag
	}
	if len(descriptor) > 0 {
		doc.MetaDescriptor = descriptor
	}
	if doc.Namespace == "" {
		doc.Namespace = namespace
	}
	if doc.Queue == "" {
		doc.Queue = queue
	}
	return doc, etag, nil
}

// GetMessage loads the metadata document for the provided queue/id pair.
func (s *Service) GetMessage(ctx context.Context, namespace, queue, id string) (MessageResult, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return MessageResult{}, err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return MessageResult{}, err
	}
	doc, etag, err := s.loadMessageDocument(ctx, ns, name, messageMetaPath(name, id))
	if err != nil {
		return MessageResult{}, err
	}
	return MessageResult{Document: doc, ETag: etag}, nil
}

// SaveMessageDocument writes metadata via CAS.
func (s *Service) SaveMessageDocument(ctx context.Context, namespace, queue, id string, doc *messageDocument, expectedETag string) (string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return "", err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return "", err
	}
	if doc.Namespace == "" {
		doc.Namespace = ns
	}
	if doc.Namespace != ns || doc.Queue != name || doc.ID != id {
		return "", fmt.Errorf("queue: document mismatch for %s/%s", name, id)
	}
	metaKey := messageMetaPath(name, id)
	metaPayload, metaDescriptor, err := s.encodeMessageMeta(ns, metaKey, doc)
	if err != nil {
		return "", err
	}
	opts := storage.PutObjectOptions{
		ExpectedETag: expectedETag,
		ContentType:  storage.ContentTypeProtobuf,
		Descriptor:   metaDescriptor,
	}
	if s.crypto != nil && s.crypto.Enabled() {
		opts.ContentType = storage.ContentTypeProtobufEncrypted
	}
	info, err := s.store.PutObject(ctx, ns, metaKey, bytes.NewReader(metaPayload), opts)
	if err != nil {
		return "", err
	}
	newETag := expectedETag
	if info != nil && info.ETag != "" {
		newETag = info.ETag
	}
	if newETag != "" {
		desc := MessageDescriptor{
			Namespace:    ns,
			ID:           doc.ID,
			MetadataKey:  metaKey,
			MetadataETag: newETag,
			Document:     *doc,
		}
		s.notifyCacheUpdate(ns, name, desc)
	} else {
		s.invalidateQueueCache(ns, name)
	}
	return newETag, nil
}

// DeleteMessage removes metadata and payload (best-effort) respecting CAS.
func (s *Service) DeleteMessage(ctx context.Context, namespace, queue, id string, expectedETag string) error {
	return s.deleteMessageInternal(ctx, namespace, queue, id, expectedETag, true)
}

func (s *Service) deleteMessageInternal(ctx context.Context, namespace, queue, id string, expectedETag string, notify bool) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return err
	}
	metaKey := messageMetaPath(name, id)
	if err := s.store.DeleteObject(ctx, ns, metaKey, storage.DeleteObjectOptions{ExpectedETag: expectedETag}); err != nil {
		return err
	}
	payloadKey := messagePayloadPath(name, id)
	if !s.enqueuePayloadDelete(ns, payloadKey) {
		_ = s.store.DeleteObject(ctx, ns, payloadKey, storage.DeleteObjectOptions{IgnoreNotFound: true})
	}
	if notify {
		s.removeFromReadyCache(ns, name, id)
	}
	return nil
}

// IncrementAttempts updates attempts and visibility after a successful lease acquire.
func (s *Service) IncrementAttempts(ctx context.Context, namespace, queue string, doc *messageDocument, expectedETag string, visibility time.Duration) (string, error) {
	if doc == nil {
		return "", fmt.Errorf("queue: nil message document")
	}
	if visibility <= 0 {
		visibility = time.Duration(doc.VisibilityTimeout) * time.Second
		if visibility <= 0 {
			visibility = s.cfg.DefaultVisibilityTimeout
		}
	}
	now := s.clk.Now().UTC()
	doc.Attempts++
	doc.NotVisibleUntil = now.Add(visibility)
	doc.VisibilityTimeout = int64(visibility / time.Second)
	doc.UpdatedAt = now
	doc.LastError = nil
	if doc.Namespace == "" {
		doc.Namespace = namespace
	}
	return s.SaveMessageDocument(ctx, namespace, queue, doc.ID, doc, expectedETag)
}

func clearMessageLease(doc *messageDocument) {
	if doc == nil {
		return
	}
	doc.LeaseID = ""
	doc.LeaseFencingToken = 0
	doc.LeaseTxnID = ""
}

// Reschedule moves message visibility forward without incrementing attempts (used for nack).
func (s *Service) Reschedule(ctx context.Context, namespace, queue string, doc *messageDocument, expectedETag string, delay time.Duration) (string, error) {
	if doc == nil {
		return "", fmt.Errorf("queue: nil message document")
	}
	if delay < 0 {
		delay = 0
	}
	now := s.clk.Now().UTC()
	doc.NotVisibleUntil = now.Add(delay)
	doc.UpdatedAt = now
	clearMessageLease(doc)
	if doc.Namespace == "" {
		doc.Namespace = namespace
	}
	return s.SaveMessageDocument(ctx, namespace, queue, doc.ID, doc, expectedETag)
}

// ExtendVisibility pushes not_visible_until forward relative to now.
func (s *Service) ExtendVisibility(ctx context.Context, namespace, queue string, doc *messageDocument, expectedETag string, extension time.Duration) (string, error) {
	if doc == nil {
		return "", fmt.Errorf("queue: nil message document")
	}
	if extension <= 0 {
		extension = time.Duration(doc.VisibilityTimeout) * time.Second
		if extension <= 0 {
			extension = s.cfg.DefaultVisibilityTimeout
		}
	}
	now := s.clk.Now().UTC()
	doc.NotVisibleUntil = now.Add(extension)
	doc.VisibilityTimeout = int64(extension / time.Second)
	doc.UpdatedAt = now
	if doc.Namespace == "" {
		doc.Namespace = namespace
	}
	return s.SaveMessageDocument(ctx, namespace, queue, doc.ID, doc, expectedETag)
}

// GetPayload streams the message payload.
func (s *Service) GetPayload(ctx context.Context, namespace, queue, id string) (PayloadResult, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return PayloadResult{}, err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return PayloadResult{}, err
	}
	payloadKey := messagePayloadPath(name, id)
	descriptor := []byte(nil)
	if ctxDesc, ok := storage.ObjectDescriptorFromContext(ctx); ok && len(ctxDesc) > 0 {
		descriptor = append([]byte(nil), ctxDesc...)
	}
	obj, err := s.store.GetObject(ctx, ns, payloadKey)
	if err != nil {
		return PayloadResult{}, err
	}
	reader := obj.Reader
	info := obj.Info
	if s.crypto != nil && s.crypto.Enabled() {
		if len(descriptor) == 0 && info != nil {
			descriptor = append([]byte(nil), info.Descriptor...)
		}
		if len(descriptor) == 0 {
			reader.Close()
			return PayloadResult{}, fmt.Errorf("queue: missing payload descriptor for %s", fullObjectKey(ns, payloadKey))
		}
		mat, err := s.crypto.MaterialFromDescriptor(storage.QueuePayloadContext(ns, payloadKey), descriptor)
		if err != nil {
			reader.Close()
			return PayloadResult{}, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(reader, mat)
		if err != nil {
			reader.Close()
			return PayloadResult{}, err
		}
		if info != nil {
			info.ContentType = ""
			info.Descriptor = append([]byte(nil), descriptor...)
		}
		return PayloadResult{Reader: decReader, Info: info}, nil
	}
	return PayloadResult{Reader: reader, Info: info}, nil
}

// EnsureStateExists creates an empty state document if missing.
func (s *Service) EnsureStateExists(ctx context.Context, namespace, queue, id string) (string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return "", err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return "", err
	}
	stateKey := messageStatePath(name, id)
	now := s.clk.Now().UTC()
	corr := correlation.ID(ctx)
	if corr == "" {
		corr = correlation.Generate()
	}
	doc := stateDocument{
		Type:          "queue_state",
		Namespace:     ns,
		Queue:         name,
		MessageID:     id,
		CreatedAt:     now,
		UpdatedAt:     now,
		CorrelationID: corr,
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("queue: marshal state: %w", err)
	}
	info, err := s.store.PutObject(ctx, ns, stateKey, bytes.NewReader(payload), storage.PutObjectOptions{
		IfNotExists: true,
		ContentType: "application/json",
	})
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			obj, loadErr := s.store.GetObject(ctx, ns, stateKey)
			if loadErr != nil {
				return "", loadErr
			}
			_ = obj.Reader.Close()
			if obj.Info != nil {
				return obj.Info.ETag, nil
			}
			return "", nil
		}
		return "", err
	}
	if info != nil {
		return info.ETag, nil
	}
	return "", nil
}

// MoveToDLQ moves metadata/payload/state to the dead-letter queue.
func (s *Service) MoveToDLQ(ctx context.Context, namespace, queue, id string, doc *messageDocument, metaETag string) error {
	return s.moveToDLQInternal(ctx, namespace, queue, id, doc, metaETag, true)
}

func (s *Service) moveToDLQInternal(ctx context.Context, namespace, queue, id string, doc *messageDocument, metaETag string, notify bool) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return err
	}
	if doc.Namespace == "" {
		doc.Namespace = ns
	}
	if doc.Namespace != ns || doc.Queue != name || doc.ID != id {
		return fmt.Errorf("queue: document mismatch for DLQ move")
	}
	metaKey := messageMetaPath(name, id)
	dlqMeta := messageDLQMetaPath(name, id)
	payloadKey := messagePayloadPath(name, id)
	dlqPayload := messageDLQPayloadPath(name, id)
	stateKey := messageStatePath(name, id)
	dlqState := messageDLQStatePath(name, id)

	doc.LastError = map[string]any{
		"reason":       "max_attempts_exceeded",
		"time_rfc3339": s.clk.Now().UTC().Format(time.RFC3339Nano),
	}
	doc.MetaDescriptor = nil
	metaPayload, metaDescriptor, err := s.encodeMessageMeta(ns, dlqMeta, doc)
	if err != nil {
		return err
	}
	metaOpts := storage.PutObjectOptions{
		IfNotExists: true,
		ContentType: storage.ContentTypeProtobuf,
		Descriptor:  metaDescriptor,
	}
	if s.crypto != nil && s.crypto.Enabled() {
		metaOpts.ContentType = storage.ContentTypeProtobufEncrypted
	}
	if _, err := s.store.PutObject(ctx, ns, dlqMeta, bytes.NewReader(metaPayload), metaOpts); err != nil {
		return err
	}
	if err := s.copyObject(ctx, ns, payloadKey, dlqPayload, true, storage.QueuePayloadContext(ns, payloadKey), storage.QueuePayloadContext(ns, dlqPayload)); err != nil {
		return err
	}
	if err := s.copyObject(ctx, ns, stateKey, dlqState, true, "", ""); err != nil {
		return err
	}
	if err := s.store.DeleteObject(ctx, ns, metaKey, storage.DeleteObjectOptions{ExpectedETag: metaETag}); err != nil {
		return err
	}
	_ = s.store.DeleteObject(ctx, ns, payloadKey, storage.DeleteObjectOptions{IgnoreNotFound: true})
	_ = s.store.DeleteObject(ctx, ns, stateKey, storage.DeleteObjectOptions{IgnoreNotFound: true})
	if notify {
		s.removeFromReadyCache(ns, name, id)
	}
	return nil
}

func (s *Service) copyObject(ctx context.Context, namespace, srcKey, dstKey string, ifNotExists bool, srcContext, dstContext string) error {
	obj, err := s.store.GetObject(ctx, namespace, srcKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return err
	}
	reader := obj.Reader
	info := obj.Info
	var plainReader io.Reader = reader
	plainCloser := reader
	if srcContext != "" && s.crypto != nil && s.crypto.Enabled() {
		descriptor := []byte(nil)
		if info != nil {
			descriptor = append([]byte(nil), info.Descriptor...)
		}
		if len(descriptor) == 0 {
			reader.Close()
			return fmt.Errorf("queue: missing descriptor for %s", fullObjectKey(namespace, srcKey))
		}
		mat, err := s.crypto.MaterialFromDescriptor(srcContext, descriptor)
		if err != nil {
			reader.Close()
			return err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(reader, mat)
		if err != nil {
			reader.Close()
			return err
		}
		plainReader = decReader
		plainCloser = decReader
	}
	defer plainCloser.Close()

	finalReader := plainReader
	descriptor := []byte(nil)
	var finalReaderCloser io.ReadCloser
	if dstContext != "" && s.crypto != nil && s.crypto.Enabled() {
		minted, err := s.crypto.MintMaterial(dstContext)
		if err != nil {
			return err
		}
		descriptor = append([]byte(nil), minted.Descriptor...)
		mat := minted.Material
		pr, pw := io.Pipe()
		encWriter, err := s.crypto.EncryptWriterForMaterial(pw, mat)
		if err != nil {
			pw.Close()
			return err
		}
		startEncryptedPipe(ctx, plainReader, encWriter, pw)
		finalReader = pr
		finalReaderCloser = pr
	} else if info != nil && len(info.Descriptor) > 0 {
		descriptor = append([]byte(nil), info.Descriptor...)
	}
	if finalReaderCloser != nil {
		defer finalReaderCloser.Close()
	}

	opts := storage.PutObjectOptions{
		ContentType: "",
		Descriptor:  descriptor,
	}
	if info != nil {
		opts.ContentType = info.ContentType
	}
	if dstContext != "" && s.crypto != nil && s.crypto.Enabled() {
		opts.ContentType = storage.ContentTypeOctetStreamEncrypted
	}
	if ifNotExists {
		opts.IfNotExists = true
	}
	if _, err := s.store.PutObject(ctx, namespace, dstKey, finalReader, opts); err != nil {
		if ifNotExists && errors.Is(err, storage.ErrCASMismatch) {
			return nil
		}
		return err
	}
	return nil
}

func startEncryptedPipe(ctx context.Context, src io.Reader, encWriter io.WriteCloser, pw *io.PipeWriter) <-chan struct{} {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	var closeOnce sync.Once
	closePipe := func(err error) {
		closeOnce.Do(func() {
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			_ = pw.Close()
		})
	}
	go func() {
		defer close(done)
		stopCancel := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				closePipe(ctx.Err())
			case <-stopCancel:
			}
		}()
		if _, err := io.Copy(encWriter, src); err != nil {
			close(stopCancel)
			_ = encWriter.Close()
			closePipe(err)
			return
		}
		close(stopCancel)
		if err := encWriter.Close(); err != nil {
			closePipe(err)
			return
		}
		closePipe(nil)
	}()
	return done
}

// DeleteState removes the workflow state document for queue message.
func (s *Service) DeleteState(ctx context.Context, namespace, queue, id string, expectedETag string) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return err
	}
	stateKey := messageStatePath(name, id)
	opts := storage.DeleteObjectOptions{
		ExpectedETag:   expectedETag,
		IgnoreNotFound: expectedETag == "",
	}
	err = s.store.DeleteObject(ctx, ns, stateKey, opts)
	if errors.Is(err, storage.ErrNotFound) && expectedETag == "" {
		return nil
	}
	return err
}

// LoadState retrieves the JSON state document associated with the queue message.
func (s *Service) LoadState(ctx context.Context, namespace, queue, id string) (StateResult, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return StateResult{}, err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return StateResult{}, err
	}
	stateKey := messageStatePath(name, id)
	obj, err := s.store.GetObject(ctx, ns, stateKey)
	if err != nil {
		return StateResult{}, err
	}
	defer obj.Reader.Close()
	var doc stateDocument
	if err := json.NewDecoder(obj.Reader).Decode(&doc); err != nil {
		return StateResult{}, fmt.Errorf("queue: decode state %q: %w", fullObjectKey(ns, stateKey), err)
	}
	etag := ""
	if obj.Info != nil {
		etag = obj.Info.ETag
	}
	if doc.Namespace == "" {
		doc.Namespace = ns
	}
	if doc.Queue == "" {
		doc.Queue = name
	}
	return StateResult{Document: &doc, ETag: etag}, nil
}

// SaveState updates the workflow state document via CAS.
func (s *Service) SaveState(ctx context.Context, namespace, queue, id string, doc *stateDocument, expectedETag string) (string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return "", err
	}
	ns, err := sanitizeNamespace(namespace)
	if err != nil {
		return "", err
	}
	if doc.Namespace == "" {
		doc.Namespace = ns
	}
	if doc.Queue == "" {
		doc.Queue = name
	}
	if doc.Namespace != ns || doc.Queue != name || doc.MessageID != id {
		return "", fmt.Errorf("queue: state document mismatch for %s/%s", name, id)
	}
	now := s.clk.Now().UTC()
	doc.UpdatedAt = now
	payload, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("queue: marshal state: %w", err)
	}
	stateKey := messageStatePath(name, id)
	info, err := s.store.PutObject(ctx, ns, stateKey, bytes.NewReader(payload), storage.PutObjectOptions{
		ContentType:  "application/json",
		ExpectedETag: expectedETag,
	})
	if err != nil {
		return "", err
	}
	if info != nil {
		return info.ETag, nil
	}
	return "", nil
}

// Nack updates metadata with a delay and optional error payload.
func (s *Service) Nack(ctx context.Context, namespace, queue string, doc *messageDocument, expectedETag string, delay time.Duration, lastErr any) (string, error) {
	if delay < 0 {
		delay = 0
	}
	now := s.clk.Now().UTC()
	if delay == 0 {
		doc.NotVisibleUntil = now
	} else {
		seconds := int64(math.Ceil(delay.Seconds()))
		if seconds <= 0 {
			seconds = 1
		}
		doc.NotVisibleUntil = now.Add(time.Duration(seconds) * time.Second)
		doc.VisibilityTimeout = seconds
	}
	doc.UpdatedAt = now
	if doc.Namespace == "" {
		doc.Namespace = namespace
	}
	doc.LastError = lastErr
	clearMessageLease(doc)
	return s.SaveMessageDocument(ctx, namespace, queue, doc.ID, doc, expectedETag)
}

// Ack removes state (if present) and message metadata/payload.
func (s *Service) Ack(ctx context.Context, namespace, queue, id string, metaETag string, stateETag string, stateRequired bool) error {
	if stateRequired || stateETag != "" {
		if err := s.DeleteState(ctx, namespace, queue, id, stateETag); err != nil {
			if stateRequired || (!errors.Is(err, storage.ErrNotFound) && stateETag != "") {
				return err
			}
		}
	}
	return s.DeleteMessage(ctx, namespace, queue, id, metaETag)
}
