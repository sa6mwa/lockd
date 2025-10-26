package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
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
	Crypto                   *storage.Crypto
}

// Service implements queue primitives atop the storage backend.
type Service struct {
	store  storage.Backend
	clk    clock.Clock
	cfg    Config
	ready  sync.Map // map[string]*readyCache
	crypto *storage.Crypto
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
	return &Service{
		store:  store,
		clk:    clk,
		cfg:    cfg,
		crypto: cfg.Crypto,
	}, nil
}

func (s *Service) readyCacheForQueue(queue string) *readyCache {
	if value, ok := s.ready.Load(queue); ok {
		return value.(*readyCache)
	}
	cache := newReadyCache(s, queue)
	actual, loaded := s.ready.LoadOrStore(queue, cache)
	if loaded {
		return actual.(*readyCache)
	}
	return cache
}

func (s *Service) notifyCacheUpdate(queue string, desc MessageDescriptor) {
	if value, ok := s.ready.Load(queue); ok {
		value.(*readyCache).upsert(desc)
	}
}

func (s *Service) removeFromReadyCache(queue, id string) {
	if value, ok := s.ready.Load(queue); ok {
		value.(*readyCache).remove(id)
	}
}

func (s *Service) invalidateQueueCache(queue string) {
	if value, ok := s.ready.Load(queue); ok {
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
	Queue            string
	ID               string
	EnqueuedAt       time.Time
	NotVisibleUntil  time.Time
	Visibility       time.Duration
	MaxAttempts      int
	Attempts         int
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
	ID           string
	MetadataKey  string
	MetadataETag string
	Document     messageDocument
}

// StateDocument captures metadata about queue state blobs.
type StateDocument struct {
	Type          string    `json:"type"`
	Queue         string    `json:"queue"`
	MessageID     string    `json:"message_id"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	CorrelationID string    `json:"correlation_id,omitempty"`
}

// MessageDocument captures per-message metadata stored alongside payloads.
type MessageDocument struct {
	Type               string         `json:"type"`
	Queue              string         `json:"queue"`
	ID                 string         `json:"id"`
	EnqueuedAt         time.Time      `json:"enqueued_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
	Attempts           int            `json:"attempts"`
	NotVisibleUntil    time.Time      `json:"not_visible_until"`
	MaxAttempts        int            `json:"max_attempts"`
	Attributes         map[string]any `json:"attributes,omitempty"`
	LastError          any            `json:"last_error,omitempty"`
	PayloadBytes       int64          `json:"payload_bytes,omitempty"`
	PayloadContentType string         `json:"payload_content_type,omitempty"`
	VisibilityTimeout  int64          `json:"visibility_timeout_seconds,omitempty"`
	ExpiresAt          *time.Time     `json:"expires_at,omitempty"`
	CorrelationID      string         `json:"correlation_id,omitempty"`
	PayloadDescriptor  []byte         `json:"payload_descriptor,omitempty"`
	MetaDescriptor     []byte         `json:"meta_descriptor,omitempty"`
}

type messageDocument = MessageDocument

type stateDocument = StateDocument

// Enqueue inserts a new message into queue and returns its metadata.
func (s *Service) Enqueue(ctx context.Context, queue string, payload io.Reader, opts EnqueueOptions) (*Message, error) {
	name, err := sanitizeQueueName(queue)
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
	reader, payloadDescriptor, counting, err := s.preparePayloadReader(payloadKey, payload)
	if err != nil {
		return nil, err
	}
	payloadOpts := storage.PutObjectOptions{
		IfNotExists: true,
		ContentType: payloadContentType,
		Descriptor:  payloadDescriptor,
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payloadOpts.ContentType = storage.ContentTypeOctetStreamEncrypted
	}
	payloadInfo, err := s.store.PutObject(ctx, payloadKey, reader, payloadOpts)
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
	delay := opts.Delay
	if delay < 0 {
		delay = 0
	}
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

	metaPayload, metaDescriptor, err := s.encodeMessageMeta(metaKey, &doc)
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
	metaInfo, err := s.store.PutObject(ctx, metaKey, bytes.NewReader(metaPayload), metaOpts)
	if err != nil {
		if rmErr := s.store.DeleteObject(ctx, payloadKey, storage.DeleteObjectOptions{}); rmErr != nil {
			// best-effort clean-up, log via wrapped backend if available
		}
		return nil, err
	}
	if metaInfo != nil && metaInfo.ETag != "" {
		desc := MessageDescriptor{
			ID:           doc.ID,
			MetadataKey:  metaKey,
			MetadataETag: metaInfo.ETag,
			Document:     doc,
		}
		s.notifyCacheUpdate(name, desc)
	} else {
		s.invalidateQueueCache(name)
	}

	result := &Message{
		Queue:           name,
		ID:              id,
		EnqueuedAt:      now,
		NotVisibleUntil: notVisibleUntil,
		Visibility:      visibility,
		MaxAttempts:     maxAttempts,
		Attempts:        0,
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

func (s *Service) preparePayloadReader(payloadKey string, payload io.Reader) (io.Reader, []byte, *countingReader, error) {
	counting := &countingReader{r: payload}
	if s.crypto == nil || !s.crypto.Enabled() {
		return counting, nil, counting, nil
	}
	mat, descBytes, err := s.crypto.MintMaterial(storage.QueuePayloadContext(payloadKey))
	if err != nil {
		return nil, nil, nil, err
	}
	pr, pw := io.Pipe()
	encWriter, err := s.crypto.EncryptWriterForMaterial(pw, mat)
	if err != nil {
		pw.Close()
		return nil, nil, nil, err
	}
	go func() {
		if _, err := io.Copy(encWriter, counting); err != nil {
			encWriter.Close()
			pw.CloseWithError(err)
			return
		}
		if err := encWriter.Close(); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()
	return pr, append([]byte(nil), descBytes...), counting, nil
}

func (s *Service) encodeMessageMeta(metaKey string, doc *messageDocument) ([]byte, []byte, error) {
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
		mat, err = s.crypto.MaterialFromDescriptor(storage.QueueMetaContext(metaKey), descriptor)
		if err != nil {
			descriptor = nil
		}
	}
	if len(descriptor) == 0 {
		var descBytes []byte
		mat, descBytes, err = s.crypto.MintMaterial(storage.QueueMetaContext(metaKey))
		if err != nil {
			return nil, nil, err
		}
		descriptor = append([]byte(nil), descBytes...)
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

var queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func sanitizeQueueName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !queueNamePattern.MatchString(name) {
		return "", ErrInvalidQueue
	}
	return name, nil
}

func messageMetaPath(queue, id string) string {
	return path.Join("q", queue, "msg", id+metaFileExtension)
}

func messagePayloadPath(queue, id string) string {
	return path.Join("q", queue, "msg", id+".bin")
}

// EnsureMessageReady verifies that the message payload and its metadata exist before delivery.
func (s *Service) EnsureMessageReady(ctx context.Context, queue, id string) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	key := messagePayloadPath(name, id)
	body, _, err := s.store.GetObject(ctx, key)
	if err != nil {
		return err
	}
	if body != nil {
		_ = body.Close()
	}
	return nil
}

func messageStatePath(queue, id string) string {
	return path.Join("q", queue, "state", id+".json")
}

func messageDLQMetaPath(queue, id string) string {
	return path.Join("q", queue, "dlq", "msg", id+metaFileExtension)
}

func messageDLQStatePath(queue, id string) string {
	return path.Join("q", queue, "dlq", "state", id+".json")
}

func messageDLQPayloadPath(queue, id string) string {
	return path.Join("q", queue, "dlq", "msg", id+".bin")
}

// MessageLeaseKey returns the logical lock key used for message leasing.
func MessageLeaseKey(queue, id string) (string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return "", err
	}
	return path.Join("q", name, "msg", id), nil
}

// StateLeaseKey returns the logical lock key used for workflow state leasing.
func StateLeaseKey(queue, id string) (string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return "", err
	}
	return path.Join("q", name, "state", id), nil
}

// IsQueueStateKey reports whether the logical key belongs to a queue state lease.
func IsQueueStateKey(key string) bool {
	clean := path.Clean(key)
	if clean == "" || clean == "." {
		return false
	}
	segments := strings.Split(clean, "/")
	if len(segments) != 4 {
		return false
	}
	return segments[0] == "q" && segments[2] == "state"
}

func cloneAttributes(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func contentTypeOrDefault(contentType string) string {
	if strings.TrimSpace(contentType) == "" {
		return storage.ContentTypeOctetStream
	}
	return contentType
}

// ListMessages enumerates message metadata objects in lexical order.
func (s *Service) ListMessages(ctx context.Context, queue string, startAfter string, limit int) ([]MessageDescriptor, string, bool, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return nil, "", false, err
	}
	prefix := path.Join("q", name, "msg") + "/"
	opts := storage.ListOptions{
		Prefix: prefix,
		Limit:  limit,
	}
	if startAfter != "" {
		opts.StartAfter = startAfter
	}
	result, err := s.store.ListObjects(ctx, opts)
	if err != nil {
		return nil, "", false, err
	}
	descriptors := make([]MessageDescriptor, 0, len(result.Objects))
	for _, obj := range result.Objects {
		if !strings.HasSuffix(obj.Key, metaFileExtension) {
			continue
		}
		id := strings.TrimSuffix(path.Base(obj.Key), metaFileExtension)
		docPtr, etag, err := s.loadMessageDocument(ctx, obj.Key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, "", false, err
		}
		if docPtr.Queue != name || docPtr.ID != id {
			continue
		}
		descriptor := MessageDescriptor{
			ID:           docPtr.ID,
			MetadataKey:  obj.Key,
			MetadataETag: etag,
			Document:     *docPtr,
		}
		descriptors = append(descriptors, descriptor)
	}
	next := result.NextStartAfter
	return descriptors, next, result.Truncated, nil
}

// NextCandidate returns the next visible message ready for processing.
func (s *Service) NextCandidate(ctx context.Context, queue string, startAfter string, pageSize int) (*MessageDescriptor, string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return nil, "", err
	}
	cache := s.readyCacheForQueue(name)
	if startAfter != "" {
		cache.resetWithCursor(startAfter)
	}
	now := s.clk.Now().UTC()
	desc, err := cache.next(ctx, pageSize, now)
	if err != nil {
		return nil, "", err
	}
	if desc == nil {
		return nil, "", storage.ErrNotFound
	}
	return desc, "", nil
}

// RefreshReadyCache syncs the ready cache for the provided queue immediately.
func (s *Service) RefreshReadyCache(ctx context.Context, queue string) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	cache := s.readyCacheForQueue(name)
	_, err = cache.refresh(ctx, defaultCachePageSize, s.clk.Now().UTC(), false)
	return err
}

func (s *Service) listMessageObjects(ctx context.Context, queue string, startAfter string, limit int) ([]storage.ObjectInfo, string, bool, error) {
	prefix := path.Join("q", queue, "msg") + "/"
	opts := storage.ListOptions{
		Prefix: prefix,
		Limit:  limit,
	}
	if startAfter != "" {
		opts.StartAfter = startAfter
	}
	result, err := s.store.ListObjects(ctx, opts)
	if err != nil {
		return nil, "", false, err
	}
	return result.Objects, result.NextStartAfter, result.Truncated, nil
}

func (s *Service) loadDescriptor(ctx context.Context, metadataKey string) (*MessageDescriptor, error) {
	doc, etag, err := s.loadMessageDocument(ctx, metadataKey)
	if err != nil {
		return nil, err
	}
	return &MessageDescriptor{
		ID:           doc.ID,
		MetadataKey:  metadataKey,
		MetadataETag: etag,
		Document:     *doc,
	}, nil
}

// loadMessageDocument fetches and decodes the message metadata.
func (s *Service) loadMessageDocument(ctx context.Context, metadataKey string) (*messageDocument, string, error) {
	reader, info, err := s.store.GetObject(ctx, metadataKey)
	if err != nil {
		return nil, "", err
	}
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
			return nil, "", fmt.Errorf("queue: missing metadata descriptor for %s", metadataKey)
		}
		mat, err := s.crypto.MaterialFromDescriptor(storage.QueueMetaContext(metadataKey), descriptor)
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
			return nil, "", fmt.Errorf("queue: read message %q: %w", metadataKey, err)
		}
	} else {
		defer reader.Close()
		var err error
		data, err = io.ReadAll(reader)
		if err != nil {
			return nil, "", fmt.Errorf("queue: read message %q: %w", metadataKey, err)
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
	return doc, etag, nil
}

// GetMessage loads the metadata document for the provided queue/id pair.
func (s *Service) GetMessage(ctx context.Context, queue, id string) (*MessageDocument, string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return nil, "", err
	}
	return s.loadMessageDocument(ctx, messageMetaPath(name, id))
}

// SaveMessageDocument writes metadata via CAS.
func (s *Service) SaveMessageDocument(ctx context.Context, queue, id string, doc *messageDocument, expectedETag string) (string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return "", err
	}
	if doc.Queue != name || doc.ID != id {
		return "", fmt.Errorf("queue: document mismatch for %s/%s", name, id)
	}
	metaKey := messageMetaPath(name, id)
	metaPayload, metaDescriptor, err := s.encodeMessageMeta(metaKey, doc)
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
	info, err := s.store.PutObject(ctx, metaKey, bytes.NewReader(metaPayload), opts)
	if err != nil {
		return "", err
	}
	newETag := expectedETag
	if info != nil && info.ETag != "" {
		newETag = info.ETag
	}
	if newETag != "" {
		desc := MessageDescriptor{
			ID:           doc.ID,
			MetadataKey:  metaKey,
			MetadataETag: newETag,
			Document:     *doc,
		}
		s.notifyCacheUpdate(name, desc)
	} else {
		s.invalidateQueueCache(name)
	}
	return newETag, nil
}

// DeleteMessage removes metadata and payload (best-effort) respecting CAS.
func (s *Service) DeleteMessage(ctx context.Context, queue, id string, expectedETag string) error {
	return s.deleteMessageInternal(ctx, queue, id, expectedETag, true)
}

func (s *Service) deleteMessageInternal(ctx context.Context, queue, id string, expectedETag string, notify bool) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	metaKey := messageMetaPath(name, id)
	if err := s.store.DeleteObject(ctx, metaKey, storage.DeleteObjectOptions{ExpectedETag: expectedETag}); err != nil {
		return err
	}
	payloadKey := messagePayloadPath(name, id)
	_ = s.store.DeleteObject(ctx, payloadKey, storage.DeleteObjectOptions{IgnoreNotFound: true})
	if notify {
		s.removeFromReadyCache(name, id)
	}
	return nil
}

// IncrementAttempts updates attempts and visibility after a successful lease acquire.
func (s *Service) IncrementAttempts(ctx context.Context, queue string, doc *messageDocument, expectedETag string, visibility time.Duration) (string, error) {
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
	return s.SaveMessageDocument(ctx, queue, doc.ID, doc, expectedETag)
}

// Reschedule moves message visibility forward without incrementing attempts (used for nack).
func (s *Service) Reschedule(ctx context.Context, queue string, doc *messageDocument, expectedETag string, delay time.Duration) (string, error) {
	if delay < 0 {
		delay = 0
	}
	now := s.clk.Now().UTC()
	doc.NotVisibleUntil = now.Add(delay)
	doc.UpdatedAt = now
	return s.SaveMessageDocument(ctx, queue, doc.ID, doc, expectedETag)
}

// ExtendVisibility pushes not_visible_until forward relative to now.
func (s *Service) ExtendVisibility(ctx context.Context, queue string, doc *messageDocument, expectedETag string, extension time.Duration) (string, error) {
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
	return s.SaveMessageDocument(ctx, queue, doc.ID, doc, expectedETag)
}

// GetPayload streams the message payload.
func (s *Service) GetPayload(ctx context.Context, queue, id string) (io.ReadCloser, *storage.ObjectInfo, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return nil, nil, err
	}
	payloadKey := messagePayloadPath(name, id)
	descriptor := []byte(nil)
	if ctxDesc, ok := storage.ObjectDescriptorFromContext(ctx); ok && len(ctxDesc) > 0 {
		descriptor = append([]byte(nil), ctxDesc...)
	}
	reader, info, err := s.store.GetObject(ctx, payloadKey)
	if err != nil {
		return nil, nil, err
	}
	if s.crypto != nil && s.crypto.Enabled() {
		if len(descriptor) == 0 && info != nil {
			descriptor = append([]byte(nil), info.Descriptor...)
		}
		if len(descriptor) == 0 {
			reader.Close()
			return nil, nil, fmt.Errorf("queue: missing payload descriptor for %s", payloadKey)
		}
		mat, err := s.crypto.MaterialFromDescriptor(storage.QueuePayloadContext(payloadKey), descriptor)
		if err != nil {
			reader.Close()
			return nil, nil, err
		}
		decReader, err := s.crypto.DecryptReaderForMaterial(reader, mat)
		if err != nil {
			reader.Close()
			return nil, nil, err
		}
		if info != nil {
			info.ContentType = ""
			info.Descriptor = append([]byte(nil), descriptor...)
		}
		return decReader, info, nil
	}
	return reader, info, nil
}

// EnsureStateExists creates an empty state document if missing.
func (s *Service) EnsureStateExists(ctx context.Context, queue, id string) (string, error) {
	name, err := sanitizeQueueName(queue)
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
	info, err := s.store.PutObject(ctx, stateKey, bytes.NewReader(payload), storage.PutObjectOptions{
		IfNotExists: true,
		ContentType: "application/json",
	})
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			reader, meta, loadErr := s.store.GetObject(ctx, stateKey)
			if loadErr != nil {
				return "", loadErr
			}
			_ = reader.Close()
			if meta != nil {
				return meta.ETag, nil
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
func (s *Service) MoveToDLQ(ctx context.Context, queue, id string, doc *messageDocument, metaETag string) error {
	return s.moveToDLQInternal(ctx, queue, id, doc, metaETag, true)
}

func (s *Service) moveToDLQInternal(ctx context.Context, queue, id string, doc *messageDocument, metaETag string, notify bool) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	if doc.Queue != name || doc.ID != id {
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
	metaPayload, metaDescriptor, err := s.encodeMessageMeta(dlqMeta, doc)
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
	if _, err := s.store.PutObject(ctx, dlqMeta, bytes.NewReader(metaPayload), metaOpts); err != nil {
		return err
	}
	if err := s.copyObject(ctx, payloadKey, dlqPayload, true, storage.QueuePayloadContext(payloadKey), storage.QueuePayloadContext(dlqPayload)); err != nil {
		return err
	}
	if err := s.copyObject(ctx, stateKey, dlqState, true, "", ""); err != nil {
		return err
	}
	if err := s.store.DeleteObject(ctx, metaKey, storage.DeleteObjectOptions{ExpectedETag: metaETag}); err != nil {
		return err
	}
	_ = s.store.DeleteObject(ctx, payloadKey, storage.DeleteObjectOptions{IgnoreNotFound: true})
	_ = s.store.DeleteObject(ctx, stateKey, storage.DeleteObjectOptions{IgnoreNotFound: true})
	if notify {
		s.removeFromReadyCache(name, id)
	}
	return nil
}

func (s *Service) copyObject(ctx context.Context, srcKey, dstKey string, ifNotExists bool, srcContext, dstContext string) error {
	reader, info, err := s.store.GetObject(ctx, srcKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return err
	}
	var plainReader io.Reader = reader
	plainCloser := reader
	if srcContext != "" && s.crypto != nil && s.crypto.Enabled() {
		descriptor := []byte(nil)
		if info != nil {
			descriptor = append([]byte(nil), info.Descriptor...)
		}
		if len(descriptor) == 0 {
			reader.Close()
			return fmt.Errorf("queue: missing descriptor for %s", srcKey)
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
	if dstContext != "" && s.crypto != nil && s.crypto.Enabled() {
		mat, descBytes, err := s.crypto.MintMaterial(dstContext)
		if err != nil {
			return err
		}
		descriptor = append([]byte(nil), descBytes...)
		pr, pw := io.Pipe()
		encWriter, err := s.crypto.EncryptWriterForMaterial(pw, mat)
		if err != nil {
			pw.Close()
			return err
		}
		go func() {
			if _, err := io.Copy(encWriter, plainReader); err != nil {
				encWriter.Close()
				pw.CloseWithError(err)
				return
			}
			if err := encWriter.Close(); err != nil {
				pw.CloseWithError(err)
				return
			}
			pw.Close()
		}()
		finalReader = pr
	} else if info != nil && len(info.Descriptor) > 0 {
		descriptor = append([]byte(nil), info.Descriptor...)
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
	if _, err := s.store.PutObject(ctx, dstKey, finalReader, opts); err != nil {
		if ifNotExists && errors.Is(err, storage.ErrCASMismatch) {
			return nil
		}
		return err
	}
	return nil
}

// DeleteState removes the workflow state document for queue message.
func (s *Service) DeleteState(ctx context.Context, queue, id string, expectedETag string) error {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return err
	}
	stateKey := messageStatePath(name, id)
	opts := storage.DeleteObjectOptions{
		ExpectedETag:   expectedETag,
		IgnoreNotFound: expectedETag == "",
	}
	err = s.store.DeleteObject(ctx, stateKey, opts)
	if errors.Is(err, storage.ErrNotFound) && expectedETag == "" {
		return nil
	}
	return err
}

// LoadState retrieves the JSON state document associated with the queue message.
func (s *Service) LoadState(ctx context.Context, queue, id string) (*StateDocument, string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return nil, "", err
	}
	stateKey := messageStatePath(name, id)
	reader, info, err := s.store.GetObject(ctx, stateKey)
	if err != nil {
		return nil, "", err
	}
	defer reader.Close()
	var doc stateDocument
	if err := json.NewDecoder(reader).Decode(&doc); err != nil {
		return nil, "", fmt.Errorf("queue: decode state %q: %w", stateKey, err)
	}
	etag := ""
	if info != nil {
		etag = info.ETag
	}
	return &doc, etag, nil
}

// SaveState updates the workflow state document via CAS.
func (s *Service) SaveState(ctx context.Context, queue, id string, doc *stateDocument, expectedETag string) (string, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return "", err
	}
	if doc.Queue != name || doc.MessageID != id {
		return "", fmt.Errorf("queue: state document mismatch for %s/%s", name, id)
	}
	now := s.clk.Now().UTC()
	doc.UpdatedAt = now
	payload, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("queue: marshal state: %w", err)
	}
	stateKey := messageStatePath(name, id)
	info, err := s.store.PutObject(ctx, stateKey, bytes.NewReader(payload), storage.PutObjectOptions{
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
func (s *Service) Nack(ctx context.Context, queue string, doc *messageDocument, expectedETag string, delay time.Duration, lastErr any) (string, error) {
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
	doc.LastError = lastErr
	return s.SaveMessageDocument(ctx, queue, doc.ID, doc, expectedETag)
}

// Ack removes state (if present) and message metadata/payload.
func (s *Service) Ack(ctx context.Context, queue, id string, metaETag string, stateETag string, stateRequired bool) error {
	if err := s.DeleteState(ctx, queue, id, stateETag); err != nil {
		if stateRequired || (!errors.Is(err, storage.ErrNotFound) && stateETag != "") {
			return err
		}
	}
	return s.DeleteMessage(ctx, queue, id, metaETag)
}
