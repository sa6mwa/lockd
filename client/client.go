package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/pslog"
)

const (
	envClientHTTPTrace    = "LOCKD_CLIENT_HTTPTRACE"
	envClientHTTPTraceAlt = "LOCKD_CLIENT_TRACE_HTTP"
)

func envBool(name string) bool {
	val, ok := os.LookupEnv(name)
	if !ok {
		return false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return true
	}
	if b, err := strconv.ParseBool(val); err == nil {
		return b
	}
	switch strings.ToLower(val) {
	case "1", "t", "true", "yes", "on":
		return true
	}
	return false
}

// EnqueueOptions controls enqueue behaviour.
type EnqueueOptions struct {
	Delay       time.Duration
	Visibility  time.Duration
	TTL         time.Duration
	MaxAttempts int
	Attributes  map[string]any
	ContentType string
}

// DequeueOptions guides dequeue behaviour.
type DequeueOptions struct {
	Owner        string
	Visibility   time.Duration
	BlockSeconds int64 // set to BlockNoWait (-1) for immediate return, 0 for forever, >0 to wait seconds
	PageSize     int
	StartAfter   string
	OnCloseDelay time.Duration // optional delay applied when QueueMessage.Close auto-nacks the lease
}

// SubscribeOptions configures continuous streaming consumption via Subscribe.
type SubscribeOptions struct {
	Owner        string
	Visibility   time.Duration
	BlockSeconds int64
	Prefetch     int
	StartAfter   string
	OnCloseDelay time.Duration
}

// DequeueResult captures the outcome of a dequeue request.
type DequeueResult struct {
	Message    *QueueMessageHandle
	Messages   []*QueueMessageHandle
	NextCursor string
}

// MessageHandler is invoked for each message delivered via Subscribe.
type MessageHandler func(context.Context, *QueueMessage) error

// MessageHandlerWithState is invoked for stateful subscriptions and receives the associated workflow state handle.
type MessageHandlerWithState func(context.Context, *QueueMessage, *QueueStateHandle) error

// QueueMessageHandle models a leased queue message and provides helpers to ack/nack/extend.
type QueueMessageHandle struct {
	client *Client
	msg    api.Message
	cursor string

	payloadMu       sync.Mutex
	payloadStream   io.ReadCloser
	payloadReader   *queuePayloadReader
	payloadConsumed bool
	payloadClosed   bool

	mu     sync.Mutex
	done   bool
	acked  bool
	nacked bool
	state  *QueueStateHandle
}

// QueueStateHandle exposes the workflow state lease associated with a stateful dequeue.
type QueueStateHandle struct {
	client         *Client
	queue          string
	messageID      string
	leaseID        string
	fencingToken   int64
	leaseExpiresAt int64
	stateETag      string
	correlationID  string
}

// Queue returns the queue name.
func (h *QueueMessageHandle) Queue() string {
	if h == nil {
		return ""
	}
	return h.msg.Queue
}

// MessageID returns the message identifier.
func (h *QueueMessageHandle) MessageID() string {
	if h == nil {
		return ""
	}
	return h.msg.MessageID
}

// Attempts returns the delivery attempts recorded for the message.
func (h *QueueMessageHandle) Attempts() int {
	if h == nil {
		return 0
	}
	return h.msg.Attempts
}

// MaxAttempts returns the configured maximum attempts for the message.
func (h *QueueMessageHandle) MaxAttempts() int {
	if h == nil {
		return 0
	}
	return h.msg.MaxAttempts
}

// NotVisibleUntil reports when the message becomes visible again.
func (h *QueueMessageHandle) NotVisibleUntil() time.Time {
	if h == nil {
		return time.Time{}
	}
	return time.Unix(h.msg.NotVisibleUntilUnix, 0)
}

// VisibilityTimeout returns the current visibility timeout.
func (h *QueueMessageHandle) VisibilityTimeout() time.Duration {
	if h == nil {
		return 0
	}
	return time.Duration(h.msg.VisibilityTimeoutSeconds) * time.Second
}

// PayloadReader returns a streaming reader for the message payload.
// Callers must close the returned reader when finished. The payload can only
// be consumed once; subsequent calls return an error.
func (h *QueueMessageHandle) PayloadReader() (io.ReadCloser, error) {
	if h == nil {
		return nil, fmt.Errorf("lockd: nil queue handle")
	}
	h.payloadMu.Lock()
	defer h.payloadMu.Unlock()
	if h.payloadClosed {
		return nil, fmt.Errorf("lockd: queue payload already closed")
	}
	if h.payloadReader != nil {
		return h.payloadReader, nil
	}
	if h.payloadStream == nil {
		h.payloadClosed = true
		h.payloadConsumed = true
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	reader := &queuePayloadReader{handle: h, reader: h.payloadStream}
	h.payloadReader = reader
	h.payloadConsumed = true
	return reader, nil
}

// WritePayloadTo streams the payload into w, closing the payload afterwards.
func (h *QueueMessageHandle) WritePayloadTo(w io.Writer) (int64, error) {
	reader, err := h.PayloadReader()
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	return io.Copy(w, reader)
}

// DecodePayloadJSON decodes the payload as JSON into v.
func (h *QueueMessageHandle) DecodePayloadJSON(v any) error {
	reader, err := h.PayloadReader()
	if err != nil {
		return err
	}
	defer reader.Close()
	return json.NewDecoder(reader).Decode(v)
}

// ClosePayload releases any remaining payload stream resources without reading.
func (h *QueueMessageHandle) ClosePayload() error {
	if h == nil {
		return nil
	}
	h.payloadMu.Lock()
	defer h.payloadMu.Unlock()
	if h.payloadClosed {
		return nil
	}
	h.payloadClosed = true
	var err error
	if h.payloadReader != nil {
		if cerr := h.payloadReader.reader.Close(); cerr != nil {
			err = cerr
		}
		h.payloadReader = nil
	}
	if h.payloadStream != nil {
		if cerr := h.payloadStream.Close(); err == nil {
			err = cerr
		}
		h.payloadStream = nil
	}
	return err
}

type queuePayloadReader struct {
	handle *QueueMessageHandle
	reader io.ReadCloser
	once   sync.Once
}

func (r *queuePayloadReader) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *queuePayloadReader) Close() error {
	var err error
	r.once.Do(func() {
		err = r.handle.ClosePayload()
	})
	return err
}

// ContentType returns the payload content type.
func (h *QueueMessageHandle) ContentType() string {
	if h == nil {
		return ""
	}
	return h.msg.PayloadContentType
}

// PayloadSize returns the declared payload size in bytes.
func (h *QueueMessageHandle) PayloadSize() int64 {
	if h == nil {
		return 0
	}
	return h.msg.PayloadBytes
}

// Cursor returns the next-cursor value associated with the dequeue call.
func (h *QueueMessageHandle) Cursor() string {
	if h == nil {
		return ""
	}
	return h.cursor
}

// LeaseID returns the active message lease identifier.
func (h *QueueMessageHandle) LeaseID() string {
	if h == nil {
		return ""
	}
	return h.msg.LeaseID
}

// LeaseExpiresAt returns the unix timestamp when the message lease expires.
func (h *QueueMessageHandle) LeaseExpiresAt() int64 {
	if h == nil {
		return 0
	}
	return h.msg.LeaseExpiresAtUnix
}

// FencingToken exposes the current message fencing token.
func (h *QueueMessageHandle) FencingToken() int64 {
	if h == nil {
		return 0
	}
	return h.msg.FencingToken
}

// MetaETag returns the metadata ETag associated with the message.
func (h *QueueMessageHandle) MetaETag() string {
	if h == nil {
		return ""
	}
	return h.msg.MetaETag
}

// CorrelationID returns the correlation identifier associated with the message lifecycle.
func (h *QueueMessageHandle) CorrelationID() string {
	if h == nil {
		return ""
	}
	return h.msg.CorrelationID
}

// StateHandle exposes the state lease handle when using DequeueWithState.
func (h *QueueMessageHandle) StateHandle() *QueueStateHandle {
	if h == nil {
		return nil
	}
	return h.state
}

// Ack removes the message (and state, when present) from the queue.
func (h *QueueMessageHandle) Ack(ctx context.Context) error {
	if h == nil {
		return fmt.Errorf("lockd: nil queue handle")
	}
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		return fmt.Errorf("lockd: queue handle already closed")
	}
	req := api.AckRequest{
		Queue:        h.msg.Queue,
		MessageID:    h.msg.MessageID,
		LeaseID:      h.msg.LeaseID,
		FencingToken: h.msg.FencingToken,
		MetaETag:     h.msg.MetaETag,
		StateETag:    h.msg.StateETag,
	}
	if h.state != nil {
		req.StateLeaseID = h.state.leaseID
		req.StateFencingToken = h.state.fencingToken
		if h.state.stateETag != "" {
			req.StateETag = h.state.stateETag
		}
	}
	h.mu.Unlock()
	corr := h.msg.CorrelationID
	if corr != "" {
		ctx = WithCorrelationID(ctx, corr)
	}
	if err := h.ClosePayload(); err != nil {
		return err
	}
	res, err := h.client.QueueAck(ctx, req)
	if err != nil {
		return err
	}
	if res != nil && res.CorrelationID != "" {
		h.msg.CorrelationID = res.CorrelationID
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.done = true
	h.acked = true
	h.state = nil
	if h.msg.LeaseID != "" {
		h.client.leaseTokens.Delete(h.msg.LeaseID)
	}
	if h.msg.StateLeaseID != "" {
		h.client.leaseTokens.Delete(h.msg.StateLeaseID)
	}
	return nil
}

// Nack releases the message with an optional delay and error payload.
func (h *QueueMessageHandle) Nack(ctx context.Context, delay time.Duration, lastErr any) error {
	if h == nil {
		return fmt.Errorf("lockd: nil queue handle")
	}
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		return fmt.Errorf("lockd: queue handle already closed")
	}
	req := api.NackRequest{
		Queue:        h.msg.Queue,
		MessageID:    h.msg.MessageID,
		LeaseID:      h.msg.LeaseID,
		FencingToken: h.msg.FencingToken,
		MetaETag:     h.msg.MetaETag,
		DelaySeconds: secondsFromDuration(delay),
		LastError:    lastErr,
	}
	if h.state != nil {
		req.StateLeaseID = h.state.leaseID
		req.StateFencingToken = h.state.fencingToken
	}
	h.mu.Unlock()
	corr := h.msg.CorrelationID
	if corr != "" {
		ctx = WithCorrelationID(ctx, corr)
	}
	if err := h.ClosePayload(); err != nil {
		return err
	}
	res, err := h.client.QueueNack(ctx, req)
	if err != nil {
		return err
	}
	if res != nil && res.CorrelationID != "" {
		h.msg.CorrelationID = res.CorrelationID
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if res != nil && res.MetaETag != "" {
		h.msg.MetaETag = res.MetaETag
	}
	h.done = true
	h.nacked = true
	h.state = nil
	if h.msg.LeaseID != "" {
		h.client.leaseTokens.Delete(h.msg.LeaseID)
	}
	if h.msg.StateLeaseID != "" {
		h.client.leaseTokens.Delete(h.msg.StateLeaseID)
	}
	return nil
}

// Extend pushes the visibility timeout and lease forward.
func (h *QueueMessageHandle) Extend(ctx context.Context, extendBy time.Duration) error {
	if h == nil {
		return fmt.Errorf("lockd: nil queue handle")
	}
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		return fmt.Errorf("lockd: queue handle already closed")
	}
	req := api.ExtendRequest{
		Queue:           h.msg.Queue,
		MessageID:       h.msg.MessageID,
		LeaseID:         h.msg.LeaseID,
		FencingToken:    h.msg.FencingToken,
		MetaETag:        h.msg.MetaETag,
		ExtendBySeconds: secondsFromDuration(extendBy),
	}
	if h.state != nil {
		req.StateLeaseID = h.state.leaseID
		req.StateFencingToken = h.state.fencingToken
	}
	h.mu.Unlock()
	corr := h.msg.CorrelationID
	if corr != "" {
		ctx = WithCorrelationID(ctx, corr)
	}
	res, err := h.client.QueueExtend(ctx, req)
	if err != nil {
		return err
	}
	if res != nil && res.CorrelationID != "" {
		h.msg.CorrelationID = res.CorrelationID
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if res != nil {
		if res.MetaETag != "" {
			h.msg.MetaETag = res.MetaETag
		}
		if res.VisibilityTimeoutSeconds > 0 {
			h.msg.VisibilityTimeoutSeconds = res.VisibilityTimeoutSeconds
		}
		if res.LeaseExpiresAtUnix > 0 {
			h.msg.LeaseExpiresAtUnix = res.LeaseExpiresAtUnix
		}
		if h.state != nil && res.StateLeaseExpiresAtUnix > 0 {
			h.state.leaseExpiresAt = res.StateLeaseExpiresAtUnix
		}
	}
	return nil
}

// QueueMessage wraps a QueueMessageHandle and provides io.ReadCloser semantics with
// automatic nack-on-close when the caller forgets to ack explicitly.
type QueueMessage struct {
	handle       *QueueMessageHandle
	payload      io.ReadCloser
	payloadMu    sync.Mutex
	closeOnce    sync.Once
	closeErr     error
	onCloseDelay time.Duration
}

func newQueueMessage(handle *QueueMessageHandle, payload io.ReadCloser, onCloseDelay time.Duration) *QueueMessage {
	return &QueueMessage{handle: handle, payload: payload, onCloseDelay: onCloseDelay}
}

// SetOnCloseDelay adjusts the delay used when Close() auto-nacks the message.
func (m *QueueMessage) SetOnCloseDelay(d time.Duration) {
	if m != nil {
		m.onCloseDelay = d
	}
}

// Queue returns the queue name.
func (m *QueueMessage) Queue() string {
	if m == nil || m.handle == nil {
		return ""
	}
	return m.handle.Queue()
}

// MessageID returns the message identifier.
func (m *QueueMessage) MessageID() string {
	if m == nil || m.handle == nil {
		return ""
	}
	return m.handle.MessageID()
}

// Attempts returns the recorded delivery attempts.
func (m *QueueMessage) Attempts() int {
	if m == nil || m.handle == nil {
		return 0
	}
	return m.handle.Attempts()
}

// MaxAttempts returns the configured maximum delivery attempts.
func (m *QueueMessage) MaxAttempts() int {
	if m == nil || m.handle == nil {
		return 0
	}
	return m.handle.MaxAttempts()
}

// NotVisibleUntil reports when the message will become visible again.
func (m *QueueMessage) NotVisibleUntil() time.Time {
	if m == nil || m.handle == nil {
		return time.Time{}
	}
	return m.handle.NotVisibleUntil()
}

// VisibilityTimeout returns the current visibility timeout.
func (m *QueueMessage) VisibilityTimeout() time.Duration {
	if m == nil || m.handle == nil {
		return 0
	}
	return m.handle.VisibilityTimeout()
}

// Cursor returns the next-cursor associated with the dequeue response.
func (m *QueueMessage) Cursor() string {
	if m == nil || m.handle == nil {
		return ""
	}
	return m.handle.Cursor()
}

// LeaseID exposes the underlying lease identifier.
func (m *QueueMessage) LeaseID() string {
	if m == nil || m.handle == nil {
		return ""
	}
	return m.handle.LeaseID()
}

// LeaseExpiresAt returns when the lease currently expires.
func (m *QueueMessage) LeaseExpiresAt() int64 {
	if m == nil || m.handle == nil {
		return 0
	}
	return m.handle.LeaseExpiresAt()
}

// FencingToken returns the monotonic fencing token issued with the lease.
func (m *QueueMessage) FencingToken() int64 {
	if m == nil || m.handle == nil {
		return 0
	}
	return m.handle.FencingToken()
}

// MetaETag returns the metadata ETag currently associated with the message.
func (m *QueueMessage) MetaETag() string {
	if m == nil || m.handle == nil {
		return ""
	}
	return m.handle.MetaETag()
}

// CorrelationID returns the lifecycle correlation identifier for the message.
func (m *QueueMessage) CorrelationID() string {
	if m == nil || m.handle == nil {
		return ""
	}
	return m.handle.CorrelationID()
}

// StateHandle exposes the workflow state handle when using DequeueWithState.
func (m *QueueMessage) StateHandle() *QueueStateHandle {
	if m == nil || m.handle == nil {
		return nil
	}
	return m.handle.StateHandle()
}

// ContentType returns the payload content type.
func (m *QueueMessage) ContentType() string {
	if m == nil || m.handle == nil {
		return ""
	}
	return m.handle.ContentType()
}

// PayloadSize reports the payload size in bytes when known.
func (m *QueueMessage) PayloadSize() int64 {
	if m == nil || m.handle == nil {
		return 0
	}
	return m.handle.PayloadSize()
}

func (m *QueueMessage) ensurePayload() (io.ReadCloser, error) {
	m.payloadMu.Lock()
	if m.payload != nil {
		reader := m.payload
		m.payloadMu.Unlock()
		return reader, nil
	}
	m.payloadMu.Unlock()

	m.handle.payloadMu.Lock()
	closed := m.handle.payloadClosed
	m.handle.payloadMu.Unlock()
	if closed {
		return nil, nil
	}
	reader, err := m.handle.PayloadReader()
	if err != nil {
		return nil, err
	}
	m.payloadMu.Lock()
	if m.payload == nil {
		m.payload = reader
	} else {
		// another goroutine won the race; reuse existing reader.
		_ = reader.Close()
		reader = m.payload
	}
	m.payloadMu.Unlock()
	return reader, nil
}

// Read streams bytes from the message payload.
func (m *QueueMessage) Read(p []byte) (int, error) {
	if m == nil {
		return 0, fmt.Errorf("lockd: nil queue message")
	}
	reader, err := m.ensurePayload()
	if err != nil {
		return 0, err
	}
	if reader == nil {
		return 0, io.EOF
	}
	return reader.Read(p)
}

// PayloadReader returns an independent ReadCloser view over the payload.
func (m *QueueMessage) PayloadReader() (io.ReadCloser, error) {
	reader, err := m.ensurePayload()
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return reader, nil
}

// WritePayloadTo streams the payload into w.
func (m *QueueMessage) WritePayloadTo(w io.Writer) (int64, error) {
	return io.Copy(w, m)
}

// DecodePayloadJSON decodes the payload JSON into v.
func (m *QueueMessage) DecodePayloadJSON(v any) error {
	decoder := json.NewDecoder(io.NopCloser(m))
	return decoder.Decode(v)
}

// ClosePayload releases the underlying payload stream without altering the lease state.
func (m *QueueMessage) ClosePayload() error {
	if m == nil {
		return nil
	}
	m.payloadMu.Lock()
	reader := m.payload
	m.payload = nil
	m.payloadMu.Unlock()
	if reader != nil {
		_ = reader.Close()
	}
	return m.handle.ClosePayload()
}

// Close releases underlying resources and auto-nacks the message when it has not been acked or nacked explicitly.
func (m *QueueMessage) Close() error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		if err := m.ClosePayload(); err != nil && m.closeErr == nil {
			m.closeErr = err
		}
		m.handle.mu.Lock()
		acked := m.handle.acked
		nacked := m.handle.nacked
		m.handle.mu.Unlock()
		if acked || nacked {
			return
		}
		ctx, cancel := m.handle.client.requestContextNoTimeout(context.Background())
		defer cancel()
		if err := m.handle.Nack(ctx, m.onCloseDelay, nil); err != nil && m.closeErr == nil {
			m.closeErr = err
		}
	})
	return m.closeErr
}

// Ack removes the message from the queue.
func (m *QueueMessage) Ack(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("lockd: nil queue message")
	}
	if err := m.handle.Ack(ctx); err != nil {
		return err
	}
	return nil
}

// Nack releases the lease and requeues the message.
func (m *QueueMessage) Nack(ctx context.Context, delay time.Duration, lastErr any) error {
	if m == nil {
		return fmt.Errorf("lockd: nil queue message")
	}
	return m.handle.Nack(ctx, delay, lastErr)
}

// Extend pushes the lease and visibility timeout forward.
func (m *QueueMessage) Extend(ctx context.Context, extendBy time.Duration) error {
	if m == nil {
		return fmt.Errorf("lockd: nil queue message")
	}
	return m.handle.Extend(ctx, extendBy)
}

// Queue returns the queue name.
func (s *QueueStateHandle) Queue() string {
	if s == nil {
		return ""
	}
	return s.queue
}

// MessageID returns the associated message ID.
func (s *QueueStateHandle) MessageID() string {
	if s == nil {
		return ""
	}
	return s.messageID
}

// LeaseID returns the workflow state lease identifier.
func (s *QueueStateHandle) LeaseID() string {
	if s == nil {
		return ""
	}
	return s.leaseID
}

// FencingToken returns the workflow state fencing token.
func (s *QueueStateHandle) FencingToken() int64 {
	if s == nil {
		return 0
	}
	return s.fencingToken
}

// LeaseExpiresAt returns the unix timestamp when the state lease expires.
func (s *QueueStateHandle) LeaseExpiresAt() int64 {
	if s == nil {
		return 0
	}
	return s.leaseExpiresAt
}

// ETag returns the workflow state ETag.
func (s *QueueStateHandle) ETag() string {
	if s == nil {
		return ""
	}
	return s.stateETag
}

// CorrelationID returns the message correlation identifier associated with the state lease.
func (s *QueueStateHandle) CorrelationID() string {
	if s == nil {
		return ""
	}
	return s.correlationID
}

const headerFencingToken = "X-Fencing-Token"
const headerShutdownImminent = "Shutdown-Imminent"
const headerCorrelationID = "X-Correlation-Id"

// ErrMissingFencingToken is returned when an operation needs a fencing token but none was found.
var ErrMissingFencingToken = errors.New("lockd: fencing token required")

var (
	acquireRandMu sync.Mutex
	acquireRand   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

const defaultEndpointPort = "9341"

// ParseEndpoints splits a comma-separated server list and normalizes each
// endpoint, applying default schemes based on whether mTLS is disabled.
func ParseEndpoints(raw string, disableMTLS bool) ([]string, error) {
	parts := strings.Split(raw, ",")
	return parseEndpointSlice(parts, disableMTLS)
}

func parseEndpointSlice(parts []string, disableMTLS bool) ([]string, error) {
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		ep := strings.TrimSpace(part)
		if ep == "" {
			continue
		}
		normalized, err := normalizeEndpoint(ep, disableMTLS)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, normalized)
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("lockd: no server endpoints provided")
	}
	return endpoints, nil
}

func normalizeEndpoint(raw string, disableMTLS bool) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("lockd: empty endpoint")
	}
	if strings.HasPrefix(trimmed, "unix://") {
		return trimmed, nil
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return "", fmt.Errorf("lockd: parse endpoint %q: %w", trimmed, err)
		}
		return ensurePort(u, defaultEndpointPort), nil
	}
	scheme := "https://"
	if disableMTLS {
		scheme = "http://"
	}
	u, err := url.Parse(scheme + trimmed)
	if err != nil {
		return "", fmt.Errorf("lockd: parse endpoint %q: %w", trimmed, err)
	}
	return ensurePort(u, defaultEndpointPort), nil
}

func ensurePort(u *url.URL, defaultPort string) string {
	host := u.Hostname()
	if host == "" {
		return strings.TrimRight(u.String(), "/")
	}
	port := u.Port()
	if port == "" {
		port = defaultPort
	}
	if strings.Contains(host, ":") {
		u.Host = "[" + strings.Trim(host, "[]") + "]:" + port
	} else {
		u.Host = net.JoinHostPort(host, port)
	}
	return strings.TrimRight(u.String(), "/")
}

func makeBodyFactory(body io.Reader) (func() (io.Reader, error), error) {
	if body == nil {
		return func() (io.Reader, error) { return http.NoBody, nil }, nil
	}
	if rs, ok := body.(io.ReadSeeker); ok {
		return func() (io.Reader, error) {
			_, err := rs.Seek(0, io.SeekStart)
			return rs, err
		}, nil
	}
	if buf, ok := body.(*bytes.Buffer); ok {
		data := append([]byte(nil), buf.Bytes()...)
		return func() (io.Reader, error) { return bytes.NewReader(data), nil }, nil
	}
	if closer, ok := body.(io.Closer); ok {
		defer closer.Close()
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	return func() (io.Reader, error) { return bytes.NewReader(data), nil }, nil
}

func hasKey(keyvals []any, target string) bool {
	for i := 0; i+1 < len(keyvals); i += 2 {
		if key, ok := keyvals[i].(string); ok && key == target {
			return true
		}
	}
	return false
}

func (c *Client) enrichKeyvals(ctx context.Context, keyvals []any) []any {
	if ctx == nil {
		return keyvals
	}
	cid := CorrelationIDFromContext(ctx)
	if cid == "" || hasKey(keyvals, "cid") {
		return keyvals
	}
	enriched := append([]any(nil), keyvals...)
	enriched = append(enriched, "cid", cid)
	return enriched
}

func (c *Client) logTraceCtx(ctx context.Context, msg string, keyvals ...any) {
	if c.logger == nil {
		return
	}
	keyvals = c.enrichKeyvals(ctx, keyvals)
	c.logger.Trace(msg, keyvals...)
}

func (c *Client) logDebugCtx(ctx context.Context, msg string, keyvals ...any) {
	if c.logger == nil {
		return
	}
	keyvals = c.enrichKeyvals(ctx, keyvals)
	c.logger.Debug(msg, keyvals...)
}

func (c *Client) logInfoCtx(ctx context.Context, msg string, keyvals ...any) {
	if c.logger == nil {
		return
	}
	keyvals = c.enrichKeyvals(ctx, keyvals)
	c.logger.Info(msg, keyvals...)
}

func (c *Client) logWarnCtx(ctx context.Context, msg string, keyvals ...any) {
	if c.logger == nil {
		return
	}
	keyvals = c.enrichKeyvals(ctx, keyvals)
	c.logger.Warn(msg, keyvals...)
}

func (c *Client) logErrorCtx(ctx context.Context, msg string, keyvals ...any) {
	if c.logger == nil {
		return
	}
	keyvals = c.enrichKeyvals(ctx, keyvals)
	c.logger.Error(msg, keyvals...)
}

func (c *Client) logTrace(msg string, keyvals ...any) {
	c.logTraceCtx(nil, msg, keyvals...)
}

func (c *Client) logDebug(msg string, keyvals ...any) {
	c.logDebugCtx(nil, msg, keyvals...)
}

func (c *Client) logInfo(msg string, keyvals ...any) {
	c.logInfoCtx(nil, msg, keyvals...)
}

func (c *Client) logWarn(msg string, keyvals ...any) {
	c.logWarnCtx(nil, msg, keyvals...)
}

func (c *Client) logError(msg string, keyvals ...any) {
	c.logErrorCtx(nil, msg, keyvals...)
}
func prepareHTTPResources(endpoints []string) (*http.Client, []string, error) {
	if len(endpoints) == 0 {
		return nil, nil, fmt.Errorf("lockd: no endpoints provided")
	}
	normalized := make([]string, len(endpoints))
	var httpClient *http.Client
	multi := len(endpoints) > 1
	for i, ep := range endpoints {
		if multi && strings.HasPrefix(ep, "unix://") {
			return nil, nil, fmt.Errorf("lockd: unix endpoints cannot be combined with others")
		}
		cli, base, err := buildHTTPClient(ep)
		if err != nil {
			return nil, nil, err
		}
		if i == 0 {
			httpClient = cli
		}
		normalized[i] = strings.TrimRight(base, "/")
	}
	return httpClient, normalized, nil
}

func (c *Client) initialize(endpoints []string) error {
	httpClient, normalized, err := prepareHTTPResources(endpoints)
	if err != nil {
		return err
	}
	if c.httpClient == nil {
		c.httpClient = httpClient
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}
	if c.httpClient.Transport == nil {
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			c.httpClient.Transport = base.Clone()
		}
	}
	if c.httpClient.Timeout != 0 {
		c.httpClient.Timeout = 0
	}
	if c.httpTimeout <= 0 {
		c.httpTimeout = defaultHTTPTimeout
	}
	if len(normalized) > 1 && c.httpTimeout == defaultHTTPTimeout {
		c.httpTimeout = 3 * time.Second
	}
	if c.closeTimeout <= 0 {
		c.closeTimeout = defaultCloseTimeout
	}
	if c.keepAliveTimeout <= 0 {
		c.keepAliveTimeout = defaultKeepAliveTimeout
	}
	if c.keepAliveTimeout > c.closeTimeout {
		c.keepAliveTimeout = c.closeTimeout
	}
	if c.forUpdateTimeout <= 0 {
		c.forUpdateTimeout = defaultForUpdateTimeout
	}
	if !c.httpTraceEnabled {
		if envBool(envClientHTTPTrace) || envBool(envClientHTTPTraceAlt) {
			c.httpTraceEnabled = true
		}
	}
	c.endpoints = normalized
	c.lastEndpoint = normalized[0]
	c.logInfo("client.init", "endpoints", normalized)
	return nil
}

// Default client tuning knobs exposed for callers that want to mirror lockd's defaults.
const (
	DefaultHTTPTimeout           = 15 * time.Second
	DefaultCloseTimeout          = 5 * time.Second
	DefaultKeepAliveTimeout      = 5 * time.Second
	DefaultForUpdateTimeout      = 15 * time.Minute
	DefaultAcquireBaseDelay      = time.Second
	DefaultAcquireMaxDelay       = 5 * time.Second
	DefaultAcquireMultiplier     = 1.2
	DefaultAcquireJitter         = 100 * time.Millisecond
	DefaultFailureRetries        = 5
	DefaultAcquireFailureRetries = DefaultFailureRetries
)

const (
	defaultHTTPTimeout      = DefaultHTTPTimeout
	defaultCloseTimeout     = DefaultCloseTimeout
	defaultKeepAliveTimeout = DefaultKeepAliveTimeout
	defaultForUpdateTimeout = DefaultForUpdateTimeout
)

// BlockWaitForever causes Acquire to wait indefinitely for a lease. BlockNoWait skips waiting entirely.
const (
	BlockWaitForever int64 = 0
	BlockNoWait      int64 = api.BlockNoWait
)

func secondsFromDuration(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	seconds := d.Seconds()
	if seconds <= 0 {
		return 0
	}
	return int64(math.Ceil(seconds))
}

// LeaseSession models an active lease returned by Acquire.
type LeaseSession struct {
	client *Client
	api.AcquireResponse
	fencingToken   string
	mu             sync.Mutex
	closed         bool
	closeErr       error
	lastSnapshot   *StateSnapshot
	correlationID  string
	endpoint       string
	drainOnce      sync.Once
	drainTriggered atomic.Bool
}

// AcquireForUpdateHandler is invoked while a lease is held. The provided context
// is canceled if the client loses the lease or keepalive fails.
type AcquireForUpdateHandler func(context.Context, *AcquireForUpdateContext) error

// AcquireForUpdateContext exposes the active lease session and the snapshot that
// was read before the handler executed.
type AcquireForUpdateContext struct {
	Session *LeaseSession
	State   *StateSnapshot
}

func (a *AcquireForUpdateContext) session() (*LeaseSession, error) {
	if a == nil || a.Session == nil {
		return nil, fmt.Errorf("lockd: acquire-for-update session closed")
	}
	return a.Session, nil
}

// Update streams a new JSON document via Update, preserving the lease.
func (a *AcquireForUpdateContext) Update(ctx context.Context, body io.Reader) (*UpdateResult, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.Update(ctx, body)
}

// UpdateBytes is a convenience wrapper over Update that accepts a byte slice.
func (a *AcquireForUpdateContext) UpdateBytes(ctx context.Context, body []byte) (*UpdateResult, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.UpdateBytes(ctx, body)
}

// UpdateWithOptions allows callers to override conditional headers for the update.
func (a *AcquireForUpdateContext) UpdateWithOptions(ctx context.Context, body io.Reader, opts UpdateOptions) (*UpdateResult, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.UpdateWithOptions(ctx, body, opts)
}

// KeepAlive extends the lease TTL.
func (a *AcquireForUpdateContext) KeepAlive(ctx context.Context, ttl time.Duration) (*api.KeepAliveResponse, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.KeepAlive(ctx, ttl)
}

// Load unmarshals the latest state into v.
func (a *AcquireForUpdateContext) Load(ctx context.Context, v any) error {
	sess, err := a.session()
	if err != nil {
		return err
	}
	return sess.Load(ctx, v)
}

// Save marshals and updates the state with the supplied value.
func (a *AcquireForUpdateContext) Save(ctx context.Context, v any) error {
	sess, err := a.session()
	if err != nil {
		return err
	}
	return sess.Save(ctx, v)
}

// Remove deletes the current state while the handler holds the lease.
func (a *AcquireForUpdateContext) Remove(ctx context.Context) (*api.RemoveResponse, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.Remove(ctx)
}

// RemoveWithOptions deletes the state while allowing conditional overrides.
func (a *AcquireForUpdateContext) RemoveWithOptions(ctx context.Context, opts RemoveOptions) (*api.RemoveResponse, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.RemoveWithOptions(ctx, opts)
}

// StateSnapshot represents the current JSON state and metadata for a lease.
type StateSnapshot struct {
	Reader   io.ReadCloser
	ETag     string
	Version  int64
	HasState bool
}

// Close releases the underlying reader if present.
func (s *StateSnapshot) Close() error {
	if s == nil || s.Reader == nil {
		return nil
	}
	return s.Reader.Close()
}

// Bytes returns the raw JSON payload.
func (s *StateSnapshot) Bytes() ([]byte, error) {
	if s == nil || s.Reader == nil {
		return nil, nil
	}
	return io.ReadAll(s.Reader)
}

// Decode unmarshals the JSON payload into v.
func (s *StateSnapshot) Decode(v any) error {
	if s == nil || s.Reader == nil {
		return nil
	}
	return json.NewDecoder(s.Reader).Decode(v)
}

func newLeaseSession(c *Client, resp api.AcquireResponse, token string, endpoint string) *LeaseSession {
	session := &LeaseSession{
		client:          c,
		AcquireResponse: resp,
		fencingToken:    token,
		correlationID:   resp.CorrelationID,
		endpoint:        endpoint,
	}
	c.registerSession(session)
	return session
}

func (s *LeaseSession) fencing() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fencingToken != "" {
		return s.fencingToken
	}
	if token, err := s.client.fencingToken(s.LeaseID, ""); err == nil {
		s.fencingToken = token
		return token
	}
	return ""
}

func (s *LeaseSession) setFencing(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fencingToken = token
	if v, err := strconv.ParseInt(token, 10, 64); err == nil {
		s.FencingToken = v
	}
}

func (s *LeaseSession) correlation() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.correlationID != "" {
		return s.correlationID
	}
	if s.CorrelationID != "" {
		s.correlationID = s.CorrelationID
	}
	return s.correlationID
}

func (s *LeaseSession) setCorrelation(id string) {
	if id == "" {
		return
	}
	s.mu.Lock()
	s.correlationID = id
	s.CorrelationID = id
	s.mu.Unlock()
}

func (s *LeaseSession) closeContext() (context.Context, context.CancelFunc) {
	return s.client.closeContext()
}

// FencingTokenString returns the current fencing token associated with the lease.
func (s *LeaseSession) FencingTokenString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fencingToken != "" {
		return s.fencingToken
	}
	if token, err := s.client.fencingToken(s.LeaseID, ""); err == nil {
		s.fencingToken = token
		if v, err := strconv.ParseInt(token, 10, 64); err == nil {
			s.FencingToken = v
		}
		return token
	}
	return ""
}

// Update streams new JSON state for the session's key while preserving the lease.
func (s *LeaseSession) Update(ctx context.Context, body io.Reader) (*UpdateResult, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	opts := UpdateOptions{
		IfETag: s.StateETag,
	}
	if s.Version > 0 {
		opts.IfVersion = strconv.FormatInt(s.Version, 10)
	}
	opts.FencingToken = s.fencing()
	return s.applyUpdate(ctx, body, opts)
}

// UpdateBytes is a convenience wrapper around Update that accepts an in-memory payload.
func (s *LeaseSession) UpdateBytes(ctx context.Context, body []byte) (*UpdateResult, error) {
	return s.Update(ctx, bytes.NewReader(body))
}

// UpdateWithOptions allows callers to override conditional metadata.
func (s *LeaseSession) UpdateWithOptions(ctx context.Context, body io.Reader, opts UpdateOptions) (*UpdateResult, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	if opts.IfETag == "" {
		opts.IfETag = s.StateETag
	}
	if opts.IfVersion == "" && s.Version > 0 {
		opts.IfVersion = strconv.FormatInt(s.Version, 10)
	}
	if opts.FencingToken == "" {
		opts.FencingToken = s.fencing()
	}
	return s.applyUpdate(ctx, body, opts)
}

func (s *LeaseSession) applyUpdate(ctx context.Context, body io.Reader, opts UpdateOptions) (*UpdateResult, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	res, err := s.client.Update(ctx, s.Key, s.LeaseID, body, opts)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.Version = res.NewVersion
	s.StateETag = res.NewStateETag
	if token, err := s.client.fencingToken(s.LeaseID, ""); err == nil {
		s.fencingToken = token
		if v, err := strconv.ParseInt(token, 10, 64); err == nil {
			s.FencingToken = v
		}
	}
	s.mu.Unlock()
	return res, nil
}

// Remove deletes the current state while holding the lease, enforcing the
// cached version and etag when present.
func (s *LeaseSession) Remove(ctx context.Context) (*api.RemoveResponse, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	opts := RemoveOptions{
		IfETag: s.StateETag,
	}
	if s.Version > 0 {
		opts.IfVersion = strconv.FormatInt(s.Version, 10)
	}
	opts.FencingToken = s.fencing()
	return s.applyRemove(ctx, opts)
}

// RemoveWithOptions allows callers to override conditional metadata for delete.
func (s *LeaseSession) RemoveWithOptions(ctx context.Context, opts RemoveOptions) (*api.RemoveResponse, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	if opts.IfETag == "" {
		opts.IfETag = s.StateETag
	}
	if opts.IfVersion == "" && s.Version > 0 {
		opts.IfVersion = strconv.FormatInt(s.Version, 10)
	}
	if opts.FencingToken == "" {
		opts.FencingToken = s.fencing()
	}
	return s.applyRemove(ctx, opts)
}

func (s *LeaseSession) applyRemove(ctx context.Context, opts RemoveOptions) (*api.RemoveResponse, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	res, err := s.client.Remove(ctx, s.Key, s.LeaseID, opts)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if res != nil && res.NewVersion > 0 {
		s.Version = res.NewVersion
	}
	s.StateETag = ""
	s.lastSnapshot = nil
	if token, err := s.client.fencingToken(s.LeaseID, ""); err == nil {
		s.fencingToken = token
		if v, err := strconv.ParseInt(token, 10, 64); err == nil {
			s.FencingToken = v
		}
	}
	s.mu.Unlock()
	return res, nil
}

// Get refreshes the current state snapshot using the active lease.
func (s *LeaseSession) Get(ctx context.Context) (*StateSnapshot, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	reader, etag, version, err := s.client.Get(ctx, s.Key, s.LeaseID)
	if err != nil {
		return nil, err
	}
	snap := &StateSnapshot{Reader: reader, ETag: etag, HasState: reader != nil || etag != ""}
	if version != "" {
		if v, parseErr := strconv.ParseInt(version, 10, 64); parseErr == nil {
			snap.Version = v
		}
		if snap.Version > 0 {
			snap.HasState = true
		}
	}
	s.mu.Lock()
	if etag != "" {
		s.StateETag = etag
	}
	if snap.Version > 0 {
		s.Version = snap.Version
	}
	if token, errToken := s.client.fencingToken(s.LeaseID, ""); errToken == nil {
		s.fencingToken = token
		if v, errParse := strconv.ParseInt(token, 10, 64); errParse == nil {
			s.FencingToken = v
		}
	}
	s.lastSnapshot = snap
	s.mu.Unlock()
	return snap, nil
}

// GetBytes returns the current state blob as a byte slice.
func (s *LeaseSession) GetBytes(ctx context.Context) ([]byte, error) {
	snap, err := s.Get(ctx)
	if err != nil {
		return nil, err
	}
	if snap == nil || !snap.HasState {
		return nil, nil
	}
	defer snap.Close()
	return snap.Bytes()
}

// Load reads the current state into v. When no state exists, v is untouched.
func (s *LeaseSession) Load(ctx context.Context, v any) error {
	if v == nil {
		return nil
	}
	snap, err := s.Get(ctx)
	if err != nil {
		return err
	}
	if snap == nil || !snap.HasState {
		return nil
	}
	defer snap.Close()
	return snap.Decode(v)
}

// Save serialises v as JSON and updates the state.
func (s *LeaseSession) Save(ctx context.Context, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = s.UpdateBytes(ctx, data)
	return err
}

// KeepAlive extends the lease TTL without altering the stored state.
func (s *LeaseSession) KeepAlive(ctx context.Context, ttl time.Duration) (*api.KeepAliveResponse, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	req := api.KeepAliveRequest{
		Key:        s.Key,
		LeaseID:    s.LeaseID,
		TTLSeconds: int64(ttl.Seconds()),
	}
	resp, err := s.client.KeepAlive(ctx, req)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.ExpiresAt = resp.ExpiresAt
	s.mu.Unlock()
	return resp, nil
}

// Release relinquishes the lease early; it is safe to call multiple times.
func (s *LeaseSession) Release(ctx context.Context) error {
	ctx = WithCorrelationID(ctx, s.correlation())
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	resp, err := s.client.releaseInternal(ctx, api.ReleaseRequest{
		Key:     s.Key,
		LeaseID: s.LeaseID,
	}, s.endpoint)
	if err != nil {
		s.mu.Lock()
		s.closeErr = err
		s.mu.Unlock()
		return err
	}
	if resp != nil && !resp.Released {
		err := fmt.Errorf("lockd: release denied")
		s.mu.Lock()
		s.closeErr = err
		s.mu.Unlock()
		return err
	}
	s.mu.Lock()
	s.closed = true
	s.closeErr = nil
	s.mu.Unlock()
	s.client.unregisterSession(s.LeaseID)
	return nil
}

func (s *LeaseSession) triggerDrainRelease(reason string) {
	if s == nil || s.client == nil {
		return
	}
	if !s.client.drainAwareShutdown {
		return
	}
	if !s.drainTriggered.CompareAndSwap(false, true) {
		return
	}
	key := s.Key
	leaseID := s.LeaseID
	endpoint := s.endpoint
	fields := []any{"key", key, "lease_id", leaseID, "endpoint", endpoint, "reason", reason}
	s.client.logInfoCtx(context.Background(), "client.shutdown.auto_release.start", fields...)
	go func() {
		ctx, cancel := s.closeContext()
		defer cancel()
		if err := s.Release(ctx); err != nil {
			s.client.logWarnCtx(ctx, "client.shutdown.auto_release.error", append(fields, "error", err)...)
			return
		}
		s.client.logInfoCtx(ctx, "client.shutdown.auto_release.complete", fields...)
	}()
}

// Close is equivalent to Release using a background context.
func (s *LeaseSession) Close() error {
	ctx, cancel := s.closeContext()
	defer cancel()
	return s.Release(ctx)
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelReadCloser) Close() error {
	var err error
	c.once.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		if c.ReadCloser != nil {
			err = c.ReadCloser.Close()
		}
	})
	return err
}

// UpdateResult captures the response from Update.
type UpdateResult struct {
	NewVersion   int64  `json:"new_version"`
	NewStateETag string `json:"new_state_etag"`
	BytesWritten int64  `json:"bytes"`
}

// UpdateOptions controls conditional update semantics.
type UpdateOptions struct {
	IfETag       string
	IfVersion    string
	FencingToken string
}

// RemoveOptions controls conditional delete semantics.
type RemoveOptions struct {
	IfETag       string
	IfVersion    string
	FencingToken string
}

type correlationTransport struct {
	base http.RoundTripper
	id   string
}

func (t *correlationTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	if t.id != "" {
		req.Header.Set(headerCorrelationID, t.id)
	}
	return t.baseTransport().RoundTrip(req)
}

func (t *correlationTransport) baseTransport() http.RoundTripper {
	if t.base != nil {
		return t.base
	}
	return http.DefaultTransport
}

// WithCorrelationTransport wraps base with a RoundTripper that overwrites the
// X-Correlation-Id header on every request. Invalid identifiers are ignored.
func WithCorrelationTransport(base http.RoundTripper, id string) http.RoundTripper {
	normalized, ok := NormalizeCorrelationID(id)
	if !ok {
		normalized = ""
	}
	return &correlationTransport{base: base, id: normalized}
}

// WithCorrelationHTTPClient returns a shallow copy of cli (or a new client when
// cli is nil) with a transport that ensures X-Correlation-Id is set on all
// requests. The original client is left untouched.
func WithCorrelationHTTPClient(cli *http.Client, id string) *http.Client {
	var base http.Client
	if cli != nil {
		base = *cli
	}
	base.Transport = WithCorrelationTransport(base.Transport, id)
	return &base
}

// CorrelationIDFromResponse reads the X-Correlation-Id header from resp.
func CorrelationIDFromResponse(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	return resp.Header.Get(headerCorrelationID)
}

// Client is a convenience wrapper around the lockd HTTP API.
type Client struct {
	endpoints          []string
	lastEndpoint       string
	shuffleEndpoints   bool
	httpClient         *http.Client
	httpTraceEnabled   bool
	leaseTokens        sync.Map
	sessions           sync.Map
	httpTimeout        time.Duration
	closeTimeout       time.Duration
	keepAliveTimeout   time.Duration
	forUpdateTimeout   time.Duration
	disableMTLS        bool
	logger             pslog.Base
	failureRetries     int
	drainAwareShutdown bool
	shutdownNotified   atomic.Bool

	closeOnce sync.Once
}

// RegisterLeaseToken stores a lease -> fencing token mapping for subsequent
// requests. This is useful when the token is obtained out-of-band (for example
// via environment variables between CLI invocations).
func (c *Client) RegisterLeaseToken(leaseID, token string) {
	if leaseID == "" || token == "" {
		return
	}
	c.leaseTokens.Store(leaseID, token)
}

func (c *Client) registerSession(sess *LeaseSession) {
	if c == nil || sess == nil || sess.LeaseID == "" {
		return
	}
	c.sessions.Store(sess.LeaseID, sess)
}

func (c *Client) unregisterSession(leaseID string) {
	if leaseID == "" {
		return
	}
	c.sessions.Delete(leaseID)
}

func (c *Client) fencingToken(leaseID, override string) (string, error) {
	if override != "" {
		return override, nil
	}
	if leaseID == "" {
		return "", ErrMissingFencingToken
	}
	if v, ok := c.leaseTokens.Load(leaseID); ok {
		if token, ok := v.(string); ok && token != "" {
			return token, nil
		}
	}
	return "", ErrMissingFencingToken
}

func (c *Client) closeContext() (context.Context, context.CancelFunc) {
	if c.closeTimeout > 0 {
		return context.WithTimeout(context.Background(), c.closeTimeout)
	}
	return context.WithCancel(context.Background())
}

func (c *Client) keepAliveContext(parent context.Context) (context.Context, context.CancelFunc) {
	if c.keepAliveTimeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, c.keepAliveTimeout)
}

func (c *Client) applyCorrelationHeader(ctx context.Context, req *http.Request, explicit string) {
	if req == nil {
		return
	}
	if normalized, ok := NormalizeCorrelationID(explicit); ok {
		req.Header.Set(headerCorrelationID, normalized)
		return
	}
	if id := CorrelationIDFromContext(ctx); id != "" {
		if normalized, ok := NormalizeCorrelationID(id); ok {
			req.Header.Set(headerCorrelationID, normalized)
		}
	}
}

func (c *Client) allowFailure(count int) bool {
	if c.failureRetries < 0 {
		return true
	}
	return count <= c.failureRetries
}

func (c *Client) requestContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if c.httpTimeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, c.httpTimeout)
}

func (c *Client) requestContextNoTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return parent, func() {}
}

type idleCloser interface {
	CloseIdleConnections()
}

func (c *Client) closeIdleConnections() {
	if c == nil || c.httpClient == nil {
		return
	}
	if transport := c.httpClient.Transport; transport != nil {
		if closer, ok := transport.(idleCloser); ok {
			closer.CloseIdleConnections()
		}
		return
	}
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		base.CloseIdleConnections()
	}
}

func shouldResetConnection(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
		if errors.Is(err, context.Canceled) {
			return false
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return true
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

// Close releases any idle HTTP connections held by the client.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closeIdleConnections()
	})
	return nil
}

func (c *Client) acquireRequestContext(parent context.Context, req api.AcquireRequest) (context.Context, context.CancelFunc) {
	if req.BlockSecs == BlockNoWait {
		return c.requestContext(parent)
	}
	if parent == nil {
		parent = context.Background()
	}
	return parent, func() {}
}

type endpointRequestBuilder func(base string) (*http.Request, context.CancelFunc, error)

func (c *Client) shuffledEndpoints() []string {
	endpoints := make([]string, len(c.endpoints))
	copy(endpoints, c.endpoints)
	if len(endpoints) > 1 && c.shuffleEndpoints {
		acquireRandMu.Lock()
		shuffler := acquireRand
		shuffler.Shuffle(len(endpoints), func(i, j int) {
			endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
		})
		acquireRandMu.Unlock()
	}
	return endpoints
}

func (c *Client) newHTTPTrace(ctx context.Context, endpoint string) *httptrace.ClientTrace {
	if c == nil || !c.httpTraceEnabled {
		return nil
	}
	return &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			fields := []any{"endpoint", endpoint, "reused", info.Reused, "was_idle", info.WasIdle}
			if conn := info.Conn; conn != nil {
				if remote := conn.RemoteAddr(); remote != nil {
					fields = append(fields, "remote", remote.String())
				}
			}
			c.logTraceCtx(ctx, "client.http.trace.got_conn", fields...)
		},
		PutIdleConn: func(err error) {
			fields := []any{"endpoint", endpoint}
			if err != nil {
				fields = append(fields, "error", err)
			}
			c.logTraceCtx(ctx, "client.http.trace.put_idle", fields...)
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			fields := []any{"endpoint", endpoint}
			if info.Err != nil {
				fields = append(fields, "error", info.Err)
			}
			c.logTraceCtx(ctx, "client.http.trace.wrote_request", fields...)
		},
		GotFirstResponseByte: func() {
			c.logTraceCtx(ctx, "client.http.trace.first_byte", "endpoint", endpoint)
		},
	}
}

func (c *Client) attemptEndpoints(builder endpointRequestBuilder, preferred string) (*http.Response, context.CancelFunc, string, error) {
	if len(c.endpoints) == 0 {
		return nil, nil, "", fmt.Errorf("lockd: no endpoints configured")
	}
	order := c.shuffledEndpoints()
	if preferred != "" {
		for i, base := range order {
			if base == preferred {
				if i != 0 {
					order[0], order[i] = order[i], order[0]
				}
				break
			}
		}
	}
	c.logTrace("client.http.order", "endpoints", order)
	var lastErr error
	var lastCID string
	for attempt, base := range order {
		req, cancel, err := builder(base)
		if err != nil {
			if cancel != nil {
				cancel()
			}
			c.logDebug("client.http.builder_error", "endpoint", base, "error", err)
			return nil, nil, "", err
		}
		cid := req.Header.Get(headerCorrelationID)
		if cid == "" {
			cid = CorrelationIDFromContext(req.Context())
		}
		lastCID = cid
		attemptKV := []any{"endpoint", base, "attempt", attempt + 1, "total", len(order)}
		if cid != "" {
			attemptKV = append(attemptKV, "cid", cid)
		}
		start := time.Now()
		c.logTraceCtx(req.Context(), "client.http.attempt", attemptKV...)
		if tr := c.newHTTPTrace(req.Context(), base); tr != nil {
			req = req.WithContext(httptrace.WithClientTrace(req.Context(), tr))
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			if cancel != nil {
				cancel()
			}
			if shouldResetConnection(err) {
				c.closeIdleConnections()
			}
			errorKV := append([]any{}, attemptKV...)
			errorKV = append(errorKV, "error", err, "duration", time.Since(start))
			c.logTraceCtx(req.Context(), "client.http.error", errorKV...)
			lastErr = err
			continue
		}
		successKV := append([]any{}, attemptKV...)
		successKV = append(successKV, "status", resp.StatusCode, "duration", time.Since(start))
		c.logTraceCtx(req.Context(), "client.http.success", successKV...)
		c.lastEndpoint = base
		return resp, cancel, base, nil
	}
	orderStr := strings.Join(order, ",")
	if lastErr == nil {
		lastErr = fmt.Errorf("lockd: all endpoints unreachable (attempted %s)", orderStr)
	} else {
		lastErr = fmt.Errorf("lockd: all endpoints unreachable (attempted %s): %w", orderStr, lastErr)
	}
	debugKV := []any{"order", order, "error", lastErr}
	if lastCID != "" {
		debugKV = append(debugKV, "cid", lastCID)
	}
	c.logDebug("client.http.unreachable", debugKV...)
	return nil, nil, "", lastErr
}

func (c *Client) forUpdateContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if c.forUpdateTimeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, c.forUpdateTimeout)
}

// Option customises client construction.
type Option func(*Client)

// WithHTTPClient supplies a custom HTTP client.
func WithHTTPClient(cli *http.Client) Option {
	return func(c *Client) {
		if cli != nil {
			c.httpClient = cli
		}
	}
}

// WithLogger supplies a logger for client diagnostics. Passing nil disables logging.
func WithLogger(logger pslog.Base) Option {
	return func(c *Client) {
		if logger == nil {
			c.logger = nil
			return
		}
		if full, ok := logger.(pslog.Logger); ok {
			c.logger = loggingutil.WithSubsystem(full, "client.sdk")
			return
		}
		c.logger = logger
	}
}

// WithEndpointShuffle toggles random shuffling of endpoints before each request.
// When disabled, endpoints are tried in the order provided.
func WithEndpointShuffle(enabled bool) Option {
	return func(c *Client) {
		c.shuffleEndpoints = enabled
	}
}

// WithDisableMTLS toggles mutual TLS expectations for scheme inference.
// When disabled (false, default), base URLs without a scheme assume HTTPS.
// When enabled (true), bare endpoints default to HTTP and TLS client
// certificates are not loaded automatically.
func WithDisableMTLS(disable bool) Option {
	return func(c *Client) {
		c.disableMTLS = disable
	}
}

// WithFailureRetries overrides how many times non-acquire operations retry on failure.
// A value <0 allows infinite retries.
func WithFailureRetries(n int) Option {
	return func(c *Client) {
		c.failureRetries = n
	}
}

// WithHTTPTimeout overrides the default HTTP client timeout.
func WithHTTPTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.httpTimeout = d
		}
	}
}

// WithCloseTimeout overrides the timeout used when auto-releasing leases during Close().
func WithCloseTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.closeTimeout = d
		}
	}
}

// WithKeepAliveTimeout overrides the timeout used for keepalive requests.
func WithKeepAliveTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.keepAliveTimeout = d
		}
	}
}

// WithDrainAwareShutdown toggles automatic lease releases when the server signals shutdown.
func WithDrainAwareShutdown(enabled bool) Option {
	return func(c *Client) {
		c.drainAwareShutdown = enabled
	}
}

// WithForUpdateTimeout overrides the timeout used for acquire-for-update requests.
func WithForUpdateTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.forUpdateTimeout = d
		}
	}
}

// New creates a new client targeting baseURL (e.g. https://localhost:9341).
// Unix-domain sockets are supported via base URLs such as unix:///var/run/lockd.sock;
// ensure the server is running with mTLS disabled or supply a compatible client bundle.
// Example:
//
//	cli, err := client.New("unix:///tmp/lockd.sock")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	lease, _ := cli.Acquire(ctx, api.AcquireRequest{Key: "demo", Owner: "worker", TTLSeconds: 20})
//	defer cli.Release(ctx, api.ReleaseRequest{Key: "demo", LeaseID: lease.LeaseID})
func New(baseURL string, opts ...Option) (*Client, error) {
	trimmed := strings.TrimSpace(baseURL)
	if trimmed == "" {
		return nil, fmt.Errorf("baseURL required")
	}
	c := &Client{
		shuffleEndpoints:   true,
		httpTimeout:        defaultHTTPTimeout,
		closeTimeout:       defaultCloseTimeout,
		keepAliveTimeout:   defaultKeepAliveTimeout,
		forUpdateTimeout:   defaultForUpdateTimeout,
		disableMTLS:        false,
		failureRetries:     DefaultFailureRetries,
		drainAwareShutdown: true,
	}
	for _, opt := range opts {
		opt(c)
	}
	endpoints, err := ParseEndpoints(trimmed, c.disableMTLS)
	if err != nil {
		return nil, err
	}
	if err := c.initialize(endpoints); err != nil {
		return nil, err
	}
	return c, nil
}

// NewWithEndpoints constructs a client from a slice of server endpoints.
func NewWithEndpoints(endpoints []string, opts ...Option) (*Client, error) {
	c := &Client{
		shuffleEndpoints:   true,
		httpTimeout:        defaultHTTPTimeout,
		closeTimeout:       defaultCloseTimeout,
		keepAliveTimeout:   defaultKeepAliveTimeout,
		forUpdateTimeout:   defaultForUpdateTimeout,
		disableMTLS:        false,
		failureRetries:     DefaultFailureRetries,
		drainAwareShutdown: true,
	}
	for _, opt := range opts {
		opt(c)
	}
	normalized, err := parseEndpointSlice(endpoints, c.disableMTLS)
	if err != nil {
		return nil, err
	}
	if err := c.initialize(normalized); err != nil {
		return nil, err
	}
	return c, nil
}

// AcquireConfig controls the client-side retry and backoff behaviour for Acquire and AcquireForUpdate.
type AcquireConfig struct {
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	Multiplier     float64
	Jitter         time.Duration
	FailureRetries int
	randInt63n     func(int64) int64
}

// AcquireOption customises Acquire behaviour.
type AcquireOption func(*AcquireConfig)

// WithAcquireBackoff adjusts backoff parameters.
func WithAcquireBackoff(base, max time.Duration, multiplier float64) AcquireOption {
	return func(c *AcquireConfig) {
		if base > 0 {
			c.BaseDelay = base
		}
		if max > 0 {
			c.MaxDelay = max
		}
		if multiplier > 0 {
			c.Multiplier = multiplier
		}
	}
}

// WithAcquireJitter adjusts the jitter window applied when the retry delay reaches the cap.
// A zero duration disables jitter. When positive, the final sleep duration is randomly
// offset by +/- jitter once the cap is hit.
func WithAcquireJitter(jitter time.Duration) AcquireOption {
	return func(c *AcquireConfig) {
		if jitter >= 0 {
			c.Jitter = jitter
		}
	}
}

// WithAcquireFailureRetries overrides how many times the client retries after
// failures other than lease conflicts. A value <0 allows infinite retries.
func WithAcquireFailureRetries(n int) AcquireOption {
	return func(c *AcquireConfig) {
		c.FailureRetries = n
	}
}

func withAcquireRand(fn func(int64) int64) AcquireOption {
	return func(c *AcquireConfig) {
		if fn != nil {
			c.randInt63n = fn
		}
	}
}

func defaultAcquireConfig() AcquireConfig {
	return AcquireConfig{
		BaseDelay:      DefaultAcquireBaseDelay,
		MaxDelay:       DefaultAcquireMaxDelay,
		Multiplier:     DefaultAcquireMultiplier,
		Jitter:         DefaultAcquireJitter,
		FailureRetries: DefaultAcquireFailureRetries,
		randInt63n:     acquireRandInt63n,
	}
}

func normalizeAcquireConfig(cfg *AcquireConfig) {
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = DefaultAcquireBaseDelay
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = DefaultAcquireMaxDelay
	}
	if cfg.Multiplier <= 1.0 {
		cfg.Multiplier = DefaultAcquireMultiplier
	}
	if cfg.Jitter < 0 {
		cfg.Jitter = 0
	}
	if cfg.randInt63n == nil {
		cfg.randInt63n = rand.Int63n
	}
	if cfg.FailureRetries == 0 {
		cfg.FailureRetries = DefaultAcquireFailureRetries
	}
}

// Acquire acquires a lease, retrying conflicts and transient errors.
//
// Example:
//
//	lease, err := cli.Acquire(ctx, api.AcquireRequest{
//	    Key:        "orders",
//	    Owner:      "worker-1",
//	    TTLSeconds: 30,
//	})
//	if err != nil {
//	    return err
//	}
//	defer lease.Close()
//	if err := lease.Save(ctx, map[string]any{"progress": "done"}); err != nil {
//	    return err
//	}
func (c *Client) Acquire(ctx context.Context, req api.AcquireRequest, opts ...AcquireOption) (*LeaseSession, error) {
	cfg := defaultAcquireConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	normalizeAcquireConfig(&cfg)
	delay := cfg.BaseDelay
	failureCount := 0
	oneShot := req.BlockSecs == BlockNoWait
	start := time.Now()
	deadline := time.Time{}
	if req.BlockSecs > 0 {
		deadline = start.Add(time.Duration(req.BlockSecs) * time.Second)
	}
	currentReq := req
	attempt := 0

	for {
		attempt++
		c.logTraceCtx(ctx, "client.acquire.attempt", "key", req.Key, "block_secs", currentReq.BlockSecs, "endpoint", c.lastEndpoint, "attempt", attempt)
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				err := fmt.Errorf("lockd: acquire timed out after %s", time.Since(start).Truncate(time.Millisecond))
				c.logWarnCtx(ctx, "client.acquire.deadline", "key", req.Key, "attempt", attempt, "error", err)
				return nil, err
			}
			secs := max(int64(math.Ceil(remaining.Seconds())), 1)
			currentReq.BlockSecs = secs
		}

		resp, endpoint, err := c.acquireOnce(ctx, currentReq)
		if err == nil {
			failureCount = 0
			if req.Key == "" {
				req.Key = resp.Key
			}
			if currentReq.Key == "" {
				currentReq.Key = resp.Key
			}
			keyForLog := resp.Key
			if keyForLog == "" {
				keyForLog = req.Key
			}
			token := ""
			if resp.FencingToken != 0 {
				token = strconv.FormatInt(resp.FencingToken, 10)
				c.RegisterLeaseToken(resp.LeaseID, token)
			}
			session := newLeaseSession(c, resp, token, endpoint)
			session.setCorrelation(resp.CorrelationID)
			c.logInfoCtx(ctx, "client.acquire.success", "key", keyForLog, "lease_id", resp.LeaseID, "endpoint", c.lastEndpoint, "attempt", attempt)
			return session, nil
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			c.logErrorCtx(ctx, "client.acquire.context", "key", req.Key, "error", ctxErr, "attempt", attempt)
			return nil, ctxErr
		}

		var apiErr *APIError
		expectedRetry := false
		retryAfterHint := time.Duration(0)
		qrfState := ""
		if errors.As(err, &apiErr) {
			retryAfterHint = apiErr.RetryAfterDuration()
			qrfState = apiErr.QRFState
			switch apiErr.Status {
			case http.StatusForbidden:
				c.logErrorCtx(ctx, "client.acquire.forbidden", "key", req.Key, "error", err, "attempt", attempt)
				return nil, err
			case http.StatusConflict:
				if currentReq.BlockSecs != BlockNoWait {
					expectedRetry = true
					c.logDebugCtx(ctx, "client.acquire.conflict_status", "key", req.Key, "endpoint", c.lastEndpoint, "attempt", attempt)
				}
			case http.StatusTooManyRequests:
				c.logWarnCtx(ctx, "client.acquire.throttled", "key", req.Key, "endpoint", c.lastEndpoint, "attempt", attempt, "retry_after", retryAfterHint, "qrf_state", qrfState)
				expectedRetry = true
			default:
				if apiErr.Status >= http.StatusInternalServerError {
					c.logWarnCtx(ctx, "client.acquire.server_error", "key", req.Key, "status", apiErr.Status, "endpoint", c.lastEndpoint, "attempt", attempt, "retry_after", retryAfterHint, "qrf_state", qrfState)
					expectedRetry = true
				}
			}
		}

		if oneShot {
			c.logErrorCtx(ctx, "client.acquire.failure", "key", req.Key, "error", err, "attempt", attempt)
			return nil, err
		}

		if !expectedRetry {
			failureCount++
			if cfg.FailureRetries >= 0 && failureCount > cfg.FailureRetries {
				c.logErrorCtx(ctx, "client.acquire.failure_limit", "key", req.Key, "error", err, "retries", failureCount, "attempt", attempt)
				return nil, err
			}
			c.logWarnCtx(ctx, "client.acquire.retry", "key", req.Key, "error", err, "retries", failureCount, "attempt", attempt)
		} else {
			failureCount = 0
			c.logDebugCtx(ctx, "client.acquire.conflict", "key", req.Key, "error", err, "attempt", attempt)
		}

		sleep := max(retryAfterHint, acquireRetryDelay(delay, cfg))
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				err := fmt.Errorf("lockd: acquire timed out after %s", time.Since(start).Truncate(time.Millisecond))
				c.logWarnCtx(ctx, "client.acquire.timeout", "key", req.Key, "block_secs", req.BlockSecs, "attempt", attempt, "error", err)
				return nil, err
			}
			if sleep > remaining {
				sleep = remaining
			}
		}
		c.logTraceCtx(ctx, "client.acquire.backoff", "key", req.Key, "delay", sleep, "retry_after", retryAfterHint, "attempt", attempt, "endpoint", c.lastEndpoint)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleep):
		}
		next := time.Duration(float64(delay) * cfg.Multiplier)
		if next <= 0 {
			next = cfg.BaseDelay
		}
		if next > cfg.MaxDelay {
			next = cfg.MaxDelay
		}
		if retryAfterHint > next {
			next = retryAfterHint
		}
		delay = next
	}
}

func acquireRetryDelay(cur time.Duration, cfg AcquireConfig) time.Duration {
	sleep := cur
	if sleep <= 0 {
		if cfg.BaseDelay > 0 {
			sleep = cfg.BaseDelay
		} else {
			sleep = 10 * time.Millisecond
		}
	}
	if cfg.MaxDelay > 0 && sleep > cfg.MaxDelay {
		sleep = cfg.MaxDelay
	}
	base := sleep
	if cfg.Jitter > 0 && cfg.randInt63n != nil {
		j := cfg.Jitter
		if sleep > 0 && sleep < j {
			j = sleep / 2
		}
		if j > 0 {
			max := int64(j)*2 + 1
			if max <= 0 {
				max = 1
			}
			offset := time.Duration(cfg.randInt63n(max)) - j
			sleep = base + offset
		}
	}
	if cfg.MaxDelay > 0 {
		max := cfg.MaxDelay
		if cfg.Jitter > 0 && base >= cfg.MaxDelay {
			max = cfg.MaxDelay + cfg.Jitter
		}
		if sleep > max {
			sleep = max
		}
		if sleep < 0 {
			sleep = 0
		}
	} else if sleep < 0 {
		sleep = 0
	}
	return sleep
}

func acquireRandInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	acquireRandMu.Lock()
	v := acquireRand.Int63n(n)
	acquireRandMu.Unlock()
	return v
}

func (c *Client) acquireOnce(ctx context.Context, req api.AcquireRequest) (api.AcquireResponse, string, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return api.AcquireResponse{}, "", err
	}
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.acquireRequestContext(ctx, req)
		body := bytes.NewReader(payload)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+"/v1/acquire", body)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if req.Idempotency != "" {
			httpReq.Header.Set("X-Idempotency-Key", req.Idempotency)
		}
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		return api.AcquireResponse{}, "", err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return api.AcquireResponse{}, "", c.decodeError(resp)
	}
	var acqResp api.AcquireResponse
	if err := json.NewDecoder(resp.Body).Decode(&acqResp); err != nil {
		return api.AcquireResponse{}, "", err
	}
	if acqResp.CorrelationID == "" {
		acqResp.CorrelationID = CorrelationIDFromResponse(resp)
	}
	if acqResp.CorrelationID == "" {
		acqResp.CorrelationID = CorrelationIDFromContext(ctx)
	}
	return acqResp, endpoint, nil
}

// AcquireForUpdate acquires a lease, runs handler while the lease is active, keeps the
// lease alive, and releases it on return. The helper retries the acquire/get handshake
// according to opts, surfaces the current state via AcquireForUpdateContext, and ensures
// Release is invoked even when the handler returns an error.
//
// Example:
//
//	err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
//	    Key:        "orders",
//	    Owner:      "worker-1",
//	    TTLSeconds: 45,
//	}, func(ctx context.Context, af *client.AcquireForUpdateContext) error {
//	    var doc map[string]any
//	    if err := af.Load(ctx, &doc); err != nil && !errors.Is(err, client.ErrStateNotFound) {
//	        return err
//	    }
//	    doc["checkpoint"] = "processing"
//	    return af.Save(ctx, doc)
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
func (c *Client) AcquireForUpdate(ctx context.Context, req api.AcquireRequest, handler AcquireForUpdateHandler, opts ...AcquireOption) error {
	if handler == nil {
		return fmt.Errorf("lockd: acquire-for-update handler required")
	}
	cfg := defaultAcquireConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	normalizeAcquireConfig(&cfg)
	retryCount := 0
	delay := cfg.BaseDelay
	if delay <= 0 {
		delay = 10 * time.Millisecond
	}

	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		handshakeCtx, handshakeCancel := c.forUpdateContext(ctx)
		sess, err := c.Acquire(handshakeCtx, req, opts...)
		if err != nil {
			handshakeCancel()
			return err
		}
		keyForLog := sess.Key
		if keyForLog == "" {
			keyForLog = req.Key
		}
		if req.Key == "" {
			req.Key = keyForLog
		}

		snapshot, snapErr := sess.Get(handshakeCtx)
		if snapErr != nil {
			handshakeCancel()
			c.logWarnCtx(ctx, "client.acquire_for_update.snapshot_error", "key", keyForLog, "lease_id", sess.LeaseID, "error", snapErr)
			if releaseErr := sess.Release(ctx); releaseErr != nil && !isLeaseRequiredError(releaseErr) {
				snapErr = errors.Join(snapErr, releaseErr)
			}
			if shouldRetryForUpdate(snapErr) {
				retryCount++
				if cfg.FailureRetries >= 0 && retryCount > cfg.FailureRetries {
					return snapErr
				}
				sleep := acquireRetryDelay(delay, cfg)
				if sleep <= 0 {
					sleep = 10 * time.Millisecond
				}
				if retryHint := retryAfterFromError(snapErr); retryHint > sleep {
					sleep = retryHint
				}
				delay = sleep
				c.logDebugCtx(ctx, "client.acquire_for_update.retry_after_snapshot_failure", "key", keyForLog, "lease_id", sess.LeaseID, "retries", retryCount, "delay", sleep, "retry_after", retryAfterFromError(snapErr), "qrf_state", qrfStateFromError(snapErr), "error", snapErr)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleep):
				}
				continue
			}
			return snapErr
		}

		c.logInfoCtx(ctx, "client.acquire_for_update.acquired", "key", keyForLog, "lease_id", sess.LeaseID, "endpoint", sess.endpoint)
		cleanupHandshake := handshakeCancel

		handlerCtx, handlerCancel := context.WithCancel(ctx)
		var keepErrCh <-chan error
		if req.TTLSeconds > 0 {
			ttl := max(time.Duration(req.TTLSeconds)*time.Second, time.Second)
			keepErrCh = c.startForUpdateKeepAlive(handlerCtx, handlerCancel, sess, ttl)
		}

		userCtx := &AcquireForUpdateContext{
			Session: sess,
			State:   snapshot,
		}
		handlerErr := handler(handlerCtx, userCtx)
		if snapshot != nil {
			if closeErr := snapshot.Close(); closeErr != nil {
				c.logDebugCtx(ctx, "client.acquire_for_update.state_close_error", "key", keyForLog, "lease_id", sess.LeaseID, "error", closeErr)
			}
		}
		handlerCancel()

		var keepErr error
		if keepErrCh != nil {
			keepErr = <-keepErrCh
		}

		if cleanupHandshake != nil {
			cleanupHandshake()
		}

		releaseErr := sess.Release(ctx)
		resultErr := errors.Join(handlerErr, keepErr)
		if releaseErr != nil && !isLeaseRequiredError(releaseErr) {
			resultErr = errors.Join(resultErr, releaseErr)
		}
		if resultErr != nil {
			c.logErrorCtx(ctx, "client.acquire_for_update.handler_error", "key", keyForLog, "lease_id", sess.LeaseID, "error", resultErr)
			return resultErr
		}
		c.logInfoCtx(ctx, "client.acquire_for_update.success", "key", keyForLog, "lease_id", sess.LeaseID)
		return nil
	}
}

func (c *Client) startForUpdateKeepAlive(ctx context.Context, cancel context.CancelFunc, sess *LeaseSession, ttl time.Duration) <-chan error {
	ch := make(chan error, 1)
	interval := max(ttl/2, time.Second)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				ch <- nil
				return
			case <-ticker.C:
				if _, err := sess.KeepAlive(ctx, ttl); err != nil {
					c.logWarnCtx(ctx, "client.acquire_for_update.keepalive_failed", "key", sess.Key, "lease_id", sess.LeaseID, "error", err)
					ch <- err
					cancel()
					return
				}
			}
		}
	}()
	return ch
}

func shouldRetryForUpdate(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if isLeaseRequiredError(err) {
		return true
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.Status == http.StatusTooManyRequests || apiErr.Status >= http.StatusInternalServerError {
			return true
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

// KeepAlive extends a lease.
func (c *Client) KeepAlive(ctx context.Context, req api.KeepAliveRequest) (*api.KeepAliveResponse, error) {
	c.logTraceCtx(ctx, "client.keepalive.start", "key", req.Key, "lease_id", req.LeaseID, "ttl_seconds", req.TTLSeconds)
	token, err := c.fencingToken(req.LeaseID, "")
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set(headerFencingToken, token)
	if corr := CorrelationIDFromContext(ctx); corr != "" {
		headers.Set(headerCorrelationID, corr)
	}
	c.logTraceCtx(ctx, "client.keepalive.headers", "key", req.Key, "lease_id", req.LeaseID, "fencing_token", token)
	var resp api.KeepAliveResponse
	kCtx, cancel := c.keepAliveContext(ctx)
	defer cancel()
	if _, err := c.postJSON(kCtx, "/v1/keepalive", req, &resp, headers, ""); err != nil {
		c.logErrorCtx(ctx, "client.keepalive.error", "key", req.Key, "lease_id", req.LeaseID, "fencing_token", token, "error", err)
		return nil, err
	}
	c.logTraceCtx(ctx, "client.keepalive.success", "key", req.Key, "lease_id", req.LeaseID, "fencing_token", token, "expires_at", resp.ExpiresAt)
	return &resp, nil
}

// Release drops a lease.
func (c *Client) Release(ctx context.Context, req api.ReleaseRequest) (*api.ReleaseResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.releaseInternal(ctx, req, "")
}

func (c *Client) releaseInternal(ctx context.Context, req api.ReleaseRequest, preferred string) (*api.ReleaseResponse, error) {
	c.logTraceCtx(ctx, "client.release.start", "key", req.Key, "lease_id", req.LeaseID)
	token, err := c.fencingToken(req.LeaseID, "")
	if err != nil {
		if errors.Is(err, ErrMissingFencingToken) {
			c.logDebugCtx(ctx, "client.release.no_token", "key", req.Key, "lease_id", req.LeaseID)
			return &api.ReleaseResponse{Released: true}, nil
		}
		return nil, err
	}
	headers := http.Header{}
	headers.Set(headerFencingToken, token)
	if corr := CorrelationIDFromContext(ctx); corr != "" {
		headers.Set(headerCorrelationID, corr)
	}
	c.closeIdleConnections()
	c.logTraceCtx(ctx, "client.release.headers", "key", req.Key, "lease_id", req.LeaseID, "fencing_token", token)
	var resp api.ReleaseResponse
	endpoint, err := c.postJSON(ctx, "/v1/release", req, &resp, headers, preferred)
	if err != nil {
		if isLeaseRequiredError(err) {
			c.logDebugCtx(ctx, "client.release.already_gone", "key", req.Key, "lease_id", req.LeaseID)
			c.leaseTokens.Delete(req.LeaseID)
			return &api.ReleaseResponse{Released: true}, nil
		}
		if errors.Is(err, context.DeadlineExceeded) && preferred != "" {
			c.logWarnCtx(ctx, "client.release.retry_different_endpoint", "key", req.Key, "lease_id", req.LeaseID, "endpoint", preferred, "error", err)
			for _, base := range c.endpoints {
				if base == preferred {
					continue
				}
				altCtx := ctx
				if deadline, ok := ctx.Deadline(); ok {
					remaining := time.Until(deadline)
					if remaining <= 0 {
						break
					}
					var cancel context.CancelFunc
					altCtx, cancel = context.WithTimeout(context.Background(), remaining)
					defer cancel()
				} else {
					var cancel context.CancelFunc
					altCtx, cancel = context.WithTimeout(context.Background(), c.closeTimeout)
					defer cancel()
				}
				altResp, altErr := c.postJSON(altCtx, "/v1/release", req, &resp, headers, base)
				if altErr == nil {
					endpoint = altResp
					err = nil
					break
				}
				if isLeaseRequiredError(altErr) {
					c.logDebugCtx(ctx, "client.release.already_gone", "key", req.Key, "lease_id", req.LeaseID, "endpoint", base)
					c.leaseTokens.Delete(req.LeaseID)
					return &api.ReleaseResponse{Released: true}, nil
				}
				c.logWarnCtx(ctx, "client.release.backup_failed", "key", req.Key, "lease_id", req.LeaseID, "endpoint", base, "error", altErr)
			}
			if err != nil {
				return nil, err
			}
		} else {
			c.logErrorCtx(ctx, "client.release.error", "key", req.Key, "lease_id", req.LeaseID, "fencing_token", token, "error", err)
			return nil, err
		}
	}
	if endpoint != "" {
		for _, base := range c.endpoints {
			if base == endpoint {
				continue
			}
			_, extraErr := c.postJSON(ctx, "/v1/release", req, nil, headers.Clone(), base)
			if extraErr != nil && !isLeaseRequiredError(extraErr) {
				c.logDebugCtx(ctx, "client.release.extra_endpoint_error", "key", req.Key, "lease_id", req.LeaseID, "endpoint", base, "error", extraErr)
			}
		}
	}
	c.logTraceCtx(ctx, "client.release.success", "key", req.Key, "lease_id", req.LeaseID, "fencing_token", token)
	c.leaseTokens.Delete(req.LeaseID)
	return &resp, nil
}

// Describe fetches key metadata without state.
func (c *Client) Describe(ctx context.Context, key string) (*api.DescribeResponse, error) {
	c.logTraceCtx(ctx, "client.describe.start", "key", key, "endpoint", c.lastEndpoint)
	var describe api.DescribeResponse
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		full := fmt.Sprintf("%s/v1/describe?key=%s", base, url.QueryEscape(key))
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, full, http.NoBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		c.applyCorrelationHeader(ctx, req, "")
		return req, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.describe.transport_error", "key", key, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.describe.error", "key", key, "endpoint", endpoint, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(&describe); err != nil {
		return nil, err
	}
	c.logTraceCtx(ctx, "client.describe.success", "key", key, "endpoint", endpoint, "version", describe.Version, "state_etag", describe.StateETag)
	return &describe, nil
}

// Get streams the JSON state for a key. Caller must close the returned reader.
// When the key has no state the returned reader is nil.
func (c *Client) Get(ctx context.Context, key, leaseID string) (io.ReadCloser, string, string, error) {
	token, err := c.fencingToken(leaseID, "")
	if err != nil {
		return nil, "", "", err
	}
	c.logTraceCtx(ctx, "client.get.start", "key", key, "lease_id", leaseID, "endpoint", c.lastEndpoint, "fencing_token", token)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		// Use the caller's context without the per-request HTTP timeout so that
		// AcquireForUpdate handlers can continue reading the snapshot while the
		// server is draining. Callers are expected to bound ctx themselves.
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		full := fmt.Sprintf("%s/v1/get?key=%s", base, url.QueryEscape(key))
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, full, http.NoBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		req.Header.Set("X-Lease-ID", leaseID)
		req.Header.Set(headerFencingToken, token)
		c.applyCorrelationHeader(ctx, req, "")
		return req, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.get.transport_error", "key", key, "lease_id", leaseID, "fencing_token", token, "error", err)
		return nil, "", "", err
	}
	if resp.StatusCode == http.StatusNoContent {
		resp.Body.Close()
		cancel()
		c.logDebugCtx(ctx, "client.get.empty", "key", key, "lease_id", leaseID, "endpoint", endpoint, "fencing_token", token)
		return nil, "", "", nil
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		cancel()
		c.logWarnCtx(ctx, "client.get.error", "key", key, "lease_id", leaseID, "endpoint", endpoint, "fencing_token", token, "status", resp.StatusCode)
		return nil, "", "", c.decodeError(resp)
	}
	if newToken := resp.Header.Get(headerFencingToken); newToken != "" {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	reader := &cancelReadCloser{ReadCloser: resp.Body, cancel: cancel}
	etag := resp.Header.Get("ETag")
	version := resp.Header.Get("X-Key-Version")
	c.logTraceCtx(ctx, "client.get.success", "key", key, "lease_id", leaseID, "endpoint", endpoint, "fencing_token", token, "etag", etag, "version", version)
	return reader, etag, version, nil
}

// Load delegates to sess.Load.
func (c *Client) Load(ctx context.Context, sess *LeaseSession, v any) error {
	return sess.Load(ctx, v)
}

// Save delegates to sess.Save.
func (c *Client) Save(ctx context.Context, sess *LeaseSession, v any) error {
	return sess.Save(ctx, v)
}

// GetBytes fetches the JSON state into memory and returns it along with metadata.
func (c *Client) GetBytes(ctx context.Context, key, leaseID string) ([]byte, string, string, error) {
	reader, etag, version, err := c.Get(ctx, key, leaseID)
	if err != nil {
		return nil, "", "", err
	}
	if reader == nil {
		return nil, etag, version, nil
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, "", "", err
	}
	return data, etag, version, nil
}

// Update uploads new JSON state from the provided reader.
func (c *Client) Update(ctx context.Context, key, leaseID string, body io.Reader, opts UpdateOptions) (*UpdateResult, error) {
	token, err := c.fencingToken(leaseID, opts.FencingToken)
	if err != nil {
		return nil, err
	}
	c.logTraceCtx(ctx, "client.update.start", "key", key, "lease_id", leaseID, "endpoint", c.lastEndpoint, "fencing_token", token)
	bodyFactory, err := makeBodyFactory(body)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/update?key=%s", url.QueryEscape(key))
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqBody, err := bodyFactory()
		if err != nil {
			return nil, nil, err
		}
		reqCtx, cancel := c.requestContext(ctx)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, reqBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Lease-ID", leaseID)
		if opts.IfETag != "" {
			req.Header.Set("X-If-State-ETag", opts.IfETag)
		}
		if opts.IfVersion != "" {
			req.Header.Set("X-If-Version", opts.IfVersion)
		}
		req.Header.Set(headerFencingToken, token)
		c.applyCorrelationHeader(ctx, req, "")
		return req, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.update.transport_error", "key", key, "lease_id", leaseID, "endpoint", endpoint, "fencing_token", token, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.update.error", "key", key, "lease_id", leaseID, "endpoint", endpoint, "fencing_token", token, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	var result UpdateResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	c.logTraceCtx(ctx, "client.update.success", "key", key, "lease_id", leaseID, "endpoint", endpoint, "fencing_token", token, "new_version", result.NewVersion, "new_etag", result.NewStateETag)
	if newToken := resp.Header.Get(headerFencingToken); newToken != "" {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	return &result, nil
}

// UpdateBytes uploads new JSON state from the provided byte slice.
func (c *Client) UpdateBytes(ctx context.Context, key, leaseID string, body []byte, opts UpdateOptions) (*UpdateResult, error) {
	return c.Update(ctx, key, leaseID, bytes.NewReader(body), opts)
}

// Remove deletes the JSON state for key while ensuring the lease and
// conditional headers (when provided) are honoured.
func (c *Client) Remove(ctx context.Context, key, leaseID string, opts RemoveOptions) (*api.RemoveResponse, error) {
	token, err := c.fencingToken(leaseID, opts.FencingToken)
	if err != nil {
		return nil, err
	}
	c.logTraceCtx(ctx, "client.remove.start", "key", key, "lease_id", leaseID, "endpoint", c.lastEndpoint, "fencing_token", token)
	path := fmt.Sprintf("/v1/remove?key=%s", url.QueryEscape(key))
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, http.NoBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		req.Header.Set("X-Lease-ID", leaseID)
		if opts.IfETag != "" {
			req.Header.Set("X-If-State-ETag", opts.IfETag)
		}
		if opts.IfVersion != "" {
			req.Header.Set("X-If-Version", opts.IfVersion)
		}
		req.Header.Set(headerFencingToken, token)
		c.applyCorrelationHeader(ctx, req, "")
		return req, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.remove.transport_error", "key", key, "lease_id", leaseID, "endpoint", endpoint, "fencing_token", token, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.remove.error", "key", key, "lease_id", leaseID, "endpoint", endpoint, "fencing_token", token, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	var result api.RemoveResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	c.logTraceCtx(ctx, "client.remove.success", "key", key, "lease_id", leaseID, "endpoint", endpoint, "fencing_token", token, "removed", result.Removed, "new_version", result.NewVersion)
	if newToken := resp.Header.Get(headerFencingToken); newToken != "" {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	return &result, nil
}

// APIError describes an error response from lockd.
type APIError struct {
	Status     int
	Response   api.ErrorResponse
	Body       []byte
	RetryAfter time.Duration
	QRFState   string
}

func (e *APIError) Error() string {
	if e.Response.ErrorCode != "" {
		return fmt.Sprintf("lockd: %s (%s)", e.Response.ErrorCode, e.Response.Detail)
	}
	return fmt.Sprintf("lockd: status %d", e.Status)
}

// RetryAfterDuration returns the recommended back-off hinted by the server.
func (e *APIError) RetryAfterDuration() time.Duration {
	if e == nil {
		return 0
	}
	if e.RetryAfter > 0 {
		return e.RetryAfter
	}
	if e.Response.RetryAfterSeconds > 0 {
		return time.Duration(e.Response.RetryAfterSeconds) * time.Second
	}
	return 0
}

func (c *Client) postJSON(ctx context.Context, path string, payload any, out any, headers http.Header, preferred string) (string, error) {
	c.logTraceCtx(ctx, "client.http.post.start", "path", path, "endpoint", c.lastEndpoint)
	var bodyBytes []byte
	if payload != nil {
		buf := new(bytes.Buffer)
		if err := json.NewEncoder(buf).Encode(payload); err != nil {
			return "", err
		}
		bodyBytes = buf.Bytes()
	}
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		var body io.Reader
		if payload != nil {
			body = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, body)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		if headers != nil {
			for k, vals := range headers {
				for _, v := range vals {
					req.Header.Add(k, v)
				}
			}
		}
		if req.Header.Get(headerCorrelationID) == "" {
			c.applyCorrelationHeader(ctx, req, "")
		}
		return req, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, preferred)
	if err != nil {
		c.logErrorCtx(ctx, "client.http.post.transport_error", "path", path, "error", err)
		return "", err
	}
	defer cancel()
	defer resp.Body.Close()
	c.observeShutdown(endpoint, resp)
	if resp.StatusCode >= 300 {
		c.logWarnCtx(ctx, "client.http.post.error", "path", path, "endpoint", endpoint, "status", resp.StatusCode)
		return endpoint, c.decodeError(resp)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return endpoint, err
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	c.logTraceCtx(ctx, "client.http.post.success", "path", path, "endpoint", endpoint, "status", resp.StatusCode)
	return endpoint, nil
}

func (c *Client) decodeError(resp *http.Response) error {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var errResp api.ErrorResponse
	if len(data) > 0 {
		if err := json.Unmarshal(data, &errResp); err != nil {
			// leave errResp empty, but keep body for diagnostics
			return &APIError{Status: resp.StatusCode, Body: data}
		}
	}
	retryAfter := parseRetryAfterHeader(resp.Header.Get("Retry-After"))
	if retryAfter == 0 && errResp.RetryAfterSeconds > 0 {
		retryAfter = time.Duration(errResp.RetryAfterSeconds) * time.Second
	}
	return &APIError{
		Status:     resp.StatusCode,
		Response:   errResp,
		Body:       data,
		RetryAfter: retryAfter,
		QRFState:   strings.ToLower(resp.Header.Get("X-Lockd-QRF-State")),
	}
}

func isLeaseRequiredError(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.Response.ErrorCode == "lease_required" {
			return true
		}
	}
	return false
}

func parseRetryAfterHeader(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(raw, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds * float64(time.Second))
	}
	if ts, err := http.ParseTime(raw); err == nil {
		delay := time.Until(ts)
		if delay <= 0 {
			return 0
		}
		return delay
	}
	return 0
}

func shutdownSignalFromHeader(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (c *Client) observeShutdown(endpoint string, resp *http.Response) {
	if c == nil || resp == nil || !c.drainAwareShutdown {
		return
	}
	if !shutdownSignalFromHeader(resp.Header.Get(headerShutdownImminent)) {
		return
	}
	ctx := context.Background()
	if resp.Request != nil {
		ctx = resp.Request.Context()
	}
	fields := []any{"endpoint", endpoint}
	if c.shutdownNotified.CompareAndSwap(false, true) {
		c.logInfoCtx(ctx, "client.shutdown.notice", fields...)
	} else {
		c.logDebugCtx(ctx, "client.shutdown.notice.repeat", fields...)
	}
	matchEndpoint := func(sessEndpoint string) bool {
		if endpoint == "" || sessEndpoint == "" {
			return true
		}
		return sessEndpoint == endpoint
	}
	c.sessions.Range(func(_, value any) bool {
		sess, ok := value.(*LeaseSession)
		if !ok || sess == nil {
			return true
		}
		if !matchEndpoint(sess.endpoint) {
			return true
		}
		sess.triggerDrainRelease("server_shutdown")
		return true
	})
}

func retryAfterFromError(err error) time.Duration {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.RetryAfterDuration()
	}
	return 0
}

func qrfStateFromError(err error) string {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.QRFState
	}
	return ""
}

// QueueAck acknowledges a queue message via the /v1/queue/ack API.
func (c *Client) QueueAck(ctx context.Context, req api.AckRequest) (*api.AckResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	cidCtx := CorrelationIDFromContext(ctx)
	startFields := []any{"queue", req.Queue, "mid", req.MessageID}
	if cidCtx != "" {
		startFields = append(startFields, "cid", cidCtx)
	}
	c.logTraceCtx(ctx, "client.queue.ack.start", startFields...)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+"/v1/queue/ack", bytes.NewReader(body))
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	c.observeShutdown(endpoint, resp)
	if resp.StatusCode != http.StatusOK {
		warnFields := []any{"queue", req.Queue, "mid", req.MessageID, "endpoint", endpoint, "status", resp.StatusCode}
		if cidCtx != "" {
			warnFields = append(warnFields, "cid", cidCtx)
		}
		c.logWarnCtx(ctx, "client.queue.ack.error", warnFields...)
		return nil, c.decodeError(resp)
	}
	var res api.AckResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	if res.CorrelationID == "" {
		if cid := CorrelationIDFromResponse(resp); cid != "" {
			res.CorrelationID = cid
		} else if cid := CorrelationIDFromContext(ctx); cid != "" {
			res.CorrelationID = cid
		}
	}
	successFields := []any{"queue", req.Queue, "mid", req.MessageID, "endpoint", endpoint}
	if res.CorrelationID != "" {
		successFields = append(successFields, "cid", res.CorrelationID)
	}
	c.logTraceCtx(ctx, "client.queue.ack.success", successFields...)
	return &res, nil
}

// QueueNack requeues a message with an optional visibility delay via /v1/queue/nack.
func (c *Client) QueueNack(ctx context.Context, req api.NackRequest) (*api.NackResponse, error) {
	if req.DelaySeconds < 0 {
		req.DelaySeconds = 0
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	cidCtx := CorrelationIDFromContext(ctx)
	startFields := []any{"queue", req.Queue, "mid", req.MessageID, "delay_seconds", req.DelaySeconds}
	if cidCtx != "" {
		startFields = append(startFields, "cid", cidCtx)
	}
	c.logTraceCtx(ctx, "client.queue.nack.start", startFields...)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+"/v1/queue/nack", bytes.NewReader(body))
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	c.observeShutdown(endpoint, resp)
	if resp.StatusCode != http.StatusOK {
		warnFields := []any{"queue", req.Queue, "mid", req.MessageID, "endpoint", endpoint, "status", resp.StatusCode}
		if cidCtx != "" {
			warnFields = append(warnFields, "cid", cidCtx)
		}
		c.logWarnCtx(ctx, "client.queue.nack.error", warnFields...)
		return nil, c.decodeError(resp)
	}
	var res api.NackResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	if res.CorrelationID == "" {
		if cid := CorrelationIDFromResponse(resp); cid != "" {
			res.CorrelationID = cid
		} else if cid := CorrelationIDFromContext(ctx); cid != "" {
			res.CorrelationID = cid
		}
	}
	successFields := []any{"queue", req.Queue, "mid", req.MessageID, "endpoint", endpoint, "delay_seconds", req.DelaySeconds}
	if res.CorrelationID != "" {
		successFields = append(successFields, "cid", res.CorrelationID)
	}
	c.logTraceCtx(ctx, "client.queue.nack.success", successFields...)
	return &res, nil
}

// QueueExtend extends a queue message's visibility window using /v1/queue/extend.
func (c *Client) QueueExtend(ctx context.Context, req api.ExtendRequest) (*api.ExtendResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	cidCtx := CorrelationIDFromContext(ctx)
	startFields := []any{"queue", req.Queue, "mid", req.MessageID, "extend_seconds", req.ExtendBySeconds}
	if cidCtx != "" {
		startFields = append(startFields, "cid", cidCtx)
	}
	c.logTraceCtx(ctx, "client.queue.extend.start", startFields...)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+"/v1/queue/extend", bytes.NewReader(body))
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	c.observeShutdown(endpoint, resp)
	if resp.StatusCode != http.StatusOK {
		warnFields := []any{"queue", req.Queue, "mid", req.MessageID, "endpoint", endpoint, "status", resp.StatusCode}
		if cidCtx != "" {
			warnFields = append(warnFields, "cid", cidCtx)
		}
		c.logWarnCtx(ctx, "client.queue.extend.error", warnFields...)
		return nil, c.decodeError(resp)
	}
	var res api.ExtendResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	if res.CorrelationID == "" {
		if cid := CorrelationIDFromResponse(resp); cid != "" {
			res.CorrelationID = cid
		} else if cid := CorrelationIDFromContext(ctx); cid != "" {
			res.CorrelationID = cid
		}
	}
	successFields := []any{"queue", req.Queue, "mid", req.MessageID, "endpoint", endpoint, "lease_expires_at_unix", res.LeaseExpiresAtUnix}
	if res.CorrelationID != "" {
		successFields = append(successFields, "cid", res.CorrelationID)
	}
	c.logTraceCtx(ctx, "client.queue.extend.success", successFields...)
	return &res, nil
}

func (c *Client) dequeueInternal(ctx context.Context, queue string, opts DequeueOptions, stateful bool) (*DequeueResult, error) {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return nil, fmt.Errorf("lockd: queue is required")
	}
	owner := strings.TrimSpace(opts.Owner)
	if owner == "" {
		return nil, fmt.Errorf("lockd: owner is required")
	}
	waitSeconds := opts.BlockSeconds
	if waitSeconds < 0 && waitSeconds != BlockNoWait {
		waitSeconds = BlockNoWait
	}
	if waitSeconds == BlockNoWait {
		waitSeconds = api.BlockNoWait
	}
	req := api.DequeueRequest{
		Queue:                    queue,
		Owner:                    owner,
		VisibilityTimeoutSeconds: secondsFromDuration(opts.Visibility),
		WaitSeconds:              waitSeconds,
		PageSize:                 opts.PageSize,
		StartAfter:               strings.TrimSpace(opts.StartAfter),
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	path := "/v1/queue/dequeue"
	if stateful {
		path = "/v1/queue/dequeueWithState"
	}
	cidCtx := CorrelationIDFromContext(ctx)
	beginFields := []any{
		"queue", queue,
		"owner", owner,
		"stateful", stateful,
		"visibility_seconds", req.VisibilityTimeoutSeconds,
		"wait_seconds", req.WaitSeconds,
		"page_size", req.PageSize,
		"start_after", req.StartAfter,
	}
	if cidCtx != "" {
		beginFields = append(beginFields, "cid", cidCtx)
	}
	c.logInfoCtx(ctx, "client.queue.dequeue.begin", beginFields...)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, bytes.NewReader(body))
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		return nil, err
	}
	cancel()
	c.observeShutdown(endpoint, resp)
	if resp.StatusCode != http.StatusOK {
		errorFields := []any{"queue", queue, "owner", owner, "endpoint", endpoint, "status", resp.StatusCode}
		if cidCtx != "" {
			errorFields = append(errorFields, "cid", cidCtx)
		}
		c.logWarnCtx(ctx, "client.queue.dequeue.error", errorFields...)
		err := c.decodeError(resp)
		resp.Body.Close()
		return nil, err
	}
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		resp.Body.Close()
		return nil, fmt.Errorf("lockd: unexpected dequeue content-type %q", resp.Header.Get("Content-Type"))
	}
	boundary := params["boundary"]
	if boundary == "" {
		resp.Body.Close()
		return nil, fmt.Errorf("lockd: multipart response missing boundary")
	}
	mr := multipart.NewReader(resp.Body, boundary)
	handles := make([]*QueueMessageHandle, 0, max(1, opts.PageSize))
	var lastCursor string
	var headerCID string
	for {
		metaPart, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("lockd: dequeue meta part: %w", err)
		}
		if name := metaPart.FormName(); name != "" && name != "meta" {
			metaPart.Close()
			resp.Body.Close()
			return nil, fmt.Errorf("lockd: unexpected multipart part %q", name)
		}
		var apiResp api.DequeueResponse
		if err := json.NewDecoder(metaPart).Decode(&apiResp); err != nil {
			metaPart.Close()
			resp.Body.Close()
			return nil, fmt.Errorf("lockd: decode dequeue meta: %w", err)
		}
		metaPart.Close()
		if apiResp.Message == nil {
			continue
		}
		if apiResp.Message.CorrelationID == "" {
			if cid := CorrelationIDFromResponse(resp); cid != "" {
				apiResp.Message.CorrelationID = cid
			} else if cid := CorrelationIDFromContext(ctx); cid != "" {
				apiResp.Message.CorrelationID = cid
			}
		}
		if headerCID == "" {
			headerCID = apiResp.Message.CorrelationID
		}
		payloadPart, err := mr.NextPart()
		var payloadBytes []byte
		if err == io.EOF {
			payloadBytes = nil
		} else if err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("lockd: dequeue payload part: %w", err)
		} else {
			if name := payloadPart.FormName(); name != "" && name != "payload" {
				payloadPart.Close()
				resp.Body.Close()
				return nil, fmt.Errorf("lockd: unexpected multipart part %q", name)
			}
			payloadBytes, err = io.ReadAll(payloadPart)
			payloadPart.Close()
			if err != nil {
				resp.Body.Close()
				return nil, fmt.Errorf("lockd: read dequeue payload: %w", err)
			}
		}
		payloadRC := io.NopCloser(bytes.NewReader(payloadBytes))
		handle := &QueueMessageHandle{
			client:        c,
			msg:           *apiResp.Message,
			cursor:        apiResp.NextCursor,
			payloadStream: payloadRC,
		}
		if handle.msg.LeaseID != "" {
			c.RegisterLeaseToken(handle.msg.LeaseID, strconv.FormatInt(handle.msg.FencingToken, 10))
		}
		if stateful && handle.msg.StateLeaseID != "" {
			handle.state = &QueueStateHandle{
				client:         c,
				queue:          queue,
				messageID:      handle.msg.MessageID,
				leaseID:        handle.msg.StateLeaseID,
				fencingToken:   handle.msg.StateFencingToken,
				leaseExpiresAt: handle.msg.StateLeaseExpiresAtUnix,
				stateETag:      handle.msg.StateETag,
				correlationID:  handle.msg.CorrelationID,
			}
			c.RegisterLeaseToken(handle.state.leaseID, strconv.FormatInt(handle.state.fencingToken, 10))
		}
		handles = append(handles, handle)
		lastCursor = apiResp.NextCursor
	}
	resp.Body.Close()

	if len(handles) == 0 {
		return &DequeueResult{NextCursor: lastCursor}, nil
	}
	if headerCID != "" {
		for _, handle := range handles {
			if handle != nil && handle.msg.CorrelationID == "" {
				handle.msg.CorrelationID = headerCID
			}
		}
	}
	result := &DequeueResult{
		Message:    handles[0],
		Messages:   handles,
		NextCursor: lastCursor,
	}
	if headerCID != "" {
		result.Message.msg.CorrelationID = headerCID
	}
	successFields := []any{
		"queue", queue,
		"owner", owner,
		"stateful", stateful,
		"endpoint", endpoint,
		"next_cursor", lastCursor,
		"messages", len(handles),
	}
	if handles[0].msg.CorrelationID != "" {
		successFields = append(successFields, "cid", handles[0].msg.CorrelationID)
	}
	c.logInfoCtx(ctx, "client.queue.dequeue.success", successFields...)
	return result, nil
}

// Enqueue pushes a payload into the specified queue using /v1/queue/enqueue.
func (c *Client) Enqueue(ctx context.Context, queue string, payload io.Reader, opts EnqueueOptions) (*api.EnqueueResponse, error) {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return nil, fmt.Errorf("lockd: queue is required")
	}
	if payload == nil {
		payload = bytes.NewReader(nil)
	}
	meta := api.EnqueueRequest{
		Queue:                    queue,
		DelaySeconds:             secondsFromDuration(opts.Delay),
		VisibilityTimeoutSeconds: secondsFromDuration(opts.Visibility),
		TTLSeconds:               secondsFromDuration(opts.TTL),
		MaxAttempts:              opts.MaxAttempts,
		Attributes:               opts.Attributes,
		PayloadContentType:       opts.ContentType,
	}
	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)
	contentType := fmt.Sprintf("multipart/related; boundary=%s", writer.Boundary())
	go func() {
		var err error
		defer func() {
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			if cerr := writer.Close(); cerr != nil {
				_ = pw.CloseWithError(cerr)
				return
			}
			_ = pw.Close()
		}()
		metaHeader := textproto.MIMEHeader{}
		metaHeader.Set("Content-Type", "application/json")
		metaHeader.Set("Content-Disposition", `form-data; name="meta"`)
		var metaPart io.Writer
		metaPart, err = writer.CreatePart(metaHeader)
		if err != nil {
			return
		}
		if err = json.NewEncoder(metaPart).Encode(meta); err != nil {
			return
		}
		payloadHeader := textproto.MIMEHeader{}
		payloadHeader.Set("Content-Disposition", `form-data; name="payload"`)
		ctype := strings.TrimSpace(opts.ContentType)
		if ctype == "" {
			ctype = "application/octet-stream"
		}
		payloadHeader.Set("Content-Type", ctype)
		var payloadPart io.Writer
		payloadPart, err = writer.CreatePart(payloadHeader)
		if err != nil {
			return
		}
		_, err = io.Copy(payloadPart, payload)
	}()
	cidCtx := CorrelationIDFromContext(ctx)
	beginFields := []any{"queue", queue, "content_type", opts.ContentType}
	if cidCtx != "" {
		beginFields = append(beginFields, "cid", cidCtx)
	}
	c.logInfoCtx(ctx, "client.queue.enqueue.begin", beginFields...)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+"/v1/queue/enqueue", pr)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("Content-Type", contentType)
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		errFields := []any{"queue", queue, "error", err}
		if cidCtx != "" {
			errFields = append(errFields, "cid", cidCtx)
		}
		c.logErrorCtx(ctx, "client.queue.enqueue.transport_error", errFields...)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		warnFields := []any{"queue", queue, "endpoint", endpoint, "status", resp.StatusCode}
		if cidCtx != "" {
			warnFields = append(warnFields, "cid", cidCtx)
		}
		c.logWarnCtx(ctx, "client.queue.enqueue.error", warnFields...)
		return nil, c.decodeError(resp)
	}
	var result api.EnqueueResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if result.CorrelationID == "" {
		if cid := CorrelationIDFromResponse(resp); cid != "" {
			result.CorrelationID = cid
		} else if cid := CorrelationIDFromContext(ctx); cid != "" {
			result.CorrelationID = cid
		}
	}
	successFields := []any{
		"queue", queue,
		"mid", result.MessageID,
		"attempts", result.Attempts,
		"max_attempts", result.MaxAttempts,
		"endpoint", endpoint,
	}
	if result.CorrelationID != "" {
		successFields = append(successFields, "cid", result.CorrelationID)
	}
	c.logInfoCtx(ctx, "client.queue.enqueue.success", successFields...)
	return &result, nil
}

// EnqueueBytes is a convenience helper that enqueues an in-memory payload.
func (c *Client) EnqueueBytes(ctx context.Context, queue string, payload []byte, opts EnqueueOptions) (*api.EnqueueResponse, error) {
	return c.Enqueue(ctx, queue, bytes.NewReader(payload), opts)
}

// Dequeue pops a single message from the queue using /v1/queue/dequeue.
func (c *Client) Dequeue(ctx context.Context, queue string, opts DequeueOptions) (*QueueMessage, error) {
	res, err := c.dequeueInternal(ctx, queue, opts, false)
	if err != nil {
		return nil, err
	}
	if res.Message == nil {
		return nil, fmt.Errorf("lockd: empty dequeue response")
	}
	msg := newQueueMessage(res.Message, nil, opts.OnCloseDelay)
	return msg, nil
}

// DequeueBatch retrieves up to opts.PageSize messages in a single dequeue request.
// The caller is responsible for acking or nacking every returned message.
func (c *Client) DequeueBatch(ctx context.Context, queue string, opts DequeueOptions) ([]*QueueMessage, error) {
	if opts.PageSize <= 1 {
		msg, err := c.Dequeue(ctx, queue, opts)
		if err != nil {
			return nil, err
		}
		return []*QueueMessage{msg}, nil
	}
	res, err := c.dequeueInternal(ctx, queue, opts, false)
	if err != nil {
		return nil, err
	}
	if len(res.Messages) == 0 {
		return nil, nil
	}
	batch := make([]*QueueMessage, 0, len(res.Messages))
	for _, handle := range res.Messages {
		if handle == nil {
			continue
		}
		batch = append(batch, newQueueMessage(handle, nil, opts.OnCloseDelay))
	}
	return batch, nil
}

// DequeueWithState pops a queue message and returns its state payload in one call.
func (c *Client) DequeueWithState(ctx context.Context, queue string, opts DequeueOptions) (*QueueMessage, error) {
	res, err := c.dequeueInternal(ctx, queue, opts, true)
	if err != nil {
		return nil, err
	}
	if res.Message == nil {
		return nil, fmt.Errorf("lockd: empty dequeue response")
	}
	msg := newQueueMessage(res.Message, nil, opts.OnCloseDelay)
	return msg, nil
}

// Subscribe streams queue messages continuously and invokes handler for each delivery.
func (c *Client) Subscribe(ctx context.Context, queue string, opts SubscribeOptions, handler MessageHandler) error {
	if handler == nil {
		return fmt.Errorf("lockd: message handler required")
	}
	return c.subscribe(ctx, queue, opts, handler, nil)
}

// SubscribeWithState streams queue messages with workflow state and invokes handler for each delivery.
func (c *Client) SubscribeWithState(ctx context.Context, queue string, opts SubscribeOptions, handler MessageHandlerWithState) error {
	if handler == nil {
		return fmt.Errorf("lockd: message handler required")
	}
	return c.subscribe(ctx, queue, opts, nil, handler)
}

func (c *Client) subscribe(ctx context.Context, queue string, opts SubscribeOptions, handler MessageHandler, stateHandler MessageHandlerWithState) error {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return fmt.Errorf("lockd: queue is required")
	}
	owner := strings.TrimSpace(opts.Owner)
	if owner == "" {
		return fmt.Errorf("lockd: owner is required")
	}
	prefetch := opts.Prefetch
	if prefetch <= 0 {
		prefetch = 1
	}
	blockSeconds := opts.BlockSeconds
	if blockSeconds < 0 && blockSeconds != BlockNoWait {
		blockSeconds = BlockNoWait
	}
	if blockSeconds == BlockNoWait {
		blockSeconds = api.BlockNoWait
	}
	req := api.DequeueRequest{
		Queue:                    queue,
		Owner:                    owner,
		VisibilityTimeoutSeconds: secondsFromDuration(opts.Visibility),
		WaitSeconds:              blockSeconds,
		PageSize:                 prefetch,
		StartAfter:               strings.TrimSpace(opts.StartAfter),
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	stateful := stateHandler != nil
	beginFields := []any{
		"queue", queue,
		"owner", owner,
		"stateful", stateful,
		"prefetch", prefetch,
		"wait_seconds", blockSeconds,
	}
	if cid := CorrelationIDFromContext(ctx); cid != "" {
		beginFields = append(beginFields, "cid", cid)
	}
	c.logInfoCtx(ctx, "client.queue.subscribe.begin", beginFields...)

	path := "/v1/queue/subscribe"
	if stateful {
		path = "/v1/queue/subscribeWithState"
	}
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, bytes.NewReader(body))
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}

	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		return err
	}
	defer cancel()
	defer resp.Body.Close()
	c.observeShutdown(endpoint, resp)

	if resp.StatusCode != http.StatusOK {
		err := c.decodeError(resp)
		return err
	}

	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("lockd: unexpected subscribe content-type %q", resp.Header.Get("Content-Type"))
	}
	boundary := params["boundary"]
	if boundary == "" {
		return fmt.Errorf("lockd: multipart response missing boundary")
	}
	mr := multipart.NewReader(resp.Body, boundary)
	headerCID := resp.Header.Get(headerCorrelationID)
	delivered := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		metaPart, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("lockd: subscribe meta part: %w", err)
		}
		if name := metaPart.FormName(); name != "" && name != "meta" {
			metaPart.Close()
			return fmt.Errorf("lockd: unexpected multipart part %q", name)
		}
		var apiResp api.DequeueResponse
		if err := json.NewDecoder(metaPart).Decode(&apiResp); err != nil {
			metaPart.Close()
			return fmt.Errorf("lockd: decode subscribe meta: %w", err)
		}
		metaPart.Close()
		if apiResp.Message == nil {
			continue
		}

		payloadPart, err := mr.NextPart()
		var payloadBytes []byte
		if err == io.EOF {
			payloadBytes = nil
		} else if err != nil {
			return fmt.Errorf("lockd: subscribe payload part: %w", err)
		} else {
			if name := payloadPart.FormName(); name != "" && name != "payload" {
				payloadPart.Close()
				return fmt.Errorf("lockd: unexpected multipart part %q", name)
			}
			if lengthHeader := payloadPart.Header.Get("Content-Length"); lengthHeader != "" {
				if size, parseErr := strconv.ParseInt(lengthHeader, 10, 64); parseErr == nil && size >= 0 {
					payloadBytes = make([]byte, size)
					if _, err := io.ReadFull(payloadPart, payloadBytes); err != nil {
						payloadPart.Close()
						return fmt.Errorf("lockd: read subscribe payload (len=%d): %w", size, err)
					}
				} else {
					payloadBytes, err = io.ReadAll(payloadPart)
					payloadPart.Close()
					if err != nil {
						return fmt.Errorf("lockd: read subscribe payload: %w", err)
					}
				}
			} else {
				payloadBytes, err = io.ReadAll(payloadPart)
				payloadPart.Close()
				if err != nil {
					return fmt.Errorf("lockd: read subscribe payload: %w", err)
				}
			}
		}

		if apiResp.Message.CorrelationID == "" {
			if headerCID != "" {
				apiResp.Message.CorrelationID = headerCID
			} else if cid := CorrelationIDFromContext(ctx); cid != "" {
				apiResp.Message.CorrelationID = cid
			}
		}

		payloadRC := io.NopCloser(bytes.NewReader(payloadBytes))
		handle := &QueueMessageHandle{
			client:        c,
			msg:           *apiResp.Message,
			cursor:        apiResp.NextCursor,
			payloadStream: payloadRC,
		}
		if handle.msg.LeaseID != "" {
			c.RegisterLeaseToken(handle.msg.LeaseID, strconv.FormatInt(handle.msg.FencingToken, 10))
		}
		if stateful && handle.msg.StateLeaseID != "" {
			handle.state = &QueueStateHandle{
				client:         c,
				queue:          queue,
				messageID:      handle.msg.MessageID,
				leaseID:        handle.msg.StateLeaseID,
				fencingToken:   handle.msg.StateFencingToken,
				leaseExpiresAt: handle.msg.StateLeaseExpiresAtUnix,
				stateETag:      handle.msg.StateETag,
				correlationID:  handle.msg.CorrelationID,
			}
			c.RegisterLeaseToken(handle.state.leaseID, strconv.FormatInt(handle.state.fencingToken, 10))
		}

		msg := newQueueMessage(handle, nil, opts.OnCloseDelay)
		var handlerErr error
		if stateful {
			handlerErr = stateHandler(ctx, msg, msg.StateHandle())
		} else {
			handlerErr = handler(ctx, msg)
		}
		if handlerErr != nil {
			return handlerErr
		}
		delivered++
	}

	endFields := []any{
		"queue", queue,
		"owner", owner,
		"stateful", stateful,
		"endpoint", endpoint,
		"delivered", delivered,
	}
	c.logInfoCtx(ctx, "client.queue.subscribe.complete", endFields...)
	return nil
}

func buildHTTPClient(rawBase string) (*http.Client, string, error) {
	trimmed := strings.TrimSpace(rawBase)
	if trimmed == "" {
		return nil, "", fmt.Errorf("baseURL required")
	}
	if strings.HasPrefix(trimmed, "unix://") {
		cli, base, err := newUnixHTTPClient(trimmed)
		if err != nil {
			return nil, "", err
		}
		return cli, base, nil
	}
	trimmed = strings.TrimRight(trimmed, "/")
	return &http.Client{}, trimmed, nil
}

func newUnixHTTPClient(raw string) (*http.Client, string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, "", fmt.Errorf("parse unix baseURL: %w", err)
	}
	socketPath := u.Path
	if u.Host != "" {
		if socketPath == "" || socketPath == "/" {
			socketPath = "/" + u.Host
		} else {
			socketPath = "/" + u.Host + socketPath
		}
	}
	if socketPath == "" {
		return nil, "", fmt.Errorf("unix baseURL missing socket path")
	}
	query := u.Query()
	basePath := strings.TrimRight(query.Get("path"), "/")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{Timeout: defaultHTTPTimeout, KeepAlive: 15 * time.Second}
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", socketPath)
	}
	transport.DialTLSContext = nil
	transport.TLSClientConfig = nil
	client := &http.Client{Transport: transport}
	base := "http://unix"
	if basePath != "" {
		if !strings.HasPrefix(basePath, "/") {
			basePath = "/" + basePath
		}
		base += basePath
	}
	return client, base, nil
}
