package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
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
	"path"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/pathutil"
	"pkt.systems/lockd/internal/svcfields"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/lql"
	"pkt.systems/pslog"
)

// EnqueueOptions controls enqueue behaviour.
type EnqueueOptions struct {
	// Namespace scopes the queue operation. Empty uses the client's default namespace.
	Namespace string
	// Delay postpones first visibility after enqueue.
	Delay time.Duration
	// Visibility controls how long a dequeued message stays hidden before it can be redelivered.
	Visibility time.Duration
	// TTL sets message retention. Zero uses server defaults.
	TTL time.Duration
	// MaxAttempts limits delivery attempts before dead-letter handling.
	MaxAttempts int
	// Attributes stores arbitrary JSON-serializable metadata on the message.
	Attributes map[string]any
	// ContentType is sent as the payload media type. Empty defaults to application/octet-stream.
	ContentType string
}

// DequeueOptions guides dequeue behaviour.
type DequeueOptions struct {
	// Namespace scopes the queue operation. Empty uses the client's default namespace.
	Namespace string
	// Owner identifies the worker/consumer acquiring the queue lease.
	Owner string
	// TxnID binds queue state operations to an existing transaction when required.
	TxnID string
	// Visibility controls the message lease timeout returned by dequeue.
	Visibility time.Duration
	// BlockSeconds controls long-poll behavior: BlockNoWait (-1) for immediate return,
	// 0 to wait indefinitely, and >0 to wait up to that many seconds.
	BlockSeconds int64
	// PageSize caps batched dequeue result size (for APIs that support multi-message responses).
	PageSize int
	// StartAfter resumes dequeue scanning from a server-issued cursor.
	StartAfter string
	// OnCloseDelay applies a delay before auto-nack when QueueMessage.Close is called without ack.
	OnCloseDelay time.Duration
}

// SubscribeOptions configures continuous streaming consumption via Subscribe.
type SubscribeOptions struct {
	// Namespace scopes the queue operation. Empty uses the client's default namespace.
	Namespace string
	// Owner identifies the worker/consumer processing streamed deliveries.
	// For direct Subscribe calls, this is required. StartConsumer auto-fills a
	// generated unique owner when left empty.
	Owner string
	// Visibility controls per-message lease timeout for streamed deliveries.
	Visibility time.Duration
	// BlockSeconds controls server-side wait behavior between deliveries.
	BlockSeconds int64
	// Prefetch controls how many messages the server may pipeline before handler ack/nack.
	Prefetch int
	// StartAfter resumes a previous subscription stream from a cursor.
	StartAfter string
	// OnCloseDelay applies a delay before auto-nack when handlers close without ack.
	OnCloseDelay time.Duration
}

// ConsumerMessage bundles the runtime context provided to ConsumerMessageHandler.
// The same handler can be reused across multiple ConsumerConfig entries and inspect
// Queue/WithState to branch behavior.
type ConsumerMessage struct {
	// Client is the active SDK client used by StartConsumer, allowing handlers to
	// perform additional lockd operations (enqueue, acquire, queries, etc.)
	// without constructing a second client.
	Client *Client
	// Logger is the client logger configured via WithLogger.
	// It is always non-nil (defaults to pslog.NoopLogger()).
	Logger pslog.Base
	// Queue is the subscribed queue name for this delivery.
	Queue string
	// WithState indicates whether this delivery came from a stateful subscription.
	WithState bool
	// Message is the leased queue delivery payload/metadata wrapper.
	Message *QueueMessage
	// State is the workflow state lease handle for stateful subscriptions.
	// It is nil when WithState is false.
	State *QueueStateHandle

	consumerName string
}

// Name returns the logical consumer name resolved from ConsumerConfig.Name,
// defaulting to Queue when no explicit name was configured.
func (m ConsumerMessage) Name() string {
	name := strings.TrimSpace(m.consumerName)
	if name != "" {
		return name
	}
	return strings.TrimSpace(m.Queue)
}

// ConsumerMessageHandler handles one queue delivery produced by StartConsumer.
type ConsumerMessageHandler func(context.Context, ConsumerMessage) error

// ConsumerError describes a recoverable consumer loop failure before restart.
type ConsumerError struct {
	// Name is the logical consumer name from ConsumerConfig.
	Name string
	// Queue is the queue whose consume loop failed.
	Queue string
	// WithState indicates whether the failed loop used SubscribeWithState.
	WithState bool
	// Attempt is the current consecutive failure count for this consumer.
	Attempt int
	// RestartIn is the delay before the next subscribe attempt.
	RestartIn time.Duration
	// Err is the underlying failure returned by Subscribe/SubscribeWithState.
	Err error
}

// ConsumerErrorHandler is invoked when a consume loop fails and is about to be
// restarted. Returning nil continues restart handling. Returning a non-nil error
// stops StartConsumer and returns that error (wrapped with queue context).
type ConsumerErrorHandler func(context.Context, ConsumerError) error

// ConsumerLifecycleEvent describes lifecycle transitions for one consumer.
type ConsumerLifecycleEvent struct {
	// Name is the logical consumer name from ConsumerConfig.
	Name string
	// Queue is the queue this lifecycle event belongs to.
	Queue string
	// WithState indicates whether this consumer uses SubscribeWithState.
	WithState bool
	// Attempt is the 1-based subscribe attempt sequence for this consumer.
	Attempt int
	// Err is the terminal error for the attempt. It is nil for OnStart and for
	// clean attempt completion.
	Err error
}

// ConsumerRestartPolicy configures restart behavior for failed consume loops.
type ConsumerRestartPolicy struct {
	// ImmediateRetries is the number of consecutive failures retried with zero
	// delay before exponential backoff starts.
	ImmediateRetries int
	// BaseDelay is the first delayed retry duration once immediate retries are exhausted.
	BaseDelay time.Duration
	// MaxDelay caps the restart delay.
	MaxDelay time.Duration
	// Multiplier controls exponential growth between delayed retries.
	Multiplier float64
	// Jitter randomizes delay by +/- Jitter to reduce synchronized retries.
	Jitter time.Duration
	// MaxFailures optionally stops the consumer after N consecutive failures.
	// Zero or negative means retry forever.
	MaxFailures int
}

// ConsumerConfig describes one queue consumer managed by StartConsumer.
type ConsumerConfig struct {
	// Name labels this consumer in logs and lifecycle/error callbacks.
	// Empty defaults to Queue.
	Name string
	// Queue is the queue name to subscribe to.
	Queue string
	// Options configures subscription behavior (namespace, owner, prefetch, etc.).
	// When Options.Owner is empty, StartConsumer generates a unique owner value.
	Options SubscribeOptions
	// WithState switches between Subscribe (false) and SubscribeWithState (true).
	WithState bool
	// MessageHandler processes each delivered message.
	MessageHandler ConsumerMessageHandler
	// ErrorHandler observes subscribe failures before restart.
	// When nil, StartConsumer logs and continues.
	ErrorHandler ConsumerErrorHandler
	// OnStart runs when a consumer subscribe attempt starts.
	OnStart func(context.Context, ConsumerLifecycleEvent)
	// OnStop runs when a consumer subscribe attempt stops (success, context
	// cancellation, or error).
	OnStop func(context.Context, ConsumerLifecycleEvent)
	// RestartPolicy controls retry/backoff behavior after failures.
	RestartPolicy ConsumerRestartPolicy
}

// DequeueResult captures the outcome of a dequeue request.
type DequeueResult struct {
	// Message is the primary dequeued message handle for single-message consumption paths.
	Message *QueueMessageHandle
	// Messages contains all dequeued message handles when batch dequeue is requested.
	Messages []*QueueMessageHandle
	// NextCursor is the server cursor for continuing dequeue scans.
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
	namespace      string
	stateKey       string
	queue          string
	messageID      string
	leaseID        string
	txnID          string
	fencingToken   int64
	leaseExpiresAt int64
	queueStateETag string
	stateETag      string
	version        int64
	correlationID  string
	mu             sync.Mutex
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

// Namespace returns the namespace associated with the message.
func (h *QueueMessageHandle) Namespace() string {
	if h == nil {
		return ""
	}
	return h.msg.Namespace
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
		Namespace:    h.msg.Namespace,
		Queue:        h.msg.Queue,
		MessageID:    h.msg.MessageID,
		LeaseID:      h.msg.LeaseID,
		TxnID:        h.msg.TxnID,
		FencingToken: h.msg.FencingToken,
		MetaETag:     h.msg.MetaETag,
		StateETag:    h.msg.StateETag,
	}
	if h.state != nil {
		stateLeaseID, stateFencingToken, stateETag := h.state.stateForQueueOps()
		req.StateLeaseID = stateLeaseID
		req.StateFencingToken = stateFencingToken
		if stateETag != "" {
			req.StateETag = stateETag
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
		Namespace:    h.msg.Namespace,
		Queue:        h.msg.Queue,
		MessageID:    h.msg.MessageID,
		LeaseID:      h.msg.LeaseID,
		TxnID:        h.msg.TxnID,
		FencingToken: h.msg.FencingToken,
		MetaETag:     h.msg.MetaETag,
		DelaySeconds: secondsFromDuration(delay),
		LastError:    lastErr,
	}
	if h.state != nil {
		stateLeaseID, stateFencingToken, _ := h.state.stateForQueueOps()
		req.StateLeaseID = stateLeaseID
		req.StateFencingToken = stateFencingToken
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
		Namespace:       h.msg.Namespace,
		Queue:           h.msg.Queue,
		MessageID:       h.msg.MessageID,
		LeaseID:         h.msg.LeaseID,
		TxnID:           h.msg.TxnID,
		FencingToken:    h.msg.FencingToken,
		MetaETag:        h.msg.MetaETag,
		ExtendBySeconds: secondsFromDuration(extendBy),
	}
	if h.state != nil {
		stateLeaseID, stateFencingToken, _ := h.state.stateForQueueOps()
		req.StateLeaseID = stateLeaseID
		req.StateFencingToken = stateFencingToken
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
			h.state.setLeaseExpiresAt(res.StateLeaseExpiresAtUnix)
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

// Namespace returns the namespace associated with the message.
func (m *QueueMessage) Namespace() string {
	if m == nil || m.handle == nil {
		return ""
	}
	return m.handle.Namespace()
}

// TxnID returns the transaction id associated with the message lease, if any.
func (m *QueueMessage) TxnID() string {
	if m == nil || m.handle == nil {
		return ""
	}
	return m.handle.msg.TxnID
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

// WritePayloadTo streams the payload into w and closes the payload afterwards.
func (m *QueueMessage) WritePayloadTo(w io.Writer) (int64, error) {
	if m == nil {
		return 0, fmt.Errorf("lockd: nil queue message")
	}
	reader, err := m.PayloadReader()
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	return io.Copy(w, reader)
}

// DecodePayloadJSON decodes the payload JSON into v and closes the payload afterwards.
func (m *QueueMessage) DecodePayloadJSON(v any) error {
	if m == nil {
		return fmt.Errorf("lockd: nil queue message")
	}
	reader, err := m.PayloadReader()
	if err != nil {
		return err
	}
	defer reader.Close()
	return json.NewDecoder(reader).Decode(v)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queue
}

// MessageID returns the associated message ID.
func (s *QueueStateHandle) MessageID() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.messageID
}

// LeaseID returns the workflow state lease identifier.
func (s *QueueStateHandle) LeaseID() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaseID
}

// FencingToken returns the workflow state fencing token.
func (s *QueueStateHandle) FencingToken() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fencingToken
}

// LeaseExpiresAt returns the unix timestamp when the state lease expires.
func (s *QueueStateHandle) LeaseExpiresAt() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaseExpiresAt
}

// ETag returns the workflow state ETag.
func (s *QueueStateHandle) ETag() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateETag
}

// CorrelationID returns the message correlation identifier associated with the state lease.
func (s *QueueStateHandle) CorrelationID() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.correlationID
}

func queueStateKey(queue, messageID string) string {
	queue = strings.TrimSpace(queue)
	messageID = strings.TrimSpace(messageID)
	if queue == "" || messageID == "" {
		return ""
	}
	return path.Join("q", queue, "state", messageID)
}

func (s *QueueStateHandle) snapshot() (*Client, string, string, string, string, string, string, int64, error) {
	if s == nil {
		return nil, "", "", "", "", "", "", 0, fmt.Errorf("lockd: nil queue state handle")
	}
	s.mu.Lock()
	cli := s.client
	namespace := strings.TrimSpace(s.namespace)
	key := strings.TrimSpace(s.stateKey)
	if key == "" {
		key = queueStateKey(s.queue, s.messageID)
	}
	leaseID := strings.TrimSpace(s.leaseID)
	txnID := strings.TrimSpace(s.txnID)
	correlationID := strings.TrimSpace(s.correlationID)
	stateETag := strings.TrimSpace(s.stateETag)
	version := s.version
	s.mu.Unlock()
	if cli == nil {
		return nil, "", "", "", "", "", "", 0, fmt.Errorf("lockd: queue state client nil")
	}
	if namespace == "" {
		namespace = cli.Namespace()
	}
	if key == "" {
		return nil, "", "", "", "", "", "", 0, fmt.Errorf("lockd: queue state key required")
	}
	if leaseID == "" {
		return nil, "", "", "", "", "", "", 0, fmt.Errorf("lockd: queue state lease_id required")
	}
	return cli, namespace, key, leaseID, txnID, correlationID, stateETag, version, nil
}

func (s *QueueStateHandle) setLeaseExpiresAt(leaseExpiresAt int64) {
	if s == nil || leaseExpiresAt <= 0 {
		return
	}
	s.mu.Lock()
	s.leaseExpiresAt = leaseExpiresAt
	s.mu.Unlock()
}

func (s *QueueStateHandle) stateForQueueOps() (string, int64, string) {
	if s == nil {
		return "", 0, ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaseID, s.fencingToken, s.queueStateETag
}

func (s *QueueStateHandle) updateAfterMutation(newStateETag string, newVersion int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if newStateETag != "" {
		s.stateETag = newStateETag
	}
	if newVersion > 0 {
		s.version = newVersion
	}
	s.mu.Unlock()
}

func (s *QueueStateHandle) clearStateAfterRemove(newVersion int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if newVersion > 0 {
		s.version = newVersion
	}
	s.stateETag = ""
	s.mu.Unlock()
}

func (s *QueueStateHandle) syncFencingToken() string {
	if s == nil || s.client == nil {
		return ""
	}
	leaseID := s.LeaseID()
	if leaseID == "" {
		return ""
	}
	token, err := s.client.fencingToken(leaseID, "")
	if err != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.fencingToken <= 0 {
			return ""
		}
		return strconv.FormatInt(s.fencingToken, 10)
	}
	if parsed, parseErr := strconv.ParseInt(token, 10, 64); parseErr == nil {
		s.mu.Lock()
		s.fencingToken = parsed
		s.mu.Unlock()
	}
	return token
}

// Get reads the current workflow state snapshot for the message's state lease.
func (s *QueueStateHandle) Get(ctx context.Context) (*StateSnapshot, error) {
	cli, namespace, key, leaseID, _, corr, _, _, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	ctx = WithCorrelationID(ctx, corr)
	resp, err := cli.Get(ctx, key,
		WithGetNamespace(namespace),
		WithGetLeaseID(leaseID),
		WithGetPublicDisabled(true),
	)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	defer resp.Close()
	reader := resp.detachReader()
	etag := resp.ETag
	snap := &StateSnapshot{Reader: reader, ETag: etag, HasState: resp.HasState || etag != ""}
	if ver := strings.TrimSpace(resp.Version); ver != "" {
		if v, parseErr := strconv.ParseInt(ver, 10, 64); parseErr == nil {
			snap.Version = v
		}
		if snap.Version > 0 {
			snap.HasState = true
		}
	}
	if snap.HasState {
		s.updateAfterMutation(etag, snap.Version)
	} else {
		s.clearStateAfterRemove(snap.Version)
	}
	s.syncFencingToken()
	return snap, nil
}

// GetBytes returns the current workflow state payload as bytes.
func (s *QueueStateHandle) GetBytes(ctx context.Context) ([]byte, error) {
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

// Load unmarshals the current workflow state into v.
func (s *QueueStateHandle) Load(ctx context.Context, v any) error {
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

// Update streams new workflow state JSON while preserving the state lease.
func (s *QueueStateHandle) Update(ctx context.Context, body io.Reader, opts ...UpdateOption) (*UpdateResult, error) {
	_, _, _, _, _, _, etag, version, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	options := UpdateOptions{
		IfETag: etag,
	}
	if version > 0 {
		options.IfVersion = strconv.FormatInt(version, 10)
	}
	options.FencingToken = s.syncFencingToken()
	if len(opts) > 0 {
		options = applyUpdateOptions(options, opts)
	}
	return s.UpdateWithOptions(ctx, body, options)
}

// UpdateBytes is a convenience wrapper around Update for in-memory payloads.
func (s *QueueStateHandle) UpdateBytes(ctx context.Context, body []byte, opts ...UpdateOption) (*UpdateResult, error) {
	return s.Update(ctx, bytes.NewReader(body), opts...)
}

// UpdateWithOptions updates workflow state while allowing conditional/header overrides.
func (s *QueueStateHandle) UpdateWithOptions(ctx context.Context, body io.Reader, opts UpdateOptions) (*UpdateResult, error) {
	cli, namespace, key, leaseID, txnID, corr, etag, version, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	ctx = WithCorrelationID(ctx, corr)
	if opts.IfETag == "" {
		opts.IfETag = etag
	}
	if opts.IfVersion == "" && version > 0 {
		opts.IfVersion = strconv.FormatInt(version, 10)
	}
	if opts.FencingToken == "" {
		opts.FencingToken = s.syncFencingToken()
	}
	if opts.Namespace == "" {
		opts.Namespace = namespace
	}
	if opts.TxnID == "" {
		opts.TxnID = txnID
	}
	res, err := cli.Update(ctx, key, leaseID, body, opts)
	if err != nil {
		return nil, err
	}
	if res != nil {
		s.updateAfterMutation(res.NewStateETag, res.NewVersion)
	}
	s.syncFencingToken()
	return res, nil
}

// Save marshals v as JSON and updates workflow state through the state lease.
func (s *QueueStateHandle) Save(ctx context.Context, v any, opts ...UpdateOption) error {
	var body io.Reader
	switch src := v.(type) {
	case *Document:
		if src == nil {
			return fmt.Errorf("lockd: document nil")
		}
		data, err := src.Bytes()
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	case io.Reader:
		body = src
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	_, err := s.Update(ctx, body, opts...)
	return err
}

// UpdateMetadata mutates metadata for the workflow state key.
func (s *QueueStateHandle) UpdateMetadata(ctx context.Context, meta MetadataOptions) (*MetadataResult, error) {
	if meta.empty() {
		return nil, fmt.Errorf("lockd: metadata options required")
	}
	cli, namespace, key, leaseID, txnID, corr, _, version, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	ctx = WithCorrelationID(ctx, corr)
	opts := UpdateOptions{
		Namespace: namespace,
		Metadata:  meta,
	}
	if opts.TxnID == "" {
		opts.TxnID = txnID
	}
	if version > 0 {
		opts.IfVersion = strconv.FormatInt(version, 10)
	}
	opts.FencingToken = s.syncFencingToken()
	res, err := cli.UpdateMetadata(ctx, key, leaseID, opts)
	if err != nil {
		return nil, err
	}
	s.syncFencingToken()
	return res, nil
}

// Remove deletes workflow state while preserving queue lease lifecycle semantics.
func (s *QueueStateHandle) Remove(ctx context.Context) (*api.RemoveResponse, error) {
	_, _, _, _, _, _, etag, version, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	opts := RemoveOptions{
		IfETag: etag,
	}
	if version > 0 {
		opts.IfVersion = strconv.FormatInt(version, 10)
	}
	opts.FencingToken = s.syncFencingToken()
	return s.RemoveWithOptions(ctx, opts)
}

// RemoveWithOptions deletes workflow state with optional conditional overrides.
func (s *QueueStateHandle) RemoveWithOptions(ctx context.Context, opts RemoveOptions) (*api.RemoveResponse, error) {
	cli, namespace, key, leaseID, txnID, corr, etag, version, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	ctx = WithCorrelationID(ctx, corr)
	if opts.IfETag == "" {
		opts.IfETag = etag
	}
	if opts.IfVersion == "" && version > 0 {
		opts.IfVersion = strconv.FormatInt(version, 10)
	}
	if opts.FencingToken == "" {
		opts.FencingToken = s.syncFencingToken()
	}
	if opts.Namespace == "" {
		opts.Namespace = namespace
	}
	if opts.TxnID == "" {
		opts.TxnID = txnID
	}
	res, err := cli.Remove(ctx, key, leaseID, opts)
	if err != nil {
		return nil, err
	}
	if res != nil {
		s.clearStateAfterRemove(res.NewVersion)
	}
	s.syncFencingToken()
	return res, nil
}

const headerFencingToken = "X-Fencing-Token"
const headerTxnID = "X-Txn-ID"
const headerQueryCursor = "X-Lockd-Query-Cursor"
const headerQueryIndexSeq = "X-Lockd-Query-Index-Seq"
const headerQueryMetadata = "X-Lockd-Query-Metadata"
const headerQueryReturn = "X-Lockd-Query-Return"
const contentTypeNDJSON = "application/x-ndjson"
const headerShutdownImminent = "Shutdown-Imminent"
const (
	minQueueAutoExtendInterval = 250 * time.Millisecond
	maxQueueAutoExtendInterval = 30 * time.Second
	defaultQueueAutoExtendTick = 15 * time.Second
)
const (
	headerCorrelationID       = "X-Correlation-Id"
	headerMetadataQueryHidden = "X-Lockd-Meta-Query-Hidden"
)

// ErrMissingFencingToken is returned when an operation needs a fencing token but none was found.
var ErrMissingFencingToken = errors.New("lockd: fencing token required")

var (
	acquireRandMu    sync.Mutex
	acquireRand      = rand.New(rand.NewSource(time.Now().UnixNano()))
	consumerOwnerSeq atomic.Uint64
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
	c.logTraceCtx(context.Background(), msg, keyvals...)
}

func (c *Client) logDebug(msg string, keyvals ...any) {
	c.logDebugCtx(context.Background(), msg, keyvals...)
}

func (c *Client) logInfo(msg string, keyvals ...any) {
	c.logInfoCtx(context.Background(), msg, keyvals...)
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
	if c.logger == nil {
		c.logger = pslog.NoopLogger()
	}
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
	ownedTransport := false
	if c.httpClient.Transport == nil {
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			c.httpClient.Transport = base.Clone()
			ownedTransport = true
		}
	}
	if ownedTransport {
		if tr, ok := c.httpClient.Transport.(*http.Transport); ok {
			applyDefaultTransportTuning(tr)
		}
	}
	if !c.disableMTLS && strings.TrimSpace(c.bundlePath) != "" {
		bundlePath := c.bundlePath
		if !c.bundlePathDisableExpansion {
			bundlePath, err = pathutil.ExpandUserAndEnv(bundlePath)
			if err != nil {
				return fmt.Errorf("lockd: expand client bundle path %q: %w", c.bundlePath, err)
			}
			c.bundlePath = bundlePath
		}
		clientBundle, err := tlsutil.LoadClientBundle(bundlePath)
		if err != nil {
			return fmt.Errorf("lockd: load client bundle %s: %w", bundlePath, err)
		}
		tr, ok := c.httpClient.Transport.(*http.Transport)
		if !ok || tr == nil {
			return fmt.Errorf("lockd: with bundle path requires *http.Transport, got %T", c.httpClient.Transport)
		}
		cloned := tr.Clone()
		cloned.TLSClientConfig = buildClientTLS(clientBundle)
		c.httpClient.Transport = cloned
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
	if c.defaultNamespace == "" {
		c.defaultsMu.Lock()
		c.defaultNamespace = namespaces.Default
		c.defaultsMu.Unlock()
	} else {
		normalized, err := namespaces.Normalize(c.defaultNamespace, namespaces.Default)
		if err != nil {
			return fmt.Errorf("lockd: client default namespace: %w", err)
		}
		c.defaultsMu.Lock()
		c.defaultNamespace = normalized
		c.defaultsMu.Unlock()
	}
	c.endpoints = normalized
	c.lastEndpoint = normalized[0]
	c.logInfo("client.init", "endpoints", normalized)
	return nil
}

func (c *Client) namespaceFor(ns string) (string, error) {
	return namespaces.Normalize(ns, c.Namespace())
}

// Default client tuning knobs exposed for callers that want to mirror lockd's defaults.
const (
	DefaultHTTPTimeout           = 15 * time.Second
	DefaultCloseTimeout          = 5 * time.Second
	DefaultKeepAliveTimeout      = 5 * time.Second
	DefaultMaxIdleConns          = 256
	DefaultMaxIdleConnsPerHost   = 128
	DefaultForUpdateTimeout      = 15 * time.Minute
	DefaultAcquireBaseDelay      = time.Second
	DefaultAcquireMaxDelay       = 5 * time.Second
	DefaultAcquireMultiplier     = 1.2
	DefaultAcquireJitter         = 100 * time.Millisecond
	DefaultFailureRetries        = 5
	DefaultAcquireFailureRetries = DefaultFailureRetries
	// DefaultConsumerImmediateRetries controls how many consecutive consumer
	// failures are retried without delay before backoff starts.
	DefaultConsumerImmediateRetries = 3
	// DefaultConsumerBaseDelay is the first delayed retry duration after immediate retries.
	DefaultConsumerBaseDelay = 250 * time.Millisecond
	// DefaultConsumerMaxDelay caps consumer restart backoff growth.
	DefaultConsumerMaxDelay = 5 * time.Minute
	// DefaultConsumerMultiplier is the exponential growth factor for restart delay.
	DefaultConsumerMultiplier = 2.0
	// DefaultConsumerJitter randomizes restart delay by +/- this duration.
	DefaultConsumerJitter = 0 * time.Second
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

func applyDefaultTransportTuning(tr *http.Transport) {
	if tr == nil {
		return
	}
	if tr.MaxIdleConns < DefaultMaxIdleConns {
		tr.MaxIdleConns = DefaultMaxIdleConns
	}
	if tr.MaxIdleConnsPerHost < DefaultMaxIdleConnsPerHost {
		tr.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	}
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
	drainTriggered atomic.Bool
	attachmentsMu  sync.Mutex
	attachments    map[io.ReadCloser]struct{}
}

// ReleaseOptions controls commit vs rollback semantics during lease release.
type ReleaseOptions struct {
	// Rollback discards staged state/attachments instead of committing them.
	Rollback bool
}

// AcquireForUpdateHandler is invoked while a lease is held. The provided context
// is canceled if the client loses the lease or keepalive fails.
type AcquireForUpdateHandler func(context.Context, *AcquireForUpdateContext) error

// AcquireForUpdateContext exposes the active lease session and the snapshot that
// was read before the handler executed.
type AcquireForUpdateContext struct {
	// Session is the active lease context used for updates/removals/metadata changes.
	Session *LeaseSession
	// State is the pre-handler snapshot fetched after acquire and before user logic runs.
	State *StateSnapshot
}

func (a *AcquireForUpdateContext) session() (*LeaseSession, error) {
	if a == nil || a.Session == nil {
		return nil, fmt.Errorf("lockd: acquire-for-update session closed")
	}
	return a.Session, nil
}

// Update streams a new JSON document via Update, preserving the lease.
func (a *AcquireForUpdateContext) Update(ctx context.Context, body io.Reader, opts ...UpdateOption) (*UpdateResult, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.Update(ctx, body, opts...)
}

// UpdateBytes is a convenience wrapper over Update that accepts a byte slice.
func (a *AcquireForUpdateContext) UpdateBytes(ctx context.Context, body []byte, opts ...UpdateOption) (*UpdateResult, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.UpdateBytes(ctx, body, opts...)
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
func (a *AcquireForUpdateContext) Save(ctx context.Context, v any, opts ...UpdateOption) error {
	sess, err := a.session()
	if err != nil {
		return err
	}
	return sess.Save(ctx, v, opts...)
}

// UpdateMetadata mutates metadata for the active key.
func (a *AcquireForUpdateContext) UpdateMetadata(ctx context.Context, meta MetadataOptions) (*MetadataResult, error) {
	sess, err := a.session()
	if err != nil {
		return nil, err
	}
	return sess.UpdateMetadata(ctx, meta)
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
	// Reader streams the state payload. It may be nil when HasState is false.
	Reader io.ReadCloser
	// ETag is the backend entity tag for CAS-protected writes/deletes.
	ETag string
	// Version is lockd's monotonic version counter for the key.
	Version int64
	// HasState reports whether the key currently has a committed JSON document.
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
func (s *LeaseSession) Update(ctx context.Context, body io.Reader, options ...UpdateOption) (*UpdateResult, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	opts := UpdateOptions{
		IfETag: s.StateETag,
	}
	if s.Version > 0 {
		opts.IfVersion = strconv.FormatInt(s.Version, 10)
	}
	opts.FencingToken = s.fencing()
	if len(options) > 0 {
		opts = applyUpdateOptions(opts, options)
	}
	return s.applyUpdate(ctx, body, opts)
}

// UpdateBytes is a convenience wrapper around Update that accepts an in-memory payload.
func (s *LeaseSession) UpdateBytes(ctx context.Context, body []byte, options ...UpdateOption) (*UpdateResult, error) {
	return s.Update(ctx, bytes.NewReader(body), options...)
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
	if opts.Namespace == "" {
		opts.Namespace = s.Namespace
	}
	if opts.TxnID == "" {
		opts.TxnID = s.TxnID
	}
	res, err := s.client.updateWithPreferred(ctx, s.Key, s.LeaseID, body, opts, s.endpoint)
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
	if opts.Namespace == "" {
		opts.Namespace = s.Namespace
	}
	if opts.TxnID == "" {
		opts.TxnID = s.TxnID
	}
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
	resp, err := s.client.Get(ctx, s.Key,
		WithGetNamespace(s.Namespace),
		WithGetLeaseID(s.LeaseID),
		WithGetPublicDisabled(true),
	)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	defer resp.Close()
	reader := resp.detachReader()
	etag := resp.ETag
	snap := &StateSnapshot{Reader: reader, ETag: etag, HasState: resp.HasState || etag != ""}
	if ver := strings.TrimSpace(resp.Version); ver != "" {
		if v, parseErr := strconv.ParseInt(ver, 10, 64); parseErr == nil {
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
func (s *LeaseSession) Save(ctx context.Context, v any, opts ...UpdateOption) error {
	var body io.Reader
	switch src := v.(type) {
	case *Document:
		if src == nil {
			return fmt.Errorf("lockd: document nil")
		}
		data, err := src.Bytes()
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	case io.Reader:
		body = src
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	_, err := s.Update(ctx, body, opts...)
	return err
}

// UpdateMetadata toggles metadata flags without mutating the JSON state.
func (s *LeaseSession) UpdateMetadata(ctx context.Context, meta MetadataOptions) (*MetadataResult, error) {
	if meta.empty() {
		return nil, fmt.Errorf("lockd: metadata options required")
	}
	ctx = WithCorrelationID(ctx, s.correlation())
	opts := UpdateOptions{
		Namespace: s.Namespace,
		Metadata:  meta,
	}
	if opts.TxnID == "" {
		opts.TxnID = s.TxnID
	}
	if s.Version > 0 {
		opts.IfVersion = strconv.FormatInt(s.Version, 10)
	}
	opts.FencingToken = s.fencing()
	return s.client.updateMetadataWithPreferred(ctx, s.Key, s.LeaseID, opts, s.endpoint)
}

// KeepAlive extends the lease TTL without altering the stored state.
func (s *LeaseSession) KeepAlive(ctx context.Context, ttl time.Duration) (*api.KeepAliveResponse, error) {
	ctx = WithCorrelationID(ctx, s.correlation())
	req := api.KeepAliveRequest{
		Namespace:  s.Namespace,
		Key:        s.Key,
		LeaseID:    s.LeaseID,
		TTLSeconds: int64(ttl.Seconds()),
		TxnID:      s.TxnID,
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
// It commits staged changes by default. Use ReleaseWithOptions to request a rollback.
func (s *LeaseSession) Release(ctx context.Context) error {
	return s.ReleaseWithOptions(ctx, ReleaseOptions{})
}

// ReleaseWithOptions relinquishes the lease and allows callers to rollback staged changes.
func (s *LeaseSession) ReleaseWithOptions(ctx context.Context, opts ReleaseOptions) error {
	ctx = WithCorrelationID(ctx, s.correlation())
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	req := api.ReleaseRequest{
		Namespace: s.Namespace,
		Key:       s.Key,
		LeaseID:   s.LeaseID,
		TxnID:     s.TxnID,
		Rollback:  opts.Rollback,
	}
	resp, err := s.client.releaseInternal(ctx, req, s.endpoint)
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
	s.client.ClearLeaseID(s.LeaseID)
	s.closeAttachments()
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

// GetResponse encapsulates the payload and headers returned by Client.Get.
type GetResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string
	// Key identifies the lock/state key within the namespace.
	Key string
	// ETag is the entity tag used for optimistic concurrency and cache validation.
	ETag string
	// Version is the server version header for the returned key state.
	Version string
	// HasState reports whether the requested key currently has committed state.
	HasState bool
	reader   io.ReadCloser
	client   *Client
	public   bool
}

// QueryReturn describes the payload mode exposed by /v1/query.
type QueryReturn string

const (
	// QueryReturnKeys streams the default keys-only JSON object.
	QueryReturnKeys QueryReturn = QueryReturn(api.QueryReturnKeys)
	// QueryReturnDocuments streams newline-delimited documents.
	QueryReturnDocuments QueryReturn = QueryReturn(api.QueryReturnDocuments)
)

var errDocumentMissing = errors.New("lockd: document unavailable (keys mode)")

func (mode QueryReturn) normalize() QueryReturn {
	switch strings.ToLower(strings.TrimSpace(string(mode))) {
	case "":
		return ""
	case string(QueryReturnKeys):
		return QueryReturnKeys
	case string(QueryReturnDocuments), "document", "docs":
		return QueryReturnDocuments
	default:
		return ""
	}
}

// QueryResponse describes the result set returned by Client.Query.
type QueryResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string
	// Cursor is the pagination cursor returned by the query engine.
	Cursor string
	// IndexSeq is the index sequence observed by the query execution.
	IndexSeq uint64
	// Metadata carries metadata values returned by the server for this object.
	Metadata map[string]string

	mode    QueryReturn
	keys    []string
	keyCopy []string
	stream  *queryStream
}

// Mode reports whether the query streamed keys or documents.
func (qr *QueryResponse) Mode() QueryReturn {
	if qr == nil {
		return QueryReturnKeys
	}
	if qr.mode == "" {
		return QueryReturnKeys
	}
	return qr.mode
}

// Keys returns a defensive copy of the key slice. When the response streams
// documents, Keys drains the stream, collects the keys, and closes the reader.
func (qr *QueryResponse) Keys() []string {
	if qr == nil {
		return nil
	}
	if qr.Mode() == QueryReturnDocuments {
		if len(qr.keys) == 0 && qr.stream != nil {
			var keys []string
			for {
				row, err := qr.stream.Next()
				if err == io.EOF {
					qr.Close()
					break
				}
				if err != nil {
					qr.Close()
					return nil
				}
				keys = append(keys, row.Key)
				_ = row.doc.closeSilently()
			}
			qr.keys = keys
		}
		if len(qr.keys) == 0 {
			return nil
		}
		if !slices.Equal(qr.keyCopy, qr.keys) {
			qr.keyCopy = slices.Clone(qr.keys)
		}
		return qr.keyCopy
	}
	if len(qr.keys) == 0 {
		return nil
	}
	if !slices.Equal(qr.keyCopy, qr.keys) {
		qr.keyCopy = slices.Clone(qr.keys)
	}
	return qr.keyCopy
}

// Close releases the underlying reader when the response streams documents.
func (qr *QueryResponse) Close() error {
	if qr == nil || qr.stream == nil {
		return nil
	}
	stream := qr.stream
	qr.stream = nil
	return stream.Close()
}

// ForEach invokes fn for every entry in the response. For document streams the
// handler receives fully populated QueryRow values.
func (qr *QueryResponse) ForEach(fn func(QueryRow) error) error {
	if qr == nil || fn == nil {
		return nil
	}
	if qr.Mode() != QueryReturnDocuments {
		for _, key := range qr.keys {
			if err := fn(QueryRow{Namespace: qr.Namespace, Key: key}); err != nil {
				return err
			}
		}
		return nil
	}
	if qr.stream == nil {
		return nil
	}
	for {
		row, err := qr.stream.Next()
		if err == io.EOF {
			qr.Close()
			return nil
		}
		if err != nil {
			qr.Close()
			return err
		}
		qRow := QueryRow{
			Namespace: row.Namespace,
			Key:       row.Key,
			Version:   row.Version,
			doc:       row.doc,
		}
		if err := fn(qRow); err != nil {
			qRow.release()
			qr.Close()
			return err
		}
		if err := qRow.release(); err != nil {
			qr.Close()
			return err
		}
	}
}

func newKeyQueryResponse(resp api.QueryResponse, mode QueryReturn) *QueryResponse {
	copyKeys := make([]string, len(resp.Keys))
	copy(copyKeys, resp.Keys)
	return &QueryResponse{
		Namespace: resp.Namespace,
		Cursor:    resp.Cursor,
		IndexSeq:  resp.IndexSeq,
		Metadata:  cloneStringMap(resp.Metadata),
		mode:      mode,
		keys:      copyKeys,
	}
}

func newDocumentQueryResponse(namespace, cursor string, indexSeq uint64, metadata map[string]string, reader io.ReadCloser) *QueryResponse {
	return &QueryResponse{
		Namespace: namespace,
		Cursor:    cursor,
		IndexSeq:  indexSeq,
		Metadata:  cloneStringMap(metadata),
		mode:      QueryReturnDocuments,
		stream:    newQueryStream(reader),
	}
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]string, len(source))
	for k, v := range source {
		out[k] = v
	}
	return out
}

func mergeMetadata(body map[string]string, header map[string]string) map[string]string {
	if len(header) == 0 {
		return body
	}
	out := make(map[string]string, len(body)+len(header))
	for k, v := range header {
		out[k] = v
	}
	for k, v := range body {
		out[k] = v
	}
	return out
}

func detectQueryReturn(headerValue, contentType string) QueryReturn {
	if mode := QueryReturn(headerValue).normalize(); mode != "" {
		return mode
	}
	if strings.HasPrefix(strings.ToLower(contentType), contentTypeNDJSON) {
		return QueryReturnDocuments
	}
	return QueryReturnKeys
}

func parseUintHeader(value string) uint64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
		return parsed
	}
	return 0
}

func parseQueryMetadataHeader(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

// QueryRow represents a single row returned from /v1/query.
type QueryRow struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string
	// Key identifies the lock/state key within the namespace.
	Key string
	// Version is the key version associated with this query row.
	Version int64
	doc     *streamDocumentHandle
}

// HasDocument reports whether the row was populated with a document payload.
func (row QueryRow) HasDocument() bool {
	return row.doc != nil
}

// DocumentReader returns a streaming reader for the row's document.
// Callers must Close the returned reader when finished.
func (row QueryRow) DocumentReader() (io.ReadCloser, error) {
	if row.doc == nil {
		return nil, errDocumentMissing
	}
	return row.doc.acquireReader()
}

// DocumentInto unmarshals the document payload into target when present.
func (row QueryRow) DocumentInto(target any) error {
	if target == nil {
		return nil
	}
	reader, err := row.DocumentReader()
	if err != nil {
		return err
	}
	defer reader.Close()
	return json.NewDecoder(reader).Decode(target)
}

// Document loads the payload into a client.Document.
func (row QueryRow) Document() (*Document, error) {
	reader, err := row.DocumentReader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	doc := NewDocument(row.Namespace, row.Key)
	if err := doc.LoadFrom(reader); err != nil {
		return nil, err
	}
	if row.Version > 0 {
		doc.Version = strconv.FormatInt(row.Version, 10)
	}
	return doc, nil
}

func (row QueryRow) release() error {
	if row.doc == nil {
		return nil
	}
	return row.doc.closeSilently()
}

// Reader exposes the underlying body stream. Call Close when finished.
func (gr *GetResponse) Reader() io.ReadCloser {
	if gr == nil {
		return nil
	}
	return gr.reader
}

// Bytes loads the state blob into memory and closes the underlying reader.
func (gr *GetResponse) Bytes() ([]byte, error) {
	if gr == nil || !gr.HasState {
		return nil, nil
	}
	reader := gr.detachReader()
	if reader == nil {
		return nil, nil
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// Document hydrates the response body into a Document and consumes the reader.
func (gr *GetResponse) Document() (*Document, error) {
	if gr == nil || !gr.HasState {
		return nil, nil
	}
	reader := gr.detachReader()
	if reader == nil {
		return nil, nil
	}
	defer reader.Close()
	doc := &Document{
		Namespace: gr.Namespace,
		Key:       gr.Key,
		Version:   gr.Version,
		ETag:      gr.ETag,
		Metadata:  map[string]string{},
	}
	if err := doc.LoadFrom(reader); err != nil {
		return nil, err
	}
	return doc, nil
}

// Close releases the underlying HTTP body when streaming isnt required.
func (gr *GetResponse) Close() error {
	reader := gr.detachReader()
	if reader == nil {
		return nil
	}
	return reader.Close()
}

func (gr *GetResponse) detachReader() io.ReadCloser {
	if gr == nil {
		return nil
	}
	reader := gr.reader
	gr.reader = nil
	return reader
}

// GetOptions tweaks the behaviour of Client.Get / Client.Load.
type GetOptions struct {
	// Namespace scopes the read. Empty uses the client's default namespace.
	Namespace string
	// LeaseID enforces lease-bound reads when provided.
	LeaseID string
	// DisablePublic forces authenticated/lease-backed reads instead of public fast-path reads.
	DisablePublic bool
}

// GetOption applies custom behaviour to Client.Get.
type GetOption func(*GetOptions)

// WithGetNamespace overrides namespace for Get.
// Empty values are normalized by server/client defaults.
func WithGetNamespace(ns string) GetOption {
	return func(opts *GetOptions) {
		if opts != nil {
			opts.Namespace = ns
		}
	}
}

// WithGetLeaseID sets lease id used for lease-bound reads.
// Leave unset for public/read-only snapshots when allowed.
func WithGetLeaseID(id string) GetOption {
	return func(opts *GetOptions) {
		if opts != nil {
			opts.LeaseID = id
		}
	}
}

// WithGetPublicDisabled disables public-read fallback and requires lease-backed semantics.
func WithGetPublicDisabled(disable bool) GetOption {
	return func(opts *GetOptions) {
		if opts != nil {
			opts.DisablePublic = disable
		}
	}
}

// LoadOptions mirrors GetOptions for Client.Load.
type LoadOptions struct {
	// GetOptions carries namespace/lease/public-read behavior for Load.
	GetOptions
}

// LoadOption customises Client.Load.
type LoadOption func(*LoadOptions)

// WithLoadNamespace overrides namespace for Load.
func WithLoadNamespace(ns string) LoadOption {
	return func(opts *LoadOptions) {
		if opts != nil {
			opts.Namespace = ns
		}
	}
}

// WithLoadLeaseID sets lease id for Load when lease-bound reads are required.
func WithLoadLeaseID(id string) LoadOption {
	return func(opts *LoadOptions) {
		if opts != nil {
			opts.LeaseID = id
		}
	}
}

// WithLoadPublicDisabled disables public-read fallback for Load.
func WithLoadPublicDisabled(disable bool) LoadOption {
	return func(opts *LoadOptions) {
		if opts != nil {
			opts.DisablePublic = disable
		}
	}
}

func applyGetOptions(optFns []GetOption) GetOptions {
	opts := GetOptions{}
	for _, fn := range optFns {
		if fn != nil {
			fn(&opts)
		}
	}
	return opts
}

func applyLoadOptions(optFns []LoadOption) LoadOptions {
	opts := LoadOptions{}
	for _, fn := range optFns {
		if fn != nil {
			fn(&opts)
		}
	}
	return opts
}

// UpdateResult captures the response from Update.
type UpdateResult struct {
	// NewVersion is the updated monotonic version after a successful mutation.
	NewVersion int64 `json:"new_version"`
	// NewStateETag is the updated state entity tag after a successful mutation.
	NewStateETag string `json:"new_state_etag"`
	// BytesWritten is the number of state bytes accepted by the update.
	BytesWritten int64 `json:"bytes"`
	// Metadata carries metadata values returned by the server for this object.
	Metadata api.MetadataAttributes `json:"metadata,omitempty"`
}

// MetadataResult captures metadata-only mutation outcomes.
type MetadataResult struct {
	// Version is the new metadata version after mutation.
	Version int64
	// Metadata is the server's effective metadata document after mutation.
	Metadata api.MetadataAttributes
}

// UpdateOptions controls conditional update semantics.
type UpdateOptions struct {
	// IfETag sets a conditional ETag guard (maps to If-Match semantics).
	IfETag string
	// IfVersion sets a conditional version guard (X-If-Version).
	IfVersion string
	// FencingToken provides explicit fencing when not already registered on the client.
	FencingToken string
	// Namespace scopes the mutation. Empty uses the client's default namespace.
	Namespace string
	// Metadata applies metadata mutations alongside the state update.
	Metadata MetadataOptions
	// TxnID binds the mutation to a transaction coordinator record.
	TxnID string
}

// UpdateOption customizes update behaviour.
type UpdateOption interface {
	apply(*UpdateOptions)
}

type updateOptionFunc func(*UpdateOptions)

func (f updateOptionFunc) apply(opts *UpdateOptions) {
	f(opts)
}

// WithTxnID binds an update/metadata mutation to a transaction coordinator id.
// The value is copied to UpdateOptions.TxnID and MetadataOptions.TxnID.
func WithTxnID(txnID string) UpdateOption {
	return updateOptionFunc(func(opts *UpdateOptions) {
		opts.TxnID = txnID
		if opts.Metadata.TxnID == "" {
			opts.Metadata.TxnID = txnID
		}
	})
}

// WithMetadata attaches metadata mutations to the same request as the state update.
func WithMetadata(meta MetadataOptions) UpdateOption {
	return updateOptionFunc(func(opts *UpdateOptions) {
		opts.Metadata = meta
	})
}

// WithQueryHidden marks the key hidden from /v1/query after the update commits.
func WithQueryHidden() UpdateOption {
	return WithMetadata(MetadataOptions{QueryHidden: Bool(true)})
}

// WithQueryVisible clears the query-hidden metadata flag after update commit.
func WithQueryVisible() UpdateOption {
	val := Bool(false)
	return WithMetadata(MetadataOptions{QueryHidden: val})
}

// MetadataOptions captures metadata mutations attached to updates.
type MetadataOptions struct {
	// QueryHidden marks a key hidden/visible from query results when non-nil.
	QueryHidden *bool
	// TxnID binds metadata-only mutations to a transaction.
	TxnID string
}

// NamespaceConfigOptions controls concurrency for namespace configuration mutations.
type NamespaceConfigOptions struct {
	// IfMatch enforces optimistic concurrency against the current namespace-config ETag.
	IfMatch string
}

// FlushIndexOptions customises index flush behaviour.
type FlushIndexOptions struct {
	// Mode accepts "async" (default) or "wait" for synchronous completion.
	Mode string
}

// FlushOption customises FlushIndex behaviour.
type FlushOption func(*FlushIndexOptions)

// WithFlushModeWait forces FlushIndex to block until indexing catches up.
func WithFlushModeWait() FlushOption {
	return func(opts *FlushIndexOptions) {
		opts.Mode = "wait"
	}
}

// WithFlushModeAsync schedules FlushIndex asynchronously and returns immediately.
func WithFlushModeAsync() FlushOption {
	return func(opts *FlushIndexOptions) {
		opts.Mode = "async"
	}
}

type metadataMutationPayload struct {
	QueryHidden *bool `json:"query_hidden,omitempty"`
}

// Bool returns a pointer to v.
func Bool(v bool) *bool {
	b := v
	return &b
}

func (o MetadataOptions) empty() bool {
	return o.QueryHidden == nil
}

func (o MetadataOptions) applyHeaders(req *http.Request) {
	if o.QueryHidden != nil {
		req.Header.Set(headerMetadataQueryHidden, strconv.FormatBool(*o.QueryHidden))
	}
}

func applyUpdateOptions(base UpdateOptions, opts []UpdateOption) UpdateOptions {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt.apply(&base)
	}
	return base
}

// RemoveOptions controls conditional delete semantics.
type RemoveOptions struct {
	// IfETag sets a conditional ETag guard (maps to If-Match semantics).
	IfETag string
	// IfVersion sets a conditional version guard (X-If-Version).
	IfVersion string
	// FencingToken provides explicit fencing when not already registered on the client.
	FencingToken string
	// Namespace scopes the delete. Empty uses the client's default namespace.
	Namespace string
	// TxnID binds the delete to a transaction coordinator record.
	TxnID string
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
	endpoints                  []string
	lastEndpoint               string
	shuffleEndpoints           bool
	httpClient                 *http.Client
	httpTraceEnabled           bool
	leaseTokens                sync.Map
	sessions                   sync.Map
	httpTimeout                time.Duration
	closeTimeout               time.Duration
	keepAliveTimeout           time.Duration
	forUpdateTimeout           time.Duration
	disableMTLS                bool
	bundlePath                 string
	bundlePathDisableExpansion bool
	logger                     pslog.Base
	failureRetries             int
	drainAwareShutdown         bool
	shutdownNotified           atomic.Bool
	defaultsMu                 sync.RWMutex
	defaultNamespace           string
	defaultLeaseID             string

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

func (c *Client) sessionByLease(leaseID string) *LeaseSession {
	if leaseID == "" {
		return nil
	}
	if sess, ok := c.sessions.Load(leaseID); ok {
		if ls, ok := sess.(*LeaseSession); ok {
			return ls
		}
	}
	return nil
}

// UseNamespace updates the default namespace used when callers omit one.
func (c *Client) UseNamespace(ns string) error {
	if c == nil {
		return fmt.Errorf("lockd: client nil")
	}
	normalized, err := namespaces.Normalize(ns, namespaces.Default)
	if err != nil {
		return err
	}
	c.defaultsMu.Lock()
	c.defaultNamespace = normalized
	c.defaultsMu.Unlock()
	return nil
}

// Namespace returns the default namespace currently configured on the client.
func (c *Client) Namespace() string {
	c.defaultsMu.RLock()
	defer c.defaultsMu.RUnlock()
	if c.defaultNamespace == "" {
		return namespaces.Default
	}
	return c.defaultNamespace
}

// UseLeaseID configures the client to reuse the provided lease for subsequent requests.
func (c *Client) UseLeaseID(leaseID string) {
	c.defaultsMu.Lock()
	c.defaultLeaseID = strings.TrimSpace(leaseID)
	c.defaultsMu.Unlock()
}

// ClearLeaseID removes the sticky lease when it matches leaseID.
func (c *Client) ClearLeaseID(leaseID string) {
	c.defaultsMu.Lock()
	if c.defaultLeaseID == leaseID {
		c.defaultLeaseID = ""
	}
	c.defaultsMu.Unlock()
}

func (c *Client) defaultLease() string {
	c.defaultsMu.RLock()
	leaseID := c.defaultLeaseID
	c.defaultsMu.RUnlock()
	return leaseID
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
		if resp.StatusCode == http.StatusServiceUnavailable && attempt < len(order)-1 {
			data, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil {
				apiErr := c.decodeErrorWithBody(resp, data)
				if isNodePassiveError(apiErr) || len(data) == 0 {
					if cancel != nil {
						cancel()
					}
					continue
				}
				resp.Body = io.NopCloser(bytes.NewReader(data))
			} else {
				if cancel != nil {
					cancel()
				}
				continue
			}
		}
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

func isNodePassiveError(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Response.ErrorCode == "node_passive"
	}
	return false
}

func isTransportError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return true
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

// WithHTTPClient supplies a custom HTTP client/transport stack.
// Use this when you need custom TLS roots, proxies, tracing round-trippers, or
// connection pooling behavior not covered by SDK defaults.
func WithHTTPClient(cli *http.Client) Option {
	return func(c *Client) {
		if cli != nil {
			c.httpClient = cli
		}
	}
}

// WithLogger supplies a logger for client diagnostics.
// Passing nil falls back to pslog.NoopLogger().
func WithLogger(logger pslog.Base) Option {
	return func(c *Client) {
		if logger == nil {
			c.logger = pslog.NoopLogger()
			return
		}
		if full, ok := logger.(pslog.Logger); ok {
			c.logger = svcfields.WithSubsystem(full, "client.sdk")
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

// WithBundlePath configures an mTLS client bundle PEM (CA cert + client cert + key).
// By default, "$VARS" and leading "~/" are expanded before loading.
// Use WithBundlePathDisableExpansion() to treat the path literally.
func WithBundlePath(path string) Option {
	return func(c *Client) {
		c.bundlePath = strings.TrimSpace(path)
	}
}

// WithBundlePathDisableExpansion disables env/tilde expansion for WithBundlePath.
func WithBundlePathDisableExpansion() Option {
	return func(c *Client) {
		c.bundlePathDisableExpansion = true
	}
}

// WithDefaultNamespace overrides the namespace applied when request payloads/options
// omit Namespace.
func WithDefaultNamespace(ns string) Option {
	return func(c *Client) {
		c.defaultNamespace = ns
	}
}

// WithFailureRetries overrides how many times non-acquire operations retry on failure.
// A value <0 allows infinite retries.
func WithFailureRetries(n int) Option {
	return func(c *Client) {
		c.failureRetries = n
	}
}

// WithHTTPTimeout overrides per-request timeout used by SDK-issued HTTP calls.
// This timeout does not apply to acquire/dequeue wait-forever calls that intentionally
// hold long-poll requests open.
func WithHTTPTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.httpTimeout = d
		}
	}
}

// WithCloseTimeout overrides timeout used when Close() auto-releases tracked leases.
func WithCloseTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.closeTimeout = d
		}
	}
}

// WithKeepAliveTimeout overrides timeout used for lease keepalive requests.
func WithKeepAliveTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.keepAliveTimeout = d
		}
	}
}

// WithHTTPTrace enables net/http/httptrace diagnostics on SDK requests.
// Traces are emitted through the configured client logger.
func WithHTTPTrace() Option {
	return func(c *Client) {
		c.httpTraceEnabled = true
	}
}

// WithDrainAwareShutdown toggles automatic lease release when server responses
// include Shutdown-Imminent drain signaling.
func WithDrainAwareShutdown(enabled bool) Option {
	return func(c *Client) {
		c.drainAwareShutdown = enabled
	}
}

// WithForUpdateTimeout overrides timeout bound for AcquireForUpdate orchestration.
// Handler execution still follows the caller's context deadline/cancellation.
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
		defaultNamespace:   namespaces.Default,
		logger:             pslog.NoopLogger(),
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
		defaultNamespace:   namespaces.Default,
		logger:             pslog.NoopLogger(),
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
	// BaseDelay is the starting backoff delay after retryable failures.
	BaseDelay time.Duration
	// MaxDelay caps exponential backoff growth.
	MaxDelay time.Duration
	// Multiplier is the exponential growth factor applied between retries.
	Multiplier float64
	// Jitter randomizes capped delays by +/- Jitter to reduce thundering herds.
	Jitter time.Duration
	// FailureRetries controls retries for non-conflict transient failures; <0 means unbounded.
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
	} else if req.BlockSecs == BlockWaitForever {
		if ctxDeadline, ok := ctx.Deadline(); ok {
			deadline = ctxDeadline
		}
	}
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
	currentReq := req
	currentReq.Namespace = namespace
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
			c.logInfoCtx(ctx, "client.acquire.success", "key", keyForLog, "lease_id", resp.LeaseID, "txn_id", resp.TxnID, "endpoint", c.lastEndpoint, "attempt", attempt)
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
		if req.TxnID != "" {
			httpReq.Header.Set("X-Txn-ID", req.TxnID)
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
			if shouldRetryForUpdate(err) {
				retryCount++
				if cfg.FailureRetries >= 0 && retryCount > cfg.FailureRetries {
					c.logErrorCtx(ctx, "client.acquire_for_update.acquire_error", "key", req.Key, "error", err, "retries", retryCount)
					return err
				}
				sleep := acquireRetryDelay(delay, cfg)
				if sleep <= 0 {
					sleep = 10 * time.Millisecond
				}
				if retryHint := retryAfterFromError(err); retryHint > sleep {
					sleep = retryHint
				}
				delay = sleep
				c.logDebugCtx(ctx, "client.acquire_for_update.retry_after_acquire_error", "key", req.Key, "retries", retryCount, "delay", sleep, "retry_after", retryAfterFromError(err), "qrf_state", qrfStateFromError(err), "error", err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleep):
				}
				continue
			}
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
			c.logWarnCtx(ctx, "client.acquire_for_update.snapshot_error", "key", keyForLog, "lease_id", sess.LeaseID, "txn_id", sess.TxnID, "error", snapErr)
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
				c.logDebugCtx(ctx, "client.acquire_for_update.retry_after_snapshot_failure", "key", keyForLog, "lease_id", sess.LeaseID, "txn_id", sess.TxnID, "retries", retryCount, "delay", sleep, "retry_after", retryAfterFromError(snapErr), "qrf_state", qrfStateFromError(snapErr), "error", snapErr)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleep):
				}
				continue
			}
			return snapErr
		}

		c.logInfoCtx(ctx, "client.acquire_for_update.acquired", "key", keyForLog, "lease_id", sess.LeaseID, "txn_id", sess.TxnID, "endpoint", sess.endpoint)
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
				c.logDebugCtx(ctx, "client.acquire_for_update.state_close_error", "key", keyForLog, "lease_id", sess.LeaseID, "txn_id", sess.TxnID, "error", closeErr)
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
		keepRetryable := keepErr == nil || isNodePassiveError(keepErr) || isTransportError(keepErr)
		if releaseErr != nil && isNodePassiveError(releaseErr) && handlerErr == nil && keepRetryable {
			retryCount := 0
			delay := cfg.BaseDelay
			if delay <= 0 {
				delay = 10 * time.Millisecond
			}
			for {
				retryCount++
				if cfg.FailureRetries >= 0 && retryCount > cfg.FailureRetries {
					break
				}
				sleep := acquireRetryDelay(delay, cfg)
				if sleep <= 0 {
					sleep = 10 * time.Millisecond
				}
				if retryHint := retryAfterFromError(releaseErr); retryHint > sleep {
					sleep = retryHint
				}
				delay = sleep
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleep):
				}
				nextErr := sess.Release(ctx)
				if nextErr == nil || isLeaseRequiredError(nextErr) {
					releaseErr = nil
					if keepErr != nil && (isNodePassiveError(keepErr) || isTransportError(keepErr)) {
						keepErr = nil
					}
					break
				}
				if !isNodePassiveError(nextErr) {
					releaseErr = nextErr
					break
				}
				releaseErr = nextErr
			}
		}
		resultErr := errors.Join(handlerErr, keepErr)
		if releaseErr != nil && !isLeaseRequiredError(releaseErr) {
			resultErr = errors.Join(resultErr, releaseErr)
		}
		if resultErr != nil {
			if isNodePassiveError(resultErr) {
				c.logWarnCtx(ctx, "client.acquire_for_update.node_passive", "key", keyForLog, "lease_id", sess.LeaseID, "txn_id", sess.TxnID, "error", resultErr)
				return resultErr
			}
			if shouldRetryForUpdate(resultErr) {
				retryCount++
				if cfg.FailureRetries >= 0 && retryCount > cfg.FailureRetries {
					c.logErrorCtx(ctx, "client.acquire_for_update.handler_error", "key", keyForLog, "lease_id", sess.LeaseID, "txn_id", sess.TxnID, "error", resultErr)
					return resultErr
				}
				sleep := acquireRetryDelay(delay, cfg)
				if sleep <= 0 {
					sleep = 10 * time.Millisecond
				}
				if retryHint := retryAfterFromError(resultErr); retryHint > sleep {
					sleep = retryHint
				}
				delay = sleep
				c.logDebugCtx(ctx, "client.acquire_for_update.retry_after_handler_error", "key", keyForLog, "lease_id", sess.LeaseID, "txn_id", sess.TxnID, "retries", retryCount, "delay", sleep, "retry_after", retryAfterFromError(resultErr), "qrf_state", qrfStateFromError(resultErr), "error", resultErr)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleep):
				}
				continue
			}
			c.logErrorCtx(ctx, "client.acquire_for_update.handler_error", "key", keyForLog, "lease_id", sess.LeaseID, "txn_id", sess.TxnID, "error", resultErr)
			return resultErr
		}
		c.logInfoCtx(ctx, "client.acquire_for_update.success", "key", keyForLog, "lease_id", sess.LeaseID, "txn_id", sess.TxnID)
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
					c.logWarnCtx(ctx, "client.acquire_for_update.keepalive_failed", "key", sess.Key, "lease_id", sess.LeaseID, "txn_id", sess.TxnID, "error", err)
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
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if isLeaseRequiredError(err) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
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
	if errors.Is(err, context.Canceled) {
		return false
	}
	return false
}

// KeepAlive extends a lease.
func (c *Client) KeepAlive(ctx context.Context, req api.KeepAliveRequest) (*api.KeepAliveResponse, error) {
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
	c.logTraceCtx(ctx, "client.keepalive.start", "key", req.Key, "lease_id", req.LeaseID, "ttl_seconds", req.TTLSeconds)
	token, err := c.fencingToken(req.LeaseID, "")
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set(headerFencingToken, token)
	if txn := strings.TrimSpace(req.TxnID); txn != "" {
		headers.Set(headerTxnID, txn)
	}
	if corr := CorrelationIDFromContext(ctx); corr != "" {
		headers.Set(headerCorrelationID, corr)
	}
	c.logTraceCtx(ctx, "client.keepalive.headers", "key", req.Key, "lease_id", req.LeaseID, "fencing_token", token, "txn_id", req.TxnID)
	var resp api.KeepAliveResponse
	kCtx, cancel := c.keepAliveContext(ctx)
	defer cancel()
	if _, err := c.postJSON(kCtx, "/v1/keepalive", req, &resp, headers, ""); err != nil {
		c.logErrorCtx(ctx, "client.keepalive.error", "key", req.Key, "lease_id", req.LeaseID, "fencing_token", token, "txn_id", req.TxnID, "error", err)
		return nil, err
	}
	c.logTraceCtx(ctx, "client.keepalive.success", "key", req.Key, "lease_id", req.LeaseID, "fencing_token", token, "txn_id", req.TxnID, "expires_at", resp.ExpiresAt)
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
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
	c.logTraceCtx(ctx, "client.release.start", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID)
	token, err := c.fencingToken(req.LeaseID, "")
	if err != nil {
		if errors.Is(err, ErrMissingFencingToken) {
			c.logDebugCtx(ctx, "client.release.no_token", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID)
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
	c.logTraceCtx(ctx, "client.release.headers", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID, "fencing_token", token)
	var resp api.ReleaseResponse
	endpoint, err := c.postJSON(ctx, "/v1/release", req, &resp, headers, preferred)
	if err != nil {
		if isLeaseRequiredError(err) {
			c.logDebugCtx(ctx, "client.release.already_gone", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID)
			c.leaseTokens.Delete(req.LeaseID)
			return &api.ReleaseResponse{Released: true}, nil
		}
		if errors.Is(err, context.DeadlineExceeded) && preferred != "" {
			c.logWarnCtx(ctx, "client.release.retry_different_endpoint", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID, "endpoint", preferred, "error", err)
			for _, base := range c.endpoints {
				if base == preferred {
					continue
				}
				altCtx := ctx
				var cancel context.CancelFunc
				if deadline, ok := ctx.Deadline(); ok {
					remaining := time.Until(deadline)
					if remaining <= 0 {
						break
					}
					altCtx, cancel = context.WithTimeout(context.Background(), remaining)
				} else if c.closeTimeout > 0 {
					altCtx, cancel = context.WithTimeout(context.Background(), c.closeTimeout)
				}
				altResp, altErr := c.postJSON(altCtx, "/v1/release", req, &resp, headers, base)
				if cancel != nil {
					cancel()
				}
				if altErr == nil {
					endpoint = altResp
					err = nil
					break
				}
				if isLeaseRequiredError(altErr) {
					c.logDebugCtx(ctx, "client.release.already_gone", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID, "endpoint", base)
					c.leaseTokens.Delete(req.LeaseID)
					return &api.ReleaseResponse{Released: true}, nil
				}
				c.logWarnCtx(ctx, "client.release.backup_failed", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID, "endpoint", base, "error", altErr)
			}
			if err != nil {
				return nil, err
			}
		} else {
			c.logErrorCtx(ctx, "client.release.error", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID, "fencing_token", token, "error", err)
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
				c.logDebugCtx(ctx, "client.release.extra_endpoint_error", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID, "endpoint", base, "error", extraErr)
			}
		}
	}
	c.logTraceCtx(ctx, "client.release.success", "key", req.Key, "lease_id", req.LeaseID, "txn_id", req.TxnID, "fencing_token", token)
	c.leaseTokens.Delete(req.LeaseID)
	return &resp, nil
}

// Describe fetches key metadata without state.
func (c *Client) Describe(ctx context.Context, key string) (*api.DescribeResponse, error) {
	c.logTraceCtx(ctx, "client.describe.start", "key", key, "endpoint", c.lastEndpoint)
	namespace, err := c.namespaceFor("")
	if err != nil {
		return nil, err
	}
	var describe api.DescribeResponse
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		full := fmt.Sprintf("%s/v1/describe?key=%s&namespace=%s", base, url.QueryEscape(key), url.QueryEscape(namespace))
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

// NamespaceConfigResult captures a namespace config document and its ETag.
type NamespaceConfigResult struct {
	// Config is the effective namespace configuration document returned by the server.
	Config *api.NamespaceConfigResponse
	// ETag is the namespace configuration ETag for optimistic concurrency control.
	ETag string
}

// GetNamespaceConfig returns the namespace configuration document and its ETag.
func (c *Client) GetNamespaceConfig(ctx context.Context, namespace string) (NamespaceConfigResult, error) {
	ns, err := c.namespaceFor(namespace)
	if err != nil {
		return NamespaceConfigResult{}, err
	}
	c.logTraceCtx(ctx, "client.namespace.get.start", "namespace", ns, "endpoint", c.lastEndpoint)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		full := fmt.Sprintf("%s/v1/namespace?namespace=%s", base, url.QueryEscape(ns))
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
		c.logErrorCtx(ctx, "client.namespace.get.transport_error", "namespace", ns, "error", err)
		return NamespaceConfigResult{}, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.namespace.get.error", "namespace", ns, "endpoint", endpoint, "status", resp.StatusCode)
		return NamespaceConfigResult{}, c.decodeError(resp)
	}
	var out api.NamespaceConfigResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return NamespaceConfigResult{}, err
	}
	etag := strings.Trim(resp.Header.Get("ETag"), "\"")
	c.logTraceCtx(ctx, "client.namespace.get.success", "namespace", ns, "endpoint", endpoint, "etag", etag)
	return NamespaceConfigResult{Config: &out, ETag: etag}, nil
}

// UpdateNamespaceConfig mutates namespace-level settings and returns the updated configuration.
func (c *Client) UpdateNamespaceConfig(ctx context.Context, req api.NamespaceConfigRequest, opts NamespaceConfigOptions) (NamespaceConfigResult, error) {
	ns, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return NamespaceConfigResult{}, err
	}
	req.Namespace = ns
	if req.Query == nil {
		return NamespaceConfigResult{}, fmt.Errorf("lockd: namespace query configuration required")
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return NamespaceConfigResult{}, err
	}
	path := fmt.Sprintf("/v1/namespace?namespace=%s", url.QueryEscape(ns))
	c.logTraceCtx(ctx, "client.namespace.set.start", "namespace", ns, "endpoint", c.lastEndpoint)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, bytes.NewReader(payload))
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if strings.TrimSpace(opts.IfMatch) != "" {
			httpReq.Header.Set("If-Match", opts.IfMatch)
		}
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.namespace.set.transport_error", "namespace", ns, "error", err)
		return NamespaceConfigResult{}, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.namespace.set.error", "namespace", ns, "endpoint", endpoint, "status", resp.StatusCode)
		return NamespaceConfigResult{}, c.decodeError(resp)
	}
	var out api.NamespaceConfigResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return NamespaceConfigResult{}, err
	}
	etag := strings.Trim(resp.Header.Get("ETag"), "\"")
	c.logTraceCtx(ctx, "client.namespace.set.success", "namespace", ns, "endpoint", endpoint, "etag", etag)
	return NamespaceConfigResult{Config: &out, ETag: etag}, nil
}

// FlushIndex forces the namespace index writer to flush pending documents.
func (c *Client) FlushIndex(ctx context.Context, namespace string, optFns ...FlushOption) (*api.IndexFlushResponse, error) {
	ns, err := c.namespaceFor(namespace)
	if err != nil {
		return nil, err
	}
	options := FlushIndexOptions{Mode: "async"}
	for _, fn := range optFns {
		if fn != nil {
			fn(&options)
		}
	}
	mode := strings.TrimSpace(options.Mode)
	if mode == "" {
		mode = "async"
	}
	req := api.IndexFlushRequest{Namespace: ns, Mode: mode}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	path := "/v1/index/flush"
	if ns != "" {
		path += fmt.Sprintf("?namespace=%s", url.QueryEscape(ns))
	}
	c.logTraceCtx(ctx, "client.index.flush.start", "namespace", ns, "mode", mode, "endpoint", c.lastEndpoint)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, bytes.NewReader(payload))
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
		c.logErrorCtx(ctx, "client.index.flush.transport_error", "namespace", ns, "mode", mode, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		c.logWarnCtx(ctx, "client.index.flush.error", "namespace", ns, "mode", mode, "endpoint", endpoint, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	var out api.IndexFlushResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	c.logTraceCtx(ctx, "client.index.flush.success", "namespace", ns, "mode", mode, "endpoint", endpoint, "status", resp.StatusCode)
	return &out, nil
}

// QueryOptions controls /v1/query execution hints.
type QueryOptions struct {
	request api.QueryRequest
	// Engine forces execution strategy ("auto", "index", "scan").
	Engine string
	// Refresh controls visibility semantics (for example "wait_for").
	Refresh string
	// Return controls payload shape (keys or streamed documents).
	Return   QueryReturn
	parseErr error
}

// QueryOption customizes query execution.
type QueryOption func(*QueryOptions)

func applyQueryOptions(optFns []QueryOption) QueryOptions {
	opts := QueryOptions{}
	for _, fn := range optFns {
		if fn != nil {
			fn(&opts)
		}
	}
	return opts
}

func cloneQueryRequest(req api.QueryRequest) api.QueryRequest {
	cloned := req
	if req.Fields != nil {
		cloned.Fields = make(map[string]any, len(req.Fields))
		for k, v := range req.Fields {
			cloned.Fields[k] = v
		}
	}
	return cloned
}

// WithQueryRequest copies a full QueryRequest into the option set.
// Subsequent WithQuery* options can override individual fields.
func WithQueryRequest(req *api.QueryRequest) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil || req == nil {
			return
		}
		opts.request = cloneQueryRequest(*req)
	}
}

// WithQueryNamespace overrides namespace used for query execution.
func WithQueryNamespace(ns string) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil {
			return
		}
		opts.request.Namespace = strings.TrimSpace(ns)
	}
}

// WithQueryLimit caps number of rows returned by the server.
func WithQueryLimit(limit int) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil {
			return
		}
		if limit < 0 {
			limit = 0
		}
		opts.request.Limit = limit
	}
}

// WithQueryCursor resumes a previous query page using server-provided cursor token.
func WithQueryCursor(cursor string) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil {
			return
		}
		opts.request.Cursor = cursor
	}
}

// WithQueryFields sets field projection map passed to query execution.
func WithQueryFields(fields map[string]any) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil {
			return
		}
		if len(fields) == 0 {
			opts.request.Fields = nil
			return
		}
		if opts.request.Fields == nil {
			opts.request.Fields = make(map[string]any, len(fields))
		}
		for k, v := range fields {
			opts.request.Fields[k] = v
		}
	}
}

// WithQuery parses an LQL expression and sets the selector for the request.
// Parse errors are surfaced when Query executes.
func WithQuery(expr string) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil {
			return
		}
		if opts.parseErr != nil {
			return
		}
		expr = strings.TrimSpace(expr)
		if expr == "" {
			opts.request.Selector = api.Selector{}
			return
		}
		sel, err := lql.ParseSelectorString(expr)
		if err != nil {
			opts.parseErr = err
			return
		}
		if sel.IsEmpty() {
			opts.request.Selector = api.Selector{}
			return
		}
		opts.request.Selector = sel
	}
}

// WithQuerySelector installs an already-parsed selector (useful when callers
// construct selector ASTs directly).
func WithQuerySelector(sel api.Selector) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil {
			return
		}
		opts.request.Selector = sel
	}
}

// WithQueryEngine forces query execution engine ("auto", "index", or "scan").
func WithQueryEngine(engine string) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil {
			return
		}
		switch strings.ToLower(strings.TrimSpace(engine)) {
		case "index", "scan", "auto":
			opts.Engine = strings.ToLower(engine)
		case "":
			opts.Engine = ""
		default:
			opts.Engine = engine
		}
	}
}

// WithQueryEngineAuto explicitly selects the auto engine.
func WithQueryEngineAuto() QueryOption {
	return WithQueryEngine("auto")
}

// WithQueryEngineIndex selects the secondary index engine.
func WithQueryEngineIndex() QueryOption {
	return WithQueryEngine("index")
}

// WithQueryEngineScan selects the scan engine.
func WithQueryEngineScan() QueryOption {
	return WithQueryEngine("scan")
}

// WithQueryRefresh selects refresh policy (for example "wait_for" to block
// until indexed visibility).
func WithQueryRefresh(mode string) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil {
			return
		}
		switch strings.ToLower(strings.TrimSpace(mode)) {
		case "wait_for", "wait-for":
			opts.Refresh = "wait_for"
		case "":
			opts.Refresh = ""
		default:
			opts.Refresh = mode
		}
	}
}

// WithQueryRefreshWaitFor waits until documents are visible in the selected engine.
func WithQueryRefreshWaitFor() QueryOption {
	return WithQueryRefresh("wait_for")
}

// WithQueryRefreshImmediate clears the refresh hint (default behaviour).
func WithQueryRefreshImmediate() QueryOption {
	return WithQueryRefresh("")
}

// WithQueryBlock waits for the queryable view to observe in-flight documents.
func WithQueryBlock() QueryOption {
	return WithQueryRefreshWaitFor()
}

// WithQueryReturn selects payload mode for /v1/query ("keys" or "documents").
func WithQueryReturn(mode QueryReturn) QueryOption {
	return func(opts *QueryOptions) {
		if opts == nil {
			return
		}
		mode = mode.normalize()
		if mode != "" {
			opts.Return = mode
			opts.request.Return = api.QueryReturn(mode)
		} else {
			opts.Return = ""
			opts.request.Return = ""
		}
	}
}

// WithQueryReturnKeys forces the default keys-only response mode.
func WithQueryReturnKeys() QueryOption {
	return WithQueryReturn(QueryReturnKeys)
}

// WithQueryReturnDocuments streams documents as NDJSON rows.
func WithQueryReturnDocuments() QueryOption {
	return WithQueryReturn(QueryReturnDocuments)
}

// Query executes a selector search within a namespace. Usage is option-driven:
//
//	resp, err := cli.Query(ctx,
//	    client.WithQueryNamespace("orders"),
//	    client.WithQuery(`eq{field=/status,value=open}`),
//	    client.WithQueryLimit(20),
//	    client.WithQueryReturnDocuments(),
//	)
//	if err != nil { /* handle */ }
//	resp.ForEach(func(row client.QueryRow) error {
//	    // row.Document is populated in return=documents mode
//	    return nil
//	})
//
// Callers can combine helpers (namespace, selector, engine hints, cursor,
// return mode, etc.) without assembling api.QueryRequest manually.
func (c *Client) Query(ctx context.Context, optFns ...QueryOption) (*QueryResponse, error) {
	if c == nil {
		return nil, fmt.Errorf("lockd: client nil")
	}
	opts := applyQueryOptions(optFns)
	if opts.parseErr != nil {
		return nil, opts.parseErr
	}
	ns, err := c.namespaceFor(opts.request.Namespace)
	if err != nil {
		return nil, err
	}
	opts.request.Namespace = ns
	returnMode := opts.Return.normalize()
	if returnMode != "" {
		opts.request.Return = api.QueryReturn(returnMode)
	} else {
		opts.request.Return = ""
	}
	payload, err := json.Marshal(opts.request)
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	if ns != "" {
		params.Set("namespace", ns)
	}
	if opts.Engine != "" {
		params.Set("engine", opts.Engine)
	}
	if opts.Refresh != "" {
		params.Set("refresh", opts.Refresh)
	}
	if returnMode != "" {
		params.Set("return", string(returnMode))
	}
	path := "/v1/query"
	if encoded := params.Encode(); encoded != "" {
		path += "?" + encoded
	}
	c.logTraceCtx(ctx, "client.query.start", "namespace", ns, "engine", opts.Engine, "mode", func() string {
		if returnMode == "" {
			return ""
		}
		return string(returnMode)
	}(), "endpoint", c.lastEndpoint)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, bytes.NewReader(payload))
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
		c.logErrorCtx(ctx, "client.query.transport_error", "namespace", ns, "engine", opts.Engine, "error", err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer cancel()
		defer resp.Body.Close()
		c.logWarnCtx(ctx, "client.query.error", "namespace", ns, "engine", opts.Engine, "endpoint", endpoint, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	contentType := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type")))
	mode := detectQueryReturn(resp.Header.Get(headerQueryReturn), contentType)
	cursorHeader := strings.TrimSpace(resp.Header.Get(headerQueryCursor))
	indexSeq := parseUintHeader(resp.Header.Get(headerQueryIndexSeq))
	headerMetadata := parseQueryMetadataHeader(resp.Header.Get(headerQueryMetadata))
	switch mode {
	case QueryReturnDocuments:
		reader := &cancelReadCloser{ReadCloser: resp.Body, cancel: cancel}
		respObj := newDocumentQueryResponse(ns, cursorHeader, indexSeq, headerMetadata, reader)
		c.logTraceCtx(ctx, "client.query.success", "namespace", ns, "engine", opts.Engine, "mode", "documents", "endpoint", endpoint, "cursor", cursorHeader)
		return respObj, nil
	default:
		defer cancel()
		defer resp.Body.Close()
		var out api.QueryResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, err
		}
		if out.Cursor == "" {
			out.Cursor = cursorHeader
		}
		if out.IndexSeq == 0 {
			out.IndexSeq = indexSeq
		}
		if out.Namespace == "" {
			out.Namespace = ns
		}
		out.Metadata = mergeMetadata(out.Metadata, headerMetadata)
		respObj := newKeyQueryResponse(out, mode)
		c.logTraceCtx(ctx, "client.query.success", "namespace", ns, "engine", opts.Engine, "mode", "keys", "endpoint", endpoint, "keys", len(respObj.keys))
		return respObj, nil
	}
}

// Get fetches the JSON state for key and returns a streaming response.
func (c *Client) Get(ctx context.Context, key string, optFns ...GetOption) (*GetResponse, error) {
	return c.getWithOptions(ctx, key, applyGetOptions(optFns))
}

func (c *Client) getWithOptions(ctx context.Context, key string, opts GetOptions) (*GetResponse, error) {
	if c == nil {
		return nil, fmt.Errorf("lockd: client nil")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("lockd: key required")
	}
	namespace, err := c.namespaceFor(opts.Namespace)
	if err != nil {
		return nil, err
	}
	leaseID := strings.TrimSpace(opts.LeaseID)
	if leaseID == "" {
		leaseID = c.defaultLease()
	}
	public := leaseID == "" && !opts.DisablePublic
	if !public && leaseID == "" {
		return nil, fmt.Errorf("lockd: lease_id required for key %s", key)
	}
	token := ""
	if leaseID != "" {
		token, err = c.fencingToken(leaseID, "")
		if err != nil {
			return nil, err
		}
	}
	logFields := []any{"key", key, "namespace", namespace, "endpoint", c.lastEndpoint}
	if public {
		logFields = append(logFields, "public", true)
	} else {
		logFields = append(logFields, "lease_id", leaseID, "fencing_token", token)
	}
	logKey := "client.get.start"
	if public {
		logKey = "client.get_public.start"
	}
	c.logTraceCtx(ctx, logKey, logFields...)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		full := fmt.Sprintf("%s/v1/get?key=%s&namespace=%s", base, url.QueryEscape(key), url.QueryEscape(namespace))
		if public {
			full += "&public=1"
		}
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, full, http.NoBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		if !public {
			req.Header.Set("X-Lease-ID", leaseID)
			req.Header.Set(headerFencingToken, token)
		}
		c.applyCorrelationHeader(ctx, req, "")
		return req, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		errorFields := []any{"key", key, "namespace", namespace, "endpoint", endpoint, "error", err}
		if public {
			errorFields = append(errorFields, "public", true)
		} else {
			errorFields = append(errorFields, "lease_id", leaseID, "fencing_token", token)
		}
		c.logErrorCtx(ctx, "client.get.transport_error", errorFields...)
		return nil, err
	}
	if resp.StatusCode == http.StatusNoContent {
		resp.Body.Close()
		cancel()
		emptyFields := []any{"key", key, "namespace", namespace, "endpoint", endpoint}
		if public {
			emptyFields = append(emptyFields, "public", true)
		} else {
			emptyFields = append(emptyFields, "lease_id", leaseID, "fencing_token", token)
		}
		c.logDebugCtx(ctx, "client.get.empty", emptyFields...)
		return &GetResponse{Namespace: namespace, Key: key, ETag: strings.Trim(resp.Header.Get("ETag"), "\""), Version: resp.Header.Get("X-Key-Version"), client: c, public: public}, nil
	}
	if resp.StatusCode != http.StatusOK {
		defer cancel()
		defer resp.Body.Close()
		errorFields := []any{"key", key, "namespace", namespace, "endpoint", endpoint, "status", resp.StatusCode}
		if public {
			errorFields = append(errorFields, "public", true)
		} else {
			errorFields = append(errorFields, "lease_id", leaseID, "fencing_token", token)
		}
		c.logWarnCtx(ctx, "client.get.error", errorFields...)
		return nil, c.decodeError(resp)
	}
	if !public {
		if newToken := resp.Header.Get(headerFencingToken); newToken != "" {
			c.RegisterLeaseToken(leaseID, newToken)
		}
	}
	reader := &cancelReadCloser{ReadCloser: resp.Body, cancel: cancel}
	result := &GetResponse{
		Namespace: namespace,
		Key:       key,
		ETag:      strings.Trim(resp.Header.Get("ETag"), "\""),
		Version:   resp.Header.Get("X-Key-Version"),
		HasState:  true,
		reader:    reader,
		client:    c,
		public:    public,
	}
	successFields := []any{"key", key, "namespace", namespace, "endpoint", endpoint, "etag", result.ETag, "version", result.Version}
	if public {
		successFields = append(successFields, "public", true)
	} else {
		successFields = append(successFields, "lease_id", leaseID, "fencing_token", token)
	}
	c.logTraceCtx(ctx, "client.get.success", successFields...)
	return result, nil
}

// Load unmarshals the current state for key into v. When no state exists, v is left untouched.
func (c *Client) Load(ctx context.Context, key string, v any, optFns ...LoadOption) error {
	if v == nil {
		return nil
	}
	resp, err := c.getWithOptions(ctx, key, applyLoadOptions(optFns).GetOptions)
	if err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	defer resp.Close()
	if !resp.HasState {
		return nil
	}
	reader := resp.Reader()
	if reader == nil {
		return nil
	}
	switch target := v.(type) {
	case *Document:
		target.Namespace = resp.Namespace
		target.Key = resp.Key
		target.Version = resp.Version
		target.ETag = resp.ETag
		if target.Metadata == nil {
			target.Metadata = map[string]string{}
		}
		if target.Body == nil {
			target.Body = make(map[string]any)
		}
		return json.NewDecoder(reader).Decode(&target.Body)
	default:
		return json.NewDecoder(reader).Decode(target)
	}
}

// Save delegates to sess.Save.
func (c *Client) Save(ctx context.Context, sess *LeaseSession, v any) error {
	return sess.Save(ctx, v)
}

// Update uploads new JSON state from the provided reader.
func (c *Client) Update(ctx context.Context, key, leaseID string, body io.Reader, opts UpdateOptions) (*UpdateResult, error) {
	return c.updateWithPreferred(ctx, key, leaseID, body, opts, "")
}

func (c *Client) updateWithPreferred(ctx context.Context, key, leaseID string, body io.Reader, opts UpdateOptions, preferred string) (*UpdateResult, error) {
	namespace, err := c.namespaceFor(opts.Namespace)
	if err != nil {
		return nil, err
	}
	opts.Namespace = namespace
	if opts.TxnID == "" {
		if sess := c.sessionByLease(leaseID); sess != nil {
			opts.TxnID = sess.TxnID
		}
	}
	token, err := c.fencingToken(leaseID, opts.FencingToken)
	if err != nil {
		return nil, err
	}
	c.logTraceCtx(ctx, "client.update.start", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", c.lastEndpoint, "fencing_token", token)
	bodyFactory, err := makeBodyFactory(body)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/update?key=%s&namespace=%s", url.QueryEscape(key), url.QueryEscape(namespace))
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
		if opts.TxnID != "" {
			req.Header.Set(headerTxnID, opts.TxnID)
		}
		if opts.IfETag != "" {
			req.Header.Set("X-If-State-ETag", opts.IfETag)
		}
		if opts.IfVersion != "" {
			req.Header.Set("X-If-Version", opts.IfVersion)
		}
		req.Header.Set(headerFencingToken, token)
		opts.Metadata.applyHeaders(req)
		c.applyCorrelationHeader(ctx, req, "")
		return req, cancel, nil
	}
	retries := c.failureRetries
	delay := 10 * time.Millisecond
	maxRetryDelay := 500 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		resp, cancel, endpoint, err := c.attemptEndpoints(builder, preferred)
		if err != nil {
			if cancel != nil {
				cancel()
			}
			c.logErrorCtx(ctx, "client.update.transport_error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", endpoint, "fencing_token", token, "error", err)
			if retries == 0 || !isTransportError(err) {
				return nil, err
			}
			if retries > 0 {
				retries--
			}
			sleep := delay
			if sleep > maxRetryDelay {
				sleep = maxRetryDelay
			}
			if sleep > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(sleep):
				}
			}
			if delay < time.Second {
				delay *= 2
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			c.logWarnCtx(ctx, "client.update.error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", endpoint, "fencing_token", token, "status", resp.StatusCode)
			err := c.decodeError(resp)
			resp.Body.Close()
			if cancel != nil {
				cancel()
			}
			if retries == 0 || !isNodePassiveError(err) {
				return nil, err
			}
			if retries > 0 {
				retries--
			}
			sleep := delay
			if retryHint := retryAfterFromError(err); retryHint > sleep {
				sleep = retryHint
			}
			if sleep > maxRetryDelay {
				sleep = maxRetryDelay
			}
			if sleep > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(sleep):
				}
			}
			if delay < time.Second {
				delay *= 2
			}
			continue
		}
		var result UpdateResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			if cancel != nil {
				cancel()
			}
			return nil, err
		}
		resp.Body.Close()
		if cancel != nil {
			cancel()
		}
		c.logTraceCtx(ctx, "client.update.success", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", endpoint, "fencing_token", token, "new_version", result.NewVersion, "new_etag", result.NewStateETag)
		if newToken := resp.Header.Get(headerFencingToken); newToken != "" {
			c.RegisterLeaseToken(leaseID, newToken)
		}
		return &result, nil
	}
}

// UpdateBytes uploads new JSON state from the provided byte slice.
func (c *Client) UpdateBytes(ctx context.Context, key, leaseID string, body []byte, opts UpdateOptions) (*UpdateResult, error) {
	return c.Update(ctx, key, leaseID, bytes.NewReader(body), opts)
}

// UpdateMetadata mutates lock metadata without modifying the JSON state.
func (c *Client) UpdateMetadata(ctx context.Context, key, leaseID string, opts UpdateOptions) (*MetadataResult, error) {
	return c.updateMetadataWithPreferred(ctx, key, leaseID, opts, "")
}

func (c *Client) updateMetadataWithPreferred(ctx context.Context, key, leaseID string, opts UpdateOptions, preferred string) (*MetadataResult, error) {
	if opts.Metadata.empty() {
		return nil, fmt.Errorf("lockd: metadata options required")
	}
	namespace, err := c.namespaceFor(opts.Namespace)
	if err != nil {
		return nil, err
	}
	opts.Namespace = namespace
	if opts.TxnID == "" {
		if sess := c.sessionByLease(leaseID); sess != nil {
			opts.TxnID = sess.TxnID
		}
	}
	token, err := c.fencingToken(leaseID, opts.FencingToken)
	if err != nil {
		return nil, err
	}
	payload := metadataMutationPayload{
		QueryHidden: opts.Metadata.QueryHidden,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/metadata?key=%s&namespace=%s", url.QueryEscape(key), url.QueryEscape(namespace))
	c.logTraceCtx(ctx, "client.metadata.start", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "namespace", namespace)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, bytes.NewReader(body))
		if err != nil {
			cancel()
			return nil, nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Lease-ID", leaseID)
		if opts.TxnID != "" {
			req.Header.Set(headerTxnID, opts.TxnID)
		}
		if opts.IfVersion != "" {
			req.Header.Set("X-If-Version", opts.IfVersion)
		}
		req.Header.Set(headerFencingToken, token)
		c.applyCorrelationHeader(ctx, req, "")
		return req, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, preferred)
	if err != nil {
		c.logErrorCtx(ctx, "client.metadata.transport_error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "namespace", namespace, "endpoint", endpoint, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.metadata.error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "namespace", namespace, "endpoint", endpoint, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	var apiResp api.MetadataUpdateResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}
	if newToken := resp.Header.Get(headerFencingToken); newToken != "" {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	c.logTraceCtx(ctx, "client.metadata.success", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "namespace", namespace, "endpoint", endpoint, "version", apiResp.Version)
	return &MetadataResult{
		Version:  apiResp.Version,
		Metadata: apiResp.Metadata,
	}, nil
}

// Remove deletes the JSON state for key while ensuring the lease and
// conditional headers (when provided) are honoured.
func (c *Client) Remove(ctx context.Context, key, leaseID string, opts RemoveOptions) (*api.RemoveResponse, error) {
	namespace, err := c.namespaceFor(opts.Namespace)
	if err != nil {
		return nil, err
	}
	opts.Namespace = namespace
	if opts.TxnID == "" {
		if sess := c.sessionByLease(leaseID); sess != nil {
			opts.TxnID = sess.TxnID
		}
	}
	token, err := c.fencingToken(leaseID, opts.FencingToken)
	if err != nil {
		return nil, err
	}
	c.logTraceCtx(ctx, "client.remove.start", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", c.lastEndpoint, "fencing_token", token)
	path := fmt.Sprintf("/v1/remove?key=%s&namespace=%s", url.QueryEscape(key), url.QueryEscape(namespace))
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, http.NoBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		req.Header.Set("X-Lease-ID", leaseID)
		if opts.TxnID != "" {
			req.Header.Set(headerTxnID, opts.TxnID)
		}
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
		c.logErrorCtx(ctx, "client.remove.transport_error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", endpoint, "fencing_token", token, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.remove.error", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", endpoint, "fencing_token", token, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	var result api.RemoveResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	c.logTraceCtx(ctx, "client.remove.success", "key", key, "lease_id", leaseID, "txn_id", opts.TxnID, "endpoint", endpoint, "fencing_token", token, "removed", result.Removed, "new_version", result.NewVersion)
	if newToken := resp.Header.Get(headerFencingToken); newToken != "" {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	return &result, nil
}

// TxnPrepare records a pending decision for the txn, merging participants/expiry.
func (c *Client) TxnPrepare(ctx context.Context, req api.TxnDecisionRequest) (*api.TxnDecisionResponse, error) {
	req.State = "pending"
	return c.txnDecision(ctx, "/v1/txn/decide", req)
}

// TxnCommit records a commit decision and applies it to participants.
func (c *Client) TxnCommit(ctx context.Context, req api.TxnDecisionRequest) (*api.TxnDecisionResponse, error) {
	req.State = "commit"
	return c.txnDecision(ctx, "/v1/txn/decide", req)
}

// TxnRollback records a rollback decision and applies it to participants.
func (c *Client) TxnRollback(ctx context.Context, req api.TxnDecisionRequest) (*api.TxnDecisionResponse, error) {
	req.State = "rollback"
	return c.txnDecision(ctx, "/v1/txn/decide", req)
}

// TxnReplay replays the decision (or rolls back expired pending) for txnID.
func (c *Client) TxnReplay(ctx context.Context, txnID string) (*api.TxnReplayResponse, error) {
	payload := api.TxnReplayRequest{TxnID: strings.TrimSpace(txnID)}
	if payload.TxnID == "" {
		return nil, fmt.Errorf("lockd: txn_id is required")
	}
	var resp api.TxnReplayResponse
	if _, err := c.postJSON(ctx, "/v1/txn/replay", payload, &resp, nil, ""); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) txnDecision(ctx context.Context, path string, req api.TxnDecisionRequest) (*api.TxnDecisionResponse, error) {
	req.TxnID = strings.TrimSpace(req.TxnID)
	if req.TxnID == "" {
		return nil, fmt.Errorf("lockd: txn_id is required")
	}
	var resp api.TxnDecisionResponse
	if _, err := c.postJSON(ctx, path, req, &resp, nil, ""); err != nil {
		return nil, err
	}
	return &resp, nil
}

// APIError describes an error response from lockd.
type APIError struct {
	// Status is the HTTP status code returned by the server.
	Status int
	// Response is the decoded lockd error envelope, when available.
	Response api.ErrorResponse
	// Body contains the raw response body bytes for additional diagnostics.
	Body []byte
	// RetryAfter is the parsed retry delay hint from headers, when provided.
	RetryAfter time.Duration
	// QRFState carries queue-resilience-fallback diagnostics surfaced by the server.
	QRFState string
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
		for k, vals := range headers {
			for _, v := range vals {
				req.Header.Add(k, v)
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
	return c.decodeErrorWithBody(resp, data)
}

func (c *Client) decodeErrorWithBody(resp *http.Response, data []byte) error {
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
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
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
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
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
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
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
	namespace, err := c.namespaceFor(opts.Namespace)
	if err != nil {
		return nil, err
	}
	waitSeconds := opts.BlockSeconds
	if waitSeconds < 0 && waitSeconds != BlockNoWait {
		waitSeconds = BlockNoWait
	}
	if waitSeconds == BlockNoWait {
		waitSeconds = api.BlockNoWait
	}
	req := api.DequeueRequest{
		Namespace:                namespace,
		Queue:                    queue,
		Owner:                    owner,
		TxnID:                    strings.TrimSpace(opts.TxnID),
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
				namespace:      handle.msg.Namespace,
				stateKey:       queueStateKey(handle.msg.Queue, handle.msg.MessageID),
				queue:          handle.msg.Queue,
				messageID:      handle.msg.MessageID,
				leaseID:        handle.msg.StateLeaseID,
				txnID:          handle.msg.StateTxnID,
				fencingToken:   handle.msg.StateFencingToken,
				leaseExpiresAt: handle.msg.StateLeaseExpiresAtUnix,
				queueStateETag: handle.msg.StateETag,
				correlationID:  handle.msg.CorrelationID,
			}
			c.RegisterLeaseToken(handle.state.LeaseID(), strconv.FormatInt(handle.state.FencingToken(), 10))
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
	namespace, err := c.namespaceFor(opts.Namespace)
	if err != nil {
		return nil, err
	}
	meta := api.EnqueueRequest{
		Namespace:                namespace,
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
// While the handler runs, the client renews the in-flight queue lease implicitly
// via QueueExtend to reduce timeout risk for long-running handlers.
func (c *Client) Subscribe(ctx context.Context, queue string, opts SubscribeOptions, handler MessageHandler) error {
	if handler == nil {
		return fmt.Errorf("lockd: message handler required")
	}
	return c.subscribe(ctx, queue, opts, handler, nil)
}

// SubscribeWithState streams queue messages with workflow state and invokes handler for each delivery.
// While the handler runs, the client renews both the message lease and the
// associated state lease implicitly via QueueExtend.
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
	namespace, err := c.namespaceFor("")
	if err != nil {
		return err
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
		Namespace:                namespace,
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

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
		if err != nil {
			return err
		}
		c.observeShutdown(endpoint, resp)

		if resp.StatusCode != http.StatusOK {
			apiErr := c.decodeError(resp)
			resp.Body.Close()
			cancel()
			if apiErr != nil {
				if typed, ok := apiErr.(*APIError); ok && typed.Response.ErrorCode == "waiting" {
					time.Sleep(500 * time.Microsecond)
					continue
				}
				if typed, ok := apiErr.(*APIError); ok && typed.Response.ErrorCode == "cas_mismatch" {
					time.Sleep(500 * time.Microsecond)
					continue
				}
				return apiErr
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("lockd: subscribe unexpected status %d", resp.StatusCode)
		}

		mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
			resp.Body.Close()
			cancel()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("lockd: unexpected subscribe content-type %q", resp.Header.Get("Content-Type"))
		}
		boundary := params["boundary"]
		if boundary == "" {
			resp.Body.Close()
			cancel()
			return fmt.Errorf("lockd: multipart response missing boundary")
		}

		mr := multipart.NewReader(resp.Body, boundary)
		headerCID := resp.Header.Get(headerCorrelationID)
		delivered := 0

		for {
			select {
			case <-ctx.Done():
				resp.Body.Close()
				cancel()
				return ctx.Err()
			default:
			}

			metaPart, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				resp.Body.Close()
				cancel()
				return fmt.Errorf("lockd: subscribe meta part: %w", err)
			}
			if name := metaPart.FormName(); name != "" && name != "meta" {
				metaPart.Close()
				resp.Body.Close()
				cancel()
				return fmt.Errorf("lockd: unexpected multipart part %q", name)
			}
			var apiResp api.DequeueResponse
			if err := json.NewDecoder(metaPart).Decode(&apiResp); err != nil {
				metaPart.Close()
				resp.Body.Close()
				cancel()
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
				resp.Body.Close()
				cancel()
				return fmt.Errorf("lockd: subscribe payload part: %w", err)
			} else {
				if name := payloadPart.FormName(); name != "" && name != "payload" {
					payloadPart.Close()
					resp.Body.Close()
					cancel()
					return fmt.Errorf("lockd: unexpected multipart part %q", name)
				}
				if lengthHeader := payloadPart.Header.Get("Content-Length"); lengthHeader != "" {
					if size, parseErr := strconv.ParseInt(lengthHeader, 10, 64); parseErr == nil && size >= 0 {
						payloadBytes = make([]byte, size)
						if _, err := io.ReadFull(payloadPart, payloadBytes); err != nil {
							payloadPart.Close()
							resp.Body.Close()
							cancel()
							return fmt.Errorf("lockd: read subscribe payload (len=%d): %w", size, err)
						}
					} else {
						payloadBytes, err = io.ReadAll(payloadPart)
						payloadPart.Close()
						if err != nil {
							resp.Body.Close()
							cancel()
							return fmt.Errorf("lockd: read subscribe payload: %w", err)
						}
					}
				} else {
					payloadBytes, err = io.ReadAll(payloadPart)
					payloadPart.Close()
					if err != nil {
						resp.Body.Close()
						cancel()
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
					namespace:      handle.msg.Namespace,
					stateKey:       queueStateKey(handle.msg.Queue, handle.msg.MessageID),
					queue:          handle.msg.Queue,
					messageID:      handle.msg.MessageID,
					leaseID:        handle.msg.StateLeaseID,
					txnID:          handle.msg.StateTxnID,
					fencingToken:   handle.msg.StateFencingToken,
					leaseExpiresAt: handle.msg.StateLeaseExpiresAtUnix,
					queueStateETag: handle.msg.StateETag,
					correlationID:  handle.msg.CorrelationID,
				}
				c.RegisterLeaseToken(handle.state.LeaseID(), strconv.FormatInt(handle.state.FencingToken(), 10))
			}

			msg := newQueueMessage(handle, nil, opts.OnCloseDelay)
			var handlerErr error
			if stateful {
				handlerErr = c.runQueueHandlerWithAutoExtend(ctx, msg, func(handlerCtx context.Context) error {
					return stateHandler(handlerCtx, msg, msg.StateHandle())
				})
			} else {
				handlerErr = c.runQueueHandlerWithAutoExtend(ctx, msg, func(handlerCtx context.Context) error {
					return handler(handlerCtx, msg)
				})
			}
			if handlerErr != nil {
				resp.Body.Close()
				cancel()
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
		resp.Body.Close()
		cancel()
		return nil
	}
}

func queueAutoExtendDelay(visibility time.Duration) time.Duration {
	if visibility <= 0 {
		return defaultQueueAutoExtendTick
	}
	delay := visibility / 2
	if delay < minQueueAutoExtendInterval {
		delay = minQueueAutoExtendInterval
	}
	if delay > maxQueueAutoExtendInterval {
		delay = maxQueueAutoExtendInterval
	}
	return delay
}

func queueAutoExtendBy(visibility time.Duration) time.Duration {
	if visibility <= 0 {
		return 0
	}
	return visibility
}

func queueMessageClosed(msg *QueueMessage) bool {
	if msg == nil || msg.handle == nil {
		return true
	}
	msg.handle.mu.Lock()
	defer msg.handle.mu.Unlock()
	return msg.handle.done
}

func (c *Client) autoExtendQueueMessage(ctx context.Context, msg *QueueMessage, stop <-chan struct{}) error {
	for {
		delay := queueAutoExtendDelay(msg.VisibilityTimeout())
		timer := time.NewTimer(delay)
		select {
		case <-stop:
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
		if queueMessageClosed(msg) {
			return nil
		}
		extendBy := queueAutoExtendBy(msg.VisibilityTimeout())
		if err := msg.Extend(ctx, extendBy); err != nil {
			if ctx.Err() != nil || queueMessageClosed(msg) {
				return nil
			}
			return fmt.Errorf("lockd: queue lease auto-extend failed: %w", err)
		}
	}
}

func (c *Client) runQueueHandlerWithAutoExtend(ctx context.Context, msg *QueueMessage, handler func(context.Context) error) error {
	if msg == nil || msg.handle == nil || handler == nil {
		if handler == nil {
			return nil
		}
		return handler(ctx)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	handlerDone := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				handlerDone <- consumerPanicError("message handler", r)
			}
		}()
		handlerDone <- handler(runCtx)
	}()

	stopExtend := make(chan struct{})
	extendDone := make(chan error, 1)
	go func() {
		extendDone <- c.autoExtendQueueMessage(runCtx, msg, stopExtend)
	}()

	var extendErr error
	extendSettled := false
	for {
		select {
		case err := <-handlerDone:
			cancel()
			close(stopExtend)
			if !extendSettled {
				if e := <-extendDone; e != nil {
					extendErr = e
				}
			}
			if err != nil && extendErr != nil {
				return errors.Join(err, extendErr)
			}
			if err != nil {
				return err
			}
			return extendErr
		case err := <-extendDone:
			extendSettled = true
			if err == nil {
				continue
			}
			extendErr = err
			cancel()
		}
	}
}

type consumerRuntimeConfig struct {
	name           string
	queue          string
	options        SubscribeOptions
	withState      bool
	messageHandler ConsumerMessageHandler
	errorHandler   ConsumerErrorHandler
	onStart        func(context.Context, ConsumerLifecycleEvent)
	onStop         func(context.Context, ConsumerLifecycleEvent)
	restartPolicy  ConsumerRestartPolicy
}

func defaultConsumerRestartPolicy() ConsumerRestartPolicy {
	return ConsumerRestartPolicy{
		ImmediateRetries: DefaultConsumerImmediateRetries,
		BaseDelay:        DefaultConsumerBaseDelay,
		MaxDelay:         DefaultConsumerMaxDelay,
		Multiplier:       DefaultConsumerMultiplier,
		Jitter:           DefaultConsumerJitter,
	}
}

func normalizeConsumerRestartPolicy(policy ConsumerRestartPolicy) ConsumerRestartPolicy {
	defaults := defaultConsumerRestartPolicy()
	if policy.ImmediateRetries < 0 {
		policy.ImmediateRetries = 0
	}
	if policy.BaseDelay <= 0 {
		policy.BaseDelay = defaults.BaseDelay
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = defaults.MaxDelay
	}
	if policy.BaseDelay > policy.MaxDelay {
		policy.BaseDelay = policy.MaxDelay
	}
	if policy.Multiplier <= 1 {
		policy.Multiplier = defaults.Multiplier
	}
	if policy.Jitter < 0 {
		policy.Jitter = 0
	}
	if policy.MaxFailures < 0 {
		policy.MaxFailures = 0
	}
	return policy
}

func normalizeConsumerConfigs(consumers []ConsumerConfig) ([]consumerRuntimeConfig, error) {
	if len(consumers) == 0 {
		return nil, fmt.Errorf("lockd: at least one consumer config is required")
	}
	normalized := make([]consumerRuntimeConfig, 0, len(consumers))
	for idx, cfg := range consumers {
		name := strings.TrimSpace(cfg.Name)
		queue := strings.TrimSpace(cfg.Queue)
		if queue == "" {
			return nil, fmt.Errorf("lockd: consumer config[%d] queue is required", idx)
		}
		if name == "" {
			name = queue
		}
		if cfg.MessageHandler == nil {
			return nil, fmt.Errorf("lockd: consumer config[%d] message handler is required", idx)
		}
		opts := cfg.Options
		opts.Owner = strings.TrimSpace(opts.Owner)
		if opts.Owner == "" {
			opts.Owner = defaultConsumerOwner(name)
		}
		normalized = append(normalized, consumerRuntimeConfig{
			name:           name,
			queue:          queue,
			options:        opts,
			withState:      cfg.WithState,
			messageHandler: cfg.MessageHandler,
			errorHandler:   cfg.ErrorHandler,
			onStart:        cfg.OnStart,
			onStop:         cfg.OnStop,
			restartPolicy:  normalizeConsumerRestartPolicy(cfg.RestartPolicy),
		})
	}
	return normalized, nil
}

func sanitizeConsumerOwnerToken(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	lastDash := false
	for _, r := range s {
		allowed := (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '.'
		if !allowed {
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
			continue
		}
		b.WriteRune(r)
		lastDash = r == '-'
	}
	token := strings.Trim(b.String(), "-")
	return token
}

func defaultConsumerOwner(name string) string {
	base := sanitizeConsumerOwnerToken(name)
	if base == "" {
		base = "consumer"
	}
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	host = sanitizeConsumerOwnerToken(host)
	if host == "" {
		host = "unknown-host"
	}
	seq := consumerOwnerSeq.Add(1)
	return fmt.Sprintf("consumer-%s-%s-%d-%d", base, host, os.Getpid(), seq)
}

func consumerRestartDelay(failures int, policy ConsumerRestartPolicy) time.Duration {
	if failures <= 0 {
		return 0
	}
	if failures <= policy.ImmediateRetries {
		return 0
	}
	delayedAttempt := failures - policy.ImmediateRetries
	delay := policy.BaseDelay
	for i := 1; i < delayedAttempt; i++ {
		next := time.Duration(float64(delay) * policy.Multiplier)
		if next <= delay {
			next = policy.MaxDelay
		}
		delay = next
		if delay >= policy.MaxDelay {
			delay = policy.MaxDelay
			break
		}
	}
	if delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}
	if policy.Jitter <= 0 || delay <= 0 {
		return delay
	}
	j := policy.Jitter
	if delay < j {
		j = delay
	}
	if j <= 0 {
		return delay
	}
	span := int64(j)*2 + 1
	if span <= 0 {
		return delay
	}
	delay += time.Duration(rand.Int63n(span)) - j
	if delay < 0 {
		delay = 0
	}
	if delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}
	return delay
}

func consumerPanicError(component string, recovered any) error {
	stack := strings.TrimSpace(string(debug.Stack()))
	if stack == "" {
		return fmt.Errorf("lockd: consumer %s panic: %v", component, recovered)
	}
	return fmt.Errorf("lockd: consumer %s panic: %v\n%s", component, recovered, stack)
}

func invokeConsumerLifecycleHook(ctx context.Context, hook func(context.Context, ConsumerLifecycleEvent), event ConsumerLifecycleEvent) (err error) {
	if hook == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = consumerPanicError("lifecycle hook", r)
		}
	}()
	hook(ctx, event)
	return nil
}

func invokeConsumerErrorHandler(ctx context.Context, handler ConsumerErrorHandler, event ConsumerError) (err error) {
	if handler == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = consumerPanicError("error handler", r)
		}
	}()
	return handler(ctx, event)
}

func (c *Client) runConsumerAttemptRecover(ctx context.Context, cfg consumerRuntimeConfig) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = consumerPanicError("message handler", r)
		}
	}()
	return c.runConsumerAttempt(ctx, cfg)
}

func (c *Client) runConsumerLoop(ctx context.Context, cfg consumerRuntimeConfig) error {
	attempt := 0
	failures := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		attempt++
		if hookErr := invokeConsumerLifecycleHook(ctx, cfg.onStart, ConsumerLifecycleEvent{
			Name:      cfg.name,
			Queue:     cfg.queue,
			WithState: cfg.withState,
			Attempt:   attempt,
		}); hookErr != nil {
			if err := c.handleConsumerFailure(ctx, cfg, &failures, hookErr); err != nil {
				return err
			}
			continue
		}

		err := c.runConsumerAttemptRecover(ctx, cfg)
		stopErr := invokeConsumerLifecycleHook(ctx, cfg.onStop, ConsumerLifecycleEvent{
			Name:      cfg.name,
			Queue:     cfg.queue,
			WithState: cfg.withState,
			Attempt:   attempt,
			Err:       err,
		})
		if stopErr != nil {
			err = errors.Join(err, stopErr)
		}
		if err == nil {
			failures = 0
			continue
		}
		if err := c.handleConsumerFailure(ctx, cfg, &failures, err); err != nil {
			return err
		}
	}
}

func (c *Client) handleConsumerFailure(ctx context.Context, cfg consumerRuntimeConfig, failures *int, runErr error) error {
	if runErr == nil {
		return nil
	}
	if ctx.Err() != nil {
		return nil
	}
	if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		if ctx.Err() != nil {
			return nil
		}
	}
	*failures = *failures + 1
	if cfg.restartPolicy.MaxFailures > 0 && *failures > cfg.restartPolicy.MaxFailures {
		return fmt.Errorf("lockd: consumer %q queue %q exceeded max failures (%d): %w", cfg.name, cfg.queue, cfg.restartPolicy.MaxFailures, runErr)
	}
	delay := consumerRestartDelay(*failures, cfg.restartPolicy)
	event := ConsumerError{
		Name:      cfg.name,
		Queue:     cfg.queue,
		WithState: cfg.withState,
		Attempt:   *failures,
		RestartIn: delay,
		Err:       runErr,
	}
	if cfg.errorHandler != nil {
		if handlerErr := invokeConsumerErrorHandler(ctx, cfg.errorHandler, event); handlerErr != nil {
			return fmt.Errorf("lockd: consumer %q queue %q error handler: %w", cfg.name, cfg.queue, handlerErr)
		}
	} else {
		c.logWarnCtx(ctx,
			"client.consumer.restart",
			"consumer", cfg.name,
			"queue", cfg.queue,
			"stateful", cfg.withState,
			"attempt", *failures,
			"restart_in", delay,
			"error", runErr,
		)
	}
	if delay <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(delay):
	}
	return nil
}

func (c *Client) runConsumerAttempt(ctx context.Context, cfg consumerRuntimeConfig) error {
	if cfg.withState {
		return c.SubscribeWithState(ctx, cfg.queue, cfg.options, func(handlerCtx context.Context, msg *QueueMessage, state *QueueStateHandle) error {
			return cfg.messageHandler(handlerCtx, ConsumerMessage{
				Client:       c,
				Logger:       c.logger,
				Queue:        cfg.queue,
				WithState:    true,
				Message:      msg,
				State:        state,
				consumerName: cfg.name,
			})
		})
	}
	return c.Subscribe(ctx, cfg.queue, cfg.options, func(handlerCtx context.Context, msg *QueueMessage) error {
		return cfg.messageHandler(handlerCtx, ConsumerMessage{
			Client:       c,
			Logger:       c.logger,
			Queue:        cfg.queue,
			WithState:    false,
			Message:      msg,
			consumerName: cfg.name,
		})
	})
}

// StartConsumer starts one or more long-running queue consumers and blocks until
// they terminate. Each ConsumerConfig runs in its own goroutine and restarts on
// failure according to RestartPolicy. Panics from message handlers, lifecycle
// hooks, and error handlers are recovered and treated as consume-loop failures.
// Cancel ctx to stop all consumers; context cancellation returns nil.
func (c *Client) StartConsumer(ctx context.Context, consumers ...ConsumerConfig) error {
	if c == nil {
		return fmt.Errorf("lockd: client is nil")
	}
	if ctx == nil {
		return fmt.Errorf("lockd: context is required")
	}
	normalized, err := normalizeConsumerConfigs(consumers)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(normalized))
	var wg sync.WaitGroup
	for _, cfg := range normalized {
		cfg := cfg
		wg.Add(1)
		go func() {
			defer wg.Done()
			if runErr := c.runConsumerLoop(runCtx, cfg); runErr != nil {
				select {
				case errCh <- runErr:
				default:
				}
				cancel()
			}
		}()
	}

	wg.Wait()
	close(errCh)
	var firstErr error
	for runErr := range errCh {
		if runErr != nil && firstErr == nil {
			firstErr = runErr
		}
	}
	if firstErr != nil {
		return firstErr
	}
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

func buildClientTLS(bundle *tlsutil.ClientBundle) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{bundle.Certificate},
		RootCAs:            bundle.CAPool,
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyServerCertificate(rawCerts, bundle.CAPool)
		},
	}
}

func verifyServerCertificate(rawCerts [][]byte, roots *x509.CertPool) error {
	if len(rawCerts) == 0 {
		return errors.New("mtls: missing server certificate")
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("mtls: parse server certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	leaf := certs[0]
	opts := x509.VerifyOptions{
		Roots:         roots,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		Intermediates: x509.NewCertPool(),
		CurrentTime:   time.Now(),
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := leaf.Verify(opts); err != nil {
		return fmt.Errorf("mtls: verify server certificate: %w", err)
	}
	return nil
}
