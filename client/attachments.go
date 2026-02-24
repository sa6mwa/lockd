package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"pkt.systems/lockd/api"
)

const (
	headerAttachmentID        = "X-Attachment-ID"
	headerAttachmentName      = "X-Attachment-Name"
	headerAttachmentSize      = "X-Attachment-Size"
	headerAttachmentCreatedAt = "X-Attachment-Created-At"
	headerAttachmentUpdatedAt = "X-Attachment-Updated-At"
	headerAttachmentSHA256    = "X-Attachment-SHA256"
)

// AttachmentInfo describes attachment metadata.
type AttachmentInfo struct {
	// ID is the unique identifier for the referenced object.
	ID string
	// Name is the human-readable identifier for the referenced object.
	Name string
	// Size is the payload size in bytes.
	Size int64
	// PlaintextSHA256 is the SHA-256 checksum of the uploaded plaintext payload.
	PlaintextSHA256 string
	// ContentType is the media type associated with the payload.
	ContentType string
	// CreatedAtUnix is the creation timestamp as Unix seconds.
	CreatedAtUnix int64
	// UpdatedAtUnix is the last update timestamp as Unix seconds.
	UpdatedAtUnix int64
}

// Attachment exposes a streaming attachment payload.
type Attachment struct {
	AttachmentInfo
	reader   io.ReadCloser
	deleteFn func(context.Context) error
	once     sync.Once
}

// Read implements io.Reader.
func (a *Attachment) Read(p []byte) (int, error) {
	if a == nil || a.reader == nil {
		return 0, io.EOF
	}
	return a.reader.Read(p)
}

// Close releases the underlying reader.
func (a *Attachment) Close() error {
	if a == nil || a.reader == nil {
		return nil
	}
	var err error
	a.once.Do(func() {
		err = a.reader.Close()
	})
	return err
}

// Delete removes the attachment from the state key when a lease is available.
func (a *Attachment) Delete(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if a.deleteFn == nil {
		return fmt.Errorf("lockd: attachment delete requires a lease")
	}
	return a.deleteFn(ctx)
}

// AttachRequest captures parameters for staging an attachment.
type AttachRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string
	// Key identifies the lock/state key within the namespace.
	Key string
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken *int64
	// Name is the human-readable identifier for the referenced object.
	Name string
	// Body provides the request or response payload stream/content.
	Body io.Reader
	// ContentType is the media type associated with the payload.
	ContentType string
	// MaxBytes optionally enforces an upper bound for attachment payload size in bytes.
	MaxBytes *int64
	// PreventOverwrite rejects the request when an attachment with the same selector already exists.
	PreventOverwrite bool
}

// AttachResult reports the staged attachment metadata.
type AttachResult struct {
	// Attachment contains metadata for the staged or retrieved attachment.
	Attachment AttachmentInfo
	// Noop is true when attach detected identical existing content and skipped a write.
	Noop bool
	// Version is the lockd monotonic version for the target object.
	Version int64
}

// AttachmentSelector identifies an attachment by id or name.
type AttachmentSelector struct {
	// ID is the unique identifier for the referenced object.
	ID string
	// Name is the human-readable identifier for the referenced object.
	Name string
}

// ListAttachmentsRequest lists attachments for a key.
type ListAttachmentsRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string
	// Key identifies the lock/state key within the namespace.
	Key string
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken *int64
	// Public enables read-only attachment listing without lease credentials when public reads are allowed.
	Public bool
}

// AttachmentList collects attachment metadata for a key.
type AttachmentList struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string
	// Key identifies the lock/state key within the namespace.
	Key string
	// Attachments enumerates attachments associated with the target key.
	Attachments []AttachmentInfo
}

// GetAttachmentRequest retrieves a single attachment payload.
type GetAttachmentRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string
	// Key identifies the lock/state key within the namespace.
	Key string
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken *int64
	// Public enables read-only attachment retrieval without lease credentials when public reads are allowed.
	Public bool
	// Selector identifies which attachment to retrieve (by ID, Name, or both).
	Selector AttachmentSelector
}

// DeleteAttachmentRequest removes a single attachment.
type DeleteAttachmentRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string
	// Key identifies the lock/state key within the namespace.
	Key string
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken *int64
	// Selector identifies which attachment to delete (by ID, Name, or both).
	Selector AttachmentSelector
}

// DeleteAllAttachmentsRequest removes all attachments for a key.
type DeleteAllAttachmentsRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string
	// Key identifies the lock/state key within the namespace.
	Key string
	// LeaseID identifies the active lease required for protected mutations.
	LeaseID string
	// TxnID associates the operation with a transaction coordinator record.
	TxnID string
	// FencingToken is the monotonic token used to fence stale writers.
	FencingToken *int64
}

// DeleteAttachmentResult reports delete status for a single attachment.
type DeleteAttachmentResult struct {
	// Deleted reports delete results for the requested attachment operation.
	Deleted bool
	// Version is the lockd monotonic version for the target object.
	Version int64
}

// DeleteAllAttachmentsResult reports delete status for all attachments.
type DeleteAllAttachmentsResult struct {
	// Deleted reports delete results for the requested attachment operation.
	Deleted int
	// Version is the lockd monotonic version for the target object.
	Version int64
}

// AttachmentStore exposes attachment operations scoped to a key.
type AttachmentStore struct {
	client    *Client
	namespace string
	key       string
	leaseID   string
	txnID     string
	public    bool
}

// Attach stages an attachment payload using the configured lease context.
func (s *AttachmentStore) Attach(ctx context.Context, req AttachRequest) (*AttachResult, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("lockd: attachment client unavailable")
	}
	if s.public {
		return nil, fmt.Errorf("lockd: attachment upload requires a lease")
	}
	if req.Namespace == "" {
		req.Namespace = s.namespace
	}
	if req.Key == "" {
		req.Key = s.key
	}
	if req.LeaseID == "" {
		req.LeaseID = s.leaseID
	}
	if req.TxnID == "" {
		req.TxnID = s.txnID
	}
	return s.client.Attach(ctx, req)
}

// List returns attachment metadata for the key.
func (s *AttachmentStore) List(ctx context.Context) (*AttachmentList, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("lockd: attachment client unavailable")
	}
	return s.client.ListAttachments(ctx, ListAttachmentsRequest{
		Namespace: s.namespace,
		Key:       s.key,
		LeaseID:   s.leaseID,
		TxnID:     s.txnID,
		Public:    s.public,
	})
}

// Retrieve streams a single attachment payload.
func (s *AttachmentStore) Retrieve(ctx context.Context, selector AttachmentSelector) (*Attachment, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("lockd: attachment client unavailable")
	}
	return s.client.GetAttachment(ctx, GetAttachmentRequest{
		Namespace: s.namespace,
		Key:       s.key,
		LeaseID:   s.leaseID,
		TxnID:     s.txnID,
		Public:    s.public,
		Selector:  selector,
	})
}

// RetrieveAll loads all attachments and returns a slice of Attachment readers.
func (s *AttachmentStore) RetrieveAll(ctx context.Context) ([]*Attachment, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("lockd: attachment client unavailable")
	}
	listing, err := s.List(ctx)
	if err != nil {
		return nil, err
	}
	if listing == nil || len(listing.Attachments) == 0 {
		return nil, nil
	}
	out := make([]*Attachment, 0, len(listing.Attachments))
	for _, info := range listing.Attachments {
		att, err := s.Retrieve(ctx, AttachmentSelector{ID: info.ID, Name: info.Name})
		if err != nil {
			for _, opened := range out {
				_ = opened.Close()
			}
			return nil, err
		}
		out = append(out, att)
	}
	return out, nil
}

// Delete removes a single attachment.
func (s *AttachmentStore) Delete(ctx context.Context, selector AttachmentSelector) (*DeleteAttachmentResult, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("lockd: attachment client unavailable")
	}
	if s.public {
		return nil, fmt.Errorf("lockd: attachment delete requires a lease")
	}
	return s.client.DeleteAttachment(ctx, DeleteAttachmentRequest{
		Namespace: s.namespace,
		Key:       s.key,
		LeaseID:   s.leaseID,
		TxnID:     s.txnID,
		Selector:  selector,
	})
}

// DeleteAll removes all attachments for the key.
func (s *AttachmentStore) DeleteAll(ctx context.Context) (*DeleteAllAttachmentsResult, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("lockd: attachment client unavailable")
	}
	if s.public {
		return nil, fmt.Errorf("lockd: attachment delete requires a lease")
	}
	return s.client.DeleteAllAttachments(ctx, DeleteAllAttachmentsRequest{
		Namespace: s.namespace,
		Key:       s.key,
		LeaseID:   s.leaseID,
		TxnID:     s.txnID,
	})
}

// Attachments returns an attachment store scoped to the lease session.
func (s *LeaseSession) Attachments() *AttachmentStore {
	if s == nil || s.client == nil {
		return nil
	}
	return &AttachmentStore{
		client:    s.client,
		namespace: s.Namespace,
		key:       s.Key,
		leaseID:   s.LeaseID,
		txnID:     s.TxnID,
		public:    false,
	}
}

// Attach stages an attachment on the active lease.
func (s *LeaseSession) Attach(ctx context.Context, req AttachRequest) (*AttachResult, error) {
	store := s.Attachments()
	if store == nil {
		return nil, fmt.Errorf("lockd: lease session unavailable")
	}
	res, err := store.Attach(ctx, req)
	if err == nil && res != nil && res.Version > 0 {
		s.mu.Lock()
		s.Version = res.Version
		s.mu.Unlock()
	}
	return res, err
}

// ListAttachments lists attachment metadata for the lease key.
func (s *LeaseSession) ListAttachments(ctx context.Context) (*AttachmentList, error) {
	store := s.Attachments()
	if store == nil {
		return nil, fmt.Errorf("lockd: lease session unavailable")
	}
	return store.List(ctx)
}

// RetrieveAttachment streams a single attachment payload for the lease key.
func (s *LeaseSession) RetrieveAttachment(ctx context.Context, selector AttachmentSelector) (*Attachment, error) {
	store := s.Attachments()
	if store == nil {
		return nil, fmt.Errorf("lockd: lease session unavailable")
	}
	return store.Retrieve(ctx, selector)
}

// RetrieveAllAttachments streams all attachments for the lease key.
func (s *LeaseSession) RetrieveAllAttachments(ctx context.Context) ([]*Attachment, error) {
	store := s.Attachments()
	if store == nil {
		return nil, fmt.Errorf("lockd: lease session unavailable")
	}
	return store.RetrieveAll(ctx)
}

// DeleteAttachment removes a single attachment for the lease key.
func (s *LeaseSession) DeleteAttachment(ctx context.Context, selector AttachmentSelector) (*DeleteAttachmentResult, error) {
	store := s.Attachments()
	if store == nil {
		return nil, fmt.Errorf("lockd: lease session unavailable")
	}
	res, err := store.Delete(ctx, selector)
	if err == nil && res != nil && res.Version > 0 {
		s.mu.Lock()
		s.Version = res.Version
		s.mu.Unlock()
	}
	return res, err
}

// DeleteAllAttachments removes all attachments for the lease key.
func (s *LeaseSession) DeleteAllAttachments(ctx context.Context) (*DeleteAllAttachmentsResult, error) {
	store := s.Attachments()
	if store == nil {
		return nil, fmt.Errorf("lockd: lease session unavailable")
	}
	res, err := store.DeleteAll(ctx)
	if err == nil && res != nil && res.Version > 0 {
		s.mu.Lock()
		s.Version = res.Version
		s.mu.Unlock()
	}
	return res, err
}

// Attachments returns an attachment store scoped to the get response.
func (gr *GetResponse) Attachments() *AttachmentStore {
	if gr == nil || gr.client == nil {
		return nil
	}
	return &AttachmentStore{
		client:    gr.client,
		namespace: gr.Namespace,
		key:       gr.Key,
		public:    true,
	}
}

// ListAttachments lists attachment metadata for the get response.
func (gr *GetResponse) ListAttachments(ctx context.Context) (*AttachmentList, error) {
	store := gr.Attachments()
	if store == nil {
		return nil, fmt.Errorf("lockd: attachment client unavailable")
	}
	if !gr.public {
		return nil, fmt.Errorf("lockd: attachments require public reads or a lease session")
	}
	return store.List(ctx)
}

// RetrieveAttachment streams a single attachment payload for the get response.
func (gr *GetResponse) RetrieveAttachment(ctx context.Context, selector AttachmentSelector) (*Attachment, error) {
	store := gr.Attachments()
	if store == nil {
		return nil, fmt.Errorf("lockd: attachment client unavailable")
	}
	if !gr.public {
		return nil, fmt.Errorf("lockd: attachments require public reads or a lease session")
	}
	return store.Retrieve(ctx, selector)
}

// Attach stages an attachment payload for the key.
func (c *Client) Attach(ctx context.Context, req AttachRequest) (*AttachResult, error) {
	if c == nil {
		return nil, fmt.Errorf("lockd: client nil")
	}
	if req.Body == nil {
		return nil, fmt.Errorf("lockd: attachment body required")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return nil, fmt.Errorf("lockd: key required")
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("lockd: attachment name required")
	}
	leaseID := strings.TrimSpace(req.LeaseID)
	if leaseID == "" {
		leaseID = c.defaultLease()
	}
	if leaseID == "" {
		return nil, fmt.Errorf("lockd: lease_id required")
	}
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
	if req.TxnID == "" {
		if sess := c.sessionByLease(leaseID); sess != nil {
			req.TxnID = sess.TxnID
		}
	}
	if req.TxnID == "" {
		return nil, fmt.Errorf("lockd: txn_id required")
	}
	token, err := c.fencingToken(leaseID, req.FencingToken)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/attachments?key=%s&namespace=%s&name=%s", url.QueryEscape(key), url.QueryEscape(namespace), url.QueryEscape(name))
	if req.ContentType != "" {
		path += "&content_type=" + url.QueryEscape(req.ContentType)
	}
	if req.PreventOverwrite {
		path += "&prevent_overwrite=1"
	}
	if req.MaxBytes != nil {
		path += "&max_bytes=" + strconv.FormatInt(*req.MaxBytes, 10)
	}
	c.logTraceCtx(ctx, "client.attach.start", "key", key, "lease_id", leaseID, "txn_id", req.TxnID, "endpoint", c.lastEndpoint)

	factory, replayable := attachmentBodyFactory(req.Body)
	used := false
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		var body io.Reader
		if replayable {
			var err error
			body, err = factory()
			if err != nil {
				return nil, nil, err
			}
		} else {
			if used {
				return nil, nil, fmt.Errorf("lockd: attachment body is not replayable")
			}
			used = true
			body = req.Body
		}
		reqCtx, cancel := c.requestContext(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, base+path, body)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		contentType := strings.TrimSpace(req.ContentType)
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		httpReq.Header.Set("Content-Type", contentType)
		httpReq.Header.Set("X-Lease-ID", leaseID)
		httpReq.Header.Set(headerTxnID, req.TxnID)
		httpReq.Header.Set(headerFencingToken, strconv.FormatInt(token, 10))
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.attach.transport_error", "key", key, "lease_id", leaseID, "txn_id", req.TxnID, "endpoint", endpoint, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.attach.error", "key", key, "lease_id", leaseID, "txn_id", req.TxnID, "endpoint", endpoint, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	var apiResp api.AttachResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}
	if newToken, ok := parseFencingTokenHeader(resp.Header.Get(headerFencingToken)); ok {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	result := &AttachResult{
		Attachment: attachmentInfoFromAPI(apiResp.Attachment),
		Noop:       apiResp.Noop,
		Version:    apiResp.Version,
	}
	c.logTraceCtx(ctx, "client.attach.success", "key", key, "lease_id", leaseID, "txn_id", req.TxnID, "endpoint", endpoint, "attachment_id", result.Attachment.ID)
	return result, nil
}

// ListAttachments lists attachments for a key.
func (c *Client) ListAttachments(ctx context.Context, req ListAttachmentsRequest) (*AttachmentList, error) {
	if c == nil {
		return nil, fmt.Errorf("lockd: client nil")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return nil, fmt.Errorf("lockd: key required")
	}
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
	leaseID := strings.TrimSpace(req.LeaseID)
	if leaseID == "" {
		leaseID = c.defaultLease()
	}
	public := req.Public && leaseID == ""
	if !public && leaseID == "" {
		return nil, fmt.Errorf("lockd: lease_id required")
	}
	if !public && req.TxnID == "" {
		if sess := c.sessionByLease(leaseID); sess != nil {
			req.TxnID = sess.TxnID
		}
	}
	if !public && req.TxnID == "" {
		return nil, fmt.Errorf("lockd: txn_id required")
	}
	path := fmt.Sprintf("/v1/attachments?key=%s&namespace=%s", url.QueryEscape(key), url.QueryEscape(namespace))
	if public {
		path += "&public=1"
	}
	c.logTraceCtx(ctx, "client.attachments.list.start", "key", key, "namespace", namespace, "endpoint", c.lastEndpoint)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodGet, base+path, http.NoBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		if !public {
			token, err := c.fencingToken(leaseID, req.FencingToken)
			if err != nil {
				cancel()
				return nil, nil, err
			}
			httpReq.Header.Set("X-Lease-ID", leaseID)
			httpReq.Header.Set(headerTxnID, req.TxnID)
			httpReq.Header.Set(headerFencingToken, strconv.FormatInt(token, 10))
		}
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.attachments.list.transport_error", "key", key, "endpoint", endpoint, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.attachments.list.error", "key", key, "endpoint", endpoint, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	var apiResp api.AttachmentListResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}
	result := &AttachmentList{
		Namespace:   apiResp.Namespace,
		Key:         apiResp.Key,
		Attachments: attachmentInfoSliceFromAPI(apiResp.Attachments),
	}
	c.logTraceCtx(ctx, "client.attachments.list.success", "key", key, "endpoint", endpoint, "count", len(result.Attachments))
	return result, nil
}

// GetAttachment retrieves a single attachment payload.
func (c *Client) GetAttachment(ctx context.Context, req GetAttachmentRequest) (*Attachment, error) {
	if c == nil {
		return nil, fmt.Errorf("lockd: client nil")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return nil, fmt.Errorf("lockd: key required")
	}
	selector := normalizeAttachmentSelector(req.Selector)
	if selector.ID == "" && selector.Name == "" {
		return nil, fmt.Errorf("lockd: attachment id or name required")
	}
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
	leaseID := strings.TrimSpace(req.LeaseID)
	if leaseID == "" {
		leaseID = c.defaultLease()
	}
	public := req.Public && leaseID == ""
	if !public && leaseID == "" {
		return nil, fmt.Errorf("lockd: lease_id required")
	}
	if !public && req.TxnID == "" {
		if sess := c.sessionByLease(leaseID); sess != nil {
			req.TxnID = sess.TxnID
		}
	}
	if !public && req.TxnID == "" {
		return nil, fmt.Errorf("lockd: txn_id required")
	}
	path := fmt.Sprintf("/v1/attachment?key=%s&namespace=%s", url.QueryEscape(key), url.QueryEscape(namespace))
	if selector.Name != "" {
		path += "&name=" + url.QueryEscape(selector.Name)
	}
	if selector.ID != "" {
		path += "&id=" + url.QueryEscape(selector.ID)
	}
	if public {
		path += "&public=1"
	}
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContextNoTimeout(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodGet, base+path, http.NoBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		if !public {
			token, err := c.fencingToken(leaseID, req.FencingToken)
			if err != nil {
				cancel()
				return nil, nil, err
			}
			httpReq.Header.Set("X-Lease-ID", leaseID)
			httpReq.Header.Set(headerTxnID, req.TxnID)
			httpReq.Header.Set(headerFencingToken, strconv.FormatInt(token, 10))
		}
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.attachment.get.transport_error", "key", key, "endpoint", endpoint, "error", err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer cancel()
		defer resp.Body.Close()
		c.logWarnCtx(ctx, "client.attachment.get.error", "key", key, "endpoint", endpoint, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	reader := &cancelReadCloser{ReadCloser: resp.Body, cancel: cancel}
	info := attachmentInfoFromHeaders(resp.Header, selector)
	att := &Attachment{AttachmentInfo: info, reader: reader}
	if !public {
		if sess := c.sessionByLease(leaseID); sess != nil {
			att.reader = sess.trackAttachment(reader)
		}
		att.deleteFn = func(ctx context.Context) error {
			_, err := c.DeleteAttachment(ctx, DeleteAttachmentRequest{
				Namespace: namespace,
				Key:       key,
				LeaseID:   leaseID,
				TxnID:     req.TxnID,
				Selector:  AttachmentSelector{ID: info.ID, Name: info.Name},
			})
			return err
		}
	}
	c.logTraceCtx(ctx, "client.attachment.get.success", "key", key, "endpoint", endpoint, "attachment_id", info.ID)
	return att, nil
}

// DeleteAttachment removes a single attachment.
func (c *Client) DeleteAttachment(ctx context.Context, req DeleteAttachmentRequest) (*DeleteAttachmentResult, error) {
	if c == nil {
		return nil, fmt.Errorf("lockd: client nil")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return nil, fmt.Errorf("lockd: key required")
	}
	selector := normalizeAttachmentSelector(req.Selector)
	if selector.ID == "" && selector.Name == "" {
		return nil, fmt.Errorf("lockd: attachment id or name required")
	}
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
	leaseID := strings.TrimSpace(req.LeaseID)
	if leaseID == "" {
		leaseID = c.defaultLease()
	}
	if leaseID == "" {
		return nil, fmt.Errorf("lockd: lease_id required")
	}
	if req.TxnID == "" {
		if sess := c.sessionByLease(leaseID); sess != nil {
			req.TxnID = sess.TxnID
		}
	}
	if req.TxnID == "" {
		return nil, fmt.Errorf("lockd: txn_id required")
	}
	path := fmt.Sprintf("/v1/attachment?key=%s&namespace=%s", url.QueryEscape(key), url.QueryEscape(namespace))
	if selector.Name != "" {
		path += "&name=" + url.QueryEscape(selector.Name)
	}
	if selector.ID != "" {
		path += "&id=" + url.QueryEscape(selector.ID)
	}
	c.logTraceCtx(ctx, "client.attachment.delete.start", "key", key, "endpoint", c.lastEndpoint)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodDelete, base+path, http.NoBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		token, err := c.fencingToken(leaseID, req.FencingToken)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("X-Lease-ID", leaseID)
		httpReq.Header.Set(headerTxnID, req.TxnID)
		httpReq.Header.Set(headerFencingToken, strconv.FormatInt(token, 10))
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.attachment.delete.transport_error", "key", key, "endpoint", endpoint, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.attachment.delete.error", "key", key, "endpoint", endpoint, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	var apiResp api.DeleteAttachmentResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}
	if newToken, ok := parseFencingTokenHeader(resp.Header.Get(headerFencingToken)); ok {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	return &DeleteAttachmentResult{Deleted: apiResp.Deleted, Version: apiResp.Version}, nil
}

// DeleteAllAttachments removes all attachments for a key.
func (c *Client) DeleteAllAttachments(ctx context.Context, req DeleteAllAttachmentsRequest) (*DeleteAllAttachmentsResult, error) {
	if c == nil {
		return nil, fmt.Errorf("lockd: client nil")
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return nil, fmt.Errorf("lockd: key required")
	}
	namespace, err := c.namespaceFor(req.Namespace)
	if err != nil {
		return nil, err
	}
	req.Namespace = namespace
	leaseID := strings.TrimSpace(req.LeaseID)
	if leaseID == "" {
		leaseID = c.defaultLease()
	}
	if leaseID == "" {
		return nil, fmt.Errorf("lockd: lease_id required")
	}
	if req.TxnID == "" {
		if sess := c.sessionByLease(leaseID); sess != nil {
			req.TxnID = sess.TxnID
		}
	}
	if req.TxnID == "" {
		return nil, fmt.Errorf("lockd: txn_id required")
	}
	path := fmt.Sprintf("/v1/attachments?key=%s&namespace=%s", url.QueryEscape(key), url.QueryEscape(namespace))
	c.logTraceCtx(ctx, "client.attachments.delete_all.start", "key", key, "endpoint", c.lastEndpoint)
	builder := func(base string) (*http.Request, context.CancelFunc, error) {
		reqCtx, cancel := c.requestContext(ctx)
		httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodDelete, base+path, http.NoBody)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		token, err := c.fencingToken(leaseID, req.FencingToken)
		if err != nil {
			cancel()
			return nil, nil, err
		}
		httpReq.Header.Set("X-Lease-ID", leaseID)
		httpReq.Header.Set(headerTxnID, req.TxnID)
		httpReq.Header.Set(headerFencingToken, strconv.FormatInt(token, 10))
		c.applyCorrelationHeader(ctx, httpReq, "")
		return httpReq, cancel, nil
	}
	resp, cancel, endpoint, err := c.attemptEndpoints(builder, "")
	if err != nil {
		c.logErrorCtx(ctx, "client.attachments.delete_all.transport_error", "key", key, "endpoint", endpoint, "error", err)
		return nil, err
	}
	defer cancel()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		c.logWarnCtx(ctx, "client.attachments.delete_all.error", "key", key, "endpoint", endpoint, "status", resp.StatusCode)
		return nil, c.decodeError(resp)
	}
	var apiResp api.DeleteAllAttachmentsResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}
	if newToken, ok := parseFencingTokenHeader(resp.Header.Get(headerFencingToken)); ok {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	return &DeleteAllAttachmentsResult{Deleted: apiResp.Deleted, Version: apiResp.Version}, nil
}

func attachmentBodyFactory(body io.Reader) (func() (io.Reader, error), bool) {
	if body == nil {
		return func() (io.Reader, error) { return http.NoBody, nil }, true
	}
	if rs, ok := body.(io.ReadSeeker); ok {
		return func() (io.Reader, error) {
			_, err := rs.Seek(0, io.SeekStart)
			return rs, err
		}, true
	}
	return nil, false
}

func attachmentInfoFromAPI(info api.AttachmentInfo) AttachmentInfo {
	return AttachmentInfo{
		ID:              info.ID,
		Name:            info.Name,
		Size:            info.Size,
		PlaintextSHA256: strings.TrimSpace(info.PlaintextSHA256),
		ContentType:     info.ContentType,
		CreatedAtUnix:   info.CreatedAtUnix,
		UpdatedAtUnix:   info.UpdatedAtUnix,
	}
}

func attachmentInfoSliceFromAPI(items []api.AttachmentInfo) []AttachmentInfo {
	if len(items) == 0 {
		return nil
	}
	out := make([]AttachmentInfo, 0, len(items))
	for _, item := range items {
		out = append(out, attachmentInfoFromAPI(item))
	}
	return out
}

func normalizeAttachmentSelector(sel AttachmentSelector) AttachmentSelector {
	return AttachmentSelector{
		ID:   strings.TrimSpace(sel.ID),
		Name: strings.TrimSpace(sel.Name),
	}
}

func attachmentInfoFromHeaders(headers http.Header, selector AttachmentSelector) AttachmentInfo {
	info := AttachmentInfo{
		ID:              strings.TrimSpace(headers.Get(headerAttachmentID)),
		Name:            strings.TrimSpace(headers.Get(headerAttachmentName)),
		PlaintextSHA256: strings.TrimSpace(headers.Get(headerAttachmentSHA256)),
		ContentType:     strings.TrimSpace(headers.Get("Content-Type")),
	}
	if info.ID == "" {
		info.ID = selector.ID
	}
	if info.Name == "" {
		info.Name = selector.Name
	}
	if size := parseInt64Header(headers.Get(headerAttachmentSize)); size > 0 {
		info.Size = size
	} else if size := parseInt64Header(headers.Get("Content-Length")); size > 0 {
		info.Size = size
	}
	if created := parseInt64Header(headers.Get(headerAttachmentCreatedAt)); created > 0 {
		info.CreatedAtUnix = created
	}
	if updated := parseInt64Header(headers.Get(headerAttachmentUpdatedAt)); updated > 0 {
		info.UpdatedAtUnix = updated
	}
	return info
}

func parseInt64Header(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if v, err := strconv.ParseInt(value, 10, 64); err == nil {
		return v
	}
	return 0
}

func (s *LeaseSession) trackAttachment(reader io.ReadCloser) io.ReadCloser {
	if s == nil || reader == nil {
		return reader
	}
	wrapper := &trackedReadCloser{ReadCloser: reader}
	s.attachmentsMu.Lock()
	if s.attachments == nil {
		s.attachments = make(map[io.ReadCloser]struct{})
	}
	s.attachments[wrapper] = struct{}{}
	s.attachmentsMu.Unlock()
	wrapper.onClose = func() {
		s.untrackAttachment(wrapper)
	}
	return wrapper
}

func (s *LeaseSession) untrackAttachment(reader io.ReadCloser) {
	if s == nil || reader == nil {
		return
	}
	s.attachmentsMu.Lock()
	if s.attachments != nil {
		delete(s.attachments, reader)
	}
	s.attachmentsMu.Unlock()
}

func (s *LeaseSession) closeAttachments() {
	if s == nil {
		return
	}
	s.attachmentsMu.Lock()
	attachments := s.attachments
	s.attachments = nil
	s.attachmentsMu.Unlock()
	for reader := range attachments {
		_ = reader.Close()
	}
}

type trackedReadCloser struct {
	io.ReadCloser
	once    sync.Once
	onClose func()
}

func (t *trackedReadCloser) Close() error {
	if t == nil {
		return nil
	}
	var err error
	t.once.Do(func() {
		if t.onClose != nil {
			t.onClose()
		}
		if t.ReadCloser != nil {
			err = t.ReadCloser.Close()
		}
	})
	return err
}
