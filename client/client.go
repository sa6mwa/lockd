package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"pkt.systems/lockd/internal/api"
)

// Public aliases for request/response payloads shared with the HTTP API.
type (
	AcquireRequest    = api.AcquireRequest
	AcquireResponse   = api.AcquireResponse
	KeepAliveRequest  = api.KeepAliveRequest
	KeepAliveResponse = api.KeepAliveResponse
	ReleaseRequest    = api.ReleaseRequest
	ReleaseResponse   = api.ReleaseResponse
	DescribeResponse  = api.DescribeResponse
	ErrorResponse     = api.ErrorResponse
)

const headerFencingToken = "X-Fencing-Token"

var ErrMissingFencingToken = errors.New("lockd: fencing token required")

// UpdateStateResult captures the response from UpdateState.
type UpdateStateResult struct {
	NewVersion   int64  `json:"new_version"`
	NewStateETag string `json:"new_state_etag"`
	BytesWritten int64  `json:"bytes"`
}

// UpdateStateOptions controls conditional update semantics.
type UpdateStateOptions struct {
	IfETag       string
	IfVersion    string
	FencingToken string
}

// AcquireForUpdateResult describes the metadata and stream returned by AcquireForUpdate.
type AcquireForUpdateResult struct {
	Key           string
	LeaseID       string
	Owner         string
	ExpiresAt     int64
	Version       int64
	StateETag     string
	FencingToken  string
	Body          io.ReadCloser
	ContentLength int64
}

// Close releases resources associated with the AcquireForUpdate stream.
func (r *AcquireForUpdateResult) Close() error {
	if r == nil || r.Body == nil {
		return nil
	}
	return r.Body.Close()
}

// Client is a convenience wrapper around the lockd HTTP API.
type Client struct {
	baseURL     string
	httpClient  *http.Client
	leaseTokens sync.Map
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

// New creates a new client targeting baseURL (e.g. https://localhost:9341).
// Unix-domain sockets are supported via base URLs such as unix:///var/run/lockd.sock;
// ensure the server is running with mTLS disabled or supply a compatible client bundle.
// Example:
//
//	cli, err := client.New("unix:///tmp/lockd.sock")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	lease, _ := cli.Acquire(ctx, client.AcquireRequest{Key: "demo", Owner: "worker", TTLSeconds: 20})
//	defer cli.Release(ctx, client.ReleaseRequest{Key: "demo", LeaseID: lease.LeaseID})
func New(baseURL string, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("baseURL required")
	}
	httpClient, resolvedBase, err := buildHTTPClient(baseURL)
	if err != nil {
		return nil, err
	}
	c := &Client{
		baseURL:    resolvedBase,
		httpClient: httpClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Acquire requests a lease for the given key.
// AcquireConfig controls Acquire behaviour (client-side retries, etc).
type AcquireConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
}

// AcquireOption customises Acquire behaviour.
type AcquireOption func(*AcquireConfig)

// WithAcquireMaxAttempts sets client retry attempts.
func WithAcquireMaxAttempts(n int) AcquireOption {
	return func(c *AcquireConfig) {
		if n > 0 {
			c.MaxAttempts = n
		}
	}
}

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

// Acquire acquires a lease, retrying conflicts and transient errors.
func (c *Client) Acquire(ctx context.Context, req AcquireRequest, opts ...AcquireOption) (*AcquireResponse, error) {
	cfg := AcquireConfig{
		MaxAttempts: 20,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    3 * time.Second,
		Multiplier:  2.0,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	delay := cfg.BaseDelay
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		var resp AcquireResponse
		if err := c.postJSON(ctx, "/v1/acquire", req, &resp, nil); err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) {
				if apiErr.Status == http.StatusConflict && req.BlockSecs > 0 {
					// continue to retry while blocking.
				} else if apiErr.Status >= 500 || apiErr.Status == http.StatusTooManyRequests {
					// retry on server-side/transient failures.
				} else {
					return nil, err
				}
			} else {
				return nil, err
			}
			if attempt == cfg.MaxAttempts {
				return nil, err
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
			next := time.Duration(float64(delay) * cfg.Multiplier)
			if next > cfg.MaxDelay {
				next = cfg.MaxDelay
			}
			delay = next
			continue
		}
		if resp.FencingToken != 0 {
			c.RegisterLeaseToken(resp.LeaseID, strconv.FormatInt(resp.FencingToken, 10))
		}
		return &resp, nil
	}
	return nil, fmt.Errorf("acquire retry exhausted")
}

// AcquireForUpdate atomically acquires a lease and streams the current state.
func (c *Client) AcquireForUpdate(ctx context.Context, req AcquireRequest, opts ...AcquireOption) (*AcquireForUpdateResult, error) {
	cfg := AcquireConfig{
		MaxAttempts: 20,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    3 * time.Second,
		Multiplier:  2.0,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	delay := cfg.BaseDelay
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		res, err := c.tryAcquireForUpdate(ctx, req)
		if err == nil {
			return res, nil
		}
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			if apiErr.Status == http.StatusConflict && req.BlockSecs > 0 {
				// retry while blocking
			} else if apiErr.Status >= 500 || apiErr.Status == http.StatusTooManyRequests {
				// backoff and retry
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
		if attempt == cfg.MaxAttempts {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		next := time.Duration(float64(delay) * cfg.Multiplier)
		if next > cfg.MaxDelay {
			next = cfg.MaxDelay
		}
		delay = next
	}
	return nil, fmt.Errorf("acquire-for-update retry exhausted")
}

func (c *Client) tryAcquireForUpdate(ctx context.Context, req AcquireRequest) (*AcquireForUpdateResult, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/acquire-for-update", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.Idempotency != "" {
		httpReq.Header.Set("X-Idempotency-Key", req.Idempotency)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		defer resp.Body.Close()
		return nil, c.decodeError(resp)
	}
	leaseID := resp.Header.Get("X-Lease-ID")
	if leaseID == "" {
		resp.Body.Close()
		return nil, fmt.Errorf("lockd: acquire-for-update missing X-Lease-ID header")
	}
	fencing := resp.Header.Get(headerFencingToken)
	if fencing != "" {
		c.RegisterLeaseToken(leaseID, fencing)
	}
	res := &AcquireForUpdateResult{
		Key:           req.Key,
		LeaseID:       leaseID,
		Owner:         resp.Header.Get("X-Lease-Owner"),
		StateETag:     resp.Header.Get("ETag"),
		FencingToken:  fencing,
		Body:          resp.Body,
		ContentLength: resp.ContentLength,
	}
	if expires := resp.Header.Get("X-Lease-Expires-At"); expires != "" {
		if v, parseErr := strconv.ParseInt(expires, 10, 64); parseErr == nil {
			res.ExpiresAt = v
		}
	}
	if version := resp.Header.Get("X-Key-Version"); version != "" {
		if v, parseErr := strconv.ParseInt(version, 10, 64); parseErr == nil {
			res.Version = v
		}
	}
	return res, nil
}

// AcquireForUpdateBytes reads the streamed state into memory while keeping the lease active.
func (c *Client) AcquireForUpdateBytes(ctx context.Context, req AcquireRequest, opts ...AcquireOption) ([]byte, *AcquireForUpdateResult, error) {
	res, err := c.AcquireForUpdate(ctx, req, opts...)
	if err != nil {
		return nil, nil, err
	}
	data, err := io.ReadAll(res.Body)
	if err != nil {
		res.Body.Close()
		return nil, nil, err
	}
	return data, res, nil
}

// KeepAlive extends a lease.
func (c *Client) KeepAlive(ctx context.Context, req KeepAliveRequest) (*KeepAliveResponse, error) {
	token, err := c.fencingToken(req.LeaseID, "")
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set(headerFencingToken, token)
	var resp KeepAliveResponse
	if err := c.postJSON(ctx, "/v1/keepalive", req, &resp, headers); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Release drops a lease.
func (c *Client) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResponse, error) {
	token, err := c.fencingToken(req.LeaseID, "")
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set(headerFencingToken, token)
	var resp ReleaseResponse
	if err := c.postJSON(ctx, "/v1/release", req, &resp, headers); err != nil {
		return nil, err
	}
	c.leaseTokens.Delete(req.LeaseID)
	return &resp, nil
}

// Describe fetches key metadata without state.
func (c *Client) Describe(ctx context.Context, key string) (*DescribeResponse, error) {
	url := fmt.Sprintf("%s/v1/describe?key=%s", c.baseURL, url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, c.decodeError(resp)
	}
	var describe DescribeResponse
	if err := json.NewDecoder(resp.Body).Decode(&describe); err != nil {
		return nil, err
	}
	return &describe, nil
}

// GetState streams the JSON state for a key. Caller must close the returned reader.
// When the key has no state the returned reader is nil.
func (c *Client) GetState(ctx context.Context, key, leaseID string) (io.ReadCloser, string, string, error) {
	url := fmt.Sprintf("%s/v1/get-state?key=%s", c.baseURL, url.QueryEscape(key))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, http.NoBody)
	if err != nil {
		return nil, "", "", err
	}
	req.Header.Set("X-Lease-ID", leaseID)
	token, err := c.fencingToken(leaseID, "")
	if err != nil {
		return nil, "", "", err
	}
	req.Header.Set(headerFencingToken, token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", "", err
	}
	if resp.StatusCode == http.StatusNoContent {
		resp.Body.Close()
		return nil, "", "", nil
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, "", "", c.decodeError(resp)
	}
	if newToken := resp.Header.Get(headerFencingToken); newToken != "" {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	return resp.Body, resp.Header.Get("ETag"), resp.Header.Get("X-Key-Version"), nil
}

// GetStateBytes fetches the JSON state into memory and returns it along with metadata.
func (c *Client) GetStateBytes(ctx context.Context, key, leaseID string) ([]byte, string, string, error) {
	reader, etag, version, err := c.GetState(ctx, key, leaseID)
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

// UpdateState uploads new JSON state from the provided reader.
func (c *Client) UpdateState(ctx context.Context, key, leaseID string, body io.Reader, opts UpdateStateOptions) (*UpdateStateResult, error) {
	url := fmt.Sprintf("%s/v1/update-state?key=%s", c.baseURL, url.QueryEscape(key))
	var payload io.Reader = body
	if payload == nil {
		payload = http.NoBody
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Lease-ID", leaseID)
	if opts.IfETag != "" {
		req.Header.Set("X-If-State-ETag", opts.IfETag)
	}
	if opts.IfVersion != "" {
		req.Header.Set("X-If-Version", opts.IfVersion)
	}
	token, err := c.fencingToken(leaseID, opts.FencingToken)
	if err != nil {
		return nil, err
	}
	req.Header.Set(headerFencingToken, token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, c.decodeError(resp)
	}
	var result UpdateStateResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if newToken := resp.Header.Get(headerFencingToken); newToken != "" {
		c.RegisterLeaseToken(leaseID, newToken)
	}
	return &result, nil
}

// UpdateStateBytes uploads new JSON state from the provided byte slice.
func (c *Client) UpdateStateBytes(ctx context.Context, key, leaseID string, body []byte, opts UpdateStateOptions) (*UpdateStateResult, error) {
	return c.UpdateState(ctx, key, leaseID, bytes.NewReader(body), opts)
}

// APIError describes an error response from lockd.
type APIError struct {
	Status   int
	Response ErrorResponse
	Body     []byte
}

func (e *APIError) Error() string {
	if e.Response.ErrorCode != "" {
		return fmt.Sprintf("lockd: %s (%s)", e.Response.ErrorCode, e.Response.Detail)
	}
	return fmt.Sprintf("lockd: status %d", e.Status)
}

func (c *Client) postJSON(ctx context.Context, path string, payload any, out any, headers http.Header) error {
	buf := new(bytes.Buffer)
	if payload != nil {
		if err := json.NewEncoder(buf).Encode(payload); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if headers != nil {
		for k, vals := range headers {
			for _, v := range vals {
				req.Header.Add(k, v)
			}
		}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return c.decodeError(resp)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) decodeError(resp *http.Response) error {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var errResp ErrorResponse
	if len(data) > 0 {
		if err := json.Unmarshal(data, &errResp); err != nil {
			// leave errResp empty, but keep body for diagnostics
			return &APIError{Status: resp.StatusCode, Body: data}
		}
	}
	return &APIError{Status: resp.StatusCode, Response: errResp, Body: data}
}

func buildHTTPClient(rawBase string) (*http.Client, string, error) {
	trimmed := strings.TrimRight(rawBase, "/")
	if strings.HasPrefix(rawBase, "unix://") {
		cli, base, err := newUnixHTTPClient(rawBase)
		if err != nil {
			return nil, "", err
		}
		return cli, base, nil
	}
	return &http.Client{Timeout: 15 * time.Second}, trimmed, nil
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
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 15 * time.Second}
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", socketPath)
	}
	transport.DialTLSContext = nil
	transport.TLSClientConfig = nil
	client := &http.Client{Timeout: 15 * time.Second, Transport: transport}
	base := "http://unix"
	if basePath != "" {
		if !strings.HasPrefix(basePath, "/") {
			basePath = "/" + basePath
		}
		base += basePath
	}
	return client, base, nil
}
