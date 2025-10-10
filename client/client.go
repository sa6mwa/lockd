package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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

// UpdateStateResult captures the response from UpdateState.
type UpdateStateResult struct {
	NewVersion   int64  `json:"new_version"`
	NewStateETag string `json:"new_state_etag"`
	BytesWritten int64  `json:"bytes"`
}

// UpdateStateOptions controls conditional update semantics.
type UpdateStateOptions struct {
	IfETag    string
	IfVersion string
}

// Client is a convenience wrapper around the lockd HTTP API.
type Client struct {
	baseURL    string
	httpClient *http.Client
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

// New creates a new client targeting baseURL (e.g. http://localhost:8443).
func New(baseURL string, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("baseURL required")
	}
	trimmed := strings.TrimRight(baseURL, "/")
	c := &Client{
		baseURL:    trimmed,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Acquire requests a lease for the given key.
func (c *Client) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	var resp AcquireResponse
	if err := c.postJSON(ctx, "/v1/acquire", req, &resp, nil); err != nil {
		return nil, err
	}
	return &resp, nil
}

// KeepAlive extends a lease.
func (c *Client) KeepAlive(ctx context.Context, req KeepAliveRequest) (*KeepAliveResponse, error) {
	var resp KeepAliveResponse
	if err := c.postJSON(ctx, "/v1/keepalive", req, &resp, nil); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Release drops a lease.
func (c *Client) Release(ctx context.Context, req ReleaseRequest) (*ReleaseResponse, error) {
	var resp ReleaseResponse
	if err := c.postJSON(ctx, "/v1/release", req, &resp, nil); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Describe fetches key metadata without state.
func (c *Client) Describe(ctx context.Context, key string) (*DescribeResponse, error) {
	url := fmt.Sprintf("%s/v1/describe?key=%s", c.baseURL, key)
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

// GetState streams the JSON state for a key. Returns nil body when no state exists.
func (c *Client) GetState(ctx context.Context, key, leaseID string) ([]byte, string, string, error) {
	url := fmt.Sprintf("%s/v1/get_state?key=%s", c.baseURL, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, http.NoBody)
	if err != nil {
		return nil, "", "", err
	}
	req.Header.Set("X-Lease-ID", leaseID)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil, "", "", nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", "", c.decodeError(resp)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", "", err
	}
	return data, resp.Header.Get("ETag"), resp.Header.Get("X-Key-Version"), nil
}

// UpdateState uploads new JSON state. body should contain valid JSON payload.
func (c *Client) UpdateState(ctx context.Context, key, leaseID string, body []byte, opts UpdateStateOptions) (*UpdateStateResult, error) {
	url := fmt.Sprintf("%s/v1/update_state?key=%s", c.baseURL, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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
	return &result, nil
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
