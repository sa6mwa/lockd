package tcrm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/tcclient"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

const (
	registerPath   = "/v1/tc/rm/register"
	unregisterPath = "/v1/tc/rm/unregister"
)

// ReplicatorConfig configures RM registry replication.
type ReplicatorConfig struct {
	Store            *Store
	Cluster          *tccluster.Store
	SelfEndpoint     string
	Logger           pslog.Logger
	Timeout          time.Duration
	DisableMTLS      bool
	ClientBundlePath string
	ServerBundlePath string
	ServerBundle     *tlsutil.Bundle
	TrustPEM         [][]byte
	HTTPClient       *http.Client
}

// ReplicationFailure captures a failed peer update.
type ReplicationFailure struct {
	Endpoint string
	Err      error
}

// ReplicationError reports failed replication attempts.
type ReplicationError struct {
	Operation string
	Failures  []ReplicationFailure
}

func (e *ReplicationError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return "rm replication failed"
	}
	var b strings.Builder
	if e.Operation == "" {
		b.WriteString("rm replication failed: ")
	} else {
		b.WriteString("rm replication ")
		b.WriteString(e.Operation)
		b.WriteString(" failed: ")
	}
	for i, failure := range e.Failures {
		if i > 0 {
			b.WriteString("; ")
		}
		if failure.Endpoint != "" {
			b.WriteString(failure.Endpoint)
		} else {
			b.WriteString("<unknown>")
		}
		if failure.Err != nil {
			b.WriteString(": ")
			b.WriteString(failure.Err.Error())
		}
	}
	return b.String()
}

// Replicator updates the local RM registry and replicates changes to TC peers.
type Replicator struct {
	store        *Store
	cluster      *tccluster.Store
	selfEndpoint string
	logger       pslog.Logger
	timeout      time.Duration
	httpClient   *http.Client
	clientMu     sync.Mutex
	clientCfg    tcclient.Config
}

// NewReplicator constructs a Replicator.
func NewReplicator(cfg ReplicatorConfig) (*Replicator, error) {
	if cfg.Store == nil {
		return nil, errors.New("tcrm: store required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	clientCfg := tcclient.Config{
		DisableMTLS:      cfg.DisableMTLS,
		BundlePath:       cfg.ClientBundlePath,
		ServerBundlePath: cfg.ServerBundlePath,
		ServerBundle:     cfg.ServerBundle,
		Timeout:          cfg.Timeout,
		TrustPEM:         cfg.TrustPEM,
	}
	return &Replicator{
		store:        cfg.Store,
		cluster:      cfg.Cluster,
		selfEndpoint: strings.TrimSpace(cfg.SelfEndpoint),
		logger:       logger,
		timeout:      cfg.Timeout,
		httpClient:   cfg.HTTPClient,
		clientCfg:    clientCfg,
	}, nil
}

// Register updates the local registry and replicates the update to TC peers.
func (r *Replicator) Register(ctx context.Context, backendHash, endpoint string, headers http.Header) (UpdateResult, error) {
	if err := r.preflight(ctx, "register"); err != nil {
		return UpdateResult{}, err
	}
	updated, err := r.store.Register(ctx, backendHash, endpoint)
	if err != nil {
		return UpdateResult{}, err
	}
	if err := r.replicate(ctx, registerPath, backendHash, endpoint, headers); err != nil {
		if updated.Changed {
			if _, rollbackErr := r.store.Unregister(ctx, backendHash, endpoint); rollbackErr != nil && r.logger != nil {
				r.logger.Warn("tc.rm.rollback.failed", "operation", "register", "backend_hash", backendHash, "endpoint", endpoint, "error", rollbackErr)
			}
		}
		return updated, err
	}
	return updated, nil
}

// RegisterLocal updates only the local registry (no replication).
func (r *Replicator) RegisterLocal(ctx context.Context, backendHash, endpoint string) (UpdateResult, error) {
	return r.store.Register(ctx, backendHash, endpoint)
}

// Unregister updates the local registry and replicates the update to TC peers.
func (r *Replicator) Unregister(ctx context.Context, backendHash, endpoint string, headers http.Header) (UpdateResult, error) {
	if err := r.preflight(ctx, "unregister"); err != nil {
		return UpdateResult{}, err
	}
	updated, err := r.store.Unregister(ctx, backendHash, endpoint)
	if err != nil {
		return UpdateResult{}, err
	}
	if err := r.replicate(ctx, unregisterPath, backendHash, endpoint, headers); err != nil {
		if updated.Changed {
			if _, rollbackErr := r.store.Register(ctx, backendHash, endpoint); rollbackErr != nil && r.logger != nil {
				r.logger.Warn("tc.rm.rollback.failed", "operation", "unregister", "backend_hash", backendHash, "endpoint", endpoint, "error", rollbackErr)
			}
		}
		return updated, err
	}
	return updated, nil
}

// UnregisterLocal updates only the local registry (no replication).
func (r *Replicator) UnregisterLocal(ctx context.Context, backendHash, endpoint string) (UpdateResult, error) {
	return r.store.Unregister(ctx, backendHash, endpoint)
}

func (r *Replicator) replicate(ctx context.Context, path, backendHash, endpoint string, headers http.Header) error {
	if r == nil || r.cluster == nil {
		return nil
	}
	result, err := r.cluster.Active(ctx)
	if err != nil {
		return err
	}
	endpoints := tccluster.NormalizeEndpoints(result.Endpoints)
	if len(endpoints) == 0 {
		return nil
	}
	self := strings.TrimSpace(r.selfEndpoint)
	failures := make([]ReplicationFailure, 0)
	for _, target := range endpoints {
		if self != "" && target == self {
			continue
		}
		if err := r.post(ctx, target, path, backendHash, endpoint, headers); err != nil {
			failures = append(failures, ReplicationFailure{Endpoint: target, Err: err})
			if r.logger != nil {
				r.logger.Warn("tc.rm.replicate.failed", "endpoint", target, "error", err)
			}
		}
	}
	if len(failures) > 0 {
		return &ReplicationError{Operation: replicationOp(path), Failures: failures}
	}
	return nil
}

func (r *Replicator) preflight(ctx context.Context, operation string) error {
	if r == nil || r.cluster == nil {
		return nil
	}
	result, err := r.cluster.Active(ctx)
	if err != nil {
		return err
	}
	endpoints := tccluster.NormalizeEndpoints(result.Endpoints)
	if len(endpoints) == 0 {
		return nil
	}
	self := strings.TrimSpace(r.selfEndpoint)
	client, err := r.ensureHTTPClient()
	if err != nil {
		return err
	}
	timeout := r.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	failures := make([]ReplicationFailure, 0)
	for _, target := range endpoints {
		if self != "" && target == self {
			continue
		}
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		req, reqErr := http.NewRequestWithContext(reqCtx, http.MethodGet, joinEndpoint(target, "/v1/tc/leader"), nil)
		if reqErr != nil {
			cancel()
			failures = append(failures, ReplicationFailure{Endpoint: target, Err: reqErr})
			continue
		}
		resp, err := client.Do(req)
		cancel()
		if err != nil {
			failures = append(failures, ReplicationFailure{Endpoint: target, Err: err})
			continue
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		if resp.StatusCode != http.StatusOK {
			failures = append(failures, ReplicationFailure{Endpoint: target, Err: fmt.Errorf("status %d", resp.StatusCode)})
			continue
		}
	}
	if len(failures) > 0 {
		if operation == "" {
			operation = "replication"
		}
		return &ReplicationError{Operation: operation, Failures: failures}
	}
	return nil
}

func (r *Replicator) ensureHTTPClient() (*http.Client, error) {
	if r == nil {
		return nil, errors.New("tcrm: replicator not configured")
	}
	if r.httpClient != nil {
		return r.httpClient, nil
	}
	r.clientMu.Lock()
	defer r.clientMu.Unlock()
	if r.httpClient != nil {
		return r.httpClient, nil
	}
	client, err := tcclient.NewHTTPClient(r.clientCfg)
	if err != nil {
		return nil, err
	}
	r.httpClient = client
	return client, nil
}

func (r *Replicator) post(ctx context.Context, base, path, backendHash, endpoint string, headers http.Header) error {
	client, err := r.ensureHTTPClient()
	if err != nil {
		return err
	}
	var payload any
	if path == unregisterPath {
		payload = api.TCRMUnregisterRequest{BackendHash: backendHash, Endpoint: endpoint}
	} else {
		payload = api.TCRMRegisterRequest{BackendHash: backendHash, Endpoint: endpoint}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	timeout := r.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, joinEndpoint(base, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	copyHeader(req.Header, headers)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return fmt.Errorf("status %d", resp.StatusCode)
}

func replicationOp(path string) string {
	switch path {
	case registerPath:
		return "register"
	case unregisterPath:
		return "unregister"
	default:
		return path
	}
}

func joinEndpoint(base, suffix string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		return suffix
	}
	if strings.HasSuffix(base, "/") {
		base = strings.TrimSuffix(base, "/")
	}
	if !strings.HasPrefix(suffix, "/") {
		suffix = "/" + suffix
	}
	return base + suffix
}

func copyHeader(dst, src http.Header) {
	if len(src) == 0 {
		return
	}
	for key, values := range src {
		if len(values) == 0 {
			continue
		}
		for _, val := range values {
			dst.Add(key, val)
		}
	}
}
