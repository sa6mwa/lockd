package txncoord

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
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/tcclient"
	"pkt.systems/pslog"
)

const (
	commitPath   = "/v1/txn/commit"
	rollbackPath = "/v1/txn/rollback"
)

// Config defines coordinator behavior for TC decisions and RM fan-out.
type Config struct {
	Core              *core.Service
	Logger            pslog.Logger
	DecisionRetention time.Duration

	FanoutProvider    EndpointProvider
	FanoutGate        FanoutGate
	FanoutTimeout     time.Duration
	FanoutMaxAttempts int
	FanoutBaseDelay   time.Duration
	FanoutMaxDelay    time.Duration
	FanoutMultiplier  float64

	DisableMTLS      bool
	ClientBundlePath string
	FanoutTrustPEM   [][]byte
	HTTPClient       *http.Client
}

// EndpointProvider resolves RM endpoints for a backend hash.
type EndpointProvider interface {
	Endpoints(ctx context.Context, backendHash string) ([]string, error)
}

// FanoutGate optionally pauses decisions between local apply and remote fan-out.
type FanoutGate func(ctx context.Context, rec core.TxnRecord) error

// Coordinator records decisions and fans out to RMs.
type Coordinator struct {
	core              *core.Service
	logger            pslog.Logger
	decisionRetention time.Duration
	metrics           *txncoordMetrics
	endpointProvider  EndpointProvider
	fanoutGate        FanoutGate
	timeout           time.Duration
	maxAttempts       int
	baseDelay         time.Duration
	maxDelay          time.Duration
	multiplier        float64
	httpClient        *http.Client
	clientConfig      tcclient.Config
	clientMu          sync.Mutex
}

// FanoutError reports failed fan-out endpoints.
type FanoutError struct {
	TxnID    string
	State    core.TxnState
	Failures []EndpointFailure
}

// EndpointFailure captures a failed endpoint apply attempt.
type EndpointFailure struct {
	BackendHash string
	Endpoint    string
	Err         error
}

func (e *FanoutError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return "txn fanout failed"
	}
	var b strings.Builder
	b.WriteString("txn fanout failed: ")
	for i, f := range e.Failures {
		if i > 0 {
			b.WriteString("; ")
		}
		if f.Endpoint != "" {
			b.WriteString(f.Endpoint)
		} else {
			b.WriteString("<none>")
		}
		if f.BackendHash != "" {
			b.WriteString(" (backend ")
			b.WriteString(f.BackendHash)
			b.WriteString(")")
		}
		if f.Err != nil {
			b.WriteString(": ")
			b.WriteString(f.Err.Error())
		}
	}
	return b.String()
}

// New constructs a Coordinator.
func New(cfg Config) (*Coordinator, error) {
	if cfg.Core == nil {
		return nil, errors.New("txncoord: core service required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	clientConfig := tcclient.Config{
		DisableMTLS: cfg.DisableMTLS,
		BundlePath:  cfg.ClientBundlePath,
		Timeout:     cfg.FanoutTimeout,
		TrustPEM:    cfg.FanoutTrustPEM,
	}
	return &Coordinator{
		core:              cfg.Core,
		logger:            logger,
		decisionRetention: cfg.DecisionRetention,
		metrics:           newTxncoordMetrics(logger),
		endpointProvider:  cfg.FanoutProvider,
		fanoutGate:        cfg.FanoutGate,
		timeout:           cfg.FanoutTimeout,
		maxAttempts:       cfg.FanoutMaxAttempts,
		baseDelay:         cfg.FanoutBaseDelay,
		maxDelay:          cfg.FanoutMaxDelay,
		multiplier:        cfg.FanoutMultiplier,
		httpClient:        cfg.HTTPClient,
		clientConfig:      clientConfig,
	}, nil
}

// Decide records a decision and fans out apply requests as needed.
func (c *Coordinator) Decide(ctx context.Context, rec core.TxnRecord) (core.TxnState, error) {
	if rec.TxnID == "" {
		return "", errors.New("txncoord: txn_id required")
	}
	if rec.State == "" {
		rec.State = core.TxnStatePending
	}
	decisionStart := time.Now()
	if rec.State != core.TxnStatePending && c.decisionRetention > 0 {
		now := time.Now().Unix()
		retentionExpiry := now + int64(c.decisionRetention/time.Second)
		if retentionExpiry > rec.ExpiresAtUnix {
			rec.ExpiresAtUnix = retentionExpiry
		}
	}
	decided, err := c.core.DecideTxn(ctx, rec)
	if err != nil {
		return "", err
	}
	if c.metrics != nil {
		c.metrics.recordDecide(ctx, decided.State, time.Since(decisionStart))
	}
	if c.logger != nil {
		c.logger.Info("txn.tc.decide.recorded",
			"txn_id", decided.TxnID,
			"state", decided.State,
			"participants", len(decided.Participants),
			"expires_at", decided.ExpiresAtUnix,
			"tc_term", decided.TCTerm,
			"duration_ms", time.Since(decisionStart).Milliseconds(),
		)
	}
	state := decided.State
	if state == core.TxnStatePending {
		return state, nil
	}
	applyStart := time.Now()
	if _, err := c.core.ApplyTxnDecisionRecord(ctx, decided); err != nil {
		if c.logger != nil {
			c.logger.Warn("txn.tc.apply.local.failed",
				"txn_id", decided.TxnID,
				"state", state,
				"duration_ms", time.Since(applyStart).Milliseconds(),
				"error", err,
			)
		}
		return state, err
	}
	if c.endpointProvider == nil {
		return state, nil
	}
	localHash := strings.TrimSpace(c.core.BackendHash())
	groups := groupParticipantsByBackend(decided.Participants, localHash)
	if localHash != "" {
		delete(groups, localHash)
	}
	if len(groups) == 0 {
		return state, nil
	}
	if c.fanoutGate != nil {
		if err := c.fanoutGate(ctx, *decided); err != nil {
			return state, err
		}
	}
	fanoutStart := time.Now()
	fanoutErr := c.fanout(ctx, decided.TxnID, state, decided.TCTerm, groups, decided.ExpiresAtUnix)
	if c.metrics != nil {
		result := "ok"
		if fanoutErr != nil {
			result = "error"
		}
		c.metrics.recordFanout(ctx, state, time.Since(fanoutStart), result)
	}
	if fanoutErr != nil {
		return state, fanoutErr
	}
	if c.logger != nil {
		c.logger.Info("txn.tc.fanout.complete",
			"txn_id", decided.TxnID,
			"state", state,
			"backend_groups", len(groups),
			"duration_ms", time.Since(fanoutStart).Milliseconds(),
		)
	}
	return state, nil
}

func (c *Coordinator) fanout(ctx context.Context, txnID string, state core.TxnState, term uint64, groups map[string][]core.TxnParticipant, expiresAt int64) error {
	var failures []EndpointFailure
	for backendHash, participants := range groups {
		if len(participants) == 0 {
			continue
		}
		if c.endpointProvider == nil {
			if c.metrics != nil {
				c.metrics.recordFanoutFailure(ctx, state, backendHash, "no_endpoint")
			}
			failures = append(failures, EndpointFailure{BackendHash: backendHash, Err: errors.New("fanout endpoint provider not configured")})
			continue
		}
		endpoints, err := c.endpointProvider.Endpoints(ctx, backendHash)
		if err != nil {
			if c.metrics != nil {
				c.metrics.recordFanoutFailure(ctx, state, backendHash, "endpoint_lookup_failed")
			}
			failures = append(failures, EndpointFailure{BackendHash: backendHash, Err: fmt.Errorf("resolve RM endpoints: %w", err)})
			if c.logger != nil {
				c.logger.Warn("txn.coordinator.fanout.lookup_failed", "txn_id", txnID, "state", state, "backend_hash", backendHash, "error", err)
			}
			continue
		}
		endpoints = sanitizeEndpoints(endpoints)
		if len(endpoints) == 0 {
			if c.metrics != nil {
				c.metrics.recordFanoutFailure(ctx, state, backendHash, "no_endpoint")
			}
			failures = append(failures, EndpointFailure{BackendHash: backendHash, Err: fmt.Errorf("no RM endpoints registered for backend hash")})
			continue
		}
		applied := false
		var backendFailures []EndpointFailure
		for _, endpoint := range endpoints {
			if err := c.applyWithRetry(ctx, endpoint, txnID, state, term, backendHash, participants, expiresAt); err != nil {
				var mismatch *backendMismatchError
				if errors.As(err, &mismatch) {
					continue
				}
				backendFailures = append(backendFailures, EndpointFailure{BackendHash: backendHash, Endpoint: endpoint, Err: err})
				continue
			}
			applied = true
			break
		}
		if applied {
			if err := c.core.MarkTxnParticipantsApplied(ctx, txnID, participants); err != nil {
				if c.metrics != nil {
					c.metrics.recordFanoutFailure(ctx, state, backendHash, "apply_record_failed")
				}
				if c.logger != nil {
					c.logger.Warn("txn.coordinator.fanout.mark_applied.failed", "txn_id", txnID, "state", state, "backend_hash", backendHash, "error", err)
				}
				failures = append(failures, EndpointFailure{BackendHash: backendHash, Err: err})
				continue
			}
			continue
		}
		if len(backendFailures) > 0 {
			for _, failure := range backendFailures {
				if c.metrics != nil {
					c.metrics.recordFanoutFailure(ctx, state, backendHash, "endpoint_failed")
				}
				if c.logger != nil {
					c.logger.Warn("txn.coordinator.fanout.failed", "txn_id", txnID, "state", state, "endpoint", failure.Endpoint, "backend_hash", backendHash, "error", failure.Err)
				}
			}
			failures = append(failures, backendFailures...)
			continue
		}
		if c.metrics != nil {
			c.metrics.recordFanoutFailure(ctx, state, backendHash, "no_endpoint")
		}
		failures = append(failures, EndpointFailure{BackendHash: backendHash, Err: fmt.Errorf("no matching RM endpoint for backend hash")})
	}
	if len(failures) > 0 {
		return &FanoutError{TxnID: txnID, State: state, Failures: failures}
	}
	return nil
}

func (c *Coordinator) applyWithRetry(ctx context.Context, endpoint, txnID string, state core.TxnState, term uint64, backendHash string, participants []core.TxnParticipant, expiresAt int64) error {
	attempts := c.maxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	delay := c.baseDelay
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if c.metrics != nil {
			c.metrics.recordFanoutAttempt(ctx, state, backendHash)
		}
		err := c.applyOnce(ctx, endpoint, txnID, state, term, backendHash, participants, expiresAt)
		if err == nil {
			return nil
		}
		if attempt == attempts {
			return err
		}
		if delay <= 0 {
			delay = 50 * time.Millisecond
		}
		if c.maxDelay > 0 && delay > c.maxDelay {
			delay = c.maxDelay
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if c.multiplier > 1 {
			next := time.Duration(float64(delay)*c.multiplier + 0.5)
			delay = next
		}
	}
	return fmt.Errorf("txncoord: fanout attempts exhausted")
}

func (c *Coordinator) ensureHTTPClient() (*http.Client, error) {
	if c == nil {
		return nil, errors.New("txncoord: coordinator not configured")
	}
	if c.httpClient != nil {
		return c.httpClient, nil
	}
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	if c.httpClient != nil {
		return c.httpClient, nil
	}
	client, err := tcclient.NewHTTPClient(c.clientConfig)
	if err != nil {
		return nil, err
	}
	c.httpClient = client
	return client, nil
}

func (c *Coordinator) applyOnce(ctx context.Context, endpoint, txnID string, state core.TxnState, term uint64, backendHash string, participants []core.TxnParticipant, expiresAt int64) error {
	client, err := c.ensureHTTPClient()
	if err != nil {
		return err
	}
	path := commitPath
	if state == core.TxnStateRollback {
		path = rollbackPath
	}
	url := joinEndpoint(endpoint, path)
	payload := api.TxnDecisionRequest{
		TxnID:             txnID,
		State:             string(state),
		Participants:      toAPIParticipants(participants),
		ExpiresAtUnix:     expiresAt,
		TCTerm:            term,
		TargetBackendHash: backendHash,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	timeout := c.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	var errResp api.ErrorResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&errResp); decodeErr == nil && errResp.ErrorCode != "" {
		if errResp.ErrorCode == "txn_backend_mismatch" {
			return &backendMismatchError{Endpoint: endpoint, TargetBackendHash: backendHash}
		}
		return fmt.Errorf("status %d: %s", resp.StatusCode, errResp.ErrorCode)
	}
	return fmt.Errorf("status %d", resp.StatusCode)
}

func sanitizeEndpoints(endpoints []string) []string {
	out := make([]string, 0, len(endpoints))
	for _, raw := range endpoints {
		if trimmed := strings.TrimSpace(raw); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
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

type backendMismatchError struct {
	Endpoint          string
	TargetBackendHash string
}

func (e *backendMismatchError) Error() string {
	if e == nil {
		return "backend mismatch"
	}
	if e.Endpoint == "" {
		return fmt.Sprintf("backend mismatch for %s", e.TargetBackendHash)
	}
	return fmt.Sprintf("backend mismatch for %s on %s", e.TargetBackendHash, e.Endpoint)
}

func toAPIParticipants(list []core.TxnParticipant) []api.TxnParticipant {
	if len(list) == 0 {
		return nil
	}
	out := make([]api.TxnParticipant, 0, len(list))
	for _, p := range list {
		out = append(out, api.TxnParticipant{
			Namespace:   p.Namespace,
			Key:         p.Key,
			BackendHash: p.BackendHash,
		})
	}
	return out
}

func groupParticipantsByBackend(list []core.TxnParticipant, localHash string) map[string][]core.TxnParticipant {
	if len(list) == 0 {
		return nil
	}
	localHash = strings.TrimSpace(localHash)
	out := make(map[string][]core.TxnParticipant)
	for _, p := range list {
		if p.Applied {
			continue
		}
		hash := strings.TrimSpace(p.BackendHash)
		if hash == "" {
			if localHash == "" {
				continue
			}
			hash = localHash
		}
		out[hash] = append(out[hash], p)
	}
	return out
}
