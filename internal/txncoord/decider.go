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
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/internal/tcleader"
	"pkt.systems/pslog"
)

// DeciderConfig configures a TC-aware decider that handles leader forwarding.
type DeciderConfig struct {
	Coordinator      *Coordinator
	Leader           *tcleader.Manager
	Logger           pslog.Logger
	ForwardTimeout   time.Duration
	DisableMTLS      bool
	ClientBundlePath string
	ForwardTrustPEM  [][]byte
	HTTPClient       *http.Client
}

// Decider implements core.TCDecider using the TC leader and coordinator.
type Decider struct {
	coordinator *Coordinator
	leader      *tcleader.Manager
	logger      pslog.Logger
	timeout     time.Duration
	httpClient  *http.Client
	clientMu    sync.Mutex
	clientCfg   tcclient.Config
}

// NewDecider builds a TC decider.
func NewDecider(cfg DeciderConfig) (*Decider, error) {
	if cfg.Coordinator == nil {
		return nil, errors.New("txncoord: coordinator required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	clientCfg := tcclient.Config{
		DisableMTLS: cfg.DisableMTLS,
		BundlePath:  cfg.ClientBundlePath,
		Timeout:     cfg.ForwardTimeout,
		TrustPEM:    cfg.ForwardTrustPEM,
	}
	return &Decider{
		coordinator: cfg.Coordinator,
		leader:      cfg.Leader,
		logger:      logger,
		timeout:     cfg.ForwardTimeout,
		httpClient:  cfg.HTTPClient,
		clientCfg:   clientCfg,
	}, nil
}

// NewErrorDecider returns a TC decider that always fails with the supplied error.
func NewErrorDecider(err error) core.TCDecider {
	return errorDecider{err: err}
}

type errorDecider struct {
	err error
}

func (d errorDecider) Enlist(_ context.Context, _ core.TxnRecord) error {
	return d.failure()
}

func (d errorDecider) Decide(_ context.Context, _ core.TxnRecord) (core.TxnState, error) {
	return "", d.failure()
}

func (d errorDecider) failure() error {
	if d.err == nil {
		return core.Failure{Code: "txn_coordinator_unavailable", Detail: "txn coordinator unavailable", HTTPStatus: http.StatusServiceUnavailable}
	}
	return core.Failure{Code: "txn_coordinator_unavailable", Detail: d.err.Error(), HTTPStatus: http.StatusServiceUnavailable}
}

// Enlist records a pending decision with the TC leader.
func (d *Decider) Enlist(ctx context.Context, rec core.TxnRecord) error {
	rec.State = core.TxnStatePending
	_, err := d.Decide(ctx, rec)
	return err
}

// Decide records a transaction decision, enforcing TC leader forwarding when enabled.
func (d *Decider) Decide(ctx context.Context, rec core.TxnRecord) (core.TxnState, error) {
	if d == nil || d.coordinator == nil {
		return "", core.Failure{Code: "txn_coordinator_unavailable", Detail: "txn coordinator unavailable", HTTPStatus: http.StatusServiceUnavailable}
	}
	rec.TxnID = strings.TrimSpace(rec.TxnID)
	if rec.TxnID == "" {
		return "", errors.New("txncoord: txn_id required")
	}
	if rec.State == "" {
		rec.State = core.TxnStatePending
	}
	switch rec.State {
	case core.TxnStatePending, core.TxnStateCommit, core.TxnStateRollback:
	default:
		return "", fmt.Errorf("txncoord: invalid state %q", rec.State)
	}

	if d.leader == nil || !d.leader.Enabled() {
		return d.coordinator.Decide(ctx, rec)
	}

	now := time.Now().UTC()
	if d.leader != nil {
		now = d.leader.Now()
	}
	info, ok := currentLeader(d.leader, now)
	if !ok {
		return "", core.Failure{Code: "tc_unavailable", Detail: "tc leader unavailable", HTTPStatus: http.StatusServiceUnavailable}
	}
	if !info.IsLeader {
		state, err := d.forwardDecide(ctx, info.LeaderEndpoint, rec)
		if err != nil {
			var failure core.Failure
			if errors.As(err, &failure) {
				return "", err
			}
			return "", core.Failure{
				Code:           "tc_not_leader",
				Detail:         "tc leader unavailable",
				HTTPStatus:     http.StatusConflict,
				LeaderEndpoint: info.LeaderEndpoint,
			}
		}
		return state, nil
	}
	rec.TCTerm = info.Term
	return d.coordinator.Decide(ctx, rec)
}

func currentLeader(manager *tcleader.Manager, now time.Time) (tcleader.LeaderInfo, bool) {
	if manager == nil || !manager.Enabled() {
		return tcleader.LeaderInfo{}, false
	}
	info := manager.Leader(now)
	if info.LeaderEndpoint == "" || info.Term == 0 {
		return info, false
	}
	if !info.ExpiresAt.IsZero() && !info.ExpiresAt.After(now) {
		return info, false
	}
	return info, true
}

func (d *Decider) ensureHTTPClient() (*http.Client, error) {
	if d == nil {
		return nil, errors.New("txncoord: decider not configured")
	}
	if d.httpClient != nil {
		return d.httpClient, nil
	}
	d.clientMu.Lock()
	defer d.clientMu.Unlock()
	if d.httpClient != nil {
		return d.httpClient, nil
	}
	client, err := tcclient.NewHTTPClient(d.clientCfg)
	if err != nil {
		return nil, err
	}
	d.httpClient = client
	return client, nil
}

func (d *Decider) forwardDecide(ctx context.Context, leaderEndpoint string, rec core.TxnRecord) (core.TxnState, error) {
	leaderEndpoint = strings.TrimSpace(leaderEndpoint)
	if leaderEndpoint == "" {
		return "", errors.New("txncoord: leader endpoint missing")
	}
	client, err := d.ensureHTTPClient()
	if err != nil {
		return "", err
	}
	payload := api.TxnDecisionRequest{
		TxnID:         rec.TxnID,
		State:         string(rec.State),
		Participants:  toAPIParticipants(rec.Participants),
		ExpiresAtUnix: rec.ExpiresAtUnix,
		TCTerm:        rec.TCTerm,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	timeout := d.timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	targetURL, err := tccluster.JoinEndpoint(leaderEndpoint, "/v1/txn/decide")
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var out api.TxnDecisionResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return "", err
		}
		state := core.TxnState(strings.ToLower(strings.TrimSpace(out.State)))
		switch state {
		case core.TxnStatePending, core.TxnStateCommit, core.TxnStateRollback:
			return state, nil
		default:
			return "", fmt.Errorf("txncoord: invalid decision state %q", out.State)
		}
	}
	var errResp api.ErrorResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&errResp); decodeErr == nil && errResp.ErrorCode != "" {
		return "", core.Failure{
			Code:           errResp.ErrorCode,
			Detail:         errResp.Detail,
			HTTPStatus:     resp.StatusCode,
			LeaderEndpoint: errResp.LeaderEndpoint,
			RetryAfter:     errResp.RetryAfterSeconds,
		}
	}
	return "", core.Failure{Code: "tc_forward_failed", Detail: fmt.Sprintf("leader status %d", resp.StatusCode), HTTPStatus: resp.StatusCode}
}
