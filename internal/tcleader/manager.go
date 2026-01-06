package tcleader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/tcclient"
	"pkt.systems/pslog"
)

const (
	defaultLeaseTTL           = 15 * time.Second
	defaultLeaseRequestTTL    = 5 * time.Second
	defaultElectionBackoff    = 500 * time.Millisecond
	defaultElectionBackoffMax = 3 * time.Second
)

// DefaultLeaseTTL is the default leader lease TTL.
const DefaultLeaseTTL = defaultLeaseTTL

// LeaderInfo describes the current leader state observed by this node.
type LeaderInfo struct {
	LeaderID       string
	LeaderEndpoint string
	Term           uint64
	ExpiresAt      time.Time
	IsLeader       bool
}

// Config configures the TC leader manager.
type Config struct {
	SelfID       string
	SelfEndpoint string
	Endpoints    []string
	LeaseTTL     time.Duration
	Logger       pslog.Logger
	HTTPClient   *http.Client
	DisableMTLS  bool
	ClientBundle string
	TrustPEM     [][]byte
	Clock        clock.Clock
}

// Manager runs a quorum-based TC leader election.
type Manager struct {
	selfEndpoint string
	selfID       string
	endpoints    []string
	leaseTTL     time.Duration
	leaseStore   *LeaseStore
	httpClient   *http.Client
	clientConfig tcclient.Config
	logger       pslog.Logger
	clock        clock.Clock

	mu        sync.RWMutex
	isLeader  bool
	term      uint64
	expiresAt time.Time

	startOnce sync.Once
}

// NewManager constructs a leader manager.
func NewManager(cfg Config) (*Manager, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	endpoints := normalizeEndpoints(cfg.Endpoints)
	selfID := strings.TrimSpace(cfg.SelfID)
	clientConfig := tcclient.Config{
		DisableMTLS: cfg.DisableMTLS,
		BundlePath:  cfg.ClientBundle,
		Timeout:     defaultLeaseRequestTTL,
		TrustPEM:    cfg.TrustPEM,
	}
	if len(endpoints) == 0 {
		selfEndpoint := normalizeEndpoint(cfg.SelfEndpoint)
		if selfID == "" && selfEndpoint != "" {
			selfID = stableID(selfEndpoint)
		}
		leaseTTL := cfg.LeaseTTL
		if leaseTTL <= 0 {
			leaseTTL = defaultLeaseTTL
		}
		return &Manager{
			logger:       logger,
			leaseStore:   &LeaseStore{},
			selfEndpoint: selfEndpoint,
			selfID:       selfID,
			leaseTTL:     leaseTTL,
			clientConfig: clientConfig,
			clock:        clk,
		}, nil
	}
	selfEndpoint := normalizeEndpoint(cfg.SelfEndpoint)
	if selfEndpoint == "" {
		return nil, errors.New("tcleader: self endpoint required")
	}
	if !containsEndpoint(endpoints, selfEndpoint) {
		return nil, fmt.Errorf("tcleader: self endpoint %q missing from tc cluster membership", selfEndpoint)
	}
	if selfID == "" {
		selfID = stableID(selfEndpoint)
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = defaultLeaseTTL
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		var err error
		httpClient, err = tcclient.NewHTTPClient(clientConfig)
		if err != nil {
			return nil, err
		}
	}
	return &Manager{
		selfEndpoint: selfEndpoint,
		selfID:       selfID,
		endpoints:    endpoints,
		leaseTTL:     leaseTTL,
		leaseStore:   &LeaseStore{},
		httpClient:   httpClient,
		clientConfig: clientConfig,
		clock:        clk,
		logger:       logger,
	}, nil
}

// LeaseStore returns the local lease store.
func (m *Manager) LeaseStore() *LeaseStore {
	if m == nil {
		return nil
	}
	return m.leaseStore
}

// LeaseTTL returns the effective lease TTL.
func (m *Manager) LeaseTTL() time.Duration {
	if m == nil {
		return 0
	}
	return m.leaseTTL
}

// Now returns the current time using the manager's clock.
func (m *Manager) Now() time.Time {
	if m == nil {
		return time.Now().UTC()
	}
	return m.clockNow()
}

// Enabled reports whether leader election is configured.
func (m *Manager) Enabled() bool {
	if m == nil {
		return false
	}
	return len(m.endpointsSnapshot()) > 0
}

// SetEndpoints replaces the current election peer list.
func (m *Manager) SetEndpoints(endpoints []string) error {
	if m == nil {
		return nil
	}
	normalized := normalizeEndpoints(endpoints)
	if len(normalized) == 0 {
		prevEndpoints := m.endpointsSnapshot()
		releaseTerm, ok := m.leaderTermForRelease()
		m.mu.Lock()
		m.endpoints = nil
		m.isLeader = false
		m.expiresAt = time.Time{}
		m.mu.Unlock()
		if ok {
			targets := normalizeEndpoints(append(prevEndpoints, m.selfEndpoint))
			ctx, cancel := m.leaseRoundContext(context.Background())
			m.releaseAll(ctx, targets, releaseTerm)
			cancel()
		} else {
			_ = m.releaseLocalLease()
		}
		return nil
	}
	if m.selfEndpoint == "" {
		return errors.New("tcleader: self endpoint required")
	}
	if !containsEndpoint(normalized, m.selfEndpoint) {
		return fmt.Errorf("tcleader: self endpoint %q missing from tc cluster membership", m.selfEndpoint)
	}
	if err := m.ensureHTTPClient(); err != nil {
		return err
	}
	m.mu.Lock()
	m.endpoints = normalized
	isLeader := m.isLeader
	expiresAt := m.expiresAt
	m.mu.Unlock()
	now := m.clockNow()
	if isLeader && expiresAt.After(now) {
		return nil
	}
	if m.leaseStore != nil {
		rec := m.leaseStore.Leader()
		if rec.LeaderID == m.selfID && normalizeEndpoint(rec.LeaderEndpoint) == m.selfEndpoint && rec.ExpiresAt.After(now) {
			m.adoptLeaderLease(rec.Term, rec.ExpiresAt)
			return nil
		}
	}
	m.mu.Lock()
	m.isLeader = false
	m.expiresAt = time.Time{}
	m.mu.Unlock()
	return nil
}

func (m *Manager) leaderTermForRelease() (uint64, bool) {
	if m == nil {
		return 0, false
	}
	m.mu.RLock()
	term := m.term
	isLeader := m.isLeader
	selfID := m.selfID
	m.mu.RUnlock()
	if isLeader && term > 0 {
		return term, true
	}
	if m.leaseStore == nil {
		return 0, false
	}
	rec := m.leaseStore.Leader()
	if rec.LeaderID != "" && rec.LeaderID == selfID && rec.Term > 0 {
		return rec.Term, true
	}
	return 0, false
}

// Leader returns the current leader information.
func (m *Manager) Leader(now time.Time) LeaderInfo {
	if m == nil {
		return LeaderInfo{}
	}
	m.mu.RLock()
	isLeader := m.isLeader
	term := m.term
	expiresAt := m.expiresAt
	selfEndpoint := m.selfEndpoint
	selfID := m.selfID
	m.mu.RUnlock()
	if isLeader {
		return LeaderInfo{
			LeaderID:       selfID,
			LeaderEndpoint: selfEndpoint,
			Term:           term,
			ExpiresAt:      expiresAt,
			IsLeader:       true,
		}
	}
	if m.leaseStore == nil {
		return LeaderInfo{}
	}
	rec := m.leaseStore.Leader()
	if !rec.Observed {
		return LeaderInfo{
			Term:      rec.Term,
			ExpiresAt: rec.ExpiresAt,
		}
	}
	return LeaderInfo{
		LeaderID:       rec.LeaderID,
		LeaderEndpoint: rec.LeaderEndpoint,
		Term:           rec.Term,
		ExpiresAt:      rec.ExpiresAt,
	}
}

func (m *Manager) ensureHTTPClient() error {
	if m == nil {
		return errors.New("tcleader: http client not configured")
	}
	if m.httpClient != nil {
		return nil
	}
	client, err := tcclient.NewHTTPClient(m.clientConfig)
	if err != nil {
		return err
	}
	m.httpClient = client
	return nil
}

func (m *Manager) endpointsSnapshot() []string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.endpoints) == 0 {
		return nil
	}
	snapshot := make([]string, len(m.endpoints))
	copy(snapshot, m.endpoints)
	return snapshot
}

// Start launches the election loop.
func (m *Manager) Start(ctx context.Context) {
	if m == nil {
		return
	}
	m.startOnce.Do(func() {
		go m.run(ctx)
	})
}

func (m *Manager) run(ctx context.Context) {
	backoff := defaultElectionBackoff
	rng := rand.New(rand.NewSource(rngSeed(m.clockNow(), m.selfID)))
	staggered := false
	for {
		endpoints := m.endpointsSnapshot()
		if len(endpoints) == 0 {
			if m.isLeaderState() {
				_ = m.releaseLocalLease()
				m.stepDown()
			}
			staggered = false
			m.sleep(ctx, time.Second)
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		if m.isLeaderState() {
			if ok := m.renewLeases(ctx, endpoints); !ok {
				if m.leaseStillValid(m.clockNow()) {
					retry := m.leaseTTL / 6
					if retry <= 0 {
						retry = time.Second
					}
					m.sleep(ctx, retry)
					continue
				}
				m.releaseAll(ctx, endpoints, m.term)
				m.stepDown()
				backoff = defaultElectionBackoff
			}
			staggered = false
			wait := m.leaseTTL / 3
			if wait <= 0 {
				wait = time.Second
			}
			m.sleep(ctx, wait)
			continue
		}
		if wait := m.observedLeaderWait(m.clockNow()); wait > 0 {
			backoff = defaultElectionBackoff
			staggered = false
			m.sleep(ctx, wait)
			continue
		}
		if m.observeLeader(ctx, endpoints) {
			if wait := m.observedLeaderWait(m.clockNow()); wait > 0 {
				backoff = defaultElectionBackoff
				staggered = false
				m.sleep(ctx, wait)
				continue
			}
		}
		if !staggered {
			m.sleep(ctx, m.electionSkew(defaultElectionBackoff, endpoints))
			staggered = true
		}
		if ok := m.tryElect(ctx, endpoints, rng); ok {
			backoff = defaultElectionBackoff
			staggered = false
			continue
		}
		m.sleep(ctx, jitter(rng, backoff))
		if backoff < defaultElectionBackoffMax {
			backoff = minDuration(backoff*2, defaultElectionBackoffMax)
		}
	}
}

func (m *Manager) isLeaderState() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isLeader
}

func (m *Manager) leaseStillValid(now time.Time) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.isLeader {
		return false
	}
	if m.expiresAt.IsZero() {
		return false
	}
	return m.expiresAt.After(now)
}

func (m *Manager) observedLeaderWait(now time.Time) time.Duration {
	if m == nil || m.leaseStore == nil {
		return 0
	}
	rec := m.leaseStore.Leader()
	if rec.LeaderID == "" || rec.LeaderEndpoint == "" {
		return 0
	}
	if rec.ExpiresAt.IsZero() || !rec.ExpiresAt.After(now) {
		return 0
	}
	wait := rec.ExpiresAt.Sub(now)
	if wait <= 0 {
		return 0
	}
	maxWait := m.leaseTTL / 3
	if maxWait <= 0 {
		maxWait = time.Second
	}
	if wait > maxWait {
		wait = maxWait
	}
	return wait
}

func (m *Manager) observeLeader(ctx context.Context, endpoints []string) bool {
	if m == nil || len(endpoints) == 0 {
		return false
	}
	roundCtx, cancel := m.leaseRoundContext(ctx)
	defer cancel()
	now := m.clockNow()
	selfEndpoint := normalizeEndpoint(m.selfEndpoint)
	for _, endpoint := range endpoints {
		info, err := m.getLeader(roundCtx, endpoint)
		if err != nil {
			continue
		}
		leaderID := strings.TrimSpace(info.LeaderID)
		leaderEndpoint := normalizeEndpoint(info.LeaderEndpoint)
		if leaderEndpoint == "" || info.Term == 0 || info.ExpiresAtUnix == 0 {
			continue
		}
		expiresAt := time.UnixMilli(info.ExpiresAtUnix)
		if !expiresAt.After(now) {
			continue
		}
		if m.leaseStore != nil {
			m.leaseStore.Follow(now, leaderID, leaderEndpoint, info.Term, expiresAt)
		}
		if leaderEndpoint == selfEndpoint && (leaderID == "" || leaderID == m.selfID) {
			if m.leaseStore != nil {
				rec := m.leaseStore.Leader()
				if rec.LeaderID == m.selfID && normalizeEndpoint(rec.LeaderEndpoint) == selfEndpoint && rec.ExpiresAt.After(now) && rec.Term > 0 {
					m.adoptLeaderLease(rec.Term, rec.ExpiresAt)
				}
			}
		}
		return true
	}
	return false
}

func (m *Manager) tryElect(ctx context.Context, endpoints []string, rng *rand.Rand) bool {
	if len(endpoints) == 0 {
		return false
	}
	term := m.nextTerm(ctx, endpoints)
	quorum := quorumSize(len(endpoints))
	grants, selfGranted := m.collectLeaseVotes(ctx, endpoints, quorum, false, func(ctx context.Context, endpoint string) bool {
		ok, _ := m.acquireLease(ctx, endpoint, term)
		return ok
	})
	if selfGranted && grants >= quorum {
		m.becomeLeader(term)
		if m.logger != nil {
			m.logger.Info("tc.leader.elected",
				"leader_id", m.selfID,
				"leader_endpoint", m.selfEndpoint,
				"term", term,
				"quorum", quorum,
				"grants", grants)
		}
		return true
	}
	m.releaseAll(ctx, endpoints, term)
	if m.logger != nil {
		m.logger.Debug("tc.leader.election.failed",
			"candidate_id", m.selfID,
			"term", term,
			"grants", grants,
			"quorum", quorum)
	}
	_ = rng
	return false
}

func (m *Manager) nextTerm(ctx context.Context, endpoints []string) uint64 {
	maxTerm := uint64(0)
	for _, endpoint := range endpoints {
		term := m.fetchTerm(ctx, endpoint)
		if term > maxTerm {
			maxTerm = term
		}
	}
	if maxTerm == 0 {
		return 1
	}
	return maxTerm + 1
}

func (m *Manager) fetchTerm(ctx context.Context, endpoint string) uint64 {
	if endpoint == m.selfEndpoint {
		rec := m.leaseStore.Leader()
		return rec.Term
	}
	info, err := m.getLeader(ctx, endpoint)
	if err != nil {
		return 0
	}
	return info.Term
}

func (m *Manager) renewLeases(ctx context.Context, endpoints []string) bool {
	quorum := quorumSize(len(endpoints))
	grants, selfGranted := m.collectLeaseVotes(ctx, endpoints, quorum, false, func(ctx context.Context, endpoint string) bool {
		ok, _ := m.renewLease(ctx, endpoint, m.term)
		if ok {
			return true
		}
		ok, _ = m.acquireLease(ctx, endpoint, m.term)
		return ok
	})
	if selfGranted && grants >= quorum {
		m.mu.Lock()
		m.expiresAt = m.clockNow().Add(m.leaseTTL)
		m.mu.Unlock()
		return true
	}
	if m.logger != nil {
		m.logger.Warn("tc.leader.renew.failed",
			"leader_id", m.selfID,
			"term", m.term,
			"self_ok", selfGranted,
			"grants", grants,
			"quorum", quorum)
	}
	return false
}

func (m *Manager) collectLeaseVotes(ctx context.Context, endpoints []string, quorum int, stopOnQuorum bool, fn func(context.Context, string) bool) (int, bool) {
	if len(endpoints) == 0 {
		return 0, false
	}
	roundCtx, cancel := m.leaseRoundContext(ctx)
	defer cancel()
	type vote struct {
		endpoint string
		ok       bool
	}
	votes := make(chan vote, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint := endpoint
		go func() {
			ok := fn(roundCtx, endpoint)
			votes <- vote{endpoint: endpoint, ok: ok}
		}()
	}
	grants := 0
	selfGranted := false
	remaining := len(endpoints)
	timedOut := false
	for remaining > 0 {
		select {
		case v := <-votes:
			remaining--
			if v.ok {
				grants++
				if v.endpoint == m.selfEndpoint {
					selfGranted = true
				}
			}
			if stopOnQuorum && selfGranted && grants >= quorum {
				cancel()
				return grants, selfGranted
			}
		case <-roundCtx.Done():
			if !timedOut {
				timedOut = true
				cancel()
			}
		}
	}
	return grants, selfGranted
}

func (m *Manager) leaseRoundContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := defaultLeaseRequestTTL
	if m.leaseTTL > 0 {
		timeout = minDuration(defaultLeaseRequestTTL, (m.leaseTTL*2)/3)
		const renewSafetyMargin = 100 * time.Millisecond
		if timeout > renewSafetyMargin {
			timeout -= renewSafetyMargin
		}
	}
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	ctx, cancel := context.WithCancel(parent)
	timer := m.clockAfter(timeout)
	go func() {
		select {
		case <-parent.Done():
			cancel()
		case <-ctx.Done():
		case <-timer:
			cancel()
		}
	}()
	return ctx, cancel
}

func (m *Manager) becomeLeader(term uint64) {
	m.mu.Lock()
	m.isLeader = true
	m.term = term
	m.expiresAt = m.clockNow().Add(m.leaseTTL)
	m.mu.Unlock()
}

func (m *Manager) adoptLeaderLease(term uint64, expiresAt time.Time) {
	if m == nil {
		return
	}
	if term == 0 || expiresAt.IsZero() {
		return
	}
	if !expiresAt.After(m.clockNow()) {
		return
	}
	m.mu.Lock()
	if !m.isLeader {
		m.isLeader = true
		m.term = term
		m.expiresAt = expiresAt
	}
	m.mu.Unlock()
}

func (m *Manager) stepDown() {
	m.mu.Lock()
	wasLeader := m.isLeader
	m.isLeader = false
	m.expiresAt = time.Time{}
	m.mu.Unlock()
	if wasLeader && m.logger != nil {
		m.logger.Warn("tc.leader.stepped_down", "leader_id", m.selfID, "term", m.term)
	}
}

func (m *Manager) acquireLease(ctx context.Context, endpoint string, term uint64) (bool, *api.TCLeaseAcquireResponse) {
	req := api.TCLeaseAcquireRequest{
		CandidateID:       m.selfID,
		CandidateEndpoint: m.selfEndpoint,
		Term:              term,
		TTLMillis:         int64(m.leaseTTL / time.Millisecond),
	}
	if endpoint == m.selfEndpoint {
		rec, err := m.leaseStore.Acquire(m.clockNow(), req.CandidateID, req.CandidateEndpoint, req.Term, m.leaseTTL)
		if err != nil {
			return false, nil
		}
		resp := api.TCLeaseAcquireResponse{
			Granted:        true,
			LeaderID:       rec.LeaderID,
			LeaderEndpoint: rec.LeaderEndpoint,
			Term:           rec.Term,
			ExpiresAtUnix:  rec.ExpiresAt.UnixMilli(),
		}
		return true, &resp
	}
	resp, err := m.postAcquire(ctx, endpoint, req)
	if err != nil {
		return false, nil
	}
	return resp.Granted, resp
}

func (m *Manager) renewLease(ctx context.Context, endpoint string, term uint64) (bool, *api.TCLeaseRenewResponse) {
	req := api.TCLeaseRenewRequest{
		LeaderID:  m.selfID,
		Term:      term,
		TTLMillis: int64(m.leaseTTL / time.Millisecond),
	}
	if endpoint == m.selfEndpoint {
		rec, err := m.leaseStore.Renew(m.clockNow(), req.LeaderID, req.Term, m.leaseTTL)
		if err != nil {
			return false, nil
		}
		resp := api.TCLeaseRenewResponse{
			Renewed:        true,
			LeaderID:       rec.LeaderID,
			LeaderEndpoint: rec.LeaderEndpoint,
			Term:           rec.Term,
			ExpiresAtUnix:  rec.ExpiresAt.UnixMilli(),
		}
		return true, &resp
	}
	resp, err := m.postRenew(ctx, endpoint, req)
	if err != nil {
		return false, nil
	}
	return resp.Renewed, resp
}

func (m *Manager) releaseLease(ctx context.Context, endpoint string, term uint64) error {
	req := api.TCLeaseReleaseRequest{
		LeaderID: m.selfID,
		Term:     term,
	}
	if endpoint == m.selfEndpoint {
		_, err := m.leaseStore.Release(m.clockNow(), req.LeaderID, req.Term)
		return err
	}
	return m.postRelease(ctx, endpoint, req)
}

func (m *Manager) releaseLocalLease() error {
	if m == nil || m.leaseStore == nil {
		return nil
	}
	_, err := m.leaseStore.Release(m.clockNow(), m.selfID, m.term)
	return err
}

func (m *Manager) releaseAll(ctx context.Context, endpoints []string, term uint64) {
	if len(endpoints) == 0 {
		_ = m.releaseLocalLease()
		return
	}
	for _, endpoint := range endpoints {
		_ = m.releaseLease(ctx, endpoint, term)
	}
}

func (m *Manager) postAcquire(ctx context.Context, endpoint string, req api.TCLeaseAcquireRequest) (*api.TCLeaseAcquireResponse, error) {
	resp := &api.TCLeaseAcquireResponse{}
	if err := m.postLease(ctx, endpoint, "/v1/tc/lease/acquire", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *Manager) postRenew(ctx context.Context, endpoint string, req api.TCLeaseRenewRequest) (*api.TCLeaseRenewResponse, error) {
	resp := &api.TCLeaseRenewResponse{}
	if err := m.postLease(ctx, endpoint, "/v1/tc/lease/renew", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *Manager) postRelease(ctx context.Context, endpoint string, req api.TCLeaseReleaseRequest) error {
	return m.postLease(ctx, endpoint, "/v1/tc/lease/release", req, &api.TCLeaseReleaseResponse{})
}

func (m *Manager) postLease(ctx context.Context, endpoint, path string, payload any, out any) error {
	if err := m.ensureHTTPClient(); err != nil {
		return err
	}
	url := joinEndpoint(endpoint, path)
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		if out != nil {
			if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
				return err
			}
		}
		return nil
	}
	var errResp api.ErrorResponse
	_ = json.NewDecoder(resp.Body).Decode(&errResp)
	return fmt.Errorf("status %d: %s", resp.StatusCode, errResp.ErrorCode)
}

func (m *Manager) getLeader(ctx context.Context, endpoint string) (api.TCLeaderResponse, error) {
	if err := m.ensureHTTPClient(); err != nil {
		return api.TCLeaderResponse{}, err
	}
	url := joinEndpoint(endpoint, "/v1/tc/leader")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return api.TCLeaderResponse{}, err
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return api.TCLeaderResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return api.TCLeaderResponse{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	var out api.TCLeaderResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return api.TCLeaderResponse{}, err
	}
	return out, nil
}

func normalizeEndpoints(list []string) []string {
	seen := make(map[string]struct{}, len(list))
	out := make([]string, 0, len(list))
	for _, raw := range list {
		trimmed := normalizeEndpoint(raw)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}

func normalizeEndpoint(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	return strings.TrimSuffix(trimmed, "/")
}

func containsEndpoint(list []string, target string) bool {
	target = normalizeEndpoint(target)
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
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

func quorumSize(n int) int {
	if n <= 0 {
		return 0
	}
	return n/2 + 1
}

func (m *Manager) sleep(ctx context.Context, d time.Duration) {
	if d <= 0 || m == nil {
		return
	}
	select {
	case <-ctx.Done():
	case <-m.clockAfter(d):
	}
}

func jitter(rng *rand.Rand, base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	if rng == nil {
		return base
	}
	j := time.Duration(rng.Int63n(int64(base)))
	return base + j
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func stableID(endpoint string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(endpoint)))
	return fmt.Sprintf("tc-%x", sum[:8])
}

func (m *Manager) electionSkew(base time.Duration, endpoints []string) time.Duration {
	if base <= 0 || len(endpoints) == 0 || m == nil {
		return 0
	}
	self := normalizeEndpoint(m.selfEndpoint)
	if self == "" {
		return 0
	}
	if len(endpoints) == 1 {
		return 0
	}
	idx := -1
	for i, endpoint := range endpoints {
		if normalizeEndpoint(endpoint) == self {
			idx = i
			break
		}
	}
	if idx < 0 {
		return 0
	}
	return time.Duration(int64(base) * int64(idx) / int64(len(endpoints)))
}

func rngSeed(now time.Time, selfID string) int64 {
	seed := now.UnixNano()
	if selfID == "" {
		return seed
	}
	sum := sha256.Sum256([]byte(selfID))
	return seed ^ int64(binary.LittleEndian.Uint64(sum[:8]))
}

func (m *Manager) clockNow() time.Time {
	if m == nil || m.clock == nil {
		return time.Now().UTC()
	}
	return m.clock.Now()
}

func (m *Manager) clockAfter(d time.Duration) <-chan time.Time {
	if m == nil || m.clock == nil {
		return time.After(d)
	}
	return m.clock.After(d)
}
