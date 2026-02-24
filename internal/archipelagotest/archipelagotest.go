package archipelagotest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/tcclient"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
)

// HTTPClientFunc returns an HTTP client for reaching TC endpoints.
type HTTPClientFunc func(testing.TB, *lockd.TestServer) *http.Client

// RestartSpec describes a TC server restart using the original index.
type RestartSpec struct {
	Index int
	Start func(testing.TB) *lockd.TestServer
}

var (
	reservedTCPAddrsMu sync.Mutex
	reservedTCPAddrs   = make(map[string]struct{})
)

// ReserveTCPAddr returns an unused localhost address suitable for test servers.
func ReserveTCPAddr(t testing.TB) string {
	t.Helper()
	const maxAttempts = 20
	for attempt := 0; attempt < maxAttempts; attempt++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve addr: %v", err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()

		reservedTCPAddrsMu.Lock()
		if _, exists := reservedTCPAddrs[addr]; !exists {
			reservedTCPAddrs[addr] = struct{}{}
			reservedTCPAddrsMu.Unlock()
			return addr
		}
		reservedTCPAddrsMu.Unlock()
	}
	t.Fatalf("reserve addr: exhausted retries")
	return ""
}

func serverHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	if ts == nil {
		t.Fatalf("server http client: nil test server")
	}
	creds := ts.TestMTLSCredentials()
	if !creds.Valid() {
		client, err := ts.NewHTTPClient()
		if err != nil {
			t.Fatalf("server http client: %v", err)
		}
		return client
	}
	bundle, err := tlsutil.LoadBundleFromBytes(creds.ServerBundle())
	if err != nil {
		t.Fatalf("server http client: parse bundle: %v", err)
	}
	client, err := tcclient.NewHTTPClient(tcclient.Config{
		ServerBundle: bundle,
		Timeout:      5 * time.Second,
	})
	if err != nil {
		t.Fatalf("server http client: build: %v", err)
	}
	return client
}

func serverClusterIdentity(t testing.TB, ts *lockd.TestServer) (string, string) {
	t.Helper()
	if ts == nil {
		t.Fatalf("server identity: nil server")
	}
	creds := ts.TestMTLSCredentials()
	if creds.Valid() {
		bundle, err := tlsutil.LoadBundleFromBytes(creds.ServerBundle())
		if err != nil {
			t.Fatalf("server identity: parse bundle: %v", err)
		}
		identity := tccluster.IdentityFromCertificate(bundle.ServerCert)
		if identity == "" {
			t.Fatalf("server identity: missing spiffe identity for %s", ts.URL())
		}
		return identity, "spiffe"
	}
	endpoint := strings.TrimSpace(ts.URL())
	if endpoint == "" {
		t.Fatalf("server identity: missing endpoint")
	}
	return tccluster.IdentityFromEndpoint(endpoint), "endpoint"
}

func serverClusterIdentitySafe(ts *lockd.TestServer) (string, string, error) {
	if ts == nil {
		return "", "", fmt.Errorf("nil server")
	}
	creds := ts.TestMTLSCredentials()
	if creds.Valid() {
		bundle, err := tlsutil.LoadBundleFromBytes(creds.ServerBundle())
		if err != nil {
			return "", "", fmt.Errorf("parse bundle: %w", err)
		}
		identity := tccluster.IdentityFromCertificate(bundle.ServerCert)
		if identity == "" {
			return "", "", fmt.Errorf("missing spiffe identity")
		}
		return identity, "spiffe", nil
	}
	endpoint := strings.TrimSpace(ts.URL())
	if endpoint == "" {
		return "", "", fmt.Errorf("missing endpoint")
	}
	return tccluster.IdentityFromEndpoint(endpoint), "endpoint", nil
}

func requireStableServerIdentity(t testing.TB, before, after *lockd.TestServer) {
	t.Helper()
	if before == nil || after == nil {
		t.Fatalf("server identity: restart requires non-nil servers")
	}
	beforeID, beforeSource := serverClusterIdentity(t, before)
	afterID, afterSource := serverClusterIdentity(t, after)
	if beforeID == "" || afterID == "" {
		return
	}
	if beforeID != afterID {
		t.Fatalf("server identity changed on restart (%s -> %s) for %s: %s(%s) -> %s(%s)",
			before.URL(), after.URL(), before.URL(), beforeID, beforeSource, afterID, afterSource)
	}
}

func dumpClusterLeases(t testing.TB, servers []*lockd.TestServer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, ts := range servers {
		if ts == nil {
			t.Logf("cluster leases: nil server")
			continue
		}
		backend := ts.Backend()
		if backend == nil {
			t.Logf("cluster leases: %s has nil backend", ts.URL())
			continue
		}
		store := tccluster.NewStore(backend, nil, clock.Real{})
		result, err := store.Active(ctx)
		if err != nil {
			t.Logf("cluster leases: %s active error: %v", ts.URL(), err)
			continue
		}
		records := append([]tccluster.LeaseRecord(nil), result.Records...)
		sort.Slice(records, func(i, j int) bool {
			if records[i].Endpoint == records[j].Endpoint {
				return records[i].Identity < records[j].Identity
			}
			return records[i].Endpoint < records[j].Endpoint
		})
		identity, source, err := serverClusterIdentitySafe(ts)
		if err != nil {
			t.Logf("cluster leases: %s identity error: %v", ts.URL(), err)
		} else {
			t.Logf("cluster leases: %s self identity=%s (%s)", ts.URL(), identity, source)
		}
		for _, record := range records {
			t.Logf("cluster leases: %s identity=%s endpoint=%s updated=%d expires=%d",
				ts.URL(), record.Identity, record.Endpoint, record.UpdatedAtUnix, record.ExpiresAtUnix)
		}
	}
}

// JoinCluster announces membership leases for every supplied endpoint.
func JoinCluster(t testing.TB, servers []*lockd.TestServer, endpoints []string, clientFn HTTPClientFunc, timeout time.Duration) {
	t.Helper()
	if len(servers) == 0 {
		t.Fatalf("join cluster: no servers")
	}
	deadline := time.Now().Add(timeout)
	for {
		if err := joinClusterOnce(t, servers, endpoints, clientFn); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("join cluster: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// JoinClusterOnce announces membership leases for every supplied endpoint once.
func JoinClusterOnce(t testing.TB, servers []*lockd.TestServer, endpoints []string, clientFn HTTPClientFunc) error {
	t.Helper()
	if len(servers) == 0 {
		return fmt.Errorf("join cluster: no servers")
	}
	return joinClusterOnce(t, servers, endpoints, clientFn)
}

func joinClusterOnce(t testing.TB, servers []*lockd.TestServer, endpoints []string, clientFn HTTPClientFunc) error {
	targets := normalizeEndpoints(endpoints)
	if len(targets) == 0 {
		return fmt.Errorf("join cluster: endpoints required")
	}
	for _, ts := range servers {
		if ts == nil {
			return fmt.Errorf("join cluster: nil server")
		}
		self := normalizeEndpoint(ts.URL())
		if self == "" {
			return fmt.Errorf("join cluster: server endpoint required")
		}
		var client *http.Client
		if clientFn != nil {
			client = clientFn(t, ts)
		} else {
			client = serverHTTPClient(t, ts)
		}
		req := api.TCClusterAnnounceRequest{SelfEndpoint: self}
		body, err := json.Marshal(req)
		if err != nil {
			return fmt.Errorf("join cluster: marshal: %w", err)
		}
		for _, target := range targets {
			if target == "" {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/v1/tc/cluster/announce", bytes.NewReader(body))
			if err != nil {
				cancel()
				return fmt.Errorf("join cluster: request: %w", err)
			}
			httpReq.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(httpReq)
			cancel()
			if err != nil {
				return fmt.Errorf("join cluster: %w", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("join cluster: status=%d", resp.StatusCode)
			}
		}
	}
	return nil
}

// LeaveCluster deletes the supplied endpoints from membership leases.
func LeaveCluster(t testing.TB, servers []*lockd.TestServer, endpoints []string, clientFn HTTPClientFunc, timeout time.Duration) {
	t.Helper()
	if len(servers) == 0 {
		t.Fatalf("leave cluster: no servers")
	}
	if len(endpoints) == 0 {
		t.Fatalf("leave cluster: endpoints required")
	}
	deadline := time.Now().Add(timeout)
	for {
		if err := leaveClusterOnce(t, servers, endpoints, clientFn); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("leave cluster: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func leaveClusterOnce(t testing.TB, servers []*lockd.TestServer, endpoints []string, clientFn HTTPClientFunc) error {
	targets := normalizeEndpoints(endpoints)
	if len(targets) == 0 {
		return fmt.Errorf("leave cluster: endpoints required")
	}
	serverByEndpoint := make(map[string]*lockd.TestServer, len(servers))
	for _, ts := range servers {
		if ts == nil {
			return fmt.Errorf("leave cluster: nil server")
		}
		serverByEndpoint[normalizeEndpoint(ts.URL())] = ts
	}
	for _, endpoint := range targets {
		ts := serverByEndpoint[endpoint]
		if ts == nil {
			return fmt.Errorf("leave cluster: endpoint %s missing from servers", endpoint)
		}
		var client *http.Client
		if clientFn != nil {
			client = clientFn(t, ts)
		} else {
			client = serverHTTPClient(t, ts)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL()+"/v1/tc/cluster/leave", nil)
		if err != nil {
			cancel()
			return fmt.Errorf("leave cluster: request: %w", err)
		}
		resp, err := client.Do(httpReq)
		cancel()
		if err != nil {
			return fmt.Errorf("leave cluster: %w", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("leave cluster: status=%d", resp.StatusCode)
		}
	}
	return nil
}

// FetchClusterList returns the TC cluster membership list for the supplied TC.
func FetchClusterList(t testing.TB, tc *lockd.TestServer, clientFn HTTPClientFunc) api.TCClusterListResponse {
	t.Helper()
	if tc == nil {
		t.Fatalf("cluster list: tc required")
	}
	client := http.DefaultClient
	if clientFn != nil {
		client = clientFn(t, tc)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tc.URL()+"/v1/tc/cluster/list", nil)
	if err != nil {
		t.Fatalf("cluster list: request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("cluster list: request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cluster list: status=%d", resp.StatusCode)
	}
	var payload api.TCClusterListResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("cluster list: decode: %v", err)
	}
	return payload
}

// WaitForClusterMembers waits until every TC reports the expected membership list.
func WaitForClusterMembers(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc, expected []string, timeout time.Duration) {
	t.Helper()
	if len(servers) == 0 {
		t.Fatalf("cluster members: no servers")
	}
	expected = normalizeEndpoints(expected)
	if len(expected) == 0 {
		t.Fatalf("cluster members: expected endpoints required")
	}
	deadline := time.Now().Add(timeout)
	for {
		mismatch := ""
		for _, ts := range servers {
			resp := FetchClusterList(t, ts, clientFn)
			have := normalizeEndpoints(resp.Endpoints)
			if !equalStringSlices(have, expected) {
				mismatch = fmt.Sprintf("cluster list mismatch on %s (have=%v want=%v)", ts.URL(), have, expected)
				break
			}
		}
		if mismatch == "" {
			return
		}
		if time.Now().After(deadline) {
			dumpClusterLeases(t, servers)
			t.Fatalf("cluster members: %s", mismatch)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WaitForLeader waits until all supplied servers agree on a leader.
func WaitForLeader(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc, timeout time.Duration) (string, uint64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		leader, term, ok := leaderConsensus(t, servers, clientFn)
		if ok {
			return leader, term
		}
		if time.Now().After(deadline) {
			t.Fatalf("wait leader: timed out")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WaitForLeaderOn waits until a specific server observes a valid leader.
func WaitForLeaderOn(t testing.TB, ts *lockd.TestServer, clientFn HTTPClientFunc, timeout time.Duration) (string, uint64) {
	t.Helper()
	if ts == nil {
		t.Fatalf("wait leader on: server required")
	}
	deadline := time.Now().Add(timeout)
	for {
		now := time.Now().UnixMilli()
		var client *http.Client
		if clientFn != nil {
			client = clientFn(t, ts)
		} else {
			httpClient, err := ts.NewHTTPClient()
			if err != nil {
				client = nil
			}
			if httpClient != nil {
				client = httpClient
			}
		}
		if client != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/v1/tc/leader", nil)
			if err == nil {
				resp, err := client.Do(req)
				if err == nil && resp != nil {
					if resp.StatusCode == http.StatusOK {
						var payload api.TCLeaderResponse
						if err := json.NewDecoder(resp.Body).Decode(&payload); err == nil {
							_ = resp.Body.Close()
							if strings.TrimSpace(payload.LeaderEndpoint) != "" && payload.ExpiresAtUnix > now {
								cancel()
								return payload.LeaderEndpoint, payload.Term
							}
						} else {
							_ = resp.Body.Close()
						}
					} else {
						_ = resp.Body.Close()
					}
				}
			}
			cancel()
		}
		if time.Now().After(deadline) {
			t.Fatalf("wait leader on %s: timed out", ts.URL())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WaitForLeaderUnavailable waits until the TC leader endpoint reports unavailable.
func WaitForLeaderUnavailable(t testing.TB, ts *lockd.TestServer, clientFn HTTPClientFunc, timeout time.Duration) {
	t.Helper()
	if ts == nil {
		t.Fatalf("wait leader unavailable: server required")
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		var client *http.Client
		if clientFn != nil {
			client = clientFn(t, ts)
		} else {
			httpClient, err := ts.NewHTTPClient()
			if err != nil {
				lastErr = err
				client = nil
			} else {
				client = httpClient
			}
		}
		if client != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/v1/tc/leader", nil)
			if err == nil {
				resp, err := client.Do(req)
				if err == nil && resp != nil {
					body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
					resp.Body.Close()
					if resp.StatusCode == http.StatusServiceUnavailable {
						cancel()
						return
					}
					lastErr = fmt.Errorf("leader endpoint status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
				} else if err != nil {
					lastErr = err
				}
			} else {
				lastErr = err
			}
			cancel()
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				t.Fatalf("wait leader unavailable on %s: timed out: %v", ts.URL(), lastErr)
			}
			t.Fatalf("wait leader unavailable on %s: timed out", ts.URL())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func fetchLeaderResponse(t testing.TB, ts *lockd.TestServer, clientFn HTTPClientFunc) (api.TCLeaderResponse, error) {
	t.Helper()
	if ts == nil {
		return api.TCLeaderResponse{}, fmt.Errorf("leader response: nil server")
	}
	var client *http.Client
	if clientFn != nil {
		client = clientFn(t, ts)
	} else {
		httpClient, err := ts.NewHTTPClient()
		if err != nil {
			return api.TCLeaderResponse{}, err
		}
		client = httpClient
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/v1/tc/leader", nil)
	if err != nil {
		return api.TCLeaderResponse{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return api.TCLeaderResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return api.TCLeaderResponse{}, fmt.Errorf("leader status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload api.TCLeaderResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return api.TCLeaderResponse{}, err
	}
	return payload, nil
}

func stopServerForFailover(t testing.TB, ts *lockd.TestServer, timeout time.Duration, contextLabel string) {
	t.Helper()
	if ts == nil {
		t.Fatalf("scenario: stop server (%s): nil server", contextLabel)
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	drainGrace := time.Duration(float64(timeout) * 0.8)
	if drainGrace <= 0 {
		drainGrace = timeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := ts.Stop(ctx, lockd.WithDrainLeases(drainGrace), lockd.WithShutdownTimeout(timeout)); err != nil {
		t.Fatalf("scenario: stop server (%s) %s: %v", contextLabel, ts.URL(), err)
	}
}

// WaitForLeaderAll waits until every server reports the same non-expired leader.
func WaitForLeaderAll(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc, timeout time.Duration) (string, uint64) {
	t.Helper()
	leader, term, ok, lastErr := waitForLeaderAll(t, servers, clientFn, timeout)
	if ok {
		return leader, term
	}
	if lastErr != nil {
		t.Fatalf("wait leader all: timed out: %v", lastErr)
	}
	t.Fatalf("wait leader all: timed out")
	return "", 0
}

// WaitForLeaderAllChange waits until every server reports the same leader different from prev.
func WaitForLeaderAllChange(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc, prev string, timeout time.Duration) (string, uint64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	var candidate string
	var candidateTerm uint64
	stable := 0
	const stableRequired = 3
	for {
		if time.Now().After(deadline) {
			diag := summarizeLeaderStatuses(t, servers, clientFn)
			if lastErr != nil {
				t.Fatalf("wait leader all change: timed out (prev=%s): %v\n%s", prev, lastErr, diag)
			}
			t.Fatalf("wait leader all change: timed out (prev=%s)\n%s", prev, diag)
		}
		leader, term, ok := leaderConsensus(t, servers, clientFn)
		if ok && leader != "" && leader != prev {
			if leader == candidate && term == candidateTerm {
				stable++
			} else {
				candidate = leader
				candidateTerm = term
				stable = 1
			}
			if stable >= stableRequired {
				remaining := time.Until(deadline)
				confirmTimeout := 10 * time.Second
				if remaining < confirmTimeout {
					confirmTimeout = remaining
				}
				if confirmTimeout < time.Second {
					confirmTimeout = time.Second
				}
				leaderAll, termAll, okAll, err := waitForLeaderAll(t, servers, clientFn, confirmTimeout)
				if okAll && leaderAll == candidate && termAll == candidateTerm {
					return leaderAll, termAll
				}
				if err != nil {
					lastErr = err
				}
			}
		} else {
			candidate = ""
			candidateTerm = 0
			stable = 0
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func summarizeLeaderStatuses(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("leader status summary:\n")
	for _, ts := range servers {
		if ts == nil {
			continue
		}
		resp, err := fetchLeaderResponse(t, ts, clientFn)
		if err != nil {
			fmt.Fprintf(&b, "  %s: %v\n", ts.URL(), err)
			continue
		}
		fmt.Fprintf(&b, "  %s: leader=%s term=%d expires=%d\n", ts.URL(), resp.LeaderEndpoint, resp.Term, resp.ExpiresAtUnix)
	}
	return b.String()
}

func leaderExpirySlack(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc, prev string) time.Duration {
	t.Helper()
	prev = strings.TrimSpace(prev)
	if prev == "" {
		return 0
	}
	now := time.Now().UnixMilli()
	var maxRemaining int64
	for _, ts := range servers {
		if ts == nil {
			continue
		}
		resp, err := fetchLeaderResponse(t, ts, clientFn)
		if err != nil {
			continue
		}
		leader := strings.TrimSpace(resp.LeaderEndpoint)
		// After removing the old leader from membership, peers can report an
		// empty leader endpoint while the previous lease/term is still expiring.
		// Treat both "still old leader" and "temporarily no leader" as transition
		// slack so membership churn tests wait long enough on slower backends.
		if leader != prev && leader != "" {
			continue
		}
		if resp.ExpiresAtUnix <= now {
			continue
		}
		remaining := resp.ExpiresAtUnix - now
		if remaining > maxRemaining {
			maxRemaining = remaining
		}
	}
	if maxRemaining <= 0 {
		return 0
	}
	return time.Duration(maxRemaining) * time.Millisecond
}

func waitForLeaderAll(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc, timeout time.Duration) (string, uint64, bool, error) {
	t.Helper()
	if len(servers) == 0 {
		return "", 0, false, fmt.Errorf("no servers")
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		now := time.Now().UnixMilli()
		var leader string
		var term uint64
		ok := true
		for _, ts := range servers {
			if ts == nil {
				ok = false
				lastErr = fmt.Errorf("nil server")
				break
			}
			var client *http.Client
			if clientFn != nil {
				client = clientFn(t, ts)
			} else {
				httpClient, err := ts.NewHTTPClient()
				if err != nil {
					ok = false
					lastErr = fmt.Errorf("http client %s: %w", ts.URL(), err)
					break
				}
				client = httpClient
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/v1/tc/leader", nil)
			if err != nil {
				cancel()
				ok = false
				lastErr = fmt.Errorf("leader request %s: %w", ts.URL(), err)
				break
			}
			resp, err := client.Do(req)
			if err != nil || resp.StatusCode != http.StatusOK {
				status := 0
				body := ""
				if resp != nil {
					status = resp.StatusCode
					data, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
					_ = resp.Body.Close()
					body = strings.TrimSpace(string(data))
				}
				cancel()
				lastErr = fmt.Errorf("leader status %s status=%d err=%v body=%s self_endpoint=%q tc_bundle=%q mtls=%v",
					ts.URL(), status, err, body, ts.Config.SelfEndpoint, ts.Config.TCClientBundlePath, ts.Config.MTLSEnabled())
				if resp != nil {
					_ = resp.Body.Close()
				}
				ok = false
				break
			}
			var payload api.TCLeaderResponse
			if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
				_ = resp.Body.Close()
				ok = false
				cancel()
				lastErr = fmt.Errorf("leader decode %s: %w", ts.URL(), err)
				break
			}
			_ = resp.Body.Close()
			cancel()
			if strings.TrimSpace(payload.LeaderEndpoint) == "" || payload.ExpiresAtUnix <= now {
				ok = false
				lastErr = fmt.Errorf("leader missing/expired %s leader=%q term=%d expires=%d now=%d",
					ts.URL(), payload.LeaderEndpoint, payload.Term, payload.ExpiresAtUnix, now)
				break
			}
			if leader == "" {
				leader = payload.LeaderEndpoint
				term = payload.Term
				continue
			}
			if leader != payload.LeaderEndpoint || term != payload.Term {
				ok = false
				lastErr = fmt.Errorf("leader mismatch %s leader=%q term=%d expected=%q/%d",
					ts.URL(), payload.LeaderEndpoint, payload.Term, leader, term)
				break
			}
		}
		if ok && leader != "" {
			return leader, term, true, nil
		}
		if time.Now().After(deadline) {
			return "", 0, false, lastErr
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WaitForLeaderChange waits for a new leader different from prev.
func WaitForLeaderChange(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc, prev string, timeout time.Duration) (string, uint64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		leader, term, ok := leaderConsensus(t, servers, clientFn)
		if ok && leader != "" && leader != prev {
			return leader, term
		}
		if time.Now().After(deadline) {
			t.Fatalf("wait leader change: timed out (prev=%s)", prev)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func leaderConsensus(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc) (string, uint64, bool) {
	type leaderKey struct {
		endpoint string
		term     uint64
	}
	total := 0
	now := time.Now().UnixMilli()
	counts := make(map[leaderKey]int)
	for _, ts := range servers {
		if ts == nil {
			continue
		}
		total++
		var client *http.Client
		if clientFn != nil {
			client = clientFn(t, ts)
		} else {
			httpClient, err := ts.NewHTTPClient()
			if err != nil {
				continue
			}
			client = httpClient
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/v1/tc/leader", nil)
		if err != nil {
			cancel()
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			cancel()
			continue
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			cancel()
			continue
		}
		var payload api.TCLeaderResponse
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			_ = resp.Body.Close()
			cancel()
			continue
		}
		_ = resp.Body.Close()
		cancel()
		if strings.TrimSpace(payload.LeaderEndpoint) == "" || payload.ExpiresAtUnix <= now {
			continue
		}
		key := leaderKey{endpoint: payload.LeaderEndpoint, term: payload.Term}
		counts[key]++
	}
	if total == 0 {
		return "", 0, false
	}
	quorum := total/2 + 1
	var best leaderKey
	bestCount := 0
	for key, count := range counts {
		if count > bestCount {
			best = key
			bestCount = count
		}
	}
	if strings.TrimSpace(best.endpoint) == "" || bestCount == 0 {
		return "", 0, false
	}
	if bestCount >= quorum {
		return best.endpoint, best.term, true
	}
	return "", 0, false
}

// WaitForNoLeader waits until every server reports no valid leader.
func WaitForNoLeader(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if allNoLeader(t, servers, clientFn) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("wait no leader: timed out")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func allNoLeader(t testing.TB, servers []*lockd.TestServer, clientFn HTTPClientFunc) bool {
	t.Helper()
	now := time.Now().UnixMilli()
	for _, ts := range servers {
		if ts == nil {
			continue
		}
		var client *http.Client
		if clientFn != nil {
			client = clientFn(t, ts)
		} else {
			httpClient, err := ts.NewHTTPClient()
			if err != nil {
				return false
			}
			client = httpClient
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/v1/tc/leader", nil)
		if err != nil {
			cancel()
			return false
		}
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				_ = resp.Body.Close()
			}
			cancel()
			return false
		}
		var payload api.TCLeaderResponse
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			_ = resp.Body.Close()
			cancel()
			return false
		}
		_ = resp.Body.Close()
		cancel()
		if strings.TrimSpace(payload.LeaderEndpoint) != "" && payload.ExpiresAtUnix > now {
			return false
		}
	}
	return true
}

// NonLeader returns a TC endpoint that is not the leader (or nil if unavailable).
func NonLeader(servers []*lockd.TestServer, leaderEndpoint string) *lockd.TestServer {
	for _, ts := range servers {
		if ts == nil {
			continue
		}
		if ts.URL() != leaderEndpoint {
			return ts
		}
	}
	return nil
}

// ActiveNonLeader returns an active non-leader server, falling back to the leader when none respond.
func ActiveNonLeader(ctx context.Context, servers []*lockd.TestServer, leaderEndpoint string) *lockd.TestServer {
	if len(servers) == 0 {
		return nil
	}
	candidates := make([]*lockd.TestServer, 0, len(servers))
	for _, ts := range servers {
		if ts == nil || ts.URL() == leaderEndpoint {
			continue
		}
		candidates = append(candidates, ts)
	}
	if len(candidates) == 0 {
		return serverByEndpoint(servers, leaderEndpoint)
	}
	active := findActiveServer(ctx, candidates)
	if active != nil {
		return active
	}
	if leader := serverByEndpoint(servers, leaderEndpoint); leader != nil {
		if findActiveServer(ctx, []*lockd.TestServer{leader}) != nil {
			return leader
		}
	}
	return candidates[0]
}

// FanoutGate coordinates pausing fan-out after local apply.
type FanoutGate struct {
	mu      sync.Mutex
	target  string
	started chan struct{}
	release chan struct{}
	armed   bool
}

// NewFanoutGate constructs a gate for deterministic fan-out interruption.
func NewFanoutGate() *FanoutGate {
	return &FanoutGate{}
}

// Arm prepares the gate to block the next fan-out for txnID.
func (g *FanoutGate) Arm(txnID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	txnID = strings.TrimSpace(txnID)
	g.target = txnID
	g.started = make(chan struct{})
	g.release = make(chan struct{})
	g.armed = txnID != ""
}

// Hook blocks fan-out for the armed txn until released or context cancellation.
func (g *FanoutGate) Hook(ctx context.Context, rec core.TxnRecord) error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	if !g.armed || rec.TxnID != g.target {
		g.mu.Unlock()
		return nil
	}
	started := g.started
	release := g.release
	g.armed = false
	g.mu.Unlock()
	close(started)
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForStart blocks until the gate is reached or times out.
func (g *FanoutGate) WaitForStart(t testing.TB, timeout time.Duration) {
	t.Helper()
	if g == nil {
		t.Fatalf("fanout gate required")
		return
	}
	g.mu.Lock()
	started := g.started
	g.mu.Unlock()
	if started == nil {
		t.Fatalf("fanout gate not armed")
	}
	select {
	case <-started:
		return
	case <-time.After(timeout):
		t.Fatalf("fanout gate: timed out waiting for start")
	}
}

// Release unblocks any waiter in Hook.
func (g *FanoutGate) Release() {
	if g == nil {
		return
	}
	g.mu.Lock()
	release := g.release
	g.release = nil
	g.mu.Unlock()
	if release != nil {
		close(release)
	}
}

// RegisterRM registers the RM endpoint with the supplied TC endpoint.
func RegisterRM(t testing.TB, tc *lockd.TestServer, rm *lockd.TestServer, clientFn HTTPClientFunc) {
	t.Helper()
	registerRM(t, tc, rm, clientFn, nil)
}

// RegisterRMWithBackend registers the provided backend hash + endpoint on the TC.
func RegisterRMWithBackend(t testing.TB, tc *lockd.TestServer, backendHash, endpoint string, clientFn HTTPClientFunc) error {
	t.Helper()
	return postRMRegistry(t, tc, clientFn, "/v1/tc/rm/register", backendHash, endpoint, nil)
}

// UnregisterRMWithBackend unregisters the provided backend hash + endpoint on the TC.
func UnregisterRMWithBackend(t testing.TB, tc *lockd.TestServer, backendHash, endpoint string, clientFn HTTPClientFunc) error {
	t.Helper()
	return postRMRegistry(t, tc, clientFn, "/v1/tc/rm/unregister", backendHash, endpoint, nil)
}

// RegisterRMLocal registers an RM endpoint without replicating to TC peers.
func RegisterRMLocal(t testing.TB, tc *lockd.TestServer, rm *lockd.TestServer, clientFn HTTPClientFunc) {
	t.Helper()
	headers := http.Header{"X-Lockd-TC-Replicate": []string{"1"}}
	registerRM(t, tc, rm, clientFn, headers)
}

func registerRM(t testing.TB, tc *lockd.TestServer, rm *lockd.TestServer, clientFn HTTPClientFunc, headers http.Header) {
	t.Helper()
	if tc == nil || rm == nil {
		t.Fatalf("register rm: tc/rm required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backend := rm.Backend()
	if backend == nil {
		t.Fatalf("register rm: rm backend required")
	}
	hash, err := backend.BackendHash(ctx)
	if err != nil {
		t.Fatalf("register rm: backend hash: %v", err)
	}
	req := api.TCRMRegisterRequest{
		BackendHash: strings.TrimSpace(hash),
		Endpoint:    rm.URL(),
	}
	if req.BackendHash == "" {
		t.Fatalf("register rm: backend hash empty")
	}
	err = postRMRegistry(t, tc, func(tb testing.TB, _ *lockd.TestServer) *http.Client {
		tb.Helper()
		if clientFn != nil {
			return clientFn(tb, rm)
		}
		return http.DefaultClient
	}, "/v1/tc/rm/register", req.BackendHash, req.Endpoint, headers)
	if err != nil {
		t.Fatalf("register rm: %v", err)
	}
}

func postRMRegistry(t testing.TB, tc *lockd.TestServer, clientFn HTTPClientFunc, path, backendHash, endpoint string, headers http.Header) error {
	t.Helper()
	if tc == nil {
		return fmt.Errorf("rm registry: tc required")
	}
	backendHash = strings.TrimSpace(backendHash)
	endpoint = strings.TrimSpace(endpoint)
	if backendHash == "" {
		return fmt.Errorf("rm registry: backend hash required")
	}
	if endpoint == "" {
		return fmt.Errorf("rm registry: endpoint required")
	}
	var payload any
	if path == "/v1/tc/rm/unregister" {
		payload = api.TCRMUnregisterRequest{BackendHash: backendHash, Endpoint: endpoint}
	} else {
		payload = api.TCRMRegisterRequest{BackendHash: backendHash, Endpoint: endpoint}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("rm registry: marshal: %w", err)
	}
	client := http.DefaultClient
	if clientFn != nil {
		client = clientFn(t, tc)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, tc.URL()+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("rm registry: request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for key, vals := range headers {
		for _, val := range vals {
			httpReq.Header.Add(key, val)
		}
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("rm registry: request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	var errResp api.ErrorResponse
	if data, readErr := io.ReadAll(resp.Body); readErr == nil && len(data) > 0 {
		_ = json.Unmarshal(data, &errResp)
	}
	return &lockdclient.APIError{Status: resp.StatusCode, Response: errResp}
}

// FetchRMList returns the RM registry listing from the supplied TC.
func FetchRMList(t testing.TB, tc *lockd.TestServer, clientFn HTTPClientFunc) api.TCRMListResponse {
	t.Helper()
	if tc == nil {
		t.Fatalf("rm list: tc required")
	}
	client := http.DefaultClient
	if clientFn != nil {
		client = clientFn(t, tc)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tc.URL()+"/v1/tc/rm/list", nil)
	if err != nil {
		t.Fatalf("rm list: request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("rm list: request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rm list: status=%d", resp.StatusCode)
	}
	var payload api.TCRMListResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("rm list: decode: %v", err)
	}
	return payload
}

// ExpectedRMEndpoints groups RM endpoints by backend hash for the supplied servers.
func ExpectedRMEndpoints(t testing.TB, servers []*lockd.TestServer) map[string][]string {
	t.Helper()
	expected := make(map[string][]string)
	for _, ts := range servers {
		if ts == nil || ts.Backend() == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		hash, err := ts.Backend().BackendHash(ctx)
		cancel()
		if err != nil {
			t.Fatalf("rm expected: backend hash: %v", err)
		}
		hash = strings.TrimSpace(hash)
		if hash == "" {
			continue
		}
		expected[hash] = append(expected[hash], ts.URL())
	}
	for hash, endpoints := range expected {
		expected[hash] = normalizeEndpoints(endpoints)
	}
	return expected
}

// WaitForRMRegistry waits until every TC reports all expected RM endpoints.
func WaitForRMRegistry(t testing.TB, tcs []*lockd.TestServer, clientFn HTTPClientFunc, expected map[string][]string, timeout time.Duration) {
	t.Helper()
	if len(expected) == 0 {
		t.Fatalf("rm registry: expected endpoints required")
	}
	deadline := time.Now().Add(timeout)
	for {
		missing := ""
		for _, tc := range tcs {
			resp := FetchRMList(t, tc, clientFn)
			have := make(map[string][]string, len(resp.Backends))
			for _, backend := range resp.Backends {
				hash := strings.TrimSpace(backend.BackendHash)
				if hash == "" {
					continue
				}
				have[hash] = normalizeEndpoints(backend.Endpoints)
			}
			for hash, wantEndpoints := range expected {
				haveEndpoints := have[hash]
				for _, endpoint := range wantEndpoints {
					if !containsEndpoint(haveEndpoints, endpoint) {
						missing = fmt.Sprintf("missing %s on %s for backend %s", endpoint, tc.URL(), hash)
						break
					}
				}
				if missing != "" {
					break
				}
			}
			if missing != "" {
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rm registry: %s", missing)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WaitForRMRegistryEndpoint waits until every TC either sees or does not see an endpoint.
func WaitForRMRegistryEndpoint(t testing.TB, tcs []*lockd.TestServer, clientFn HTTPClientFunc, backendHash, endpoint string, expect bool, timeout time.Duration) {
	t.Helper()
	if len(tcs) == 0 {
		t.Fatalf("rm registry endpoint: tc servers required")
	}
	backendHash = strings.TrimSpace(backendHash)
	endpoint = strings.TrimSpace(endpoint)
	if backendHash == "" || endpoint == "" {
		t.Fatalf("rm registry endpoint: backend hash and endpoint required")
	}
	deadline := time.Now().Add(timeout)
	for {
		mismatch := ""
		for _, tc := range tcs {
			resp := FetchRMList(t, tc, clientFn)
			have := rmRegistryHasEndpoint(resp, backendHash, endpoint)
			if have != expect {
				mismatch = fmt.Sprintf("rm registry endpoint mismatch on %s (backend=%s endpoint=%s have=%v want=%v)",
					tc.URL(), backendHash, endpoint, have, expect)
				break
			}
		}
		if mismatch == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("rm registry endpoint: %s", mismatch)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// RequireRMRegistry asserts that all expected backend hashes are present on every TC.
func RequireRMRegistry(t testing.TB, tcs []*lockd.TestServer, clientFn HTTPClientFunc, expected []string) {
	t.Helper()
	want := make(map[string]struct{}, len(expected))
	for _, hash := range expected {
		hash = strings.TrimSpace(hash)
		if hash == "" {
			continue
		}
		want[hash] = struct{}{}
	}
	if len(want) == 0 {
		t.Fatalf("rm registry: expected hashes required")
	}
	for _, tc := range tcs {
		resp := FetchRMList(t, tc, clientFn)
		have := make(map[string]struct{})
		for _, backend := range resp.Backends {
			have[strings.TrimSpace(backend.BackendHash)] = struct{}{}
		}
		for hash := range want {
			if _, ok := have[hash]; !ok {
				t.Fatalf("rm registry: missing backend hash %q on %s", hash, tc.URL())
			}
		}
	}
}

func rmRegistryHasEndpoint(resp api.TCRMListResponse, backendHash, endpoint string) bool {
	backendHash = strings.TrimSpace(backendHash)
	endpoint = strings.TrimSpace(endpoint)
	if backendHash == "" || endpoint == "" {
		return false
	}
	for _, backend := range resp.Backends {
		if strings.TrimSpace(backend.BackendHash) != backendHash {
			continue
		}
		return containsEndpoint(backend.Endpoints, endpoint)
	}
	return false
}

// RunLeaderFailoverScenario exercises pending txns across islands, leader drop, and commit forwarding.
func RunLeaderFailoverScenario(t testing.TB, tcs []*lockd.TestServer, islandA *lockd.TestServer, islandB *lockd.TestServer, tcHTTP HTTPClientFunc, rmHTTP HTTPClientFunc, tcClient func(testing.TB, *lockd.TestServer) *lockdclient.Client, restarts map[int]RestartSpec) {
	t.Helper()
	if islandA == nil || islandB == nil {
		t.Fatalf("scenario: island servers required")
	}
	RunLeaderFailoverScenarioMulti(t, tcs, []*lockd.TestServer{islandA, islandB}, tcHTTP, rmHTTP, tcClient, restarts)
}

// RunLeaderFailoverScenarioMulti exercises pending txns across multiple islands, leader drop, and commit forwarding.
func RunLeaderFailoverScenarioMulti(t testing.TB, tcs []*lockd.TestServer, islands []*lockd.TestServer, tcHTTP HTTPClientFunc, rmHTTP HTTPClientFunc, tcClient func(testing.TB, *lockd.TestServer) *lockdclient.Client, restarts map[int]RestartSpec) {
	t.Helper()
	if len(islands) < 2 {
		t.Fatalf("scenario: at least two islands required")
	}
	if tcClient == nil {
		t.Fatalf("scenario: tc client builder required")
	}
	if rmHTTP == nil {
		t.Fatalf("scenario: rm client builder required")
	}
	leaderEndpoint, _ := WaitForLeaderAll(t, tcs, tcHTTP, 45*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	clients, hashes, keys := collectIslandInputs(ctx, t, islands, tcs, tcHTTP)
	keyByHash := make(map[string]string, len(hashes))
	clientByHash := make(map[string]*lockdclient.Client, len(hashes))
	for i, hash := range hashes {
		if strings.TrimSpace(hash) == "" {
			t.Fatalf("scenario: backend hash missing for key %q", keys[i])
		}
		if keyByHash[hash] != "" {
			t.Fatalf("scenario: duplicate backend hash %q", hash)
		}
		keyByHash[hash] = keys[i]
		clientByHash[hash] = clients[i]
	}
	const scenarioTTLSeconds = 90

	lease0, err := clients[0].Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keys[0],
		Owner:      "archipelago",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire 0: %v", err)
	}
	for i := 1; i < len(clients); i++ {
		lease, err := clients[i].Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        keys[i],
			Owner:      "archipelago",
			TTLSeconds: scenarioTTLSeconds,
			TxnID:      lease0.TxnID,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("scenario: acquire %d: %v", i, err)
		}
		if err := retryOnNodePassive(ctx, func() error {
			return lease.Save(ctx, map[string]any{"value": fmt.Sprintf("island-%d", i)})
		}); err != nil {
			t.Fatalf("scenario: save %d: %v", i, err)
		}
	}
	if err := retryOnNodePassive(ctx, func() error {
		return lease0.Save(ctx, map[string]any{"value": "island-0"})
	}); err != nil {
		t.Fatalf("scenario: save 0: %v", err)
	}

	pendingTargets := []int{1}
	if len(clients) == 2 {
		pendingTargets = []int{0, 1}
	} else if len(clients) >= 3 {
		pendingTargets = []int{1, 2}
	}
	type pendingLeaseInfo struct {
		namespace    string
		key          string
		leaseID      string
		txnID        string
		backendHash  string
		fencingToken int64
	}
	pendingLeases := make([]pendingLeaseInfo, 0, len(pendingTargets))
	for _, target := range pendingTargets {
		pendingKey := fmt.Sprintf("archi-pending-%d-%d", target, time.Now().UnixNano())
		lease, err := clients[target].Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        pendingKey,
			Owner:      "archipelago-pending",
			TTLSeconds: scenarioTTLSeconds,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("scenario: acquire pending %d: %v", target, err)
		}
		if err := retryOnNodePassive(ctx, func() error {
			return lease.Save(ctx, map[string]any{"value": "pending"})
		}); err != nil {
			t.Fatalf("scenario: save pending %d: %v", target, err)
		}
		pendingLeases = append(pendingLeases, pendingLeaseInfo{
			namespace:    lease.Namespace,
			key:          lease.Key,
			leaseID:      lease.LeaseID,
			txnID:        lease.TxnID,
			backendHash:  hashes[target],
			fencingToken: lease.FencingToken,
		})
	}

	stopServerForFailover(t, leaderTS, 10*time.Second, "leader_failover")

	var alive []*lockd.TestServer
	for _, tc := range tcs {
		if tc != nil && tc != leaderTS {
			alive = append(alive, tc)
		}
	}
	newLeader, _ := WaitForLeaderChange(t, alive, tcHTTP, leaderEndpoint, 45*time.Second)

	aliveByHash := make(map[string]*lockd.TestServer, len(hashes))
	for _, hash := range hashes {
		aliveIsland := serverForHash(alive, hash)
		if aliveIsland == nil {
			t.Fatalf("scenario: missing live server for backend %q", hash)
		}
		aliveByHash[hash] = aliveIsland
	}

	for _, tc := range alive {
		for _, rm := range alive {
			RegisterRMLocal(t, tc, rm, rmHTTP)
		}
	}
	expectedRMs := ExpectedRMEndpoints(t, alive)
	WaitForRMRegistry(t, alive, tcHTTP, expectedRMs, 15*time.Second)

	newKey := fmt.Sprintf("archi-new-%d", time.Now().UnixNano())
	_, err = clientByHash[hashes[len(hashes)-1]].Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        newKey,
		Owner:      "archipelago-new",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire new txn: %v", err)
	}

	pickCtx, pickCancel := context.WithTimeout(context.Background(), 5*time.Second)
	nonLeader := ActiveNonLeader(pickCtx, alive, newLeader)
	pickCancel()
	if nonLeader == nil {
		t.Fatalf("scenario: non-leader not found")
	}
	observedLeader, _ := WaitForLeaderOn(t, nonLeader, tcHTTP, 20*time.Second)
	if strings.TrimSpace(observedLeader) != "" && observedLeader != newLeader {
		newLeader = observedLeader
	}
	tcCli := tcClient(t, nonLeader)

	participants := make([]api.TxnParticipant, 0, len(keys))
	for i, key := range keys {
		participants = append(participants, api.TxnParticipant{
			Namespace:   namespaces.Default,
			Key:         key,
			BackendHash: hashes[i],
		})
	}
	req := api.TxnDecisionRequest{
		TxnID:        lease0.TxnID,
		Participants: participants,
	}
	if _, err := tcCli.TxnCommit(ctx, req); err != nil {
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "node_passive" {
			leaderTS := serverByEndpoint(alive, newLeader)
			if leaderTS == nil {
				leaderTS = serverByEndpoint(tcs, newLeader)
			}
			if leaderTS == nil {
				t.Fatalf("scenario: leader %q not found for commit retry", newLeader)
			}
			if _, retryErr := tcClient(t, leaderTS).TxnCommit(ctx, req); retryErr != nil {
				t.Fatalf("scenario: txn commit retry: %v", retryErr)
			}
		} else {
			t.Fatalf("scenario: txn commit: %v", err)
		}
	}

	for _, hash := range hashes {
		key := keyByHash[hash]
		if key == "" {
			t.Fatalf("scenario: missing key for backend %q", hash)
		}
		candidates := serversForBackendHash(alive, hash)
		if len(candidates) == 0 {
			candidates = serversForBackendHash(tcs, hash)
		}
		if len(candidates) == 0 {
			t.Fatalf("scenario: missing state verification servers for backend %q", hash)
		}
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 45*time.Second)
		activeServer := findActiveServer(waitCtx, candidates)
		waitCancel()
		if activeServer == nil {
			t.Fatalf("scenario: no active server for backend %q while verifying committed state", hash)
		}
		WaitForState(t, ensureClient(t, activeServer), key, true, 60*time.Second)
	}
	if len(pendingLeases) > 0 {
		for _, pending := range pendingLeases {
			target := serverForHash(alive, pending.backendHash)
			if target == nil {
				target = serverForHash(tcs, pending.backendHash)
			}
			if target == nil {
				t.Fatalf("scenario: pending release target missing for backend %q", pending.backendHash)
			}
			cli := ensureClient(t, target)
			if pending.fencingToken > 0 {
				cli.RegisterLeaseToken(pending.leaseID, pending.fencingToken)
			}
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := cli.Release(releaseCtx, api.ReleaseRequest{
				Namespace: pending.namespace,
				Key:       pending.key,
				LeaseID:   pending.leaseID,
				TxnID:     pending.txnID,
				Rollback:  true,
			})
			releaseCancel()
			if err != nil {
				t.Fatalf("scenario: release pending lease %s: %v", pending.key, err)
			}
		}
	}
	if restarts != nil {
		leaderIndex := indexOfServer(tcs, leaderTS)
		if leaderIndex >= 0 {
			if spec, ok := restarts[leaderIndex]; ok && spec.Start != nil {
				restarted := spec.Start(t)
				requireStableServerIdentity(t, leaderTS, restarted)
				tcs[leaderIndex] = restarted
				for i, island := range islands {
					if island == leaderTS {
						islands[i] = restarted
						break
					}
				}
				WaitForLeaderAll(t, tcs, tcHTTP, 45*time.Second)
			}
		}
	}
}

// RunNonLeaderForwardUnavailableScenario verifies non-leader forwarding and leader-unavailable errors.
func RunNonLeaderForwardUnavailableScenario(t testing.TB, tcs []*lockd.TestServer, islands []*lockd.TestServer, tcHTTP HTTPClientFunc, tcClient func(testing.TB, *lockd.TestServer) *lockdclient.Client) {
	t.Helper()
	if len(tcs) < 2 {
		t.Fatalf("scenario: at least two TC servers required")
	}
	if len(islands) == 0 {
		t.Fatalf("scenario: at least one island required")
	}
	if tcClient == nil {
		t.Fatalf("scenario: tc client builder required")
	}

	leaderEndpoint, _ := WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}
	activeCtx, activeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer activeCancel()
	for findActiveServer(activeCtx, []*lockd.TestServer{leaderTS}) == nil {
		if activeCtx.Err() != nil {
			t.Fatalf("scenario: leader %q not active before forward", leaderEndpoint)
		}
		time.Sleep(200 * time.Millisecond)
		leaderEndpoint, _ = WaitForLeaderAll(t, tcs, tcHTTP, 5*time.Second)
		leaderTS = serverByEndpoint(tcs, leaderEndpoint)
		if leaderTS == nil {
			t.Fatalf("scenario: leader %q not found after refresh", leaderEndpoint)
		}
	}
	pickCtx, pickCancel := context.WithTimeout(context.Background(), 5*time.Second)
	nonLeader := ActiveNonLeader(pickCtx, tcs, leaderEndpoint)
	pickCancel()
	if nonLeader == nil {
		t.Fatalf("scenario: non-leader not found")
	}
	WaitForLeader(t, []*lockd.TestServer{nonLeader}, tcHTTP, 10*time.Second)
	const minLeaseRemaining = 3 * time.Second
	leaseDeadline := time.Now().Add(10 * time.Second)
	for {
		payload, err := fetchLeaderResponse(t, nonLeader, tcHTTP)
		if err == nil {
			now := time.Now().UnixMilli()
			if strings.TrimSpace(payload.LeaderEndpoint) != "" && payload.ExpiresAtUnix > now {
				if payload.LeaderEndpoint != leaderEndpoint {
					leaderEndpoint = payload.LeaderEndpoint
					leaderTS = serverByEndpoint(tcs, leaderEndpoint)
					if leaderTS == nil {
						t.Fatalf("scenario: leader %q not found after lease refresh", leaderEndpoint)
					}
					pickCtx, pickCancel := context.WithTimeout(context.Background(), 5*time.Second)
					nonLeader = ActiveNonLeader(pickCtx, tcs, leaderEndpoint)
					pickCancel()
					if nonLeader == nil {
						t.Fatalf("scenario: non-leader not found after lease refresh")
					}
					WaitForLeader(t, []*lockd.TestServer{nonLeader}, tcHTTP, 10*time.Second)
					leaseDeadline = time.Now().Add(10 * time.Second)
					continue
				}
				remaining := time.Duration(payload.ExpiresAtUnix-now) * time.Millisecond
				if remaining >= minLeaseRemaining {
					break
				}
			}
		}
		if time.Now().After(leaseDeadline) {
			t.Fatalf("scenario: leader lease not stable before stop (leader=%q)", leaderEndpoint)
		}
		time.Sleep(100 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	island := nonLeader
	if island == nil || island.Backend() == nil {
		t.Fatalf("scenario: island backend required")
	}
	hash, err := island.Backend().BackendHash(ctx)
	if err != nil {
		t.Fatalf("scenario: backend hash: %v", err)
	}
	hash = strings.TrimSpace(hash)
	if hash == "" {
		t.Fatalf("scenario: backend hash empty")
	}
	forwardCLI := ensureClient(t, island)
	forwardServers := serversForBackendHash(tcs, hash)
	if len(forwardServers) == 0 {
		forwardServers = tcs
	}
	forwardEndpoints := endpointsForServers(forwardServers)
	activeEndpoint := ""
	if active := findActiveServer(ctx, forwardServers); active != nil {
		activeEndpoint = strings.TrimSpace(active.URL())
	}
	if island != nil {
		islandEndpoint := strings.TrimSpace(island.URL())
		if islandEndpoint != "" {
			ordered := make([]string, 0, len(forwardEndpoints)+2)
			if activeEndpoint != "" {
				ordered = append(ordered, activeEndpoint)
			}
			if islandEndpoint != "" && islandEndpoint != activeEndpoint {
				ordered = append(ordered, islandEndpoint)
			}
			for _, endpoint := range forwardEndpoints {
				if endpoint == islandEndpoint || endpoint == activeEndpoint {
					continue
				}
				ordered = append(ordered, endpoint)
			}
			forwardEndpoints = ordered
		}
	}
	if len(forwardEndpoints) > 0 {
		cli, err := island.NewEndpointsClient(forwardEndpoints, lockdclient.WithEndpointShuffle(false))
		if err != nil {
			t.Fatalf("scenario: forward client: %v", err)
		}
		forwardCLI = cli
	}

	key := fmt.Sprintf("archi-forward-%d", time.Now().UnixNano())
	lease, err := forwardCLI.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "archipelago-forward",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire forward: %v", err)
	}
	if err := saveWithRetry(t, lease, map[string]any{"value": "forward"}, 20*time.Second); err != nil {
		t.Fatalf("scenario: save forward: %v", err)
	}

	if _, err := tcClient(t, nonLeader).TxnCommit(ctx, api.TxnDecisionRequest{
		TxnID: lease.TxnID,
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: key, BackendHash: hash},
		},
	}); err != nil {
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "node_passive" {
			if _, err := tcClient(t, leaderTS).TxnCommit(ctx, api.TxnDecisionRequest{
				TxnID: lease.TxnID,
				Participants: []api.TxnParticipant{
					{Namespace: namespaces.Default, Key: key, BackendHash: hash},
				},
			}); err != nil {
				t.Fatalf("scenario: non-leader forward commit via leader: %v", err)
			}
		} else {
			t.Fatalf("scenario: non-leader forward commit: %v", err)
		}
	}
	WaitForState(t, forwardCLI, key, true, 20*time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = leaderTS.Abort(stopCtx)
	stopCancel()

	observedLeader, _ := WaitForLeader(t, []*lockd.TestServer{nonLeader}, tcHTTP, 2*time.Second)
	if observedLeader != leaderEndpoint {
		t.Fatalf("scenario: expected non-leader to observe %q, got %q", leaderEndpoint, observedLeader)
	}

	failCtx, failCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer failCancel()
	_, err = tcClient(t, nonLeader).TxnCommit(failCtx, api.TxnDecisionRequest{
		TxnID: xid.New().String(),
	})
	if err == nil {
		t.Fatalf("scenario: expected tc_not_leader when leader is unreachable")
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("scenario: expected api error, got %v", err)
	}
	if apiErr.Response.ErrorCode != "tc_not_leader" {
		t.Fatalf("scenario: expected tc_not_leader, got %s", apiErr.Response.ErrorCode)
	}
	if apiErr.Response.LeaderEndpoint != leaderEndpoint {
		t.Fatalf("scenario: expected leader endpoint %q, got %q", leaderEndpoint, apiErr.Response.LeaderEndpoint)
	}
}

// RunRMRegistryReplicationScenario verifies replication failures do not partially update registries.
func RunRMRegistryReplicationScenario(t testing.TB, tcs []*lockd.TestServer, tcHTTP HTTPClientFunc, serverHTTP HTTPClientFunc, restarts map[int]RestartSpec) {
	t.Helper()
	if len(tcs) < 2 {
		t.Fatalf("scenario: at least two TC servers required")
	}
	if serverHTTP == nil {
		t.Fatalf("scenario: server http client required")
	}
	allEndpoints := endpointsForServers(tcs)
	JoinCluster(t, tcs, allEndpoints, tcHTTP, 30*time.Second)
	WaitForClusterMembers(t, tcs, tcHTTP, allEndpoints, 20*time.Second)
	leaderEndpoint, _ := WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}
	down := NonLeader(tcs, leaderEndpoint)
	if down == nil {
		t.Fatalf("scenario: non-leader not found for replication failure")
	}
	downIndex := indexOfServer(tcs, down)
	if downIndex < 0 {
		t.Fatalf("scenario: down server index not found")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = down.Abort(stopCtx)
	stopCancel()

	active := leaderTS
	if active == nil || active == down {
		active = tcs[0]
	}
	if active == nil {
		t.Fatalf("scenario: active tc required")
	}
	WaitForLeaderOn(t, active, tcHTTP, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	hash, err := active.Backend().BackendHash(ctx)
	cancel()
	if err != nil {
		t.Fatalf("scenario: backend hash: %v", err)
	}
	hash = strings.TrimSpace(hash)
	if hash == "" {
		t.Fatalf("scenario: backend hash empty")
	}
	targetEndpoint := strings.TrimSpace(active.URL())
	if targetEndpoint == "" {
		t.Fatalf("scenario: endpoint empty")
	}
	fakeEndpoint := fmt.Sprintf("%s/fake-%s", strings.TrimRight(targetEndpoint, "/"), xid.New().String())

	err = UnregisterRMWithBackend(t, active, hash, targetEndpoint, serverHTTP)
	if err == nil {
		t.Fatalf("scenario: expected unregister replication error")
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("scenario: expected api error for unregister, got %v", err)
	}
	if apiErr.Response.ErrorCode != "tc_rm_replication_failed" {
		t.Fatalf("scenario: expected tc_rm_replication_failed, got %s", apiErr.Response.ErrorCode)
	}

	alive := filterServersByIndex(tcs, map[int]struct{}{downIndex: {}})
	WaitForRMRegistryEndpoint(t, alive, tcHTTP, hash, targetEndpoint, true, 10*time.Second)

	err = RegisterRMWithBackend(t, active, hash, fakeEndpoint, serverHTTP)
	if err == nil {
		t.Fatalf("scenario: expected register replication error")
	}
	if !errors.As(err, &apiErr) {
		t.Fatalf("scenario: expected api error for register, got %v", err)
	}
	if apiErr.Response.ErrorCode != "tc_rm_replication_failed" {
		t.Fatalf("scenario: expected tc_rm_replication_failed, got %s", apiErr.Response.ErrorCode)
	}
	WaitForRMRegistryEndpoint(t, alive, tcHTTP, hash, fakeEndpoint, false, 10*time.Second)

	if restarts == nil {
		t.Fatalf("scenario: restart map required")
	}
	spec, ok := restarts[downIndex]
	if !ok || spec.Start == nil {
		t.Fatalf("scenario: restart spec missing for downed peer")
	}
	restarted := spec.Start(t)
	requireStableServerIdentity(t, down, restarted)
	tcs[downIndex] = restarted
	JoinCluster(t, tcs, endpointsForServers(tcs), tcHTTP, 30*time.Second)
	leaderEndpoint, _ = WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
	leaderTS = serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found after restart", leaderEndpoint)
	}
	for _, rm := range tcs {
		if rm == nil {
			continue
		}
		RegisterRM(t, leaderTS, rm, serverHTTP)
	}
	baseline := ExpectedRMEndpoints(t, tcs)
	WaitForRMRegistry(t, tcs, tcHTTP, baseline, 20*time.Second)

	if err := RegisterRMWithBackend(t, active, hash, fakeEndpoint, serverHTTP); err != nil {
		t.Fatalf("scenario: register after restart: %v", err)
	}
	expected := baseline
	expected[hash] = normalizeEndpoints(append(expected[hash], fakeEndpoint))
	WaitForRMRegistry(t, tcs, tcHTTP, expected, 20*time.Second)

	if err := UnregisterRMWithBackend(t, active, hash, fakeEndpoint, serverHTTP); err != nil {
		t.Fatalf("scenario: unregister after restart: %v", err)
	}
	WaitForRMRegistryEndpoint(t, tcs, tcHTTP, hash, fakeEndpoint, false, 20*time.Second)
}

// RunRMApplyTermFencingScenario validates term fencing and idempotent apply on RM endpoints.
func RunRMApplyTermFencingScenario(t testing.TB, tcs []*lockd.TestServer, islands []*lockd.TestServer, tcHTTP HTTPClientFunc, restarts map[int]RestartSpec) {
	t.Helper()
	if len(tcs) == 0 || len(islands) == 0 {
		t.Fatalf("scenario: tc servers and islands required")
	}
	leaderEndpoint, term := WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}

	if term < 2 {
		stopServerForFailover(t, leaderTS, 10*time.Second, "rm_apply_term_fencing")
		alive := filterServersByIndex(tcs, map[int]struct{}{indexOfServer(tcs, leaderTS): {}})
		WaitForLeaderChange(t, alive, tcHTTP, leaderEndpoint, 30*time.Second)
		if restarts != nil {
			idx := indexOfServer(tcs, leaderTS)
			if idx >= 0 {
				if spec, ok := restarts[idx]; ok && spec.Start != nil {
					restarted := spec.Start(t)
					requireStableServerIdentity(t, leaderTS, restarted)
					tcs[idx] = restarted
				}
			}
		}
		JoinCluster(t, tcs, endpointsForServers(tcs), tcHTTP, 30*time.Second)
		leaderEndpoint, term = WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
		leaderTS = serverByEndpoint(tcs, leaderEndpoint)
		if leaderTS == nil {
			t.Fatalf("scenario: leader not found after restart")
		}
	}

	target := islands[0]
	if target == nil {
		t.Fatalf("scenario: target island required")
	}
	if leaderTS != nil && target == leaderTS {
		for _, island := range islands {
			if island != nil && island != leaderTS {
				target = island
				break
			}
		}
	}
	if target == nil {
		t.Fatalf("scenario: target island unavailable")
	}
	WaitForLeaderOn(t, target, tcHTTP, 20*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hash, err := target.Backend().BackendHash(ctx)
	if err != nil {
		t.Fatalf("scenario: backend hash: %v", err)
	}
	hash = strings.TrimSpace(hash)
	if hash == "" {
		t.Fatalf("scenario: backend hash empty")
	}
	if candidates := serversForBackendHash(tcs, hash); len(candidates) > 0 {
		if active := findActiveServer(ctx, candidates); active != nil {
			target = active
		}
	}
	WaitForLeaderOn(t, target, tcHTTP, 20*time.Second)
	key := fmt.Sprintf("archi-rm-term-%d", time.Now().UnixNano())
	lease, err := ensureClient(t, target).Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        key,
		Owner:      "archipelago-rm-term",
		TTLSeconds: 60,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire: %v", err)
	}
	// Avoid staging state here; Update enlists participants via the TC decider and
	// can fail with tc_not_leader during leader churn. We only need a lease + txn
	// id to exercise RM apply term fencing.

	participants := []api.TxnParticipant{
		{Namespace: namespaces.Default, Key: key, BackendHash: hash},
	}
	if err := postTxnCommitWithParticipants(t, target, tcHTTP, lease.TxnID, term, participants); err != nil {
		t.Fatalf("scenario: commit apply: %v", err)
	}
	if err := postTxnCommitWithParticipants(t, target, tcHTTP, lease.TxnID, term, participants); err != nil {
		t.Fatalf("scenario: commit apply idempotent: %v", err)
	}
	err = postTxnRollbackWithParticipants(t, target, tcHTTP, lease.TxnID, term, participants)
	if err == nil {
		t.Fatalf("scenario: expected txn_conflict on rollback with same term")
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("scenario: expected api error, got %v", err)
	}
	if apiErr.Response.ErrorCode != "txn_conflict" {
		t.Fatalf("scenario: expected txn_conflict, got %s", apiErr.Response.ErrorCode)
	}
	if term > 1 {
		err = postTxnCommitWithParticipants(t, target, tcHTTP, lease.TxnID, term-1, participants)
		if err == nil {
			t.Fatalf("scenario: expected tc_term_stale for stale term")
		}
		if !errors.As(err, &apiErr) {
			t.Fatalf("scenario: expected api error, got %v", err)
		}
		if apiErr.Response.ErrorCode != "tc_term_stale" {
			t.Fatalf("scenario: expected tc_term_stale, got %s", apiErr.Response.ErrorCode)
		}
	}
}

// RunTCMembershipChurnScenario verifies membership updates and leader eligibility.
func RunTCMembershipChurnScenario(t testing.TB, tcs []*lockd.TestServer, tcHTTP HTTPClientFunc) {
	t.Helper()
	if len(tcs) < 2 {
		t.Fatalf("scenario: at least two TC servers required")
	}
	scale := func(base time.Duration) time.Duration {
		switch {
		case len(tcs) >= 6:
			return base * 2
		case len(tcs) >= 4:
			return base + base/2
		default:
			return base
		}
	}
	joinTimeout := scale(30 * time.Second)
	memberTimeout := scale(20 * time.Second)
	leaderTimeout := scale(30 * time.Second)
	leaveTimeout := scale(20 * time.Second)
	leaderChangeTimeout := scale(20 * time.Second)
	leaderDisableTimeout := scale(20 * time.Second)

	allEndpoints := endpointsForServers(tcs)
	JoinCluster(t, tcs, allEndpoints, tcHTTP, joinTimeout)
	WaitForClusterMembers(t, tcs, tcHTTP, allEndpoints, memberTimeout)
	leaderEndpoint, _ := WaitForLeaderAll(t, tcs, tcHTTP, leaderTimeout)
	if leaderEndpoint == "" {
		t.Fatalf("scenario: leader endpoint required")
	}
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}
	leaderIndex := indexOfServer(tcs, leaderTS)
	if leaderIndex < 0 {
		t.Fatalf("scenario: leader index not found")
	}
	remaining := filterServersByIndex(tcs, map[int]struct{}{leaderIndex: {}})
	if len(remaining) == 0 {
		t.Fatalf("scenario: no servers remain after leader removal")
	}

	LeaveCluster(t, tcs, []string{leaderEndpoint}, tcHTTP, leaveTimeout)

	expected := make([]string, 0, len(allEndpoints)-1)
	for _, ep := range allEndpoints {
		if strings.TrimSpace(ep) == leaderEndpoint {
			continue
		}
		expected = append(expected, ep)
	}
	WaitForClusterMembers(t, tcs, tcHTTP, expected, memberTimeout)

	for _, tc := range tcs {
		resp := FetchClusterList(t, tc, tcHTTP)
		if !equalStringSlices(resp.Endpoints, normalizeEndpoints(resp.Endpoints)) {
			t.Fatalf("scenario: cluster list not canonical on %s: %v", tc.URL(), resp.Endpoints)
		}
		if containsEndpoint(resp.Endpoints, leaderEndpoint) {
			t.Fatalf("scenario: leader endpoint still present on %s", tc.URL())
		}
	}
	WaitForLeaderUnavailable(t, leaderTS, tcHTTP, leaderDisableTimeout)

	if slack := leaderExpirySlack(t, remaining, tcHTTP, leaderEndpoint); slack > 0 {
		// Cloud/object-store backends can lag while converging leader lease
		// visibility after membership churn. Allow a larger, bounded buffer.
		extended := slack + 15*time.Second
		maxExtended := scale(3 * time.Minute)
		if extended > maxExtended {
			extended = maxExtended
		}
		if extended > leaderChangeTimeout {
			leaderChangeTimeout = extended
		}
	}
	newLeader, _ := WaitForLeaderAllChange(t, remaining, tcHTTP, leaderEndpoint, leaderChangeTimeout)
	sample := FetchClusterList(t, remaining[0], tcHTTP)
	if !containsEndpoint(sample.Endpoints, newLeader) {
		t.Fatalf("scenario: new leader %q not in membership list", newLeader)
	}

	JoinCluster(t, tcs, allEndpoints, tcHTTP, joinTimeout)
	WaitForClusterMembers(t, tcs, tcHTTP, allEndpoints, memberTimeout)
	WaitForLeaderAll(t, tcs, tcHTTP, leaderTimeout)
}

// RunQueueStateFailoverScenario validates stateful queue participants across leader failover.
func RunQueueStateFailoverScenario(t testing.TB, tcs []*lockd.TestServer, islands []*lockd.TestServer, tcHTTP HTTPClientFunc, tcClient func(testing.TB, *lockd.TestServer) *lockdclient.Client, restarts map[int]RestartSpec) {
	t.Helper()
	if len(islands) < 2 {
		t.Fatalf("scenario: at least two islands required")
	}
	if tcClient == nil {
		t.Fatalf("scenario: tc client builder required")
	}
	leaderEndpoint, term := WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}

	backendHashFor := func(ts *lockd.TestServer) string {
		if ts == nil || ts.Backend() == nil {
			return ""
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		hash, err := ts.Backend().BackendHash(ctx)
		cancel()
		if err != nil {
			t.Fatalf("scenario: backend hash: %v", err)
		}
		return strings.TrimSpace(hash)
	}
	leaderHash := backendHashFor(leaderTS)
	targetHash := ""
	for _, island := range islands {
		if island == nil || island == leaderTS {
			continue
		}
		hash := backendHashFor(island)
		if hash != "" && hash != leaderHash {
			targetHash = hash
			break
		}
	}
	if targetHash == "" {
		for _, island := range islands {
			if island == nil {
				continue
			}
			hash := backendHashFor(island)
			if hash != "" {
				targetHash = hash
				break
			}
		}
	}
	queueIsland := serverByEndpoint(tcs, preferredEndpointForBackend(ExpectedRMEndpoints(t, tcs), targetHash))
	if queueIsland == nil {
		queueIsland = serverForHash(islands, targetHash)
	}
	if queueIsland == nil {
		queueIsland = islands[0]
		if queueIsland == leaderTS {
			for _, island := range islands {
				if island != nil && island != leaderTS {
					queueIsland = island
					break
				}
			}
		}
	}
	if queueIsland == nil {
		t.Fatalf("scenario: queue island required")
	}
	queueCandidates := serversForBackendHash(tcs, targetHash)
	if len(queueCandidates) == 0 {
		queueCandidates = []*lockd.TestServer{queueIsland}
	} else {
		seen := false
		for _, candidate := range queueCandidates {
			if candidate == queueIsland {
				seen = true
				break
			}
		}
		if !seen && queueIsland != nil {
			queueCandidates = append(queueCandidates, queueIsland)
		}
	}
	activeQueueCtx, activeQueueCancel := context.WithTimeout(context.Background(), 20*time.Second)
	activeQueue := findActiveServer(activeQueueCtx, queueCandidates)
	activeQueueCancel()
	if activeQueue == nil {
		t.Fatalf("scenario: queue active node not found")
	}
	queueCLI := ensureClient(t, activeQueue)
	queueEndpoints := endpointsForServers(queueCandidates)
	if activeQueue != nil {
		queueEndpoint := strings.TrimSpace(activeQueue.URL())
		if queueEndpoint != "" {
			ordered := make([]string, 0, len(queueEndpoints)+1)
			ordered = append(ordered, queueEndpoint)
			for _, endpoint := range queueEndpoints {
				if endpoint == queueEndpoint {
					continue
				}
				ordered = append(ordered, endpoint)
			}
			queueEndpoints = ordered
		}
	}
	if len(queueEndpoints) > 0 {
		clientHost := activeQueue
		if clientHost == nil && len(queueCandidates) > 0 {
			clientHost = queueCandidates[0]
		}
		if clientHost == nil {
			t.Fatalf("scenario: queue client host required")
		}
		cli, err := clientHost.NewEndpointsClient(queueEndpoints, lockdclient.WithEndpointShuffle(false))
		if err != nil {
			t.Fatalf("scenario: queue client: %v", err)
		}
		queueCLI = cli
	}
	queueName := fmt.Sprintf("archi-queue-%d", time.Now().UnixNano())
	owner := fmt.Sprintf("archi-queue-owner-%d", time.Now().UnixNano())

	enqueueWithRetry := func(ctx context.Context, queue string, payload []byte) error {
		var lastErr error
		for {
			if ctx != nil && ctx.Err() != nil {
				if lastErr != nil {
					return lastErr
				}
				return ctx.Err()
			}
			_, err := queueCLI.EnqueueBytes(ctx, queue, payload, lockdclient.EnqueueOptions{})
			if err == nil {
				return nil
			}
			lastErr = err
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) {
				if apiErr.Response.ErrorCode == "node_passive" {
					sleep := apiErr.RetryAfterDuration()
					if sleep <= 0 {
						sleep = 100 * time.Millisecond
					}
					time.Sleep(sleep)
					continue
				}
			}
			if isRetryableNetError(err) || strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "connection reset") || strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "all endpoints unreachable") {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return err
		}
	}

	enqueueCtx, enqueueCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := enqueueWithRetry(enqueueCtx, queueName, []byte("archipelago-queue"))
	enqueueCancel()
	if err != nil {
		t.Fatalf("scenario: enqueue: %v", err)
	}

	txnID := xid.New().String()
	msg, err := dequeueWithStateRetry(t, queueCLI, queueName, lockdclient.DequeueOptions{
		Owner:        owner,
		TxnID:        txnID,
		Visibility:   10 * time.Second,
		BlockSeconds: 1,
	}, 15*time.Second)
	if err != nil {
		t.Fatalf("scenario: dequeue with state: %v", err)
	}
	if msg == nil || msg.StateHandle() == nil {
		t.Fatalf("scenario: expected stateful message")
	}
	_ = msg.ClosePayload()

	ns := msg.Namespace()
	if ns == "" {
		ns = namespaces.Default
	}
	messageKey, err := queue.MessageLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("scenario: message lease key: %v", err)
	}
	stateKey, err := queue.StateLeaseKey(ns, queueName, msg.MessageID())
	if err != nil {
		t.Fatalf("scenario: state lease key: %v", err)
	}
	participantMessage := strings.TrimPrefix(messageKey, ns+"/")
	participantState := strings.TrimPrefix(stateKey, ns+"/")

	backendHash := backendHashFor(queueIsland)
	if backendHash == "" {
		t.Fatalf("scenario: backend hash empty")
	}

	participants := []api.TxnParticipant{
		{Namespace: ns, Key: participantMessage, BackendHash: backendHash},
		{Namespace: ns, Key: participantState, BackendHash: backendHash},
	}

	extendDuration := 60 * time.Second
	extendExpiresAt := time.Now().Add(extendDuration).Unix()
	if err := postTxnDecideWithParticipants(t, leaderTS, tcHTTP, txnID, term, core.TxnStatePending, extendExpiresAt, participants); err != nil {
		t.Fatalf("scenario: extend txn expiry: %v", err)
	}
	extendCtx, extendCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := msg.Extend(extendCtx, extendDuration); err != nil {
		extendCancel()
		t.Fatalf("scenario: extend queue lease: %v", err)
	}
	extendCancel()

	stopServerForFailover(t, leaderTS, 10*time.Second, "queue_state_failover")

	alive := make([]*lockd.TestServer, 0, len(tcs)-1)
	for _, tc := range tcs {
		if tc != nil && tc != leaderTS {
			alive = append(alive, tc)
		}
	}
	newLeader, _ := WaitForLeaderChange(t, alive, tcHTTP, leaderEndpoint, 45*time.Second)
	commitTarget := NonLeader(alive, newLeader)
	if commitTarget == nil {
		commitTarget = serverByEndpoint(alive, newLeader)
	}
	if commitTarget == nil {
		t.Fatalf("scenario: commit target not found")
	}
	WaitForLeaderOn(t, commitTarget, tcHTTP, 20*time.Second)
	if err := commitTxnWithRetry(t, alive, commitTarget, tcClient, api.TxnDecisionRequest{
		TxnID:        txnID,
		Participants: participants,
	}, 30*time.Second); err != nil {
		t.Fatalf("scenario: commit after failover: %v", err)
	}

	queueProbe := queueCLI
	probeEndpoints := endpointsForServers(queueCandidates)
	if activeQueue != nil {
		queueEndpoint := strings.TrimSpace(activeQueue.URL())
		if queueEndpoint != "" {
			ordered := make([]string, 0, len(probeEndpoints)+1)
			ordered = append(ordered, queueEndpoint)
			for _, endpoint := range probeEndpoints {
				if endpoint == queueEndpoint {
					continue
				}
				ordered = append(ordered, endpoint)
			}
			probeEndpoints = ordered
		}
	}
	if len(probeEndpoints) > 0 {
		clientHost := activeQueue
		if clientHost == nil && queueIsland != nil {
			clientHost = queueIsland
		}
		if clientHost == nil && len(queueCandidates) > 0 {
			clientHost = queueCandidates[0]
		}
		if clientHost == nil {
			t.Fatalf("scenario: queue probe client host required")
		}
		cli, err := clientHost.NewEndpointsClient(probeEndpoints, lockdclient.WithEndpointShuffle(false))
		if err != nil {
			t.Fatalf("scenario: queue probe client: %v", err)
		}
		queueProbe = cli
	}

	waitForQueueEmpty := func() {
		deadline := time.Now().Add(45 * time.Second)
		for {
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 2*time.Second)
			probe, err := queueProbe.Dequeue(checkCtx, queueName, lockdclient.DequeueOptions{
				Owner:        owner + "-probe",
				BlockSeconds: lockdclient.BlockNoWait,
			})
			checkCancel()
			if probe != nil {
				_ = probe.Close()
				t.Fatalf("scenario: expected queue empty, got message %s", probe.MessageID())
			}
			var apiErr *lockdclient.APIError
			if errors.As(err, &apiErr) && (apiErr.Response.ErrorCode == "waiting" || apiErr.Response.ErrorCode == "tc_not_leader" || apiErr.Response.ErrorCode == "tc_unavailable") {
				if apiErr.Response.ErrorCode != "waiting" && time.Now().Before(deadline) {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("scenario: queue still visible after commit: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitForQueueEmpty()

	stateObjKey := queueStateObjectKey(queueName, msg.MessageID())
	stateMetaKey := participantState
	checkCtx, checkCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := waitForQueueStateObject(checkCtx, queueIsland.Backend(), ns, stateObjKey, false); err != nil {
		checkCancel()
		t.Fatalf("scenario: state object still present after commit: %v", err)
	}
	checkCancel()
	metaRes, err := queueIsland.Backend().LoadMeta(context.Background(), ns, stateMetaKey)
	if err != nil {
		t.Fatalf("scenario: load state meta after commit: %v", err)
	}
	if metaRes.Meta != nil && metaRes.Meta.Lease != nil {
		t.Fatalf("scenario: expected state lease cleared after commit")
	}

	if restarts != nil {
		leaderIndex := indexOfServer(tcs, leaderTS)
		if leaderIndex >= 0 {
			if spec, ok := restarts[leaderIndex]; ok && spec.Start != nil {
				restarted := spec.Start(t)
				requireStableServerIdentity(t, leaderTS, restarted)
				tcs[leaderIndex] = restarted
				JoinCluster(t, tcs, endpointsForServers(tcs), tcHTTP, 30*time.Second)
			}
		}
	}

	// Stale lease yields queue_message_lease_mismatch.
	queueNameStale := fmt.Sprintf("archi-queue-stale-%d", time.Now().UnixNano())
	ownerStale := owner + "-stale"
	enqueueCtx, enqueueCancel = context.WithTimeout(context.Background(), 5*time.Second)
	err = enqueueWithRetry(enqueueCtx, queueNameStale, []byte("archipelago-queue-stale"))
	enqueueCancel()
	if err != nil {
		t.Fatalf("scenario: enqueue stale: %v", err)
	}

	leaderEndpoint, term = WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
	leaderTS = serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader missing for stale pending extension")
	}

	staleVisibility := 5 * time.Second
	staleTxnID := xid.New().String()
	var staleMsg *lockdclient.QueueMessage
	for attempt := 0; attempt < 3; attempt++ {
		staleMsg, err = dequeueWithStateRetry(t, queueCLI, queueNameStale, lockdclient.DequeueOptions{
			Owner:        ownerStale,
			TxnID:        staleTxnID,
			Visibility:   staleVisibility,
			BlockSeconds: 1,
		}, 15*time.Second)
		if err == nil {
			break
		}
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "txn_conflict" {
			staleTxnID = xid.New().String()
			continue
		}
		t.Fatalf("scenario: dequeue stale with state: %v", err)
	}
	if err != nil {
		t.Fatalf("scenario: dequeue stale with state: %v", err)
	}
	if staleMsg == nil || staleMsg.StateHandle() == nil {
		t.Fatalf("scenario: expected stale stateful message")
	}
	_ = staleMsg.ClosePayload()
	staleMessageKey, err := queue.MessageLeaseKey(ns, queueNameStale, staleMsg.MessageID())
	if err != nil {
		t.Fatalf("scenario: stale message lease key: %v", err)
	}
	staleStateKey, err := queue.StateLeaseKey(ns, queueNameStale, staleMsg.MessageID())
	if err != nil {
		t.Fatalf("scenario: stale state lease key: %v", err)
	}
	staleParticipants := []api.TxnParticipant{
		{Namespace: ns, Key: strings.TrimPrefix(staleMessageKey, ns+"/"), BackendHash: backendHash},
		{Namespace: ns, Key: strings.TrimPrefix(staleStateKey, ns+"/"), BackendHash: backendHash},
	}

	staleExtendExpiresAt := time.Now().Add(30 * time.Second).Unix()
	staleErr := postTxnDecideWithParticipants(t, leaderTS, tcHTTP, staleTxnID, term, core.TxnStatePending, staleExtendExpiresAt, staleParticipants)
	if staleErr != nil {
		var apiErr *lockdclient.APIError
		if errors.As(staleErr, &apiErr) && (apiErr.Response.ErrorCode == "tc_not_leader" || apiErr.Response.ErrorCode == "tc_unavailable" || apiErr.Response.ErrorCode == "tc_term_stale") {
			leaderEndpoint, term = WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
			leaderTS = serverByEndpoint(tcs, leaderEndpoint)
			if leaderTS == nil {
				t.Fatalf("scenario: leader missing for stale pending extension")
			}
			staleErr = postTxnDecideWithParticipants(t, leaderTS, tcHTTP, staleTxnID, term, core.TxnStatePending, staleExtendExpiresAt, staleParticipants)
		}
	}
	if staleErr != nil {
		t.Fatalf("scenario: extend stale txn expiry: %v", staleErr)
	}

	if expiresAt := staleMsg.LeaseExpiresAt(); expiresAt > 0 {
		wait := time.Until(time.Unix(expiresAt, 0).Add(200 * time.Millisecond))
		if wait < staleVisibility {
			wait = staleVisibility + 200*time.Millisecond
		}
		if wait > 0 {
			time.Sleep(wait)
		}
	} else {
		time.Sleep(staleVisibility + 200*time.Millisecond)
	}

	var reacquireMsg *lockdclient.QueueMessage
	for attempt := 0; attempt < 3; attempt++ {
		reacquireMsg, err = dequeueWithStateRetry(t, queueCLI, queueNameStale, lockdclient.DequeueOptions{
			Owner:        ownerStale + "-reacquire",
			Visibility:   staleVisibility,
			BlockSeconds: 1,
		}, 15*time.Second)
		if err != nil {
			t.Fatalf("scenario: reacquire stale message: %v", err)
		}
		if reacquireMsg == nil {
			t.Fatalf("scenario: reacquire message missing")
		}
		if reacquireMsg.TxnID() != staleTxnID {
			break
		}
		_ = reacquireMsg.ClosePayload()
		nackCtx, nackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = reacquireMsg.Nack(nackCtx, 0, nil)
		nackCancel()
		time.Sleep(staleVisibility + 200*time.Millisecond)
	}
	if reacquireMsg == nil {
		t.Fatalf("scenario: reacquire message missing")
	}
	if reacquireMsg.TxnID() == staleTxnID {
		t.Fatalf("scenario: expected new txn id after lease expiry")
	}
	_ = reacquireMsg.ClosePayload()

	leaderEndpoint, term = WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
	leaderTS = serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader missing for stale commit")
	}
	staleCommitTarget := leaderTS
	if candidates := serversForBackendHash(tcs, backendHash); len(candidates) > 0 {
		if active := findActiveServer(context.Background(), candidates); active != nil {
			staleCommitTarget = active
		}
	} else if queueIsland != nil {
		staleCommitTarget = queueIsland
	}
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = retryOnNodePassive(commitCtx, func() error {
		return postTxnCommitWithParticipants(t, staleCommitTarget, tcHTTP, staleTxnID, term, staleParticipants)
	})
	commitCancel()
	if err == nil {
		t.Fatalf("scenario: expected queue_message_lease_mismatch or txn_conflict for stale lease")
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("scenario: expected api error, got %v", err)
	}
	if apiErr.Response.ErrorCode != "queue_message_lease_mismatch" && apiErr.Response.ErrorCode != "txn_conflict" {
		t.Fatalf("scenario: expected queue_message_lease_mismatch or txn_conflict, got %s", apiErr.Response.ErrorCode)
	}

	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := reacquireMsg.Ack(ackCtx); err != nil {
		ackCancel()
		t.Fatalf("scenario: cleanup ack: %v", err)
	}
	ackCancel()
}

// RunQuorumLossDuringRenewScenario verifies leader step-down on quorum loss and term fencing on recovery.
func RunQuorumLossDuringRenewScenario(t testing.TB, tcs []*lockd.TestServer, islands []*lockd.TestServer, restarts []RestartSpec, tcHTTP HTTPClientFunc, tcClient func(testing.TB, *lockd.TestServer) *lockdclient.Client) {
	t.Helper()
	if len(tcs) < 2 {
		t.Fatalf("scenario: at least two TC servers required")
	}
	if len(islands) == 0 {
		t.Fatalf("scenario: at least one island required")
	}
	if len(restarts) == 0 {
		t.Fatalf("scenario: restart specs required")
	}
	if tcClient == nil {
		t.Fatalf("scenario: tc client builder required")
	}

	endpoints := endpointsForServers(tcs)
	WaitForClusterMembers(t, tcs, tcHTTP, endpoints, 30*time.Second)
	_, oldTerm := WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	clients, hashes, keys := collectIslandInputs(ctx, t, islands, tcs, tcHTTP)
	const scenarioTTLSeconds = 90

	lease0, err := clients[0].Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keys[0],
		Owner:      "archipelago-quorum-loss",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire 0: %v", err)
	}
	for i := 1; i < len(clients); i++ {
		lease, err := clients[i].Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        keys[i],
			Owner:      "archipelago-quorum-loss",
			TTLSeconds: scenarioTTLSeconds,
			TxnID:      lease0.TxnID,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("scenario: acquire %d: %v", i, err)
		}
		if err := retryOnNodePassive(ctx, func() error {
			return lease.Save(ctx, map[string]any{"value": fmt.Sprintf("island-%d", i)})
		}); err != nil {
			t.Fatalf("scenario: save %d: %v", i, err)
		}
	}
	if err := retryOnNodePassive(ctx, func() error {
		return lease0.Save(ctx, map[string]any{"value": "island-0"})
	}); err != nil {
		t.Fatalf("scenario: save 0: %v", err)
	}

	participants := make([]api.TxnParticipant, 0, len(keys))
	for i, key := range keys {
		participants = append(participants, api.TxnParticipant{
			Namespace:   namespaces.Default,
			Key:         key,
			BackendHash: hashes[i],
		})
	}

	stopIndexes := make(map[int]struct{}, len(restarts))
	down := make([]*lockd.TestServer, 0, len(restarts))
	beforeByIndex := make(map[int]*lockd.TestServer, len(restarts))
	for _, spec := range restarts {
		if spec.Start == nil {
			t.Fatalf("scenario: restart spec missing start")
		}
		if spec.Index < 0 || spec.Index >= len(tcs) {
			t.Fatalf("scenario: restart index %d out of range", spec.Index)
		}
		if _, ok := stopIndexes[spec.Index]; ok {
			continue
		}
		stopIndexes[spec.Index] = struct{}{}
		ts := tcs[spec.Index]
		if ts == nil {
			t.Fatalf("scenario: restart server %d is nil", spec.Index)
		}
		down = append(down, ts)
		beforeByIndex[spec.Index] = ts
	}
	for _, ts := range down {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = ts.Abort(stopCtx)
		stopCancel()
	}

	alive := filterServersByIndex(tcs, stopIndexes)
	if len(alive) == 0 {
		t.Fatalf("scenario: no servers remain after quorum loss")
	}
	WaitForNoLeader(t, alive, tcHTTP, 20*time.Second)

	failCtx, failCancel := context.WithTimeout(context.Background(), 15*time.Second)
	_, err = tcClient(t, alive[0]).TxnCommit(failCtx, api.TxnDecisionRequest{
		TxnID:        lease0.TxnID,
		Participants: participants,
	})
	failCancel()
	if err == nil {
		t.Fatalf("scenario: expected commit to fail while leader unavailable")
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("scenario: expected api error, got %v", err)
	}
	if apiErr.Response.ErrorCode != "tc_unavailable" && apiErr.Response.ErrorCode != "node_passive" {
		t.Fatalf("scenario: expected tc_unavailable or node_passive, got %s", apiErr.Response.ErrorCode)
	}

	for _, spec := range restarts {
		restarted := spec.Start(t)
		if before := beforeByIndex[spec.Index]; before != nil {
			requireStableServerIdentity(t, before, restarted)
		}
		tcs[spec.Index] = restarted
	}

	endpoints = endpointsForServers(tcs)
	JoinCluster(t, tcs, endpoints, tcHTTP, 30*time.Second)

	newLeader, newTerm := WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
	if newTerm <= oldTerm {
		t.Fatalf("scenario: expected term to increase: old=%d new=%d", oldTerm, newTerm)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 20*time.Second)
	if !waitForActiveBackends(waitCtx, tcs, participants) {
		waitCancel()
		t.Fatalf("scenario: active backends not available after quorum restore")
	}
	waitCancel()

	pickCtx, pickCancel := context.WithTimeout(context.Background(), 5*time.Second)
	target := ActiveNonLeader(pickCtx, tcs, newLeader)
	pickCancel()
	if target == nil {
		target = serverByEndpoint(tcs, newLeader)
	}
	if target == nil {
		t.Fatalf("scenario: commit target missing after quorum restore")
	}
	WaitForLeaderOn(t, target, tcHTTP, 20*time.Second)
	if err := commitTxnWithRetry(t, tcs, target, tcClient, api.TxnDecisionRequest{
		TxnID:        lease0.TxnID,
		Participants: participants,
	}, 20*time.Second); err != nil {
		t.Fatalf("scenario: commit after quorum restore: %v", err)
	}

	for i, hash := range hashes {
		aliveServer := serverForHash(tcs, hash)
		if aliveServer == nil {
			t.Fatalf("scenario: missing server for backend %q", hash)
		}
		WaitForState(t, ensureClient(t, aliveServer), keys[i], true, 20*time.Second)
	}

	leaderTS := serverByEndpoint(tcs, newLeader)
	if leaderTS == nil {
		leaderTS = islands[0]
	}
	err = postTxnCommitWithTerm(t, leaderTS, tcHTTP, xid.New().String(), oldTerm)
	if err == nil {
		t.Fatalf("scenario: expected tc_term_stale for old term")
	}
	if !errors.As(err, &apiErr) {
		t.Fatalf("scenario: expected api error, got %v", err)
	}
	if apiErr.Response.ErrorCode != "tc_term_stale" {
		t.Fatalf("scenario: expected tc_term_stale, got %s", apiErr.Response.ErrorCode)
	}
}

func commitTxnWithRetry(t testing.TB, tcs []*lockd.TestServer, target *lockd.TestServer, tcClient func(testing.TB, *lockd.TestServer) *lockdclient.Client, req api.TxnDecisionRequest, timeout time.Duration) error {
	t.Helper()
	if target == nil {
		return fmt.Errorf("commit retry: target required")
	}
	if tcClient == nil {
		return fmt.Errorf("commit retry: client builder required")
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		attemptTimeout := 5 * time.Second
		if remaining < attemptTimeout {
			attemptTimeout = remaining
		}
		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		_, err := tcClient(t, target).TxnCommit(ctx, req)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.Response.ErrorCode {
			case "node_passive":
				if target != nil {
					lookup := remaining
					if lookup > 5*time.Second {
						lookup = 5 * time.Second
					}
					lookupCtx, cancel := context.WithTimeout(context.Background(), lookup)
					hash, hashErr := target.Backend().BackendHash(lookupCtx)
					hash = strings.TrimSpace(hash)
					var active *lockd.TestServer
					if hashErr == nil && hash != "" {
						candidates := serversForBackendHash(tcs, hash)
						active = findActiveServer(lookupCtx, candidates)
					}
					if active == nil {
						active = findActiveServer(lookupCtx, tcs)
					}
					cancel()
					if active != nil {
						target = active
					}
				}
				sleep := apiErr.RetryAfterDuration()
				if sleep <= 0 {
					sleep = 200 * time.Millisecond
				}
				if sleep > remaining {
					sleep = remaining
				}
				time.Sleep(sleep)
				continue
			case "txn_conflict":
				return nil
			case "txn_fanout_failed":
				if fanoutConflictOnly(apiErr.Response.Detail) {
					return nil
				}
				if fanoutNodePassiveOnly(apiErr.Response.Detail) {
					wait := remaining
					if wait > 2*time.Second {
						wait = 2 * time.Second
					}
					waitCtx, cancel := context.WithTimeout(context.Background(), wait)
					_ = waitForActiveBackends(waitCtx, tcs, req.Participants)
					cancel()
					sleep := apiErr.RetryAfterDuration()
					if sleep <= 0 {
						sleep = 200 * time.Millisecond
					}
					if sleep > remaining {
						sleep = remaining
					}
					time.Sleep(sleep)
					continue
				}
			case "tc_not_leader", "tc_unavailable":
				if apiErr.Response.LeaderEndpoint != "" && len(tcs) > 0 {
					if leader := serverByEndpoint(tcs, apiErr.Response.LeaderEndpoint); leader != nil {
						target = leader
					}
				}
				sleep := apiErr.RetryAfterDuration()
				if sleep <= 0 {
					sleep = 200 * time.Millisecond
				}
				if sleep > remaining {
					sleep = remaining
				}
				time.Sleep(sleep)
				continue
			}
		}
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "all endpoints unreachable") {
			if next := nextServer(tcs, target); next != nil {
				target = next
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if time.Now().Before(deadline) && isRetryableNetError(err) {
			if next := nextServer(tcs, target); next != nil {
				target = next
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return err
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("commit retry: timed out")
}

func nextServer(tcs []*lockd.TestServer, current *lockd.TestServer) *lockd.TestServer {
	if len(tcs) == 0 {
		return current
	}
	if current == nil {
		for _, ts := range tcs {
			if ts != nil {
				return ts
			}
		}
		return current
	}
	for i, ts := range tcs {
		if ts == current {
			for j := 1; j < len(tcs)+1; j++ {
				idx := (i + j) % len(tcs)
				if tcs[idx] != nil && tcs[idx] != current {
					return tcs[idx]
				}
			}
			return current
		}
	}
	return current
}

func isRetryableNetError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

func waitForActiveBackends(ctx context.Context, tcs []*lockd.TestServer, participants []api.TxnParticipant) bool {
	if len(participants) == 0 || len(tcs) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(participants))
	for _, p := range participants {
		hash := strings.TrimSpace(p.BackendHash)
		if hash == "" {
			continue
		}
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}
		candidates := serversForBackendHash(tcs, hash)
		if len(candidates) == 0 {
			return false
		}
		if findActiveServer(ctx, candidates) == nil {
			return false
		}
	}
	return len(seen) > 0
}

func saveWithRetry(t testing.TB, lease *lockdclient.LeaseSession, payload any, timeout time.Duration) error {
	t.Helper()
	if lease == nil {
		return fmt.Errorf("save retry: lease required")
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		attemptTimeout := 5 * time.Second
		if remaining < attemptTimeout {
			attemptTimeout = remaining
		}
		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		err := lease.Save(ctx, payload)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.Response.ErrorCode {
			case "tc_not_leader", "tc_unavailable", "node_passive":
				sleep := apiErr.RetryAfterDuration()
				if sleep <= 0 {
					sleep = 200 * time.Millisecond
				}
				if sleep > remaining {
					sleep = remaining
				}
				time.Sleep(sleep)
				continue
			}
		}
		if errors.Is(err, context.DeadlineExceeded) && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return err
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("save retry: timed out")
}

func fanoutConflictOnly(detail string) bool {
	detail = strings.TrimSpace(detail)
	if detail == "" || !strings.Contains(detail, "txn_conflict") {
		return false
	}
	codes := make([]int, 0, 2)
	for {
		idx := strings.Index(detail, "status ")
		if idx == -1 {
			break
		}
		detail = detail[idx+len("status "):]
		if len(detail) < 3 {
			break
		}
		codeStr := detail[:3]
		code, err := strconv.Atoi(codeStr)
		if err == nil {
			codes = append(codes, code)
		}
	}
	if len(codes) == 0 {
		return false
	}
	for _, code := range codes {
		if code != http.StatusConflict {
			return false
		}
	}
	return true
}

func fanoutNodePassiveOnly(detail string) bool {
	detail = strings.TrimSpace(detail)
	if detail == "" {
		return false
	}
	found := false
	for {
		idx := strings.Index(detail, "status ")
		if idx == -1 {
			break
		}
		detail = detail[idx+len("status "):]
		if len(detail) < 3 {
			return false
		}
		codeStr := detail[:3]
		if _, err := strconv.Atoi(codeStr); err != nil {
			return false
		}
		detail = detail[3:]
		colon := strings.Index(detail, ":")
		if colon == -1 {
			return false
		}
		codeText := strings.TrimSpace(detail[colon+1:])
		if codeText == "" {
			return false
		}
		codeWord := strings.Fields(codeText)[0]
		codeWord = strings.Trim(codeWord, ";,")
		if codeWord != "node_passive" {
			return false
		}
		found = true
	}
	return found
}

// RunLeaderDecisionFanoutInterruptedScenario stops the leader after decision recording but before fan-out.
func RunLeaderDecisionFanoutInterruptedScenario(t testing.TB, tcs []*lockd.TestServer, islands []*lockd.TestServer, tcHTTP HTTPClientFunc, tcClient func(testing.TB, *lockd.TestServer) *lockdclient.Client, gate *FanoutGate, restarts map[int]RestartSpec) {
	t.Helper()
	if len(tcs) < 2 {
		t.Fatalf("scenario: at least two TC servers required")
	}
	if len(islands) < 2 {
		t.Fatalf("scenario: at least two islands required")
	}
	if tcClient == nil {
		t.Fatalf("scenario: tc client builder required")
	}
	if gate == nil {
		t.Fatalf("scenario: fanout gate required")
	}

	leaderEndpoint, _ := WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}

	selected := islands
	if len(selected) > 2 {
		selected = selected[:2]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	clients, hashes, keys := collectIslandInputs(ctx, t, selected, tcs, tcHTTP)
	const scenarioTTLSeconds = 30

	lease0, err := clients[0].Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keys[0],
		Owner:      "archipelago-fanout-interrupt",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire 0: %v", err)
	}
	lease1, err := clients[1].Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keys[1],
		Owner:      "archipelago-fanout-interrupt",
		TTLSeconds: scenarioTTLSeconds,
		TxnID:      lease0.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire 1: %v", err)
	}
	if err := retryOnNodePassive(ctx, func() error {
		return lease1.Save(ctx, map[string]any{"value": "fanout-b"})
	}); err != nil {
		t.Fatalf("scenario: save 1: %v", err)
	}
	if err := retryOnNodePassive(ctx, func() error {
		return lease0.Save(ctx, map[string]any{"value": "fanout-a"})
	}); err != nil {
		t.Fatalf("scenario: save 0: %v", err)
	}

	gate.Arm(lease0.TxnID)

	participants := []api.TxnParticipant{
		{Namespace: namespaces.Default, Key: keys[0], BackendHash: hashes[0]},
		{Namespace: namespaces.Default, Key: keys[1], BackendHash: hashes[1]},
	}

	commitErrCh := make(chan error, 1)
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	go func() {
		_, err := tcClient(t, leaderTS).TxnCommit(commitCtx, api.TxnDecisionRequest{
			TxnID:        lease0.TxnID,
			Participants: participants,
		})
		commitErrCh <- err
	}()

	gate.WaitForStart(t, 15*time.Second)

	stopServerForFailover(t, leaderTS, 10*time.Second, "non_leader_forward_unavailable")
	gate.Release()
	commitCancel()

	select {
	case err := <-commitErrCh:
		if err == nil {
			t.Fatalf("scenario: expected commit to fail after leader shutdown")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("scenario: commit did not return after leader shutdown")
	}

	alive := make([]*lockd.TestServer, 0, len(tcs)-1)
	for _, ts := range tcs {
		if ts != nil && ts != leaderTS {
			alive = append(alive, ts)
		}
	}
	newLeader, _ := WaitForLeaderChange(t, alive, tcHTTP, leaderEndpoint, 45*time.Second)
	newLeaderTS := serverByEndpoint(alive, newLeader)
	if newLeaderTS == nil {
		t.Fatalf("scenario: new leader %q not found", newLeader)
	}

	if _, err := tcClient(t, newLeaderTS).TxnCommit(ctx, api.TxnDecisionRequest{
		TxnID:        lease0.TxnID,
		Participants: participants,
	}); err != nil {
		t.Fatalf("scenario: commit after failover: %v", err)
	}

	for i, hash := range hashes {
		candidates := serversForBackendHash(alive, hash)
		if len(candidates) == 0 {
			t.Fatalf("scenario: missing live server for backend %q", hash)
		}
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 45*time.Second)
		activeServer := findActiveServer(waitCtx, candidates)
		waitCancel()
		if activeServer == nil {
			t.Fatalf("scenario: no active server for backend %q after leader failover", hash)
		}
		WaitForState(t, ensureClient(t, activeServer), keys[i], true, 60*time.Second)
	}
	if restarts != nil {
		leaderIndex := indexOfServer(tcs, leaderTS)
		if leaderIndex >= 0 {
			if spec, ok := restarts[leaderIndex]; ok && spec.Start != nil {
				restarted := spec.Start(t)
				requireStableServerIdentity(t, leaderTS, restarted)
				tcs[leaderIndex] = restarted
				for i, island := range islands {
					if island == leaderTS {
						islands[i] = restarted
						break
					}
				}
			}
		}
	}
}

// RunFanoutScenarioMulti exercises multi-island fanout without leader churn.
func RunFanoutScenarioMulti(t testing.TB, tcs []*lockd.TestServer, islands []*lockd.TestServer, tcHTTP HTTPClientFunc, tcClient func(testing.TB, *lockd.TestServer) *lockdclient.Client) {
	t.Helper()
	if len(islands) < 2 {
		t.Fatalf("scenario: at least two islands required")
	}
	if tcClient == nil {
		t.Fatalf("scenario: tc client builder required")
	}
	leaderEndpoint, _ := WaitForLeaderAll(t, tcs, tcHTTP, 45*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	clients, hashes, keys := collectIslandInputs(ctx, t, islands, tcs, tcHTTP)
	const scenarioTTLSeconds = 30

	lease0, err := clients[0].Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keys[0],
		Owner:      "archipelago",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire 0: %v", err)
	}
	for i := 1; i < len(clients); i++ {
		lease, err := clients[i].Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        keys[i],
			Owner:      "archipelago",
			TTLSeconds: scenarioTTLSeconds,
			TxnID:      lease0.TxnID,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("scenario: acquire %d: %v", i, err)
		}
		if err := retryOnNodePassive(ctx, func() error {
			return lease.Save(ctx, map[string]any{"value": fmt.Sprintf("island-%d", i)})
		}); err != nil {
			t.Fatalf("scenario: save %d: %v", i, err)
		}
	}
	if err := retryOnNodePassive(ctx, func() error {
		return lease0.Save(ctx, map[string]any{"value": "island-0"})
	}); err != nil {
		t.Fatalf("scenario: save 0: %v", err)
	}

	nonLeader := NonLeader(tcs, leaderEndpoint)
	if nonLeader == nil {
		nonLeader = leaderTS
	}
	tcCli := tcClient(t, nonLeader)

	participants := make([]api.TxnParticipant, 0, len(keys))
	for i, key := range keys {
		participants = append(participants, api.TxnParticipant{
			Namespace:   namespaces.Default,
			Key:         key,
			BackendHash: hashes[i],
		})
	}
	req := api.TxnDecisionRequest{
		TxnID:        lease0.TxnID,
		Participants: participants,
	}
	if _, err := tcCli.TxnCommit(ctx, req); err != nil {
		t.Fatalf("scenario: txn commit: %v", err)
	}

	for i, cli := range clients {
		WaitForState(t, cli, keys[i], true, 20*time.Second)
	}
}

// RunAttachmentTxnScenario exercises attachment staging and commit across islands.
func RunAttachmentTxnScenario(t testing.TB, tcs []*lockd.TestServer, islands []*lockd.TestServer, tcHTTP HTTPClientFunc, tcClient func(testing.TB, *lockd.TestServer) *lockdclient.Client) {
	t.Helper()
	if len(islands) < 2 {
		t.Fatalf("scenario: at least two islands required")
	}
	if tcClient == nil {
		t.Fatalf("scenario: tc client builder required")
	}
	leaderEndpoint, _ := WaitForLeaderAll(t, tcs, tcHTTP, 45*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	clients, hashes, keys := collectIslandInputs(ctx, t, islands, tcs, tcHTTP)
	const scenarioTTLSeconds = 30

	lease0, err := clients[0].Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keys[0],
		Owner:      "archipelago-attach",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire 0: %v", err)
	}
	if _, err := lease0.Attach(ctx, lockdclient.AttachRequest{
		Name: "attachment-0.bin",
		Body: bytes.NewReader([]byte("payload-0")),
	}); err != nil {
		t.Fatalf("scenario: attach 0: %v", err)
	}

	for i := 1; i < len(clients); i++ {
		lease, err := clients[i].Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        keys[i],
			Owner:      fmt.Sprintf("archipelago-attach-%d", i),
			TTLSeconds: scenarioTTLSeconds,
			TxnID:      lease0.TxnID,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("scenario: acquire %d: %v", i, err)
		}
		if _, err := lease.Attach(ctx, lockdclient.AttachRequest{
			Name: fmt.Sprintf("attachment-%d.bin", i),
			Body: bytes.NewReader([]byte(fmt.Sprintf("payload-%d", i))),
		}); err != nil {
			t.Fatalf("scenario: attach %d: %v", i, err)
		}
	}

	nonLeader := NonLeader(tcs, leaderEndpoint)
	if nonLeader == nil {
		nonLeader = leaderTS
	}
	tcCli := tcClient(t, nonLeader)

	participants := make([]api.TxnParticipant, 0, len(keys))
	for i, key := range keys {
		participants = append(participants, api.TxnParticipant{
			Namespace:   namespaces.Default,
			Key:         key,
			BackendHash: hashes[i],
		})
	}
	req := api.TxnDecisionRequest{
		TxnID:        lease0.TxnID,
		Participants: participants,
	}
	if _, err := tcCli.TxnCommit(ctx, req); err != nil {
		t.Fatalf("scenario: txn commit: %v", err)
	}

	for i, cli := range clients {
		attachments := WaitForAttachments(t, cli, keys[i], 1, 20*time.Second)
		expectedName := fmt.Sprintf("attachment-%d.bin", i)
		if len(attachments) != 1 || attachments[0].Name != expectedName {
			t.Fatalf("scenario: attachments %d mismatch: %+v", i, attachments)
		}
		att, err := cli.GetAttachment(ctx, lockdclient.GetAttachmentRequest{
			Key:      keys[i],
			Public:   true,
			Selector: lockdclient.AttachmentSelector{Name: expectedName},
		})
		if err != nil {
			t.Fatalf("scenario: retrieve %d: %v", i, err)
		}
		data, err := io.ReadAll(att)
		att.Close()
		if err != nil {
			t.Fatalf("scenario: read %d: %v", i, err)
		}
		expectedPayload := fmt.Sprintf("payload-%d", i)
		if string(data) != expectedPayload {
			t.Fatalf("scenario: payload %d mismatch: %q", i, data)
		}
	}
}

func collectIslandInputs(ctx context.Context, t testing.TB, islands []*lockd.TestServer, allServers []*lockd.TestServer, tcHTTP HTTPClientFunc) ([]*lockdclient.Client, []string, []string) {
	t.Helper()
	hashSeen := make(map[string]struct{}, len(islands))
	clients := make([]*lockdclient.Client, 0, len(islands))
	hashes := make([]string, 0, len(islands))
	keys := make([]string, 0, len(islands))

	for idx, island := range islands {
		if island == nil {
			t.Fatalf("scenario: island %d is nil", idx)
		}
		WaitForLeaderOn(t, island, tcHTTP, 30*time.Second)
		hash, err := island.Backend().BackendHash(ctx)
		if err != nil {
			t.Fatalf("scenario: backend hash %d: %v", idx, err)
		}
		hash = strings.TrimSpace(hash)
		if hash == "" {
			t.Fatalf("scenario: backend hash %d empty", idx)
		}
		if _, ok := hashSeen[hash]; ok {
			t.Fatalf("scenario: duplicate backend hash %q", hash)
		}
		hashSeen[hash] = struct{}{}
		candidates := serversForBackendHash(allServers, hash)
		if len(candidates) == 0 {
			t.Fatalf("scenario: backend hash %q has no servers", hash)
		}
		active := findActiveServer(ctx, candidates)
		if active == nil {
			t.Fatalf("scenario: backend hash %q has no active server", hash)
		}
		endpoints := endpointsForServers(candidates)
		preferred := normalizeEndpoint(active.URL())
		for i, endpoint := range endpoints {
			if endpoint == preferred && i != 0 {
				endpoints[0], endpoints[i] = endpoints[i], endpoints[0]
				break
			}
		}
		client, err := active.NewEndpointsClient(endpoints, lockdclient.WithEndpointShuffle(false))
		if err != nil {
			t.Fatalf("scenario: client %d: %v", idx, err)
		}
		clients = append(clients, client)
		hashes = append(hashes, hash)
		keys = append(keys, fmt.Sprintf("archi-%d-%d", idx, time.Now().UnixNano()))
	}
	return clients, hashes, keys
}

func retryOnNodePassive(ctx context.Context, op func() error) error {
	for {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		err := op()
		if err == nil {
			return nil
		}
		var apiErr *lockdclient.APIError
		if !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "node_passive" {
			return err
		}
		delay := apiErr.RetryAfterDuration()
		if delay <= 0 {
			delay = 100 * time.Millisecond
		}
		time.Sleep(delay)
	}
}

func indexOfServer(servers []*lockd.TestServer, target *lockd.TestServer) int {
	if target == nil {
		return -1
	}
	for i, ts := range servers {
		if ts == target {
			return i
		}
	}
	return -1
}

// WaitForState polls until the key has (or does not have) state.
func WaitForState(t testing.TB, cli *lockdclient.Client, key string, expect bool, timeout time.Duration) {
	t.Helper()
	if cli == nil {
		t.Fatalf("wait for state: client required")
	}
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := cli.Get(ctx, key)
		cancel()
		if err == nil && resp != nil {
			hasState := resp.HasState
			_ = resp.Close()
			if hasState == expect {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("wait for state: key=%s expect=%v", key, expect)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WaitForAttachments polls until at least expect attachments are visible via public reads.
func WaitForAttachments(t testing.TB, cli *lockdclient.Client, key string, expect int, timeout time.Duration) []lockdclient.AttachmentInfo {
	t.Helper()
	if cli == nil {
		t.Fatalf("wait for attachments: client required")
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		list, err := cli.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{Key: key, Public: true})
		cancel()
		if err == nil && list != nil && len(list.Attachments) >= expect {
			return list.Attachments
		}
		if err != nil {
			lastErr = err
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				t.Fatalf("wait for attachments: %v", lastErr)
			}
			t.Fatalf("wait for attachments: key=%s expect=%d", key, expect)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func ensureClient(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
	t.Helper()
	if ts == nil {
		t.Fatalf("ensure client: server required")
		return nil
	}
	if ts.Client != nil {
		return ts.Client
	}
	cli, err := ts.NewClient()
	if err != nil {
		t.Fatalf("ensure client: %v", err)
	}
	return cli
}

func serverByEndpoint(servers []*lockd.TestServer, endpoint string) *lockd.TestServer {
	for _, ts := range servers {
		if ts != nil && ts.URL() == endpoint {
			return ts
		}
	}
	return nil
}

func serverForHash(servers []*lockd.TestServer, backendHash string) *lockd.TestServer {
	backendHash = strings.TrimSpace(backendHash)
	if backendHash == "" {
		return nil
	}
	candidates := serversForBackendHash(servers, backendHash)
	if len(candidates) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	active := findActiveServer(ctx, candidates)
	cancel()
	if active != nil {
		return active
	}
	return candidates[0]
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

func preferredEndpointForBackend(expected map[string][]string, backendHash string) string {
	backendHash = strings.TrimSpace(backendHash)
	if backendHash == "" || len(expected) == 0 {
		return ""
	}
	endpoints := expected[backendHash]
	if len(endpoints) == 0 {
		return ""
	}
	return normalizeEndpoint(endpoints[0])
}

func normalizeEndpoint(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	return strings.TrimSuffix(trimmed, "/")
}

func filterServersByIndex(servers []*lockd.TestServer, skip map[int]struct{}) []*lockd.TestServer {
	if len(servers) == 0 {
		return nil
	}
	out := make([]*lockd.TestServer, 0, len(servers))
	for idx, ts := range servers {
		if ts == nil {
			continue
		}
		if _, ok := skip[idx]; ok {
			continue
		}
		out = append(out, ts)
	}
	return out
}

func endpointsForServers(servers []*lockd.TestServer) []string {
	out := make([]string, 0, len(servers))
	for _, ts := range servers {
		if ts == nil {
			continue
		}
		out = append(out, ts.URL())
	}
	return out
}

func serversForBackendHash(servers []*lockd.TestServer, backendHash string) []*lockd.TestServer {
	backendHash = strings.TrimSpace(backendHash)
	if backendHash == "" {
		return nil
	}
	out := make([]*lockd.TestServer, 0, len(servers))
	for _, ts := range servers {
		if ts == nil || ts.Backend() == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		hash, err := ts.Backend().BackendHash(ctx)
		cancel()
		if err != nil {
			continue
		}
		if strings.TrimSpace(hash) == backendHash {
			out = append(out, ts)
		}
	}
	return out
}

func probeActive(ctx context.Context, cli *lockdclient.Client, key string) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	lease, err := cli.Acquire(probeCtx, api.AcquireRequest{
		Key:        key,
		Owner:      "ha-probe",
		TTLSeconds: 5,
		BlockSecs:  lockdclient.BlockNoWait,
	})
	if err != nil {
		return false, err
	}
	releaseCtx, releaseCancel := context.WithTimeout(ctx, 2*time.Second)
	_ = lease.Release(releaseCtx)
	releaseCancel()
	return true, nil
}

func findActiveServer(ctx context.Context, servers []*lockd.TestServer) *lockd.TestServer {
	if len(servers) == 0 {
		return nil
	}
	key := "ha-probe-" + uuidv7.NewString()
	for {
		if ctx != nil && ctx.Err() != nil {
			return nil
		}
		for _, ts := range servers {
			if ts == nil {
				continue
			}
			cli, err := ts.NewClient(lockdclient.WithEndpointShuffle(false))
			if err != nil {
				continue
			}
			active, probeErr := probeActive(ctx, cli, key)
			_ = cli.Close()
			if probeErr == nil && active {
				return ts
			}
			if probeErr != nil {
				var apiErr *lockdclient.APIError
				if errors.As(probeErr, &apiErr) {
					if apiErr.Response.ErrorCode == "node_passive" {
						continue
					}
					return nil
				}
				if isRetryableNetError(probeErr) || strings.Contains(probeErr.Error(), "connection refused") || strings.Contains(probeErr.Error(), "connection reset") || strings.Contains(probeErr.Error(), "EOF") {
					continue
				}
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func postTxnCommitWithTerm(t testing.TB, ts *lockd.TestServer, clientFn HTTPClientFunc, txnID string, term uint64) error {
	t.Helper()
	payload := api.TxnDecisionRequest{
		TxnID:  txnID,
		TCTerm: term,
	}
	return postTxnApply(t, ts, clientFn, "/v1/txn/commit", payload)
}

func postTxnCommitWithParticipants(t testing.TB, ts *lockd.TestServer, clientFn HTTPClientFunc, txnID string, term uint64, participants []api.TxnParticipant) error {
	t.Helper()
	payload := api.TxnDecisionRequest{
		TxnID:        txnID,
		TCTerm:       term,
		Participants: participants,
	}
	if target := targetBackendHash(participants); target != "" {
		payload.TargetBackendHash = target
	}
	return postTxnApply(t, ts, clientFn, "/v1/txn/commit", payload)
}

func postTxnDecideWithParticipants(t testing.TB, ts *lockd.TestServer, clientFn HTTPClientFunc, txnID string, term uint64, state core.TxnState, expiresAt int64, participants []api.TxnParticipant) error {
	t.Helper()
	payload := api.TxnDecisionRequest{
		TxnID:         txnID,
		State:         string(state),
		TCTerm:        term,
		ExpiresAtUnix: expiresAt,
		Participants:  participants,
	}
	return postTxnApply(t, ts, clientFn, "/v1/txn/decide", payload)
}

func postTxnRollbackWithParticipants(t testing.TB, ts *lockd.TestServer, clientFn HTTPClientFunc, txnID string, term uint64, participants []api.TxnParticipant) error {
	t.Helper()
	payload := api.TxnDecisionRequest{
		TxnID:        txnID,
		TCTerm:       term,
		Participants: participants,
	}
	if target := targetBackendHash(participants); target != "" {
		payload.TargetBackendHash = target
	}
	return postTxnApply(t, ts, clientFn, "/v1/txn/rollback", payload)
}

func targetBackendHash(participants []api.TxnParticipant) string {
	if len(participants) == 0 {
		return ""
	}
	var hash string
	for _, participant := range participants {
		if participant.BackendHash == "" {
			return ""
		}
		if hash == "" {
			hash = participant.BackendHash
			continue
		}
		if participant.BackendHash != hash {
			return ""
		}
	}
	return hash
}

func postTxnApply(t testing.TB, ts *lockd.TestServer, clientFn HTTPClientFunc, path string, payload api.TxnDecisionRequest) error {
	t.Helper()
	if ts == nil {
		t.Fatalf("txn apply: server required")
	}
	client := http.DefaultClient
	if clientFn != nil {
		client = clientFn(t, ts)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("txn apply marshal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL()+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("txn apply request: %v", err)
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
	if data, readErr := io.ReadAll(resp.Body); readErr == nil && len(data) > 0 {
		_ = json.Unmarshal(data, &errResp)
	}
	return &lockdclient.APIError{Status: resp.StatusCode, Response: errResp}
}

func containsEndpoint(list []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func queueStateObjectKey(queueName, messageID string) string {
	return path.Join("q", queueName, "state", messageID+".json")
}

func waitForQueueStateObject(ctx context.Context, backend storage.Backend, namespace, stateKey string, shouldExist bool) error {
	for {
		obj, err := backend.GetObject(ctx, namespace, stateKey)
		if err == nil {
			_ = obj.Reader.Close()
			if shouldExist {
				return nil
			}
		} else if errors.Is(err, storage.ErrNotFound) {
			if !shouldExist {
				return nil
			}
		} else {
			return err
		}
		if ctx.Err() != nil {
			if shouldExist {
				return fmt.Errorf("state object still missing")
			}
			return fmt.Errorf("state object still present")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func dequeueWithStateRetry(t testing.TB, cli *lockdclient.Client, queueName string, opts lockdclient.DequeueOptions, timeout time.Duration) (*lockdclient.QueueMessage, error) {
	t.Helper()
	if cli == nil {
		return nil, fmt.Errorf("dequeue with state: client required")
	}
	if strings.TrimSpace(queueName) == "" {
		return nil, fmt.Errorf("dequeue with state: queue required")
	}
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("dequeue with state: timed out")
		}
		attemptTimeout := remaining
		if attemptTimeout > 5*time.Second {
			attemptTimeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		msg, err := cli.DequeueWithState(ctx, queueName, opts)
		cancel()
		if err == nil {
			if msg == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return msg, nil
		}
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.Response.ErrorCode {
			case "waiting", "tc_not_leader", "tc_unavailable":
				sleep := apiErr.RetryAfterDuration()
				if sleep <= 0 {
					sleep = 200 * time.Millisecond
				}
				if sleep > remaining {
					sleep = remaining
				}
				time.Sleep(sleep)
				continue
			}
		}
		return nil, err
	}
}
