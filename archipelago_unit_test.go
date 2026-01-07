package lockd_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/archipelagotest"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/cryptoutil"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

type tcAuthMaterial struct {
	clientBundlePath string
	trustDir         string
}

type sharedMTLSAuthority struct {
	ca           *tlsutil.CA
	material     cryptoutil.MetadataMaterial
	clientBundle []byte
	pool         *mtlsCredPool
}

type mtlsCredPool struct {
	creds   []lockd.TestMTLSCredentials
	allowed map[string]struct{}
	next    uint32
}

const archipelagoUnitLeaderTTL = 2 * time.Second

func newMTLSCredPool(creds []lockd.TestMTLSCredentials, allowed []string) *mtlsCredPool {
	pool := &mtlsCredPool{
		creds:   creds,
		allowed: make(map[string]struct{}, len(allowed)),
	}
	for _, host := range dedupeHosts(allowed) {
		pool.allowed[host] = struct{}{}
	}
	return pool
}

func (p *mtlsCredPool) matches(hosts []string) bool {
	if p == nil || len(p.creds) == 0 {
		return false
	}
	for _, host := range dedupeHosts(hosts) {
		if _, ok := p.allowed[host]; !ok {
			return false
		}
	}
	return true
}

func (p *mtlsCredPool) nextCred() lockd.TestMTLSCredentials {
	if p == nil || len(p.creds) == 0 {
		return lockd.TestMTLSCredentials{}
	}
	idx := atomic.AddUint32(&p.next, 1)
	return p.creds[(int(idx)-1)%len(p.creds)]
}

func newTCAuthority(t testing.TB) tcAuthMaterial {
	t.Helper()
	if archipelagoFixture == nil {
		t.Fatalf("tc auth: fixture not initialized")
	}
	return archipelagoFixture.tcAuth
}

func newSharedMTLSAuthority(t testing.TB) sharedMTLSAuthority {
	t.Helper()
	if archipelagoFixture == nil {
		t.Fatalf("mtls: fixture not initialized")
	}
	return archipelagoFixture.mtls
}

func (a sharedMTLSAuthority) issueServerCredentials(t testing.TB, hosts []string) lockd.TestMTLSCredentials {
	t.Helper()
	if a.ca == nil {
		t.Fatalf("mtls: missing ca")
	}
	if a.pool != nil && a.pool.matches(hosts) {
		return a.pool.nextCred()
	}
	deduped := dedupeHosts(hosts)
	nodeID := uuidv7.NewString()
	spiffeURI, err := lockd.SPIFFEURIForServer(nodeID)
	if err != nil {
		t.Fatalf("mtls: spiffe uri: %v", err)
	}
	serverIssued, err := a.ca.IssueServerWithRequest(tlsutil.ServerCertRequest{
		CommonName: "lockd-test-server",
		Validity:   365 * 24 * time.Hour,
		Hosts:      deduped,
		URIs:       []*url.URL{spiffeURI},
	})
	if err != nil {
		t.Fatalf("mtls: issue server: %v", err)
	}
	serverBundle, err := tlsutil.EncodeServerBundle(a.ca.CertPEM, nil, serverIssued.CertPEM, serverIssued.KeyPEM, nil)
	if err != nil {
		t.Fatalf("mtls: encode server bundle: %v", err)
	}
	serverBundle, err = cryptoutil.ApplyMetadataMaterial(serverBundle, a.material)
	if err != nil {
		t.Fatalf("mtls: apply metadata: %v", err)
	}
	creds, err := lockd.NewTestMTLSCredentialsFromBundles(serverBundle, a.clientBundle)
	if err != nil {
		t.Fatalf("mtls: build credentials: %v", err)
	}
	return creds
}

func dedupeHosts(hosts []string) []string {
	seen := make(map[string]struct{}, len(hosts))
	out := make([]string, 0, len(hosts))
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	return out
}

func testServerHosts(addr string) []string {
	hosts := []string{"127.0.0.1", "localhost"}
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		hosts = append(hosts, host)
	}
	return hosts
}

func reserveAddrs(t testing.TB, count int) []string {
	t.Helper()
	if count <= 0 {
		return nil
	}
	addrs := make([]string, 0, count)
	for i := 0; i < count; i++ {
		addrs = append(addrs, archipelagotest.ReserveTCPAddr(t))
	}
	return addrs
}

func endpointsFromAddrs(scheme string, addrs []string) []string {
	out := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		out = append(out, fmt.Sprintf("%s://%s", scheme, addr))
	}
	return out
}

func serverHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	if ts == nil {
		t.Fatalf("server http client: nil test server")
	}
	if len(ts.Config.BundlePEM) == 0 {
		t.Fatalf("server http client: missing server bundle")
	}
	bundle, err := tlsutil.LoadBundleFromBytes(ts.Config.BundlePEM)
	if err != nil {
		t.Fatalf("server http client: load bundle: %v", err)
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("server http client: default transport is %T", http.DefaultTransport)
	}
	transport := base.Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion:         tls.VersionTLS12,
		RootCAs:            bundle.CAPool,
		Certificates:       []tls.Certificate{bundle.ServerCertificate},
		ClientSessionCache: tls.NewLRUClientSessionCache(256),
	}
	return &http.Client{Transport: transport}
}

func baseArchipelagoConfig() lockd.Config {
	return lockd.Config{
		Store:             "mem://",
		ListenProto:       "tcp",
		DefaultTTL:        30 * time.Second,
		MaxTTL:            2 * time.Minute,
		AcquireBlock:      2 * time.Second,
		SweeperInterval:   time.Second,
		DisableMTLS:       false,
		TCFanoutTimeout:   250 * time.Millisecond,
		TCFanoutBaseDelay: 25 * time.Millisecond,
		TCFanoutMaxDelay:  100 * time.Millisecond,
	}
}

func tcClientFor(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
	t.Helper()
	cli, err := ts.NewEndpointsClient([]string{ts.URL()}, lockdclient.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("tc client: %v", err)
	}
	return cli
}

func startTwoNodeArchipelago(t testing.TB, leaseTTL time.Duration, clk clock.Clock) (*lockd.TestServer, *lockd.TestServer, []*lockd.TestServer, []string) {
	t.Helper()
	baseCfg := baseArchipelagoConfig()
	tcAuth := newTCAuthority(t)
	mtls := newSharedMTLSAuthority(t)
	backendA := memorybackend.NewWithConfig(memorybackend.Config{})
	backendB := memorybackend.NewWithConfig(memorybackend.Config{})
	scheme := "https"
	addrsA := reserveAddrs(t, 1)
	addrsB := reserveAddrs(t, 1)
	endpointsA := endpointsFromAddrs(scheme, addrsA)
	endpointsB := endpointsFromAddrs(scheme, addrsB)
	baseCfg.TCJoinEndpoints = []string{endpointsA[0], endpointsB[0]}
	nodeA := startMemNode(t, baseCfg, tcAuth, mtls, backendA, addrsA[0], scheme, leaseTTL, clk)
	nodeB := startMemNode(t, baseCfg, tcAuth, mtls, backendB, addrsB[0], scheme, leaseTTL, clk)
	endpoints := []string{endpointsA[0], endpointsB[0]}
	return nodeA, nodeB, []*lockd.TestServer{nodeA, nodeB}, endpoints
}

func TestArchipelagoUnitMembershipAnnounceList(t *testing.T) {
	t.Parallel()
	clk := clock.NewManual(time.Now().UTC())
	leaseTTL := archipelagoUnitLeaderTTL
	_, _, tcs, endpoints := startTwoNodeArchipelago(t, leaseTTL, clk)
	httpClient := serverHTTPClient

	joinClusterOnce(t, tcs, endpoints, httpClient)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 50*time.Millisecond, 2*time.Second)
}

func TestArchipelagoUnitLeaveRemovesSelf(t *testing.T) {
	t.Parallel()
	clk := clock.NewManual(time.Now().UTC())
	leaseTTL := archipelagoUnitLeaderTTL
	nodeA, nodeB, tcs, endpoints := startTwoNodeArchipelago(t, leaseTTL, clk)
	httpClient := serverHTTPClient

	joinClusterOnce(t, tcs, endpoints, httpClient)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 50*time.Millisecond, 2*time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = nodeA.Stop(stopCtx)
	stopCancel()

	alive := []*lockd.TestServer{nodeB}
	waitForClusterMembersWithClock(t, alive, httpClient, endpointsForServers(alive), clk, 50*time.Millisecond, 2*time.Second)
}

func TestArchipelagoUnitLeaveStopsAutoAnnounceUntilJoin(t *testing.T) {
	t.Parallel()
	clk := clock.NewManual(time.Now().UTC())
	leaseTTL := archipelagoUnitLeaderTTL
	nodeA, nodeB, tcs, endpoints := startTwoNodeArchipelago(t, leaseTTL, clk)
	httpClient := serverHTTPClient

	joinClusterOnce(t, tcs, endpoints, httpClient)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 50*time.Millisecond, 2*time.Second)

	client := httpClient(t, nodeA)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeA.URL()+"/v1/tc/cluster/leave", nil)
	if err != nil {
		cancel()
		t.Fatalf("cluster leave request: %v", err)
	}
	resp, err := client.Do(req)
	cancel()
	if err != nil {
		t.Fatalf("cluster leave request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cluster leave status=%d", resp.StatusCode)
	}

	waitForClusterMembersWithClock(t, []*lockd.TestServer{nodeB}, httpClient, []string{nodeB.URL()}, clk, 50*time.Millisecond, 2*time.Second)

	membershipTTL := tccluster.DeriveLeaseTTL(leaseTTL)
	advanceManualClock(t, clk, membershipTTL/3+200*time.Millisecond)
	list := archipelagotest.FetchClusterList(t, nodeB, httpClient)
	have := tccluster.NormalizeEndpoints(list.Endpoints)
	if tccluster.ContainsEndpoint(have, nodeA.URL()) {
		t.Fatalf("expected %s to stay out after leave; have=%v", nodeA.URL(), have)
	}

	joinClusterOnce(t, tcs, endpoints, httpClient)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 50*time.Millisecond, 2*time.Second)
}

func TestArchipelagoUnitLeaveIsSelfOnly(t *testing.T) {
	t.Parallel()
	clk := clock.NewManual(time.Now().UTC())
	leaseTTL := archipelagoUnitLeaderTTL
	nodeA, nodeB, tcs, endpoints := startTwoNodeArchipelago(t, leaseTTL, clk)
	httpClient := serverHTTPClient

	joinClusterOnce(t, tcs, endpoints, httpClient)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 50*time.Millisecond, 2*time.Second)

	client := httpClient(t, nodeA)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeB.URL()+"/v1/tc/cluster/leave", nil)
	if err != nil {
		cancel()
		t.Fatalf("cluster leave request: %v", err)
	}
	resp, err := client.Do(req)
	cancel()
	if err != nil {
		t.Fatalf("cluster leave request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cluster leave status=%d", resp.StatusCode)
	}

	list := archipelagotest.FetchClusterList(t, nodeB, httpClient)
	have := tccluster.NormalizeEndpoints(list.Endpoints)
	if !tccluster.ContainsEndpoint(have, nodeB.URL()) {
		t.Fatalf("expected %s to remain in cluster list; have=%v", nodeB.URL(), have)
	}
}

func TestArchipelagoUnitAbortPreservesLeaseUntilExpiry(t *testing.T) {
	t.Parallel()
	clk := clock.NewManual(time.Now().UTC())
	leaseTTL := archipelagoUnitLeaderTTL
	nodeA, nodeB, tcs, endpoints := startTwoNodeArchipelago(t, leaseTTL, clk)
	httpClient := serverHTTPClient

	joinClusterOnce(t, tcs, endpoints, httpClient)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 50*time.Millisecond, 2*time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = nodeA.Abort(stopCtx)
	stopCancel()

	waitForClusterMembersWithClock(t, []*lockd.TestServer{nodeB}, httpClient, endpoints, clk, 50*time.Millisecond, 2*time.Second)

	membershipTTL := tccluster.DeriveLeaseTTL(leaseTTL)
	waitForClusterMembersWithClock(t, []*lockd.TestServer{nodeB}, httpClient, []string{nodeB.URL()}, clk, membershipTTL/3, membershipTTL)
}

func TestArchipelagoUnitLeaderElectionStepdown(t *testing.T) {
	t.Parallel()
	clk := clock.NewManual(time.Now().UTC())
	leaseTTL := archipelagoUnitLeaderTTL
	_, _, tcs, endpoints := startTwoNodeArchipelago(t, leaseTTL, clk)
	httpClient := serverHTTPClient

	joinClusterOnce(t, tcs, endpoints, httpClient)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 50*time.Millisecond, 2*time.Second)

	leaderEndpoint, oldTerm := waitForLeaderWithClock(t, tcs, httpClient, clk, 50*time.Millisecond, 2*time.Second)
	nonLeader := archipelagotest.NonLeader(tcs, leaderEndpoint)
	if nonLeader == nil {
		t.Fatalf("scenario: non-leader not found")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = nonLeader.Abort(stopCtx)
	stopCancel()

	alive := filterServers(tcs, nonLeader)
	membershipTTL := tccluster.DeriveLeaseTTL(leaseTTL)
	// Allow quorum loss to expire the leader lease under the manual clock.
	waitForNoLeaderWithClock(t, alive, httpClient, clk, 50*time.Millisecond, membershipTTL)
	waitForClusterMembersWithClock(t, alive, httpClient, endpointsForServers(alive), clk, membershipTTL/3, membershipTTL)
	_, newTerm := waitForLeaderWithClock(t, alive, httpClient, clk, 50*time.Millisecond, 2*time.Second)
	if newTerm <= oldTerm {
		t.Fatalf("expected term to increase: old=%d new=%d", oldTerm, newTerm)
	}
}

func TestArchipelagoUnitNonLeaderForwardUnavailable(t *testing.T) {
	t.Parallel()
	clk := clock.NewManual(time.Now().UTC())
	// Keep leader info valid while the manual clock advances after abort.
	leaseTTL := 5 * time.Second
	_, _, tcs, endpoints := startTwoNodeArchipelago(t, leaseTTL, clk)
	httpClient := serverHTTPClient

	joinClusterOnce(t, tcs, endpoints, httpClient)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 50*time.Millisecond, leaseTTL)

	leaderEndpoint, _ := waitForLeaderAllWithClock(t, tcs, httpClient, clk, 50*time.Millisecond, leaseTTL)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}
	nonLeader := archipelagotest.NonLeader(tcs, leaderEndpoint)
	if nonLeader == nil {
		t.Fatalf("scenario: non-leader not found")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = leaderTS.Abort(stopCtx)
	stopCancel()

	waitForLeaderOnWithClock(t, nonLeader, httpClient, clk, leaderEndpoint, 50*time.Millisecond, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := tcClientFor(t, nonLeader).TxnCommit(ctx, api.TxnDecisionRequest{
		TxnID: xid.New().String(),
	})
	if err == nil {
		t.Fatalf("expected commit to fail with leader unavailable")
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected api error, got %v", err)
	}
	if apiErr.Response.ErrorCode != "tc_not_leader" {
		t.Fatalf("expected tc_not_leader, got %s", apiErr.Response.ErrorCode)
	}
	if apiErr.Response.LeaderEndpoint != leaderEndpoint {
		t.Fatalf("expected leader endpoint %q, got %q", leaderEndpoint, apiErr.Response.LeaderEndpoint)
	}
}

func TestArchipelagoUnitRMRegistryReplication(t *testing.T) {
	t.Parallel()
	clk := clock.NewManual(time.Now().UTC())
	leaseTTL := archipelagoUnitLeaderTTL
	_, _, tcs, endpoints := startTwoNodeArchipelago(t, leaseTTL, clk)
	httpClient := serverHTTPClient

	joinClusterOnce(t, tcs, endpoints, httpClient)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 50*time.Millisecond, 2*time.Second)

	leaderEndpoint, _ := waitForLeaderWithClock(t, tcs, httpClient, clk, 50*time.Millisecond, 2*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}
	down := archipelagotest.NonLeader(tcs, leaderEndpoint)
	if down == nil {
		t.Fatalf("scenario: non-leader not found")
	}

	archipelagotest.RegisterRM(t, leaderTS, leaderTS, httpClient)
	archipelagotest.RegisterRM(t, leaderTS, down, httpClient)
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	waitForRMRegistryWithClock(t, tcs, httpClient, expectedRMs, clk, 50*time.Millisecond, 2*time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = down.Abort(stopCtx)
	stopCancel()

	hashB := backendHash(t, down)
	if err := archipelagotest.UnregisterRMWithBackend(t, leaderTS, hashB, down.URL(), httpClient); err == nil {
		t.Fatalf("expected unregister replication error")
	} else if apiErr, ok := err.(*lockdclient.APIError); !ok {
		t.Fatalf("expected api error, got %v", err)
	} else if apiErr.Response.ErrorCode != "tc_rm_replication_failed" {
		t.Fatalf("expected tc_rm_replication_failed, got %s", apiErr.Response.ErrorCode)
	}
	waitForRMRegistryEndpointWithClock(t, []*lockd.TestServer{leaderTS}, httpClient, hashB, down.URL(), true, clk, 50*time.Millisecond, 2*time.Second)

	fakeEndpoint := down.URL() + "/fake"
	if err := archipelagotest.RegisterRMWithBackend(t, leaderTS, hashB, fakeEndpoint, httpClient); err == nil {
		t.Fatalf("expected register replication error")
	} else if apiErr, ok := err.(*lockdclient.APIError); !ok {
		t.Fatalf("expected api error, got %v", err)
	} else if apiErr.Response.ErrorCode != "tc_rm_replication_failed" {
		t.Fatalf("expected tc_rm_replication_failed, got %s", apiErr.Response.ErrorCode)
	}
	waitForRMRegistryEndpointWithClock(t, []*lockd.TestServer{leaderTS}, httpClient, hashB, fakeEndpoint, false, clk, 50*time.Millisecond, 2*time.Second)
}

func startMemNode(t testing.TB, base lockd.Config, tcAuth tcAuthMaterial, mtls sharedMTLSAuthority, backend *memorybackend.Store, addr, scheme string, leaseTTL time.Duration, clk clock.Clock) *lockd.TestServer {
	t.Helper()
	cfg := base
	cfg.Listen = addr
	cfg.SelfEndpoint = fmt.Sprintf("%s://%s", scheme, addr)
	cfg.TCClientBundlePath = tcAuth.clientBundlePath
	cfg.TCTrustDir = tcAuth.trustDir
	cfg.TCAllowDefaultCA = true
	creds := mtls.issueServerCredentials(t, testServerHosts(addr))
	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestBackend(backend),
		lockd.WithTestListener("tcp", addr),
		lockd.WithTestTCLeaderLeaseTTL(leaseTTL),
		lockd.WithTestLoggerFromTB(t, pslog.DebugLevel),
		lockd.WithTestMTLSCredentials(creds),
		lockd.WithTestMTLS(),
	}
	if clk != nil {
		opts = append(opts, lockd.WithTestClock(clk))
		opts = append(opts, lockd.WithTestCloseDefaults(
			lockd.WithDrainLeases(-1),
			lockd.WithShutdownTimeout(500*time.Millisecond),
		))
	}
	return lockd.StartTestServer(t, opts...)
}

func startMemIsland(t testing.TB, base lockd.Config, tcAuth tcAuthMaterial, mtls sharedMTLSAuthority, backend *memorybackend.Store, scheme string, leaseTTL time.Duration, addrs []string, clk clock.Clock) ([]*lockd.TestServer, []string) {
	t.Helper()
	if len(addrs) == 0 {
		t.Fatalf("start mem island: addresses required")
	}
	servers := make([]*lockd.TestServer, 0, len(addrs))
	endpoints := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		servers = append(servers, startMemNode(t, base, tcAuth, mtls, backend, addr, scheme, leaseTTL, clk))
		endpoints = append(endpoints, fmt.Sprintf("%s://%s", scheme, addr))
	}
	return servers, endpoints
}

func joinClusterOnce(t testing.TB, servers []*lockd.TestServer, endpoints []string, clientFn archipelagotest.HTTPClientFunc) {
	t.Helper()
	if err := archipelagotest.JoinClusterOnce(t, servers, endpoints, clientFn); err != nil {
		t.Fatalf("join cluster: %v", err)
	}
}

func advanceManualClock(t testing.TB, clk *clock.Manual, step time.Duration) {
	t.Helper()
	if clk == nil {
		return
	}
	if step <= 0 {
		step = 50 * time.Millisecond
	}
	clk.Advance(step)
	runtime.Gosched()
	time.Sleep(1 * time.Millisecond)
}

func waitForLeaderWithClock(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, clk *clock.Manual, step, timeout time.Duration) (string, uint64) {
	t.Helper()
	if clk == nil {
		return archipelagotest.WaitForLeader(t, servers, clientFn, timeout)
	}
	if step <= 0 {
		step = 50 * time.Millisecond
	}
	deadline := clk.Now().Add(timeout)
	for {
		leader, term, ok := leaderConsensusFromServersAt(t, servers, clientFn, clk.Now())
		if ok {
			return leader, term
		}
		if clk.Now().After(deadline) {
			t.Fatalf("wait leader (manual clock): timed out")
		}
		advanceManualClock(t, clk, step)
	}
}

func waitForRMRegistryWithClock(t testing.TB, tcs []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, expected map[string][]string, clk *clock.Manual, step, timeout time.Duration) {
	t.Helper()
	if clk == nil {
		archipelagotest.WaitForRMRegistry(t, tcs, clientFn, expected, timeout)
		return
	}
	if len(expected) == 0 {
		t.Fatalf("rm registry: expected endpoints required")
	}
	if step <= 0 {
		step = 200 * time.Millisecond
	}
	deadline := clk.Now().Add(timeout)
	for {
		missing := ""
		for _, tc := range tcs {
			resp := archipelagotest.FetchRMList(t, tc, clientFn)
			have := make(map[string][]string, len(resp.Backends))
			for _, backend := range resp.Backends {
				hash := strings.TrimSpace(backend.BackendHash)
				if hash == "" {
					continue
				}
				have[hash] = tccluster.NormalizeEndpoints(backend.Endpoints)
			}
			for hash, wantEndpoints := range expected {
				haveEndpoints := have[hash]
				for _, endpoint := range wantEndpoints {
					if !tccluster.ContainsEndpoint(haveEndpoints, endpoint) {
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
		if clk.Now().After(deadline) {
			t.Fatalf("rm registry: %s", missing)
		}
		advanceManualClock(t, clk, step)
	}
}

func waitForRMRegistryEndpointWithClock(t testing.TB, tcs []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, backendHash, endpoint string, expect bool, clk *clock.Manual, step, timeout time.Duration) {
	t.Helper()
	if clk == nil {
		archipelagotest.WaitForRMRegistryEndpoint(t, tcs, clientFn, backendHash, endpoint, expect, timeout)
		return
	}
	if len(tcs) == 0 {
		t.Fatalf("rm registry endpoint: tc servers required")
	}
	backendHash = strings.TrimSpace(backendHash)
	endpoint = strings.TrimSpace(endpoint)
	if backendHash == "" || endpoint == "" {
		t.Fatalf("rm registry endpoint: backend hash and endpoint required")
	}
	if step <= 0 {
		step = 200 * time.Millisecond
	}
	deadline := clk.Now().Add(timeout)
	for {
		mismatch := ""
		for _, tc := range tcs {
			resp := archipelagotest.FetchRMList(t, tc, clientFn)
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
		if clk.Now().After(deadline) {
			t.Fatalf("rm registry endpoint: %s", mismatch)
		}
		advanceManualClock(t, clk, step)
	}
}

func waitForLeaderChangeWithClock(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, clk *clock.Manual, prev string, step, timeout time.Duration) (string, uint64) {
	t.Helper()
	if clk == nil {
		return archipelagotest.WaitForLeaderChange(t, servers, clientFn, prev, timeout)
	}
	if step <= 0 {
		step = 50 * time.Millisecond
	}
	deadline := clk.Now().Add(timeout)
	for {
		leader, term, ok := leaderConsensusFromServersAt(t, servers, clientFn, clk.Now())
		if ok && leader != "" && leader != prev {
			return leader, term
		}
		if clk.Now().After(deadline) {
			t.Fatalf("wait leader change (manual clock): timed out (prev=%s)", prev)
		}
		advanceManualClock(t, clk, step)
	}
}

func waitForLeaderAllWithClock(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, clk *clock.Manual, step, timeout time.Duration) (string, uint64) {
	t.Helper()
	if clk == nil {
		return archipelagotest.WaitForLeaderAll(t, servers, clientFn, timeout)
	}
	if step <= 0 {
		step = 50 * time.Millisecond
	}
	deadline := clk.Now().Add(timeout)
	for {
		leader, term, ok := leaderAllFromServersAt(t, servers, clientFn, clk.Now())
		if ok {
			return leader, term
		}
		if clk.Now().After(deadline) {
			t.Fatalf("wait leader all (manual clock): timed out")
		}
		advanceManualClock(t, clk, step)
	}
}

func waitForLeaderAllChangeWithClock(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, clk *clock.Manual, prev string, step, timeout time.Duration) (string, uint64) {
	t.Helper()
	if clk == nil {
		return archipelagotest.WaitForLeaderAllChange(t, servers, clientFn, prev, timeout)
	}
	if step <= 0 {
		step = 50 * time.Millisecond
	}
	deadline := clk.Now().Add(timeout)
	for {
		leader, term, ok := leaderAllFromServersAt(t, servers, clientFn, clk.Now())
		if ok && leader != "" && leader != prev {
			return leader, term
		}
		if clk.Now().After(deadline) {
			t.Fatalf("wait leader all change (manual clock): timed out (prev=%s)", prev)
		}
		advanceManualClock(t, clk, step)
	}
}

func waitForLeaderOnWithClock(t testing.TB, ts *lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, clk *clock.Manual, want string, step, timeout time.Duration) (string, uint64) {
	t.Helper()
	if ts == nil {
		t.Fatalf("wait leader on: server required")
	}
	if step <= 0 {
		step = 50 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	if clk != nil {
		deadline = clk.Now().Add(timeout)
	}
	for {
		now := time.Now()
		if clk != nil {
			now = clk.Now()
		}
		leader, term, ok := leaderInfoOnAt(t, ts, clientFn, now)
		if ok && leader != "" && (want == "" || leader == want) {
			return leader, term
		}
		if clk == nil && time.Now().After(deadline) {
			t.Fatalf("wait leader on: timed out (want=%s)", want)
		}
		if clk != nil && clk.Now().After(deadline) {
			t.Fatalf("wait leader on (manual clock): timed out (want=%s)", want)
		}
		if clk != nil {
			advanceManualClock(t, clk, step)
			continue
		}
		time.Sleep(step)
	}
}

func waitForClusterMembersWithClock(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, expected []string, clk *clock.Manual, step, timeout time.Duration) {
	t.Helper()
	if clk == nil {
		archipelagotest.WaitForClusterMembers(t, servers, clientFn, expected, timeout)
		return
	}
	if len(servers) == 0 {
		t.Fatalf("cluster members: no servers")
	}
	expected = tccluster.NormalizeEndpoints(expected)
	if len(expected) == 0 {
		t.Fatalf("cluster members: expected endpoints required")
	}
	if step <= 0 {
		step = 200 * time.Millisecond
	}
	deadline := clk.Now().Add(timeout)
	for {
		mismatch := ""
		for _, ts := range servers {
			resp := archipelagotest.FetchClusterList(t, ts, clientFn)
			have := tccluster.NormalizeEndpoints(resp.Endpoints)
			if !equalStringSlices(have, expected) {
				mismatch = fmt.Sprintf("cluster list mismatch on %s (have=%v want=%v)", ts.URL(), have, expected)
				break
			}
		}
		if mismatch == "" {
			return
		}
		if clk.Now().After(deadline) {
			t.Fatalf("cluster members: %s", mismatch)
		}
		advanceManualClock(t, clk, step)
	}
}

func waitForNoLeaderWithClock(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, clk *clock.Manual, step, timeout time.Duration) {
	t.Helper()
	if clk == nil {
		waitForNoLeader(t, servers, clientFn, timeout)
		return
	}
	if step <= 0 {
		step = 200 * time.Millisecond
	}
	deadline := clk.Now().Add(timeout)
	for {
		if allNoLeaderWithClock(t, servers, clientFn, clk) {
			return
		}
		if clk.Now().After(deadline) {
			t.Fatalf("wait no leader: timed out")
		}
		advanceManualClock(t, clk, step)
	}
}

func leaderConsensusFromServersAt(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, now time.Time) (string, uint64, bool) {
	t.Helper()
	type leaderKey struct {
		endpoint string
		term     uint64
	}
	total := 0
	counts := make(map[leaderKey]int)
	for _, ts := range servers {
		if ts == nil {
			continue
		}
		total++
		leader, term, ok := leaderInfoOnAt(t, ts, clientFn, now)
		if !ok {
			continue
		}
		counts[leaderKey{endpoint: leader, term: term}]++
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
	if best.endpoint == "" || bestCount == 0 {
		return "", 0, false
	}
	if bestCount >= quorum {
		return best.endpoint, best.term, true
	}
	if len(counts) == 1 {
		return best.endpoint, best.term, true
	}
	return "", 0, false
}

func leaderConsensusFromServers(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc) (string, uint64, bool) {
	t.Helper()
	return leaderConsensusFromServersAt(t, servers, clientFn, time.Now())
}

func leaderAllFromServersAt(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, now time.Time) (string, uint64, bool) {
	t.Helper()
	if len(servers) == 0 {
		return "", 0, false
	}
	var leader string
	var term uint64
	for _, ts := range servers {
		if ts == nil {
			return "", 0, false
		}
		seenLeader, seenTerm, ok := leaderInfoOnAt(t, ts, clientFn, now)
		if !ok {
			return "", 0, false
		}
		if leader == "" {
			leader = seenLeader
			term = seenTerm
			continue
		}
		if leader != seenLeader || term != seenTerm {
			return "", 0, false
		}
	}
	if leader == "" || term == 0 {
		return "", 0, false
	}
	return leader, term, true
}

func leaderAllFromServers(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc) (string, uint64, bool) {
	t.Helper()
	return leaderAllFromServersAt(t, servers, clientFn, time.Now())
}

func serverByEndpoint(servers []*lockd.TestServer, endpoint string) *lockd.TestServer {
	for _, ts := range servers {
		if ts != nil && ts.URL() == endpoint {
			return ts
		}
	}
	return nil
}

func filterServers(servers []*lockd.TestServer, remove ...*lockd.TestServer) []*lockd.TestServer {
	out := make([]*lockd.TestServer, 0, len(servers))
	for _, ts := range servers {
		if ts == nil || containsServer(remove, ts) {
			continue
		}
		out = append(out, ts)
	}
	return out
}

func containsServer(list []*lockd.TestServer, target *lockd.TestServer) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
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

func firstOther(servers []*lockd.TestServer, skip *lockd.TestServer, extraSkips ...*lockd.TestServer) *lockd.TestServer {
	for _, ts := range servers {
		if ts == nil || ts == skip || containsServer(extraSkips, ts) {
			continue
		}
		return ts
	}
	return nil
}

func waitForNoLeader(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, timeout time.Duration) {
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

func allNoLeader(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc) bool {
	t.Helper()
	now := time.Now().UnixMilli()
	for _, ts := range servers {
		if ts == nil {
			continue
		}
		client := http.DefaultClient
		if clientFn != nil {
			client = clientFn(t, ts)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/v1/tc/leader", nil)
		if err != nil {
			cancel()
			return false
		}
		resp, err := client.Do(req)
		cancel()
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				_ = resp.Body.Close()
			}
			return false
		}
		var payload api.TCLeaderResponse
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			_ = resp.Body.Close()
			return false
		}
		_ = resp.Body.Close()
		if payload.LeaderEndpoint != "" && payload.ExpiresAtUnix > now {
			return false
		}
	}
	return true
}

func allNoLeaderWithClock(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, clk *clock.Manual) bool {
	t.Helper()
	now := time.Now().UnixMilli()
	if clk != nil {
		now = clk.Now().UnixMilli()
	}
	for _, ts := range servers {
		if ts == nil {
			continue
		}
		client := http.DefaultClient
		if clientFn != nil {
			client = clientFn(t, ts)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/v1/tc/leader", nil)
		if err != nil {
			cancel()
			return false
		}
		resp, err := client.Do(req)
		cancel()
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				_ = resp.Body.Close()
			}
			return false
		}
		var payload api.TCLeaderResponse
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			_ = resp.Body.Close()
			return false
		}
		_ = resp.Body.Close()
		if payload.LeaderEndpoint != "" && payload.ExpiresAtUnix > now {
			return false
		}
	}
	return true
}

func leaderInfoOnAt(t testing.TB, ts *lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, now time.Time) (string, uint64, bool) {
	t.Helper()
	if ts == nil {
		return "", 0, false
	}
	nowMillis := now.UnixMilli()
	client := http.DefaultClient
	if clientFn != nil {
		client = clientFn(t, ts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL()+"/v1/tc/leader", nil)
	if err != nil {
		cancel()
		return "", 0, false
	}
	resp, err := client.Do(req)
	cancel()
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return "", 0, false
	}
	var payload api.TCLeaderResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		_ = resp.Body.Close()
		return "", 0, false
	}
	_ = resp.Body.Close()
	if payload.LeaderEndpoint == "" || payload.ExpiresAtUnix <= nowMillis {
		return "", 0, false
	}
	return payload.LeaderEndpoint, payload.Term, true
}

func leaderInfoOn(t testing.TB, ts *lockd.TestServer, clientFn archipelagotest.HTTPClientFunc) (string, uint64, bool) {
	t.Helper()
	return leaderInfoOnAt(t, ts, clientFn, time.Now())
}

func waitForLeaderOnOptional(t testing.TB, ts *lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, timeout time.Duration) (string, uint64, bool) {
	t.Helper()
	if ts == nil {
		return "", 0, false
	}
	deadline := time.Now().Add(timeout)
	for {
		leader, term, ok := leaderInfoOn(t, ts, clientFn)
		if ok {
			return leader, term, true
		}
		if time.Now().After(deadline) {
			return "", 0, false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForLeaderOn(t testing.TB, ts *lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, timeout time.Duration) (string, uint64) {
	t.Helper()
	leader, term, ok := waitForLeaderOnOptional(t, ts, clientFn, timeout)
	if ok {
		return leader, term
	}
	if ts == nil {
		t.Fatalf("wait leader on: server required")
	}
	t.Fatalf("wait leader on %s: timed out", ts.URL())
	return "", 0
}

func waitForLeaderOnAny(t testing.TB, servers []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, expectedLeader string, timeout time.Duration) (*lockd.TestServer, string, uint64) {
	t.Helper()
	if len(servers) == 0 {
		t.Fatalf("wait leader on any: no servers")
	}
	deadline := time.Now().Add(timeout)
	for {
		for _, ts := range servers {
			leader, term, ok := leaderInfoOn(t, ts, clientFn)
			if !ok {
				continue
			}
			if expectedLeader != "" && leader != expectedLeader {
				continue
			}
			return ts, leader, term
		}
		if time.Now().After(deadline) {
			suffix := ""
			if expectedLeader != "" {
				suffix = fmt.Sprintf(" (expected %s)", expectedLeader)
			}
			t.Fatalf("wait leader on any: timed out%s for %v", suffix, endpointsForServers(servers))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForLeaderPair(t testing.TB, islandA, islandB []*lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, timeout time.Duration) (*lockd.TestServer, *lockd.TestServer, string) {
	t.Helper()
	if len(islandA) == 0 || len(islandB) == 0 {
		t.Fatalf("wait leader pair: islands required")
	}
	deadline := time.Now().Add(timeout)
	for {
		for _, tsA := range islandA {
			leaderA, _, okA := leaderInfoOn(t, tsA, clientFn)
			if !okA {
				continue
			}
			for _, tsB := range islandB {
				leaderB, _, okB := leaderInfoOn(t, tsB, clientFn)
				if okB && leaderB == leaderA {
					return tsA, tsB, leaderA
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("wait leader pair: timed out for %v and %v", endpointsForServers(islandA), endpointsForServers(islandB))
		}
		time.Sleep(100 * time.Millisecond)
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
		return tccluster.ContainsEndpoint(backend.Endpoints, endpoint)
	}
	return false
}

func backendHash(t testing.TB, ts *lockd.TestServer) string {
	t.Helper()
	if ts == nil || ts.Backend() == nil {
		t.Fatalf("backend hash: server/backend required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	hash, err := ts.Backend().BackendHash(ctx)
	if err != nil {
		t.Fatalf("backend hash: %v", err)
	}
	return hash
}

func postTxnCommitWithTerm(t testing.TB, ts *lockd.TestServer, clientFn archipelagotest.HTTPClientFunc, txnID string, term uint64) error {
	t.Helper()
	if ts == nil {
		t.Fatalf("txn commit: server required")
	}
	client := http.DefaultClient
	if clientFn != nil {
		client = clientFn(t, ts)
	}
	payload := api.TxnDecisionRequest{
		TxnID:  txnID,
		TCTerm: term,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("txn commit marshal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL()+"/v1/txn/commit", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("txn commit request: %v", err)
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
