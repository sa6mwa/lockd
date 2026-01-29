//go:build integration && mem && !lq && !query && !crypto

package memintegration

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/archipelagotest"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

func TestMemArchipelagoLeaderFailover(t *testing.T) {
	leaseTTL := 10 * time.Second
	baseCfg := buildMemConfig(t)
	baseCfg.HAMode = "concurrent"
	cryptotest.ConfigureTCAuth(t, &baseCfg)
	baseCfg.TCClientBundlePath = cryptotest.SharedTCClientBundlePath(t)

	scheme := "http"
	if cryptotest.TestMTLSEnabled() {
		scheme = "https"
	}

	backends := []*memorybackend.Store{
		memorybackend.NewWithConfig(memorybackend.Config{}),
		memorybackend.NewWithConfig(memorybackend.Config{}),
		memorybackend.NewWithConfig(memorybackend.Config{}),
	}
	fanoutGate := archipelagotest.NewFanoutGate()

	addrsByBackend := make([][]string, 0, len(backends))
	endpoints := make([]string, 0, 6)
	for range backends {
		addrs := []string{
			archipelagotest.ReserveTCPAddr(t),
			archipelagotest.ReserveTCPAddr(t),
		}
		addrsByBackend = append(addrsByBackend, addrs)
		for _, addr := range addrs {
			endpoints = append(endpoints, fmt.Sprintf("%s://%s", scheme, addr))
		}
	}
	baseCfg.TCJoinEndpoints = append([]string{}, endpoints...)

	tcs := make([]*lockd.TestServer, 0, 6)
	islands := make([]*lockd.TestServer, 0, len(backends))
	restartSpecs := make([]archipelagotest.RestartSpec, 0, 3)
	restartMap := make(map[int]archipelagotest.RestartSpec, 6)
	for backendIdx, backend := range backends {
		addrs := addrsByBackend[backendIdx]
		for i := 0; i < len(addrs); i++ {
			addr := addrs[i]
			ts := startMemArchipelagoNode(t, baseCfg, backend, addr, scheme, leaseTTL, fanoutGate)
			idx := len(tcs)
			backendRef := backend
			addrRef := addr
			credsRef := ts.TestMTLSCredentials()
			spec := archipelagotest.RestartSpec{
				Index: idx,
				Start: func(tb testing.TB) *lockd.TestServer {
					tb.Helper()
					return startMemArchipelagoNode(tb, baseCfg, backendRef, addrRef, scheme, leaseTTL, fanoutGate, credsRef)
				},
			}
			restartMap[idx] = spec
			tcs = append(tcs, ts)
			if i == 0 {
				islands = append(islands, ts)
			} else {
				restartSpecs = append(restartSpecs, spec)
			}
		}
	}

	tcHTTP := func(t testing.TB, ts *lockd.TestServer) *http.Client {
		t.Helper()
		return cryptotest.RequireServerHTTPClient(t, ts)
	}
	rmHTTP := func(t testing.TB, ts *lockd.TestServer) *http.Client {
		t.Helper()
		return cryptotest.RequireServerHTTPClient(t, ts)
	}

	archipelagotest.JoinCluster(t, tcs, endpoints, tcHTTP, 20*time.Second)
	archipelagotest.WaitForLeaderAll(t, tcs, tcHTTP, 30*time.Second)

	for _, rm := range tcs {
		cryptotest.RegisterRMWithTimeout(t, tcs[0], rm, 30*time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hashes := make([]string, 0, len(islands))
	for idx, island := range islands {
		hash, err := island.Backend().BackendHash(ctx)
		if err != nil {
			t.Fatalf("hash %d: %v", idx, err)
		}
		hashes = append(hashes, hash)
	}
	archipelagotest.RequireRMRegistry(t, tcs, tcHTTP, hashes)
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, tcHTTP, expectedRMs, 20*time.Second)

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		return cryptotest.RequireTCClient(t, ts, lockdclient.WithEndpointShuffle(false))
	}

	archipelagotest.RunLeaderDecisionFanoutInterruptedScenario(t, tcs, islands, tcHTTP, tcClient, fanoutGate, restartMap)
	archipelagotest.RunQuorumLossDuringRenewScenario(t, tcs, islands, restartSpecs, tcHTTP, tcClient)
	archipelagotest.RunLeaderFailoverScenarioMulti(t, tcs, islands, tcHTTP, rmHTTP, tcClient, restartMap)
	archipelagotest.RunRMRegistryReplicationScenario(t, tcs, tcHTTP, rmHTTP, restartMap)
	archipelagotest.RunRMApplyTermFencingScenario(t, tcs, islands, tcHTTP, restartMap)
	archipelagotest.RunQueueStateFailoverScenario(t, tcs, islands, tcHTTP, tcClient, restartMap)
	archipelagotest.RunTCMembershipChurnScenario(t, tcs, tcHTTP)
	archipelagotest.RunNonLeaderForwardUnavailableScenario(t, tcs, islands, tcHTTP, tcClient)
}

func TestMemArchipelagoFanoutPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("archipelago fanout pressure test skipped with -short")
	}

	leaseTTL := 10 * time.Second
	baseCfg := buildMemConfig(t)
	baseCfg.HAMode = "concurrent"
	cryptotest.ConfigureTCAuth(t, &baseCfg)
	baseCfg.TCClientBundlePath = cryptotest.SharedTCClientBundlePath(t)

	scheme := "http"
	if cryptotest.TestMTLSEnabled() {
		scheme = "https"
	}

	backends := []*memorybackend.Store{
		memorybackend.NewWithConfig(memorybackend.Config{}),
		memorybackend.NewWithConfig(memorybackend.Config{}),
		memorybackend.NewWithConfig(memorybackend.Config{}),
		memorybackend.NewWithConfig(memorybackend.Config{}),
		memorybackend.NewWithConfig(memorybackend.Config{}),
	}

	addrsByBackend := make([][]string, 0, len(backends))
	endpoints := make([]string, 0, 10)
	for range backends {
		addrs := []string{
			archipelagotest.ReserveTCPAddr(t),
			archipelagotest.ReserveTCPAddr(t),
		}
		addrsByBackend = append(addrsByBackend, addrs)
		for _, addr := range addrs {
			endpoints = append(endpoints, fmt.Sprintf("%s://%s", scheme, addr))
		}
	}
	baseCfg.TCJoinEndpoints = append([]string{}, endpoints...)

	tcs := make([]*lockd.TestServer, 0, 10)
	islands := make([]*lockd.TestServer, 0, len(backends))
	for backendIdx, backend := range backends {
		addrs := addrsByBackend[backendIdx]
		for i := 0; i < len(addrs); i++ {
			addr := addrs[i]
			ts := startMemArchipelagoNode(t, baseCfg, backend, addr, scheme, leaseTTL, nil)
			tcs = append(tcs, ts)
			if i == 0 {
				islands = append(islands, ts)
			}
		}
	}

	tcHTTP := func(t testing.TB, ts *lockd.TestServer) *http.Client {
		t.Helper()
		return cryptotest.RequireServerHTTPClient(t, ts)
	}

	archipelagotest.JoinCluster(t, tcs, endpoints, tcHTTP, 20*time.Second)

	for _, rm := range tcs {
		cryptotest.RegisterRM(t, tcs[0], rm)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hashes := make([]string, 0, len(islands))
	for idx, island := range islands {
		hash, err := island.Backend().BackendHash(ctx)
		if err != nil {
			t.Fatalf("hash %d: %v", idx, err)
		}
		hashes = append(hashes, hash)
	}
	archipelagotest.RequireRMRegistry(t, tcs, tcHTTP, hashes)
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, tcHTTP, expectedRMs, 20*time.Second)

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		return cryptotest.RequireTCClient(t, ts, lockdclient.WithEndpointShuffle(false))
	}

	const fanoutRuns = 8
	for i := 0; i < fanoutRuns; i++ {
		i := i
		t.Run(fmt.Sprintf("fanout-%02d", i), func(t *testing.T) {
			t.Parallel()
			archipelagotest.RunFanoutScenarioMulti(t, tcs, islands, tcHTTP, tcClient)
		})
	}
}

func startMemArchipelagoNode(tb testing.TB, base lockd.Config, backend *memorybackend.Store, addr, scheme string, leaseTTL time.Duration, gate *archipelagotest.FanoutGate, creds ...lockd.TestMTLSCredentials) *lockd.TestServer {
	tb.Helper()
	cfg := base
	cfg.Listen = addr
	cfg.ListenProto = "tcp"
	cfg.SelfEndpoint = fmt.Sprintf("%s://%s", scheme, addr)

	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestBackend(backend),
		lockd.WithTestListener("tcp", addr),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestTCLeaderLeaseTTL(leaseTTL),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(60*time.Second),
			lockdclient.WithCloseTimeout(60*time.Second),
			lockdclient.WithKeepAliveTimeout(60*time.Second),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
	}
	if gate != nil {
		opts = append(opts, lockd.WithTestTCFanoutGate(gate.Hook))
	}
	if len(creds) > 0 && creds[0].Valid() {
		opts = append(opts, cryptotest.SharedMTLSOptions(tb, creds[0])...)
	} else {
		opts = append(opts, cryptotest.SharedMTLSOptions(tb)...)
	}
	return lockd.StartTestServer(tb, opts...)
}
