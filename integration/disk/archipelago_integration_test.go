//go:build integration && disk && !lq && !query && !crypto

package diskintegration

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/hatest"
	"pkt.systems/lockd/internal/archipelagotest"
	"pkt.systems/pslog"
)

func TestDiskArchipelagoLeaderFailover(t *testing.T) {
	leaseTTL := 10 * time.Second
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	rootA := prepareDiskRoot(t, "")
	rootB := prepareDiskRoot(t, "")
	cfgA := buildDiskConfig(t, rootA, 0)
	cfgB := buildDiskConfig(t, rootB, 0)
	cfgA.HAMode = "concurrent"
	cfgB.HAMode = "concurrent"
	cryptotest.ConfigureTCAuth(t, &cfgA)
	cryptotest.ConfigureTCAuth(t, &cfgB)
	cfgA.TCClientBundlePath = bundlePath
	cfgB.TCClientBundlePath = bundlePath

	scheme := "http"
	if cryptotest.TestMTLSEnabled() {
		scheme = "https"
	}

	addrA1 := archipelagotest.ReserveTCPAddr(t)
	addrA2 := archipelagotest.ReserveTCPAddr(t)
	addrB1 := archipelagotest.ReserveTCPAddr(t)
	addrB2 := archipelagotest.ReserveTCPAddr(t)
	endpoints := []string{
		fmt.Sprintf("%s://%s", scheme, addrA1),
		fmt.Sprintf("%s://%s", scheme, addrA2),
		fmt.Sprintf("%s://%s", scheme, addrB1),
		fmt.Sprintf("%s://%s", scheme, addrB2),
	}
	cfgA.TCJoinEndpoints = append([]string{}, endpoints...)
	cfgB.TCJoinEndpoints = append([]string{}, endpoints...)

	fanoutGate := archipelagotest.NewFanoutGate()
	tcA1 := startDiskArchipelagoNode(t, cfgA, addrA1, scheme, leaseTTL, fanoutGate)
	tcA2 := startDiskArchipelagoNode(t, cfgA, addrA2, scheme, leaseTTL, fanoutGate)
	tcB1 := startDiskArchipelagoNode(t, cfgB, addrB1, scheme, leaseTTL, fanoutGate)
	tcB2 := startDiskArchipelagoNode(t, cfgB, addrB2, scheme, leaseTTL, fanoutGate)
	credsA1 := tcA1.TestMTLSCredentials()
	credsA2 := tcA2.TestMTLSCredentials()
	credsB1 := tcB1.TestMTLSCredentials()
	credsB2 := tcB2.TestMTLSCredentials()

	tcs := []*lockd.TestServer{tcA1, tcA2, tcB1, tcB2}
	restartSpecs := []archipelagotest.RestartSpec{
		{
			Index: 1,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startDiskArchipelagoNode(tb, cfgA, addrA2, scheme, leaseTTL, fanoutGate, credsA2)
			},
		},
		{
			Index: 3,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startDiskArchipelagoNode(tb, cfgB, addrB2, scheme, leaseTTL, fanoutGate, credsB2)
			},
		},
	}
	restartMap := map[int]archipelagotest.RestartSpec{
		0: {
			Index: 0,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startDiskArchipelagoNode(tb, cfgA, addrA1, scheme, leaseTTL, fanoutGate, credsA1)
			},
		},
		1: {
			Index: 1,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startDiskArchipelagoNode(tb, cfgA, addrA2, scheme, leaseTTL, fanoutGate, credsA2)
			},
		},
		2: {
			Index: 2,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startDiskArchipelagoNode(tb, cfgB, addrB1, scheme, leaseTTL, fanoutGate, credsB1)
			},
		},
		3: {
			Index: 3,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startDiskArchipelagoNode(tb, cfgB, addrB2, scheme, leaseTTL, fanoutGate, credsB2)
			},
		},
	}

	tcHTTP := func(t testing.TB, ts *lockd.TestServer) *http.Client {
		t.Helper()
		return cryptotest.RequireServerHTTPClient(t, ts)
	}
	rmHTTP := func(t testing.TB, ts *lockd.TestServer) *http.Client {
		t.Helper()
		return cryptotest.RequireServerHTTPClient(t, ts)
	}
	archipelagotest.JoinCluster(t, tcs, endpoints, tcHTTP, 25*time.Second)

	var islands []*lockd.TestServer
	refreshIslands := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		activeA, probeA, err := hatest.FindActiveServer(ctx, tcA1, tcA2)
		if probeA != nil {
			_ = probeA.Close()
		}
		if err != nil {
			t.Fatalf("find active A: %v", err)
		}
		activeB, probeB, err := hatest.FindActiveServer(ctx, tcB1, tcB2)
		if probeB != nil {
			_ = probeB.Close()
		}
		if err != nil {
			t.Fatalf("find active B: %v", err)
		}
		attachHAClient(t, activeA, tcA1, tcA2)
		attachHAClient(t, activeB, tcB1, tcB2)
		islands = []*lockd.TestServer{activeA, activeB}
	}
	refreshIslands()

	for _, rm := range tcs {
		cryptotest.RegisterRM(t, tcA1, rm)
	}
	if len(islands) != 2 {
		t.Fatalf("expected 2 active islands, got %d", len(islands))
	}
	hashCtx, hashCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer hashCancel()
	hashA, err := islands[0].Backend().BackendHash(hashCtx)
	if err != nil {
		t.Fatalf("hash A: %v", err)
	}
	hashB, err := islands[1].Backend().BackendHash(hashCtx)
	if err != nil {
		t.Fatalf("hash B: %v", err)
	}
	archipelagotest.RequireRMRegistry(t, tcs, tcHTTP, []string{hashA, hashB})
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, tcHTTP, expectedRMs, 20*time.Second)

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		return cryptotest.RequireTCClient(t, ts, lockdclient.WithEndpointShuffle(false))
	}

	refreshIslands()
	archipelagotest.RunLeaderDecisionFanoutInterruptedScenario(t, tcs, islands, tcHTTP, tcClient, fanoutGate, restartMap)
	refreshIslands()
	archipelagotest.RunQuorumLossDuringRenewScenario(t, tcs, islands, restartSpecs, tcHTTP, tcClient)
	refreshIslands()
	archipelagotest.RunLeaderFailoverScenarioMulti(t, tcs, islands, tcHTTP, rmHTTP, tcClient, restartMap)
	refreshIslands()
	archipelagotest.RunRMRegistryReplicationScenario(t, tcs, tcHTTP, rmHTTP, restartMap)
	refreshIslands()
	archipelagotest.RunRMApplyTermFencingScenario(t, tcs, islands, tcHTTP, restartMap)
	refreshIslands()
	archipelagotest.RunQueueStateFailoverScenario(t, tcs, islands, tcHTTP, tcClient, restartMap)
	refreshIslands()
	archipelagotest.RunTCMembershipChurnScenario(t, tcs, tcHTTP)
	refreshIslands()
	archipelagotest.RunNonLeaderForwardUnavailableScenario(t, tcs, islands, tcHTTP, tcClient)
}

func startDiskArchipelagoNode(tb testing.TB, base lockd.Config, addr, scheme string, leaseTTL time.Duration, gate *archipelagotest.FanoutGate, creds ...lockd.TestMTLSCredentials) *lockd.TestServer {
	tb.Helper()
	cfg := base
	cfg.Listen = addr
	cfg.ListenProto = "tcp"
	cfg.SelfEndpoint = fmt.Sprintf("%s://%s", scheme, addr)

	loggerOption := diskTestLoggerOption(tb)
	clientOpts := []lockdclient.Option{
		lockdclient.WithHTTPTimeout(60 * time.Second),
		lockdclient.WithCloseTimeout(60 * time.Second),
		lockdclient.WithKeepAliveTimeout(60 * time.Second),
		lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
	}

	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", addr),
		loggerOption,
		lockd.WithTestClientOptions(clientOpts...),
		lockd.WithTestTCLeaderLeaseTTL(leaseTTL),
	}
	if gate != nil {
		options = append(options, lockd.WithTestTCFanoutGate(gate.Hook))
	}
	if len(creds) > 0 && creds[0].Valid() {
		options = append(options, cryptotest.SharedMTLSOptions(tb, creds[0])...)
	} else {
		options = append(options, cryptotest.SharedMTLSOptions(tb)...)
	}
	return lockd.StartTestServer(tb, options...)
}

func attachHAClient(t testing.TB, active *lockd.TestServer, servers ...*lockd.TestServer) *lockdclient.Client {
	t.Helper()
	if len(servers) == 0 {
		t.Fatalf("ha client: servers required")
	}
	primary := active
	if primary == nil {
		primary = servers[0]
	}
	if primary == nil {
		t.Fatalf("ha client: primary server missing")
	}
	endpoints := make([]string, 0, len(servers))
	endpoints = append(endpoints, primary.URL())
	for _, ts := range servers {
		if ts == nil || ts == primary {
			continue
		}
		endpoints = append(endpoints, ts.URL())
	}
	cli, err := primary.NewEndpointsClient(endpoints, lockdclient.WithEndpointShuffle(false))
	if err != nil {
		t.Fatalf("ha client: %v", err)
	}
	for _, ts := range servers {
		if ts != nil {
			ts.Client = cli
		}
	}
	return cli
}
