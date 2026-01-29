//go:build integration && aws && !lq && !query && !crypto

package awsintegration

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/archipelagotest"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

func TestAWSArchipelagoLeaderFailover(t *testing.T) {
	leaseTTL := 4 * time.Second
	baseCfg := loadAWSConfig(t)
	baseCfg.HAMode = "concurrent"
	cryptotest.ConfigureTCAuth(t, &baseCfg)
	baseCfg.TCClientBundlePath = cryptotest.SharedTCClientBundlePath(t)

	prefix := "archipelago-" + uuidv7.NewString()
	cfgA := baseCfg
	cfgB := baseCfg
	cfgA.Store = appendStorePath(t, baseCfg.Store, path.Join(prefix, "a"))
	cfgB.Store = appendStorePath(t, baseCfg.Store, path.Join(prefix, "b"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ensureStoreReady(t, ctx, cfgA)
	ensureStoreReady(t, ctx, cfgB)

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
	tcA1 := startAWSArchipelagoNode(t, cfgA, addrA1, scheme, leaseTTL, fanoutGate)
	tcA2 := startAWSArchipelagoNode(t, cfgA, addrA2, scheme, leaseTTL, fanoutGate)
	tcB1 := startAWSArchipelagoNode(t, cfgB, addrB1, scheme, leaseTTL, fanoutGate)
	tcB2 := startAWSArchipelagoNode(t, cfgB, addrB2, scheme, leaseTTL, fanoutGate)
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
				return startAWSArchipelagoNode(tb, cfgA, addrA2, scheme, leaseTTL, fanoutGate, credsA2)
			},
		},
		{
			Index: 3,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startAWSArchipelagoNode(tb, cfgB, addrB2, scheme, leaseTTL, fanoutGate, credsB2)
			},
		},
	}
	restartMap := map[int]archipelagotest.RestartSpec{
		0: {
			Index: 0,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startAWSArchipelagoNode(tb, cfgA, addrA1, scheme, leaseTTL, fanoutGate, credsA1)
			},
		},
		1: {
			Index: 1,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startAWSArchipelagoNode(tb, cfgA, addrA2, scheme, leaseTTL, fanoutGate, credsA2)
			},
		},
		2: {
			Index: 2,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startAWSArchipelagoNode(tb, cfgB, addrB1, scheme, leaseTTL, fanoutGate, credsB1)
			},
		},
		3: {
			Index: 3,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startAWSArchipelagoNode(tb, cfgB, addrB2, scheme, leaseTTL, fanoutGate, credsB2)
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
	archipelagotest.JoinCluster(t, tcs, endpoints, tcHTTP, 20*time.Second)

	cryptotest.RegisterRM(t, tcA1, tcA1)
	cryptotest.RegisterRM(t, tcA1, tcA2)
	cryptotest.RegisterRM(t, tcA1, tcB1)
	cryptotest.RegisterRM(t, tcA1, tcB2)

	hashA, err := tcA1.Backend().BackendHash(ctx)
	if err != nil {
		t.Fatalf("hash A: %v", err)
	}
	hashB, err := tcB1.Backend().BackendHash(ctx)
	if err != nil {
		t.Fatalf("hash B: %v", err)
	}
	archipelagotest.RequireRMRegistry(t, tcs, tcHTTP, []string{hashA, hashB})
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, tcHTTP, expectedRMs, 15*time.Second)

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		return cryptotest.RequireTCClient(t, ts, lockdclient.WithEndpointShuffle(false))
	}

	archipelagotest.RunLeaderDecisionFanoutInterruptedScenario(t, tcs, []*lockd.TestServer{tcA1, tcB1}, tcHTTP, tcClient, fanoutGate, restartMap)
	archipelagotest.RunQuorumLossDuringRenewScenario(t, tcs, []*lockd.TestServer{tcA1, tcB1}, restartSpecs, tcHTTP, tcClient)
	archipelagotest.RunLeaderFailoverScenario(t, tcs, tcA1, tcB1, tcHTTP, rmHTTP, tcClient, restartMap)
	archipelagotest.RunRMRegistryReplicationScenario(t, tcs, tcHTTP, rmHTTP, restartMap)
	archipelagotest.RunRMApplyTermFencingScenario(t, tcs, []*lockd.TestServer{tcA1, tcB1}, tcHTTP, restartMap)
	archipelagotest.RunQueueStateFailoverScenario(t, tcs, []*lockd.TestServer{tcA1, tcB1}, tcHTTP, tcClient, restartMap)
	archipelagotest.RunTCMembershipChurnScenario(t, tcs, tcHTTP)
	archipelagotest.RunNonLeaderForwardUnavailableScenario(t, tcs, []*lockd.TestServer{tcA1, tcB1}, tcHTTP, tcClient)
}

func startAWSArchipelagoNode(tb testing.TB, base lockd.Config, addr, scheme string, leaseTTL time.Duration, gate *archipelagotest.FanoutGate, creds ...lockd.TestMTLSCredentials) *lockd.TestServer {
	tb.Helper()
	cfg := base
	cfg.Listen = addr
	cfg.ListenProto = "tcp"
	cfg.SelfEndpoint = fmt.Sprintf("%s://%s", scheme, addr)

	logger := lockd.NewTestingLogger(tb, pslog.TraceLevel)
	opts := []lockd.TestServerOption{
		lockd.WithTestListener("tcp", addr),
		lockd.WithTestLogger(logger),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithLogger(logger),
		),
		lockd.WithTestTCLeaderLeaseTTL(leaseTTL),
	}
	if gate != nil {
		opts = append(opts, lockd.WithTestTCFanoutGate(gate.Hook))
	}
	if len(creds) > 0 && creds[0].Valid() {
		opts = append(opts, lockd.WithTestMTLSCredentials(creds[0]))
	}
	return startAWSTestServer(tb, cfg, opts...)
}
