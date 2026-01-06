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
	"pkt.systems/lockd/internal/archipelagotest"
	"pkt.systems/pslog"
)

func TestDiskArchipelagoLeaderFailover(t *testing.T) {
	leaseTTL := 10 * time.Second
	roots := []string{
		prepareDiskRoot(t, ""),
		prepareDiskRoot(t, ""),
		prepareDiskRoot(t, ""),
	}
	bundlePath := cryptotest.SharedTCClientBundlePath(t)
	configs := make([]lockd.Config, 0, len(roots))
	for _, root := range roots {
		cfg := buildDiskConfig(t, root, 0)
		cryptotest.ConfigureTCAuth(t, &cfg)
		cfg.TCClientBundlePath = bundlePath
		configs = append(configs, cfg)
	}

	scheme := "http"
	if cryptotest.TestMTLSEnabled() {
		scheme = "https"
	}

	addrsByConfig := make([][]string, 0, len(configs))
	endpoints := make([]string, 0, 6)
	for range configs {
		addrs := []string{
			archipelagotest.ReserveTCPAddr(t),
			archipelagotest.ReserveTCPAddr(t),
		}
		addrsByConfig = append(addrsByConfig, addrs)
		for _, addr := range addrs {
			endpoints = append(endpoints, fmt.Sprintf("%s://%s", scheme, addr))
		}
	}
	for i := range configs {
		configs[i].TCJoinEndpoints = append([]string{}, endpoints...)
	}

	tcs := make([]*lockd.TestServer, 0, 6)
	islands := make([]*lockd.TestServer, 0, len(configs))
	fanoutGate := archipelagotest.NewFanoutGate()
	restartSpecs := make([]archipelagotest.RestartSpec, 0, 3)
	restartMap := make(map[int]archipelagotest.RestartSpec, 6)
	for cfgIdx, cfg := range configs {
		addrs := addrsByConfig[cfgIdx]
		for i := 0; i < len(addrs); i++ {
			addr := addrs[i]
			ts := startDiskArchipelagoNode(t, cfg, addr, scheme, leaseTTL, fanoutGate)
			idx := len(tcs)
			cfgRef := cfg
			addrRef := addr
			credsRef := ts.TestMTLSCredentials()
			spec := archipelagotest.RestartSpec{
				Index: idx,
				Start: func(tb testing.TB) *lockd.TestServer {
					tb.Helper()
					return startDiskArchipelagoNode(tb, cfgRef, addrRef, scheme, leaseTTL, fanoutGate, credsRef)
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
	archipelagotest.JoinCluster(t, tcs, endpoints, tcHTTP, 25*time.Second)

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

	archipelagotest.RunLeaderDecisionFanoutInterruptedScenario(t, tcs, islands, tcHTTP, tcClient, fanoutGate, restartMap)
	archipelagotest.RunQuorumLossDuringRenewScenario(t, tcs, islands, restartSpecs, tcHTTP, tcClient)
	archipelagotest.RunLeaderFailoverScenarioMulti(t, tcs, islands, tcHTTP, rmHTTP, tcClient, restartMap)
	archipelagotest.RunRMRegistryReplicationScenario(t, tcs, tcHTTP, rmHTTP, restartMap)
	archipelagotest.RunRMApplyTermFencingScenario(t, tcs, islands, tcHTTP, restartMap)
	archipelagotest.RunQueueStateFailoverScenario(t, tcs, islands, tcHTTP, tcClient, restartMap)
	archipelagotest.RunTCMembershipChurnScenario(t, tcs, tcHTTP)
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
