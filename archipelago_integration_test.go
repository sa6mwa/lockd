//go:build integration
// +build integration

package lockd_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rs/xid"
	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/archipelagotest"
	"pkt.systems/lockd/internal/clock"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/namespaces"
)

func TestArchipelagoMemLeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("archipelago regression test skipped with -short")
	}

	leaseTTL := 3 * time.Second
	backendA := memorybackend.NewWithConfig(memorybackend.Config{})
	backendB := memorybackend.NewWithConfig(memorybackend.Config{})

	baseCfg := lockd.Config{
		Store:           "mem://",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    2 * time.Second,
		SweeperInterval: time.Second,
		DisableMTLS:     false,
	}

	tcAuth := newTCAuthority(t)
	mtls := newSharedMTLSAuthority(t)

	scheme := "https"
	addrsA := reserveAddrs(t, 2)
	addrsB := reserveAddrs(t, 2)
	endpoints := append(endpointsFromAddrs(scheme, addrsA), endpointsFromAddrs(scheme, addrsB)...)
	baseCfg.TCJoinEndpoints = endpoints

	tcA1 := startMemNode(t, baseCfg, tcAuth, mtls, backendA, addrsA[0], scheme, leaseTTL, nil)
	tcA2 := startMemNode(t, baseCfg, tcAuth, mtls, backendA, addrsA[1], scheme, leaseTTL, nil)
	tcB1 := startMemNode(t, baseCfg, tcAuth, mtls, backendB, addrsB[0], scheme, leaseTTL, nil)
	tcB2 := startMemNode(t, baseCfg, tcAuth, mtls, backendB, addrsB[1], scheme, leaseTTL, nil)

	tcs := []*lockd.TestServer{tcA1, tcA2, tcB1, tcB2}
	httpClient := serverHTTPClient

	archipelagotest.JoinCluster(t, tcs, endpoints, httpClient, 10*time.Second)

	archipelagotest.RegisterRM(t, tcA1, tcA1, httpClient)
	archipelagotest.RegisterRM(t, tcA1, tcA2, httpClient)
	archipelagotest.RegisterRM(t, tcA1, tcB1, httpClient)
	archipelagotest.RegisterRM(t, tcA1, tcB2, httpClient)

	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, httpClient, expectedRMs, 15*time.Second)

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		cli, err := ts.NewEndpointsClient([]string{ts.URL()}, lockdclient.WithEndpointShuffle(false))
		if err != nil {
			t.Fatalf("tc client: %v", err)
		}
		return cli
	}

	archipelagotest.RunLeaderFailoverScenario(t, tcs, tcA1, tcB1, httpClient, httpClient, tcClient, nil)
}

func TestArchipelagoMemLeaderFailoverDeterministicClock(t *testing.T) {
	if testing.Short() {
		t.Skip("archipelago regression test skipped with -short")
	}

	clk := clock.NewManual(time.Now().UTC().Add(10 * time.Minute))

	leaseTTL := 3 * time.Second
	backendA := memorybackend.NewWithConfig(memorybackend.Config{})
	backendB := memorybackend.NewWithConfig(memorybackend.Config{})

	baseCfg := lockd.Config{
		Store:           "mem://",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    2 * time.Second,
		SweeperInterval: time.Second,
		DisableMTLS:     false,
	}

	scheme := "https"
	tcAuth := newTCAuthority(t)
	mtls := newSharedMTLSAuthority(t)
	addrsA := reserveAddrs(t, 2)
	addrsB := reserveAddrs(t, 2)
	endpointsA := endpointsFromAddrs(scheme, addrsA)
	endpointsB := endpointsFromAddrs(scheme, addrsB)
	baseCfg.TCJoinEndpoints = []string{endpointsA[0], endpointsB[0]}
	islandA, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendA, scheme, leaseTTL, addrsA, clk)
	islandB, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendB, scheme, leaseTTL, addrsB, clk)

	tcs := append([]*lockd.TestServer{}, islandA...)
	tcs = append(tcs, islandB...)
	endpoints := append([]string{}, endpointsA...)
	endpoints = append(endpoints, endpointsB...)

	httpClient := serverHTTPClient

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		cli, err := ts.NewEndpointsClient([]string{ts.URL()}, lockdclient.WithEndpointShuffle(false))
		if err != nil {
			t.Fatalf("tc client: %v", err)
		}
		return cli
	}

	archipelagotest.JoinCluster(t, tcs, endpoints, httpClient, 10*time.Second)

	for _, rm := range tcs {
		archipelagotest.RegisterRM(t, islandA[0], rm, httpClient)
	}
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, httpClient, expectedRMs, 15*time.Second)

	leaderEndpoint, _ := waitForLeaderChangeWithClock(t, tcs, httpClient, clk, "", 50*time.Millisecond, 20*time.Second)
	waitForLeaderOnWithClock(t, islandA[0], httpClient, clk, leaderEndpoint, 50*time.Millisecond, 20*time.Second)
	waitForLeaderOnWithClock(t, islandB[0], httpClient, clk, leaderEndpoint, 50*time.Millisecond, 20*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("archi-det-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("archi-det-b-%d", time.Now().UnixNano())

	clientA := tcClient(t, islandA[0])
	clientB := tcClient(t, islandB[0])
	lease0, err := clientA.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "archipelago-det",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire A: %v", err)
	}
	if err := lease0.Save(ctx, map[string]any{"value": "det-a"}); err != nil {
		t.Fatalf("scenario: save A: %v", err)
	}

	lease1, err := clientB.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "archipelago-det",
		TTLSeconds: 30,
		TxnID:      lease0.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire B: %v", err)
	}
	if err := lease1.Save(ctx, map[string]any{"value": "det-b"}); err != nil {
		t.Fatalf("scenario: save B: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = leaderTS.Stop(stopCtx)
	stopCancel()

	alive := filterServers(tcs, leaderTS)
	aliveA := filterServers(islandA, leaderTS)
	if len(aliveA) == 0 {
		t.Fatalf("scenario: no alive servers in island A")
	}
	aliveB := filterServers(islandB, leaderTS)
	if len(aliveB) == 0 {
		t.Fatalf("scenario: no alive servers in island B")
	}
	newLeader, _ := waitForLeaderChangeWithClock(t, alive, httpClient, clk, leaderEndpoint, 50*time.Millisecond, 20*time.Second)
	nonLeader := archipelagotest.NonLeader(alive, newLeader)
	if nonLeader == nil {
		nonLeader = serverByEndpoint(alive, newLeader)
	}
	if nonLeader == nil {
		t.Fatalf("scenario: non-leader not found")
	}
	waitForLeaderOnWithClock(t, nonLeader, httpClient, clk, newLeader, 50*time.Millisecond, 20*time.Second)

	participants := []api.TxnParticipant{
		{Namespace: namespaces.Default, Key: keyA, BackendHash: backendHash(t, islandA[0])},
		{Namespace: namespaces.Default, Key: keyB, BackendHash: backendHash(t, islandB[0])},
	}
	if _, err := tcClient(t, nonLeader).TxnCommit(ctx, api.TxnDecisionRequest{
		TxnID:        lease0.TxnID,
		Participants: participants,
	}); err != nil {
		t.Fatalf("scenario: txn commit: %v", err)
	}

	archipelagotest.WaitForState(t, tcClient(t, aliveA[0]), keyA, true, 20*time.Second)
	archipelagotest.WaitForState(t, tcClient(t, aliveB[0]), keyB, true, 20*time.Second)
}

func TestArchipelagoMemMultiNodePendingFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("archipelago regression test skipped with -short")
	}

	leaseTTL := 9 * time.Second
	backendA := memorybackend.NewWithConfig(memorybackend.Config{})
	backendB := memorybackend.NewWithConfig(memorybackend.Config{})

	baseCfg := lockd.Config{
		Store:           "mem://",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    2 * time.Second,
		SweeperInterval: time.Second,
		DisableMTLS:     false,
	}

	scheme := "https"
	tcAuth := newTCAuthority(t)
	mtls := newSharedMTLSAuthority(t)
	addrsA := reserveAddrs(t, 3)
	addrsB := reserveAddrs(t, 2)
	endpointsA := endpointsFromAddrs(scheme, addrsA)
	endpointsB := endpointsFromAddrs(scheme, addrsB)
	baseCfg.TCJoinEndpoints = []string{endpointsA[0], endpointsB[0]}
	islandA, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendA, scheme, leaseTTL, addrsA, nil)
	islandB, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendB, scheme, leaseTTL, addrsB, nil)

	tcs := append([]*lockd.TestServer{}, islandA...)
	tcs = append(tcs, islandB...)
	endpoints := append([]string{}, endpointsA...)
	endpoints = append(endpoints, endpointsB...)

	httpClient := serverHTTPClient

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		cli, err := ts.NewEndpointsClient([]string{ts.URL()}, lockdclient.WithEndpointShuffle(false))
		if err != nil {
			t.Fatalf("tc client: %v", err)
		}
		return cli
	}

	archipelagotest.JoinCluster(t, tcs, endpoints, httpClient, 10*time.Second)

	for _, rm := range tcs {
		archipelagotest.RegisterRM(t, islandA[0], rm, httpClient)
	}
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, httpClient, expectedRMs, 15*time.Second)

	tcA, tcB, setupLeader := waitForLeaderPair(t, islandA, islandB, httpClient, 20*time.Second)
	tcPendingA := firstOther(islandA, tcA)
	if tcPendingA == nil {
		tcPendingA = tcA
	} else if leader, _, ok := waitForLeaderOnOptional(t, tcPendingA, httpClient, 5*time.Second); !ok || leader != setupLeader {
		t.Logf("pending A server %s did not observe leader %s; using %s", tcPendingA.URL(), setupLeader, tcA.URL())
		tcPendingA = tcA
	}
	tcPendingB := firstOther(islandB, tcB)
	if tcPendingB == nil {
		tcPendingB = tcB
	} else if leader, _, ok := waitForLeaderOnOptional(t, tcPendingB, httpClient, 5*time.Second); !ok || leader != setupLeader {
		t.Logf("pending B server %s did not observe leader %s; using %s", tcPendingB.URL(), setupLeader, tcB.URL())
		tcPendingB = tcB
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	scenarioTTLSeconds := int64(30)
	keyA := fmt.Sprintf("archi-main-a-%d", time.Now().UnixNano())
	keyB := fmt.Sprintf("archi-main-b-%d", time.Now().UnixNano())

	lease0, err := tcClient(t, tcA).Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "archipelago",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire main A: %v", err)
	}
	if err := lease0.Save(ctx, map[string]any{"value": "island-a"}); err != nil {
		t.Fatalf("scenario: save main A: %v", err)
	}

	lease1, err := tcClient(t, tcB).Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "archipelago",
		TTLSeconds: scenarioTTLSeconds,
		TxnID:      lease0.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire main B: %v", err)
	}
	if err := lease1.Save(ctx, map[string]any{"value": "island-b"}); err != nil {
		t.Fatalf("scenario: save main B: %v", err)
	}

	pendingKeyA := fmt.Sprintf("archi-pending-a-%d", time.Now().UnixNano())
	pendingA, err := tcClient(t, tcPendingA).Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        pendingKeyA,
		Owner:      "archipelago-pending",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire pending A: %v", err)
	}
	if err := pendingA.Save(ctx, map[string]any{"value": "pending-a"}); err != nil {
		t.Fatalf("scenario: save pending A: %v", err)
	}

	pendingKeyB := fmt.Sprintf("archi-pending-b-%d", time.Now().UnixNano())
	pendingB, err := tcClient(t, tcPendingB).Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        pendingKeyB,
		Owner:      "archipelago-pending",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("scenario: acquire pending B: %v", err)
	}
	if err := pendingB.Save(ctx, map[string]any{"value": "pending-b"}); err != nil {
		t.Fatalf("scenario: save pending B: %v", err)
	}

	leaderEndpoint, _ := archipelagotest.WaitForLeader(t, tcs, httpClient, 20*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = leaderTS.Stop(stopCtx)
	stopCancel()

	alive := filterServers(tcs, leaderTS)
	newLeader, _ := archipelagotest.WaitForLeaderChange(t, alive, httpClient, leaderEndpoint, 20*time.Second)
	tcAfterFailover, _, _ := waitForLeaderOnAny(t, islandB, httpClient, newLeader, 20*time.Second)

	newKey := fmt.Sprintf("archi-new-%d", time.Now().UnixNano())
	if _, err := tcClient(t, tcAfterFailover).Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        newKey,
		Owner:      "archipelago-new",
		TTLSeconds: scenarioTTLSeconds,
		BlockSecs:  lockdclient.BlockWaitForever,
	}); err != nil {
		t.Fatalf("scenario: acquire new txn: %v", err)
	}

	leaderNow := serverByEndpoint(alive, newLeader)
	if leaderNow == nil {
		t.Fatalf("scenario: leader %q not found in alive set", newLeader)
	}
	nonLeaderCandidates := filterServers(alive, leaderNow)
	nonLeader, _, _ := waitForLeaderOnAny(t, nonLeaderCandidates, httpClient, newLeader, 20*time.Second)
	hashA := backendHash(t, tcA)
	hashB := backendHash(t, tcB)
	participants := []api.TxnParticipant{
		{Namespace: namespaces.Default, Key: keyA, BackendHash: hashA},
		{Namespace: namespaces.Default, Key: keyB, BackendHash: hashB},
	}
	if _, err := tcClient(t, nonLeader).TxnCommit(ctx, api.TxnDecisionRequest{
		TxnID:        lease0.TxnID,
		Participants: participants,
	}); err != nil {
		t.Fatalf("scenario: txn commit: %v", err)
	}

	aliveIslandA := filterServers(islandA, leaderTS)
	if len(aliveIslandA) == 0 {
		t.Fatalf("scenario: no alive servers in island A after leader stop")
	}
	aliveIslandB := filterServers(islandB, leaderTS)
	if len(aliveIslandB) == 0 {
		t.Fatalf("scenario: no alive servers in island B after leader stop")
	}

	archipelagotest.WaitForState(t, tcClient(t, aliveIslandA[0]), keyA, true, 20*time.Second)
	archipelagotest.WaitForState(t, tcClient(t, aliveIslandB[0]), keyB, true, 20*time.Second)
}

func TestArchipelagoMemNonLeaderForwardUnavailable(t *testing.T) {
	if testing.Short() {
		t.Skip("archipelago regression test skipped with -short")
	}

	leaseTTL := 30 * time.Second
	backendA := memorybackend.NewWithConfig(memorybackend.Config{})
	backendB := memorybackend.NewWithConfig(memorybackend.Config{})

	baseCfg := lockd.Config{
		Store:           "mem://",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    2 * time.Second,
		SweeperInterval: time.Second,
		DisableMTLS:     false,
	}

	scheme := "https"
	tcAuth := newTCAuthority(t)
	mtls := newSharedMTLSAuthority(t)
	addrsA := reserveAddrs(t, 2)
	addrsB := reserveAddrs(t, 1)
	endpointsA := endpointsFromAddrs(scheme, addrsA)
	endpointsB := endpointsFromAddrs(scheme, addrsB)
	baseCfg.TCJoinEndpoints = []string{endpointsA[0], endpointsB[0]}
	islandA, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendA, scheme, leaseTTL, addrsA, nil)
	islandB, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendB, scheme, leaseTTL, addrsB, nil)

	tcs := append([]*lockd.TestServer{}, islandA...)
	tcs = append(tcs, islandB...)
	endpoints := append([]string{}, endpointsA...)
	endpoints = append(endpoints, endpointsB...)

	httpClient := serverHTTPClient

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		cli, err := ts.NewEndpointsClient([]string{ts.URL()}, lockdclient.WithEndpointShuffle(false))
		if err != nil {
			t.Fatalf("tc client: %v", err)
		}
		return cli
	}

	archipelagotest.JoinCluster(t, tcs, endpoints, httpClient, 10*time.Second)

	for _, rm := range tcs {
		archipelagotest.RegisterRM(t, islandA[0], rm, httpClient)
	}
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, httpClient, expectedRMs, 15*time.Second)

	leaderEndpoint, _ := archipelagotest.WaitForLeader(t, tcs, httpClient, 20*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}

	nonLeader := archipelagotest.NonLeader(tcs, leaderEndpoint)
	if nonLeader == nil {
		t.Fatalf("scenario: non-leader not found")
	}
	observedLeader, _ := waitForLeaderOn(t, nonLeader, httpClient, 10*time.Second)
	if observedLeader != leaderEndpoint {
		t.Fatalf("expected non-leader to observe %q, got %q", leaderEndpoint, observedLeader)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = leaderTS.Stop(stopCtx)
	stopCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := tcClient(t, nonLeader).TxnCommit(ctx, api.TxnDecisionRequest{
		TxnID: xid.New().String(),
	})
	var apiErr *lockdclient.APIError
	if err == nil {
		t.Fatalf("expected commit to fail with leader unavailable")
	}
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

func TestArchipelagoMemQuorumLossStepDown(t *testing.T) {
	if testing.Short() {
		t.Skip("archipelago regression test skipped with -short")
	}

	clk := clock.NewManual(time.Now().UTC().Add(10 * time.Minute))

	leaseTTL := 6 * time.Second
	backendA := memorybackend.NewWithConfig(memorybackend.Config{})
	backendB := memorybackend.NewWithConfig(memorybackend.Config{})

	baseCfg := lockd.Config{
		Store:           "mem://",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    2 * time.Second,
		SweeperInterval: time.Second,
		DisableMTLS:     false,
	}

	scheme := "https"
	tcAuth := newTCAuthority(t)
	mtls := newSharedMTLSAuthority(t)
	addrsA := reserveAddrs(t, 2)
	addrsB := reserveAddrs(t, 2)
	endpointsA := endpointsFromAddrs(scheme, addrsA)
	endpointsB := endpointsFromAddrs(scheme, addrsB)
	baseCfg.TCJoinEndpoints = []string{endpointsA[0], endpointsB[0]}
	islandA, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendA, scheme, leaseTTL, addrsA, clk)
	islandB, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendB, scheme, leaseTTL, addrsB, clk)

	tcs := append([]*lockd.TestServer{}, islandA...)
	tcs = append(tcs, islandB...)
	endpoints := append([]string{}, endpointsA...)
	endpoints = append(endpoints, endpointsB...)

	httpClient := serverHTTPClient

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		cli, err := ts.NewEndpointsClient([]string{ts.URL()}, lockdclient.WithEndpointShuffle(false))
		if err != nil {
			t.Fatalf("tc client: %v", err)
		}
		return cli
	}

	archipelagotest.JoinCluster(t, tcs, endpoints, httpClient, 10*time.Second)

	for _, rm := range tcs {
		archipelagotest.RegisterRM(t, islandA[0], rm, httpClient)
	}
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, httpClient, expectedRMs, 15*time.Second)

	leaderEndpoint, oldTerm := waitForLeaderWithClock(t, tcs, httpClient, clk, 50*time.Millisecond, 20*time.Second)
	leaderTS := serverByEndpoint(tcs, leaderEndpoint)
	if leaderTS == nil {
		t.Fatalf("scenario: leader %q not found", leaderEndpoint)
	}
	seedA := islandA[0]
	seedB := islandB[0]

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = leaderTS.Stop(stopCtx)
	stopCancel()

	down := []*lockd.TestServer{leaderTS}
	if extra := firstOther(islandA, leaderTS, seedA, seedB); extra != nil {
		down = append(down, extra)
	}
	if extra := firstOther(islandB, leaderTS, seedA, seedB); extra != nil {
		down = append(down, extra)
	}
	if len(down) < 3 {
		skips := append([]*lockd.TestServer{seedA, seedB}, down...)
		if extra := firstOther(tcs, leaderTS, skips...); extra != nil {
			down = append(down, extra)
		}
	}
	for _, ts := range down[1:] {
		if ts == nil {
			continue
		}
		stopCtx, stopCancel = context.WithTimeout(context.Background(), 5*time.Second)
		_ = ts.Stop(stopCtx)
		stopCancel()
	}

	alive := filterServers(tcs, down...)
	waitForNoLeaderWithClock(t, alive, httpClient, clk, 200*time.Millisecond, 15*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := tcClient(t, alive[0]).TxnCommit(ctx, api.TxnDecisionRequest{
		TxnID: xid.New().String(),
	})
	var apiErr *lockdclient.APIError
	if err == nil {
		t.Fatalf("expected commit to fail while leader unavailable")
	}
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected api error, got %v", err)
	}
	if apiErr.Response.ErrorCode != "tc_unavailable" {
		t.Fatalf("expected tc_unavailable, got %s", apiErr.Response.ErrorCode)
	}

	membershipTTL := tccluster.DeriveLeaseTTL(leaseTTL)
	waitForClusterMembersWithClock(t, alive, httpClient, endpointsForServers(alive), clk, membershipTTL/6, 20*time.Second)
	_, newTerm := waitForLeaderWithClock(t, alive, httpClient, clk, 50*time.Millisecond, 20*time.Second)
	if newTerm <= oldTerm {
		t.Fatalf("expected term to increase: old=%d new=%d", oldTerm, newTerm)
	}

	err = postTxnCommitWithTerm(t, alive[0], httpClient, xid.New().String(), oldTerm)
	if err == nil {
		t.Fatalf("expected tc_term_stale for old term")
	}
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected api error, got %v", err)
	}
	if apiErr.Response.ErrorCode != "tc_term_stale" {
		t.Fatalf("expected tc_term_stale, got %s", apiErr.Response.ErrorCode)
	}
}

func TestArchipelagoMemMembershipLeaveDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("archipelago regression test skipped with -short")
	}

	leaseTTL := 6 * time.Second
	backendA := memorybackend.NewWithConfig(memorybackend.Config{})
	backendB := memorybackend.NewWithConfig(memorybackend.Config{})

	baseCfg := lockd.Config{
		Store:           "mem://",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    2 * time.Second,
		SweeperInterval: time.Second,
		DisableMTLS:     false,
	}

	scheme := "https"
	tcAuth := newTCAuthority(t)
	mtls := newSharedMTLSAuthority(t)
	addrsA := reserveAddrs(t, 2)
	addrsB := reserveAddrs(t, 1)
	endpointsA := endpointsFromAddrs(scheme, addrsA)
	endpointsB := endpointsFromAddrs(scheme, addrsB)
	baseCfg.TCJoinEndpoints = append([]string{}, endpointsA...)
	baseCfg.TCJoinEndpoints = append(baseCfg.TCJoinEndpoints, endpointsB...)
	islandA, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendA, scheme, leaseTTL, addrsA, nil)
	islandB, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendB, scheme, leaseTTL, addrsB, nil)

	tcs := append([]*lockd.TestServer{}, islandA...)
	tcs = append(tcs, islandB...)
	endpoints := append([]string{}, endpointsA...)
	endpoints = append(endpoints, endpointsB...)

	httpClient := serverHTTPClient

	archipelagotest.JoinCluster(t, tcs, endpoints, httpClient, 10*time.Second)
	archipelagotest.WaitForClusterMembers(t, tcs, httpClient, endpoints, 20*time.Second)

	leaving := islandA[1]
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = leaving.Stop(stopCtx)
	stopCancel()

	alive := filterServers(tcs, leaving)
	archipelagotest.WaitForClusterMembers(t, alive, httpClient, endpointsForServers(alive), 10*time.Second)
}

func TestArchipelagoMemMembershipExpiryDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("archipelago regression test skipped with -short")
	}

	clk := clock.NewManual(time.Now().UTC().Add(10 * time.Minute))

	leaseTTL := 3 * time.Second
	backendA := memorybackend.NewWithConfig(memorybackend.Config{})
	backendB := memorybackend.NewWithConfig(memorybackend.Config{})

	baseCfg := lockd.Config{
		Store:           "mem://",
		ListenProto:     "tcp",
		DefaultTTL:      30 * time.Second,
		MaxTTL:          2 * time.Minute,
		AcquireBlock:    2 * time.Second,
		SweeperInterval: time.Second,
		DisableMTLS:     false,
	}

	scheme := "https"
	tcAuth := newTCAuthority(t)
	mtls := newSharedMTLSAuthority(t)
	addrsA := reserveAddrs(t, 2)
	addrsB := reserveAddrs(t, 1)
	endpointsA := endpointsFromAddrs(scheme, addrsA)
	endpointsB := endpointsFromAddrs(scheme, addrsB)
	baseCfg.TCJoinEndpoints = append([]string{}, endpointsA...)
	baseCfg.TCJoinEndpoints = append(baseCfg.TCJoinEndpoints, endpointsB...)
	islandA, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendA, scheme, leaseTTL, addrsA, clk)
	islandB, _ := startMemIsland(t, baseCfg, tcAuth, mtls, backendB, scheme, leaseTTL, addrsB, clk)

	tcs := append([]*lockd.TestServer{}, islandA...)
	tcs = append(tcs, islandB...)
	endpoints := append([]string{}, endpointsA...)
	endpoints = append(endpoints, endpointsB...)

	httpClient := serverHTTPClient

	archipelagotest.JoinCluster(t, tcs, endpoints, httpClient, 10*time.Second)
	waitForClusterMembersWithClock(t, tcs, httpClient, endpoints, clk, 200*time.Millisecond, 15*time.Second)

	down := islandA[0]
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = down.Abort(stopCtx)
	stopCancel()

	alive := filterServers(tcs, down)
	membershipTTL := tccluster.DeriveLeaseTTL(leaseTTL)
	waitForClusterMembersWithClock(t, alive, httpClient, endpointsForServers(alive), clk, membershipTTL/6, 15*time.Second)
}
