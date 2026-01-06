//go:build integration && mixed

package mixedintegration

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/archipelagotest"
	memorybackend "pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

func TestMixedArchipelagoLeaderFailover(t *testing.T) {
	leaseTTL := 6 * time.Second
	memCfg := buildMixedMemConfig(t)
	diskCfg := loadDiskConfig(t)
	minioCfg := loadMinioConfig(t)
	awsCfg := loadMixedAWSConfig(t)
	azureCfg := loadMixedAzureConfig(t)
	cryptotest.ConfigureTCAuth(t, &memCfg)
	cryptotest.ConfigureTCAuth(t, &diskCfg)
	cryptotest.ConfigureTCAuth(t, &minioCfg)
	cryptotest.ConfigureTCAuth(t, &awsCfg)
	cryptotest.ConfigureTCAuth(t, &azureCfg)
	tcBundle := cryptotest.SharedTCClientBundlePath(t)
	memCfg.TCClientBundlePath = tcBundle
	diskCfg.TCClientBundlePath = tcBundle
	minioCfg.TCClientBundlePath = tcBundle
	awsCfg.TCClientBundlePath = tcBundle
	azureCfg.TCClientBundlePath = tcBundle

	prefix := "archipelago-" + uuidv7.NewString()
	minioCfg.Store = appendStorePath(t, minioCfg.Store, path.Join(prefix, "minio"))
	awsCfg.Store = appendStorePath(t, awsCfg.Store, path.Join(prefix, "aws"))
	azureCfg.Store = appendStorePath(t, azureCfg.Store, path.Join(prefix, "azure"))
	ensureMinioBucket(t, minioCfg)
	ensureStoreReady(t, awsCfg)
	ensureStoreReady(t, azureCfg)

	scheme := "http"
	if cryptotest.TestMTLSEnabled() {
		scheme = "https"
	}

	memBackend := memorybackend.NewWithConfig(memorybackend.Config{})
	addrMem1 := archipelagotest.ReserveTCPAddr(t)
	addrMem2 := archipelagotest.ReserveTCPAddr(t)
	addrDisk1 := archipelagotest.ReserveTCPAddr(t)
	addrDisk2 := archipelagotest.ReserveTCPAddr(t)
	addrMinio1 := archipelagotest.ReserveTCPAddr(t)
	addrMinio2 := archipelagotest.ReserveTCPAddr(t)
	addrAWS1 := archipelagotest.ReserveTCPAddr(t)
	addrAzure1 := archipelagotest.ReserveTCPAddr(t)
	endpoints := []string{
		fmt.Sprintf("%s://%s", scheme, addrMem1),
		fmt.Sprintf("%s://%s", scheme, addrMem2),
		fmt.Sprintf("%s://%s", scheme, addrDisk1),
		fmt.Sprintf("%s://%s", scheme, addrDisk2),
		fmt.Sprintf("%s://%s", scheme, addrMinio1),
		fmt.Sprintf("%s://%s", scheme, addrMinio2),
		fmt.Sprintf("%s://%s", scheme, addrAWS1),
		fmt.Sprintf("%s://%s", scheme, addrAzure1),
	}
	memCfg.TCJoinEndpoints = append([]string{}, endpoints...)
	diskCfg.TCJoinEndpoints = append([]string{}, endpoints...)
	minioCfg.TCJoinEndpoints = append([]string{}, endpoints...)
	awsCfg.TCJoinEndpoints = append([]string{}, endpoints...)
	azureCfg.TCJoinEndpoints = append([]string{}, endpoints...)

	fanoutGate := archipelagotest.NewFanoutGate()
	mem1 := startMixedMemNode(t, memCfg, memBackend, addrMem1, scheme, leaseTTL, fanoutGate)
	mem2 := startMixedMemNode(t, memCfg, memBackend, addrMem2, scheme, leaseTTL, fanoutGate)
	disk1 := startMixedDiskNode(t, diskCfg, addrDisk1, scheme, leaseTTL, fanoutGate)
	disk2 := startMixedDiskNode(t, diskCfg, addrDisk2, scheme, leaseTTL, fanoutGate)
	minio1 := startMixedMinioNode(t, minioCfg, addrMinio1, scheme, leaseTTL, fanoutGate)
	minio2 := startMixedMinioNode(t, minioCfg, addrMinio2, scheme, leaseTTL, fanoutGate)
	aws1 := startMixedAWSNode(t, awsCfg, addrAWS1, scheme, leaseTTL, fanoutGate)
	azure1 := startMixedAzureNode(t, azureCfg, addrAzure1, scheme, leaseTTL, fanoutGate)
	credsMem1 := mem1.TestMTLSCredentials()
	credsMem2 := mem2.TestMTLSCredentials()
	credsDisk1 := disk1.TestMTLSCredentials()
	credsDisk2 := disk2.TestMTLSCredentials()
	credsMinio1 := minio1.TestMTLSCredentials()
	credsMinio2 := minio2.TestMTLSCredentials()
	credsAWS1 := aws1.TestMTLSCredentials()
	credsAzure1 := azure1.TestMTLSCredentials()

	tcs := []*lockd.TestServer{
		mem1,
		mem2,
		disk1,
		disk2,
		minio1,
		minio2,
		aws1,
		azure1,
	}
	restartSpecs := []archipelagotest.RestartSpec{
		{
			Index: 1,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedMemNode(tb, memCfg, memBackend, addrMem2, scheme, leaseTTL, fanoutGate, credsMem2)
			},
		},
		{
			Index: 3,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedDiskNode(tb, diskCfg, addrDisk2, scheme, leaseTTL, fanoutGate, credsDisk2)
			},
		},
		{
			Index: 5,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedMinioNode(tb, minioCfg, addrMinio2, scheme, leaseTTL, fanoutGate, credsMinio2)
			},
		},
		{
			Index: 6,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedAWSNode(tb, awsCfg, addrAWS1, scheme, leaseTTL, fanoutGate, credsAWS1)
			},
		},
	}
	restartMap := map[int]archipelagotest.RestartSpec{
		0: {
			Index: 0,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedMemNode(tb, memCfg, memBackend, addrMem1, scheme, leaseTTL, fanoutGate, credsMem1)
			},
		},
		1: {
			Index: 1,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedMemNode(tb, memCfg, memBackend, addrMem2, scheme, leaseTTL, fanoutGate, credsMem2)
			},
		},
		2: {
			Index: 2,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedDiskNode(tb, diskCfg, addrDisk1, scheme, leaseTTL, fanoutGate, credsDisk1)
			},
		},
		3: {
			Index: 3,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedDiskNode(tb, diskCfg, addrDisk2, scheme, leaseTTL, fanoutGate, credsDisk2)
			},
		},
		4: {
			Index: 4,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedMinioNode(tb, minioCfg, addrMinio1, scheme, leaseTTL, fanoutGate, credsMinio1)
			},
		},
		5: {
			Index: 5,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedMinioNode(tb, minioCfg, addrMinio2, scheme, leaseTTL, fanoutGate, credsMinio2)
			},
		},
		6: {
			Index: 6,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedAWSNode(tb, awsCfg, addrAWS1, scheme, leaseTTL, fanoutGate, credsAWS1)
			},
		},
		7: {
			Index: 7,
			Start: func(tb testing.TB) *lockd.TestServer {
				tb.Helper()
				return startMixedAzureNode(tb, azureCfg, addrAzure1, scheme, leaseTTL, fanoutGate, credsAzure1)
			},
		},
	}
	registryIslands := []*lockd.TestServer{mem1, disk1, minio1, aws1, azure1}
	txnIslands := []*lockd.TestServer{mem1, disk1, minio1}

	tcHTTP := func(t testing.TB, ts *lockd.TestServer) *http.Client {
		t.Helper()
		return cryptotest.RequireServerHTTPClient(t, ts)
	}
	rmHTTP := func(t testing.TB, ts *lockd.TestServer) *http.Client {
		t.Helper()
		return cryptotest.RequireServerHTTPClient(t, ts)
	}
	archipelagotest.JoinCluster(t, tcs, endpoints, tcHTTP, 30*time.Second)
	archipelagotest.WaitForLeaderAll(t, tcs, tcHTTP, 45*time.Second)

	for _, rm := range tcs {
		cryptotest.RegisterRMWithTimeout(t, tcs[0], rm, 60*time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hashes := make([]string, 0, len(registryIslands))
	for idx, island := range registryIslands {
		hash, err := island.Backend().BackendHash(ctx)
		if err != nil {
			t.Fatalf("hash %d: %v", idx, err)
		}
		hashes = append(hashes, hash)
	}
	archipelagotest.RequireRMRegistry(t, tcs, tcHTTP, hashes)
	expectedRMs := archipelagotest.ExpectedRMEndpoints(t, tcs)
	archipelagotest.WaitForRMRegistry(t, tcs, tcHTTP, expectedRMs, 45*time.Second)

	tcClient := func(t testing.TB, ts *lockd.TestServer) *lockdclient.Client {
		t.Helper()
		return cryptotest.RequireTCClient(t, ts, lockdclient.WithEndpointShuffle(false))
	}

	archipelagotest.RunTCMembershipChurnScenario(t, tcs, tcHTTP)
	archipelagotest.RunLeaderDecisionFanoutInterruptedScenario(t, tcs, txnIslands, tcHTTP, tcClient, fanoutGate, restartMap)
	archipelagotest.RunQuorumLossDuringRenewScenario(t, tcs, txnIslands, restartSpecs, tcHTTP, tcClient)
	archipelagotest.RunLeaderFailoverScenarioMulti(t, tcs, txnIslands, tcHTTP, rmHTTP, tcClient, restartMap)
	archipelagotest.RunAttachmentTxnScenario(t, tcs, txnIslands, tcHTTP, tcClient)
	archipelagotest.RunRMRegistryReplicationScenario(t, tcs, tcHTTP, rmHTTP, restartMap)
	archipelagotest.RunRMApplyTermFencingScenario(t, tcs, txnIslands, tcHTTP, restartMap)
	archipelagotest.RunQueueStateFailoverScenario(t, tcs, txnIslands, tcHTTP, tcClient, restartMap)
	archipelagotest.RunNonLeaderForwardUnavailableScenario(t, tcs, txnIslands, tcHTTP, tcClient)
}

func startMixedDiskNode(tb testing.TB, base lockd.Config, addr, scheme string, leaseTTL time.Duration, gate *archipelagotest.FanoutGate, creds ...lockd.TestMTLSCredentials) *lockd.TestServer {
	tb.Helper()
	cfg := base
	cfg.Listen = addr
	cfg.ListenProto = "tcp"
	cfg.SelfEndpoint = fmt.Sprintf("%s://%s", scheme, addr)

	logger := lockd.NewTestingLogger(tb, pslog.TraceLevel)
	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
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
		opts = append(opts, cryptotest.SharedMTLSOptions(tb, creds[0])...)
	} else {
		opts = append(opts, cryptotest.SharedMTLSOptions(tb)...)
	}
	return lockd.StartTestServer(tb, opts...)
}

func startMixedMinioNode(tb testing.TB, base lockd.Config, addr, scheme string, leaseTTL time.Duration, gate *archipelagotest.FanoutGate, creds ...lockd.TestMTLSCredentials) *lockd.TestServer {
	tb.Helper()
	cfg := base
	cfg.Listen = addr
	cfg.ListenProto = "tcp"
	cfg.SelfEndpoint = fmt.Sprintf("%s://%s", scheme, addr)

	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", addr),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithFailureRetries(5),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
		lockd.WithTestTCLeaderLeaseTTL(leaseTTL),
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

func startMixedAWSNode(tb testing.TB, base lockd.Config, addr, scheme string, leaseTTL time.Duration, gate *archipelagotest.FanoutGate, creds ...lockd.TestMTLSCredentials) *lockd.TestServer {
	tb.Helper()
	cfg := base
	cfg.Listen = addr
	cfg.ListenProto = "tcp"
	cfg.SelfEndpoint = fmt.Sprintf("%s://%s", scheme, addr)

	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", addr),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithFailureRetries(5),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
		lockd.WithTestTCLeaderLeaseTTL(leaseTTL),
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

func startMixedAzureNode(tb testing.TB, base lockd.Config, addr, scheme string, leaseTTL time.Duration, gate *archipelagotest.FanoutGate, creds ...lockd.TestMTLSCredentials) *lockd.TestServer {
	tb.Helper()
	cfg := base
	cfg.Listen = addr
	cfg.ListenProto = "tcp"
	cfg.SelfEndpoint = fmt.Sprintf("%s://%s", scheme, addr)

	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", addr),
		lockd.WithTestLoggerFromTB(tb, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithForUpdateTimeout(2*time.Minute),
			lockdclient.WithFailureRetries(5),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
		lockd.WithTestTCLeaderLeaseTTL(leaseTTL),
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

func startMixedMemNode(tb testing.TB, base lockd.Config, backend *memorybackend.Store, addr, scheme string, leaseTTL time.Duration, gate *archipelagotest.FanoutGate, creds ...lockd.TestMTLSCredentials) *lockd.TestServer {
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

func appendStorePath(tb testing.TB, store, suffix string) string {
	tb.Helper()
	parsed, err := url.Parse(store)
	if err != nil {
		tb.Fatalf("parse store: %v", err)
	}
	pathPart := strings.Trim(strings.TrimPrefix(parsed.Path, "/"), "/")
	if pathPart == "" {
		pathPart = suffix
	} else {
		pathPart = path.Join(pathPart, suffix)
	}
	parsed.Path = "/" + pathPart
	return parsed.String()
}
