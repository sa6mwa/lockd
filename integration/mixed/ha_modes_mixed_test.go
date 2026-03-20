//go:build integration && mixed

package mixedintegration

import (
	"context"
	"path"
	"testing"
	"time"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/hatest"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/pslog"
)

func TestMixedHASingleModeFencesAutoPeerOnDisk(t *testing.T) {
	cfgSingle := loadDiskConfig(t)
	cfgSingle.HAMode = "single"
	single := startMixedHATestServer(t, cfgSingle)

	hatest.RequireNoHALease(t, single.Backend())
	hatest.RequireNoHAMembers(t, single.Backend())

	cfgAuto := cfgSingle
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 2 * time.Second
	auto := startMixedHATestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 20*time.Second)
	hatest.RequireNoHALease(t, auto.Backend())
	hatest.RequireAcquireRelease(t, single.Client, "mixed-disk-ha-single-fence")
}

func TestMixedHASingleModeAdvertisesForMinio(t *testing.T) {
	cfgSingle := loadMinioConfig(t)
	cfgSingle.Store = appendStorePath(t, cfgSingle.Store, path.Join("mixed-ha", uuidv7.NewString(), "single-auto"))
	cfgSingle.HAMode = "single"
	cfgSingle.HASinglePresenceTTL = 5 * time.Second
	ensureMinioBucket(t, cfgSingle)
	single := startMixedHATestServer(t, cfgSingle)

	hatest.RequireNoHALease(t, single.Backend())
	hatest.WaitForHAMemberMode(t, single.Backend(), "single", 30*time.Second)

	cfgAuto := cfgSingle
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startMixedHATestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 30*time.Second)
	hatest.RequireNoHALease(t, auto.Backend())
	hatest.RequireAcquireRelease(t, single.Client, "mixed-minio-ha-single-fence")
}

func TestMixedHASingleCrashExpiryPromotesAutoAndFencesRejoinOnDisk(t *testing.T) {
	cfgSingle := loadDiskConfig(t)
	cfgSingle.HAMode = "single"
	single := startMixedHATestServer(t, cfgSingle)

	cfgAuto := cfgSingle
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 2 * time.Second
	auto := startMixedHATestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 20*time.Second)
	if err := single.Abort(context.Background()); err != nil {
		t.Fatalf("abort single: %v", err)
	}

	activeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := hatest.WaitForActive(activeCtx, auto); err != nil {
		t.Fatalf("wait for auto active: %v", err)
	}

	rejoin := startMixedHATestServer(t, cfgSingle)
	hatest.WaitForPassiveNode(t, rejoin, 20*time.Second)
	hatest.RequireNoHAMembers(t, rejoin.Backend())
}

func TestMixedHASingleCrashExpiryPromotesAutoAndFencesRejoinOnMinio(t *testing.T) {
	cfgSingle := loadMinioConfig(t)
	cfgSingle.Store = appendStorePath(t, cfgSingle.Store, path.Join("mixed-ha", uuidv7.NewString(), "single-auto-crash"))
	cfgSingle.HAMode = "single"
	cfgSingle.HASinglePresenceTTL = 5 * time.Second
	ensureMinioBucket(t, cfgSingle)
	single := startMixedHATestServer(t, cfgSingle)

	cfgAuto := cfgSingle
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startMixedHATestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 30*time.Second)
	if err := single.Abort(context.Background()); err != nil {
		t.Fatalf("abort single: %v", err)
	}

	activeCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := hatest.WaitForActive(activeCtx, auto); err != nil {
		t.Fatalf("wait for auto active: %v", err)
	}

	rejoin := startMixedHATestServer(t, cfgSingle)
	hatest.WaitForPassiveNode(t, rejoin, 30*time.Second)
	hatest.RequireNoLiveHAMemberMode(t, rejoin.Backend(), "single")
}

func startMixedHATestServer(tb testing.TB, cfg lockd.Config) *lockd.TestServer {
	tb.Helper()
	logger := lockd.NewTestingLogger(tb, pslog.TraceLevel)
	opts := []lockd.TestServerOption{
		lockd.WithTestConfig(cfg),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestLogger(logger),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithLogger(lockd.NewTestingLogger(tb, pslog.TraceLevel)),
		),
		lockd.WithTestCloseDefaults(
			lockd.WithDrainLeases(-1),
			lockd.WithShutdownTimeout(10*time.Second),
		),
	}
	opts = append(opts, cryptotest.SharedMTLSOptions(tb)...)
	return lockd.StartTestServer(tb, opts...)
}
