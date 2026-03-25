//go:build integration && minio

package miniointegration

import (
	"context"
	"path"
	"testing"
	"time"

	"pkt.systems/lockd/integration/internal/hatest"
	"pkt.systems/lockd/internal/uuidv7"
)

func TestMinioHASingleModeSkipsActiveLease(t *testing.T) {
	cfg := loadMinioConfig(t)
	cfg.Store = appendStorePath(t, cfg.Store, path.Join("ha-modes", uuidv7.NewString(), "single"))
	cfg.HAMode = "single"
	cfg.HASinglePresenceTTL = 5 * time.Second

	ts := startMinioTestServer(t, cfg)
	hatest.RequireNoHALease(t, ts.Backend())
	hatest.WaitForHAMemberMode(t, ts.Backend(), "single", 30*time.Second)
	hatest.RequireAcquireRelease(t, ts.Client, "minio-ha-single")
}

func TestMinioHAAutoPromotesToFailover(t *testing.T) {
	cfgA := loadMinioConfig(t)
	cfgA.Store = appendStorePath(t, cfgA.Store, path.Join("ha-modes", uuidv7.NewString(), "auto"))
	cfgA.HAMode = "auto"
	cfgA.HALeaseTTL = 5 * time.Second
	serverA := startMinioTestServer(t, cfgA)

	hatest.RequireNoHALease(t, serverA.Backend())

	cfgB := cfgA
	serverB := startMinioTestServer(t, cfgB)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	active, cli, err := hatest.FindActiveServer(ctx, serverA, serverB)
	if err != nil {
		t.Fatalf("find active server: %v", err)
	}
	defer cli.Close()
	hatest.RequireAcquireRelease(t, cli, "minio-ha-auto")

	passive := serverA
	if active == serverA {
		passive = serverB
	}
	hatest.WaitForPassiveNode(t, passive, 30*time.Second)
}

func TestMinioHASingleModeFencesAutoPeer(t *testing.T) {
	cfgSingle := loadMinioConfig(t)
	cfgSingle.Store = appendStorePath(t, cfgSingle.Store, path.Join("ha-modes", uuidv7.NewString(), "single-auto"))
	cfgSingle.HAMode = "single"
	cfgSingle.HASinglePresenceTTL = 5 * time.Second
	single := startMinioTestServer(t, cfgSingle)

	hatest.RequireNoHALease(t, single.Backend())
	hatest.WaitForHAMemberMode(t, single.Backend(), "single", 30*time.Second)

	cfgAuto := cfgSingle
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startMinioTestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 30*time.Second)
	hatest.RequireNoHALease(t, auto.Backend())
	hatest.RequireAcquireRelease(t, single.Client, "minio-ha-single-fence")
}

func TestMinioHASingleCrashExpiryPromotesAutoAndFencesRejoin(t *testing.T) {
	cfgSingle := loadMinioConfig(t)
	cfgSingle.Store = appendStorePath(t, cfgSingle.Store, path.Join("ha-modes", uuidv7.NewString(), "single-auto-crash"))
	cfgSingle.HAMode = "single"
	cfgSingle.HASinglePresenceTTL = 5 * time.Second
	single := startMinioTestServer(t, cfgSingle)

	cfgAuto := cfgSingle
	cfgAuto.HAMode = "auto"
	cfgAuto.HALeaseTTL = 5 * time.Second
	auto := startMinioTestServer(t, cfgAuto)

	hatest.WaitForPassiveNode(t, auto, 30*time.Second)
	if err := single.Abort(context.Background()); err != nil {
		t.Fatalf("abort single: %v", err)
	}

	activeCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := hatest.WaitForActive(activeCtx, auto); err != nil {
		t.Fatalf("wait for auto active: %v", err)
	}

	rejoin := startMinioTestServer(t, cfgSingle)
	hatest.WaitForPassiveNode(t, rejoin, 30*time.Second)
	hatest.RequireNoLiveHAMemberMode(t, rejoin.Backend(), "single")
	hatest.RequireAcquireRelease(t, auto.Client, "minio-ha-auto-after-crash")
}
