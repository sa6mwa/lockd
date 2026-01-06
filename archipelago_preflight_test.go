package lockd_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/archipelagotest"
)

func TestTCJoinPreflightFailsWithoutReachableEndpoint(t *testing.T) {
	addr := archipelagotest.ReserveTCPAddr(t)
	peer := archipelagotest.ReserveTCPAddr(t)
	cfg := baseArchipelagoConfig()
	cfg.Listen = addr
	cfg.SelfEndpoint = fmt.Sprintf("https://%s", addr)
	cfg.TCJoinEndpoints = []string{fmt.Sprintf("https://%s", peer)}
	cfg.TCFanoutTimeout = 150 * time.Millisecond

	ts, err := lockd.NewTestServer(context.Background(),
		lockd.WithTestConfig(cfg),
		lockd.WithTestMTLS(),
		lockd.WithTestStartTimeout(2*time.Second),
	)
	if err == nil {
		defer ts.Stop(context.Background())
		t.Fatal("expected tc join preflight failure")
	}
}

func TestTCJoinRequiresMTLSForMultiNode(t *testing.T) {
	addr := archipelagotest.ReserveTCPAddr(t)
	peer := archipelagotest.ReserveTCPAddr(t)
	cfg := baseArchipelagoConfig()
	cfg.Listen = addr
	cfg.SelfEndpoint = fmt.Sprintf("http://%s", addr)
	cfg.TCJoinEndpoints = []string{fmt.Sprintf("http://%s", peer)}
	cfg.DisableMTLS = true

	ts, err := lockd.NewTestServer(context.Background(),
		lockd.WithTestConfig(cfg),
		lockd.WithTestStartTimeout(2*time.Second),
	)
	if err == nil {
		defer ts.Stop(context.Background())
		t.Fatal("expected tc join mTLS requirement failure")
	}
}
