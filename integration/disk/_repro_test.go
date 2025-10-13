//go:build integration && disk

package diskintegration

import (
    "context"
    "testing"
    "time"

    lockd "pkt.systems/lockd"
    lockdclient "pkt.systems/lockd/client"
    port "pkt.systems/logport"
)

func TestRepro(t *testing.T) {
    cfg := lockd.Config{
        Store:           diskStoreURL(t.TempDir()),
        MTLS:            false,
        Listen:          pickPort(t),
        ListenProto:     "tcp",
        DefaultTTL:      5 * time.Second,
        MaxTTL:          30 * time.Second,
        AcquireBlock:    2 * time.Second,
        SweeperInterval: 2 * time.Second,
    }
    ctx := context.Background()
    _, stop, err := lockd.StartServer(ctx, cfg, lockd.WithLogger(port.NoopLogger()))
    if err != nil {
        t.Fatalf("start server: %v", err)
    }
    defer func() {
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        _ = stop(shutdownCtx)
    }()

    cli, err := lockdclient.New("http://" + cfg.Listen)
    if err != nil {
        t.Fatalf("client: %v", err)
    }

    lease, err := cli.Acquire(ctx, lockdclient.AcquireRequest{Key: "foo", Owner: "worker", TTLSeconds: 5})
    if err != nil {
        t.Fatalf("acquire: %v", err)
    }

    if _, err := cli.UpdateStateBytes(ctx, "foo", lease.LeaseID, []byte(`{"cursor":1}`), lockdclient.UpdateStateOptions{IfVersion: "0"}); err != nil {
        t.Fatalf("update: %v", err)
    }
}
