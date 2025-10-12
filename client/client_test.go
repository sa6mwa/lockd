package client_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/client"
)

func TestUnixSocketClientLifecycle(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "lockd.sock")
	cfg := lockd.Config{Store: "mem://", ListenProto: "unix", Listen: socket, MTLS: false}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, stop, err := lockd.StartServer(ctx, cfg)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() {
		_ = stop(context.Background())
	})

	cli, err := client.New("unix://" + socket)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	aCtx, aCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer aCancel()
	lease, err := cli.Acquire(aCtx, client.AcquireRequest{Key: "unix-test", Owner: "tester", TTLSeconds: 10, BlockSecs: 1})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if _, err := cli.Release(context.Background(), client.ReleaseRequest{Key: "unix-test", LeaseID: lease.LeaseID}); err != nil {
		t.Fatalf("release: %v", err)
	}
}
