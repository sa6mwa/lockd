package client_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	lockd "pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
)

func ExampleClient_AcquireForUpdate() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "lockd-example-")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer os.RemoveAll(dir)

	socketPath := filepath.Join(dir, "lockd.sock")
	cfg := lockd.Config{
		Store:       "mem://",
		ListenProto: "unix",
		Listen:      socketPath,
		MTLS:        false,
	}
	_, stop, err := lockd.StartServer(ctx, cfg)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer stop(context.Background())

	cli, err := client.New("unix://" + socketPath)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	err = cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        "example",
		Owner:      "worker-1",
		TTLSeconds: 10,
	}, func(ctx context.Context, af *client.AcquireForUpdateContext) error {
		// Save replaces the JSON state while the lease is held.
		return af.Save(ctx, map[string]any{"counter": 1})
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("state updated")
	// Output: state updated
}
