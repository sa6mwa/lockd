package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	lockd "pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
	"pkt.systems/lql"
)

func ExampleClient_Acquire() {
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
		DisableMTLS: true,
	}
	handle, err := lockd.StartServer(ctx, cfg)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	stop := handle.Stop
	defer stop(context.Background(), lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond))
	cli, err := client.New("unix://" + socketPath)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  "payments",
		Key:        "batch-2025-11",
		Owner:      "worker-1",
		TTLSeconds: 15,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer lease.Release(ctx)
	state := map[string]any{}
	if err := lql.Mutate(state,
		"/batch/id=batch-2025-11",
		"/batch/status=pending",
	); err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := lease.Save(ctx, state); err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("lease namespace:", lease.Namespace)
	fmt.Println("lease key:", lease.Key)
	// Output:
	// lease namespace: payments
	// lease key: batch-2025-11
}

func ExampleClient_Get_public() {
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
		DisableMTLS: true,
	}
	handle, err := lockd.StartServer(ctx, cfg)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	stop := handle.Stop
	defer stop(context.Background(), lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond))
	cli, err := client.New("unix://" + socketPath)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_ = cli.UseNamespace("reports")
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "payouts-2025-11",
		Owner:      "writer-1",
		TTLSeconds: 10,
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	doc := make(map[string]any)
	if err := lql.Mutate(doc,
		"/report/status=published",
		"time:/report/released_at=2025-11-01T09:00:00Z",
		"/report/summary/total=1200.50",
	); err != nil {
		fmt.Println("error:", err)
		return
	}
	if err := lease.Save(ctx, doc); err != nil {
		fmt.Println("error:", err)
		return
	}
	lease.Release(ctx)
	resp, err := cli.Get(ctx, "payouts-2025-11")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer resp.Close()
	var snapshot map[string]any
	if err := json.NewDecoder(resp.Reader()).Decode(&snapshot); err != nil {
		fmt.Println("error:", err)
		return
	}
	report := snapshot["report"].(map[string]any)
	fmt.Println("status:", report["status"])
	// Output: status: published
}

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
		DisableMTLS: true,
	}
	handle, err := lockd.StartServer(ctx, cfg)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	stop := handle.Stop
	defer stop(context.Background(), lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond))
	cli, err := client.New("unix://" + socketPath)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	err = cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        "ledger-checkpoint",
		Owner:      "worker-1",
		TTLSeconds: 10,
	}, func(ctx context.Context, af *client.AcquireForUpdateContext) error {
		state := map[string]any{}
		if err := lql.Mutate(state,
			"/cursor/batch=42",
			"time:/cursor/updated_at=2025-11-10T12:00:00Z",
		); err != nil {
			return err
		}
		return af.Save(ctx, state)
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("state updated")
	// Output: state updated
}

func ExampleClient_Query() {
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
		DisableMTLS: true,
	}
	handle, err := lockd.StartServer(ctx, cfg)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	stop := handle.Stop
	defer stop(context.Background(), lockd.WithDrainLeases(0), lockd.WithShutdownTimeout(500*time.Millisecond))
	cli, err := client.New("unix://" + socketPath)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	docs := []struct {
		OrderNo string
		Status  string
	}{
		{OrderNo: "A-1001", Status: "open"},
		{OrderNo: "A-1002", Status: "closed"},
		{OrderNo: "A-1003", Status: "open"},
	}
	for _, doc := range docs {
		lease, err := cli.Acquire(ctx, api.AcquireRequest{Namespace: "orders", Owner: "ingest", TTLSeconds: 5})
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		state := make(map[string]any)
		if err := lql.Mutate(state,
			"/order_no="+doc.OrderNo,
			"/status="+doc.Status,
		); err != nil {
			fmt.Println("error:", err)
			return
		}
		if err := lease.Save(ctx, state); err != nil {
			fmt.Println("error:", err)
			return
		}
		lease.Release(ctx)
	}
	resp, err := cli.Query(ctx,
		client.WithQueryNamespace("orders"),
		client.WithQuery(`eq{field=/status,value=open}`),
		client.WithQueryReturnDocuments(),
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	var openOrders []string
	resp.ForEach(func(row client.QueryRow) error {
		docReader, err := row.DocumentReader()
		if err != nil {
			return err
		}
		defer docReader.Close()
		var doc map[string]any
		if err := json.NewDecoder(docReader).Decode(&doc); err != nil {
			return err
		}
		if order, ok := doc["order_no"].(string); ok {
			openOrders = append(openOrders, order)
		}
		return nil
	})
	sort.Strings(openOrders)
	for _, order := range openOrders {
		fmt.Println("open order:", order)
	}
	// Output:
	// open order: A-1001
	// open order: A-1003
}
