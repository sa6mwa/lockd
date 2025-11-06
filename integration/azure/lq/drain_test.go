//go:build integration && azure && lq

package azureintegration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	shutdowntest "pkt.systems/lockd/integration/internal/shutdowntest"
	queuetestutil "pkt.systems/lockd/integration/queue/testutil"
	"pkt.systems/pslog"
)

func TestAzureQueueDrainUsesProductionDefaults(t *testing.T) {
	queuetestutil.InstallWatchdog(t, "azure-queue-drain-defaults", 2*time.Minute)

	cfg := prepareAzureQueueConfig(t, azureQueueOptions{})
	productionClose := lockd.WithTestCloseDefaults(
		lockd.WithDrainLeases(8*time.Second),
		lockd.WithShutdownTimeout(2*time.Second),
	)
	logger := lockd.NewTestingLogger(t, pslog.TraceLevel)
	ts := startAzureQueueServerWithLogger(t, cfg, logger, productionClose)
	cli := ts.Client
	ensureAzureQueueWritableOrSkip(t, cfg, cli)

	ctx := context.Background()
	key := fmt.Sprintf("azure-queue-drain-%d", time.Now().UnixNano())
	scheduleAzureLockCleanup(t, cfg, key)
	scheduleAzureLockCleanup(t, cfg, key+"-wait")
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "azure-queue-drain-holder",
		TTLSeconds: 30,
		BlockSecs:  5,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	stopCh := make(chan error, 1)
	go func() {
		stopCh <- ts.Stop(context.Background())
	}()

	payload, _ := json.Marshal(api.AcquireRequest{Key: key + "-wait", Owner: "azure-queue-drain-waiter", TTLSeconds: 5})
	httpClient, err := ts.NewHTTPClient()
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	result := shutdowntest.WaitForShutdownDrainingAcquireWithClient(t, httpClient, ts.URL()+"/v1/acquire", payload)
	if result.Response.ErrorCode != "shutdown_draining" {
		t.Fatalf("expected shutdown_draining error, got %+v", result.Response)
	}
	if result.Header.Get("Shutdown-Imminent") == "" {
		t.Fatalf("expected Shutdown-Imminent header")
	}

	select {
	case err := <-stopCh:
		if err != nil {
			t.Fatalf("server stop failed: %v", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatalf("server stop timed out")
	}
	_ = lease.Release(ctx)
}
