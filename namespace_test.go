package lockd

import (
	"context"
	"io"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
)

func TestNamespaceIsolationLocks(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	ts := startTestServerFast(t)

	defaultClient, err := ts.NewClient(client.WithDefaultNamespace(DefaultNamespace))
	if err != nil {
		t.Fatalf("default client: %v", err)
	}

	const otherNamespace = "testing"
	testingClient, err := ts.NewClient(client.WithDefaultNamespace(otherNamespace))
	if err != nil {
		t.Fatalf("testing client: %v", err)
	}

	sharedKey := "namespace-shared-object"

	sessDefault, err := defaultClient.Acquire(ctx, api.AcquireRequest{
		Key:        sharedKey,
		Owner:      "worker-default",
		TTLSeconds: int64((10 * time.Second).Seconds()),
	})
	if err != nil {
		t.Fatalf("acquire default: %v", err)
	}
	if sessDefault.Namespace != DefaultNamespace {
		t.Fatalf("expected default namespace %q, got %q", DefaultNamespace, sessDefault.Namespace)
	}
	if err := sessDefault.Save(ctx, map[string]string{"namespace": "default"}); err != nil {
		t.Fatalf("save default: %v", err)
	}
	if err := sessDefault.Release(ctx); err != nil {
		t.Fatalf("release default: %v", err)
	}

	sessTesting, err := testingClient.Acquire(ctx, api.AcquireRequest{
		Key:        sharedKey,
		Owner:      "worker-testing",
		TTLSeconds: int64((10 * time.Second).Seconds()),
	})
	if err != nil {
		t.Fatalf("acquire testing: %v", err)
	}
	if sessTesting.Namespace != otherNamespace {
		t.Fatalf("expected namespace %q, got %q", otherNamespace, sessTesting.Namespace)
	}
	if err := sessTesting.Save(ctx, map[string]string{"namespace": otherNamespace}); err != nil {
		t.Fatalf("save testing: %v", err)
	}
	if err := sessTesting.Release(ctx); err != nil {
		t.Fatalf("release testing: %v", err)
	}

	sessDefault, err = defaultClient.Acquire(ctx, api.AcquireRequest{
		Key:        sharedKey,
		Owner:      "worker-default",
		TTLSeconds: int64((10 * time.Second).Seconds()),
	})
	if err != nil {
		t.Fatalf("reacquire default: %v", err)
	}
	var defaultPayload map[string]string
	if err := sessDefault.Load(ctx, &defaultPayload); err != nil {
		t.Fatalf("load default state: %v", err)
	}
	if got := defaultPayload["namespace"]; got != "default" {
		t.Fatalf("expected default payload, got %q", got)
	}
	if err := sessDefault.Release(ctx); err != nil {
		t.Fatalf("release default (2): %v", err)
	}

	sessTesting, err = testingClient.Acquire(ctx, api.AcquireRequest{
		Key:        sharedKey,
		Owner:      "worker-testing",
		TTLSeconds: int64((10 * time.Second).Seconds()),
	})
	if err != nil {
		t.Fatalf("reacquire testing: %v", err)
	}
	var testingPayload map[string]string
	if err := sessTesting.Load(ctx, &testingPayload); err != nil {
		t.Fatalf("load testing state: %v", err)
	}
	if got := testingPayload["namespace"]; got != otherNamespace {
		t.Fatalf("expected testing payload, got %q", got)
	}
	if err := sessTesting.Release(ctx); err != nil {
		t.Fatalf("release testing (2): %v", err)
	}
}

func TestNamespaceIsolationQueues(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	ts := startTestServerFast(t)

	defaultClient, err := ts.NewClient(client.WithDefaultNamespace(DefaultNamespace))
	if err != nil {
		t.Fatalf("default client: %v", err)
	}
	const otherNamespace = "testing"
	testingClient, err := ts.NewClient(client.WithDefaultNamespace(otherNamespace))
	if err != nil {
		t.Fatalf("testing client: %v", err)
	}

	queueName := "namespace-queue"

	if _, err := defaultClient.EnqueueBytes(ctx, queueName, []byte("default"), client.EnqueueOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("enqueue default: %v", err)
	}
	if _, err := testingClient.EnqueueBytes(ctx, queueName, []byte("testing"), client.EnqueueOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("enqueue testing: %v", err)
	}

	msgDefault, err := defaultClient.Dequeue(ctx, queueName, client.DequeueOptions{
		Owner:        "worker-default",
		Visibility:   5 * time.Second,
		BlockSeconds: client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("dequeue default: %v", err)
	}
	dataDefault, err := io.ReadAll(msgDefault)
	if err != nil {
		t.Fatalf("read default payload: %v", err)
	}
	if string(dataDefault) != "default" {
		t.Fatalf("expected default payload, got %q", string(dataDefault))
	}
	if err := msgDefault.Ack(ctx); err != nil {
		t.Fatalf("ack default: %v", err)
	}

	msgTesting, err := testingClient.Dequeue(ctx, queueName, client.DequeueOptions{
		Owner:        "worker-testing",
		Visibility:   5 * time.Second,
		BlockSeconds: client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("dequeue testing: %v", err)
	}
	dataTesting, err := io.ReadAll(msgTesting)
	if err != nil {
		t.Fatalf("read testing payload: %v", err)
	}
	if string(dataTesting) != "testing" {
		t.Fatalf("expected testing payload, got %q", string(dataTesting))
	}
	if err := msgTesting.Ack(ctx); err != nil {
		t.Fatalf("ack testing: %v", err)
	}
}
