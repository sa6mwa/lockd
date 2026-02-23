package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

func TestHandleQueryToolDocumentsMode(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-query-doc-%d", time.Now().UnixNano())
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  "mcp",
		Key:        key,
		TTLSeconds: 30,
		Owner:      "mcp-test",
		BlockSecs:  api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, []byte(`{"kind":"mcp-query-doc","ok":true}`), lockdclient.UpdateOptions{
		Namespace: "mcp",
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := cli.Release(ctx, api.ReleaseRequest{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	_, out, err := s.handleQueryTool(ctx, nil, queryToolInput{
		Namespace: "mcp",
		Query:     "eq{field=/kind,value=mcp-query-doc}",
		Return:    "documents",
		Engine:    "scan",
	})
	if err != nil {
		t.Fatalf("query tool: %v", err)
	}
	if out.Mode != "documents" {
		t.Fatalf("expected documents mode, got %q", out.Mode)
	}
	if len(out.Keys) == 0 {
		t.Fatalf("expected at least one key in documents mode")
	}
	if len(out.Documents) == 0 {
		t.Fatalf("expected at least one document in documents mode")
	}
	var payload map[string]any
	if err := json.Unmarshal(out.Documents[0], &payload); err != nil {
		t.Fatalf("decode document payload: %v", err)
	}
	if got := payload["kind"]; got != "mcp-query-doc" {
		t.Fatalf("expected kind mcp-query-doc, got %#v", got)
	}
}

func TestHandleDescribeToolSupportsNamespaceOverride(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-describe-%d", time.Now().UnixNano())
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  "agents",
		Key:        key,
		TTLSeconds: 30,
		Owner:      "mcp-test",
		BlockSecs:  api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, []byte(`{"kind":"mcp-describe"}`), lockdclient.UpdateOptions{
		Namespace: "agents",
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := cli.Release(ctx, api.ReleaseRequest{
		Namespace: "agents",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	_, out, err := s.handleDescribeTool(ctx, nil, describeToolInput{
		Namespace: "agents",
		Key:       key,
	})
	if err != nil {
		t.Fatalf("describe tool: %v", err)
	}
	if out.Namespace != "agents" {
		t.Fatalf("expected namespace agents, got %q", out.Namespace)
	}
	if out.Key != key {
		t.Fatalf("expected key %q, got %q", key, out.Key)
	}
	if out.Version == 0 {
		t.Fatalf("expected non-zero version in describe output")
	}
}

func TestHandleQueueDequeueStatefulNackAndExtend(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	queue := fmt.Sprintf("mcp-queue-%d", time.Now().UnixNano())
	if _, err := cli.EnqueueBytes(ctx, queue, []byte(`{"kind":"mcp-queue"}`), lockdclient.EnqueueOptions{
		Namespace: "mcp",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	_, delivery, err := s.handleQueueDequeueTool(ctx, nil, queueDequeueToolInput{
		Namespace:   "mcp",
		Queue:       queue,
		Stateful:    true,
		BlockSecond: api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("dequeue tool: %v", err)
	}
	if !delivery.Found {
		t.Fatalf("expected dequeued message")
	}
	if delivery.StateLeaseID == "" {
		t.Fatalf("expected state lease id for stateful dequeue")
	}

	_, extendOut, err := s.handleQueueExtendTool(ctx, nil, queueExtendToolInput{
		Namespace:         delivery.Namespace,
		Queue:             delivery.Queue,
		MessageID:         delivery.MessageID,
		LeaseID:           delivery.LeaseID,
		FencingToken:      delivery.FencingToken,
		MetaETag:          delivery.MetaETag,
		StateLeaseID:      delivery.StateLeaseID,
		StateFencingToken: delivery.StateFencingToken,
		ExtendBySeconds:   5,
	})
	if err != nil {
		t.Fatalf("queue extend tool: %v", err)
	}
	if extendOut.LeaseExpiresAtUnix == 0 {
		t.Fatalf("expected lease expiry in extend output")
	}

	metaETag := delivery.MetaETag
	if extendOut.MetaETag != "" {
		metaETag = extendOut.MetaETag
	}
	_, nackOut, err := s.handleQueueNackTool(ctx, nil, queueNackToolInput{
		Namespace:         delivery.Namespace,
		Queue:             delivery.Queue,
		MessageID:         delivery.MessageID,
		LeaseID:           delivery.LeaseID,
		FencingToken:      delivery.FencingToken,
		MetaETag:          metaETag,
		StateLeaseID:      delivery.StateLeaseID,
		StateFencingToken: delivery.StateFencingToken,
		Reason:            "test-failure-path",
	})
	if err != nil {
		t.Fatalf("queue nack tool: %v", err)
	}
	if !nackOut.Requeued {
		t.Fatalf("expected nack to requeue message")
	}
}

func newToolTestServer(t *testing.T) (*server, *lockdclient.Client) {
	t.Helper()

	ts := lockd.StartTestServer(t, lockd.WithoutTestMTLS())
	cli, err := ts.NewClient(lockdclient.WithDefaultNamespace("mcp"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.Close()
	})
	return &server{
		cfg: Config{
			UpstreamServer:      ts.URL(),
			UpstreamDisableMTLS: true,
			DefaultNamespace:    "mcp",
			AgentBusQueue:       "lockd.agent.bus",
		},
		upstream: cli,
	}, cli
}
