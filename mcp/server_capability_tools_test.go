package mcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

func TestHandleQueryToolRejectsDocumentsMode(t *testing.T) {
	t.Parallel()

	s := &server{}
	_, _, err := s.handleQueryTool(context.Background(), nil, queryToolInput{
		Query:  "eq{field=/kind,value=mcp-query-doc}",
		Return: "documents",
	})
	if err == nil || !strings.Contains(err.Error(), "documents mode moved to lockd.query.stream") {
		t.Fatalf("expected documents mode rejection error, got %v", err)
	}
}

func TestHandleQueryStreamToolStreamsDocuments(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-query-stream-%d", time.Now().UnixNano())
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
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, []byte(`{"kind":"mcp-query-stream","ok":true}`), lockdclient.UpdateOptions{
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

	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()

	_, out, err := s.handleQueryStreamTool(ctx, &mcpsdk.CallToolRequest{Session: session}, queryStreamToolInput{
		Namespace:  "mcp",
		Query:      "eq{field=/kind,value=mcp-query-stream}",
		Engine:     "scan",
		ChunkBytes: 8,
	})
	if err != nil {
		t.Fatalf("query stream tool: %v", err)
	}
	if out.Mode != "documents" {
		t.Fatalf("expected documents mode, got %q", out.Mode)
	}
	if out.DocumentsStreamed == 0 {
		t.Fatalf("expected at least one streamed document")
	}
	if out.StreamedBytes == 0 {
		t.Fatalf("expected streamed bytes > 0")
	}
	if out.Chunks == 0 {
		t.Fatalf("expected chunk count > 0")
	}
	if out.ProgressToken == "" {
		t.Fatalf("expected progress token")
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

func TestHandleGetToolReturnsNumericVersionAndStreamHint(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-get-%d", time.Now().UnixNano())
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
	payload := []byte(`{"kind":"mcp-get","ok":true}`)
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, payload, lockdclient.UpdateOptions{
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

	_, out, err := s.handleGetTool(ctx, nil, getToolInput{
		Namespace: "mcp",
		Key:       key,
	})
	if err != nil {
		t.Fatalf("get tool: %v", err)
	}
	if !out.Found {
		t.Fatalf("expected found=true")
	}
	if out.Version <= 0 {
		t.Fatalf("expected numeric version > 0, got %d", out.Version)
	}
	if !out.StreamRequired {
		t.Fatalf("expected stream_required=true for present state")
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

func TestHandleQueueWatchToolBoundedByMaxEvents(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queue := fmt.Sprintf("mcp-watch-%d", time.Now().UnixNano())

	enqueueErr := make(chan error, 1)
	go func() {
		time.Sleep(150 * time.Millisecond)
		_, err := cli.EnqueueBytes(ctx, queue, []byte(`{"kind":"watch-event"}`), lockdclient.EnqueueOptions{
			Namespace: "mcp",
		})
		enqueueErr <- err
	}()

	_, out, err := s.handleQueueWatchTool(ctx, nil, queueWatchToolInput{
		Namespace:       "mcp",
		Queue:           queue,
		DurationSeconds: 3,
		MaxEvents:       1,
	})
	if err != nil {
		t.Fatalf("queue watch tool: %v", err)
	}
	if out.StopReason != "max_events" {
		t.Fatalf("expected stop_reason=max_events, got %q", out.StopReason)
	}
	if out.EventCount != 1 || len(out.Events) != 1 {
		t.Fatalf("expected exactly one event, got count=%d len=%d", out.EventCount, len(out.Events))
	}
	if out.Events[0].Queue != queue {
		t.Fatalf("expected queue %q, got %q", queue, out.Events[0].Queue)
	}
	if err := <-enqueueErr; err != nil {
		t.Fatalf("enqueue for watch: %v", err)
	}
}

func TestHandleQueueWatchToolTimeout(t *testing.T) {
	t.Parallel()

	s, _ := newToolTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queue := fmt.Sprintf("mcp-watch-timeout-%d", time.Now().UnixNano())

	_, out, err := s.handleQueueWatchTool(ctx, nil, queueWatchToolInput{
		Namespace:       "mcp",
		Queue:           queue,
		DurationSeconds: 1,
		MaxEvents:       10,
	})
	if err != nil {
		t.Fatalf("queue watch tool: %v", err)
	}
	if out.StopReason != "timeout" {
		t.Fatalf("expected stop_reason=timeout, got %q", out.StopReason)
	}
}

func TestAttachmentHeadAndGetIntegrityFields(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-attachment-%d", time.Now().UnixNano())
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
	payload := []byte(`{"kind":"attachment","ok":true}`)
	if _, err := cli.Attach(ctx, lockdclient.AttachRequest{
		Namespace:   "mcp",
		Key:         key,
		LeaseID:     lease.LeaseID,
		TxnID:       lease.TxnID,
		Name:        "hello.json",
		Body:        bytes.NewReader(payload),
		ContentType: "application/json",
	}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if _, err := cli.Release(ctx, api.ReleaseRequest{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	_, headOut, err := s.handleAttachmentHeadTool(ctx, nil, attachmentHeadToolInput{
		Namespace: "mcp",
		Key:       key,
		Name:      "hello.json",
	})
	if err != nil {
		t.Fatalf("attachments head: %v", err)
	}
	if headOut.Attachment.Name != "hello.json" {
		t.Fatalf("expected attachment name hello.json, got %q", headOut.Attachment.Name)
	}
	if headOut.Attachment.Size != int64(len(payload)) {
		t.Fatalf("expected attachment size %d, got %d", len(payload), headOut.Attachment.Size)
	}
	sum := sha256.Sum256(payload)
	expectedHash := hex.EncodeToString(sum[:])
	if headOut.Attachment.PlaintextSHA256 != expectedHash {
		t.Fatalf("expected attachment plaintext_sha256 %q, got %q", expectedHash, headOut.Attachment.PlaintextSHA256)
	}

	_, getOut, err := s.handleAttachmentGetTool(ctx, nil, attachmentGetToolInput{
		Namespace: "mcp",
		Key:       key,
		Name:      "hello.json",
	})
	if err != nil {
		t.Fatalf("attachments get: %v", err)
	}
	if getOut.PayloadSHA256 != expectedHash {
		t.Fatalf("expected payload_sha256 %q, got %q", expectedHash, getOut.PayloadSHA256)
	}
	if !getOut.StreamRequired {
		t.Fatalf("expected stream_required=true for attachment get")
	}
	if getOut.PayloadBytes != int64(len(payload)) {
		t.Fatalf("expected payload_bytes=%d, got %d", len(payload), getOut.PayloadBytes)
	}

	_, checksumOut, err := s.handleAttachmentChecksumTool(ctx, nil, attachmentChecksumToolInput{
		Namespace: "mcp",
		Key:       key,
		Name:      "hello.json",
	})
	if err != nil {
		t.Fatalf("attachments checksum: %v", err)
	}
	if checksumOut.PlaintextSHA256 != expectedHash {
		t.Fatalf("expected checksum plaintext_sha256 %q, got %q", expectedHash, checksumOut.PlaintextSHA256)
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

func newInMemoryServerSession(t *testing.T) (*mcpsdk.ServerSession, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "test-server", Version: "0.0.1"}, nil)
	t1, t2 := mcpsdk.NewInMemoryTransports()
	ss, err := server.Connect(ctx, t1, nil)
	if err != nil {
		cancel()
		t.Fatalf("server connect: %v", err)
	}
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		_ = ss.Close()
		cancel()
		t.Fatalf("client connect: %v", err)
	}
	return ss, func() {
		_ = cs.Close()
		_ = ss.Close()
		cancel()
	}
}
