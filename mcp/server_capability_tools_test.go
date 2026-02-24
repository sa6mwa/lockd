package mcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/pslog"
)

func TestHandleQueryToolReturnsKeysOnly(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-query-keys-%d", time.Now().UnixNano())
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
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, []byte(`{"kind":"mcp-query-keys","ok":true}`), lockdclient.UpdateOptions{
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
		Query:  "eq{field=/kind,value=mcp-query-keys}",
		Engine: "scan",
	})
	if err != nil {
		t.Fatalf("query tool: %v", err)
	}
	if len(out.Keys) == 0 {
		t.Fatalf("expected at least one key")
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
		Namespace: "mcp",
		Query:     "eq{field=/kind,value=mcp-query-stream}",
		Engine:    "scan",
	})
	if err != nil {
		t.Fatalf("query stream tool: %v", err)
	}
	if out.DownloadURL == "" {
		t.Fatalf("expected query download URL")
	}
	if out.DownloadMethod != http.MethodGet {
		t.Fatalf("expected query download method GET, got %q", out.DownloadMethod)
	}
	ndjson := string(mustDownloadTransfer(t, s, out.DownloadURL))
	if !strings.Contains(ndjson, `"key":"`+key+`"`) {
		t.Fatalf("expected query NDJSON to include key %q: %s", key, ndjson)
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

func TestHandleGetToolReturnsNumericVersionAndInlinePayload(t *testing.T) {
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
	if out.PayloadMode != "inline" {
		t.Fatalf("expected payload_mode=inline, got %q", out.PayloadMode)
	}
	if out.PayloadText != string(payload) {
		t.Fatalf("expected inline payload %q, got %q", string(payload), out.PayloadText)
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
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()

	_, delivery, err := s.handleQueueDequeueTool(ctx, &mcpsdk.CallToolRequest{Session: session}, queueDequeueToolInput{
		Namespace:   "mcp",
		Queue:       queue,
		Stateful:    true,
		PayloadMode: "stream",
		StateMode:   "none",
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
	if delivery.PayloadBytes == 0 {
		t.Fatalf("expected non-zero payload bytes")
	}
	if delivery.PayloadMode != "stream" {
		t.Fatalf("expected payload_mode=stream, got %q", delivery.PayloadMode)
	}
	if delivery.PayloadDownloadURL == "" {
		t.Fatalf("expected payload download URL")
	}
	if method := delivery.PayloadDownloadMethod; method != http.MethodGet {
		t.Fatalf("expected payload download method GET, got %q", method)
	}
	body := mustDownloadTransfer(t, s, delivery.PayloadDownloadURL)
	if string(body) != `{"kind":"mcp-queue"}` {
		t.Fatalf("unexpected dequeue payload body: %s", string(body))
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

func TestHandleQueueStatsToolReportsHeadAvailability(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	queue := fmt.Sprintf("mcp-stats-%d", time.Now().UnixNano())
	enqueued, err := cli.EnqueueBytes(ctx, queue, []byte(`{"kind":"stats"}`), lockdclient.EnqueueOptions{
		Namespace: "mcp",
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	_, out, err := s.handleQueueStatsTool(ctx, nil, queueStatsToolInput{
		Namespace: "mcp",
		Queue:     queue,
	})
	if err != nil {
		t.Fatalf("queue stats tool: %v", err)
	}
	if out.Namespace != "mcp" || out.Queue != queue {
		t.Fatalf("unexpected identity: namespace=%q queue=%q", out.Namespace, out.Queue)
	}
	if !out.Available {
		t.Fatalf("expected available=true")
	}
	if out.HeadMessageID != enqueued.MessageID {
		t.Fatalf("expected head message %q, got %q", enqueued.MessageID, out.HeadMessageID)
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
	if getOut.PayloadMode != "inline" {
		t.Fatalf("expected payload_mode=inline for attachment get, got %q", getOut.PayloadMode)
	}
	if getOut.PayloadText != string(payload) {
		t.Fatalf("expected inline attachment payload %q, got %q", string(payload), getOut.PayloadText)
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

func TestStateWriteStreamToolsEndToEnd(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-state-write-stream-%d", time.Now().UnixNano())
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
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()

	_, begin, err := s.handleStateWriteStreamBeginTool(ctx, &mcpsdk.CallToolRequest{Session: session}, stateWriteStreamBeginInput{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	})
	if err != nil {
		t.Fatalf("state write_stream.begin: %v", err)
	}
	if begin.UploadURL == "" {
		t.Fatalf("expected upload_url in state write stream begin output")
	}
	payload := []byte(`{"kind":"state-write-stream","ok":true}`)
	mustUploadTransfer(t, s, begin.UploadURL, payload)
	_, commit, err := s.handleStateWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID: begin.StreamID,
	})
	if err != nil {
		t.Fatalf("state write_stream.commit: %v", err)
	}
	if commit.NewVersion == 0 || commit.NewStateETag == "" {
		t.Fatalf("expected commit metadata, got version=%d etag=%q", commit.NewVersion, commit.NewStateETag)
	}

	if _, err := cli.Release(ctx, api.ReleaseRequest{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}
	getResp, err := cli.Get(ctx, key, lockdclient.WithGetNamespace("mcp"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer getResp.Close()
	gotBytes, err := getResp.Bytes()
	if err != nil {
		t.Fatalf("get bytes: %v", err)
	}
	if string(gotBytes) != string(payload) {
		t.Fatalf("unexpected state payload: %s", string(gotBytes))
	}
}

func TestStateWriteStreamStatusAndCommitExpectations(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-state-write-stream-status-%d", time.Now().UnixNano())
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
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()

	_, begin, err := s.handleStateWriteStreamBeginTool(ctx, &mcpsdk.CallToolRequest{Session: session}, stateWriteStreamBeginInput{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	})
	if err != nil {
		t.Fatalf("state write_stream.begin: %v", err)
	}
	_, statusBefore, err := s.handleStateWriteStreamStatusTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamStatusInput{
		StreamID: begin.StreamID,
	})
	if err != nil {
		t.Fatalf("state write_stream.status before upload: %v", err)
	}
	if !statusBefore.UploadCapabilityAvailable {
		t.Fatalf("expected upload capability available before upload")
	}
	if statusBefore.BytesReceived != 0 {
		t.Fatalf("expected zero bytes before upload, got %d", statusBefore.BytesReceived)
	}
	if statusBefore.CanCommit {
		t.Fatalf("expected can_commit=false before upload")
	}

	payload := []byte(`{"kind":"state-write-stream-status","ok":true}`)
	sum := sha256.Sum256(payload)
	expectedSHA := hex.EncodeToString(sum[:])
	mustUploadTransfer(t, s, begin.UploadURL, payload)

	_, statusAfter, err := s.handleStateWriteStreamStatusTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamStatusInput{
		StreamID: begin.StreamID,
	})
	if err != nil {
		t.Fatalf("state write_stream.status after upload: %v", err)
	}
	if statusAfter.UploadCapabilityAvailable {
		t.Fatalf("expected upload capability consumed after upload")
	}
	if !statusAfter.UploadCompleted {
		t.Fatalf("expected upload_completed=true")
	}
	if !statusAfter.CanCommit {
		t.Fatalf("expected can_commit=true after upload")
	}
	if statusAfter.BytesReceived != int64(len(payload)) {
		t.Fatalf("unexpected bytes_received=%d want=%d", statusAfter.BytesReceived, len(payload))
	}
	if statusAfter.PayloadSHA256 != expectedSHA {
		t.Fatalf("unexpected payload_sha256=%q want=%q", statusAfter.PayloadSHA256, expectedSHA)
	}

	wrongBytes := int64(len(payload) + 1)
	if _, _, err := s.handleStateWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID:      begin.StreamID,
		ExpectedBytes: &wrongBytes,
	}); err == nil || !strings.Contains(err.Error(), "expected_bytes mismatch") {
		t.Fatalf("expected expected_bytes mismatch on commit, got %v", err)
	}

	expectedBytes := int64(len(payload))
	_, commit, err := s.handleStateWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID:       begin.StreamID,
		ExpectedSHA256: expectedSHA,
		ExpectedBytes:  &expectedBytes,
	})
	if err != nil {
		t.Fatalf("state write_stream.commit with expectations: %v", err)
	}
	if commit.BytesReceived != int64(len(payload)) {
		t.Fatalf("unexpected commit bytes_received=%d want=%d", commit.BytesReceived, len(payload))
	}
}

func TestQueueWriteStreamToolsEndToEnd(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()
	queue := fmt.Sprintf("mcp-queue-write-stream-%d", time.Now().UnixNano())

	_, begin, err := s.handleQueueWriteStreamBeginTool(ctx, &mcpsdk.CallToolRequest{Session: session}, queueWriteStreamBeginInput{
		Namespace:   "mcp",
		Queue:       queue,
		ContentType: "application/json",
	})
	if err != nil {
		t.Fatalf("queue write_stream.begin: %v", err)
	}
	if begin.UploadURL == "" {
		t.Fatalf("expected upload_url in queue write stream begin output")
	}
	payload := []byte(`{"kind":"queue-write-stream","ok":true}`)
	mustUploadTransfer(t, s, begin.UploadURL, payload)
	_, commit, err := s.handleQueueWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID: begin.StreamID,
	})
	if err != nil {
		t.Fatalf("queue write_stream.commit: %v", err)
	}
	if commit.MessageID == "" || commit.PayloadBytes == 0 {
		t.Fatalf("expected committed message metadata, got id=%q payload_bytes=%d", commit.MessageID, commit.PayloadBytes)
	}

	msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
		Namespace:    "mcp",
		Owner:        "mcp-test",
		BlockSeconds: api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if msg == nil {
		t.Fatalf("expected dequeued message")
	}
	reader, err := msg.PayloadReader()
	if err != nil {
		t.Fatalf("payload reader: %v", err)
	}
	gotBytes, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	_ = reader.Close()
	_ = msg.ClosePayload()
	if string(gotBytes) != string(payload) {
		t.Fatalf("unexpected queued payload: %s", string(gotBytes))
	}
}

func TestQueueWriteStreamCommitHonorsExpectedChecksum(t *testing.T) {
	t.Parallel()

	s, _ := newToolTestServer(t)
	ctx := context.Background()
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()
	queue := fmt.Sprintf("mcp-queue-write-expected-%d", time.Now().UnixNano())

	_, begin, err := s.handleQueueWriteStreamBeginTool(ctx, &mcpsdk.CallToolRequest{Session: session}, queueWriteStreamBeginInput{
		Namespace:   "mcp",
		Queue:       queue,
		ContentType: "application/json",
	})
	if err != nil {
		t.Fatalf("queue write_stream.begin: %v", err)
	}
	payload := []byte(`{"kind":"queue-write-expected","ok":true}`)
	sum := sha256.Sum256(payload)
	expectedSHA := hex.EncodeToString(sum[:])
	mustUploadTransfer(t, s, begin.UploadURL, payload)

	if _, _, err := s.handleQueueWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID:       begin.StreamID,
		ExpectedSHA256: strings.Repeat("0", 64),
	}); err == nil || !strings.Contains(err.Error(), "expected_sha256 mismatch") {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}

	expectedBytes := int64(len(payload))
	_, out, err := s.handleQueueWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID:       begin.StreamID,
		ExpectedSHA256: expectedSHA,
		ExpectedBytes:  &expectedBytes,
	})
	if err != nil {
		t.Fatalf("queue write_stream.commit with expectations: %v", err)
	}
	if out.PayloadBytes != int64(len(payload)) {
		t.Fatalf("unexpected payload_bytes=%d want=%d", out.PayloadBytes, len(payload))
	}
}

func TestAttachmentsWriteStreamToolsEndToEnd(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-attachment-write-stream-%d", time.Now().UnixNano())
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
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()

	_, begin, err := s.handleAttachmentsWriteStreamBeginTool(ctx, &mcpsdk.CallToolRequest{Session: session}, attachmentsWriteStreamBeginInput{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
		Name:      "stream.bin",
	})
	if err != nil {
		t.Fatalf("attachments write_stream.begin: %v", err)
	}
	if begin.UploadURL == "" {
		t.Fatalf("expected upload_url in attachments write stream begin output")
	}
	payload := bytes.Repeat([]byte{0xde, 0xad, 0xbe, 0xef}, 256)
	mustUploadTransfer(t, s, begin.UploadURL, payload)
	_, commit, err := s.handleAttachmentsWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID: begin.StreamID,
	})
	if err != nil {
		t.Fatalf("attachments write_stream.commit: %v", err)
	}
	if commit.Attachment.ID == "" || commit.Attachment.Name != "stream.bin" {
		t.Fatalf("unexpected attachment commit output: %+v", commit.Attachment)
	}
	if _, err := cli.Release(ctx, api.ReleaseRequest{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}
	att, err := cli.GetAttachment(ctx, lockdclient.GetAttachmentRequest{
		Namespace: "mcp",
		Key:       key,
		Public:    true,
		Selector: lockdclient.AttachmentSelector{
			Name: "stream.bin",
		},
	})
	if err != nil {
		t.Fatalf("get attachment: %v", err)
	}
	defer att.Close()
	gotBytes, err := io.ReadAll(att)
	if err != nil {
		t.Fatalf("read attachment: %v", err)
	}
	if !bytes.Equal(gotBytes, payload) {
		t.Fatalf("attachment payload mismatch: got %d bytes", len(gotBytes))
	}
}

func TestAttachmentsWriteStreamCommitHonorsExpectedBytes(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-attachment-write-expected-%d", time.Now().UnixNano())
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
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()

	_, begin, err := s.handleAttachmentsWriteStreamBeginTool(ctx, &mcpsdk.CallToolRequest{Session: session}, attachmentsWriteStreamBeginInput{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
		Name:      "expected.bin",
	})
	if err != nil {
		t.Fatalf("attachments write_stream.begin: %v", err)
	}
	payload := bytes.Repeat([]byte{0xca, 0xfe}, 128)
	mustUploadTransfer(t, s, begin.UploadURL, payload)

	wrongBytes := int64(len(payload) + 2)
	if _, _, err := s.handleAttachmentsWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID:      begin.StreamID,
		ExpectedBytes: &wrongBytes,
	}); err == nil || !strings.Contains(err.Error(), "expected_bytes mismatch") {
		t.Fatalf("expected bytes mismatch, got %v", err)
	}

	expectedBytes := int64(len(payload))
	_, out, err := s.handleAttachmentsWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID:      begin.StreamID,
		ExpectedBytes: &expectedBytes,
	})
	if err != nil {
		t.Fatalf("attachments write_stream.commit with expectations: %v", err)
	}
	if out.BytesReceived != int64(len(payload)) {
		t.Fatalf("unexpected bytes_received=%d want=%d", out.BytesReceived, len(payload))
	}
}

func TestInlinePayloadLimitEnforced(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	s.cfg.InlineMaxBytes = 8
	ctx := context.Background()

	key := fmt.Sprintf("mcp-inline-limit-%d", time.Now().UnixNano())
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
	if _, _, err := s.handleStateUpdateTool(ctx, nil, stateUpdateToolInput{
		Namespace:   "mcp",
		Key:         key,
		LeaseID:     lease.LeaseID,
		TxnID:       lease.TxnID,
		PayloadText: `{"too":"large"}`,
	}); err == nil || !strings.Contains(err.Error(), "mcp.inline_max_bytes") || !strings.Contains(err.Error(), "lockd.state.write_stream.begin") || !strings.Contains(err.Error(), "lockd.hint") {
		t.Fatalf("expected inline max error for state update, got %v", err)
	}
	if _, _, err := s.handleQueueEnqueueTool(ctx, nil, queueEnqueueToolInput{
		Namespace:   "mcp",
		Queue:       "inline-limit",
		PayloadText: `{"too":"large"}`,
	}); err == nil || !strings.Contains(err.Error(), "mcp.inline_max_bytes") || !strings.Contains(err.Error(), "lockd.queue.write_stream.begin") || !strings.Contains(err.Error(), "lockd.hint") {
		t.Fatalf("expected inline max error for queue enqueue, got %v", err)
	}
}

func TestGetInlineTooLargeSuggestsStreamingAndHint(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	s.cfg.InlineMaxBytes = 32
	ctx := context.Background()

	key := fmt.Sprintf("mcp-get-inline-too-large-%d", time.Now().UnixNano())
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
	payload := strings.Repeat("a", 256)
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, []byte(`{"payload":"`+payload+`"}`), lockdclient.UpdateOptions{
		Namespace: "mcp",
		TxnID:     lease.TxnID,
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

	if _, _, err := s.handleGetTool(ctx, nil, getToolInput{
		Namespace:   "mcp",
		Key:         key,
		PayloadMode: "inline",
	}); err == nil || !strings.Contains(err.Error(), "lockd.state.stream") || !strings.Contains(err.Error(), "lockd.hint") {
		t.Fatalf("expected get inline-size guidance error, got %v", err)
	}
}

func TestAttachmentPutInlineTooLargeSuggestsStreamingAndHint(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	s.cfg.InlineMaxBytes = 16
	ctx := context.Background()

	key := fmt.Sprintf("mcp-attachment-inline-too-large-%d", time.Now().UnixNano())
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
	_, _, err = s.handleAttachmentPutTool(ctx, nil, attachmentPutToolInput{
		Namespace:   "mcp",
		Key:         key,
		LeaseID:     lease.LeaseID,
		TxnID:       lease.TxnID,
		Name:        "big.txt",
		PayloadText: strings.Repeat("b", 1024),
	})
	if err == nil || !strings.Contains(err.Error(), "lockd.attachments.write_stream.begin") || !strings.Contains(err.Error(), "lockd.hint") {
		t.Fatalf("expected attachment put inline-size guidance error, got %v", err)
	}
}

func TestStateWriteStreamCommitHonorsBeginPreconditions(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	key := fmt.Sprintf("mcp-state-write-preconditions-%d", time.Now().UnixNano())

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
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, []byte(`{"seed":1}`), lockdclient.UpdateOptions{
		Namespace: "mcp",
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("seed update: %v", err)
	}
	if _, err := cli.Release(ctx, api.ReleaseRequest{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("release seed lease: %v", err)
	}

	lease2, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  "mcp",
		Key:        key,
		TTLSeconds: 30,
		Owner:      "mcp-test",
		BlockSecs:  api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire second lease: %v", err)
	}
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()

	_, begin, err := s.handleStateWriteStreamBeginTool(ctx, &mcpsdk.CallToolRequest{Session: session}, stateWriteStreamBeginInput{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease2.LeaseID,
		TxnID:     lease2.TxnID,
		IfETag:    "invalid-etag-precondition",
	})
	if err != nil {
		t.Fatalf("state write_stream.begin: %v", err)
	}
	mustUploadTransfer(t, s, begin.UploadURL, []byte(`{"updated":true}`))
	if _, _, err := s.handleStateWriteStreamCommitTool(ctx, &mcpsdk.CallToolRequest{Session: session}, writeStreamCommitInput{
		StreamID: begin.StreamID,
	}); err == nil {
		t.Fatalf("expected commit to fail due to begin-time if_etag precondition")
	}
}

func TestStatePatchToolAppliesJSONMergePatch(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()

	key := fmt.Sprintf("mcp-state-patch-%d", time.Now().UnixNano())
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
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, []byte(`{"keep":1,"drop":{"x":1},"nested":{"a":1}}`), lockdclient.UpdateOptions{
		Namespace: "mcp",
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("seed update: %v", err)
	}

	_, out, err := s.handleStatePatchTool(ctx, nil, statePatchToolInput{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
		PatchText: `{"drop":null,"nested":{"b":2}}`,
	})
	if err != nil {
		t.Fatalf("state.patch: %v", err)
	}
	if out.NewVersion == 0 || out.NewStateETag == "" {
		t.Fatalf("expected patch output metadata, got version=%d etag=%q", out.NewVersion, out.NewStateETag)
	}

	if _, err := cli.Release(ctx, api.ReleaseRequest{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	resp, err := cli.Get(ctx, key, lockdclient.WithGetNamespace("mcp"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Close()
	got, err := resp.Bytes()
	if err != nil {
		t.Fatalf("get bytes: %v", err)
	}
	if string(got) != `{"keep":1,"nested":{"a":1,"b":2}}` {
		t.Fatalf("unexpected patched state: %s", string(got))
	}
}

func TestStateMutateToolAppliesLQLMutations(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()

	key := fmt.Sprintf("mcp-state-mutate-%d", time.Now().UnixNano())
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
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, []byte(`{"nested":{"a":1}}`), lockdclient.UpdateOptions{
		Namespace: "mcp",
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("seed update: %v", err)
	}

	_, out, err := s.handleStateMutateTool(ctx, nil, stateMutateToolInput{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
		Mutations: []string{`/status="ready"`, `/nested/b=2`},
	})
	if err != nil {
		t.Fatalf("state.mutate: %v", err)
	}
	if out.NewVersion == 0 || out.NewStateETag == "" {
		t.Fatalf("expected mutate output metadata, got version=%d etag=%q", out.NewVersion, out.NewStateETag)
	}

	if _, err := cli.Release(ctx, api.ReleaseRequest{
		Namespace: "mcp",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	resp, err := cli.Get(ctx, key, lockdclient.WithGetNamespace("mcp"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Close()
	got, err := resp.Bytes()
	if err != nil {
		t.Fatalf("get bytes: %v", err)
	}
	if string(got) != `{"nested":{"a":1,"b":2},"status":"ready"}` {
		t.Fatalf("unexpected mutated state: %s", string(got))
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
	srv := &server{
		cfg: Config{
			UpstreamServer:      ts.URL(),
			UpstreamDisableMTLS: true,
			DefaultNamespace:    "mcp",
			AgentBusQueue:       "lockd.agent.bus",
			InlineMaxBytes:      2 * 1024 * 1024,
			MCPPath:             "/mcp",
		},
		upstream:       cli,
		transferLog:    pslog.NewStructured(context.Background(), io.Discard),
		writeStreamLog: pslog.NewStructured(context.Background(), io.Discard),
		mcpHTTPPath:    "/mcp",
	}
	baseURL, err := parseBaseURL("http://mcp.local", true)
	if err != nil {
		t.Fatalf("parse base url: %v", err)
	}
	srv.baseURL = baseURL
	srv.transferPath = path.Join(srv.mcpHTTPPath, "transfer")
	srv.transfers = newTransferManager(srv.transferLog)
	srv.writeStreams = newWriteStreamManager(srv.writeStreamLog)
	return srv, cli
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

func mustUploadTransfer(t testing.TB, s *server, transferURL string, payload []byte) {
	t.Helper()
	parsed, err := url.Parse(strings.TrimSpace(transferURL))
	if err != nil {
		t.Fatalf("parse transfer URL %q: %v", transferURL, err)
	}
	req := httptest.NewRequest(http.MethodPut, parsed.Path, bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	s.handleTransfer(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("transfer upload status=%d body=%s", resp.StatusCode, string(body))
	}
}

func mustDownloadTransfer(t testing.TB, s *server, transferURL string) []byte {
	t.Helper()
	parsed, err := url.Parse(strings.TrimSpace(transferURL))
	if err != nil {
		t.Fatalf("parse transfer URL %q: %v", transferURL, err)
	}
	req := httptest.NewRequest(http.MethodGet, parsed.Path, nil)
	rec := httptest.NewRecorder()
	s.handleTransfer(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("transfer download status=%d body=%s", resp.StatusCode, string(body))
	}
	return body
}
