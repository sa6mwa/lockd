package mcpsuite

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/uuidv7"
	lockdmcp "pkt.systems/lockd/mcp"
	"pkt.systems/pslog"
)

const (
	integrationInlineMaxBytes int64 = 256 * 1024
	integrationLargePayloadX  int64 = 2
)

// ServerFactory starts a backend-specific lockd server for MCP E2E verification.
type ServerFactory func(testing.TB) *lockd.TestServer

// RunFacadeE2E runs one end-to-end MCP facade scenario against a backend-specific lockd server.
func RunFacadeE2E(t *testing.T, factory ServerFactory) {
	t.Helper()
	if factory == nil {
		t.Fatal("mcp suite requires a server factory")
	}
	ts := factory(t)
	if ts == nil {
		t.Fatal("mcp suite server factory returned nil test server")
	}

	mcpEndpoint, stop := startMCPFacade(t, ts)
	defer stop()

	progressCh := make(chan *mcpsdk.ProgressNotificationClientRequest, 256)
	cs, closeSession := connectMCPClient(t, mcpEndpoint, progressCh)
	defer closeSession()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	tools, err := cs.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	requireToolsPresent(t, tools,
		"lockd.hint",
		"lockd.lock.acquire",
		"lockd.state.update",
		"lockd.state.stream",
		"lockd.attachments.write_stream.begin",
		"lockd.queue.enqueue",
		"lockd.queue.dequeue",
		"lockd.queue.ack",
	)

	hintOut := mustCallToolObject(t, ctx, cs, "lockd.hint", map[string]any{})
	if ns := asString(hintOut["default_namespace"]); ns != "mcp" {
		t.Fatalf("expected hint default_namespace=mcp, got %q", ns)
	}

	key := "mcp-e2e-" + uuidv7.NewString()
	acqOut := mustCallToolObject(t, ctx, cs, "lockd.lock.acquire", map[string]any{
		"key":           key,
		"owner":         "mcp-e2e",
		"ttl_seconds":   int64(30),
		"block_seconds": int64(-1),
	})
	leaseID := requireStringField(t, acqOut, "lease_id")
	txnID := asString(acqOut["txn_id"])
	if fencing := asInt64(acqOut["fencing_token"]); fencing <= 0 {
		t.Fatalf("expected positive fencing_token, got %d", fencing)
	}

	statePayload := fmt.Sprintf(`{"kind":"mcp-e2e","backend":"%s","ok":true}`, key)
	updateOut := mustCallToolObject(t, ctx, cs, "lockd.state.update", map[string]any{
		"key":          key,
		"lease_id":     leaseID,
		"txn_id":       txnID,
		"payload_text": statePayload,
	})
	if v := asInt64(updateOut["new_version"]); v <= 0 {
		t.Fatalf("expected state update new_version > 0, got %d", v)
	}

	attachmentName := "e2e.txt"
	attachmentPayload := []byte("mcp attachment payload")
	beginOut := mustCallToolObject(t, ctx, cs, "lockd.attachments.write_stream.begin", map[string]any{
		"key":       key,
		"lease_id":  leaseID,
		"txn_id":    txnID,
		"name":      attachmentName,
		"max_bytes": int64(1024),
	})
	streamID := requireStringField(t, beginOut, "stream_id")
	mustCallToolObject(t, ctx, cs, "lockd.attachments.write_stream.append", map[string]any{
		"stream_id":    streamID,
		"chunk_base64": base64.StdEncoding.EncodeToString(attachmentPayload),
	})
	commitOut := mustCallToolObject(t, ctx, cs, "lockd.attachments.write_stream.commit", map[string]any{
		"stream_id": streamID,
	})
	attachmentObj := asObject(commitOut["attachment"])
	if gotName := asString(attachmentObj["name"]); gotName != attachmentName {
		t.Fatalf("expected committed attachment name %q, got %q", attachmentName, gotName)
	}

	releaseOut := mustCallToolObject(t, ctx, cs, "lockd.lock.release", map[string]any{
		"key":      key,
		"lease_id": leaseID,
		"txn_id":   txnID,
	})
	if !asBool(releaseOut["released"]) {
		t.Fatalf("expected lock release success, got %+v", releaseOut)
	}

	getOut := mustCallToolObject(t, ctx, cs, "lockd.get", map[string]any{"key": key})
	if !asBool(getOut["found"]) {
		t.Fatalf("expected lockd.get found=true, got %+v", getOut)
	}
	if !asBool(getOut["stream_required"]) {
		t.Fatalf("expected lockd.get stream_required=true, got %+v", getOut)
	}

	stateStreamOut := mustCallToolObject(t, ctx, cs, "lockd.state.stream", map[string]any{
		"key":         key,
		"chunk_bytes": int64(16),
	})
	if asInt64(stateStreamOut["streamed_bytes"]) <= 0 {
		t.Fatalf("expected state stream bytes > 0, got %+v", stateStreamOut)
	}
	if asInt64(stateStreamOut["chunks"]) <= 0 {
		t.Fatalf("expected state stream chunks > 0, got %+v", stateStreamOut)
	}

	checksumOut := mustCallToolObject(t, ctx, cs, "lockd.attachments.checksum", map[string]any{
		"key":  key,
		"name": attachmentName,
	})
	if sha := asString(checksumOut["plaintext_sha256"]); sha == "" {
		t.Fatalf("expected plaintext_sha256 in checksum output, got %+v", checksumOut)
	}

	attachmentStreamOut := mustCallToolObject(t, ctx, cs, "lockd.attachments.stream", map[string]any{
		"key":         key,
		"name":        attachmentName,
		"chunk_bytes": int64(8),
	})
	if asInt64(attachmentStreamOut["streamed_bytes"]) <= 0 {
		t.Fatalf("expected attachment stream bytes > 0, got %+v", attachmentStreamOut)
	}
	if asInt64(attachmentStreamOut["chunks"]) <= 0 {
		t.Fatalf("expected attachment stream chunks > 0, got %+v", attachmentStreamOut)
	}

	queue := "mcp-e2e-" + uuidv7.NewString()
	enqueueOut := mustCallToolObject(t, ctx, cs, "lockd.queue.enqueue", map[string]any{
		"queue":        queue,
		"payload_text": `{"kind":"mcp-e2e-queue","ok":true}`,
	})
	if msgID := asString(enqueueOut["message_id"]); msgID == "" {
		t.Fatalf("expected enqueue message_id, got %+v", enqueueOut)
	}

	dequeueOut := mustCallToolObject(t, ctx, cs, "lockd.queue.dequeue", map[string]any{
		"queue":         queue,
		"block_seconds": int64(-1),
		"chunk_bytes":   int64(8),
	})
	if !asBool(dequeueOut["found"]) {
		t.Fatalf("expected dequeue found=true, got %+v", dequeueOut)
	}
	if asInt64(dequeueOut["payload_streamed_bytes"]) <= 0 {
		t.Fatalf("expected dequeue payload_streamed_bytes > 0, got %+v", dequeueOut)
	}

	ackOut := mustCallToolObject(t, ctx, cs, "lockd.queue.ack", map[string]any{
		"queue":         queue,
		"message_id":    requireStringField(t, dequeueOut, "message_id"),
		"lease_id":      requireStringField(t, dequeueOut, "lease_id"),
		"fencing_token": asInt64(dequeueOut["fencing_token"]),
		"meta_etag":     requireStringField(t, dequeueOut, "meta_etag"),
	})
	if !asBool(ackOut["acked"]) {
		t.Fatalf("expected queue ack success, got %+v", ackOut)
	}

	statsOut := mustCallToolObject(t, ctx, cs, "lockd.queue.stats", map[string]any{"queue": queue})
	if gotQueue := asString(statsOut["queue"]); gotQueue != queue {
		t.Fatalf("expected queue stats queue=%q, got %q", queue, gotQueue)
	}

	runFailureModes(t, ctx, cs)
	runLargeStreamingChecks(t, ctx, cs)

	events := waitDrainProgress(progressCh, 250*time.Millisecond)
	if len(events) == 0 {
		t.Fatalf("expected progress notifications from stream tools, got none")
	}
	types := progressEventTypes(events)
	requireEventType(t, types, "lockd.state.chunk")
	requireEventType(t, types, "lockd.attachments.chunk")
	requireEventType(t, types, "lockd.queue.payload.chunk")
}

func runFailureModes(t testing.TB, ctx context.Context, cs *mcpsdk.ClientSession) {
	t.Helper()

	errObj := mustCallToolError(t, ctx, cs, "lockd.state.update", map[string]any{
		"key":            "mcp-failure-" + uuidv7.NewString(),
		"lease_id":       "lease-missing",
		"payload_text":   "{}",
		"payload_base64": base64.StdEncoding.EncodeToString([]byte("{}")),
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for mutually exclusive payload fields, got %+v", errObj)
	}

	errObj = mustCallToolError(t, ctx, cs, "lockd.queue.ack", map[string]any{
		"queue":         "mcp-failure-queue",
		"message_id":    "missing-message",
		"lease_id":      "missing-lease",
		"fencing_token": int64(1),
		"meta_etag":     "",
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for incomplete ack material, got %+v", errObj)
	}

	queueName := "mcp-failure-queue-" + uuidv7.NewString()
	_ = mustCallToolObject(t, ctx, cs, "lockd.queue.enqueue", map[string]any{
		"queue":        queueName,
		"payload_text": `{"kind":"mcp-failure","msg":"dequeue-max-bytes"}`,
	})

	errObj = mustCallToolError(t, ctx, cs, "lockd.queue.dequeue", map[string]any{
		"queue":         queueName,
		"block_seconds": int64(-1),
		"max_bytes":     int64(-1),
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for negative max_bytes, got %+v", errObj)
	}

	errObj = mustCallToolError(t, ctx, cs, "lockd.state.write_stream.append", map[string]any{
		"stream_id":    "missing-stream",
		"chunk_base64": "%%%not-base64%%%",
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for invalid base64 chunk, got %+v", errObj)
	}

	if _, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "lockd.tool.unknown",
		Arguments: map[string]any{},
	}); err == nil {
		t.Fatalf("expected protocol error for unknown tool")
	}
}

func runLargeStreamingChecks(t testing.TB, ctx context.Context, cs *mcpsdk.ClientSession) {
	t.Helper()

	largePayload := syntheticJSONPayload(int(integrationInlineMaxBytes*integrationLargePayloadX + 1024))
	if int64(len(largePayload)) <= integrationInlineMaxBytes {
		t.Fatalf("synthetic payload too small: %d <= %d", len(largePayload), integrationInlineMaxBytes)
	}

	// Large state payload must use write_stream when over inline limit.
	stateKey := "mcp-large-state-" + uuidv7.NewString()
	stateAcquire := mustCallToolObject(t, ctx, cs, "lockd.lock.acquire", map[string]any{
		"key":           stateKey,
		"owner":         "mcp-large-state",
		"ttl_seconds":   int64(30),
		"block_seconds": int64(-1),
	})
	stateLeaseID := requireStringField(t, stateAcquire, "lease_id")
	stateTxnID := asString(stateAcquire["txn_id"])

	errObj := mustCallToolError(t, ctx, cs, "lockd.state.update", map[string]any{
		"key":            stateKey,
		"lease_id":       stateLeaseID,
		"txn_id":         stateTxnID,
		"payload_base64": base64.StdEncoding.EncodeToString(largePayload),
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for oversized inline state update, got %+v", errObj)
	}

	stateBegin := mustCallToolObject(t, ctx, cs, "lockd.state.write_stream.begin", map[string]any{
		"key":      stateKey,
		"lease_id": stateLeaseID,
		"txn_id":   stateTxnID,
	})
	stateStreamID := requireStringField(t, stateBegin, "stream_id")
	appendStreamChunks(t, ctx, cs, "lockd.state.write_stream.append", stateStreamID, largePayload, 64*1024)
	stateCommit := mustCallToolObject(t, ctx, cs, "lockd.state.write_stream.commit", map[string]any{
		"stream_id": stateStreamID,
	})
	if asInt64(stateCommit["bytes_received"]) != int64(len(largePayload)) {
		t.Fatalf("expected state bytes_received=%d, got %+v", len(largePayload), stateCommit)
	}
	_ = mustCallToolObject(t, ctx, cs, "lockd.lock.release", map[string]any{
		"key":      stateKey,
		"lease_id": stateLeaseID,
		"txn_id":   stateTxnID,
	})

	stateStream := mustCallToolObject(t, ctx, cs, "lockd.state.stream", map[string]any{
		"key":         stateKey,
		"chunk_bytes": int64(32 * 1024),
	})
	if asInt64(stateStream["streamed_bytes"]) != int64(len(largePayload)) {
		t.Fatalf("expected state streamed_bytes=%d, got %+v", len(largePayload), stateStream)
	}
	if asInt64(stateStream["chunks"]) <= 1 {
		t.Fatalf("expected multiple chunks for large state stream, got %+v", stateStream)
	}

	// Large queue payload must use write_stream when over inline limit.
	queueName := "mcp-large-queue-" + uuidv7.NewString()
	errObj = mustCallToolError(t, ctx, cs, "lockd.queue.enqueue", map[string]any{
		"queue":          queueName,
		"payload_base64": base64.StdEncoding.EncodeToString(largePayload),
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for oversized inline enqueue, got %+v", errObj)
	}

	queueBegin := mustCallToolObject(t, ctx, cs, "lockd.queue.write_stream.begin", map[string]any{
		"queue": queueName,
	})
	queueStreamID := requireStringField(t, queueBegin, "stream_id")
	appendStreamChunks(t, ctx, cs, "lockd.queue.write_stream.append", queueStreamID, largePayload, 64*1024)
	queueCommit := mustCallToolObject(t, ctx, cs, "lockd.queue.write_stream.commit", map[string]any{
		"stream_id": queueStreamID,
	})
	if asInt64(queueCommit["bytes_received"]) != int64(len(largePayload)) {
		t.Fatalf("expected queue bytes_received=%d, got %+v", len(largePayload), queueCommit)
	}

	largeDelivery := mustCallToolObject(t, ctx, cs, "lockd.queue.dequeue", map[string]any{
		"queue":         queueName,
		"block_seconds": int64(-1),
		"chunk_bytes":   int64(32 * 1024),
	})
	if !asBool(largeDelivery["found"]) {
		t.Fatalf("expected large payload dequeue found=true, got %+v", largeDelivery)
	}
	if asInt64(largeDelivery["payload_streamed_bytes"]) != int64(len(largePayload)) {
		t.Fatalf("expected queue payload_streamed_bytes=%d, got %+v", len(largePayload), largeDelivery)
	}
	if asInt64(largeDelivery["payload_chunks"]) <= 1 {
		t.Fatalf("expected multiple chunks for large dequeue payload, got %+v", largeDelivery)
	}
	_ = mustCallToolObject(t, ctx, cs, "lockd.queue.ack", map[string]any{
		"queue":         queueName,
		"message_id":    requireStringField(t, largeDelivery, "message_id"),
		"lease_id":      requireStringField(t, largeDelivery, "lease_id"),
		"fencing_token": asInt64(largeDelivery["fencing_token"]),
		"meta_etag":     requireStringField(t, largeDelivery, "meta_etag"),
	})
}

func appendStreamChunks(t testing.TB, ctx context.Context, cs *mcpsdk.ClientSession, appendTool string, streamID string, payload []byte, chunkSize int) {
	t.Helper()
	if chunkSize <= 0 {
		t.Fatalf("chunkSize must be > 0")
	}
	for off := 0; off < len(payload); off += chunkSize {
		end := off + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		chunk := payload[off:end]
		_ = mustCallToolObject(t, ctx, cs, appendTool, map[string]any{
			"stream_id":    streamID,
			"chunk_base64": base64.StdEncoding.EncodeToString(chunk),
		})
	}
}

func mustCallToolError(t testing.TB, ctx context.Context, cs *mcpsdk.ClientSession, name string, args map[string]any) map[string]any {
	t.Helper()
	if cs == nil {
		t.Fatalf("nil mcp client session for tool %q", name)
	}
	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      name,
		Arguments: args,
	})
	if err != nil {
		t.Fatalf("call tool %s: %v", name, err)
	}
	if res == nil {
		t.Fatalf("call tool %s: nil result", name)
	}
	if !res.IsError {
		obj, _ := toolResultObject(res)
		t.Fatalf("call tool %s expected error result, got success: %+v", name, obj)
	}
	obj, err := toolResultObject(res)
	if err != nil {
		t.Fatalf("decode tool %s error output: %v (raw=%s)", name, err, toolResultText(res))
	}
	errObj := asObject(obj["error"])
	if len(errObj) == 0 {
		t.Fatalf("tool %s error result missing structured envelope: %+v", name, obj)
	}
	return errObj
}

func syntheticJSONPayload(targetBytes int) []byte {
	if targetBytes < 64 {
		targetBytes = 64
	}
	const prefix = `{"kind":"mcp-large","blob":"`
	const suffix = `"}`
	fillerLen := targetBytes - len(prefix) - len(suffix)
	if fillerLen < 1 {
		fillerLen = 1
	}
	return []byte(prefix + strings.Repeat("x", fillerLen) + suffix)
}

func waitDrainProgress(ch <-chan *mcpsdk.ProgressNotificationClientRequest, wait time.Duration) []*mcpsdk.ProgressNotificationClientRequest {
	if wait > 0 {
		time.Sleep(wait)
	}
	return drainProgress(ch)
}

func progressEventTypes(events []*mcpsdk.ProgressNotificationClientRequest) []string {
	types := make([]string, 0, len(events))
	for _, req := range events {
		if req == nil || req.Params == nil {
			continue
		}
		raw := strings.TrimSpace(req.Params.Message)
		if raw == "" {
			continue
		}
		var msg map[string]any
		if err := json.Unmarshal([]byte(raw), &msg); err != nil {
			continue
		}
		eventType := asString(msg["type"])
		if eventType != "" {
			types = append(types, eventType)
		}
	}
	return types
}

func requireEventType(t testing.TB, got []string, expected string) {
	t.Helper()
	if !slices.Contains(got, expected) {
		t.Fatalf("expected progress event type %q, got %v", expected, got)
	}
}

func startMCPFacade(t testing.TB, ts *lockd.TestServer) (string, func()) {
	t.Helper()

	listen := reserveLoopbackAddr(t)
	cfg := lockdmcp.Config{
		Listen:           listen,
		DisableTLS:       true,
		MCPPath:          "/mcp",
		UpstreamServer:   ts.URL(),
		DefaultNamespace: "mcp",
		AgentBusQueue:    "lockd.agent.bus",
		InlineMaxBytes:   integrationInlineMaxBytes,
	}
	if creds := ts.TestMTLSCredentials(); creds.Valid() {
		cfg.UpstreamDisableMTLS = false
		cfg.UpstreamClientBundlePath = writeBytesFile(t, "upstream-client.pem", creds.ClientBundle())
	} else {
		cfg.UpstreamDisableMTLS = true
	}

	srv, err := lockdmcp.NewServer(lockdmcp.NewServerRequest{
		Config: cfg,
		Logger: pslog.NewStructured(context.Background(), ioDiscard{}).With("app", "lockd"),
	})
	if err != nil {
		t.Fatalf("new mcp facade server: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Run(runCtx)
	}()

	stop := func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("mcp facade stop: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("timed out stopping mcp facade")
		}
	}
	return "http://" + listen + "/mcp", stop
}

func connectMCPClient(t testing.TB, endpoint string, progressCh chan<- *mcpsdk.ProgressNotificationClientRequest) (*mcpsdk.ClientSession, func()) {
	t.Helper()
	if strings.TrimSpace(endpoint) == "" {
		t.Fatal("mcp endpoint required")
	}
	client := mcpsdk.NewClient(&mcpsdk.Implementation{
		Name:    "lockd-mcp-integration-client",
		Version: "0.0.1",
	}, &mcpsdk.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcpsdk.ProgressNotificationClientRequest) {
			if req == nil || progressCh == nil {
				return
			}
			select {
			case progressCh <- req:
			default:
			}
		},
	})

	transport := &mcpsdk.StreamableClientTransport{
		Endpoint: endpoint,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	var (
		cs      *mcpsdk.ClientSession
		lastErr error
	)
	for attempt := 0; attempt < 50; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		session, err := client.Connect(ctx, transport, nil)
		cancel()
		if err == nil {
			cs = session
			break
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	if cs == nil {
		t.Fatalf("connect mcp client session: %v", lastErr)
	}
	return cs, func() {
		_ = cs.Close()
	}
}

func mustCallToolObject(t testing.TB, ctx context.Context, cs *mcpsdk.ClientSession, name string, args map[string]any) map[string]any {
	t.Helper()
	if cs == nil {
		t.Fatalf("nil mcp client session for tool %q", name)
	}
	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      name,
		Arguments: args,
	})
	if err != nil {
		t.Fatalf("call tool %s: %v", name, err)
	}
	if res == nil {
		t.Fatalf("call tool %s: nil result", name)
	}
	if res.IsError {
		t.Fatalf("call tool %s returned error: %s", name, toolResultText(res))
	}
	obj, err := toolResultObject(res)
	if err != nil {
		t.Fatalf("decode tool %s output: %v (raw=%s)", name, err, toolResultText(res))
	}
	return obj
}

func toolResultObject(res *mcpsdk.CallToolResult) (map[string]any, error) {
	if res == nil {
		return nil, fmt.Errorf("result required")
	}
	if res.StructuredContent != nil {
		return anyToObject(res.StructuredContent)
	}
	if len(res.Content) == 0 {
		return map[string]any{}, nil
	}
	text, ok := res.Content[0].(*mcpsdk.TextContent)
	if !ok {
		return nil, fmt.Errorf("unexpected result content type %T", res.Content[0])
	}
	if strings.TrimSpace(text.Text) == "" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func anyToObject(v any) (map[string]any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func toolResultText(res *mcpsdk.CallToolResult) string {
	if res == nil || len(res.Content) == 0 {
		return ""
	}
	parts := make([]string, 0, len(res.Content))
	for _, c := range res.Content {
		if text, ok := c.(*mcpsdk.TextContent); ok {
			parts = append(parts, strings.TrimSpace(text.Text))
		}
	}
	return strings.Join(parts, "; ")
}

func requireToolsPresent(t testing.TB, tools *mcpsdk.ListToolsResult, names ...string) {
	t.Helper()
	if tools == nil {
		t.Fatalf("list tools result missing")
	}
	available := make([]string, 0, len(tools.Tools))
	for _, tool := range tools.Tools {
		available = append(available, strings.TrimSpace(tool.Name))
	}
	for _, name := range names {
		if !slices.Contains(available, name) {
			t.Fatalf("missing expected tool %q in tool list", name)
		}
	}
}

func requireStringField(t testing.TB, obj map[string]any, field string) string {
	t.Helper()
	v := asString(obj[field])
	if v == "" {
		t.Fatalf("expected non-empty %q in %+v", field, obj)
	}
	return v
}

func asObject(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asString(v any) string {
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func asBool(v any) bool {
	b, ok := v.(bool)
	if ok {
		return b
	}
	return false
}

func asInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case json.Number:
		i, _ := n.Int64()
		return i
	default:
		return 0
	}
}

func drainProgress(ch <-chan *mcpsdk.ProgressNotificationClientRequest) []*mcpsdk.ProgressNotificationClientRequest {
	if ch == nil {
		return nil
	}
	out := make([]*mcpsdk.ProgressNotificationClientRequest, 0, 8)
	for {
		select {
		case req := <-ch:
			if req != nil {
				out = append(out, req)
			}
		default:
			return out
		}
	}
}

func reserveLoopbackAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve listen addr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func writeBytesFile(t testing.TB, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// ioDiscard avoids bringing in io package just for io.Discard on older toolchains.
type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
