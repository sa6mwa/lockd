package mcpsuite

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/uuidv7"
	lockdmcp "pkt.systems/lockd/mcp"
	mcpadmin "pkt.systems/lockd/mcp/admin"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

const (
	integrationInlineMaxBytes int64 = 256 * 1024
	integrationLargePayloadX  int64 = 2
)

type mcpTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	Scope        string `json:"scope,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

type mcpFacadeHarness struct {
	cfg          lockdmcp.Config
	baseURL      string
	endpoint     string
	issuer       string
	clientID     string
	clientSecret string
	admin        *mcpadmin.Service
	httpClient   *http.Client
	logger       pslog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan error
}

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

	facade := startMCPFacade(t, ts)
	defer facade.stop(t)

	progressCh := make(chan *mcpsdk.ProgressNotificationClientRequest, 256)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	accessToken := issueClientCredentialsToken(ctx, t, facade, facade.clientID, facade.clientSecret)
	cs, closeSession := connectMCPClient(t, facade.endpoint, progressCh, facade.authHTTPClient(accessToken))
	defer closeSession()

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

	hintOut := mustCallToolObject(ctx, t, cs, "lockd.hint", map[string]any{})
	if ns := asString(hintOut["default_namespace"]); ns != "mcp" {
		t.Fatalf("expected hint default_namespace=mcp, got %q", ns)
	}

	key := "mcp-e2e-" + uuidv7.NewString()
	acqOut := mustCallToolObject(ctx, t, cs, "lockd.lock.acquire", map[string]any{
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
	updateOut := mustCallToolObject(ctx, t, cs, "lockd.state.update", map[string]any{
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
	beginOut := mustCallToolObject(ctx, t, cs, "lockd.attachments.write_stream.begin", map[string]any{
		"key":       key,
		"lease_id":  leaseID,
		"txn_id":    txnID,
		"name":      attachmentName,
		"max_bytes": int64(1024),
	})
	streamID := requireStringField(t, beginOut, "stream_id")
	uploadURL := requireStringField(t, beginOut, "upload_url")
	mustTransferUploadHTTP(ctx, t, facade.httpClient, uploadURL, attachmentPayload)
	commitOut := mustCallToolObject(ctx, t, cs, "lockd.attachments.write_stream.commit", map[string]any{
		"stream_id": streamID,
	})
	attachmentObj := asObject(commitOut["attachment"])
	if gotName := asString(attachmentObj["name"]); gotName != attachmentName {
		t.Fatalf("expected committed attachment name %q, got %q", attachmentName, gotName)
	}

	releaseOut := mustCallToolObject(ctx, t, cs, "lockd.lock.release", map[string]any{
		"key":      key,
		"lease_id": leaseID,
		"txn_id":   txnID,
	})
	if !asBool(releaseOut["released"]) {
		t.Fatalf("expected lock release success, got %+v", releaseOut)
	}

	getOut := mustCallToolObject(ctx, t, cs, "lockd.get", map[string]any{"key": key})
	if !asBool(getOut["found"]) {
		t.Fatalf("expected lockd.get found=true, got %+v", getOut)
	}
	if !asBool(getOut["stream_required"]) {
		t.Fatalf("expected lockd.get stream_required=true, got %+v", getOut)
	}

	stateStreamOut := mustCallToolObject(ctx, t, cs, "lockd.state.stream", map[string]any{
		"key": key,
	})
	stateDownloadURL := requireStringField(t, stateStreamOut, "download_url")
	stateBytes := mustTransferDownloadHTTP(ctx, t, facade.httpClient, stateDownloadURL)
	if string(stateBytes) != statePayload {
		t.Fatalf("unexpected state stream payload: %s", string(stateBytes))
	}

	checksumOut := mustCallToolObject(ctx, t, cs, "lockd.attachments.checksum", map[string]any{
		"key":  key,
		"name": attachmentName,
	})
	if sha := asString(checksumOut["plaintext_sha256"]); sha == "" {
		t.Fatalf("expected plaintext_sha256 in checksum output, got %+v", checksumOut)
	}

	attachmentStreamOut := mustCallToolObject(ctx, t, cs, "lockd.attachments.stream", map[string]any{
		"key":  key,
		"name": attachmentName,
	})
	attachmentDownloadURL := requireStringField(t, attachmentStreamOut, "download_url")
	attachmentBytes := mustTransferDownloadHTTP(ctx, t, facade.httpClient, attachmentDownloadURL)
	if string(attachmentBytes) != string(attachmentPayload) {
		t.Fatalf("unexpected attachment stream payload: %s", string(attachmentBytes))
	}

	queue := "mcp-e2e-" + uuidv7.NewString()
	enqueueOut := mustCallToolObject(ctx, t, cs, "lockd.queue.enqueue", map[string]any{
		"queue":        queue,
		"payload_text": `{"kind":"mcp-e2e-queue","ok":true}`,
	})
	if msgID := asString(enqueueOut["message_id"]); msgID == "" {
		t.Fatalf("expected enqueue message_id, got %+v", enqueueOut)
	}

	dequeueOut := mustCallToolObject(ctx, t, cs, "lockd.queue.dequeue", map[string]any{
		"queue":         queue,
		"block_seconds": int64(-1),
	})
	if !asBool(dequeueOut["found"]) {
		t.Fatalf("expected dequeue found=true, got %+v", dequeueOut)
	}
	payloadDownloadURL := requireStringField(t, dequeueOut, "payload_download_url")
	payloadBytes := mustTransferDownloadHTTP(ctx, t, facade.httpClient, payloadDownloadURL)
	if string(payloadBytes) != `{"kind":"mcp-e2e-queue","ok":true}` {
		t.Fatalf("unexpected dequeue payload: %s", string(payloadBytes))
	}

	ackOut := mustCallToolObject(ctx, t, cs, "lockd.queue.ack", map[string]any{
		"queue":         queue,
		"message_id":    requireStringField(t, dequeueOut, "message_id"),
		"lease_id":      requireStringField(t, dequeueOut, "lease_id"),
		"fencing_token": asInt64(dequeueOut["fencing_token"]),
		"meta_etag":     requireStringField(t, dequeueOut, "meta_etag"),
	})
	if !asBool(ackOut["acked"]) {
		t.Fatalf("expected queue ack success, got %+v", ackOut)
	}

	statsOut := mustCallToolObject(ctx, t, cs, "lockd.queue.stats", map[string]any{"queue": queue})
	if gotQueue := asString(statsOut["queue"]); gotQueue != queue {
		t.Fatalf("expected queue stats queue=%q, got %q", queue, gotQueue)
	}

	runFailureModes(ctx, t, cs)
	runLargeStreamingChecks(ctx, t, facade.httpClient, cs)
	runTransferCapabilityFailureModes(ctx, t, facade, cs)
	runQueueFailureModes(ctx, t, cs)
	runConcurrencyFailureModes(ctx, t, facade, cs)
	runOAuthFailureModes(ctx, t, facade)

}

func runFailureModes(ctx context.Context, t testing.TB, cs *mcpsdk.ClientSession) {
	t.Helper()

	errObj := mustCallToolError(ctx, t, cs, "lockd.state.update", map[string]any{
		"key":            "mcp-failure-" + uuidv7.NewString(),
		"lease_id":       "lease-missing",
		"payload_text":   "{}",
		"payload_base64": base64.StdEncoding.EncodeToString([]byte("{}")),
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for mutually exclusive payload fields, got %+v", errObj)
	}

	errObj = mustCallToolError(ctx, t, cs, "lockd.queue.ack", map[string]any{
		"queue":         "mcp-failure-queue",
		"message_id":    "missing-message",
		"lease_id":      "missing-lease",
		"fencing_token": int64(1),
		"meta_etag":     "",
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for incomplete ack material, got %+v", errObj)
	}

	errObj = mustCallToolError(ctx, t, cs, "lockd.state.write_stream.append", map[string]any{
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

func runLargeStreamingChecks(ctx context.Context, t testing.TB, transferHTTPClient *http.Client, cs *mcpsdk.ClientSession) {
	t.Helper()

	largePayload := syntheticJSONPayload(int(integrationInlineMaxBytes*integrationLargePayloadX + 1024))
	if int64(len(largePayload)) <= integrationInlineMaxBytes {
		t.Fatalf("synthetic payload too small: %d <= %d", len(largePayload), integrationInlineMaxBytes)
	}

	// Large state payload must use write_stream when over inline limit.
	stateKey := "mcp-large-state-" + uuidv7.NewString()
	stateAcquire := mustCallToolObject(ctx, t, cs, "lockd.lock.acquire", map[string]any{
		"key":           stateKey,
		"owner":         "mcp-large-state",
		"ttl_seconds":   int64(30),
		"block_seconds": int64(-1),
	})
	stateLeaseID := requireStringField(t, stateAcquire, "lease_id")
	stateTxnID := asString(stateAcquire["txn_id"])

	errObj := mustCallToolError(ctx, t, cs, "lockd.state.update", map[string]any{
		"key":            stateKey,
		"lease_id":       stateLeaseID,
		"txn_id":         stateTxnID,
		"payload_base64": base64.StdEncoding.EncodeToString(largePayload),
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for oversized inline state update, got %+v", errObj)
	}

	stateBegin := mustCallToolObject(ctx, t, cs, "lockd.state.write_stream.begin", map[string]any{
		"key":      stateKey,
		"lease_id": stateLeaseID,
		"txn_id":   stateTxnID,
	})
	stateStreamID := requireStringField(t, stateBegin, "stream_id")
	stateUploadURL := requireStringField(t, stateBegin, "upload_url")
	mustTransferUploadHTTP(ctx, t, transferHTTPClient, stateUploadURL, largePayload)
	stateCommit := mustCallToolObject(ctx, t, cs, "lockd.state.write_stream.commit", map[string]any{
		"stream_id": stateStreamID,
	})
	if asInt64(stateCommit["bytes_received"]) != int64(len(largePayload)) {
		t.Fatalf("expected state bytes_received=%d, got %+v", len(largePayload), stateCommit)
	}
	_ = mustCallToolObject(ctx, t, cs, "lockd.lock.release", map[string]any{
		"key":      stateKey,
		"lease_id": stateLeaseID,
		"txn_id":   stateTxnID,
	})

	stateStream := mustCallToolObject(ctx, t, cs, "lockd.state.stream", map[string]any{
		"key": stateKey,
	})
	stateDownloadURL := requireStringField(t, stateStream, "download_url")
	stateBytes := mustTransferDownloadHTTP(ctx, t, transferHTTPClient, stateDownloadURL)
	if len(stateBytes) != len(largePayload) {
		t.Fatalf("expected state bytes=%d, got %d", len(largePayload), len(stateBytes))
	}
	if !bytes.Equal(stateBytes, largePayload) {
		t.Fatalf("large state payload mismatch")
	}

	// Large queue payload must use write_stream when over inline limit.
	queueName := "mcp-large-queue-" + uuidv7.NewString()
	errObj = mustCallToolError(ctx, t, cs, "lockd.queue.enqueue", map[string]any{
		"queue":          queueName,
		"payload_base64": base64.StdEncoding.EncodeToString(largePayload),
	})
	if code := asString(errObj["error_code"]); code != "invalid_argument" {
		t.Fatalf("expected invalid_argument for oversized inline enqueue, got %+v", errObj)
	}

	queueBegin := mustCallToolObject(ctx, t, cs, "lockd.queue.write_stream.begin", map[string]any{
		"queue": queueName,
	})
	queueStreamID := requireStringField(t, queueBegin, "stream_id")
	queueUploadURL := requireStringField(t, queueBegin, "upload_url")
	mustTransferUploadHTTP(ctx, t, transferHTTPClient, queueUploadURL, largePayload)
	queueCommit := mustCallToolObject(ctx, t, cs, "lockd.queue.write_stream.commit", map[string]any{
		"stream_id": queueStreamID,
	})
	if asInt64(queueCommit["bytes_received"]) != int64(len(largePayload)) {
		t.Fatalf("expected queue bytes_received=%d, got %+v", len(largePayload), queueCommit)
	}

	largeDelivery := mustCallToolObject(ctx, t, cs, "lockd.queue.dequeue", map[string]any{
		"queue":         queueName,
		"block_seconds": int64(-1),
	})
	if !asBool(largeDelivery["found"]) {
		t.Fatalf("expected large payload dequeue found=true, got %+v", largeDelivery)
	}
	queueDownloadURL := requireStringField(t, largeDelivery, "payload_download_url")
	queueBytes := mustTransferDownloadHTTP(ctx, t, transferHTTPClient, queueDownloadURL)
	if len(queueBytes) != len(largePayload) {
		t.Fatalf("expected queue payload bytes=%d, got %d", len(largePayload), len(queueBytes))
	}
	if !bytes.Equal(queueBytes, largePayload) {
		t.Fatalf("large queue payload mismatch")
	}
	_ = mustCallToolObject(ctx, t, cs, "lockd.queue.ack", map[string]any{
		"queue":         queueName,
		"message_id":    requireStringField(t, largeDelivery, "message_id"),
		"lease_id":      requireStringField(t, largeDelivery, "lease_id"),
		"fencing_token": asInt64(largeDelivery["fencing_token"]),
		"meta_etag":     requireStringField(t, largeDelivery, "meta_etag"),
	})
}

func runTransferCapabilityFailureModes(ctx context.Context, t testing.TB, facade *mcpFacadeHarness, cs *mcpsdk.ClientSession) {
	t.Helper()

	key := "mcp-transfer-fail-" + uuidv7.NewString()
	acq := mustCallToolObject(ctx, t, cs, "lockd.lock.acquire", map[string]any{
		"key":           key,
		"owner":         "mcp-transfer-fail",
		"ttl_seconds":   int64(30),
		"block_seconds": int64(-1),
	})
	leaseID := requireStringField(t, acq, "lease_id")
	txnID := asString(acq["txn_id"])

	stateBegin := mustCallToolObject(ctx, t, cs, "lockd.state.write_stream.begin", map[string]any{
		"key":      key,
		"lease_id": leaseID,
		"txn_id":   txnID,
	})
	uploadURL := requireStringField(t, stateBegin, "upload_url")
	if status, _, err := transferRequest(ctx, facade.httpClient, http.MethodGet, uploadURL, nil); err != nil || status != http.StatusMethodNotAllowed {
		t.Fatalf("expected GET upload_url status=405, got status=%d err=%v", status, err)
	}
	payload := []byte(`{"kind":"transfer-fail","ok":true}`)
	if status, _, err := transferRequest(ctx, facade.httpClient, http.MethodPut, uploadURL, payload); err != nil || status != http.StatusNoContent {
		t.Fatalf("expected first PUT upload_url status=204, got status=%d err=%v", status, err)
	}
	if status, _, err := transferRequest(ctx, facade.httpClient, http.MethodPut, uploadURL, payload); err != nil || status != http.StatusNotFound {
		t.Fatalf("expected reused PUT upload_url status=404, got status=%d err=%v", status, err)
	}
	_ = mustCallToolObject(ctx, t, cs, "lockd.state.write_stream.commit", map[string]any{
		"stream_id": requireStringField(t, stateBegin, "stream_id"),
	})
	_ = mustCallToolObject(ctx, t, cs, "lockd.lock.release", map[string]any{
		"key":      key,
		"lease_id": leaseID,
		"txn_id":   txnID,
	})

	stateStream := mustCallToolObject(ctx, t, cs, "lockd.state.stream", map[string]any{"key": key})
	downloadURL := requireStringField(t, stateStream, "download_url")
	if status, _, err := transferRequest(ctx, facade.httpClient, http.MethodGet, downloadURL, nil); err != nil || status != http.StatusOK {
		t.Fatalf("expected first GET download_url status=200, got status=%d err=%v", status, err)
	}
	if status, _, err := transferRequest(ctx, facade.httpClient, http.MethodGet, downloadURL, nil); err != nil || status != http.StatusNotFound {
		t.Fatalf("expected reused GET download_url status=404, got status=%d err=%v", status, err)
	}

	acq2 := mustCallToolObject(ctx, t, cs, "lockd.lock.acquire", map[string]any{
		"key":           key + "-abort",
		"owner":         "mcp-transfer-abort",
		"ttl_seconds":   int64(30),
		"block_seconds": int64(-1),
	})
	leaseID2 := requireStringField(t, acq2, "lease_id")
	txnID2 := asString(acq2["txn_id"])
	abortBegin := mustCallToolObject(ctx, t, cs, "lockd.state.write_stream.begin", map[string]any{
		"key":      key + "-abort",
		"lease_id": leaseID2,
		"txn_id":   txnID2,
	})
	abortUploadURL := requireStringField(t, abortBegin, "upload_url")
	_ = mustCallToolObject(ctx, t, cs, "lockd.state.write_stream.abort", map[string]any{
		"stream_id": requireStringField(t, abortBegin, "stream_id"),
	})
	if status, _, err := transferRequest(ctx, facade.httpClient, http.MethodPut, abortUploadURL, []byte(`{}`)); err != nil || status != http.StatusNotFound {
		t.Fatalf("expected aborted PUT upload_url status=404, got status=%d err=%v", status, err)
	}
	_ = mustCallToolObject(ctx, t, cs, "lockd.lock.release", map[string]any{
		"key":      key + "-abort",
		"lease_id": leaseID2,
		"txn_id":   txnID2,
	})
}

func runQueueFailureModes(ctx context.Context, t testing.TB, cs *mcpsdk.ClientSession) {
	t.Helper()

	queue := "mcp-queue-fail-" + uuidv7.NewString()
	_ = mustCallToolObject(ctx, t, cs, "lockd.queue.enqueue", map[string]any{
		"queue":        queue,
		"payload_text": `{"kind":"queue-failure"}`,
	})
	msg := mustCallToolObject(ctx, t, cs, "lockd.queue.dequeue", map[string]any{
		"queue":         queue,
		"block_seconds": int64(-1),
	})
	if !asBool(msg["found"]) {
		t.Fatalf("expected queue dequeue found=true")
	}
	messageID := requireStringField(t, msg, "message_id")
	leaseID := requireStringField(t, msg, "lease_id")
	metaETag := requireStringField(t, msg, "meta_etag")
	fencing := asInt64(msg["fencing_token"])

	ackErr := mustCallToolError(ctx, t, cs, "lockd.queue.ack", map[string]any{
		"queue":         queue,
		"message_id":    messageID,
		"lease_id":      leaseID + "-mismatch",
		"fencing_token": fencing,
		"meta_etag":     metaETag,
	})
	if asString(ackErr["error_code"]) == "" {
		t.Fatalf("expected structured queue ack lease mismatch error, got %+v", ackErr)
	}

	extendErr := mustCallToolError(ctx, t, cs, "lockd.queue.extend", map[string]any{
		"queue":             queue,
		"message_id":        messageID,
		"lease_id":          leaseID,
		"fencing_token":     fencing + 1,
		"meta_etag":         metaETag,
		"extend_by_seconds": int64(15),
	})
	if asString(extendErr["error_code"]) == "" {
		t.Fatalf("expected structured queue extend mismatch error, got %+v", extendErr)
	}

	deferErr := mustCallToolError(ctx, t, cs, "lockd.queue.defer", map[string]any{
		"queue":         queue,
		"message_id":    messageID,
		"lease_id":      leaseID + "-mismatch",
		"fencing_token": fencing,
		"meta_etag":     metaETag,
		"delay_seconds": int64(5),
	})
	if asString(deferErr["error_code"]) == "" {
		t.Fatalf("expected structured queue defer mismatch error, got %+v", deferErr)
	}

	_ = mustCallToolObject(ctx, t, cs, "lockd.queue.ack", map[string]any{
		"queue":         queue,
		"message_id":    messageID,
		"lease_id":      leaseID,
		"fencing_token": fencing,
		"meta_etag":     metaETag,
	})
}

func runOAuthFailureModes(ctx context.Context, t testing.TB, facade *mcpFacadeHarness) {
	t.Helper()

	_, status, body := requestToken(ctx, t, facade.httpClient, facade.baseURL+"/token", facade.clientID, "bad-secret", url.Values{
		"grant_type": {"client_credentials"},
	})
	if status != http.StatusUnauthorized {
		t.Fatalf("expected bad-secret token status=401, got %d body=%s", status, string(body))
	}

	if err := tryConnectMCPClient(ctx, facade.endpoint, facade.httpClient); err == nil {
		t.Fatalf("expected unauthenticated MCP connect failure")
	}

	token := issueClientCredentialsToken(ctx, t, facade, facade.clientID, facade.clientSecret)
	cs, closeSession := connectMCPClient(t, facade.endpoint, nil, facade.authHTTPClient(token))
	defer closeSession()
	_ = mustCallToolObject(ctx, t, cs, "lockd.hint", map[string]any{})

	if err := facade.admin.SetClientRevoked(facade.clientID, true); err != nil {
		t.Fatalf("revoke oauth client: %v", err)
	}
	if _, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{Name: "lockd.hint", Arguments: map[string]any{}}); err == nil {
		t.Fatalf("expected MCP call failure after live client revocation")
	}
	if err := facade.admin.SetClientRevoked(facade.clientID, false); err != nil {
		t.Fatalf("restore oauth client: %v", err)
	}
	issueClientCredentialsToken(ctx, t, facade, facade.clientID, facade.clientSecret)

	redirectURI := "https://client.example/callback"
	code := issueAuthorizationCode(ctx, t, facade, facade.clientID, redirectURI, "read")
	authCodeToken, _, _ := requestToken(ctx, t, facade.httpClient, facade.baseURL+"/token", facade.clientID, facade.clientSecret, url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {redirectURI},
	})
	if strings.TrimSpace(authCodeToken.RefreshToken) == "" {
		t.Fatalf("expected refresh_token from authorization_code exchange")
	}

	facade.restart(t)
	refreshed, refreshedStatus, refreshedBody := requestToken(ctx, t, facade.httpClient, facade.baseURL+"/token", facade.clientID, facade.clientSecret, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {authCodeToken.RefreshToken},
	})
	if refreshedStatus != http.StatusOK {
		t.Fatalf("expected refresh_token exchange status=200, got %d body=%s", refreshedStatus, string(refreshedBody))
	}
	if strings.TrimSpace(refreshed.AccessToken) == "" {
		t.Fatalf("expected access token after restart refresh exchange")
	}
}

func runConcurrencyFailureModes(ctx context.Context, t testing.TB, facade *mcpFacadeHarness, cs *mcpsdk.ClientSession) {
	t.Helper()

	key := "mcp-concurrency-" + uuidv7.NewString()
	acq := mustCallToolObject(ctx, t, cs, "lockd.lock.acquire", map[string]any{
		"key":           key,
		"owner":         "mcp-concurrency",
		"ttl_seconds":   int64(30),
		"block_seconds": int64(-1),
	})
	leaseID := requireStringField(t, acq, "lease_id")
	txnID := asString(acq["txn_id"])

	begin := mustCallToolObject(ctx, t, cs, "lockd.state.write_stream.begin", map[string]any{
		"key":      key,
		"lease_id": leaseID,
		"txn_id":   txnID,
	})
	uploadURL := requireStringField(t, begin, "upload_url")
	payloadA := []byte(`{"winner":"A"}`)
	payloadB := []byte(`{"winner":"B"}`)

	type result struct {
		payload []byte
		status  int
		err     error
	}
	ch := make(chan result, 2)
	go func() {
		status, _, err := transferRequest(ctx, facade.httpClient, http.MethodPut, uploadURL, payloadA)
		ch <- result{payload: payloadA, status: status, err: err}
	}()
	go func() {
		status, _, err := transferRequest(ctx, facade.httpClient, http.MethodPut, uploadURL, payloadB)
		ch <- result{payload: payloadB, status: status, err: err}
	}()
	first := <-ch
	second := <-ch
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent upload request error: first=%v second=%v", first.err, second.err)
	}
	statuses := []int{first.status, second.status}
	if !(slices.Contains(statuses, http.StatusNoContent) && slices.Contains(statuses, http.StatusNotFound)) {
		t.Fatalf("expected concurrent upload statuses {204,404}, got %v", statuses)
	}
	_ = mustCallToolObject(ctx, t, cs, "lockd.state.write_stream.commit", map[string]any{
		"stream_id": requireStringField(t, begin, "stream_id"),
	})
	_ = mustCallToolObject(ctx, t, cs, "lockd.lock.release", map[string]any{
		"key":      key,
		"lease_id": leaseID,
		"txn_id":   txnID,
	})

	stateStream := mustCallToolObject(ctx, t, cs, "lockd.state.stream", map[string]any{"key": key})
	downloadURL := requireStringField(t, stateStream, "download_url")
	ch2 := make(chan result, 2)
	go func() {
		status, body, err := transferRequest(ctx, facade.httpClient, http.MethodGet, downloadURL, nil)
		ch2 <- result{status: status, payload: body, err: err}
	}()
	go func() {
		status, body, err := transferRequest(ctx, facade.httpClient, http.MethodGet, downloadURL, nil)
		ch2 <- result{status: status, payload: body, err: err}
	}()
	downA := <-ch2
	downB := <-ch2
	if downA.err != nil || downB.err != nil {
		t.Fatalf("concurrent download request error: first=%v second=%v", downA.err, downB.err)
	}
	downloadStatuses := []int{downA.status, downB.status}
	if !(slices.Contains(downloadStatuses, http.StatusOK) && slices.Contains(downloadStatuses, http.StatusNotFound)) {
		t.Fatalf("expected concurrent download statuses {200,404}, got %v", downloadStatuses)
	}
}

func transferRequest(ctx context.Context, httpClient *http.Client, method, transferURL string, payload []byte) (int, []byte, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	var body io.Reader = http.NoBody
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSpace(transferURL), body)
	if err != nil {
		return 0, nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out, nil
}

func issueClientCredentialsToken(ctx context.Context, t testing.TB, facade *mcpFacadeHarness, clientID, clientSecret string) string {
	t.Helper()
	token, status, body := requestToken(ctx, t, facade.httpClient, facade.baseURL+"/token", clientID, clientSecret, url.Values{
		"grant_type": {"client_credentials"},
	})
	if status != http.StatusOK {
		t.Fatalf("client_credentials token exchange status=%d body=%s", status, string(body))
	}
	if strings.TrimSpace(token.AccessToken) == "" {
		t.Fatalf("client_credentials token missing access_token")
	}
	return token.AccessToken
}

func issueAuthorizationCode(ctx context.Context, t testing.TB, facade *mcpFacadeHarness, clientID, redirectURI, scope string) string {
	t.Helper()

	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", clientID)
	q.Set("redirect_uri", redirectURI)
	if strings.TrimSpace(scope) != "" {
		q.Set("scope", scope)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, facade.baseURL+"/authorize?"+q.Encode(), http.NoBody)
	if err != nil {
		t.Fatalf("create authorize request: %v", err)
	}
	httpClient := facade.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	noRedirect := *httpClient
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := noRedirect.Do(req)
	if err != nil {
		t.Fatalf("authorize request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("authorize status=%d body=%s", resp.StatusCode, string(body))
	}
	loc := strings.TrimSpace(resp.Header.Get("Location"))
	if loc == "" {
		t.Fatalf("authorize missing redirect location")
	}
	parsed, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("parse authorize location %q: %v", loc, err)
	}
	code := strings.TrimSpace(parsed.Query().Get("code"))
	if code == "" {
		t.Fatalf("authorize redirect missing code in %q", loc)
	}
	return code
}

func requestToken(ctx context.Context, t testing.TB, httpClient *http.Client, tokenURL, clientID, clientSecret string, form url.Values) (mcpTokenResponse, int, []byte) {
	t.Helper()
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("create token request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(clientID, clientSecret)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("token request failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var token mcpTokenResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, &token); err != nil {
			t.Fatalf("decode token response: %v body=%s", err, string(body))
		}
	}
	return token, resp.StatusCode, body
}

func tryConnectMCPClient(ctx context.Context, endpoint string, httpClient *http.Client) error {
	if strings.TrimSpace(endpoint) == "" {
		return fmt.Errorf("mcp endpoint required")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	client := mcpsdk.NewClient(&mcpsdk.Implementation{
		Name:    "lockd-mcp-integration-client-auth-check",
		Version: "0.0.1",
	}, nil)
	transport := &mcpsdk.StreamableClientTransport{
		Endpoint:   endpoint,
		HTTPClient: httpClient,
	}
	connCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	session, err := client.Connect(connCtx, transport, nil)
	if err != nil {
		return err
	}
	_ = session.Close()
	return nil
}

func mustTransferUploadHTTP(ctx context.Context, t testing.TB, httpClient *http.Client, transferURL string, payload []byte) {
	t.Helper()
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimSpace(transferURL), bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("create transfer upload request: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("transfer upload request failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("transfer upload status=%d body=%s", resp.StatusCode, string(body))
	}
}

func mustTransferDownloadHTTP(ctx context.Context, t testing.TB, httpClient *http.Client, transferURL string) []byte {
	t.Helper()
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSpace(transferURL), http.NoBody)
	if err != nil {
		t.Fatalf("create transfer download request: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("transfer download request failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("transfer download status=%d body=%s", resp.StatusCode, string(body))
	}
	return body
}

func mustCallToolError(ctx context.Context, t testing.TB, cs *mcpsdk.ClientSession, name string, args map[string]any) map[string]any {
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
		return nil
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

func startMCPFacade(t testing.TB, ts *lockd.TestServer) *mcpFacadeHarness {
	t.Helper()

	creds := ts.TestMTLSCredentials()
	if !creds.Valid() {
		t.Fatalf("mcp integration suite requires upstream test mTLS credentials")
	}

	root := t.TempDir()
	listen := reserveLoopbackAddr(t)
	baseURL := "https://" + listen
	issuer := baseURL
	endpoint := baseURL + "/"

	serverBundlePath := filepath.Join(root, "mcp-server.pem")
	upstreamClientBundlePath := filepath.Join(root, "upstream-client.pem")
	statePath := filepath.Join(root, "mcp.pem")
	refreshStorePath := filepath.Join(root, "mcp-auth-store.json")
	writeFile(t, serverBundlePath, creds.ServerBundle(), 0o600)
	writeFile(t, upstreamClientBundlePath, creds.ClientBundle(), 0o600)

	adminSvc := mcpadmin.New(mcpadmin.Config{
		StatePath:    statePath,
		RefreshStore: refreshStorePath,
	})
	boot, err := adminSvc.Bootstrap(mcpadmin.BootstrapRequest{
		Path:              statePath,
		Issuer:            issuer,
		InitialClientName: "integration-default",
		InitialScopes:     []string{"read", "write"},
	})
	if err != nil {
		t.Fatalf("bootstrap mcp oauth state: %v", err)
	}

	h := &mcpFacadeHarness{
		cfg: lockdmcp.Config{
			Listen:                   listen,
			DisableTLS:               false,
			BaseURL:                  baseURL,
			AllowHTTP:                false,
			BundlePath:               serverBundlePath,
			MCPPath:                  "/",
			UpstreamServer:           ts.URL(),
			UpstreamDisableMTLS:      false,
			UpstreamClientBundlePath: upstreamClientBundlePath,
			DefaultNamespace:         "mcp",
			AgentBusQueue:            "lockd.agent.bus",
			InlineMaxBytes:           integrationInlineMaxBytes,
			OAuthStatePath:           statePath,
			OAuthRefreshStorePath:    refreshStorePath,
		},
		baseURL:      baseURL,
		endpoint:     endpoint,
		issuer:       issuer,
		clientID:     boot.ClientID,
		clientSecret: boot.ClientSecret,
		admin:        adminSvc,
		httpClient:   trustedHTTPClientFromServerBundle(t, creds.ServerBundle()),
		logger:       pslog.NewStructured(context.Background(), ioDiscard{}).With("app", "lockd"),
	}
	h.start(t)
	return h
}

func (h *mcpFacadeHarness) start(t testing.TB) {
	t.Helper()

	h.mu.Lock()
	if h.cancel != nil || h.done != nil {
		h.mu.Unlock()
		t.Fatalf("mcp facade already running")
	}
	h.mu.Unlock()

	srv, err := lockdmcp.NewServer(lockdmcp.NewServerRequest{
		Config: h.cfg,
		Logger: h.logger,
	})
	if err != nil {
		t.Fatalf("new mcp facade server: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Run(runCtx)
	}()

	h.mu.Lock()
	h.cancel = cancel
	h.done = done
	h.mu.Unlock()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, h.baseURL+"/.well-known/oauth-protected-resource", http.NoBody)
		if reqErr != nil {
			t.Fatalf("build oauth metadata request: %v", reqErr)
		}
		resp, callErr := h.httpClient.Do(req)
		if callErr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("mcp facade failed readiness check at %s", h.baseURL)
}

func (h *mcpFacadeHarness) stop(t testing.TB) {
	t.Helper()

	h.mu.Lock()
	cancel := h.cancel
	done := h.done
	h.cancel = nil
	h.done = nil
	h.mu.Unlock()
	if cancel == nil || done == nil {
		return
	}

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

func (h *mcpFacadeHarness) restart(t testing.TB) {
	t.Helper()
	h.stop(t)
	h.start(t)
}

func (h *mcpFacadeHarness) authHTTPClient(token string) *http.Client {
	token = strings.TrimSpace(token)
	base := h.httpClient
	if base == nil {
		base = http.DefaultClient
	}
	clone := *base
	baseTransport := clone.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	clone.Transport = &authorizationRoundTripper{
		base:        baseTransport,
		accessToken: token,
	}
	return &clone
}

type authorizationRoundTripper struct {
	base        http.RoundTripper
	accessToken string
}

func (rt *authorizationRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	base := rt.base
	if base == nil {
		base = http.DefaultTransport
	}
	if req == nil || strings.TrimSpace(rt.accessToken) == "" {
		return base.RoundTrip(req)
	}
	clone := req.Clone(req.Context())
	clone.Header = req.Header.Clone()
	if strings.TrimSpace(clone.Header.Get("Authorization")) == "" {
		clone.Header.Set("Authorization", "Bearer "+strings.TrimSpace(rt.accessToken))
	}
	return base.RoundTrip(clone)
}

func trustedHTTPClientFromServerBundle(t testing.TB, serverBundlePEM []byte) *http.Client {
	t.Helper()
	bundle, err := tlsutil.LoadBundleFromBytes(serverBundlePEM)
	if err != nil {
		t.Fatalf("parse server bundle: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(bundle.CACertPEM) {
		t.Fatalf("append mcp ca cert to trust store")
	}
	return &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,
			},
		},
	}
}

func connectMCPClient(t testing.TB, endpoint string, progressCh chan<- *mcpsdk.ProgressNotificationClientRequest, httpClient *http.Client) (*mcpsdk.ClientSession, func()) {
	t.Helper()
	if strings.TrimSpace(endpoint) == "" {
		t.Fatal("mcp endpoint required")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
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
		Endpoint:   endpoint,
		HTTPClient: httpClient,
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

func mustCallToolObject(ctx context.Context, t testing.TB, cs *mcpsdk.ClientSession, name string, args map[string]any) map[string]any {
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
		return nil
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
		return
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

func writeFile(t testing.TB, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// ioDiscard avoids bringing in io package just for io.Discard on older toolchains.
type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
