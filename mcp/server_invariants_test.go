package mcp

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/pslog"
)

func TestValidateConfigRequiresFields(t *testing.T) {
	t.Parallel()

	if err := validateConfig(Config{}); err == nil || !strings.Contains(err.Error(), "listen address required") {
		t.Fatalf("expected listen address error, got %v", err)
	}
	if err := validateConfig(Config{Listen: "127.0.0.1:19341"}); err == nil || !strings.Contains(err.Error(), "upstream lockd server required") {
		t.Fatalf("expected upstream server error, got %v", err)
	}
	if err := validateConfig(Config{Listen: "127.0.0.1:19341", UpstreamServer: "http://127.0.0.1:9341"}); err == nil || !strings.Contains(err.Error(), "mcp base URL required") {
		t.Fatalf("expected base URL error, got %v", err)
	}
}

func TestCleanHTTPPathNormalizes(t *testing.T) {
	t.Parallel()

	if got := cleanHTTPPath(""); got != "/" {
		t.Fatalf("expected /, got %q", got)
	}
	if got := cleanHTTPPath("mcp"); got != "/mcp" {
		t.Fatalf("expected /mcp, got %q", got)
	}
	if got := cleanHTTPPath("/foo//bar/../mcp"); got != "/foo/mcp" {
		t.Fatalf("expected /foo/mcp, got %q", got)
	}
}

func TestResolveConfigDefaultsMCPPathRoot(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	applyDefaults(&cfg)
	if got := cfg.MCPPath; got != "/" {
		t.Fatalf("expected default MCPPath=/, got %q", got)
	}

	cfg = Config{MCPPath: "/mcp/v1"}
	applyDefaults(&cfg)
	if got := cfg.MCPPath; got != "/mcp/v1" {
		t.Fatalf("expected explicit MCPPath preserved, got %q", got)
	}
}

func TestTransferURLJoinsBaseAndDocroot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		baseURL string
		docroot string
		want    string
	}{
		{
			name:    "base path with root docroot",
			baseURL: "https://myserver/lockdmcp/v1",
			docroot: "/",
			want:    "https://myserver/lockdmcp/v1/transfer/cap-123",
		},
		{
			name:    "base path merged with nested docroot",
			baseURL: "https://mybase/mcp",
			docroot: "/mcp/v1",
			want:    "https://mybase/mcp/mcp/v1/transfer/cap-123",
		},
		{
			name:    "root base with docroot",
			baseURL: "https://mydomain",
			docroot: "/mcp",
			want:    "https://mydomain/mcp/transfer/cap-123",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base, err := parseBaseURL(tt.baseURL, false)
			if err != nil {
				t.Fatalf("parse base url: %v", err)
			}
			s := &server{
				baseURL:     base,
				mcpHTTPPath: cleanHTTPPath(tt.docroot),
			}
			if got := s.transferURL("cap-123"); got != tt.want {
				t.Fatalf("unexpected transfer url: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestResolveDefaultsAndOwner(t *testing.T) {
	t.Parallel()

	s := &server{cfg: Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"}}
	if got := s.resolveNamespace(""); got != "mcp" {
		t.Fatalf("expected default namespace mcp, got %q", got)
	}
	if got := s.resolveNamespace("agents"); got != "agents" {
		t.Fatalf("expected namespace override agents, got %q", got)
	}
	if got := s.resolveQueue(""); got != "lockd.agent.bus" {
		t.Fatalf("expected default queue lockd.agent.bus, got %q", got)
	}
	if got := s.resolveQueue("work"); got != "work" {
		t.Fatalf("expected queue override work, got %q", got)
	}
	if got := defaultOwner(""); got != "mcp-worker" {
		t.Fatalf("expected fallback owner mcp-worker, got %q", got)
	}
	if got := defaultOwner("client-123"); got != "mcp-client-123" {
		t.Fatalf("expected derived owner mcp-client-123, got %q", got)
	}
}

func TestHandleQueryToolValidationErrors(t *testing.T) {
	t.Parallel()

	s := &server{}
	if _, _, err := s.handleQueryTool(context.Background(), nil, queryToolInput{}); err == nil || !strings.Contains(err.Error(), "query is required") {
		t.Fatalf("expected query required error, got %v", err)
	}
	if _, _, err := s.handleQueryTool(context.Background(), nil, queryToolInput{Query: "eq{field=/a,value=b}", Engine: "bogus"}); err == nil || !strings.Contains(err.Error(), "invalid engine") {
		t.Fatalf("expected invalid engine error, got %v", err)
	}
}

func TestHandleGetToolReadModeValidation(t *testing.T) {
	t.Parallel()

	s := &server{}
	ctx := context.Background()

	if _, _, err := s.handleGetTool(ctx, nil, getToolInput{
		Key:     "k1",
		Public:  boolPtr(true),
		LeaseID: "lease-1",
	}); err == nil || !strings.Contains(err.Error(), "lease_id must be empty when public=true") {
		t.Fatalf("expected public=true lease validation error, got %v", err)
	}

	if _, _, err := s.handleGetTool(ctx, nil, getToolInput{
		Key:    "k1",
		Public: boolPtr(false),
	}); err == nil || !strings.Contains(err.Error(), "lease_id is required when public=false") {
		t.Fatalf("expected public=false lease validation error, got %v", err)
	}
}

func TestAttachmentReadModeValidation(t *testing.T) {
	t.Parallel()

	s := &server{}
	ctx := context.Background()

	if _, _, err := s.handleAttachmentListTool(ctx, nil, attachmentListToolInput{
		Key:     "k1",
		Public:  boolPtr(true),
		LeaseID: "lease-1",
	}); err == nil || !strings.Contains(err.Error(), "lease_id must be empty when public=true") {
		t.Fatalf("expected list public=true lease validation error, got %v", err)
	}

	if _, _, err := s.handleAttachmentListTool(ctx, nil, attachmentListToolInput{
		Key:    "k1",
		Public: boolPtr(false),
	}); err == nil || !strings.Contains(err.Error(), "lease_id is required when public=false") {
		t.Fatalf("expected list public=false lease validation error, got %v", err)
	}

	if _, _, err := s.handleAttachmentGetTool(ctx, nil, attachmentGetToolInput{
		Key:     "k1",
		ID:      "att-1",
		Public:  boolPtr(true),
		LeaseID: "lease-1",
	}); err == nil || !strings.Contains(err.Error(), "lease_id must be empty when public=true") {
		t.Fatalf("expected get public=true lease validation error, got %v", err)
	}

	if _, _, err := s.handleAttachmentGetTool(ctx, nil, attachmentGetToolInput{
		Key:    "k1",
		ID:     "att-1",
		Public: boolPtr(false),
	}); err == nil || !strings.Contains(err.Error(), "lease_id is required when public=false") {
		t.Fatalf("expected get public=false lease validation error, got %v", err)
	}

	if _, _, err := s.handleAttachmentHeadTool(ctx, nil, attachmentHeadToolInput{
		Key:     "k1",
		ID:      "att-1",
		Public:  boolPtr(true),
		LeaseID: "lease-1",
	}); err == nil || !strings.Contains(err.Error(), "lease_id must be empty when public=true") {
		t.Fatalf("expected head public=true lease validation error, got %v", err)
	}

	if _, _, err := s.handleAttachmentHeadTool(ctx, nil, attachmentHeadToolInput{
		Key:    "k1",
		ID:     "att-1",
		Public: boolPtr(false),
	}); err == nil || !strings.Contains(err.Error(), "lease_id is required when public=false") {
		t.Fatalf("expected head public=false lease validation error, got %v", err)
	}
}

func TestAttachmentPutModeValidation(t *testing.T) {
	t.Parallel()

	s := &server{}
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()
	_, _, err := s.handleAttachmentsWriteStreamBeginTool(context.Background(), &mcpsdk.CallToolRequest{Session: session}, attachmentsWriteStreamBeginInput{
		Key:     "k1",
		LeaseID: "lease-1",
		Name:    "att.txt",
		Mode:    "bogus",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid mode") {
		t.Fatalf("expected invalid mode error, got %v", err)
	}
}

func TestPayloadInputsMutuallyExclusive(t *testing.T) {
	t.Parallel()

	s := &server{}
	if _, _, err := s.handleStateUpdateTool(context.Background(), nil, stateUpdateToolInput{
		Key:           "k1",
		LeaseID:       "lease-1",
		PayloadText:   "{}",
		PayloadBase64: "e30=",
	}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected state update payload exclusivity error, got %v", err)
	}
	if _, _, err := s.handleStatePatchTool(context.Background(), nil, statePatchToolInput{
		Key:         "k1",
		LeaseID:     "lease-1",
		PatchText:   `{}`,
		PatchBase64: "e30=",
	}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected state patch payload exclusivity error, got %v", err)
	}
	if _, _, err := s.handleQueueEnqueueTool(context.Background(), nil, queueEnqueueToolInput{
		PayloadText:   "x",
		PayloadBase64: "eA==",
	}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected queue enqueue payload exclusivity error, got %v", err)
	}
}

func TestQueueDecisionToolsRequireLeaseMaterial(t *testing.T) {
	t.Parallel()

	s := &server{cfg: Config{DefaultNamespace: "mcp"}}
	ctx := context.Background()

	if _, _, err := s.handleQueueAckTool(ctx, nil, queueAckToolInput{}); err == nil || !strings.Contains(err.Error(), "queue, message_id, lease_id, and meta_etag are required") {
		t.Fatalf("expected ack validation error, got %v", err)
	}
	if _, _, err := s.handleQueueNackTool(ctx, nil, queueNackToolInput{}); err == nil || !strings.Contains(err.Error(), "queue, message_id, lease_id, and meta_etag are required") {
		t.Fatalf("expected nack validation error, got %v", err)
	}
	if _, _, err := s.handleQueueDeferTool(ctx, nil, queueDeferToolInput{}); err == nil || !strings.Contains(err.Error(), "queue, message_id, lease_id, and meta_etag are required") {
		t.Fatalf("expected defer validation error, got %v", err)
	}
	if _, _, err := s.handleQueueExtendTool(ctx, nil, queueExtendToolInput{}); err == nil || !strings.Contains(err.Error(), "queue, message_id, lease_id, and meta_etag are required") {
		t.Fatalf("expected extend validation error, got %v", err)
	}
}

func TestQueueSubscribeToolsRequireSession(t *testing.T) {
	t.Parallel()

	s := &server{cfg: Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"}}
	if _, _, err := s.handleQueueSubscribeTool(context.Background(), nil, queueSubscribeToolInput{}); err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected subscribe session error, got %v", err)
	}
	if _, _, err := s.handleQueueUnsubscribeTool(context.Background(), nil, queueUnsubscribeToolInput{}); err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected unsubscribe session error, got %v", err)
	}
}

func TestStateStreamToolRequiresSession(t *testing.T) {
	t.Parallel()

	s := &server{}
	_, _, err := s.handleStateStreamTool(context.Background(), nil, stateStreamToolInput{Key: "k1"})
	if err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected state stream session error, got %v", err)
	}
}

func TestAttachmentStreamToolRequiresSession(t *testing.T) {
	t.Parallel()

	s := &server{}
	_, _, err := s.handleAttachmentStreamTool(context.Background(), nil, attachmentStreamToolInput{
		Key:  "k1",
		Name: "att1",
	})
	if err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected attachment stream session error, got %v", err)
	}
}

func TestQueryStreamToolRequiresSession(t *testing.T) {
	t.Parallel()

	s := &server{}
	_, _, err := s.handleQueryStreamTool(context.Background(), nil, queryStreamToolInput{
		Query: "eq{field=/a,value=b}",
	})
	if err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected query stream session error, got %v", err)
	}
}

func TestWriteStreamToolsRequireSession(t *testing.T) {
	t.Parallel()

	s := &server{}
	ctx := context.Background()
	if _, _, err := s.handleStateWriteStreamBeginTool(ctx, nil, stateWriteStreamBeginInput{Key: "k1", LeaseID: "l1"}); err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected state write stream begin session error, got %v", err)
	}
	if _, _, err := s.handleQueueWriteStreamBeginTool(ctx, nil, queueWriteStreamBeginInput{}); err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected queue write stream begin session error, got %v", err)
	}
	if _, _, err := s.handleAttachmentsWriteStreamBeginTool(ctx, nil, attachmentsWriteStreamBeginInput{Key: "k1", LeaseID: "l1", Name: "a.bin"}); err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected attachments write stream begin session error, got %v", err)
	}
	if _, _, err := s.handleQueueWriteStreamCommitTool(ctx, nil, writeStreamCommitInput{StreamID: "s1"}); err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected queue write stream commit session error, got %v", err)
	}
	if _, _, err := s.handleAttachmentsWriteStreamAbortTool(ctx, nil, writeStreamAbortInput{StreamID: "s1"}); err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected attachments write stream abort session error, got %v", err)
	}
}

func TestQueueDequeueRequiresSessionForPayloadStreaming(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	ctx := context.Background()
	queue := "session-required-queue"
	if _, err := cli.EnqueueBytes(ctx, queue, []byte(`{"kind":"session-required"}`), lockdclient.EnqueueOptions{
		Namespace: "mcp",
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, _, err := s.handleQueueDequeueTool(ctx, nil, queueDequeueToolInput{
		Namespace:   "mcp",
		Queue:       queue,
		BlockSecond: api.BlockNoWait,
		PayloadMode: "stream",
	})
	if err == nil || !strings.Contains(err.Error(), "active MCP session") {
		t.Fatalf("expected active MCP session error for payload streaming, got %v", err)
	}
}

func TestHandleQueueEnqueueAndDequeueUseConfiguredDefaults(t *testing.T) {
	t.Parallel()

	s, _ := newToolTestServer(t)
	ctx := context.Background()
	session, closeSession := newInMemoryServerSession(t)
	defer closeSession()

	_, enq, err := s.handleQueueEnqueueTool(ctx, nil, queueEnqueueToolInput{PayloadText: `{"kind":"default-route"}`})
	if err != nil {
		t.Fatalf("enqueue tool: %v", err)
	}
	if enq.Namespace != "mcp" {
		t.Fatalf("expected default namespace mcp, got %q", enq.Namespace)
	}
	if enq.Queue != "lockd.agent.bus" {
		t.Fatalf("expected default queue lockd.agent.bus, got %q", enq.Queue)
	}

	_, deq, err := s.handleQueueDequeueTool(ctx, &mcpsdk.CallToolRequest{Session: session}, queueDequeueToolInput{BlockSecond: api.BlockNoWait})
	if err != nil {
		t.Fatalf("dequeue tool: %v", err)
	}
	if !deq.Found {
		t.Fatalf("expected queued message to be found")
	}
	if deq.Namespace != "mcp" || deq.Queue != "lockd.agent.bus" {
		t.Fatalf("unexpected dequeue target namespace=%q queue=%q", deq.Namespace, deq.Queue)
	}
}

func TestHandleInitializedAutoSubscribesDefaultQueue(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	transportServer := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "test-server", Version: "0.0.1"}, nil)
	t1, t2 := mcpsdk.NewInMemoryTransports()
	ss, err := transportServer.Connect(ctx, t1, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cs.Close()
	defer ss.Close()

	logger := pslog.NewStructured(context.Background(), io.Discard)
	manager := newSubscriptionManager(&fakeQueueWatcher{}, logger)
	defer manager.Close()

	s := &server{
		cfg:           Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"},
		subscriptions: manager,
		subscribeLog:  logger,
	}

	s.handleInitialized(ctx, &mcpsdk.InitializedRequest{
		Session: ss,
		Extra: &mcpsdk.RequestExtra{TokenInfo: &mcpauth.TokenInfo{
			UserID: "agent-1",
		}},
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		manager.mu.Lock()
		var got *queueSubscription
		for _, sub := range manager.subs {
			got = sub
			break
		}
		manager.mu.Unlock()
		if got != nil {
			if got.clientID != "agent-1" {
				t.Fatalf("expected subscribed client id agent-1, got %q", got.clientID)
			}
			if got.namespace != "mcp" || got.queue != "lockd.agent.bus" {
				t.Fatalf("unexpected subscription namespace=%q queue=%q", got.namespace, got.queue)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for auto-subscription")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNewServerOAuthBootstrapRequirement(t *testing.T) {
	t.Parallel()

	ts := lockd.StartTestServer(t, lockd.WithoutTestMTLS())
	missingState := filepath.Join(t.TempDir(), "missing-mcp.pem")

	_, err := NewServer(NewServerRequest{
		Config: Config{
			DisableTLS:          false,
			UpstreamServer:      ts.URL(),
			BaseURL:             "https://127.0.0.1",
			UpstreamDisableMTLS: true,
			OAuthStatePath:      missingState,
		},
		Logger: pslog.NewStructured(context.Background(), io.Discard),
	})
	if err == nil || !strings.Contains(err.Error(), "run 'lockd mcp bootstrap'") {
		t.Fatalf("expected bootstrap guidance error, got %v", err)
	}
}

func TestNewServerAllowsTLSDisabledWithoutBootstrapState(t *testing.T) {
	t.Parallel()

	ts := lockd.StartTestServer(t, lockd.WithoutTestMTLS())
	missingState := filepath.Join(t.TempDir(), "missing-mcp.pem")

	srv, err := NewServer(NewServerRequest{
		Config: Config{
			DisableTLS:          true,
			UpstreamServer:      ts.URL(),
			BaseURL:             "http://127.0.0.1",
			AllowHTTP:           true,
			UpstreamDisableMTLS: true,
			OAuthStatePath:      missingState,
		},
		Logger: pslog.NewStructured(context.Background(), io.Discard),
	})
	if err != nil {
		t.Fatalf("new server with TLS disabled: %v", err)
	}
	impl, ok := srv.(*server)
	if !ok {
		t.Fatalf("expected concrete *server type")
	}
	if impl.oauthManager != nil {
		t.Fatalf("expected oauth manager to be disabled when disable_tls=true")
	}
	if impl.subscriptions != nil {
		impl.subscriptions.Close()
	}
	if impl.upstream != nil {
		_ = impl.upstream.Close()
	}
}

func boolPtr(v bool) *bool {
	b := v
	return &b
}
