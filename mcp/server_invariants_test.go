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
}

func TestCleanHTTPPathNormalizes(t *testing.T) {
	t.Parallel()

	if got := cleanHTTPPath(""); got != "/mcp" {
		t.Fatalf("expected /mcp, got %q", got)
	}
	if got := cleanHTTPPath("mcp"); got != "/mcp" {
		t.Fatalf("expected /mcp, got %q", got)
	}
	if got := cleanHTTPPath("/foo//bar/../mcp"); got != "/foo/mcp" {
		t.Fatalf("expected /foo/mcp, got %q", got)
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
	if _, _, err := s.handleQueryTool(context.Background(), nil, queryToolInput{Query: "eq{field=/a,value=b}", Return: "bogus"}); err == nil || !strings.Contains(err.Error(), "invalid return mode") {
		t.Fatalf("expected invalid return mode error, got %v", err)
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

func TestHandleQueueEnqueueAndDequeueUseConfiguredDefaults(t *testing.T) {
	t.Parallel()

	s, _ := newToolTestServer(t)
	ctx := context.Background()

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

	_, deq, err := s.handleQueueDequeueTool(ctx, nil, queueDequeueToolInput{BlockSecond: api.BlockNoWait})
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
