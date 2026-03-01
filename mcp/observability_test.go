package mcp

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	mcpoauth "pkt.systems/lockd/mcp/oauth"
	mcpstate "pkt.systems/lockd/mcp/state"
	"pkt.systems/pslog"
)

func TestHTTPAccessLogsDebugWithForwardedRealIP(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := pslog.NewStructured(context.Background(), &logBuf).LogLevel(pslog.DebugLevel)
	s := &server{
		transportLog: logger,
		transferPath: "/mcp/transfer",
	}
	handler := s.withHTTPAccessLogs(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	req := httptest.NewRequest(http.MethodGet, "http://mcp.local/mcp/transfer/secret-token", http.NoBody)
	req.RemoteAddr = "10.0.0.10:9341"
	req.Header.Set(headerXForwardedFor, "203.0.113.9, 10.0.0.10")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d", rr.Code, http.StatusOK)
	}

	out := logBuf.String()
	if !strings.Contains(out, `"msg":"mcp.http.request"`) {
		t.Fatalf("expected mcp.http.request log, got %q", out)
	}
	if !strings.Contains(out, `"lvl":"debug"`) {
		t.Fatalf("expected debug log level, got %q", out)
	}
	if !strings.Contains(out, `"real_ip":"203.0.113.9"`) {
		t.Fatalf("expected real ip from x-forwarded-for, got %q", out)
	}
	if !strings.Contains(out, `"path":"/mcp/transfer/:capability"`) {
		t.Fatalf("expected sanitized transfer path, got %q", out)
	}
}

func TestHTTPAccessLogsWarnOnAuthorizationFailure(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := pslog.NewStructured(context.Background(), &logBuf).LogLevel(pslog.InfoLevel)
	s := &server{transportLog: logger}
	handler := s.withHTTPAccessLogs(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))

	req := httptest.NewRequest(http.MethodGet, "http://mcp.local/mcp", http.NoBody)
	req.RemoteAddr = "198.51.100.7:1234"
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want=%d", rr.Code, http.StatusUnauthorized)
	}

	out := logBuf.String()
	if !strings.Contains(out, `"msg":"mcp.http.request"`) {
		t.Fatalf("expected mcp.http.request log, got %q", out)
	}
	if !strings.Contains(out, `"lvl":"warn"`) {
		t.Fatalf("expected warn log level, got %q", out)
	}
	if !strings.Contains(out, `"status":401`) {
		t.Fatalf("expected 401 status field, got %q", out)
	}
	if !strings.Contains(out, `"real_ip":"198.51.100.7"`) {
		t.Fatalf("expected real ip fallback to remote addr host, got %q", out)
	}
}

func TestToolObservabilityLogsRealIP(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := pslog.NewStructured(context.Background(), &logBuf).LogLevel(pslog.InfoLevel)
	s := &server{transportLog: logger}

	handler := withToolObservability(s, "lockd.query", func(_ context.Context, _ *mcpsdk.CallToolRequest, _ struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
		return nil, struct{}{}, nil
	})
	req := &mcpsdk.CallToolRequest{
		Extra: &mcpsdk.RequestExtra{
			TokenInfo: &mcpauth.TokenInfo{UserID: "client-1"},
			Header: func() http.Header {
				h := http.Header{}
				h.Set(headerXForwardedFor, "203.0.113.44")
				return h
			}(),
		},
	}
	if _, _, err := handler(context.Background(), req, struct{}{}); err != nil {
		t.Fatalf("tool handler: %v", err)
	}

	out := logBuf.String()
	if !strings.Contains(out, `"msg":"mcp.tool.invoke"`) {
		t.Fatalf("expected mcp.tool.invoke log, got %q", out)
	}
	if !strings.Contains(out, `"lvl":"info"`) {
		t.Fatalf("expected info log level, got %q", out)
	}
	if !strings.Contains(out, `"tool":"lockd.query"`) {
		t.Fatalf("expected tool name in log, got %q", out)
	}
	if !strings.Contains(out, `"real_ip":"203.0.113.44"`) {
		t.Fatalf("expected real ip in tool log, got %q", out)
	}
}

func TestToolObservabilityWarnsForInactiveOAuthClient(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	manager, err := mcpoauth.NewManager(mcpoauth.ManagerConfig{
		StatePath:  statePath,
		TokenStore: tokenStorePath,
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := manager.RevokeClient(boot.ClientID, true); err != nil {
		t.Fatalf("revoke client: %v", err)
	}

	var logBuf bytes.Buffer
	logger := pslog.NewStructured(context.Background(), &logBuf).LogLevel(pslog.InfoLevel)
	s := &server{
		transportLog: logger,
		oauthManager: manager,
	}
	handler := withToolObservability(s, "lockd.query", func(_ context.Context, _ *mcpsdk.CallToolRequest, _ struct{}) (*mcpsdk.CallToolResult, struct{}, error) {
		t.Fatal("handler should not be called for inactive client")
		return nil, struct{}{}, nil
	})
	req := &mcpsdk.CallToolRequest{
		Extra: &mcpsdk.RequestExtra{
			TokenInfo: &mcpauth.TokenInfo{UserID: boot.ClientID},
			Header: func() http.Header {
				h := http.Header{}
				h.Set(headerXRealIP, "198.51.100.25")
				return h
			}(),
		},
	}
	_, _, err = handler(context.Background(), req, struct{}{})
	if err == nil {
		t.Fatalf("expected unauthorized error")
	}
	if !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected unauthorized error, got %v", err)
	}
	out := logBuf.String()
	if !strings.Contains(out, `"msg":"mcp.tool.invoke"`) {
		t.Fatalf("expected tool invoke log, got %q", out)
	}
	if !strings.Contains(out, `"lvl":"warn"`) {
		t.Fatalf("expected warn level, got %q", out)
	}
	if !strings.Contains(out, `"error_code":"unauthorized"`) {
		t.Fatalf("expected unauthorized error code in log, got %q", out)
	}
	if !strings.Contains(out, `"real_ip":"198.51.100.25"`) {
		t.Fatalf("expected real ip from X-Real-IP, got %q", out)
	}
}

func TestResolveRealIPFromForwardedHeader(t *testing.T) {
	t.Parallel()

	headers := http.Header{}
	headers.Set(headerForwarded, `for=198.51.100.60:1234;proto=https`)
	if got := resolveRealIP(headers, "10.0.0.1:9341"); got != "198.51.100.60" {
		t.Fatalf("real ip=%q want %q", got, "198.51.100.60")
	}
}
