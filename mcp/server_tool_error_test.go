package mcp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestToolErrorsExposeStructuredEnvelopeForValidationFailures(t *testing.T) {
	t.Parallel()

	s, _ := newToolTestServer(t)
	cs, closeFn := connectMCPClientSession(t, s)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name: toolStateUpdate,
		Arguments: map[string]any{
			"key":      "",
			"lease_id": "lease-1",
		},
	})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected isError=true")
	}
	errObj := extractToolErrorObject(t, res)
	if got := toString(errObj["error_code"]); got != "invalid_argument" {
		t.Fatalf("expected error_code invalid_argument, got %q", got)
	}
}

func TestToolErrorsExposeStructuredEnvelopeForUpstreamAPIErrors(t *testing.T) {
	t.Parallel()

	s, _ := newToolTestServer(t)
	cs, closeFn := connectMCPClientSession(t, s)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name: toolStateUpdate,
		Arguments: map[string]any{
			"key":           "missing",
			"lease_id":      "lease-missing",
			"fencing_token": 1,
			"payload_text":  "{}",
		},
	})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected isError=true")
	}
	errObj := extractToolErrorObject(t, res)
	if got := toString(errObj["error_code"]); got == "" {
		t.Fatalf("expected non-empty error_code")
	}
	if status, ok := errObj["http_status"].(float64); !ok || status == 0 {
		t.Fatalf("expected http_status in structured error, got %#v", errObj["http_status"])
	}
}

func connectMCPClientSession(t *testing.T, s *server) (*mcpsdk.ClientSession, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	mcpSrv := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "test-server", Version: "0.0.1"}, &mcpsdk.ServerOptions{
		Instructions:       defaultServerInstructions(s.cfg),
		InitializedHandler: s.handleInitialized,
	})
	s.registerResources(mcpSrv)
	s.registerTools(mcpSrv)
	t1, t2 := mcpsdk.NewInMemoryTransports()
	ss, err := mcpSrv.Connect(ctx, t1, nil)
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
	return cs, func() {
		_ = cs.Close()
		_ = ss.Close()
		cancel()
	}
}

func extractToolErrorObject(t *testing.T, res *mcpsdk.CallToolResult) map[string]any {
	t.Helper()
	if res == nil {
		t.Fatalf("expected call tool result")
	}
	if len(res.Content) == 0 {
		t.Fatalf("expected error content entry")
	}
	text, ok := res.Content[0].(*mcpsdk.TextContent)
	if !ok {
		t.Fatalf("expected text content, got %T", res.Content[0])
	}
	var content map[string]any
	if err := json.Unmarshal([]byte(text.Text), &content); err != nil {
		t.Fatalf("expected json error envelope text, got %q: %v", text.Text, err)
	}
	errRaw, ok := content["error"]
	if !ok {
		t.Fatalf("expected error object in content text, got %#v", content)
	}
	errObj, ok := errRaw.(map[string]any)
	if !ok {
		t.Fatalf("expected structured error object, got %T", errRaw)
	}
	return errObj
}

func toString(v any) string {
	s, _ := v.(string)
	return s
}
