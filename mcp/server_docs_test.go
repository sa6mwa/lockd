package mcp

import (
	"context"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestDefaultServerInstructionsIncludeNamespaceAndQueue(t *testing.T) {
	t.Parallel()
	cfg := Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"}
	text := defaultServerInstructions(cfg)
	if !strings.Contains(text, "Default namespace: mcp") {
		t.Fatalf("expected namespace in instructions: %q", text)
	}
	if !strings.Contains(text, "Default coordination queue: lockd.agent.bus") {
		t.Fatalf("expected queue in instructions: %q", text)
	}
	if !strings.Contains(text, "call lockd.hint first") {
		t.Fatalf("expected lockd.hint guidance in instructions: %q", text)
	}
}

func TestHelpToolMessagingTopic(t *testing.T) {
	t.Parallel()
	s := &server{cfg: Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"}}
	_, out, err := s.handleHelpTool(context.Background(), nil, helpToolInput{Topic: "messaging"})
	if err != nil {
		t.Fatalf("help tool: %v", err)
	}
	if out.Topic != "messaging" {
		t.Fatalf("expected messaging topic, got %q", out.Topic)
	}
	if len(out.Resources) == 0 || out.Resources[0] != docMessagingURI {
		t.Fatalf("expected messaging docs resource, got %#v", out.Resources)
	}
}

func TestHelpToolOverviewIncludesHintFirst(t *testing.T) {
	t.Parallel()
	s := &server{cfg: Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"}}
	_, out, err := s.handleHelpTool(context.Background(), nil, helpToolInput{Topic: "overview"})
	if err != nil {
		t.Fatalf("help tool: %v", err)
	}
	if len(out.NextCalls) == 0 || out.NextCalls[0] != "lockd.hint" {
		t.Fatalf("expected lockd.hint as first overview next_call, got %#v", out.NextCalls)
	}
}

func TestHandleDocResourceNotFound(t *testing.T) {
	t.Parallel()
	s := &server{cfg: Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"}}
	_, err := s.handleDocResource(context.Background(), &mcpsdk.ReadResourceRequest{
		Params: &mcpsdk.ReadResourceParams{URI: "resource://docs/missing.md"},
	})
	if err == nil {
		t.Fatalf("expected resource not found error")
	}
}
