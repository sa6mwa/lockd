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
	if !strings.Contains(text, "call lockd.hint only when namespace access is unclear") {
		t.Fatalf("expected lockd.hint guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "lockd.queue.watch") {
		t.Fatalf("expected lockd.queue.watch guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "lockd.state.stream") {
		t.Fatalf("expected lockd.state.stream guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "lockd.state.put and lockd.state.delete") {
		t.Fatalf("expected state.put/state.delete fastpath guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "in{field=/tags,any=") {
		t.Fatalf("expected tags query guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "icontains{field=/...,value=") {
		t.Fatalf("expected full-text guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "contains{f=/summary,a=\"renewal|master service agreement\"}") {
		t.Fatalf("expected contains any guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "icontains{f=/...,a=\"contract|key phrase 2\"}") {
		t.Fatalf("expected icontains any guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "date{field=/updated_at,since=yesterday}") {
		t.Fatalf("expected date selector guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "/updated_at>=2025-01-01T00:00:00Z") {
		t.Fatalf("expected temporal shorthand guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "only `date{...,since=...}` accepts `now|today|yesterday`") {
		t.Fatalf("expected temporal macro rule guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "Query enumeration: use empty query string") {
		t.Fatalf("expected match-all query guidance in instructions: %q", text)
	}
	if !strings.Contains(text, "attachment payloads are blobs and are not queryable/indexed") {
		t.Fatalf("expected attachment non-queryable guidance in instructions: %q", text)
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
	foundWatch := false
	for _, next := range out.NextCalls {
		if next == "lockd.queue.watch" {
			foundWatch = true
			break
		}
	}
	if !foundWatch {
		t.Fatalf("expected messaging next_calls to include lockd.queue.watch, got %#v", out.NextCalls)
	}
}

func TestHelpToolOverviewStartsWithQuery(t *testing.T) {
	t.Parallel()
	s := &server{cfg: Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"}}
	_, out, err := s.handleHelpTool(context.Background(), nil, helpToolInput{Topic: "overview"})
	if err != nil {
		t.Fatalf("help tool: %v", err)
	}
	if len(out.NextCalls) == 0 || out.NextCalls[0] != "lockd.query" {
		t.Fatalf("expected lockd.query as first overview next_call, got %#v", out.NextCalls)
	}
}

func TestHelpToolLQLTopicIncludesLQLResource(t *testing.T) {
	t.Parallel()
	s := &server{cfg: Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"}}
	_, out, err := s.handleHelpTool(context.Background(), nil, helpToolInput{Topic: "lql"})
	if err != nil {
		t.Fatalf("help tool: %v", err)
	}
	if out.Topic != "lql" {
		t.Fatalf("expected lql topic, got %q", out.Topic)
	}
	if len(out.Resources) == 0 || out.Resources[0] != docLQLURI {
		t.Fatalf("expected lql docs resource, got %#v", out.Resources)
	}
	if !strings.Contains(out.Summary, "eq/contains/icontains/prefix/iprefix/range/date/in/exists") {
		t.Fatalf("expected lql selector summary, got %q", out.Summary)
	}
	foundAttachmentRule := false
	for _, inv := range out.Invariants {
		if strings.Contains(inv, "attachment payloads are blobs and are not queryable or indexed") {
			foundAttachmentRule = true
			break
		}
	}
	if !foundAttachmentRule {
		t.Fatalf("expected attachment non-queryable invariant, got %#v", out.Invariants)
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

func TestLQLDocIncludesMatchAllQueryGuidance(t *testing.T) {
	t.Parallel()

	s := &server{cfg: Config{DefaultNamespace: "mcp", AgentBusQueue: "lockd.agent.bus"}}
	res, err := s.handleDocResource(context.Background(), &mcpsdk.ReadResourceRequest{
		Params: &mcpsdk.ReadResourceParams{URI: docLQLURI},
	})
	if err != nil {
		t.Fatalf("read lql resource: %v", err)
	}
	if len(res.Contents) == 0 || res.Contents[0] == nil || res.Contents[0].Text == "" {
		t.Fatalf("expected lql resource contents")
	}
	if !strings.Contains(res.Contents[0].Text, "empty query (`\"\"`)") {
		t.Fatalf("expected lql doc to include match-all guidance: %q", res.Contents[0].Text)
	}
	if !strings.Contains(res.Contents[0].Text, "date{field=/updated_at,since=now}") {
		t.Fatalf("expected lql doc to include date since examples: %q", res.Contents[0].Text)
	}
	if !strings.Contains(res.Contents[0].Text, "contains{f=/summary,a=\"Budget|Master Service Agreement|key phrase 2\"}") {
		t.Fatalf("expected lql doc to include contains any guidance: %q", res.Contents[0].Text)
	}
	if !strings.Contains(res.Contents[0].Text, "icontains{f=/...,a=\"contract|msa|key phrase 2\"}") {
		t.Fatalf("expected lql doc to include icontains any guidance: %q", res.Contents[0].Text)
	}
	if !strings.Contains(res.Contents[0].Text, "/updated_at>=2025-01-01T00:00:00Z") {
		t.Fatalf("expected lql doc to include temporal shorthand guidance: %q", res.Contents[0].Text)
	}
	if !strings.Contains(res.Contents[0].Text, "Attachment payloads are blobs and are not queryable/indexed by LQL") {
		t.Fatalf("expected lql doc to include attachment non-queryable guidance: %q", res.Contents[0].Text)
	}
}
