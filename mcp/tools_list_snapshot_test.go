package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestBuildToolsListResponseJSON(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := BuildToolsListResponseJSON(ctx, Config{})
	if err != nil {
		t.Fatalf("build tools list json: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty tools list json")
	}

	var decoded ToolsListResponse
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if decoded.JSONRPC != "2.0" {
		t.Fatalf("expected jsonrpc=2.0, got %q", decoded.JSONRPC)
	}
	if decoded.ID != 1 {
		t.Fatalf("expected id=1, got %d", decoded.ID)
	}
	if len(decoded.Result.Tools) == 0 {
		t.Fatalf("expected at least one tool")
	}

	found := map[string]bool{}
	for _, tool := range decoded.Result.Tools {
		if tool == nil {
			continue
		}
		found[tool.Name] = true
	}
	for _, want := range []string{"lockd.hint", "lockd.lock.acquire", "lockd.queue.dequeue"} {
		if !found[want] {
			t.Fatalf("missing tool %q in tools/list output", want)
		}
	}
}

func TestBuildFullMCPSpecJSONL(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := BuildFullMCPSpecJSONL(ctx, Config{})
	if err != nil {
		t.Fatalf("build full mcp spec jsonl: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty full spec jsonl output")
	}

	lines := bytes.Split(bytes.TrimSpace(out), []byte("\n"))
	if len(lines) < 2 {
		t.Fatalf("expected multiple jsonl lines, got %d", len(lines))
	}

	var first ToolsListResponse
	if err := json.Unmarshal(lines[0], &first); err != nil {
		t.Fatalf("decode first line tools/list response: %v", err)
	}
	if first.JSONRPC != "2.0" || first.ID != 1 {
		t.Fatalf("unexpected first line tools/list envelope: %#v", first)
	}
	if len(first.Result.Tools) == 0 {
		t.Fatalf("expected tools in first line")
	}

	kinds := make(map[string]bool)
	foundHelpLQL := false
	for _, line := range lines[1:] {
		var record struct {
			Kind  string `json:"kind"`
			Name  string `json:"name"`
			Topic string `json:"topic"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode jsonl record: %v; line=%s", err, string(line))
		}
		if record.Kind == "" {
			t.Fatalf("record kind missing: %s", string(line))
		}
		kinds[record.Kind] = true
		if record.Kind == "tool/call" && record.Name == "lockd.help" && record.Topic == "lql" {
			foundHelpLQL = true
		}
	}

	for _, kind := range []string{
		"initialize",
		"instructions",
		"tool",
		"resources/list",
		"resource/read",
		"resources/templates/list",
		"prompts/list",
		"tool/call",
	} {
		if !kinds[kind] {
			t.Fatalf("missing jsonl record kind %q", kind)
		}
	}
	if !foundHelpLQL {
		t.Fatalf("missing lockd.help lql topic call record")
	}
}
