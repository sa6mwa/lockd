package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	presetcfg "pkt.systems/lockd/mcp/preset"
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
	byName := map[string]*mcpsdk.Tool{}
	for _, tool := range decoded.Result.Tools {
		if tool == nil {
			continue
		}
		found[tool.Name] = true
		byName[tool.Name] = tool
	}
	for _, want := range []string{"lockd.hint", "lockd.lock.acquire", "lockd.state.put", "lockd.state.delete", "lockd.queue.dequeue"} {
		if !found[want] {
			t.Fatalf("missing tool %q in tools/list output", want)
		}
	}
	mustTool := func(name string) *mcpsdk.Tool {
		t.Helper()
		tool, ok := byName[name]
		if !ok || tool == nil {
			t.Fatalf("missing tool %q", name)
		}
		return tool
	}
	for _, tool := range decoded.Result.Tools {
		if tool == nil {
			continue
		}
		if tool.Annotations == nil || tool.Annotations.OpenWorldHint == nil || *tool.Annotations.OpenWorldHint {
			t.Fatalf("%s should set openWorldHint=false explicitly: %#v", tool.Name, tool.Annotations)
		}
	}
	if ann := mustTool("lockd.state.update").Annotations; ann == nil || ann.DestructiveHint == nil || *ann.DestructiveHint {
		t.Fatalf("lockd.state.update should be marked non-destructive: %#v", ann)
	}
	if ann := mustTool("lockd.state.put").Annotations; ann == nil || ann.DestructiveHint == nil || *ann.DestructiveHint {
		t.Fatalf("lockd.state.put should be marked non-destructive: %#v", ann)
	}
	if ann := mustTool("lockd.state.delete").Annotations; ann == nil || ann.DestructiveHint == nil || !*ann.DestructiveHint {
		t.Fatalf("lockd.state.delete should be marked destructive: %#v", ann)
	}
	if ann := mustTool("lockd.state.remove").Annotations; ann == nil || ann.DestructiveHint == nil || !*ann.DestructiveHint {
		t.Fatalf("lockd.state.remove should be marked destructive: %#v", ann)
	}
	if ann := mustTool("lockd.attachments.delete").Annotations; ann == nil || ann.DestructiveHint == nil || !*ann.DestructiveHint {
		t.Fatalf("lockd.attachments.delete should be marked destructive: %#v", ann)
	}
	if ann := mustTool("lockd.attachments.delete_all").Annotations; ann == nil || ann.DestructiveHint == nil || !*ann.DestructiveHint {
		t.Fatalf("lockd.attachments.delete_all should be marked destructive: %#v", ann)
	}
	if ann := mustTool("lockd.query").Annotations; ann == nil || ann.DestructiveHint == nil || *ann.DestructiveHint || !ann.ReadOnlyHint {
		t.Fatalf("lockd.query should be read-only/non-destructive: %#v", ann)
	}
	if ann := mustTool("lockd.queue.enqueue").Annotations; ann == nil || ann.DestructiveHint == nil || *ann.DestructiveHint {
		t.Fatalf("lockd.queue.enqueue should be non-destructive: %#v", ann)
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

func TestBuildFullMCPSpecJSONLForClientSurface(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := BuildFullMCPSpecJSONLForClientSurface(ctx, Config{}, false, []presetcfg.Definition{
		{
			Name: "memory",
			Kinds: []presetcfg.Kind{
				{
					Name:      "note",
					Namespace: "memory",
					Schema: presetcfg.Schema{
						Type: "object",
						Properties: map[string]presetcfg.Schema{
							"text": {Type: "string"},
						},
						Required: []string{"text"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("build client full mcp spec jsonl: %v", err)
	}
	lines := bytes.Split(bytes.TrimSpace(out), []byte("\n"))
	if len(lines) < 2 {
		t.Fatalf("expected multiple jsonl lines, got %d", len(lines))
	}

	var first ToolsListResponse
	if err := json.Unmarshal(lines[0], &first); err != nil {
		t.Fatalf("decode first line tools/list response: %v", err)
	}
	toolNames := make([]string, 0, len(first.Result.Tools))
	for _, tool := range first.Result.Tools {
		if tool == nil {
			continue
		}
		toolNames = append(toolNames, tool.Name)
	}
	if len(toolNames) == 0 {
		t.Fatalf("expected client surface tools")
	}
	for _, name := range toolNames {
		if strings.HasPrefix(name, "lockd.") {
			t.Fatalf("did not expect lockd tool in memory-only surface: %q", name)
		}
	}
	for _, want := range []string{
		"memory.help",
		"memory.note.query",
		"memory.note.state.put",
		"memory.note.state.get",
		"memory.note.state.delete",
		"memory.note.queue.enqueue",
		"memory.note.attachments.get",
	} {
		found := false
		for _, name := range toolNames {
			if name == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing preset tool %q in client surface: %v", want, toolNames)
		}
	}
	foundPresetHelpCall := false
	for _, line := range lines[1:] {
		var record fullMCPSpecRecord
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode full mcp spec record: %v", err)
		}
		if record.Kind == "tool/call" && record.Name == "memory.help" {
			foundPresetHelpCall = true
			break
		}
	}
	if !foundPresetHelpCall {
		t.Fatalf("missing preset help tool/call record in client surface snapshot")
	}
}
