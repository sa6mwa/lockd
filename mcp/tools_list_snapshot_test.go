package mcp

import (
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
