package mcp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	presetcfg "pkt.systems/lockd/mcp/preset"
)

func TestPresetOnlySurfaceListsGeneratedTools(t *testing.T) {
	t.Parallel()

	s, _ := newToolTestServer(t)
	cs, closeFn := connectMCPClientSessionForSurface(t, s, toolSurface{
		Presets: []presetcfg.Definition{testMemoryPresetDefinition()},
	})
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tools, err := listAllTools(ctx, cs)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}

	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	joined := strings.Join(names, ",")
	if strings.Contains(joined, "lockd.help") {
		t.Fatalf("unexpected lockd tool in preset-only surface: %s", joined)
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
		if !strings.Contains(joined, want) {
			t.Fatalf("missing preset tool %q in %s", want, joined)
		}
	}
}

func TestPresetStatePutGetAndDeleteRoundTrip(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	cs, closeFn := connectMCPClientSessionForSurface(t, s, toolSurface{
		Presets: []presetcfg.Definition{testMemoryPresetDefinition()},
	})
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("preset-note-%d", time.Now().UnixNano())
	putRes, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name: "memory.note.state.put",
		Arguments: map[string]any{
			"key":  key,
			"text": "remember this",
			"tags": []any{"alpha", "beta"},
		},
	})
	if err != nil {
		t.Fatalf("state.put: %v", err)
	}
	if putRes.IsError {
		t.Fatalf("state.put returned tool error: %+v", putRes)
	}

	got, err := cli.Get(ctx, key, lockdclient.WithGetNamespace("agents"))
	if err != nil {
		t.Fatalf("direct get: %v", err)
	}
	doc, err := decodeJSONObject(got.Reader())
	if err != nil {
		t.Fatalf("decode stored doc: %v", err)
	}
	if doc["text"] != "remember this" {
		t.Fatalf("stored doc text=%v", doc["text"])
	}

	getRes, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name: "memory.note.state.get",
		Arguments: map[string]any{
			"key": key,
		},
	})
	if err != nil {
		t.Fatalf("state.get: %v", err)
	}
	if getRes.IsError {
		t.Fatalf("state.get returned tool error: %+v", getRes)
	}
	if gotKey := getRes.StructuredContent.(map[string]any)["_lockd_key"]; gotKey != key {
		t.Fatalf("_lockd_key=%v want %s", gotKey, key)
	}

	delRes, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name: "memory.note.state.delete",
		Arguments: map[string]any{
			"key": key,
		},
	})
	if err != nil {
		t.Fatalf("state.delete: %v", err)
	}
	if delRes.IsError {
		t.Fatalf("state.delete returned tool error: %+v", delRes)
	}
}

func TestPresetStateGetIncludesAttachmentMetadata(t *testing.T) {
	t.Parallel()

	s, cli := newToolTestServer(t)
	cs, closeFn := connectMCPClientSessionForSurface(t, s, toolSurface{
		Presets: []presetcfg.Definition{testMemoryPresetDefinition()},
	})
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("preset-attachment-%d", time.Now().UnixNano())
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  "agents",
		Key:        key,
		TTLSeconds: 30,
		Owner:      "preset-test",
		BlockSecs:  api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := cli.UpdateBytes(ctx, key, lease.LeaseID, []byte(`{"text":"memo"}`), lockdclient.UpdateOptions{
		Namespace: "agents",
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := cli.Attach(ctx, lockdclient.AttachRequest{
		Namespace: "agents",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
		Name:      "memo.txt",
		Body:      strings.NewReader("hello"),
	}); err != nil {
		t.Fatalf("put attachment: %v", err)
	}
	if _, err := cli.Release(ctx, api.ReleaseRequest{
		Namespace: "agents",
		Key:       key,
		LeaseID:   lease.LeaseID,
		TxnID:     lease.TxnID,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "memory.note.state.get",
		Arguments: map[string]any{"key": key},
	})
	if err != nil {
		t.Fatalf("state.get: %v", err)
	}
	if res.IsError {
		t.Fatalf("state.get returned tool error: %+v", res)
	}
	content := res.StructuredContent.(map[string]any)
	attachments, ok := content["_lockd_attachments"].([]any)
	if !ok || len(attachments) != 1 {
		t.Fatalf("attachments=%#v want one attachment", content["_lockd_attachments"])
	}
	first, ok := attachments[0].(map[string]any)
	if !ok || first["name"] != "memo.txt" {
		t.Fatalf("attachment metadata=%#v", attachments[0])
	}
}

func connectMCPClientSessionForSurface(t *testing.T, s *server, surface toolSurface) (*mcpsdk.ClientSession, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	mcpSrv := s.newMCPServerForSurface(surface)
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

func testMemoryPresetDefinition() presetcfg.Definition {
	return presetcfg.Definition{
		Name: "memory",
		Kinds: []presetcfg.Kind{{
			Name:      "note",
			Namespace: "agents",
			Schema: presetcfg.Schema{
				Type: "object",
				Properties: map[string]presetcfg.Schema{
					"text": {Type: "string"},
					"tags": {Type: "array", Items: &presetcfg.Schema{Type: "string"}},
				},
				Required: []string{"text"},
			},
		}},
	}
}
