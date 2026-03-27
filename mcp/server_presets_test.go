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

func TestBuiltInMemoryPresetToolDescriptionsAreOperationallyDetailed(t *testing.T) {
	t.Parallel()

	def, ok := presetcfg.BuiltInDefinition("memory")
	if !ok {
		t.Fatalf("expected built-in memory preset")
	}
	s, _ := newToolTestServer(t)
	cs, closeFn := connectMCPClientSessionForSurface(t, s, toolSurface{
		Presets: []presetcfg.Definition{def},
	})
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tools, err := listAllTools(ctx, cs)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}

	byName := make(map[string]*mcpsdk.Tool, len(tools))
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		byName[tool.Name] = tool
	}
	mustDesc := func(name string) string {
		t.Helper()
		tool := byName[name]
		if tool == nil {
			t.Fatalf("missing tool %q", name)
		}
		desc := strings.TrimSpace(tool.Description)
		for _, section := range []string{"Purpose:", "Use when:", "Requires:", "Effects:", "Retry:", "Next:"} {
			if !strings.Contains(desc, section) {
				t.Fatalf("tool %s missing %s section: %q", name, section, desc)
			}
		}
		return desc
	}

	helpDesc := mustDesc("memory.help")
	if !strings.Contains(helpDesc, "call this tool first") {
		t.Fatalf("memory.help should direct discovery-first usage: %q", helpDesc)
	}
	if !strings.Contains(helpDesc, "PRESET PURPOSE: Shared AI memory and recall documents") {
		t.Fatalf("memory.help should include preset purpose from config: %q", helpDesc)
	}

	queryDesc := mustDesc("memory.bookmarks.query")
	for _, phrase := range []string{
		"RECORD PURPOSE: Saved URLs and references with titles, summaries, and retrieval tags.",
		"`query` is lockd LQL",
		"Use empty query string (`\"\"`) to enumerate all keys",
		"`cursor` continues pagination from a previous result",
		"/title=\"Quarterly plan\"",
		"icontains{field=/summary,value=benchmark}",
	} {
		if !strings.Contains(queryDesc, phrase) {
			t.Fatalf("memory.bookmarks.query missing phrase %q: %q", phrase, queryDesc)
		}
	}
	queryTool := byName["memory.bookmarks.query"]
	if queryTool == nil {
		t.Fatalf("missing tool %q", "memory.bookmarks.query")
	}
	inputSchema, ok := queryTool.InputSchema.(map[string]any)
	if !ok {
		t.Fatalf("memory.bookmarks.query input schema type = %T, want map[string]any", queryTool.InputSchema)
	}
	properties, ok := inputSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("memory.bookmarks.query input schema missing properties: %#v", inputSchema)
	}
	for _, field := range []string{"engine", "refresh", "fields"} {
		if _, ok := properties[field]; ok {
			t.Fatalf("memory.bookmarks.query should not expose %q in preset query schema", field)
		}
	}
	for field, phrase := range map[string]string{
		"query":  "Use empty string to enumerate all keys",
		"limit":  "Maximum number of keys to return in this page",
		"cursor": "Opaque continuation cursor",
	} {
		entry, ok := properties[field].(map[string]any)
		if !ok {
			t.Fatalf("memory.bookmarks.query field %q schema = %#v", field, properties[field])
		}
		desc, _ := entry["description"].(string)
		if !strings.Contains(desc, phrase) {
			t.Fatalf("memory.bookmarks.query field %q description %q missing %q", field, desc, phrase)
		}
	}

	statePutDesc := mustDesc("memory.contacts.state.put")
	for _, phrase := range []string{
		"Create or replace one `memory`",
		"Required fields for this kind are `key`, `name`",
		"schema as the durable API contract",
	} {
		if !strings.Contains(statePutDesc, phrase) {
			t.Fatalf("memory.contacts.state.put missing phrase %q: %q", phrase, statePutDesc)
		}
	}

	stateGetDesc := mustDesc("memory.reminders.state.get")
	if !strings.Contains(stateGetDesc, "_lockd_attachments") {
		t.Fatalf("memory.reminders.state.get should explain attachment metadata contract: %q", stateGetDesc)
	}

	helpRes, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "memory.help",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("memory.help call: %v", err)
	}
	if helpRes.IsError {
		t.Fatalf("memory.help returned tool error: %+v", helpRes)
	}
	helpPayload, ok := helpRes.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("memory.help payload type = %T", helpRes.StructuredContent)
	}
	if got := helpPayload["description"]; got != def.Description {
		t.Fatalf("memory.help description=%v want %q", got, def.Description)
	}
	kinds, ok := helpPayload["kinds"].([]any)
	if !ok || len(kinds) == 0 {
		t.Fatalf("memory.help kinds=%#v", helpPayload["kinds"])
	}
	firstKind, ok := kinds[0].(map[string]any)
	if !ok {
		t.Fatalf("memory.help first kind=%#v", kinds[0])
	}
	if strings.TrimSpace(fmt.Sprint(firstKind["description"])) == "" {
		t.Fatalf("memory.help kind description missing: %#v", firstKind)
	}
	workflow, ok := helpPayload["workflow"].([]any)
	if !ok || len(workflow) < 3 {
		t.Fatalf("memory.help workflow=%#v", helpPayload["workflow"])
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
