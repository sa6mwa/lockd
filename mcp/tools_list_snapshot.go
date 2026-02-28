package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"pkt.systems/pslog"
)

// ToolsListResponse mirrors a canonical JSON-RPC tools/list result payload.
type ToolsListResponse struct {
	ID      int                 `json:"id"`
	JSONRPC string              `json:"jsonrpc"`
	Result  ToolsListResultBody `json:"result"`
}

// ToolsListResultBody is the JSON-RPC "result" object for tools/list.
type ToolsListResultBody struct {
	Tools      []*mcpsdk.Tool `json:"tools"`
	NextCursor string         `json:"nextCursor,omitempty"`
}

type fullMCPSpecRecord struct {
	Kind  string `json:"kind"`
	Name  string `json:"name,omitempty"`
	Topic string `json:"topic,omitempty"`
	URI   string `json:"uri,omitempty"`
	Data  any    `json:"data"`
}

func newSnapshotServer(cfg Config) *server {
	applyDefaults(&cfg)

	logger := pslog.NewStructured(context.Background(), io.Discard).With("app", "lockd")
	s := &server{
		cfg:            cfg,
		logger:         logger,
		oauthLogger:    logger,
		lifecycleLog:   logger,
		transportLog:   logger,
		transferLog:    logger,
		subscribeLog:   logger,
		writeStreamLog: logger,
		mcpHTTPPath:    cleanHTTPPath(cfg.MCPPath),
	}
	s.transferPath = path.Join(s.mcpHTTPPath, "transfer")
	return s
}

func withSnapshotClient(ctx context.Context, cfg Config, fn func(*server, *mcpsdk.ClientSession) error) error {
	s := newSnapshotServer(cfg)

	client := mcpsdk.NewClient(&mcpsdk.Implementation{
		Name:    "lockd-mcp-tools-list",
		Version: "0.1.0",
	}, nil)
	mcpSrv := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "lockd-mcp-facade",
		Version: "0.1.0",
	}, &mcpsdk.ServerOptions{
		Instructions:       defaultServerInstructions(s.cfg),
		InitializedHandler: s.handleInitialized,
	})
	s.registerResources(mcpSrv)
	s.registerTools(mcpSrv)

	t1, t2 := mcpsdk.NewInMemoryTransports()
	ss, err := mcpSrv.Connect(ctx, t1, nil)
	if err != nil {
		return err
	}
	defer ss.Close()

	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		return err
	}
	defer cs.Close()

	return fn(s, cs)
}

func listAllTools(ctx context.Context, cs *mcpsdk.ClientSession) ([]*mcpsdk.Tool, error) {
	params := &mcpsdk.ListToolsParams{}
	tools := make([]*mcpsdk.Tool, 0)
	for {
		list, err := cs.ListTools(ctx, params)
		if err != nil {
			return nil, err
		}
		tools = append(tools, list.Tools...)
		if strings.TrimSpace(list.NextCursor) == "" {
			return tools, nil
		}
		params.Cursor = list.NextCursor
	}
}

func listAllResources(ctx context.Context, cs *mcpsdk.ClientSession) ([]*mcpsdk.Resource, error) {
	params := &mcpsdk.ListResourcesParams{}
	resources := make([]*mcpsdk.Resource, 0)
	for {
		list, err := cs.ListResources(ctx, params)
		if err != nil {
			return nil, err
		}
		resources = append(resources, list.Resources...)
		if strings.TrimSpace(list.NextCursor) == "" {
			return resources, nil
		}
		params.Cursor = list.NextCursor
	}
}

func listAllResourceTemplates(ctx context.Context, cs *mcpsdk.ClientSession) ([]*mcpsdk.ResourceTemplate, error) {
	params := &mcpsdk.ListResourceTemplatesParams{}
	templates := make([]*mcpsdk.ResourceTemplate, 0)
	for {
		list, err := cs.ListResourceTemplates(ctx, params)
		if err != nil {
			return nil, err
		}
		templates = append(templates, list.ResourceTemplates...)
		if strings.TrimSpace(list.NextCursor) == "" {
			return templates, nil
		}
		params.Cursor = list.NextCursor
	}
}

func listAllPrompts(ctx context.Context, cs *mcpsdk.ClientSession) ([]*mcpsdk.Prompt, error) {
	params := &mcpsdk.ListPromptsParams{}
	prompts := make([]*mcpsdk.Prompt, 0)
	for {
		list, err := cs.ListPrompts(ctx, params)
		if err != nil {
			return nil, err
		}
		prompts = append(prompts, list.Prompts...)
		if strings.TrimSpace(list.NextCursor) == "" {
			return prompts, nil
		}
		params.Cursor = list.NextCursor
	}
}

// BuildToolsListResponse builds a canonical tools/list payload in-process.
//
// This does not start an HTTP listener, does not require OAuth/TLS, and does not
// connect to a lockd server. It only materializes the MCP tool registry.
func BuildToolsListResponse(ctx context.Context, cfg Config) (ToolsListResponse, error) {
	out := ToolsListResponse{
		ID:      1,
		JSONRPC: "2.0",
	}
	if err := withSnapshotClient(ctx, cfg, func(_ *server, cs *mcpsdk.ClientSession) error {
		tools, err := listAllTools(ctx, cs)
		if err != nil {
			return err
		}
		out.Result.Tools = tools
		return nil
	}); err != nil {
		return ToolsListResponse{}, err
	}
	return out, nil
}

// BuildToolsListResponseJSON returns pretty-printed tools/list JSON payload.
func BuildToolsListResponseJSON(ctx context.Context, cfg Config) ([]byte, error) {
	resp, err := BuildToolsListResponse(ctx, cfg)
	if err != nil {
		return nil, err
	}
	out, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

// BuildFullMCPSpecJSONL returns newline-delimited JSON containing the complete
// in-process MCP snapshot for inspection.
//
// The first line is the canonical tools/list response (same shape as
// BuildToolsListResponseJSON without indentation). Subsequent lines are
// structured records describing server initialize metadata, tool specs,
// resources (and their contents), prompt/resource-template registries, and
// lockd.help topic outputs.
func BuildFullMCPSpecJSONL(ctx context.Context, cfg Config) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if err := withSnapshotClient(ctx, cfg, func(s *server, cs *mcpsdk.ClientSession) error {
		tools, err := listAllTools(ctx, cs)
		if err != nil {
			return fmt.Errorf("list tools: %w", err)
		}
		if err := enc.Encode(ToolsListResponse{
			ID:      1,
			JSONRPC: "2.0",
			Result: ToolsListResultBody{
				Tools: tools,
			},
		}); err != nil {
			return err
		}

		initResult := cs.InitializeResult()
		if initResult != nil {
			if err := enc.Encode(fullMCPSpecRecord{
				Kind: "initialize",
				Data: initResult,
			}); err != nil {
				return err
			}
			if strings.TrimSpace(initResult.Instructions) != "" {
				if err := enc.Encode(fullMCPSpecRecord{
					Kind: "instructions",
					Name: "initialize.instructions",
					Data: initResult.Instructions,
				}); err != nil {
					return err
				}
			}
		}
		if err := enc.Encode(fullMCPSpecRecord{
			Kind: "instructions",
			Name: "server.default",
			Data: defaultServerInstructions(s.cfg),
		}); err != nil {
			return err
		}

		for _, tool := range tools {
			if tool == nil {
				continue
			}
			if err := enc.Encode(fullMCPSpecRecord{
				Kind: "tool",
				Name: tool.Name,
				Data: tool,
			}); err != nil {
				return err
			}
		}

		resources, err := listAllResources(ctx, cs)
		if err != nil {
			return fmt.Errorf("list resources: %w", err)
		}
		if err := enc.Encode(fullMCPSpecRecord{
			Kind: "resources/list",
			Data: resources,
		}); err != nil {
			return err
		}
		for _, resource := range resources {
			if resource == nil {
				continue
			}
			if err := enc.Encode(fullMCPSpecRecord{
				Kind: "resource",
				URI:  resource.URI,
				Data: resource,
			}); err != nil {
				return err
			}
			readResult, err := cs.ReadResource(ctx, &mcpsdk.ReadResourceParams{URI: resource.URI})
			if err != nil {
				return fmt.Errorf("read resource %q: %w", resource.URI, err)
			}
			if err := enc.Encode(fullMCPSpecRecord{
				Kind: "resource/read",
				URI:  resource.URI,
				Data: readResult,
			}); err != nil {
				return err
			}
		}

		templates, err := listAllResourceTemplates(ctx, cs)
		if err != nil {
			return fmt.Errorf("list resource templates: %w", err)
		}
		if err := enc.Encode(fullMCPSpecRecord{
			Kind: "resources/templates/list",
			Data: templates,
		}); err != nil {
			return err
		}
		for _, template := range templates {
			if template == nil {
				continue
			}
			if err := enc.Encode(fullMCPSpecRecord{
				Kind: "resource/template",
				URI:  template.URITemplate,
				Data: template,
			}); err != nil {
				return err
			}
		}

		prompts, err := listAllPrompts(ctx, cs)
		if err != nil {
			return fmt.Errorf("list prompts: %w", err)
		}
		if err := enc.Encode(fullMCPSpecRecord{
			Kind: "prompts/list",
			Data: prompts,
		}); err != nil {
			return err
		}
		for _, prompt := range prompts {
			if prompt == nil {
				continue
			}
			if err := enc.Encode(fullMCPSpecRecord{
				Kind: "prompt",
				Name: prompt.Name,
				Data: prompt,
			}); err != nil {
				return err
			}
		}

		for _, topic := range []string{"overview", "locks", "messaging", "sync", "lql"} {
			callResult, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
				Name:      "lockd.help",
				Arguments: map[string]any{"topic": topic},
			})
			if err != nil {
				return fmt.Errorf("call lockd.help topic %q: %w", topic, err)
			}
			if err := enc.Encode(fullMCPSpecRecord{
				Kind:  "tool/call",
				Name:  "lockd.help",
				Topic: topic,
				Data:  callResult,
			}); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
