package mcp

import (
	"context"
	"encoding/json"
	"io"
	"path"

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

// BuildToolsListResponse builds a canonical tools/list payload in-process.
//
// This does not start an HTTP listener, does not require OAuth/TLS, and does not
// connect to a lockd server. It only materializes the MCP tool registry.
func BuildToolsListResponse(ctx context.Context, cfg Config) (ToolsListResponse, error) {
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
		return ToolsListResponse{}, err
	}
	defer ss.Close()

	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		return ToolsListResponse{}, err
	}
	defer cs.Close()

	list, err := cs.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		return ToolsListResponse{}, err
	}

	return ToolsListResponse{
		ID:      1,
		JSONRPC: "2.0",
		Result: ToolsListResultBody{
			Tools:      list.Tools,
			NextCursor: list.NextCursor,
		},
	}, nil
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
