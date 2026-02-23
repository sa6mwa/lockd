// Package mcp provides the lockd MCP facade server.
//
// The package exposes a standalone MCP runtime that fronts an upstream lockd
// cluster through the Go lockd client SDK. It is intended for agent workflows
// that need lock/state coordination, queue messaging, query/search, and
// attachments via MCP streamable HTTP.
//
// # What this package does
//
//   - Serves MCP over streamable HTTP (default path /mcp)
//   - Registers lockd tool surface for lock/state/queue/query/attachments
//   - Hosts local OAuth 2.1 endpoints for confidential clients
//   - Enforces bearer-token auth by default when TLS is enabled
//   - Forwards queue watch activity to MCP progress notifications (SSE path)
//
// The facade process itself is stateless for lock data: lock/state/queue/query
// operations are delegated to upstream lockd.
//
// # Security defaults
//
// TLS is enabled by default and uses the configured server bundle. OAuth is
// also enabled by default (unless DisableTLS is true). OAuth state and client
// configuration are persisted under the lockd config directory, and refresh
// tokens are persisted in a sidecar JSON store.
//
// If OAuth state is missing and TLS is enabled, NewServer returns an error that
// instructs the operator to bootstrap with the CLI (`lockd mcp bootstrap`).
//
// # Queue subscription behavior
//
// On MCP session initialization, the server auto-subscribes the session to the
// configured agent-bus queue (default queue `lockd.agent.bus` in default
// namespace `mcp`). Queue activity is emitted as MCP progress notifications,
// and consumers explicitly dequeue/ack/nack/defer/extend messages.
//
// Agents should start with the `lockd.hint` tool to retrieve namespace-access
// hints derived from upstream client-bundle claims before selecting namespaces
// for lock/query/queue calls.
//
// # Constructor and lifecycle
//
// Use NewServer with NewServerRequest, then call Run with a cancellable context.
// Run blocks until context cancellation or terminal serve error.
//
// Example:
//
//	ctx := context.Background()
//
//	srv, err := mcp.NewServer(mcp.NewServerRequest{
//		Config: mcp.Config{
//			Listen:                   "127.0.0.1:19341",
//			UpstreamServer:           "https://127.0.0.1:9341",
//			UpstreamClientBundlePath: "/home/user/.lockd/client.pem",
//			BundlePath:               "/home/user/.lockd/server.pem",
//			DefaultNamespace:         "mcp",
//			AgentBusQueue:            "lockd.agent.bus",
//		},
//		Logger: logger,
//	})
//	if err != nil {
//		return err
//	}
//
//	if err := srv.Run(ctx); err != nil {
//		return err
//	}
//
// # Minimal trusted-network setup (development)
//
// DisableTLS can be used to run without TLS/OAuth enforcement in trusted local
// development environments:
//
//	srv, err := mcp.NewServer(mcp.NewServerRequest{
//		Config: mcp.Config{
//			Listen:              "127.0.0.1:19341",
//			DisableTLS:          true,
//			UpstreamServer:      "http://127.0.0.1:9341",
//			UpstreamDisableMTLS: true,
//		},
//	})
//
// # Configuration
//
// Config separates three concerns:
//
//   - MCP listener and path (`Listen`, `MCPPath`)
//   - upstream lockd client connectivity (`UpstreamServer`,
//     `UpstreamClientBundlePath`, `UpstreamDisableMTLS`)
//   - OAuth/TLS and defaults (`DisableTLS`, `BundlePath`, `OAuthStatePath`,
//     `OAuthRefreshStorePath`, `DefaultNamespace`, `AgentBusQueue`)
//
// Defaults applied by this package include:
//
//   - Listen: `127.0.0.1:19341`
//   - UpstreamServer: `https://127.0.0.1:9341`
//   - DefaultNamespace: `mcp`
//   - AgentBusQueue: `lockd.agent.bus`
//   - MCPPath: `/mcp`
//
// # Surface scope
//
// TC-only transaction decision APIs are intentionally not exposed as MCP tools
// in this package. XA coordination remains available through `txn_id` on normal
// lock/state/queue/attachment operations.
package mcp
