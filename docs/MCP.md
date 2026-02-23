# Lockd MCP

This document covers both:

- CLI operation via `lockd mcp`
- Go SDK embedding via package `pkt.systems/lockd/mcp`

## Overview

The lockd MCP service is a dedicated facade process that exposes lockd capabilities through MCP streamable HTTP. It is designed for agent coordination workloads:

- lock/state coordination with leases
- queue-based messaging
- query/search over namespaced state
- attachment exchange

The MCP facade acts as a normal lockd client toward upstream lockd. In v1, authorization boundaries are inherited from the upstream client certificate and its namespace access claims.

## Runtime Model

`lockd mcp` runs as an independent service (first iteration), not yet embedded in `lockd --with-mcp`.

Core properties:

- MCP listen default: `127.0.0.1:19341`
- MCP endpoint path default: `/mcp`
- TLS enabled by default
- Local OAuth 2.1 provider enabled by default (`/authorize`, `/token`, metadata endpoints)
- default namespace: `mcp`
- default coordination queue: `lockd.agent.bus`
- each MCP session auto-subscribes to `mcp/lockd.agent.bus`

## Security Model

### Transport and server identity

By default, MCP serves over TLS using the server bundle (`--bundle` / `mcp.bundle`), same trust model as lockd server TLS bundle usage.

### Authentication

By default, MCP requires bearer tokens. The facade hosts OAuth endpoints itself:

- `GET /authorize`
- `POST /token`
- `GET /.well-known/oauth-authorization-server`
- `GET /.well-known/oauth-protected-resource`

No external OAuth service is required.

### OAuth persistence

- OAuth state (issuer, confidential clients, encrypted secret material): `mcp.state-file` (default under lockd config dir, usually `~/.lockd/mcp.pem`)
- refresh tokens: `mcp.refresh-store` (default `~/.lockd/mcp-auth-store.json`)

Refresh tokens are intentionally stored outside the PEM state file.

### Live reload

OAuth client state is reloaded from disk when it changes, so client add/revoke/update operations are applied without restarting `lockd mcp`.

## Quick Start (CLI)

1. Bootstrap OAuth material and first confidential client:

```bash
lockd mcp bootstrap --client-name default
```

2. Print agent configuration values for that client:

```bash
lockd mcp oauth client credentials --id <client_id> --format env
```

3. Start MCP facade:

```bash
lockd mcp \
  --server https://127.0.0.1:9341 \
  --client-bundle ~/.lockd/client.pem \
  --bundle ~/.lockd/server.pem \
  --listen 127.0.0.1:19341
```

If OAuth state is missing and TLS is enabled, startup fails with an explicit bootstrap action.

## CLI Reference

### `lockd mcp` serve flags

- `--listen`, `-l`: MCP listen address (default `127.0.0.1:19341`)
- `--server`, `-s`: upstream lockd server (same semantics as `lockd client`)
- `--client-bundle`, `-B`: upstream lockd client bundle used by MCP
- `--bundle`, `-b`: MCP TLS server bundle
- `--disable-tls`: disable TLS and disable OAuth bearer-token enforcement
- `--disable-mcp-upstream-mtls`: disable mTLS for MCP -> upstream lockd
- `--default-namespace`: default namespace when tools omit namespace (default `mcp`)
- `--agent-bus-queue`: default queue and auto-subscribe queue (default `lockd.agent.bus`)
- `--state-file`: OAuth state path
- `--refresh-store`: refresh-token store path
- `--issuer`: bootstrap/oauth issuer value
- `--mcp-path`: streamable MCP HTTP path (default `/mcp`)
- `--oauth-resource-url`: protected resource identifier URL

### `lockd mcp oauth` admin surface

- `lockd mcp oauth issuer get`
- `lockd mcp oauth issuer set --issuer <url>`
- `lockd mcp oauth client list`
- `lockd mcp oauth client show --id <client_id>`
- `lockd mcp oauth client credentials --id <client_id> [--rotate-secret] [--format env|json]`
- `lockd mcp oauth client add --name <name> [--scope ...]`
- `lockd mcp oauth client update --id <client_id> [--name ...] [--scope ...]`
- `lockd mcp oauth client revoke --id <client_id>`
- `lockd mcp oauth client restore --id <client_id>`
- `lockd mcp oauth client remove --id <client_id>`
- `lockd mcp oauth client rotate-secret --id <client_id>`

All `--id` fields support Cobra completion.

### Other subcommands

- `lockd mcp bootstrap`
- `lockd mcp ca-export [--bundle ...] [--out ...]`

## Config and Environment Mapping

All MCP settings are exposed through viper keys and `LOCKD_` env vars.

- `mcp.listen` -> `LOCKD_MCP_LISTEN`
- `mcp.server` -> `LOCKD_MCP_SERVER`
- `mcp.client_bundle` -> `LOCKD_MCP_CLIENT_BUNDLE`
- `mcp.bundle` -> `LOCKD_MCP_BUNDLE`
- `mcp.disable_tls` -> `LOCKD_MCP_DISABLE_TLS`
- `mcp.disable_mtls` -> `LOCKD_MCP_DISABLE_MTLS`
- `mcp.default_namespace` -> `LOCKD_MCP_DEFAULT_NAMESPACE`
- `mcp.agent_bus_queue` -> `LOCKD_MCP_AGENT_BUS_QUEUE`
- `mcp.state_file` -> `LOCKD_MCP_STATE_FILE`
- `mcp.refresh_store` -> `LOCKD_MCP_REFRESH_STORE`
- `mcp.issuer` -> `LOCKD_MCP_ISSUER`
- `mcp.path` -> `LOCKD_MCP_PATH`
- `mcp.oauth_resource_url` -> `LOCKD_MCP_OAUTH_RESOURCE_URL`

Config file YAML uses dashed dotted keys (for example `mcp.default-namespace`).

## MCP Tool Surface

Discovery and docs:

- `lockd.help`
- resource docs under `resource://docs/*`

Lock/state:

- `lockd.lock.acquire`
- `lockd.lock.keepalive`
- `lockd.lock.release`
- `lockd.get`
- `lockd.describe`
- `lockd.query`
- `lockd.state.update`
- `lockd.state.metadata`
- `lockd.state.remove`

Attachments:

- `lockd.attachments.put`
- `lockd.attachments.list`
- `lockd.attachments.get`
- `lockd.attachments.delete`
- `lockd.attachments.delete_all`

Queue/messaging:

- `lockd.queue.enqueue`
- `lockd.queue.dequeue`
- `lockd.queue.ack`
- `lockd.queue.nack`
- `lockd.queue.defer`
- `lockd.queue.extend`
- `lockd.queue.subscribe`
- `lockd.queue.unsubscribe`

Namespace/index:

- `lockd.namespace.get`
- `lockd.namespace.update`
- `lockd.index.flush`

TC-only transaction decision tools are intentionally not exposed by MCP. XA remains available through `txn_id` fields on standard lock/state/queue/attachment operations.

## Queue + SSE Behavior

- MCP forwards upstream queue watch events as MCP progress notifications.
- notifications are wake-up signals; consumers still explicitly call dequeue.
- recommended consumer loop:

1. subscribe (`lockd.queue.subscribe`) or rely on auto-subscription
2. dequeue
3. process
4. ack on success
5. nack on failure
6. defer when message should be re-queued without failure semantics
7. extend while long processing is in-flight

## Query Semantics

`lockd.query` defaults to key mode unless `return=documents` is set.

Engine behavior:

- if `engine` is omitted, behavior is `auto`
- `auto` resolves to namespace query configuration (`preferred_engine` / `fallback_engine`)

Advanced options exposed:

- `engine`: `auto|index|scan`
- `refresh`: supports `wait_for`
- `fields`: query field map
- `cursor` / `limit`

## Go SDK Embedding

Use package `pkt.systems/lockd/mcp` to embed the MCP facade in your own process:

```go
ctx := context.Background()

srv, err := mcp.NewServer(mcp.NewServerRequest{
	Config: mcp.Config{
		Listen:                   "127.0.0.1:19341",
		UpstreamServer:           "https://127.0.0.1:9341",
		UpstreamClientBundlePath: "/home/user/.lockd/client.pem",
		BundlePath:               "/home/user/.lockd/server.pem",
		DefaultNamespace:         "mcp",
		AgentBusQueue:            "lockd.agent.bus",
	},
	Logger: logger,
})
if err != nil {
	return err
}

if err := srv.Run(ctx); err != nil {
	return err
}
```

When `DisableTLS` is true, OAuth/TLS enforcement is disabled. This is intended for trusted local/dev environments.

## Operational Notes

- `lockd mcp` is intended to run behind a reverse proxy in many deployments.
- TLS is still enabled by default to preserve end-to-end encryption from proxy to lockd MCP.
- `lockd mcp ca-export` can be used to install trust for that path.

## Troubleshooting

- Startup error: `mcp oauth state missing`:
  - run `lockd mcp bootstrap`
- Upstream bundle resolution error mentioning client bundle:
  - provide `--client-bundle` (or `mcp.client-bundle`) for MCP -> lockd calls
- 401/403 on MCP endpoint:
  - verify OAuth client status, issuer, token URL, scopes, and resource URL
- No queue notifications:
  - verify session has active subscription and queue/namespace match
