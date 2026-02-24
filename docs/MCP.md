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

Large payload safety is a first-class constraint. MCP uses dual payload delivery:

- inline payload fields for small payloads (bounded by `mcp.inline_max_bytes`)
- capability URL streaming for large payloads

The MCP facade acts as a normal lockd client toward upstream lockd. In v1, authorization boundaries are inherited from the upstream client certificate and its namespace access claims.

## Runtime Model

`lockd mcp` runs as an independent service (first iteration), not yet embedded in `lockd --with-mcp`.

Core properties:

- MCP listen default: `127.0.0.1:19341`
- MCP endpoint path (docroot) default: `/`
- TLS enabled by default
- Local OAuth 2.1 provider enabled by default (`/authorize`, `/token`, metadata endpoints)
- default namespace: `mcp`
- default coordination queue: `lockd.agent.bus`
- each MCP session auto-subscribes to `mcp/lockd.agent.bus`
- transfer URLs are composed as: `base-url path + mcp-path + /transfer/<capability_id>`

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
- `--base-url`: externally reachable MCP base URL used for transfer URLs (required)
- `--allow-http`: allow `http://` base URL (unsafe; default requires HTTPS)
- `--disable-mcp-upstream-mtls`: disable mTLS for MCP -> upstream lockd
- `--inline-max-bytes`: max decoded inline payload bytes for inline tool payloads (default `2097152`)
- `--default-namespace`: default namespace when tools omit namespace (default `mcp`)
- `--agent-bus-queue`: default queue and auto-subscribe queue (default `lockd.agent.bus`)
- `--state-file`: OAuth state path
- `--refresh-store`: refresh-token store path
- `--issuer`: bootstrap/oauth issuer value
- `--mcp-path`: streamable MCP HTTP path / docroot (default `/`)
- `--oauth-resource-url`: protected resource identifier URL

Transfer URL composition examples:

- `base-url=https://myserver/lockdmcp/v1`, `mcp-path=/` -> `https://myserver/lockdmcp/v1/transfer/<id>`
- `base-url=https://mybase/mcp`, `mcp-path=/mcp/v1` -> `https://mybase/mcp/mcp/v1/transfer/<id>`
- `base-url=https://mydomain`, `mcp-path=/mcp` -> `https://mydomain/mcp/transfer/<id>`

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
- `mcp.base_url` -> `LOCKD_MCP_BASE_URL`
- `mcp.allow_http` -> `LOCKD_MCP_ALLOW_HTTP`
- `mcp.disable_mtls` -> `LOCKD_MCP_DISABLE_MTLS`
- `mcp.inline_max_bytes` -> `LOCKD_MCP_INLINE_MAX_BYTES`
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

- `lockd.hint`
- `lockd.help`
- resource docs under `resource://docs/*`

Lock/state:

- `lockd.lock.acquire`
- `lockd.lock.keepalive`
- `lockd.lock.release`
- `lockd.get`
- `lockd.describe`
- `lockd.query`
- `lockd.query.stream`
- `lockd.state.update`
- `lockd.state.patch`
- `lockd.state.write_stream.begin`
- `lockd.state.write_stream.commit`
- `lockd.state.write_stream.abort`
- `lockd.state.stream`
- `lockd.state.metadata`
- `lockd.state.remove`

Attachments:

- `lockd.attachments.put`
- `lockd.attachments.write_stream.begin`
- `lockd.attachments.write_stream.commit`
- `lockd.attachments.write_stream.abort`
- `lockd.attachments.list`
- `lockd.attachments.head`
- `lockd.attachments.checksum`
- `lockd.attachments.get`
- `lockd.attachments.stream`
- `lockd.attachments.delete`
- `lockd.attachments.delete_all`

Queue/messaging:

- `lockd.queue.enqueue`
- `lockd.queue.write_stream.begin`
- `lockd.queue.write_stream.commit`
- `lockd.queue.write_stream.abort`
- `lockd.queue.dequeue`
- `lockd.queue.stats`
- `lockd.queue.watch`
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
- `lockd.queue.stats` is the side-effect-free introspection primitive (availability + dispatcher counters).
- `lockd.queue.watch` is the bounded wake-up tool for interactive clients.
- notifications are wake-up signals; consumers still explicitly call dequeue.
- recommended consumer loop:

1. subscribe (`lockd.queue.subscribe`) or rely on auto-subscription
2. call `lockd.queue.stats` for readiness snapshot/counters
3. or call `lockd.queue.watch` with a bounded duration/event cap
4. dequeue
5. process
6. ack on success
7. nack on failure
8. defer when message should be re-queued without failure semantics
9. extend while long processing is in-flight

`lockd.queue.dequeue` supports `payload_mode=auto|inline|stream|none` and `state_mode=auto|inline|stream|none` (state mode applies when `stateful=true`).
`lockd.queue.dequeue` also returns `next_cursor`; pass it back as `cursor` on later calls when continuing the same dequeue scan.

## Contract Notes

- `lockd.get` supports `payload_mode=auto|inline|stream|none`.
- read payload via `lockd.state.stream` for explicit streaming-only calls, or use `lockd.get payload_mode=stream`.
- `lockd.state.update`, `lockd.queue.enqueue`, and `lockd.attachments.put` are inline writes and enforce `mcp.inline_max_bytes`.
- `lockd.state.patch` applies RFC 7396 JSON merge patch semantics for partial updates and is also bounded by `mcp.inline_max_bytes`.
- for larger writes, use `*.write_stream.begin` upload URLs plus `commit`.
- `lockd.get`, `lockd.attachments.list`, and `lockd.attachments.get` default to `public=true`.
- For those reads: `public=false` requires `lease_id`; `public=true` rejects `lease_id`.
- attachment writes support both `lockd.attachments.put` (inline) and `lockd.attachments.write_stream.*` (streaming) with `mode=create|upsert|replace` (`create` default).
- `lockd.attachments.head` is metadata-only by id/name and avoids payload download.
- `lockd.attachments.get` supports `payload_mode=auto|inline|stream|none`.
- read attachment payload via `lockd.attachments.stream` transfer URL (`download_url`) or via `lockd.attachments.get payload_mode=stream`.
- attachment checksums are upload-time plaintext SHA-256 values persisted in lockd metadata (`plaintext_sha256`) and can be fetched directly through `lockd.attachments.checksum` without streaming payload bytes.
- inline-over-limit errors explicitly point to streaming tools and suggest checking `lockd.hint.inline_max_payload_bytes`.

## Query Semantics

`lockd.query` returns keys only in MCP and does not accept a `return` selector.

Use `lockd.query.stream` for query-document payload streaming via one-time NDJSON capability URL (`download_url` + `GET`).

Use `lockd.get` + `lockd.state.stream` for point payload reads.

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

## Agent Workflow Recommendation

At session start:

1. call `lockd.hint` to discover namespace-access hints from client-bundle claims
   and `inline_max_payload_bytes` for inline-vs-stream planning
2. call `lockd.help` for operation sequencing
3. run queue/query/lock workflows using hinted namespaces

`lockd.hint` is advisory, but it is the fastest way for an agent to avoid namespace-forbidden calls before first operation.
