package mcp

import (
	"context"
	"fmt"
	"sort"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	docOverviewURI  = "resource://docs/overview.md"
	docLocksURI     = "resource://docs/locks.md"
	docMessagingURI = "resource://docs/messaging.md"
	docSyncURI      = "resource://docs/agent-sync.md"
	docLQLURI       = "resource://docs/lql.md"
)

func defaultServerInstructions(cfg Config) string {
	return strings.TrimSpace(fmt.Sprintf(`
lockd MCP facade operating manual:
- Default namespace: %s (per-client override may apply when configured via lockd mcp client add/update --namespace)
- Default coordination queue: %s
- Discovery workflow: start with the operation you need; call lockd.hint only when namespace access is unclear or namespace-forbidden errors occur.
- Queue workflow: dequeue -> ack | nack(failure) | defer(intentional). Use queue.extend for long-running handlers.
- Queue introspection: use lockd.queue.stats for side-effect-free queue availability and dispatcher counters.
- Queue pagination: dequeue returns `+"`next_cursor`"+`; pass it back as `+"`cursor`"+` when continuing scans.
- Bounded watch workflow: use lockd.queue.watch for interactive polling-compatible wakeups.
- Subscription workflow: lockd.queue.subscribe for long-lived runtimes that can hold session-level SSE subscriptions.
- XA workflow: optional txn_id can be attached to lock/queue/state/attachment operations; transaction decisions are applied by lockd APIs, not TC decision tools in this MCP surface.
- Lock safety: keep lease IDs/fencing tokens from lock operations and send them back on protected writes.
- Fast-path document workflow: use lockd.state.put and lockd.state.delete for single-call acquire->mutate->release when lockd is used as a document/memory store.
- Write safety: inline payloads are capped by mcp.inline_max_bytes. For payload_mode/state_mode auto, payloads <= lockd.hint.inline_max_payload_bytes are inline and larger payloads switch to stream.
- Stream workflow: use write_stream begin + upload_url + optional write_stream.status + commit for larger writes (state, queue, attachments). Commit accepts optional expected_bytes/expected_sha256.
- Transfer URL hygiene: capability URLs are bearer-style secrets; avoid shell history/process-list leakage and avoid pasting capability URLs into chat/tickets.
- Partial mutation: use lockd.state.mutate for LQL expression-based updates and lockd.state.patch for RFC 7396 merge patch updates.
- Query first when uncertain: use lockd.query for key discovery, lockd.query.stream for NDJSON document stream URLs, and lockd.state.stream / lockd.attachments.stream for point payload reads.
- Attachment rule: attachment payloads are blobs and are not queryable/indexed by LQL selectors.
- Query enumeration: use empty query string (`+"`\"\"`"+`) to match all keys/documents when you need namespace inventory.
- Memory tagging convention: unless a workflow says otherwise, store a top-level `+"`tags`"+` JSON array on saved objects and query tags with `+"`in{field=/tags,any=planning|finance}`"+`.
- Schema guidance: no fixed schema fields are required beyond `+"`tags`"+`; preserve caller-defined fields.
- Full-text retrieval: use `+"`icontains{field=/...,value=contract}`"+` for broad recall and combine with tag filters for precision.
- Multi-keyword retrieval: use `+"`contains{f=/summary,a=\"renewal|master service agreement\"}`"+` or `+"`icontains{f=/...,a=\"contract|key phrase 2\"}`"+` (`+"`value`"+` and `+"`any`"+` are mutually exclusive per clause).
- Temporal retrieval: use `+"`date{field=/updated_at,since=yesterday}`"+` for relative windows and `+"`range{field=/updated_at,gte=2025-01-01T00:00:00Z,lt=2025-02-01T00:00:00Z}`"+` or `+"`/updated_at>=2025-01-01T00:00:00Z,/updated_at<2025-02-01T00:00:00Z`"+` for explicit datetime windows.
- Temporal macro rule: only `+"`date{...,since=...}`"+` accepts `+"`now|today|yesterday`"+`.
- Documentation resources: %s, %s, %s, %s, %s
`, cfg.DefaultNamespace, cfg.AgentBusQueue, docOverviewURI, docLocksURI, docMessagingURI, docSyncURI, docLQLURI))
}

func (s *server) registerResources(srv *mcpsdk.Server) {
	for _, uri := range s.resourceURIs() {
		srv.AddResource(&mcpsdk.Resource{
			URI:         uri,
			Name:        uri,
			Title:       uri,
			Description: "lockd MCP operational documentation",
			MIMEType:    "text/markdown",
		}, s.handleDocResource)
	}
}

func (s *server) resourceURIs() []string {
	docs := s.resourceDocs()
	uris := make([]string, 0, len(docs))
	for uri := range docs {
		uris = append(uris, uri)
	}
	sort.Strings(uris)
	return uris
}

func (s *server) resourceDocs() map[string]string {
	return map[string]string{
		docOverviewURI: strings.TrimSpace(fmt.Sprintf(`
# lockd MCP Overview

Default namespace is %q unless a per-client namespace override exists; explicit tool `+"`namespace`"+` still wins.
Default coordination queue is %q in that namespace.

Recommended discovery sequence:
1. Call lockd.help.
2. Read %s, %s, and %s.
3. Use lockd.query for keys, lockd.query.stream for query documents, and lockd.get for point metadata.
4. Call lockd.hint only when namespace access is unclear or namespace-forbidden errors occur.
5. Unless workflow constraints say otherwise, persist a top-level `+"`tags`"+` array on documents (for example ["planning","finance","q3"]).
   No fixed schema fields are required beyond tags; preserve the caller's existing JSON structure.
6. Prefer lockd.state.put and lockd.state.delete for document-store style writes/deletes (single MCP call each).
7. Use lockd.lock.acquire + lockd.state.update/mutate/patch + lockd.lock.release only when you explicitly need multi-step coordination under one lease.
8. Use query="" when you need a namespace inventory (match-all), then narrow with selectors.
9. Use tag-first retrieval, multi-keyword recall, and temporal narrowing:
   - in{field=/tags,any=planning|finance}
   - and.in{field=/tags,any=customer},and.icontains{f=/...,a="renewal|notice"}
   - contains{f=/summary,a="master service agreement|renewal"}
   - date{field=/updated_at,since=today}
   - /updated_at>=2025-01-01T00:00:00Z,/updated_at<2025-02-01T00:00:00Z
10. Use queue tools for agent coordination and messaging.
11. For large writes, use lockd.*.write_stream.begin to get upload_url, upload bytes directly, optionally call write_stream.status, then commit (optionally with expected_bytes/expected_sha256).
`, s.cfg.DefaultNamespace, s.cfg.AgentBusQueue, docMessagingURI, docSyncURI, docLQLURI)),
		docLocksURI: strings.TrimSpace(`
# lockd Locking Workflow

1. Acquire lock/lease with lockd.lock.acquire.
2. Read state.
3. Stream state payload with lockd.state.stream when needed.
4. Mutate state with lockd.state.update (small payload), lockd.state.mutate (LQL expressions), lockd.state.patch (partial merge patch), or lockd.state.write_stream.* (large payload).
5. Keepalive while work is active.
6. Release lease to commit staged state/attachment changes (or rollback).

Fast-path note:
- For document-store style usage (without explicit multi-step coordination), prefer lockd.state.put and lockd.state.delete.

Critical invariants:
- Lease ID and fencing token are authority.
- Idempotency and retry strategy should be explicit at workflow level.
`),
		docMessagingURI: strings.TrimSpace(fmt.Sprintf(`
# Queue Messaging Workflow

Primary queue loop:
1. lockd.queue.enqueue to publish coordination events
   for large payloads use lockd.queue.write_stream.*
2. lockd.queue.watch for bounded wakeup signals (recommended for interactive clients)
3. lockd.queue.dequeue (default queue %q unless overridden)
   payload_mode/state_mode auto => inline when payload <= lockd.hint.inline_max_payload_bytes, else stream
   stateful dequeue is all-or-nothing (no partial message-only success when state lease cannot be acquired)
4. If processing succeeds: lockd.queue.ack
5. If processing fails: lockd.queue.nack
6. If message is not for this worker: lockd.queue.defer
7. If processing runs long: lockd.queue.extend

For push-notify:
1. lockd.queue.subscribe
2. Wait for progress notifications carrying lockd.queue.message_available payload.
3. Dequeue/ack/defer as above.
`, s.cfg.AgentBusQueue)),
		docSyncURI: strings.TrimSpace(`
# Agent Synchronization

Use queues for eventing and lock/state operations for shared context updates.
Keep queue payloads small and use key/state references for large context.
Use write_stream tools for large payload writes (begin -> upload_url -> commit).
Unless caller-specific policy says otherwise, write memory-like state with a
top-level tags array and keep it current as context evolves.
No fixed schema fields are required beyond tags.
Prefer query pattern:
- in{field=/tags,any=planning|operations}
- and.in{field=/tags,any=customer},and.icontains{f=/...,a="renewal|notice"}
- contains{f=/summary,a="master service agreement|renewal"}
- date{field=/updated_at,since=yesterday}
Use icontains over /... for broad keyword recall when exact tags are missing, and use date/range temporal selectors for time-window constraints.
Capability URLs are bearer-style secrets: avoid command-history and process-list leakage.
Use lockd.query.stream for query-document NDJSON stream URLs and lockd.state.stream / lockd.attachments.stream to obtain one-time download URLs for point payload reads.
Use lockd.attachments.head before lockd.attachments.stream when only metadata is needed.
Use namespace scoping to isolate agent groups.
Use txn_id only when you need cross-key atomic decisions; normal single-key operations should omit it.
`),
		docLQLURI: strings.TrimSpace(`
# LQL Selector Reference

Use this section as the canonical selector syntax for lockd.query and lockd.query.stream.

Selector clause families:
- Equality:
  - ` + "`eq{field=/status,value=open}`" + `
  - ` + "`eq{field=/department/code,value=finance}`" + `
- Contains (case-sensitive):
  - ` + "`contains{field=/summary,value=Budget}`" + `
  - ` + "`contains{field=/notes,value=approved}`" + `
  - ` + "`contains{f=/summary,a=\"Budget|Master Service Agreement|key phrase 2\"}`" + `
- IContains (case-insensitive):
  - ` + "`icontains{field=/summary,value=budget}`" + `
  - ` + "`icontains{field=/...,value=renewal}`" + `
  - ` + "`icontains{f=/...,a=\"contract|msa|key phrase 2\"}`" + `
- Prefix (case-sensitive):
  - ` + "`prefix{field=/owner,value=dept-}`" + `
- IPrefix (case-insensitive):
  - ` + "`iprefix{field=/owner,value=DEPT-}`" + `
- Range (numeric or datetime bounds; use one or more of gt/gte/lt/lte):
  - ` + "`range{field=/amount,gte=100}`" + `
  - ` + "`range{field=/amount,gt=50,lt=1000}`" + `
  - ` + "`range{field=/updated_at,gte=2025-01-01T00:00:00Z,lt=2025-02-01T00:00:00Z}`" + `
- Date (explicit temporal selector; supports value, bounds, and since macro):
  - ` + "`date{field=/updated_at,value=2025-01-01}`" + `
  - ` + "`date{field=/updated_at,after=2025-01-01,before=2025-02-01}`" + `
  - ` + "`date{field=/updated_at,gte=2025-01-01T00:00:00Z,lt=2025-02-01T00:00:00Z}`" + `
  - ` + "`date{f=/updated_at,a=2025-01-01,b=2025-01-03}`" + ` (alias form)
  - ` + "`date{field=/updated_at,since=now}`" + `, ` + "`date{field=/updated_at,since=today}`" + `, ` + "`date{field=/updated_at,since=yesterday}`" + `
- Membership (` + "`any`" + ` is pipe-delimited):
  - ` + "`in{field=/tags,any=planning|finance|customer}`" + `
  - ` + "`in{field=/region,any=us|eu|apac}`" + `
- Exists:
  - ` + "`exists{/metadata/etag}`" + `
  - ` + "`exists{/contract/renewal_date}`" + `

Boolean composition examples:
- AND: ` + "`and.eq{field=/status,value=open},and.range{field=/amount,gte=100}`" + `
- OR: ` + "`or.eq{field=/status,value=open},or.eq{field=/status,value=queued}`" + `
- NOT: ` + "`not.eq{field=/archived,value=true}`" + `
- Indexed groups: ` + "`and.0.eq{field=/status,value=open},and.0.range{field=/amount,gte=100},or.1.eq{field=/region,value=eu}`" + `

Shorthand operators:
- ` + "`/status=\"open\"`" + ` -> equality
- ` + "`/status!=\"closed\"`" + ` -> not-equality
- ` + "`/amount>100`" + `, ` + "`/amount>=100`" + `, ` + "`/amount<1000`" + `, ` + "`/amount<=1000`" + ` -> range
- ` + "`/updated_at>=2025-01-01T00:00:00Z`" + `, ` + "`/updated_at<\"2025-02-01T00:00:00+01:00\"`" + ` -> datetime range
- ` + "`/updated_at=\"2025-01-01\"`" + ` -> datetime-aware equality (date-only intersects timestamp on calendar date)
- Multiple expressions separated by comma/newline are combined with AND by default.

Term parameter aliases:
- ` + "`field`" + ` or ` + "`f`" + ` (example: ` + "`eq{f=/status,v=open}`" + `)
- ` + "`value`" + ` or ` + "`v`" + `
- ` + "`any`" + ` or ` + "`a`" + ` (example: ` + "`in{f=/tags,a=planning|finance}`" + `)
- ` + "`ignoreCase`" + ` or ` + "`ic`" + ` (` + "`true|false|t|f`" + `; example: ` + "`contains{f=/summary,v=budget,ic=t}`" + `)

Path wildcards:
- ` + "`*`" + ` any object child value
- ` + "`[]`" + ` any array element
- ` + "`**`" + ` any immediate child (object value or array element)
- ` + "`...`" + ` any descendant at any depth
- ` + "`/items[]/sku`" + ` is sugar for ` + "`/items/[]/sku`" + `

Value parsing:
- Unquoted literals are accepted for simple values.
- Quoted literals are supported with single or double quotes.
- Booleans and numeric literals are parsed for comparisons.
- For ` + "`in{field=/tags,any=planning|finance|customer}`" + `, ` + "`any`" + ` uses ` + "`|`" + ` as the separator.
- ` + "`contains`" + ` and ` + "`icontains`" + ` also support ` + "`any`" + ` / ` + "`a`" + ` pipe lists (for example ` + "`icontains{f=/...,a=\"contract|key phrase 2\"}`" + `).
- For ` + "`contains`" + ` / ` + "`icontains`" + ` clauses, use either ` + "`value`" + ` or ` + "`any`" + ` (not both).

Temporal behavior:
- Supported temporal literals: ` + "`YYYY-MM-DD`" + `, ` + "`RFC3339`" + `, and ` + "`RFC3339Nano`" + `.
- ` + "`range{...}`" + ` supports numeric or datetime bounds but cannot mix numeric and datetime bounds in one clause.
- Relative macros are only supported by ` + "`date{...,since=...}`" + ` and are limited to ` + "`now`" + `, ` + "`today`" + `, ` + "`yesterday`" + `.
- Shorthand/range comparators require explicit numeric or datetime literals (no macros in shorthand).

Recommended retrieval pattern:
1. Use empty query (` + "`\"\"`" + `) for match-all namespace inventory.
2. Tags-first key discovery with ` + "`in{field=/tags,any=planning|finance}`" + `.
3. Add keyword recall with ` + "`contains{f=/summary,a=\"master service agreement|renewal\"}`" + ` and/or ` + "`icontains{f=/...,a=\"contract|key phrase 2\"}`" + `.
4. Add temporal narrowing with ` + "`date{field=/updated_at,since=yesterday}`" + ` or datetime range shorthand.
5. Use lockd.query.stream when full documents are required.
6. Attachment payloads are blobs and are not queryable/indexed by LQL; retrieve attachments by key/id/name via attachment tools.

Schema guidance:
- No fixed schema fields are required beyond a top-level ` + "`tags`" + ` array convention.
`),
	}
}

func (s *server) handleDocResource(_ context.Context, req *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
	uri := ""
	if req != nil && req.Params != nil {
		uri = strings.TrimSpace(req.Params.URI)
	}
	docs := s.resourceDocs()
	content, ok := docs[uri]
	if !ok {
		return nil, mcpsdk.ResourceNotFoundError(uri)
	}
	return &mcpsdk.ReadResourceResult{
		Contents: []*mcpsdk.ResourceContents{{
			URI:      uri,
			MIMEType: "text/markdown",
			Text:     content,
		}},
	}, nil
}

type helpToolInput struct {
	Topic string `json:"topic,omitempty" jsonschema:"Optional topic: overview, locks, messaging, sync, lql"`
}

type helpToolOutput struct {
	Topic      string            `json:"topic"`
	Summary    string            `json:"summary"`
	NextCalls  []string          `json:"next_calls"`
	Resources  []string          `json:"resources"`
	Defaults   map[string]string `json:"defaults"`
	Invariants []string          `json:"invariants"`
}

func (s *server) handleHelpTool(_ context.Context, _ *mcpsdk.CallToolRequest, input helpToolInput) (*mcpsdk.CallToolResult, helpToolOutput, error) {
	topic := strings.ToLower(strings.TrimSpace(input.Topic))
	if topic == "" {
		topic = "overview"
	}
	out := helpToolOutput{
		Topic: topic,
		Defaults: map[string]string{
			"namespace": s.cfg.DefaultNamespace,
			"queue":     s.cfg.AgentBusQueue,
		},
		Invariants: []string{
			"call lockd.hint only when namespace access is unclear or namespace_forbidden occurs",
			"queue workflow is dequeue (payload_mode inline|stream|auto) then ack/nack/defer",
			"queue.watch is the bounded wake-up primitive for interactive clients",
			"defer preserves message without counting as failure",
			"extend refreshes lease for long-running handlers",
			"inline payloads are capped by mcp.inline_max_bytes; use write_stream tools for larger writes",
			"payload_mode/state_mode auto resolves inline when payload <= lockd.hint.inline_max_payload_bytes, otherwise stream",
			"write_stream.status provides bytes/checksum/readiness before commit",
			"namespace isolation follows client certificate claims",
			"lock writes must preserve lease/fencing semantics",
			"for inline payload_mode requests above the limit, use streaming variants and consult lockd.hint inline_max_payload_bytes",
			"for document-store style writes/deletes, prefer lockd.state.put and lockd.state.delete fastpaths",
			"unless workflow-specific policy says otherwise, persist a top-level tags array on documents and query with in{field=/tags,any=planning|finance}",
			"no fixed schema fields are required beyond tags; preserve caller-defined document fields",
			"for broad memory recall, use icontains{field=/...,value=contract} and combine with tag filters for precision",
			"contains/icontains support quoted any/a pipe lists for spaced phrases (for example a=\"keyword1|key phrase 2\"; use either value or any per clause)",
			"use date{...,since=now|today|yesterday} for relative temporal windows and datetime range/shorthand bounds for explicit windows",
			"attachment payloads are blobs and are not queryable or indexed by LQL selectors",
			"XA is optional: only include txn_id when coordinating multiple participants",
		},
	}
	switch topic {
	case "overview":
		out.Summary = "Start with lockd.help, use tags-backed state as memory, and query with in/icontains patterns. For document-store workflows, prefer lockd.state.put and lockd.state.delete fastpaths. Use explicit lock acquire/update/release only for coordination-heavy multi-step mutations. Inspect queue readiness with lockd.queue.stats, use queue.watch for bounded wakeups, then dequeue and ack/nack/defer messages."
		out.NextCalls = []string{"lockd.query", "lockd.query.stream", "lockd.get", "lockd.state.stream", "lockd.state.put", "lockd.state.delete", "lockd.lock.acquire", "lockd.state.update", "lockd.state.mutate", "lockd.state.patch", "lockd.state.write_stream.begin", "lockd.state.write_stream.status", "lockd.attachments.put", "lockd.attachments.write_stream.begin", "lockd.attachments.write_stream.status", "lockd.queue.stats", "lockd.queue.enqueue", "lockd.queue.write_stream.begin", "lockd.queue.write_stream.status", "lockd.queue.watch", "lockd.queue.dequeue", "lockd.queue.ack", "lockd.queue.nack", "lockd.queue.defer", "lockd.hint"}
		out.Resources = []string{docOverviewURI, docLQLURI, docMessagingURI, docSyncURI}
	case "locks":
		out.Summary = "Locks gate state mutation; keep lease identity and fencing token through the full mutation lifecycle. For simple document writes/deletes without explicit coordination, prefer lockd.state.put/delete fastpaths."
		out.NextCalls = []string{"lockd.state.put", "lockd.state.delete", "lockd.lock.acquire", "lockd.state.update", "lockd.state.mutate", "lockd.state.patch", "lockd.state.write_stream.begin", "lockd.state.write_stream.status", "lockd.attachments.put", "lockd.attachments.write_stream.begin", "lockd.attachments.write_stream.status", "lockd.lock.release", "lockd.hint"}
		out.Resources = []string{docLocksURI}
	case "messaging":
		out.Summary = "Messaging is dequeue-driven. Use payload_mode=auto/inline/stream on dequeue and state_mode for stateful dequeue state payloads. Use queue.stats for readiness/counters and queue.watch for bounded wakeups, ack success, nack failures, defer when a message is not for this worker, and extend when processing runs long. For large publishes, use queue.write_stream tools."
		out.NextCalls = []string{"lockd.queue.stats", "lockd.queue.enqueue", "lockd.queue.write_stream.begin", "lockd.queue.write_stream.status", "lockd.queue.watch", "lockd.queue.subscribe", "lockd.queue.dequeue", "lockd.queue.ack", "lockd.queue.nack", "lockd.queue.defer", "lockd.queue.extend", "lockd.hint"}
		out.Resources = []string{docMessagingURI}
	case "sync":
		out.Summary = "Coordinate through queue events and shared state; keep large context in lockd documents with tags arrays for fast in-selector retrieval plus contains/icontains keyword-any clauses and temporal selectors for date-window recall."
		out.NextCalls = []string{"lockd.query", "lockd.query.stream", "lockd.get", "lockd.queue.subscribe", "lockd.hint"}
		out.Resources = []string{docSyncURI, docLQLURI, docOverviewURI}
	case "lql":
		out.Summary = "LQL selectors use JSON Pointer paths and support eq/contains/icontains/prefix/iprefix/range/date/in/exists plus and/or/not composition, contains/icontains any-forms, shorthand datetime operators, aliases, and wildcard paths."
		out.NextCalls = []string{"lockd.query", "lockd.query.stream"}
		out.Resources = []string{docLQLURI, docOverviewURI}
	default:
		return nil, helpToolOutput{}, fmt.Errorf("unknown help topic %q", topic)
	}
	return nil, out, nil
}
