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
)

func defaultServerInstructions(cfg Config) string {
	return strings.TrimSpace(fmt.Sprintf(`
lockd MCP facade operating manual:
- Default namespace: %s
- Default coordination queue: %s
- Discovery workflow: call lockd.hint first to learn namespace-access hints, then lockd.help for workflows.
- Queue workflow: dequeue -> ack | nack(failure) | defer(intentional). Use queue.extend for long-running handlers.
- Queue introspection: use lockd.queue.stats for side-effect-free queue availability and dispatcher counters.
- Queue pagination: dequeue returns `+"`next_cursor`"+`; pass it back as `+"`cursor`"+` when continuing scans.
- Bounded watch workflow: use lockd.queue.watch for interactive polling-compatible wakeups.
- Subscription workflow: lockd.queue.subscribe for long-lived runtimes that can hold session-level SSE subscriptions.
- XA workflow: optional txn_id can be attached to lock/queue/state/attachment operations; transaction decisions are applied by lockd APIs, not TC decision tools in this MCP surface.
- Lock safety: keep lease IDs/fencing tokens from lock operations and send them back on protected writes.
- Write safety: for payloads larger than inline limits, use write_stream begin/append/commit tools (state, queue, attachments).
- Partial mutation: use lockd.state.patch for JSON merge patch updates when full replacement is unnecessary.
- Query first when uncertain: use lockd.query for key discovery, lockd.query.stream for query-document payloads, and lockd.state.stream / lockd.attachments.stream for point payload reads.
- Documentation resources: %s, %s, %s, %s
`, cfg.DefaultNamespace, cfg.AgentBusQueue, docOverviewURI, docLocksURI, docMessagingURI, docSyncURI))
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

Default namespace is %q unless callers override `+"`namespace`"+`.
Default coordination queue is %q in that namespace.

Recommended discovery sequence:
1. Call lockd.hint for namespace-access hints.
2. Call lockd.help.
3. Read %s and %s.
4. Use lockd.query for keys, lockd.query.stream for query documents, and lockd.get for point metadata.
5. Use queue tools for agent coordination and messaging.
6. For large writes, prefer lockd.state.write_stream.*, lockd.queue.write_stream.*, and lockd.attachments.write_stream.*.
`, s.cfg.DefaultNamespace, s.cfg.AgentBusQueue, docMessagingURI, docSyncURI)),
		docLocksURI: strings.TrimSpace(`
# lockd Locking Workflow

1. Acquire lock/lease with lockd.lock.acquire.
2. Read state.
3. Stream state payload with lockd.state.stream when needed.
4. Mutate state with lockd.state.update (small payload), lockd.state.patch (partial merge patch), or lockd.state.write_stream.* (large payload).
5. Keepalive while work is active.
6. Release/commit.

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
Use write_stream tools for large payload writes.
Use lockd.query.stream for query-document payload reads and lockd.state.stream for point payload reads.
Use lockd.attachments.head before lockd.attachments.stream when only metadata is needed.
Use namespace scoping to isolate agent groups.
Use txn_id only when you need cross-key atomic decisions; normal single-key operations should omit it.
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
	Topic string `json:"topic,omitempty" jsonschema:"Optional topic: overview, locks, messaging, sync"`
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
			"run lockd.hint before planning workflows so namespace choices match client claims",
			"queue workflow is dequeue (payload chunk stream) then ack/nack/defer",
			"queue.watch is the bounded wake-up primitive for interactive clients",
			"defer preserves message without counting as failure",
			"extend refreshes lease for long-running handlers",
			"inline write payloads are capped by mcp.inline_max_bytes; use write_stream tools for larger writes",
			"namespace isolation follows client certificate claims",
			"lock writes must preserve lease/fencing semantics",
			"payload reads are streaming-first: use lockd.state.stream, lockd.attachments.stream, and queue.dequeue chunk notifications for large content",
			"XA is optional: only include txn_id when coordinating multiple participants",
		},
	}
	switch topic {
	case "overview":
		out.Summary = "Start with lockd.hint and lockd.help, acquire lock when mutating shared state, inspect queue readiness with lockd.queue.stats, use queue.watch for bounded wakeups, then dequeue and ack/nack/defer messages. Use stream tools for large payload reads and write_stream tools for large writes."
		out.NextCalls = []string{"lockd.hint", "lockd.get", "lockd.state.stream", "lockd.lock.acquire", "lockd.state.update", "lockd.state.patch", "lockd.state.write_stream.begin", "lockd.queue.stats", "lockd.queue.enqueue", "lockd.queue.write_stream.begin", "lockd.queue.watch", "lockd.queue.dequeue", "lockd.queue.ack", "lockd.queue.nack", "lockd.queue.defer"}
		out.Resources = []string{docOverviewURI, docMessagingURI, docSyncURI}
	case "locks":
		out.Summary = "Locks gate state mutation; keep lease identity and fencing token through the full mutation lifecycle."
		out.NextCalls = []string{"lockd.hint", "lockd.lock.acquire", "lockd.state.update", "lockd.state.patch", "lockd.state.write_stream.begin", "lockd.attachments.write_stream.begin", "lockd.lock.release"}
		out.Resources = []string{docLocksURI}
	case "messaging":
		out.Summary = "Messaging is dequeue-driven. Dequeue streams payload chunks over progress notifications. Use queue.stats for readiness/counters and queue.watch for bounded wakeups, ack success, nack failures, defer when a message is not for this worker, and extend when processing runs long. For large publishes, use queue.write_stream tools."
		out.NextCalls = []string{"lockd.hint", "lockd.queue.stats", "lockd.queue.enqueue", "lockd.queue.write_stream.begin", "lockd.queue.watch", "lockd.queue.subscribe", "lockd.queue.dequeue", "lockd.queue.ack", "lockd.queue.nack", "lockd.queue.defer", "lockd.queue.extend"}
		out.Resources = []string{docMessagingURI}
	case "sync":
		out.Summary = "Coordinate through queue events and shared state; keep large context in lockd documents."
		out.NextCalls = []string{"lockd.hint", "lockd.query", "lockd.query.stream", "lockd.get", "lockd.queue.subscribe"}
		out.Resources = []string{docSyncURI, docOverviewURI}
	default:
		return nil, helpToolOutput{}, fmt.Errorf("unknown help topic %q", topic)
	}
	return nil, out, nil
}
