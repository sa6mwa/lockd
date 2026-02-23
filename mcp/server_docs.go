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
- Subscription workflow: lockd.queue.subscribe to receive queue availability notifications over MCP SSE progress notifications.
- XA workflow: optional txn_id can be attached to lock/queue/state/attachment operations; transaction decisions are applied by lockd APIs, not TC decision tools in this MCP surface.
- Lock safety: keep lease IDs/fencing tokens from lock operations and send them back on protected writes.
- Query first when uncertain: use lockd.query for key discovery and lockd.get for point reads.
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
4. Use lockd.query + lockd.get to locate state.
5. Use queue tools for agent coordination and messaging.
`, s.cfg.DefaultNamespace, s.cfg.AgentBusQueue, docMessagingURI, docSyncURI)),
		docLocksURI: strings.TrimSpace(`
# lockd Locking Workflow

1. Acquire lock/lease (outside this initial MCP surface this is exposed in lockd APIs/CLI/SDK).
2. Read state.
3. Mutate state.
4. Keepalive while work is active.
5. Release/commit.

Critical invariants:
- Lease ID and fencing token are authority.
- Idempotency and retry strategy should be explicit at workflow level.
`),
		docMessagingURI: strings.TrimSpace(fmt.Sprintf(`
# Queue Messaging Workflow

Primary queue loop:
1. lockd.queue.enqueue to publish coordination events
2. lockd.queue.dequeue (default queue %q unless overridden)
3. If processing succeeds: lockd.queue.ack
4. If processing fails: lockd.queue.nack
5. If message is not for this worker: lockd.queue.defer
6. If processing runs long: lockd.queue.extend

For push-notify:
1. lockd.queue.subscribe
2. Wait for progress notifications carrying lockd.queue.message_available payload.
3. Dequeue/ack/defer as above.
`, s.cfg.AgentBusQueue)),
		docSyncURI: strings.TrimSpace(`
# Agent Synchronization

Use queues for eventing and lock/state operations for shared context updates.
Keep queue payloads small and use key/state references for large context.
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
			"queue workflow is dequeue then ack/nack/defer",
			"defer preserves message without counting as failure",
			"extend refreshes lease for long-running handlers",
			"namespace isolation follows client certificate claims",
			"lock writes must preserve lease/fencing semantics",
			"XA is optional: only include txn_id when coordinating multiple participants",
		},
	}
	switch topic {
	case "overview":
		out.Summary = "Start with lockd.hint and lockd.help, acquire lock when mutating shared state, subscribe for queue notifications, then dequeue and ack/nack/defer messages."
		out.NextCalls = []string{"lockd.hint", "lockd.lock.acquire", "lockd.queue.subscribe", "lockd.queue.dequeue", "lockd.queue.ack", "lockd.queue.nack", "lockd.queue.defer"}
		out.Resources = []string{docOverviewURI, docMessagingURI, docSyncURI}
	case "locks":
		out.Summary = "Locks gate state mutation; keep lease identity and fencing token through the full mutation lifecycle."
		out.NextCalls = []string{"lockd.hint", "lockd.lock.acquire", "lockd.state.update", "lockd.lock.release"}
		out.Resources = []string{docLocksURI}
	case "messaging":
		out.Summary = "Messaging is dequeue-driven. Ack success, nack failures, defer when a message is not for this worker, and extend when processing runs long."
		out.NextCalls = []string{"lockd.hint", "lockd.queue.enqueue", "lockd.queue.subscribe", "lockd.queue.dequeue", "lockd.queue.ack", "lockd.queue.nack", "lockd.queue.defer", "lockd.queue.extend"}
		out.Resources = []string{docMessagingURI}
	case "sync":
		out.Summary = "Coordinate through queue events and shared state; keep large context in lockd documents."
		out.NextCalls = []string{"lockd.hint", "lockd.query", "lockd.get", "lockd.queue.subscribe"}
		out.Resources = []string{docSyncURI, docOverviewURI}
	default:
		return nil, helpToolOutput{}, fmt.Errorf("unknown help topic %q", topic)
	}
	return nil, out, nil
}
