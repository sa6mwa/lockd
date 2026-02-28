package mcp

import (
	"strings"
	"testing"
)

type descriptionInvariantPolicy struct {
	Bootstrap     bool
	SecretURL     bool
	TerminalLine  bool
	NonIdempotent bool
}

var descriptionInvariantPolicies = map[string]descriptionInvariantPolicy{
	toolLockAcquire:                  {Bootstrap: true, TerminalLine: true},
	toolLockKeepAlive:                {Bootstrap: true, TerminalLine: true},
	toolLockRelease:                  {Bootstrap: true},
	toolGet:                          {Bootstrap: true, SecretURL: true},
	toolDescribe:                     {},
	toolQuery:                        {},
	toolQueryStream:                  {Bootstrap: true, SecretURL: true},
	toolStateUpdate:                  {Bootstrap: true},
	toolStateMutate:                  {Bootstrap: true},
	toolStatePatch:                   {Bootstrap: true},
	toolStateWriteStreamBegin:        {Bootstrap: true, SecretURL: true},
	toolStateWriteStreamStatus:       {Bootstrap: true, TerminalLine: true},
	toolStateWriteStreamCommit:       {Bootstrap: true, TerminalLine: true},
	toolStateWriteStreamAbort:        {Bootstrap: true, TerminalLine: true},
	toolStateStream:                  {Bootstrap: true, SecretURL: true},
	toolStateMetadata:                {Bootstrap: true},
	toolStateRemove:                  {Bootstrap: true},
	toolAttachmentsPut:               {Bootstrap: true},
	toolAttachmentsWriteStreamBegin:  {Bootstrap: true, SecretURL: true},
	toolAttachmentsWriteStreamStatus: {Bootstrap: true, TerminalLine: true},
	toolAttachmentsWriteStreamCommit: {Bootstrap: true, TerminalLine: true},
	toolAttachmentsWriteStreamAbort:  {Bootstrap: true, TerminalLine: true},
	toolAttachmentsList:              {},
	toolAttachmentsHead:              {},
	toolAttachmentsChecksum:          {},
	toolAttachmentsGet:               {Bootstrap: true, SecretURL: true},
	toolAttachmentsStream:            {Bootstrap: true, SecretURL: true},
	toolAttachmentsDelete:            {Bootstrap: true},
	toolAttachmentsDeleteAll:         {Bootstrap: true},
	toolNamespaceGet:                 {},
	toolNamespaceUpdate:              {Bootstrap: true},
	toolIndexFlush:                   {},
	toolHint:                         {},
	toolHelp:                         {},
	toolQueueEnqueue:                 {Bootstrap: true},
	toolQueueWriteStreamBegin:        {Bootstrap: true, SecretURL: true},
	toolQueueWriteStreamStatus:       {Bootstrap: true, TerminalLine: true},
	toolQueueWriteStreamCommit:       {Bootstrap: true, TerminalLine: true},
	toolQueueWriteStreamAbort:        {Bootstrap: true, TerminalLine: true},
	toolQueueDequeue:                 {Bootstrap: true, SecretURL: true, TerminalLine: true},
	toolQueueStats:                   {},
	toolQueueWatch:                   {},
	toolQueueAck:                     {Bootstrap: true, NonIdempotent: true},
	toolQueueNack:                    {Bootstrap: true, NonIdempotent: true},
	toolQueueDefer:                   {Bootstrap: true, NonIdempotent: true},
	toolQueueExtend:                  {Bootstrap: true, TerminalLine: true},
	toolQueueSubscribe:               {Bootstrap: true, TerminalLine: true},
	toolQueueUnsubscribe:             {Bootstrap: true, TerminalLine: true},
}

func TestBuildToolDescriptionsCoverage(t *testing.T) {
	t.Parallel()

	cfg := Config{
		DefaultNamespace: "mcp",
		AgentBusQueue:    "lockd.agent.bus",
	}
	descriptions := buildToolDescriptions(cfg)

	if len(descriptions) != len(mcpToolNames) {
		t.Fatalf("expected %d tool descriptions, got %d", len(mcpToolNames), len(descriptions))
	}
	for _, name := range mcpToolNames {
		description, ok := descriptions[name]
		if !ok {
			t.Fatalf("missing description for %s", name)
		}
		if strings.TrimSpace(description) == "" {
			t.Fatalf("empty description for %s", name)
		}
	}
}

func TestBuildToolDescriptionsIncludeOperationalSections(t *testing.T) {
	t.Parallel()

	cfg := Config{
		DefaultNamespace: "mcp",
		AgentBusQueue:    "lockd.agent.bus",
	}
	descriptions := buildToolDescriptions(cfg)
	required := []string{
		"Purpose:",
		"Use when:",
		"Requires:",
		"Effects:",
		"Retry:",
		"Next:",
	}
	for _, name := range mcpToolNames {
		description := descriptions[name]
		for _, marker := range required {
			if !strings.Contains(description, marker) {
				t.Fatalf("description for %s missing marker %q: %q", name, marker, description)
			}
		}
	}
}

func TestBuildToolDescriptionsIncludeConfiguredDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{
		DefaultNamespace: "agents",
		AgentBusQueue:    "agents.bus",
	}
	descriptions := buildToolDescriptions(cfg)

	if !strings.Contains(descriptions[toolQueueEnqueue], "`queue` defaults to \"agents.bus\"") {
		t.Fatalf("queue enqueue description missing configured queue default: %q", descriptions[toolQueueEnqueue])
	}
	if !strings.Contains(descriptions[toolQueueEnqueue], "`namespace` defaults to \"agents\"") {
		t.Fatalf("queue enqueue description missing configured namespace default: %q", descriptions[toolQueueEnqueue])
	}
	if !strings.Contains(descriptions[toolGet], "`namespace` defaults to \"agents\"") {
		t.Fatalf("get description missing configured namespace default: %q", descriptions[toolGet])
	}
}

func TestBuildToolDescriptionsIncludeMemoryTaggingGuidance(t *testing.T) {
	t.Parallel()

	descriptions := buildToolDescriptions(Config{
		DefaultNamespace: "mcp",
		AgentBusQueue:    "lockd.agent.bus",
	})

	queryDesc := descriptions[toolQuery]
	if !strings.Contains(queryDesc, "in{field=/tags,any=") {
		t.Fatalf("query description missing tags guidance: %q", queryDesc)
	}
	if !strings.Contains(queryDesc, "icontains{field=/...,value=") {
		t.Fatalf("query description missing full-text guidance: %q", queryDesc)
	}

	updateDesc := descriptions[toolStateUpdate]
	if !strings.Contains(updateDesc, "top-level `tags` JSON array") {
		t.Fatalf("state update description missing tags-write guidance: %q", updateDesc)
	}
	if !strings.Contains(updateDesc, "in{field=/tags,any=") {
		t.Fatalf("state update description missing tags query example: %q", updateDesc)
	}
}

func TestBuildToolDescriptionsHighRiskGuards(t *testing.T) {
	t.Parallel()

	descriptions := buildToolDescriptions(Config{
		DefaultNamespace: "mcp",
		AgentBusQueue:    "lockd.agent.bus",
	})

	if len(descriptionInvariantPolicies) != len(mcpToolNames) {
		t.Fatalf("policy map/tool count mismatch: policies=%d tools=%d", len(descriptionInvariantPolicies), len(mcpToolNames))
	}

	for _, tool := range mcpToolNames {
		policy, ok := descriptionInvariantPolicies[tool]
		if !ok {
			t.Fatalf("missing invariant policy for tool %s", tool)
		}
		desc := descriptions[tool]
		lines := strings.Split(desc, "\n")
		firstLine := ""
		secondLine := ""
		if len(lines) > 0 {
			firstLine = lines[0]
		}
		if len(lines) > 1 {
			secondLine = lines[1]
		}

		if policy.Bootstrap {
			if firstLine != bootstrapOverviewLine {
				t.Fatalf("tool %s missing bootstrap first line; got %q", tool, firstLine)
			}
			if !strings.Contains(desc, nextBootstrapHelpBranch) {
				t.Fatalf("tool %s missing branching Next bootstrap hint: %q", tool, desc)
			}
			if !strings.Contains(desc, "- Otherwise ->") {
				t.Fatalf("tool %s missing branching Next otherwise hint: %q", tool, desc)
			}
			if len(lines) < 3 {
				t.Fatalf("tool %s should have bootstrap + invariant + sections: %q", tool, desc)
			}
			if !strings.HasPrefix(secondLine, "SENSITIVE:") &&
				!strings.HasPrefix(secondLine, "TERMINAL:") &&
				!strings.HasPrefix(secondLine, "DURABILITY:") &&
				!strings.HasPrefix(secondLine, "PAYLOAD RULE:") &&
				!strings.HasPrefix(secondLine, "NON-IDEMPOTENT:") {
				t.Fatalf("tool %s second line should be micro-invariant, got %q", tool, secondLine)
			}
		}

		if policy.SecretURL {
			secretPos := strings.Index(desc, secretURLLine)
			purposePos := strings.Index(desc, "Purpose:")
			if secretPos == -1 {
				t.Fatalf("tool %s missing secret URL invariant", tool)
			}
			if purposePos == -1 || secretPos > purposePos {
				t.Fatalf("tool %s secret invariant must appear before purpose section", tool)
			}
		}

		if policy.TerminalLine && !strings.Contains(desc, "TERMINAL:") {
			t.Fatalf("tool %s missing TERMINAL invariant", tool)
		}

		if policy.NonIdempotent && !strings.HasPrefix(secondLine, "NON-IDEMPOTENT:") {
			t.Fatalf("tool %s missing NON-IDEMPOTENT micro-invariant on second line", tool)
		}
	}

	for tool := range descriptionInvariantPolicies {
		found := false
		for _, name := range mcpToolNames {
			if tool == name {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("policy has unknown tool key %q", tool)
		}
	}
}

func TestBuildToolDescriptionsSemanticPhrasesStayAligned(t *testing.T) {
	t.Parallel()

	descriptions := buildToolDescriptions(Config{
		DefaultNamespace: "mcp",
		AgentBusQueue:    "lockd.agent.bus",
	})

	queueAckDesc := descriptions[toolQueueAck]
	if !strings.Contains(queueAckDesc, "`queue`, `message_id`, `lease_id`, and `meta_etag` are required") {
		t.Fatalf("queue ack required field wording drifted: %q", queueAckDesc)
	}
	if strings.Contains(queueAckDesc, "`fencing_token`, and `meta_etag` are required") {
		t.Fatalf("queue ack incorrectly marks fencing_token as hard-required: %q", queueAckDesc)
	}

	attPutDesc := descriptions[toolAttachmentsPut]
	if !strings.Contains(attPutDesc, "Provide at most one of `payload_text` or `payload_base64`") {
		t.Fatalf("attachments.put payload requirement drifted: %q", attPutDesc)
	}

	queueEnqueueDesc := descriptions[toolQueueEnqueue]
	if !strings.Contains(queueEnqueueDesc, "Provide at most one of `payload_text` or `payload_base64`") {
		t.Fatalf("queue.enqueue payload requirement drifted: %q", queueEnqueueDesc)
	}
}
