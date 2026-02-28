package mcp

import (
	"strings"
	"testing"
)

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
