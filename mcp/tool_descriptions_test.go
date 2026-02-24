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
