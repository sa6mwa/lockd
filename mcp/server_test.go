package mcp

import (
	"context"
	"io"
	"testing"

	"pkt.systems/pslog"
)

func TestApplyDefaultsSetsMCPNamespace(t *testing.T) {
	cfg := Config{}
	applyDefaults(&cfg)
	if cfg.DefaultNamespace != "mcp" {
		t.Fatalf("expected default namespace mcp, got %q", cfg.DefaultNamespace)
	}
}

func TestNewUpstreamClientUsesConfiguredDefaultNamespace(t *testing.T) {
	cfg := Config{
		UpstreamServer:      "http://127.0.0.1:9341",
		UpstreamDisableMTLS: true,
		DefaultNamespace:    "mcp",
	}
	applyDefaults(&cfg)
	cli, err := newUpstreamClient(cfg, pslog.NewStructured(context.Background(), io.Discard))
	if err != nil {
		t.Fatalf("new upstream client: %v", err)
	}
	defer cli.Close()
	if got := cli.Namespace(); got != "mcp" {
		t.Fatalf("expected default namespace mcp, got %q", got)
	}
}

func TestNewUpstreamClientRespectsNamespaceOverride(t *testing.T) {
	cfg := Config{
		UpstreamServer:      "http://127.0.0.1:9341",
		UpstreamDisableMTLS: true,
		DefaultNamespace:    "agents",
	}
	applyDefaults(&cfg)
	cli, err := newUpstreamClient(cfg, pslog.NewStructured(context.Background(), io.Discard))
	if err != nil {
		t.Fatalf("new upstream client: %v", err)
	}
	defer cli.Close()
	if got := cli.Namespace(); got != "agents" {
		t.Fatalf("expected default namespace agents, got %q", got)
	}
}
