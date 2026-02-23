package main

import (
	"context"
	"io"
	"testing"

	"pkt.systems/pslog"
)

func TestMCPCommandRegistered(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	mcpCmd, _, err := root.Find([]string{"mcp"})
	if err != nil {
		t.Fatalf("find mcp command: %v", err)
	}
	if mcpCmd == nil || mcpCmd.Name() != "mcp" {
		t.Fatalf("expected mcp command to be registered")
	}
}

func TestMCPCommandFlags(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	mcpCmd, _, err := root.Find([]string{"mcp"})
	if err != nil {
		t.Fatalf("find mcp command: %v", err)
	}
	if flag := mcpCmd.Flags().Lookup("client-bundle"); flag == nil {
		t.Fatalf("expected --client-bundle on mcp command")
	} else if flag.Shorthand != "B" {
		t.Fatalf("expected --client-bundle shorthand -B, got %q", flag.Shorthand)
	}
	if flag := mcpCmd.Flags().Lookup("listen"); flag == nil {
		t.Fatalf("expected --listen on mcp command")
	} else if flag.DefValue != "127.0.0.1:19341" {
		t.Fatalf("expected listen default 127.0.0.1:19341, got %q", flag.DefValue)
	}
	if flag := mcpCmd.Flags().Lookup("default-namespace"); flag == nil {
		t.Fatalf("expected --default-namespace on mcp command")
	} else if flag.DefValue != "mcp" {
		t.Fatalf("expected default namespace mcp, got %q", flag.DefValue)
	}
	if inherited := mcpCmd.InheritedFlags().Lookup("bundle"); inherited == nil || inherited.Shorthand != "b" {
		t.Fatalf("expected inherited --bundle/-b on mcp command")
	}
	if inherited := mcpCmd.InheritedFlags().Lookup("server"); inherited == nil || inherited.Shorthand != "s" {
		t.Fatalf("expected inherited --server/-s on mcp command")
	}
}
