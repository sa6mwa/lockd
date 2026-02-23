package main

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	mcpstate "pkt.systems/lockd/mcp/state"
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
	if flag := mcpCmd.Flags().Lookup("agent-bus-queue"); flag == nil {
		t.Fatalf("expected --agent-bus-queue on mcp command")
	} else if flag.DefValue != "lockd.agent.bus" {
		t.Fatalf("expected default agent bus queue lockd.agent.bus, got %q", flag.DefValue)
	}
	if inherited := mcpCmd.InheritedFlags().Lookup("bundle"); inherited == nil || inherited.Shorthand != "b" {
		t.Fatalf("expected inherited --bundle/-b on mcp command")
	}
	if inherited := mcpCmd.InheritedFlags().Lookup("server"); inherited == nil || inherited.Shorthand != "s" {
		t.Fatalf("expected inherited --server/-s on mcp command")
	}
}

func TestMCPOAuthClientAdminSubcommands(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	clientCmd, _, err := root.Find([]string{"mcp", "oauth", "client"})
	if err != nil {
		t.Fatalf("find mcp oauth client command: %v", err)
	}
	if clientCmd == nil {
		t.Fatalf("expected mcp oauth client command")
	}
	if showCmd, _, err := clientCmd.Find([]string{"show"}); err != nil || showCmd == nil || showCmd.Name() != "show" {
		t.Fatalf("expected show subcommand, err=%v", err)
	}
	if credCmd, _, err := clientCmd.Find([]string{"credentials"}); err != nil || credCmd == nil || credCmd.Name() != "credentials" {
		t.Fatalf("expected credentials subcommand, err=%v", err)
	}
}

func TestMCPOAuthClientIDCompletionRegistered(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	paths := [][]string{
		{"mcp", "oauth", "client", "show"},
		{"mcp", "oauth", "client", "credentials"},
		{"mcp", "oauth", "client", "remove"},
		{"mcp", "oauth", "client", "revoke"},
		{"mcp", "oauth", "client", "restore"},
		{"mcp", "oauth", "client", "rotate-secret"},
		{"mcp", "oauth", "client", "update"},
	}
	for _, p := range paths {
		cmd, _, err := root.Find(p)
		if err != nil {
			t.Fatalf("find %v: %v", p, err)
		}
		if cmd == nil {
			t.Fatalf("expected command for %v", p)
		}
		if _, ok := cmd.GetFlagCompletionFunc("id"); !ok {
			t.Fatalf("expected id completion func on %v", p)
		}
	}
}

func TestMCPOAuthClientIDCompletionValues(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	bootstrap, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap state: %v", err)
	}
	t.Setenv("LOCKD_MCP_STATE_FILE", statePath)
	t.Setenv("LOCKD_MCP_REFRESH_STORE", filepath.Join(dir, "mcp-auth-store.json"))

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	showCmd, _, err := root.Find([]string{"mcp", "oauth", "client", "show"})
	if err != nil {
		t.Fatalf("find show command: %v", err)
	}
	completer, ok := showCmd.GetFlagCompletionFunc("id")
	if !ok {
		t.Fatalf("missing id completion")
	}
	values, directive := completer(showCmd, nil, "")
	if directive != cobra.ShellCompDirectiveNoFileComp {
		t.Fatalf("expected no-file completion directive, got %v", directive)
	}
	var found bool
	for _, value := range values {
		if strings.HasPrefix(value, bootstrap.ClientID+"\t") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected completion list to include %q, got %v", bootstrap.ClientID, values)
	}
}
