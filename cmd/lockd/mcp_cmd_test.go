package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/spf13/viper"
	lockdmcp "pkt.systems/lockd/mcp"
	mcpoauth "pkt.systems/lockd/mcp/oauth"
	"pkt.systems/lockd/mcp/preset"
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
	if flag := mcpCmd.PersistentFlags().Lookup("client-bundle"); flag == nil {
		t.Fatalf("expected --client-bundle on mcp command")
	} else if flag.Shorthand != "B" {
		t.Fatalf("expected --client-bundle shorthand -B, got %q", flag.Shorthand)
	}
	if flag := mcpCmd.PersistentFlags().Lookup("listen"); flag == nil {
		t.Fatalf("expected --listen on mcp command")
	} else if flag.DefValue != "127.0.0.1:19341" {
		t.Fatalf("expected listen default 127.0.0.1:19341, got %q", flag.DefValue)
	}
	if flag := mcpCmd.PersistentFlags().Lookup("default-namespace"); flag == nil {
		t.Fatalf("expected --default-namespace on mcp command")
	} else if flag.DefValue != "mcp" {
		t.Fatalf("expected default namespace mcp, got %q", flag.DefValue)
	}
	if flag := mcpCmd.PersistentFlags().Lookup("agent-bus-queue"); flag == nil {
		t.Fatalf("expected --agent-bus-queue on mcp command")
	} else if flag.DefValue != "lockd.agent.bus" {
		t.Fatalf("expected default agent bus queue lockd.agent.bus, got %q", flag.DefValue)
	}
	if flag := mcpCmd.PersistentFlags().Lookup("inline-max-bytes"); flag == nil {
		t.Fatalf("expected --inline-max-bytes on mcp command")
	} else if flag.DefValue != "2097152" {
		t.Fatalf("expected inline max default 2097152, got %q", flag.DefValue)
	}
	if inherited := mcpCmd.InheritedFlags().Lookup("bundle"); inherited == nil || inherited.Shorthand != "b" {
		t.Fatalf("expected inherited --bundle/-b on mcp command")
	}
	if inherited := mcpCmd.InheritedFlags().Lookup("server"); inherited == nil || inherited.Shorthand != "s" {
		t.Fatalf("expected inherited --server/-s on mcp command")
	}
}

func TestMCPCommandNoBootstrapOrOAuthGroup(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	mcpCmd, _, err := root.Find([]string{"mcp"})
	if err != nil {
		t.Fatalf("find mcp command: %v", err)
	}
	var hasBootstrap, hasOAuth, hasIssuer, hasClient bool
	for _, sub := range mcpCmd.Commands() {
		switch sub.Name() {
		case "bootstrap":
			hasBootstrap = true
		case "oauth":
			hasOAuth = true
		case "issuer":
			hasIssuer = true
		case "client":
			hasClient = true
		}
	}
	if hasBootstrap {
		t.Fatalf("expected mcp bootstrap command to be removed")
	}
	if hasOAuth {
		t.Fatalf("expected mcp oauth command group to be removed")
	}
	if !hasIssuer {
		t.Fatalf("expected mcp issuer command, err=%v", err)
	}
	if !hasClient {
		t.Fatalf("expected mcp client command, err=%v", err)
	}
}

func TestMCPPersistentFlagsReachSubcommands(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	clientCmd, _, err := root.Find([]string{"mcp", "client"})
	if err != nil {
		t.Fatalf("find mcp client: %v", err)
	}
	if flag := clientCmd.InheritedFlags().Lookup("base-url"); flag == nil {
		t.Fatalf("expected --base-url to be inherited by mcp client subcommands")
	}
	if flag := clientCmd.InheritedFlags().Lookup("state-file"); flag == nil {
		t.Fatalf("expected --state-file to be inherited by mcp client subcommands")
	}
}

func TestMCPConfigGlobalFallbacks(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	if err := root.PersistentFlags().Set("server", "https://lockd.example:9341"); err != nil {
		t.Fatalf("set --server: %v", err)
	}
	if err := root.PersistentFlags().Set("bundle", "/tmp/server.pem"); err != nil {
		t.Fatalf("set --bundle: %v", err)
	}

	cfg, err := mcpConfigFromViper()
	if err != nil {
		t.Fatalf("mcpConfigFromViper: %v", err)
	}
	if cfg.UpstreamServer != "https://lockd.example:9341" {
		t.Fatalf("expected upstream server from global --server, got %q", cfg.UpstreamServer)
	}
	if cfg.BundlePath != "/tmp/server.pem" {
		t.Fatalf("expected mcp bundle from global --bundle, got %q", cfg.BundlePath)
	}
}

func TestMCPConfigGlobalClientTimeoutFallbacks(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	if err := root.PersistentFlags().Set("timeout", "11s"); err != nil {
		t.Fatalf("set --timeout: %v", err)
	}
	if err := root.PersistentFlags().Set("close-timeout", "7s"); err != nil {
		t.Fatalf("set --close-timeout: %v", err)
	}
	if err := root.PersistentFlags().Set("keepalive-timeout", "3s"); err != nil {
		t.Fatalf("set --keepalive-timeout: %v", err)
	}

	cfg, err := mcpConfigFromViper()
	if err != nil {
		t.Fatalf("mcpConfigFromViper: %v", err)
	}
	if cfg.UpstreamHTTPTimeout != 11*time.Second {
		t.Fatalf("UpstreamHTTPTimeout=%s want %s", cfg.UpstreamHTTPTimeout, 11*time.Second)
	}
	if cfg.UpstreamCloseTimeout != 7*time.Second {
		t.Fatalf("UpstreamCloseTimeout=%s want %s", cfg.UpstreamCloseTimeout, 7*time.Second)
	}
	if cfg.UpstreamKeepAliveTimeout != 3*time.Second {
		t.Fatalf("UpstreamKeepAliveTimeout=%s want %s", cfg.UpstreamKeepAliveTimeout, 3*time.Second)
	}
}

func TestMCPConfigUpstreamDisableMTLSPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		rootArgs []string
		mcpArgs  []string
		want     bool
	}{
		{
			name:    "mcp specific flag enables upstream disable mTLS",
			mcpArgs: []string{"--disable-mcp-upstream-mtls"},
			want:    true,
		},
		{
			name:     "global disable mTLS falls back when mcp flag unset",
			rootArgs: []string{"--disable-mtls"},
			want:     true,
		},
		{
			name:     "mcp explicit false overrides global true",
			rootArgs: []string{"--disable-mtls"},
			mcpArgs:  []string{"--disable-mcp-upstream-mtls=false"},
			want:     false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)

			root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
			if err := root.PersistentFlags().Parse(tt.rootArgs); err != nil {
				t.Fatalf("parse root args %v: %v", tt.rootArgs, err)
			}
			mcpCmd, _, err := root.Find([]string{"mcp"})
			if err != nil {
				t.Fatalf("find mcp command: %v", err)
			}
			if err := mcpCmd.PersistentFlags().Parse(tt.mcpArgs); err != nil {
				t.Fatalf("parse mcp args %v: %v", tt.mcpArgs, err)
			}

			cfg, err := mcpConfigFromViper()
			if err != nil {
				t.Fatalf("mcpConfigFromViper: %v", err)
			}
			if cfg.UpstreamDisableMTLS != tt.want {
				t.Fatalf("UpstreamDisableMTLS=%v want %v", cfg.UpstreamDisableMTLS, tt.want)
			}
		})
	}
}

func TestMCPCommandAppliesGlobalLogLevel(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	var buf bytes.Buffer
	root := newRootCommand(pslog.NewStructured(context.Background(), &buf))
	orig := mcpNewServer
	t.Cleanup(func() { mcpNewServer = orig })
	mcpNewServer = func(req lockdmcp.NewServerRequest) (lockdmcp.Server, error) {
		req.Logger.Debug("mcp-log-level-probe")
		return mcpStubServer{}, nil
	}

	root.SetArgs([]string{"--log-level", "debug", "mcp", "--disable-tls"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp command: %v", err)
	}
	if !strings.Contains(buf.String(), `"msg":"mcp-log-level-probe"`) {
		t.Fatalf("expected debug probe from mcp logger, got %q", buf.String())
	}
}

func TestMCPCommandUsesGlobalClientFlags(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	orig := mcpNewServer
	t.Cleanup(func() { mcpNewServer = orig })

	var captured lockdmcp.NewServerRequest
	mcpNewServer = func(req lockdmcp.NewServerRequest) (lockdmcp.Server, error) {
		captured = req
		return mcpStubServer{}, nil
	}

	root.SetArgs([]string{
		"--server", "https://lockd.example:9341",
		"--bundle", "/tmp/server.pem",
		"--disable-mtls",
		"--timeout", "11s",
		"--close-timeout", "7s",
		"--keepalive-timeout", "3s",
		"--drain-aware-shutdown=false",
		"mcp",
		"--disable-tls",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp command: %v", err)
	}
	if captured.Config.UpstreamServer != "https://lockd.example:9341" {
		t.Fatalf("UpstreamServer=%q want %q", captured.Config.UpstreamServer, "https://lockd.example:9341")
	}
	if captured.Config.BundlePath != "/tmp/server.pem" {
		t.Fatalf("BundlePath=%q want %q", captured.Config.BundlePath, "/tmp/server.pem")
	}
	if !captured.Config.UpstreamDisableMTLS {
		t.Fatalf("expected UpstreamDisableMTLS=true from global --disable-mtls fallback")
	}
	if captured.Config.UpstreamHTTPTimeout != 11*time.Second {
		t.Fatalf("UpstreamHTTPTimeout=%s want %s", captured.Config.UpstreamHTTPTimeout, 11*time.Second)
	}
	if captured.Config.UpstreamCloseTimeout != 7*time.Second {
		t.Fatalf("UpstreamCloseTimeout=%s want %s", captured.Config.UpstreamCloseTimeout, 7*time.Second)
	}
	if captured.Config.UpstreamKeepAliveTimeout != 3*time.Second {
		t.Fatalf("UpstreamKeepAliveTimeout=%s want %s", captured.Config.UpstreamKeepAliveTimeout, 3*time.Second)
	}
	if captured.Config.UpstreamDrainAware == nil || *captured.Config.UpstreamDrainAware {
		t.Fatalf("expected UpstreamDrainAware=false from global --drain-aware-shutdown=false, got %v", captured.Config.UpstreamDrainAware)
	}
}

func TestMCPCommandAutoInitIssuerDefaultsFromBaseURL(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	orig := mcpNewServer
	t.Cleanup(func() { mcpNewServer = orig })
	mcpNewServer = func(req lockdmcp.NewServerRequest) (lockdmcp.Server, error) {
		return mcpStubServer{}, nil
	}

	root.SetArgs([]string{
		"mcp",
		"--base-url", "https://public.example/mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp command: %v", err)
	}
	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if data.Issuer != "https://public.example/mcp" {
		t.Fatalf("issuer=%q want %q", data.Issuer, "https://public.example/mcp")
	}
}

func TestMCPCommandPrefersMCPDisableMTLSOverGlobal(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	orig := mcpNewServer
	t.Cleanup(func() { mcpNewServer = orig })

	var captured lockdmcp.NewServerRequest
	mcpNewServer = func(req lockdmcp.NewServerRequest) (lockdmcp.Server, error) {
		captured = req
		return mcpStubServer{}, nil
	}

	root.SetArgs([]string{
		"--disable-mtls",
		"mcp",
		"--disable-tls",
		"--disable-mcp-upstream-mtls=false",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp command: %v", err)
	}
	if captured.Config.UpstreamDisableMTLS {
		t.Fatalf("expected mcp-specific --disable-mcp-upstream-mtls=false to override global --disable-mtls")
	}
}

func TestGlobalConfigAppliesToAllSubcommands(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{"--config", "/tmp/lockd-test-missing-config.yaml", "config", "gen", "--stdout"})
	if err := root.Execute(); err == nil {
		t.Fatalf("expected missing --config file to fail for non-mcp subcommand")
	}
}

func TestGlobalConfigAppliesToVersionSubcommand(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{"--config", "/tmp/lockd-test-missing-config.yaml", "version", "--version"})
	if err := root.Execute(); err == nil {
		t.Fatalf("expected missing --config file to fail for version subcommand")
	}
}

func TestMCPClientAdminSubcommands(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	clientCmd, _, err := root.Find([]string{"mcp", "client"})
	if err != nil {
		t.Fatalf("find mcp client command: %v", err)
	}
	if clientCmd == nil {
		t.Fatalf("expected mcp client command")
	}
	if showCmd, _, err := clientCmd.Find([]string{"show"}); err != nil || showCmd == nil || showCmd.Name() != "show" {
		t.Fatalf("expected show subcommand, err=%v", err)
	}
	if credCmd, _, err := clientCmd.Find([]string{"credentials"}); err != nil || credCmd == nil || credCmd.Name() != "credentials" {
		t.Fatalf("expected credentials subcommand, err=%v", err)
	}
}

func TestMCPClientIDCompletionRegistered(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	paths := [][]string{
		{"mcp", "client", "show"},
		{"mcp", "client", "credentials"},
		{"mcp", "client", "remove"},
		{"mcp", "client", "revoke"},
		{"mcp", "client", "restore"},
		{"mcp", "client", "rotate-secret"},
		{"mcp", "client", "update"},
	}
	for _, p := range paths {
		cmd, _, err := root.Find(p)
		if err != nil {
			t.Fatalf("find %v: %v", p, err)
		}
		if cmd == nil {
			t.Fatalf("expected command for %v", p)
		}
		if cmd.ValidArgsFunction == nil {
			t.Fatalf("expected positional id completion func on %v", p)
		}
	}
}

func TestMCPClientIDCompletionValues(t *testing.T) {
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
	t.Setenv("LOCKD_MCP_TOKEN_STORE", filepath.Join(dir, "mcp-token-store.enc.json"))

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	paths := [][]string{
		{"mcp", "client", "show"},
		{"mcp", "client", "credentials"},
		{"mcp", "client", "remove"},
		{"mcp", "client", "revoke"},
		{"mcp", "client", "restore"},
		{"mcp", "client", "rotate-secret"},
		{"mcp", "client", "update"},
	}
	for _, p := range paths {
		cmd, _, err := root.Find(p)
		if err != nil {
			t.Fatalf("find %v command: %v", p, err)
		}
		if cmd.ValidArgsFunction == nil {
			t.Fatalf("missing positional completion for %v", p)
		}
		values, directive := cmd.ValidArgsFunction(cmd, nil, "")
		if directive != cobra.ShellCompDirectiveNoFileComp {
			t.Fatalf("expected no-file completion directive for %v, got %v", p, directive)
		}
		var found bool
		for _, value := range values {
			if strings.HasPrefix(value, bootstrap.ClientID+"\t") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected completion list to include %q for %v, got %v", bootstrap.ClientID, p, values)
		}
	}
}

func TestMCPClientShowUsesPositionalID(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	bootstrap, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap state: %v", err)
	}

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "show", bootstrap.ClientID,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client show <id>: %v", err)
	}
	if !strings.Contains(stdout.String(), "client_id: "+bootstrap.ClientID) {
		t.Fatalf("expected show output to include client_id, got %q", stdout.String())
	}
}

func TestMCPClientIDCommandsRejectLegacyIDFlag(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	testCases := []struct {
		name string
		args []string
	}{
		{name: "show", args: []string{"show", "--id", "abc"}},
		{name: "credentials", args: []string{"credentials", "--id", "abc"}},
		{name: "remove", args: []string{"remove", "--id", "abc"}},
		{name: "revoke", args: []string{"revoke", "--id", "abc"}},
		{name: "restore", args: []string{"restore", "--id", "abc"}},
		{name: "rotate-secret", args: []string{"rotate-secret", "--id", "abc"}},
		{name: "update", args: []string{"update", "--id", "abc", "--name", "renamed"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
			root.SetArgs(append([]string{"mcp", "client"}, tc.args...))
			if err := root.Execute(); err == nil {
				t.Fatalf("expected mcp client %s --id to fail", tc.name)
			}
		})
	}
}

func TestMCPClientCredentialsUsesPositionalID(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, clientID := bootstrapMCPStateForClientTests(t)
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "credentials", clientID,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client credentials <id>: %v", err)
	}
	if !strings.Contains(stdout.String(), "LOCKD_MCP_OAUTH_CLIENT_ID="+clientID) {
		t.Fatalf("expected credentials output to include client id %q, got %q", clientID, stdout.String())
	}
	if !strings.Contains(stdout.String(), "LOCKD_MCP_OAUTH_AUTHORIZE_URL=") {
		t.Fatalf("expected credentials output to include authorize url, got %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "LOCKD_MCP_OAUTH_REGISTRATION_URL=") {
		t.Fatalf("expected credentials output to include registration url, got %q", stdout.String())
	}
}

func TestMCPClientAddAndUpdateRedirectURIs(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, _ := bootstrapMCPStateForClientTests(t)
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "add",
		"--name", "chatgpt",
		"--redirect-uri", "https://chat.openai.com/aip/callback",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client add with redirect uri: %v", err)
	}
	clientID := parseOutputField(stdout.String(), "client_id:")
	if clientID == "" {
		t.Fatalf("expected client_id in add output, got %q", stdout.String())
	}

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "update", clientID,
		"--redirect-uri", "https://chat.openai.com/aip/callback2",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client update with redirect uri: %v", err)
	}

	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after update: %v", err)
	}
	client, ok := findClientByID(data.Clients, clientID)
	if !ok {
		t.Fatalf("client %s not found after update", clientID)
	}
	if len(client.RedirectURIs) != 1 || client.RedirectURIs[0] != "https://chat.openai.com/aip/callback2" {
		t.Fatalf("redirect uris=%v want [https://chat.openai.com/aip/callback2]", client.RedirectURIs)
	}
}

func TestMCPClientAddAndUpdateNamespace(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, _ := bootstrapMCPStateForClientTests(t)
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "add",
		"--name", "chatgpt",
		"-n", "ChatGPT",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client add with namespace: %v", err)
	}
	clientID := parseOutputField(stdout.String(), "client_id:")
	if clientID == "" {
		t.Fatalf("expected client_id in add output, got %q", stdout.String())
	}

	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after add: %v", err)
	}
	client, ok := findClientByID(data.Clients, clientID)
	if !ok {
		t.Fatalf("client %s not found after add", clientID)
	}
	if client.Namespace != "chatgpt" {
		t.Fatalf("namespace=%q want %q", client.Namespace, "chatgpt")
	}

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "update", clientID,
		"-n", "chatgpt2",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client update with namespace: %v", err)
	}
	data, err = mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after update: %v", err)
	}
	client, ok = findClientByID(data.Clients, clientID)
	if !ok {
		t.Fatalf("client %s not found after update", clientID)
	}
	if client.Namespace != "chatgpt2" {
		t.Fatalf("namespace=%q want %q", client.Namespace, "chatgpt2")
	}

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "update", clientID,
		"--namespace", "",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client update clear namespace: %v", err)
	}
	data, err = mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after clear: %v", err)
	}
	client, ok = findClientByID(data.Clients, clientID)
	if !ok {
		t.Fatalf("client %s not found after clear", clientID)
	}
	if client.Namespace != "" {
		t.Fatalf("namespace=%q want empty", client.Namespace)
	}
}

func TestMCPClientAddAndUpdatePresets(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, _ := bootstrapMCPStateForClientTests(t)
	presetPath := writeTestMCPPresetFile(t, `
preset: memory
kinds:
  - name: note
    namespace: agents
    schema:
      type: object
      properties:
        text:
          type: string
`)
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "add",
		"--name", "memory-client",
		"--preset", presetPath,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client add with preset: %v", err)
	}
	clientID := parseOutputField(stdout.String(), "client_id:")
	if clientID == "" {
		t.Fatalf("expected client_id in add output, got %q", stdout.String())
	}

	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after add: %v", err)
	}
	client, ok := findClientByID(data.Clients, clientID)
	if !ok {
		t.Fatalf("client %s not found after add", clientID)
	}
	if client.LockdPreset {
		t.Fatalf("expected lockd preset disabled for custom-only add")
	}
	if len(client.Presets) != 1 || client.Presets[0].Name != "memory" {
		t.Fatalf("unexpected presets after add: %#v", client.Presets)
	}

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "update", clientID,
		"--preset", "lockd",
		"--preset", presetPath,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client update with presets: %v", err)
	}

	data, err = mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after update: %v", err)
	}
	client, ok = findClientByID(data.Clients, clientID)
	if !ok {
		t.Fatalf("client %s not found after update", clientID)
	}
	if !client.LockdPreset {
		t.Fatalf("expected lockd preset enabled after update")
	}
	if len(client.Presets) != 1 || client.Presets[0].Name != "memory" {
		t.Fatalf("unexpected presets after update: %#v", client.Presets)
	}
}

func TestMCPClientListAndShowIncludePresets(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, _ := bootstrapMCPStateForClientTests(t)
	presetPath := writeTestMCPPresetFile(t, `
preset: memory
kinds:
  - name: note
    namespace: agents
    schema:
      type: object
      properties:
        text:
          type: string
`)
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "add",
		"--name", "memory-client",
		"--preset", "lockd",
		"--preset", presetPath,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client add: %v", err)
	}
	clientID := parseOutputField(stdout.String(), "client_id:")

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	stdout.Reset()
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "list",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client list: %v", err)
	}
	if !strings.Contains(stdout.String(), "PRESETS") || !strings.Contains(stdout.String(), "lockd,memory") {
		t.Fatalf("expected presets in client list output, got %q", stdout.String())
	}

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	stdout.Reset()
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "show", clientID,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client show: %v", err)
	}
	if !strings.Contains(stdout.String(), "presets: lockd,memory") {
		t.Fatalf("expected presets in client show output, got %q", stdout.String())
	}
}

func TestMCPClientToolsListUsesClientSurface(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, _ := bootstrapMCPStateForClientTests(t)
	presetPath := writeTestMCPPresetFile(t, `
preset: memory
kinds:
  - name: note
    namespace: memory
    schema:
      type: object
      properties:
        text:
          type: string
`)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var addOut bytes.Buffer
	root.SetOut(&addOut)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "add",
		"--name", "memory-client",
		"--preset", presetPath,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client add: %v", err)
	}
	clientID := parseOutputField(addOut.String(), "client_id:")
	if clientID == "" {
		t.Fatalf("expected client_id in add output, got %q", addOut.String())
	}

	origPrettyX := mcpPrettyXRunner
	t.Cleanup(func() { mcpPrettyXRunner = origPrettyX })
	mcpPrettyXRunner = func(_ context.Context, payload []byte, out io.Writer) error {
		_, err := out.Write(payload)
		return err
	}

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "tools-list", clientID,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client tools-list: %v", err)
	}

	lines := bytes.Split(bytes.TrimSpace(stdout.Bytes()), []byte("\n"))
	if len(lines) < 2 {
		t.Fatalf("expected multiple jsonl lines, got %d", len(lines))
	}
	var first lockdmcp.ToolsListResponse
	if err := json.Unmarshal(lines[0], &first); err != nil {
		t.Fatalf("decode first line tools/list response: %v", err)
	}
	if len(first.Result.Tools) == 0 {
		t.Fatalf("expected tools in tools-list output")
	}
	for _, tool := range first.Result.Tools {
		if tool == nil {
			continue
		}
		if strings.HasPrefix(tool.Name, "lockd.") {
			t.Fatalf("did not expect lockd tool in memory-only client surface: %q", tool.Name)
		}
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"name":"memory.help"`)) {
		t.Fatalf("expected memory.help tool record in output, got %q", stdout.String())
	}
}

func TestMCPClientToolsListWritesOutputFile(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, clientID := bootstrapMCPStateForClientTests(t)
	origPrettyX := mcpPrettyXRunner
	t.Cleanup(func() { mcpPrettyXRunner = origPrettyX })
	mcpPrettyXRunner = func(_ context.Context, payload []byte, out io.Writer) error {
		_, err := out.Write(payload)
		return err
	}

	outPath := filepath.Join(t.TempDir(), "tools.jsonl")
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "tools-list", clientID,
		"--out", outPath,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client tools-list --out: %v", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read tools-list output file: %v", err)
	}
	if !bytes.Contains(data, []byte(`"jsonrpc":"2.0"`)) {
		t.Fatalf("expected tools/list envelope in file output, got %q", string(data))
	}
}

func TestMCPPresetCommandListsAndExportsBuiltIns(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{"mcp", "preset", "list"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp preset list: %v", err)
	}
	if !strings.Contains(stdout.String(), "memory") {
		t.Fatalf("expected memory in preset list, got %q", stdout.String())
	}

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	stdout.Reset()
	root.SetOut(&stdout)
	root.SetArgs([]string{"mcp", "preset", "get", "memory"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp preset get memory: %v", err)
	}
	defs, err := preset.ParseYAML(stdout.Bytes())
	if err != nil {
		t.Fatalf("parse built-in preset yaml: %v", err)
	}
	if len(defs) != 1 || defs[0].Name != "memory" {
		t.Fatalf("unexpected built-in preset defs: %#v", defs)
	}

	outPath := filepath.Join(t.TempDir(), "presets.yaml")
	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	stdout.Reset()
	root.SetOut(&stdout)
	root.SetArgs([]string{"mcp", "preset", "get", "memory", "--out", outPath})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp preset get --out: %v", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read preset output: %v", err)
	}
	if !bytes.Contains(data, []byte("preset: memory")) {
		t.Fatalf("expected memory preset in written yaml, got %q", string(data))
	}
}

func TestMCPPresetCommandHelpUsesSubcommands(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{"mcp", "preset", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp preset --help: %v", err)
	}
	output := stdout.String()
	if !strings.Contains(output, "list") || !strings.Contains(output, "get") {
		t.Fatalf("expected list/get in help output, got %q", output)
	}
	if strings.Contains(output, "  -l, --list ") || strings.Contains(output, "\n      --list ") {
		t.Fatalf("did not expect legacy --list flag in help output, got %q", output)
	}
	if strings.Contains(output, "DisableFlagParsing") {
		t.Fatalf("did not expect custom parser artifacts in help output, got %q", output)
	}
}

func TestMCPClientNamespaceValidationErrors(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, clientID := bootstrapMCPStateForClientTests(t)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "add",
		"--name", "bad",
		"-n", "bad/namespace",
	})
	if err := root.Execute(); err == nil {
		t.Fatalf("expected add with invalid namespace to fail")
	}

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "update", clientID,
		"-n", "bad/namespace",
	})
	if err := root.Execute(); err == nil {
		t.Fatalf("expected update with invalid namespace to fail")
	}
}

func TestMCPClientRevokeRestoreUsePositionalID(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, clientID := bootstrapMCPStateForClientTests(t)
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "revoke", clientID,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client revoke <id>: %v", err)
	}
	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after revoke: %v", err)
	}
	revokedClient, ok := findClientByID(data.Clients, clientID)
	if !ok {
		t.Fatalf("client %s not found after revoke", clientID)
	}
	if !revokedClient.Revoked {
		t.Fatalf("expected client %s revoked after revoke command", clientID)
	}

	root = newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "restore", clientID,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client restore <id>: %v", err)
	}
	data, err = mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after restore: %v", err)
	}
	restoredClient, ok := findClientByID(data.Clients, clientID)
	if !ok {
		t.Fatalf("client %s not found after restore", clientID)
	}
	if restoredClient.Revoked {
		t.Fatalf("expected client %s active after restore command", clientID)
	}
}

func TestMCPClientRotateSecretUsesPositionalID(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, clientID := bootstrapMCPStateForClientTests(t)
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "rotate-secret", clientID,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client rotate-secret <id>: %v", err)
	}
	if !strings.Contains(stdout.String(), "client_id: "+clientID) {
		t.Fatalf("expected rotate-secret output to include client id %q, got %q", clientID, stdout.String())
	}
	if !strings.Contains(stdout.String(), "client_secret: ") {
		t.Fatalf("expected rotate-secret output to include client_secret, got %q", stdout.String())
	}
}

func TestMCPClientUpdateUsesPositionalID(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, clientID := bootstrapMCPStateForClientTests(t)
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "update", clientID,
		"--name", "renamed-client",
		"--scope", "alpha",
		"--scope", "beta",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client update <id>: %v", err)
	}
	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after update: %v", err)
	}
	updatedClient, ok := findClientByID(data.Clients, clientID)
	if !ok {
		t.Fatalf("client %s not found after update", clientID)
	}
	if updatedClient.Name != "renamed-client" {
		t.Fatalf("expected updated name %q, got %q", "renamed-client", updatedClient.Name)
	}
	if strings.Join(updatedClient.Scopes, ",") != "alpha,beta" {
		t.Fatalf("expected updated scopes alpha,beta, got %v", updatedClient.Scopes)
	}
}

func TestMCPClientRemoveUsesPositionalID(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	statePath, tokenStorePath, clientID := bootstrapMCPStateForClientTests(t)
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "remove", clientID,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client remove <id>: %v", err)
	}
	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after remove: %v", err)
	}
	if _, ok := findClientByID(data.Clients, clientID); ok {
		t.Fatalf("expected client %s removed from state", clientID)
	}
}

func TestMCPClientAddAutoInitializesState(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--base-url", "https://public.example/mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "add",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client add: %v", err)
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("expected state file after add, stat err: %v", err)
	}
	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(data.Clients) != 2 {
		t.Fatalf("expected bootstrap+added clients, got %d", len(data.Clients))
	}
	if data.Issuer != "https://public.example/mcp" {
		t.Fatalf("expected issuer defaulted from --base-url, got %q", data.Issuer)
	}
}

func TestMCPClientAddUsesDefaultNameWhenEmpty(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "add",
		"--name", "",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client add with empty --name: %v", err)
	}
	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(data.Clients) != 2 {
		t.Fatalf("expected bootstrap+added clients, got %d", len(data.Clients))
	}
}

func TestMCPClientAddAutoInitIssuerFollowsBaseURLAcrossFreshState(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	run := func(baseURL string) string {
		root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
		root.SetArgs([]string{
			"mcp",
			"--base-url", baseURL,
			"--state-file", statePath,
			"--token-store", tokenStorePath,
			"client", "add",
		})
		if err := root.Execute(); err != nil {
			t.Fatalf("execute mcp client add with base-url %q: %v", baseURL, err)
		}
		data, err := mcpstate.Load(statePath)
		if err != nil {
			t.Fatalf("load state: %v", err)
		}
		return data.Issuer
	}

	first := run("https://first.example/mcp")
	if first != "https://first.example/mcp" {
		t.Fatalf("issuer first=%q want %q", first, "https://first.example/mcp")
	}
	if err := os.Remove(statePath); err != nil {
		t.Fatalf("remove state for fresh re-init: %v", err)
	}
	_ = os.Remove(tokenStorePath)
	second := run("https://second.example/edge")
	if second != "https://second.example/edge" {
		t.Fatalf("issuer second=%q want %q", second, "https://second.example/edge")
	}
}

func TestMCPClientAddAutoInitIssuerPublishesOAuthMetadataFromBaseURL(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	baseURL := "https://public.example/edge/mcp"

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--base-url", baseURL,
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "add",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp client add: %v", err)
	}

	mgr, err := mcpoauth.NewManager(mcpoauth.ManagerConfig{
		StatePath:  statePath,
		TokenStore: tokenStorePath,
	})
	if err != nil {
		t.Fatalf("new oauth manager: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", http.NoBody)
	rr := httptest.NewRecorder()
	mgr.HandleAuthServerMetadata(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("auth metadata status=%d body=%q", rr.Code, rr.Body.String())
	}

	var meta mcpoauth.AuthServerMetadata
	if err := json.Unmarshal(rr.Body.Bytes(), &meta); err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if meta.Issuer != baseURL {
		t.Fatalf("issuer=%q want %q", meta.Issuer, baseURL)
	}
	if meta.AuthorizationEndpoint != baseURL+"/authorize" {
		t.Fatalf("authorization_endpoint=%q want %q", meta.AuthorizationEndpoint, baseURL+"/authorize")
	}
	if meta.TokenEndpoint != baseURL+"/token" {
		t.Fatalf("token_endpoint=%q want %q", meta.TokenEndpoint, baseURL+"/token")
	}
	if meta.RegistrationEndpoint != baseURL+"/register" {
		t.Fatalf("registration_endpoint=%q want %q", meta.RegistrationEndpoint, baseURL+"/register")
	}
}

func TestDefaultMCPIssuerUsesBaseURLWhenProvided(t *testing.T) {
	cfg := lockdmcp.Config{
		BaseURL: "https://public.example/mcp/v1",
		Listen:  "127.0.0.1:19341",
	}
	issuer, err := defaultMCPIssuer(cfg)
	if err != nil {
		t.Fatalf("defaultMCPIssuer: %v", err)
	}
	if issuer != "https://public.example/mcp/v1" {
		t.Fatalf("issuer=%q want %q", issuer, "https://public.example/mcp/v1")
	}
}

func TestMCPIssuerSetAutoInitializesState(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"issuer", "set",
		"https://issuer.example",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute mcp issuer set: %v", err)
	}
	data, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if data.Issuer != "https://issuer.example" {
		t.Fatalf("issuer=%q want %q", data.Issuer, "https://issuer.example")
	}
}

func TestMCPIssuerSetRejectsLegacyIssuerFlag(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{"mcp", "issuer", "set", "--issuer", "https://issuer.example"})
	if err := root.Execute(); err == nil {
		t.Fatalf("expected mcp issuer set --issuer to fail")
	}
}

func TestMCPIssuerGetDoesNotEmitOAuthInternalLogs(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	_, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://issuer.example",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap state: %v", err)
	}

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"issuer", "get",
	})
	stderr := captureMCPStderr(t, func() {
		err = root.Execute()
	})
	if err != nil {
		t.Fatalf("execute mcp issuer get: %v", err)
	}
	if strings.Contains(stderr, "oauth state reloaded") {
		t.Fatalf("unexpected oauth internal log output on stderr: %q", stderr)
	}
	if got := strings.TrimSpace(stdout.String()); got != "https://issuer.example" {
		t.Fatalf("issuer output=%q want %q", got, "https://issuer.example")
	}
}

func TestMCPClientListDoesNotAutoInitializeState(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "list",
	})
	err := root.Execute()
	if err == nil {
		t.Fatalf("expected list to fail when state is missing")
	}
	if _, statErr := os.Stat(statePath); !os.IsNotExist(statErr) {
		t.Fatalf("expected list to not create state, stat err=%v", statErr)
	}
}

func TestMCPClientListDoesNotEmitOAuthInternalLogs(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	bootstrap, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap state: %v", err)
	}

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"mcp",
		"--state-file", statePath,
		"--token-store", tokenStorePath,
		"client", "list",
	})
	stderr := captureMCPStderr(t, func() {
		err = root.Execute()
	})
	if err != nil {
		t.Fatalf("execute mcp client list: %v", err)
	}
	if strings.Contains(stderr, "oauth state reloaded") {
		t.Fatalf("unexpected oauth internal log output on stderr: %q", stderr)
	}
	out := stdout.String()
	if !strings.Contains(out, "DEFAULT_NS") {
		t.Fatalf("expected client list header to include DEFAULT_NS, got %q", out)
	}
	if strings.Contains(out, "NAMESPACE\tSTATUS") {
		t.Fatalf("expected client list header to use DEFAULT_NS instead of NAMESPACE, got %q", out)
	}
	if !strings.Contains(out, bootstrap.ClientID) {
		t.Fatalf("expected client list output to include client id %q, got %q", bootstrap.ClientID, out)
	}
}

func captureMCPStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	orig := os.Stderr
	os.Stderr = w
	t.Cleanup(func() {
		os.Stderr = orig
		_ = r.Close()
		_ = w.Close()
	})

	fn()

	_ = w.Close()
	var out bytes.Buffer
	if _, err := io.Copy(&out, r); err != nil {
		t.Fatalf("copy stderr: %v", err)
	}
	return out.String()
}

func bootstrapMCPStateForClientTests(t *testing.T) (statePath, tokenStorePath, clientID string) {
	t.Helper()
	dir := t.TempDir()
	statePath = filepath.Join(dir, "mcp.pem")
	tokenStorePath = filepath.Join(dir, "mcp-token-store.enc.json")
	bootstrap, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap state: %v", err)
	}
	return statePath, tokenStorePath, bootstrap.ClientID
}

func findClientByID(clients map[string]mcpstate.Client, clientID string) (mcpstate.Client, bool) {
	client, ok := clients[clientID]
	return client, ok
}

func writeTestMCPPresetFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "preset.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(data)+"\n"), 0o644); err != nil {
		t.Fatalf("write test preset file: %v", err)
	}
	return path
}

func parseOutputField(out, key string) string {
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), key) {
			value := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), key))
			return value
		}
	}
	return ""
}

type mcpStubServer struct{}

func (mcpStubServer) Run(context.Context) error { return nil }
