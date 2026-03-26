package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"pkt.systems/lockd"
	lockdmcp "pkt.systems/lockd/mcp"
	mcpadmin "pkt.systems/lockd/mcp/admin"
	"pkt.systems/lockd/mcp/preset"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

const (
	mcpListenKey           = "mcp.listen"
	mcpServerKey           = "mcp.server"
	mcpClientBundleKey     = "mcp.client_bundle"
	mcpBundleKey           = "mcp.bundle"
	mcpDisableTLSKey       = "mcp.disable_tls"
	mcpBaseURLKey          = "mcp.base_url"
	mcpAllowHTTPKey        = "mcp.allow_http"
	mcpDisableMTLSKey      = "mcp.disable_mtls"
	mcpInlineMaxBytesKey   = "mcp.inline_max_bytes"
	mcpDefaultNamespaceKey = "mcp.default_namespace"
	mcpAgentBusQueueKey    = "mcp.agent_bus_queue"
	mcpStateFileKey        = "mcp.state_file"
	mcpTokenStoreKey       = "mcp.token_store"
	mcpIssuerKey           = "mcp.issuer"
	mcpPathKey             = "mcp.path"
	mcpOAuthResourceURLKey = "mcp.oauth_resource_url"
)

var mcpNewServer = lockdmcp.NewServer
var mcpPrettyXRunner = runPrettyX

const mcpAutoBootstrapClientName = "default"

func newMCPCommand(baseLogger pslog.Logger, inheritedServerFlag, inheritedBundleFlag, inheritedDisableMTLSFlag *pflag.Flag) *cobra.Command {
	serveCmd := &cobra.Command{
		Use:   "mcp",
		Short: "Run and administer the lockd MCP facade server",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := mcpConfigFromViper()
			if err != nil {
				return err
			}
			if !cfg.DisableTLS {
				adminSvc := newMCPAdminService(cfg)
				if err := ensureMCPStateInitialized(cfg, adminSvc, strings.TrimSpace(viper.GetString(mcpIssuerKey))); err != nil {
					return err
				}
			}
			svc, err := mcpNewServer(lockdmcp.NewServerRequest{
				Config: cfg,
				Logger: applyGlobalLogLevel(baseLogger),
			})
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			return svc.Run(ctx)
		},
	}

	flags := serveCmd.PersistentFlags()
	flags.StringP("listen", "l", "127.0.0.1:19341", "listen address for the MCP server")
	flags.StringP("client-bundle", "B", "", "lockd client bundle PEM used by MCP when calling upstream lockd")
	flags.Bool("disable-tls", false, "disable TLS and OAuth enforcement for MCP HTTP endpoint")
	flags.String("base-url", "", "externally reachable MCP base URL; OAuth/.well-known are rooted at this path and mcp-path is resolved beneath it (required, e.g. https://host or https://host/mcp)")
	flags.Bool("allow-http", false, "allow http:// base URLs for transfer capability URLs (unsafe; default requires https)")
	flags.String("state-file", "", "MCP OAuth state file path (default $HOME/.lockd/mcp.pem)")
	flags.String("token-store", "", "encrypted OAuth access-token store path (default $HOME/.lockd/mcp-token-store.enc.json)")
	flags.String("issuer", "", "OAuth issuer URL (used by issuer set and runtime state initialization; default derives from --base-url)")
	flags.String("mcp-path", "/", "MCP streamable endpoint path relative to --base-url path (default /)")
	flags.String("oauth-resource-url", "", "OAuth protected resource identifier URL (default <issuer> + <mcp-path>)")
	flags.Bool("disable-mcp-upstream-mtls", false, "disable mTLS when MCP calls upstream lockd")
	flags.Int64("inline-max-bytes", 2*1024*1024, "maximum decoded inline payload bytes for state.update and queue.enqueue")
	flags.String("default-namespace", "mcp", "default namespace used by MCP tools when not explicitly set")
	flags.String("agent-bus-queue", "lockd.agent.bus", "queue auto-subscribed for each MCP session")

	mustBindMCPFlag(mcpListenKey, "LOCKD_MCP_LISTEN", flags.Lookup("listen"))
	mustBindMCPFlag(mcpClientBundleKey, "LOCKD_MCP_CLIENT_BUNDLE", flags.Lookup("client-bundle"))
	mustBindMCPFlag(mcpDisableTLSKey, "LOCKD_MCP_DISABLE_TLS", flags.Lookup("disable-tls"))
	mustBindMCPFlag(mcpBaseURLKey, "LOCKD_MCP_BASE_URL", flags.Lookup("base-url"))
	mustBindMCPFlag(mcpAllowHTTPKey, "LOCKD_MCP_ALLOW_HTTP", flags.Lookup("allow-http"))
	mustBindMCPFlag(mcpStateFileKey, "LOCKD_MCP_STATE_FILE", flags.Lookup("state-file"))
	mustBindMCPFlag(mcpTokenStoreKey, "LOCKD_MCP_TOKEN_STORE", flags.Lookup("token-store"))
	mustBindMCPFlag(mcpIssuerKey, "LOCKD_MCP_ISSUER", flags.Lookup("issuer"))
	mustBindMCPFlag(mcpPathKey, "LOCKD_MCP_PATH", flags.Lookup("mcp-path"))
	mustBindMCPFlag(mcpOAuthResourceURLKey, "LOCKD_MCP_OAUTH_RESOURCE_URL", flags.Lookup("oauth-resource-url"))
	mustBindMCPFlag(mcpDisableMTLSKey, "LOCKD_MCP_DISABLE_MTLS", flags.Lookup("disable-mcp-upstream-mtls"))
	mustBindMCPFlag(mcpInlineMaxBytesKey, "LOCKD_MCP_INLINE_MAX_BYTES", flags.Lookup("inline-max-bytes"))
	mustBindMCPFlag(mcpDefaultNamespaceKey, "LOCKD_MCP_DEFAULT_NAMESPACE", flags.Lookup("default-namespace"))
	mustBindMCPFlag(mcpAgentBusQueueKey, "LOCKD_MCP_AGENT_BUS_QUEUE", flags.Lookup("agent-bus-queue"))

	if inheritedServerFlag != nil {
		mustBindMCPFlag(mcpServerKey, "LOCKD_MCP_SERVER", inheritedServerFlag)
	}
	if inheritedBundleFlag != nil {
		mustBindMCPFlag(mcpBundleKey, "LOCKD_MCP_BUNDLE", inheritedBundleFlag)
	}
	if inheritedDisableMTLSFlag != nil {
		mustBindMCPFlag(clientDisableMTLSKey, "LOCKD_CLIENT_DISABLE_MTLS", inheritedDisableMTLSFlag)
	}

	serveCmd.AddCommand(newMCPIssuerCommand())
	serveCmd.AddCommand(newMCPClientCommand())
	serveCmd.AddCommand(newMCPPresetCommand())
	serveCmd.AddCommand(newMCPExportCACommand())
	return serveCmd
}

func mustBindMCPFlag(key, env string, flag *pflag.Flag) {
	if flag == nil {
		panic(fmt.Sprintf("flag for key %s not found", key))
	}
	if err := viper.BindPFlag(key, flag); err != nil {
		panic(err)
	}
	if env != "" {
		if err := viper.BindEnv(key, env); err != nil {
			panic(err)
		}
	}
}

func mcpConfigFromViper() (lockdmcp.Config, error) {
	upstreamDrainAware := viper.GetBool(clientDrainAwareKey)
	if !viper.IsSet(clientDrainAwareKey) {
		upstreamDrainAware = true
	}
	cfg := lockdmcp.Config{
		Listen:                    strings.TrimSpace(viper.GetString(mcpListenKey)),
		DisableTLS:                viper.GetBool(mcpDisableTLSKey),
		BaseURL:                   strings.TrimSpace(viper.GetString(mcpBaseURLKey)),
		AllowHTTP:                 viper.GetBool(mcpAllowHTTPKey),
		BundlePath:                strings.TrimSpace(viper.GetString(mcpBundleKey)),
		UpstreamServer:            strings.TrimSpace(viper.GetString(mcpServerKey)),
		InlineMaxBytes:            viper.GetInt64(mcpInlineMaxBytesKey),
		DefaultNamespace:          strings.TrimSpace(viper.GetString(mcpDefaultNamespaceKey)),
		AgentBusQueue:             strings.TrimSpace(viper.GetString(mcpAgentBusQueueKey)),
		UpstreamDisableMTLS:       viper.GetBool(mcpDisableMTLSKey),
		UpstreamHTTPTimeout:       viper.GetDuration(clientTimeoutKey),
		UpstreamCloseTimeout:      viper.GetDuration(clientCloseTimeoutKey),
		UpstreamKeepAliveTimeout:  viper.GetDuration(clientKeepAliveTimeoutKey),
		UpstreamDrainAware:        &upstreamDrainAware,
		UpstreamClientBundlePath:  strings.TrimSpace(viper.GetString(mcpClientBundleKey)),
		OAuthStatePath:            strings.TrimSpace(viper.GetString(mcpStateFileKey)),
		OAuthTokenStorePath:       strings.TrimSpace(viper.GetString(mcpTokenStoreKey)),
		MCPPath:                   strings.TrimSpace(viper.GetString(mcpPathKey)),
		OAuthProtectedResourceURL: strings.TrimSpace(viper.GetString(mcpOAuthResourceURLKey)),
	}
	if cfg.UpstreamServer == "" {
		cfg.UpstreamServer = strings.TrimSpace(viper.GetString(clientServerKey))
	}
	if cfg.BundlePath == "" {
		cfg.BundlePath = strings.TrimSpace(viper.GetString("bundle"))
	}
	if !viper.IsSet(mcpDisableMTLSKey) {
		cfg.UpstreamDisableMTLS = viper.GetBool(clientDisableMTLSKey)
	}
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:19341"
	}
	if cfg.OAuthStatePath != "" {
		var err error
		cfg.OAuthStatePath, err = expandPath(cfg.OAuthStatePath)
		if err != nil {
			return lockdmcp.Config{}, fmt.Errorf("expand MCP state file: %w", err)
		}
	}
	if cfg.OAuthTokenStorePath != "" {
		var err error
		cfg.OAuthTokenStorePath, err = expandPath(cfg.OAuthTokenStorePath)
		if err != nil {
			return lockdmcp.Config{}, fmt.Errorf("expand MCP token store path: %w", err)
		}
	}
	return cfg, nil
}

func newMCPIssuerCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "issuer", Short: "Get or set OAuth issuer"}
	cmd.AddCommand(&cobra.Command{
		Use:   "get",
		Short: "Print current OAuth issuer",
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			issuer, err := adminSvc.Issuer()
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), issuer)
			return nil
		},
	})
	setCmd := &cobra.Command{
		Use:   "set <issuer>",
		Short: "Set OAuth issuer",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			value := strings.TrimSpace(args[0])
			if value == "" {
				return fmt.Errorf("issuer is required")
			}
			cfg, err := mcpConfigFromViper()
			if err != nil {
				return err
			}
			adminSvc := newMCPAdminService(cfg)
			if err := ensureMCPStateInitialized(cfg, adminSvc, value); err != nil {
				return err
			}
			if err := adminSvc.SetIssuer(value); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "issuer updated: %s\n", value)
			return nil
		},
	}
	cmd.AddCommand(setCmd)
	return cmd
}

func newMCPClientCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "client", Short: "Manage OAuth confidential clients"}

	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List OAuth clients",
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			clients, err := adminSvc.ListClients()
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			if len(clients) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "(no oauth clients)")
				return nil
			}
			sort.Slice(clients, func(i, j int) bool { return clients[i].ID < clients[j].ID })
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "ID\tNAME\tDEFAULT_NS\tPRESETS\tSTATUS\tSCOPES\tREDIRECT_URIS")
			for _, c := range clients {
				status := "active"
				if c.Revoked {
					status = "revoked"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", c.ID, c.Name, c.Namespace, strings.Join(enabledClientPresetNames(c), ","), status, strings.Join(c.Scopes, ","), strings.Join(c.RedirectURIs, ","))
			}
			return w.Flush()
		},
	})

	showCmd := &cobra.Command{
		Use:               "show <id>",
		Short:             "Show OAuth client details and endpoints",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMCPOAuthClientIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			showID := strings.TrimSpace(args[0])
			client, err := adminSvc.GetClient(showID)
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			credentials, err := adminSvc.Credentials(mcpadmin.CredentialsRequest{
				ClientID: showID,
				MCPPath:  oauthMCPPathFromConfig(),
			})
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			status := "active"
			if client.Revoked {
				status = "revoked"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "client_id: %s\n", client.ID)
			fmt.Fprintf(cmd.OutOrStdout(), "name: %s\n", client.Name)
			fmt.Fprintf(cmd.OutOrStdout(), "namespace: %s\n", client.Namespace)
			fmt.Fprintf(cmd.OutOrStdout(), "presets: %s\n", strings.Join(enabledClientPresetNames(client), ","))
			fmt.Fprintf(cmd.OutOrStdout(), "status: %s\n", status)
			fmt.Fprintf(cmd.OutOrStdout(), "scopes: %s\n", strings.Join(client.Scopes, ","))
			fmt.Fprintf(cmd.OutOrStdout(), "redirect_uris: %s\n", strings.Join(client.RedirectURIs, ","))
			fmt.Fprintf(cmd.OutOrStdout(), "issuer: %s\n", credentials.Issuer)
			fmt.Fprintf(cmd.OutOrStdout(), "authorize_url: %s\n", credentials.AuthorizationURL)
			fmt.Fprintf(cmd.OutOrStdout(), "token_url: %s\n", credentials.TokenURL)
			fmt.Fprintf(cmd.OutOrStdout(), "registration_url: %s\n", credentials.RegistrationURL)
			fmt.Fprintf(cmd.OutOrStdout(), "resource_url: %s\n", credentials.ResourceURL)
			fmt.Fprintln(cmd.OutOrStdout(), "client_secret: (not retrievable after issuance; use 'rotate-secret' or 'credentials --rotate-secret')")
			return nil
		},
	}
	cmd.AddCommand(showCmd)

	var rotateSecret bool
	var credentialsFormat string
	credentialsCmd := &cobra.Command{
		Use:               "credentials <id>",
		Short:             "Print agent configuration credentials for a client",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMCPOAuthClientIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			credentialsID := strings.TrimSpace(args[0])
			payload, err := adminSvc.Credentials(mcpadmin.CredentialsRequest{
				ClientID:     credentialsID,
				RotateSecret: rotateSecret,
				ResourceURL:  oauthResourceURLOverrideFromConfig(),
				MCPPath:      oauthMCPPathFromConfig(),
			})
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			switch strings.ToLower(strings.TrimSpace(credentialsFormat)) {
			case "json":
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(payload)
			case "env", "":
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_CLIENT_ID=%s\n", payload.ClientID)
				if payload.ClientSecret != "" {
					fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_CLIENT_SECRET=%s\n", payload.ClientSecret)
				} else {
					fmt.Fprintln(cmd.OutOrStdout(), "# client secret not retrievable; rerun with --rotate-secret to issue a new one")
				}
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_ISSUER=%s\n", payload.Issuer)
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_AUTHORIZE_URL=%s\n", payload.AuthorizationURL)
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_TOKEN_URL=%s\n", payload.TokenURL)
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_REGISTRATION_URL=%s\n", payload.RegistrationURL)
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_RESOURCE_URL=%s\n", payload.ResourceURL)
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_SCOPES=%s\n", strings.Join(payload.Scopes, ","))
				return nil
			default:
				return fmt.Errorf("unsupported --format %q (expected env|json)", credentialsFormat)
			}
		},
	}
	credentialsCmd.Flags().BoolVar(&rotateSecret, "rotate-secret", false, "rotate and print a newly issued client secret")
	credentialsCmd.Flags().StringVar(&credentialsFormat, "format", "env", "output format (env|json)")
	cmd.AddCommand(credentialsCmd)

	var addName string
	var addNamespace string
	var addPresetFlags []string
	var addScopes []string
	var addRedirectURIs []string
	addCmd := &cobra.Command{
		Use:   "add",
		Short: "Create a new OAuth client",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := mcpConfigFromViper()
			if err != nil {
				return err
			}
			adminSvc := newMCPAdminService(cfg)
			if err := ensureMCPStateInitialized(cfg, adminSvc, strings.TrimSpace(viper.GetString(mcpIssuerKey))); err != nil {
				return err
			}
			name := strings.TrimSpace(addName)
			if name == "" {
				name, err = nextAvailableMCPClientName(adminSvc, "client")
				if err != nil {
					return err
				}
			}
			lockdPreset, presets, err := resolveMCPPresetFlags(addPresetFlags)
			if err != nil {
				return err
			}
			resp, err := adminSvc.AddClient(mcpadmin.AddClientRequest{
				Name:         name,
				Namespace:    strings.TrimSpace(addNamespace),
				LockdPreset:  lockdPreset,
				Presets:      presets,
				Scopes:       addScopes,
				RedirectURIs: addRedirectURIs,
			})
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "client_id: %s\n", resp.ClientID)
			fmt.Fprintf(cmd.OutOrStdout(), "client_secret: %s\n", resp.ClientSecret)
			return nil
		},
	}
	addCmd.Flags().StringVar(&addName, "name", "", "client display name (default auto-generated)")
	addCmd.Flags().StringVarP(&addNamespace, "namespace", "n", "", "default namespace override for this client")
	addCmd.Flags().StringSliceVarP(&addPresetFlags, "preset", "p", nil, "enable preset(s): lockd or preset YAML file path(s)")
	addCmd.Flags().StringSliceVar(&addScopes, "scope", nil, "allowed scope(s)")
	addCmd.Flags().StringSliceVar(&addRedirectURIs, "redirect-uri", nil, "allowed redirect URI(s) for authorization_code flow")
	cmd.AddCommand(addCmd)

	rmCmd := &cobra.Command{
		Use:               "remove <id>",
		Short:             "Remove an OAuth client",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMCPOAuthClientIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			client, err := adminSvc.GetClient(strings.TrimSpace(args[0]))
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			if err := adminSvc.RemoveClient(client.ID); err != nil {
				return explainMCPAdminStateError(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "removed client %s\n", client.ID)
			return nil
		},
	}
	cmd.AddCommand(rmCmd)

	revokeCmd := &cobra.Command{
		Use:               "revoke <id>",
		Short:             "Revoke an OAuth client",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMCPOAuthClientIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			client, err := adminSvc.GetClient(strings.TrimSpace(args[0]))
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			if err := adminSvc.SetClientRevoked(client.ID, true); err != nil {
				return explainMCPAdminStateError(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "revoked client %s\n", client.ID)
			return nil
		},
	}
	cmd.AddCommand(revokeCmd)

	restoreCmd := &cobra.Command{
		Use:               "restore <id>",
		Short:             "Restore a revoked OAuth client",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMCPOAuthClientIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			client, err := adminSvc.GetClient(strings.TrimSpace(args[0]))
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			if err := adminSvc.SetClientRevoked(client.ID, false); err != nil {
				return explainMCPAdminStateError(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "restored client %s\n", client.ID)
			return nil
		},
	}
	cmd.AddCommand(restoreCmd)

	rotateCmd := &cobra.Command{
		Use:               "rotate-secret <id>",
		Short:             "Rotate OAuth client secret",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMCPOAuthClientIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			client, err := adminSvc.GetClient(strings.TrimSpace(args[0]))
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			secret, err := adminSvc.RotateClientSecret(client.ID)
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "client_id: %s\n", client.ID)
			fmt.Fprintf(cmd.OutOrStdout(), "client_secret: %s\n", secret)
			return nil
		},
	}
	cmd.AddCommand(rotateCmd)

	var toolsListOutPath string
	toolsListCmd := &cobra.Command{
		Use:               "tools-list <id>",
		Short:             "Dump the effective MCP tool snapshot for one OAuth client",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMCPOAuthClientIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			clientID := strings.TrimSpace(args[0])
			client, err := adminSvc.GetClient(clientID)
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			cfg, err := mcpConfigFromViper()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			payload, err := lockdmcp.BuildFullMCPSpecJSONLForClientSurface(ctx, cfg, client.LockdPreset, client.Presets)
			if err != nil {
				return fmt.Errorf("build client tool snapshot: %w", err)
			}
			if strings.TrimSpace(toolsListOutPath) != "" {
				return writePrettyXOutput(ctx, payload, toolsListOutPath)
			}
			return writePrettyXOutputTo(ctx, payload, cmd.OutOrStdout())
		},
	}
	toolsListCmd.Flags().StringVarP(&toolsListOutPath, "out", "o", "", "write pretty JSONL output to a file instead of stdout")
	cmd.AddCommand(toolsListCmd)

	var updateName string
	var updateNamespace string
	var updatePresetFlags []string
	var updateScopes []string
	var updateRedirectURIs []string
	updateCmd := &cobra.Command{
		Use:               "update <id>",
		Short:             "Update OAuth client attributes",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMCPOAuthClientIDArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			namespaceSet := cmd.Flags().Changed("namespace")
			presetSet := cmd.Flags().Changed("preset")
			if strings.TrimSpace(updateName) == "" && updateScopes == nil && updateRedirectURIs == nil && !namespaceSet && !presetSet {
				return fmt.Errorf("at least one of --name, --namespace, --preset, --scope, or --redirect-uri must be set")
			}
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			client, err := adminSvc.GetClient(strings.TrimSpace(args[0]))
			if err != nil {
				return explainMCPAdminStateError(err)
			}
			var namespacePtr *string
			if namespaceSet {
				trimmedNamespace := strings.TrimSpace(updateNamespace)
				namespacePtr = &trimmedNamespace
			}
			var lockdPresetPtr *bool
			var presets []preset.Definition
			if presetSet {
				lockdPreset, resolvedPresets, err := resolveMCPPresetFlags(updatePresetFlags)
				if err != nil {
					return err
				}
				lockdPresetPtr = &lockdPreset
				presets = resolvedPresets
			}
			if err := adminSvc.UpdateClient(mcpadmin.UpdateClientRequest{
				ClientID:     client.ID,
				Name:         updateName,
				Namespace:    namespacePtr,
				LockdPreset:  lockdPresetPtr,
				Presets:      presets,
				Scopes:       updateScopes,
				RedirectURIs: updateRedirectURIs,
			}); err != nil {
				return explainMCPAdminStateError(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "updated client %s\n", client.ID)
			return nil
		},
	}
	updateCmd.Flags().StringVar(&updateName, "name", "", "updated client name")
	updateCmd.Flags().StringVarP(&updateNamespace, "namespace", "n", "", "replacement default namespace override for this client (empty clears override)")
	updateCmd.Flags().StringSliceVarP(&updatePresetFlags, "preset", "p", nil, "replacement preset set: lockd and/or preset YAML file path(s)")
	updateCmd.Flags().StringSliceVar(&updateScopes, "scope", nil, "replacement scope list")
	updateCmd.Flags().StringSliceVar(&updateRedirectURIs, "redirect-uri", nil, "replacement redirect URI allow-list")
	cmd.AddCommand(updateCmd)

	return cmd
}

func newMCPPresetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preset",
		Short: "List or export built-in MCP preset templates",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List built-in MCP preset templates",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, name := range preset.BuiltInNames() {
				fmt.Fprintln(cmd.OutOrStdout(), name)
			}
			return nil
		},
	})
	var outPath string
	getCmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Print one built-in MCP preset template",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := strings.ToLower(strings.TrimSpace(args[0]))
			payload, err := preset.BuiltInYAML(target)
			if err != nil {
				return err
			}
			if strings.TrimSpace(outPath) != "" {
				if err := os.WriteFile(outPath, payload, 0o644); err != nil {
					return fmt.Errorf("write preset output %s: %w", outPath, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "wrote preset yaml: %s\n", outPath)
				return nil
			}
			_, err = cmd.OutOrStdout().Write(payload)
			return err
		},
	}
	getCmd.Flags().StringVarP(&outPath, "out", "o", "", "write preset YAML to a file instead of stdout")
	cmd.AddCommand(getCmd)
	return cmd
}

func completeMCPOAuthClientID(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	adminSvc, err := openMCPAdmin()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	clients, err := adminSvc.ListClients()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i].ID < clients[j].ID })
	out := make([]string, 0, len(clients))
	for _, client := range clients {
		if toComplete != "" && !strings.HasPrefix(client.ID, toComplete) {
			continue
		}
		status := "active"
		if client.Revoked {
			status = "revoked"
		}
		label := strings.TrimSpace(client.Name)
		if label == "" {
			label = status
		} else {
			label = fmt.Sprintf("%s (%s)", label, status)
		}
		out = append(out, fmt.Sprintf("%s\t%s", client.ID, label))
	}
	return out, cobra.ShellCompDirectiveNoFileComp
}

func completeMCPOAuthClientIDArg(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return completeMCPOAuthClientID(cmd, args, toComplete)
}

func newMCPAdminService(cfg lockdmcp.Config) *mcpadmin.Service {
	return mcpadmin.New(mcpadmin.Config{
		StatePath:  cfg.OAuthStatePath,
		TokenStore: cfg.OAuthTokenStorePath,
	})
}

func ensureMCPStateInitialized(cfg lockdmcp.Config, adminSvc *mcpadmin.Service, issuerOverride string) error {
	if cfg.DisableTLS {
		return nil
	}
	_, err := adminSvc.ListClients()
	if err == nil {
		return nil
	}
	if !errors.Is(err, mcpadmin.ErrNotBootstrapped) {
		return err
	}
	issuer := strings.TrimSpace(issuerOverride)
	if issuer == "" {
		var derr error
		issuer, derr = defaultMCPIssuer(cfg)
		if derr != nil {
			return derr
		}
	}
	if _, err := adminSvc.Bootstrap(mcpadmin.BootstrapRequest{
		Path:              cfg.OAuthStatePath,
		Issuer:            issuer,
		InitialClientName: mcpAutoBootstrapClientName,
	}); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			if _, checkErr := adminSvc.ListClients(); checkErr == nil {
				return nil
			}
		}
		return err
	}
	return nil
}

func resolveMCPPresetFlags(values []string) (bool, []preset.Definition, error) {
	if len(values) == 0 {
		return true, nil, nil
	}
	lockdPreset := false
	defs := make([]preset.Definition, 0)
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return false, nil, fmt.Errorf("preset value must not be empty")
		}
		if strings.EqualFold(trimmed, preset.ReservedLockdName) {
			lockdPreset = true
			continue
		}
		loaded, err := preset.LoadFile(trimmed)
		if err != nil {
			return false, nil, err
		}
		defs = append(defs, loaded...)
	}
	normalized, err := preset.NormalizeCollection(defs)
	switch {
	case len(defs) == 0:
		normalized = nil
	case err != nil:
		return false, nil, err
	}
	if !lockdPreset && len(normalized) == 0 {
		return false, nil, fmt.Errorf("at least one preset is required")
	}
	return lockdPreset, normalized, nil
}

func enabledClientPresetNames(client mcpadmin.Client) []string {
	names := make([]string, 0, len(client.Presets)+1)
	if client.LockdPreset {
		names = append(names, preset.ReservedLockdName)
	}
	for _, def := range client.Presets {
		names = append(names, def.Name)
	}
	sort.Strings(names)
	return names
}

func nextAvailableMCPClientName(adminSvc *mcpadmin.Service, base string) (string, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		base = "client"
	}
	clients, err := adminSvc.ListClients()
	if err != nil {
		return "", explainMCPAdminStateError(err)
	}
	used := make(map[string]struct{}, len(clients))
	for _, client := range clients {
		name := strings.ToLower(strings.TrimSpace(client.Name))
		if name == "" {
			continue
		}
		used[name] = struct{}{}
	}
	candidate := base
	if _, ok := used[strings.ToLower(candidate)]; !ok {
		return candidate, nil
	}
	for i := 2; ; i++ {
		candidate = fmt.Sprintf("%s-%d", base, i)
		if _, ok := used[strings.ToLower(candidate)]; !ok {
			return candidate, nil
		}
	}
}

func explainMCPAdminStateError(err error) error {
	if errors.Is(err, mcpadmin.ErrNotBootstrapped) {
		return fmt.Errorf("mcp oauth state missing: initialize by running 'lockd mcp', 'lockd mcp issuer set <url>', or 'lockd mcp client add'")
	}
	return err
}

func openMCPAdmin() (*mcpadmin.Service, error) {
	cfg, err := mcpConfigFromViper()
	if err != nil {
		return nil, err
	}
	return newMCPAdminService(cfg), nil
}

func writePrettyXOutput(ctx context.Context, payload []byte, outPath string) error {
	outPath = strings.TrimSpace(outPath)
	if outPath == "" {
		return fmt.Errorf("output path is required")
	}
	outPath, err := expandPath(outPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output file %s: %w", outPath, err)
	}
	defer f.Close()
	if err := writePrettyXOutputTo(ctx, payload, f); err != nil {
		return err
	}
	return nil
}

func writePrettyXOutputTo(ctx context.Context, payload []byte, out io.Writer) error {
	if err := mcpPrettyXRunner(ctx, payload, out); err != nil {
		return fmt.Errorf("format jsonl with prettyx: %w", err)
	}
	return nil
}

func runPrettyX(ctx context.Context, payload []byte, out io.Writer) error {
	cmd := exec.CommandContext(ctx, "prettyx")
	cmd.Stdin = bytes.NewReader(payload)
	cmd.Stdout = out
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if strings.TrimSpace(stderr.String()) != "" {
			return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return err
	}
	return nil
}

func oauthResourceURLOverrideFromConfig() string {
	cfg, err := mcpConfigFromViper()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(cfg.OAuthProtectedResourceURL)
}

func oauthMCPPathFromConfig() string {
	cfg, err := mcpConfigFromViper()
	if err != nil {
		return "/"
	}
	path := strings.TrimSpace(cfg.MCPPath)
	if path == "" {
		return "/"
	}
	return path
}

func newMCPExportCACommand() *cobra.Command {
	var outPath string
	var bundlePath string
	cmd := &cobra.Command{
		Use:   "ca-export",
		Short: "Export the local MCP CA certificate",
		RunE: func(cmd *cobra.Command, args []string) error {
			if bundlePath == "" {
				bundlePath = strings.TrimSpace(viper.GetString(mcpBundleKey))
			}
			if bundlePath == "" {
				if def, err := lockd.DefaultBundlePath(); err == nil {
					bundlePath = def
				}
			}
			var err error
			bundlePath, err = expandPath(bundlePath)
			if err != nil {
				return err
			}
			bundle, err := tlsutil.LoadBundle(bundlePath, "")
			if err != nil {
				return fmt.Errorf("load bundle: %w", err)
			}
			if len(bundle.CACertPEM) == 0 {
				return fmt.Errorf("bundle %s does not contain a CA certificate", bundlePath)
			}
			if outPath == "" {
				_, err = cmd.OutOrStdout().Write(bundle.CACertPEM)
				return err
			}
			outPath, err = expandPath(outPath)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
				return fmt.Errorf("create output dir: %w", err)
			}
			if err := os.WriteFile(outPath, bundle.CACertPEM, 0o644); err != nil {
				return fmt.Errorf("write ca: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "wrote CA cert to %s\n", outPath)
			return nil
		},
	}
	cmd.Flags().StringVar(&bundlePath, "bundle", "", "server bundle PEM to read (default $HOME/.lockd/server.pem)")
	cmd.Flags().StringVar(&outPath, "out", "", "output path for CA certificate (default stdout)")
	return cmd
}

func defaultMCPIssuer(cfg lockdmcp.Config) (string, error) {
	baseIssuer, err := issuerFromBaseURL(cfg.BaseURL)
	if err != nil {
		return "", err
	}
	if baseIssuer != "" {
		return baseIssuer, nil
	}

	scheme := "https"
	if cfg.DisableTLS {
		scheme = "http"
	}
	host, port, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return "", fmt.Errorf("parse --listen %q: %w", cfg.Listen, err)
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	if !cfg.DisableTLS {
		bundlePath := strings.TrimSpace(cfg.BundlePath)
		if bundlePath == "" {
			if def, err := lockd.DefaultBundlePath(); err == nil {
				bundlePath = def
			}
		}
		if bundlePath != "" {
			if expanded, err := expandPath(bundlePath); err == nil {
				if bundle, err := tlsutil.LoadBundle(expanded, ""); err == nil && bundle.ServerCert != nil {
					if certHost := preferredCertHost(bundle.ServerCert.DNSNames, bundle.ServerCert.IPAddresses); certHost != "" {
						host = certHost
					}
				}
			}
		}
	}
	hostPort := net.JoinHostPort(host, port)
	u := url.URL{Scheme: scheme, Host: hostPort}
	return u.String(), nil
}

func issuerFromBaseURL(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", nil
	}
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	u, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parse --base-url %q: %w", raw, err)
	}
	if strings.TrimSpace(u.Scheme) == "" {
		return "", fmt.Errorf("parse --base-url %q: missing scheme", raw)
	}
	if strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("parse --base-url %q: missing host", raw)
	}
	u.Fragment = ""
	u.RawFragment = ""
	u.RawQuery = ""
	return strings.TrimRight(u.String(), "/"), nil
}

func preferredCertHost(dnsNames []string, ips []net.IP) string {
	for _, dns := range dnsNames {
		dns = strings.TrimSpace(dns)
		if dns == "" || dns == "*" {
			continue
		}
		return dns
	}
	for _, ip := range ips {
		if ip == nil {
			continue
		}
		return ip.String()
	}
	return ""
}
