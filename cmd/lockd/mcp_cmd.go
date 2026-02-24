package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"pkt.systems/lockd"
	lockdmcp "pkt.systems/lockd/mcp"
	mcpadmin "pkt.systems/lockd/mcp/admin"
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
	mcpRefreshStoreKey     = "mcp.refresh_store"
	mcpIssuerKey           = "mcp.issuer"
	mcpPathKey             = "mcp.path"
	mcpOAuthResourceURLKey = "mcp.oauth_resource_url"
)

func newMCPCommand(baseLogger pslog.Logger, inheritedServerFlag, inheritedBundleFlag, inheritedDisableMTLSFlag *pflag.Flag) *cobra.Command {
	serveCmd := &cobra.Command{
		Use:   "mcp",
		Short: "Run and administer the lockd MCP facade server",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			_, err := loadConfigFile()
			return err
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := mcpConfigFromViper()
			if err != nil {
				return err
			}
			svc, err := lockdmcp.NewServer(lockdmcp.NewServerRequest{
				Config: cfg,
				Logger: baseLogger,
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

	flags := serveCmd.Flags()
	flags.StringP("listen", "l", "127.0.0.1:19341", "listen address for the MCP server")
	flags.StringP("client-bundle", "B", "", "lockd client bundle PEM used by MCP when calling upstream lockd")
	flags.Bool("disable-tls", false, "disable TLS and OAuth enforcement for MCP HTTP endpoint")
	flags.String("base-url", "", "externally reachable MCP base URL used for transfer capability URLs (required, e.g. https://host/mcp)")
	flags.Bool("allow-http", false, "allow http:// base URLs for transfer capability URLs (unsafe; default requires https)")
	flags.String("state-file", "", "MCP OAuth state file path (default $HOME/.lockd/mcp.pem)")
	flags.String("refresh-store", "", "refresh-token store path (default $HOME/.lockd/mcp-auth-store.json)")
	flags.String("issuer", "", "OAuth issuer URL (used by bootstrap and oauth issuer set/get)")
	flags.String("mcp-path", "/", "HTTP path (docroot) for the MCP streamable endpoint")
	flags.String("oauth-resource-url", "", "OAuth protected resource identifier URL (default <issuer>/mcp)")
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
	mustBindMCPFlag(mcpRefreshStoreKey, "LOCKD_MCP_REFRESH_STORE", flags.Lookup("refresh-store"))
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
		mustBindMCPFlag(mcpDisableMTLSKey, "LOCKD_MCP_DISABLE_MTLS", inheritedDisableMTLSFlag)
	}

	serveCmd.AddCommand(newMCPBootstrapCommand())
	serveCmd.AddCommand(newMCPOAuthCommand())
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
		UpstreamClientBundlePath:  strings.TrimSpace(viper.GetString(mcpClientBundleKey)),
		OAuthStatePath:            strings.TrimSpace(viper.GetString(mcpStateFileKey)),
		OAuthRefreshStorePath:     strings.TrimSpace(viper.GetString(mcpRefreshStoreKey)),
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
	if cfg.OAuthRefreshStorePath != "" {
		var err error
		cfg.OAuthRefreshStorePath, err = expandPath(cfg.OAuthRefreshStorePath)
		if err != nil {
			return lockdmcp.Config{}, fmt.Errorf("expand MCP refresh store path: %w", err)
		}
	}
	return cfg, nil
}

func newMCPBootstrapCommand() *cobra.Command {
	var force bool
	var clientName string
	var scopes []string
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap MCP OAuth material and initial confidential client",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := mcpConfigFromViper()
			if err != nil {
				return err
			}
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			issuer := strings.TrimSpace(viper.GetString(mcpIssuerKey))
			if issuer == "" {
				issuer, err = defaultMCPIssuer(cfg)
				if err != nil {
					return err
				}
			}
			resp, err := adminSvc.Bootstrap(mcpadmin.BootstrapRequest{
				Path:              cfg.OAuthStatePath,
				Issuer:            issuer,
				InitialClientName: clientName,
				InitialScopes:     scopes,
				Force:             force,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "wrote MCP oauth state: %s\n", resp.Path)
			fmt.Fprintf(cmd.OutOrStdout(), "issuer: %s\n", resp.Issuer)
			fmt.Fprintf(cmd.OutOrStdout(), "client_id: %s\n", resp.ClientID)
			fmt.Fprintf(cmd.OutOrStdout(), "client_secret: %s\n", resp.ClientSecret)
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing MCP state file")
	cmd.Flags().StringVar(&clientName, "client-name", "default", "name for initial confidential client")
	cmd.Flags().StringSliceVar(&scopes, "scope", nil, "optional initial OAuth scope(s)")
	return cmd
}

func newMCPOAuthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "oauth",
		Short: "Manage MCP OAuth clients and issuer",
	}
	cmd.AddCommand(newMCPOAuthIssuerCommand())
	cmd.AddCommand(newMCPOAuthClientCommand())
	return cmd
}

func newMCPOAuthIssuerCommand() *cobra.Command {
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
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), issuer)
			return nil
		},
	})
	var value string
	setCmd := &cobra.Command{
		Use:   "set",
		Short: "Set OAuth issuer",
		RunE: func(cmd *cobra.Command, args []string) error {
			value = strings.TrimSpace(value)
			if value == "" {
				return fmt.Errorf("--issuer is required")
			}
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			if err := adminSvc.SetIssuer(value); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "issuer updated: %s\n", value)
			return nil
		},
	}
	setCmd.Flags().StringVar(&value, "issuer", "", "issuer URL")
	cmd.AddCommand(setCmd)
	return cmd
}

func newMCPOAuthClientCommand() *cobra.Command {
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
				return err
			}
			if len(clients) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "(no oauth clients)")
				return nil
			}
			sort.Slice(clients, func(i, j int) bool { return clients[i].ID < clients[j].ID })
			for _, c := range clients {
				revoked := "active"
				if c.Revoked {
					revoked = "revoked"
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s  %s  %s  scopes=%s\n", c.ID, c.Name, revoked, strings.Join(c.Scopes, ","))
			}
			return nil
		},
	})

	var showID string
	showCmd := &cobra.Command{
		Use:   "show",
		Short: "Show OAuth client details and endpoints",
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			client, err := adminSvc.GetClient(showID)
			if err != nil {
				return err
			}
			credentials, err := adminSvc.Credentials(mcpadmin.CredentialsRequest{
				ClientID: showID,
				MCPPath:  oauthMCPPathFromConfig(),
			})
			if err != nil {
				return err
			}
			status := "active"
			if client.Revoked {
				status = "revoked"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "client_id: %s\n", client.ID)
			fmt.Fprintf(cmd.OutOrStdout(), "name: %s\n", client.Name)
			fmt.Fprintf(cmd.OutOrStdout(), "status: %s\n", status)
			fmt.Fprintf(cmd.OutOrStdout(), "scopes: %s\n", strings.Join(client.Scopes, ","))
			fmt.Fprintf(cmd.OutOrStdout(), "issuer: %s\n", credentials.Issuer)
			fmt.Fprintf(cmd.OutOrStdout(), "authorize_url: %s\n", credentials.AuthorizeURL)
			fmt.Fprintf(cmd.OutOrStdout(), "token_url: %s\n", credentials.TokenURL)
			fmt.Fprintf(cmd.OutOrStdout(), "resource_url: %s\n", credentials.ResourceURL)
			fmt.Fprintln(cmd.OutOrStdout(), "client_secret: (not retrievable after issuance; use 'rotate-secret' or 'credentials --rotate-secret')")
			return nil
		},
	}
	showCmd.Flags().StringVar(&showID, "id", "", "client ID")
	_ = showCmd.MarkFlagRequired("id")
	mustRegisterFlagCompletion(showCmd, "id", completeMCPOAuthClientID)
	cmd.AddCommand(showCmd)

	var credentialsID string
	var rotateSecret bool
	var credentialsFormat string
	credentialsCmd := &cobra.Command{
		Use:   "credentials",
		Short: "Print agent configuration credentials for a client",
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			payload, err := adminSvc.Credentials(mcpadmin.CredentialsRequest{
				ClientID:     credentialsID,
				RotateSecret: rotateSecret,
				ResourceURL:  oauthResourceURLOverrideFromConfig(),
				MCPPath:      oauthMCPPathFromConfig(),
			})
			if err != nil {
				return err
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
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_AUTHORIZE_URL=%s\n", payload.AuthorizeURL)
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_TOKEN_URL=%s\n", payload.TokenURL)
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_RESOURCE_URL=%s\n", payload.ResourceURL)
				fmt.Fprintf(cmd.OutOrStdout(), "LOCKD_MCP_OAUTH_SCOPES=%s\n", strings.Join(payload.Scopes, ","))
				return nil
			default:
				return fmt.Errorf("unsupported --format %q (expected env|json)", credentialsFormat)
			}
		},
	}
	credentialsCmd.Flags().StringVar(&credentialsID, "id", "", "client ID")
	credentialsCmd.Flags().BoolVar(&rotateSecret, "rotate-secret", false, "rotate and print a newly issued client secret")
	credentialsCmd.Flags().StringVar(&credentialsFormat, "format", "env", "output format (env|json)")
	_ = credentialsCmd.MarkFlagRequired("id")
	mustRegisterFlagCompletion(credentialsCmd, "id", completeMCPOAuthClientID)
	cmd.AddCommand(credentialsCmd)

	var addName string
	var addScopes []string
	addCmd := &cobra.Command{
		Use:   "add",
		Short: "Create a new OAuth client",
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			resp, err := adminSvc.AddClient(mcpadmin.AddClientRequest{Name: addName, Scopes: addScopes})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "client_id: %s\n", resp.ClientID)
			fmt.Fprintf(cmd.OutOrStdout(), "client_secret: %s\n", resp.ClientSecret)
			return nil
		},
	}
	addCmd.Flags().StringVar(&addName, "name", "", "client display name")
	addCmd.Flags().StringSliceVar(&addScopes, "scope", nil, "allowed scope(s)")
	_ = addCmd.MarkFlagRequired("name")
	cmd.AddCommand(addCmd)

	var rmID string
	rmCmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove an OAuth client",
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			if err := adminSvc.RemoveClient(rmID); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "removed client %s\n", rmID)
			return nil
		},
	}
	rmCmd.Flags().StringVar(&rmID, "id", "", "client ID")
	_ = rmCmd.MarkFlagRequired("id")
	mustRegisterFlagCompletion(rmCmd, "id", completeMCPOAuthClientID)
	cmd.AddCommand(rmCmd)

	var revokeID string
	revokeCmd := &cobra.Command{
		Use:   "revoke",
		Short: "Revoke an OAuth client",
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			if err := adminSvc.SetClientRevoked(revokeID, true); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "revoked client %s\n", revokeID)
			return nil
		},
	}
	revokeCmd.Flags().StringVar(&revokeID, "id", "", "client ID")
	_ = revokeCmd.MarkFlagRequired("id")
	mustRegisterFlagCompletion(revokeCmd, "id", completeMCPOAuthClientID)
	cmd.AddCommand(revokeCmd)

	var restoreID string
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a revoked OAuth client",
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			if err := adminSvc.SetClientRevoked(restoreID, false); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "restored client %s\n", restoreID)
			return nil
		},
	}
	restoreCmd.Flags().StringVar(&restoreID, "id", "", "client ID")
	_ = restoreCmd.MarkFlagRequired("id")
	mustRegisterFlagCompletion(restoreCmd, "id", completeMCPOAuthClientID)
	cmd.AddCommand(restoreCmd)

	var rotateID string
	rotateCmd := &cobra.Command{
		Use:   "rotate-secret",
		Short: "Rotate OAuth client secret",
		RunE: func(cmd *cobra.Command, args []string) error {
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			secret, err := adminSvc.RotateClientSecret(rotateID)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "client_id: %s\n", rotateID)
			fmt.Fprintf(cmd.OutOrStdout(), "client_secret: %s\n", secret)
			return nil
		},
	}
	rotateCmd.Flags().StringVar(&rotateID, "id", "", "client ID")
	_ = rotateCmd.MarkFlagRequired("id")
	mustRegisterFlagCompletion(rotateCmd, "id", completeMCPOAuthClientID)
	cmd.AddCommand(rotateCmd)

	var updateID string
	var updateName string
	var updateScopes []string
	updateCmd := &cobra.Command{
		Use:   "update",
		Short: "Update OAuth client attributes",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(updateName) == "" && updateScopes == nil {
				return fmt.Errorf("at least one of --name or --scope must be set")
			}
			adminSvc, err := openMCPAdmin()
			if err != nil {
				return err
			}
			if err := adminSvc.UpdateClient(mcpadmin.UpdateClientRequest{ClientID: updateID, Name: updateName, Scopes: updateScopes}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "updated client %s\n", updateID)
			return nil
		},
	}
	updateCmd.Flags().StringVar(&updateID, "id", "", "client ID")
	updateCmd.Flags().StringVar(&updateName, "name", "", "updated client name")
	updateCmd.Flags().StringSliceVar(&updateScopes, "scope", nil, "replacement scope list")
	_ = updateCmd.MarkFlagRequired("id")
	mustRegisterFlagCompletion(updateCmd, "id", completeMCPOAuthClientID)
	cmd.AddCommand(updateCmd)

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

func mustRegisterFlagCompletion(cmd *cobra.Command, flag string, fn cobra.CompletionFunc) {
	if err := cmd.RegisterFlagCompletionFunc(flag, fn); err != nil {
		panic(err)
	}
}

func openMCPAdmin() (*mcpadmin.Service, error) {
	cfg, err := mcpConfigFromViper()
	if err != nil {
		return nil, err
	}
	service := mcpadmin.New(mcpadmin.Config{
		StatePath:    cfg.OAuthStatePath,
		RefreshStore: cfg.OAuthRefreshStorePath,
	})
	if _, err := service.ListClients(); err != nil {
		if errors.Is(err, mcpadmin.ErrNotBootstrapped) {
			return nil, fmt.Errorf("mcp oauth state missing: run 'lockd mcp bootstrap'")
		}
		return nil, err
	}
	return service, nil
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
