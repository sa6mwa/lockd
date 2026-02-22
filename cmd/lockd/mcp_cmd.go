package main

import (
	"context"
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
	"pkt.systems/lockd/mcp/oauth"
	mcpstate "pkt.systems/lockd/mcp/state"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

const (
	mcpListenKey           = "mcp.listen"
	mcpServerKey           = "mcp.server"
	mcpClientBundleKey     = "mcp.client_bundle"
	mcpBundleKey           = "mcp.bundle"
	mcpDisableTLSKey       = "mcp.disable_tls"
	mcpDisableMTLSKey      = "mcp.disable_mtls"
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
	flags.String("state-file", "", "MCP OAuth state file path (default $HOME/.lockd/mcp.pem)")
	flags.String("refresh-store", "", "refresh-token store path (default $HOME/.lockd/mcp-auth-store.json)")
	flags.String("issuer", "", "OAuth issuer URL (used by bootstrap and oauth issuer set/get)")
	flags.String("mcp-path", "/mcp", "HTTP path for the MCP streamable endpoint")
	flags.String("oauth-resource-url", "", "OAuth protected resource identifier URL (default <issuer>/mcp)")
	flags.Bool("disable-mcp-upstream-mtls", false, "disable mTLS when MCP calls upstream lockd")

	mustBindMCPFlag(mcpListenKey, "LOCKD_MCP_LISTEN", flags.Lookup("listen"))
	mustBindMCPFlag(mcpClientBundleKey, "LOCKD_MCP_CLIENT_BUNDLE", flags.Lookup("client-bundle"))
	mustBindMCPFlag(mcpDisableTLSKey, "LOCKD_MCP_DISABLE_TLS", flags.Lookup("disable-tls"))
	mustBindMCPFlag(mcpStateFileKey, "LOCKD_MCP_STATE_FILE", flags.Lookup("state-file"))
	mustBindMCPFlag(mcpRefreshStoreKey, "LOCKD_MCP_REFRESH_STORE", flags.Lookup("refresh-store"))
	mustBindMCPFlag(mcpIssuerKey, "LOCKD_MCP_ISSUER", flags.Lookup("issuer"))
	mustBindMCPFlag(mcpPathKey, "LOCKD_MCP_PATH", flags.Lookup("mcp-path"))
	mustBindMCPFlag(mcpOAuthResourceURLKey, "LOCKD_MCP_OAUTH_RESOURCE_URL", flags.Lookup("oauth-resource-url"))
	mustBindMCPFlag(mcpDisableMTLSKey, "LOCKD_MCP_DISABLE_MTLS", flags.Lookup("disable-mcp-upstream-mtls"))

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
		BundlePath:                strings.TrimSpace(viper.GetString(mcpBundleKey)),
		UpstreamServer:            strings.TrimSpace(viper.GetString(mcpServerKey)),
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
			issuer := strings.TrimSpace(viper.GetString(mcpIssuerKey))
			if issuer == "" {
				issuer, err = defaultMCPIssuer(cfg)
				if err != nil {
					return err
				}
			}
			resp, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
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
			mgr, err := openOAuthManager()
			if err != nil {
				return err
			}
			issuer, err := mgr.Issuer()
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
			mgr, err := openOAuthManager()
			if err != nil {
				return err
			}
			if err := mgr.SetIssuer(value); err != nil {
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
			mgr, err := openOAuthManager()
			if err != nil {
				return err
			}
			clients, err := mgr.ListClients()
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

	var addName string
	var addScopes []string
	addCmd := &cobra.Command{
		Use:   "add",
		Short: "Create a new OAuth client",
		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, err := openOAuthManager()
			if err != nil {
				return err
			}
			resp, err := mgr.AddClient(oauth.AddClientRequest{Name: addName, Scopes: addScopes})
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
			mgr, err := openOAuthManager()
			if err != nil {
				return err
			}
			if err := mgr.RemoveClient(rmID); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "removed client %s\n", rmID)
			return nil
		},
	}
	rmCmd.Flags().StringVar(&rmID, "id", "", "client ID")
	_ = rmCmd.MarkFlagRequired("id")
	cmd.AddCommand(rmCmd)

	var revokeID string
	revokeCmd := &cobra.Command{
		Use:   "revoke",
		Short: "Revoke an OAuth client",
		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, err := openOAuthManager()
			if err != nil {
				return err
			}
			if err := mgr.RevokeClient(revokeID, true); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "revoked client %s\n", revokeID)
			return nil
		},
	}
	revokeCmd.Flags().StringVar(&revokeID, "id", "", "client ID")
	_ = revokeCmd.MarkFlagRequired("id")
	cmd.AddCommand(revokeCmd)

	var restoreID string
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore a revoked OAuth client",
		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, err := openOAuthManager()
			if err != nil {
				return err
			}
			if err := mgr.RevokeClient(restoreID, false); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "restored client %s\n", restoreID)
			return nil
		},
	}
	restoreCmd.Flags().StringVar(&restoreID, "id", "", "client ID")
	_ = restoreCmd.MarkFlagRequired("id")
	cmd.AddCommand(restoreCmd)

	var rotateID string
	rotateCmd := &cobra.Command{
		Use:   "rotate-secret",
		Short: "Rotate OAuth client secret",
		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, err := openOAuthManager()
			if err != nil {
				return err
			}
			secret, err := mgr.RotateClientSecret(rotateID)
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
			mgr, err := openOAuthManager()
			if err != nil {
				return err
			}
			if err := mgr.UpdateClient(oauth.UpdateClientRequest{ClientID: updateID, Name: updateName, Scopes: updateScopes}); err != nil {
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
	cmd.AddCommand(updateCmd)

	return cmd
}

func openOAuthManager() (*oauth.Manager, error) {
	cfg, err := mcpConfigFromViper()
	if err != nil {
		return nil, err
	}
	mgr, err := oauth.NewManager(oauth.ManagerConfig{
		StatePath:    cfg.OAuthStatePath,
		RefreshStore: cfg.OAuthRefreshStorePath,
	})
	if err != nil {
		if errors.Is(err, mcpstate.ErrNotBootstrapped) {
			return nil, fmt.Errorf("mcp oauth state missing: run 'lockd mcp bootstrap'")
		}
		return nil, err
	}
	return mgr, nil
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
