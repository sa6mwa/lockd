package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/tlsutil"
)

func newAuthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "auth",
		Short:        "Manage lockd certificates",
		SilenceUsage: true,
	}
	cmd.AddCommand(newAuthNewServerCommand())
	cmd.AddCommand(newAuthNewClientCommand())
	cmd.AddCommand(newAuthRevokeClientCommand())
	cmd.AddCommand(newAuthInspectCommand())
	return cmd
}

func newAuthNewServerCommand() *cobra.Command {
	var out string
	var cn string
	var hosts string
	var caCN string
	var validity time.Duration
	var force bool

	cmd := &cobra.Command{
		Use:   "new server",
		Short: "Create a new server bundle (CA + server certificate)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if out == "" {
				path, err := lockd.DefaultBundlePath()
				if err != nil {
					return err
				}
				out = path
			}
			ca, err := tlsutil.GenerateCA(caCN, validity)
			if err != nil {
				return err
			}

			hostList := strings.Split(hosts, ",")
			serverCert, serverKey, err := ca.IssueServer(hostList, cn, validity)
			if err != nil {
				return err
			}
			bundleBytes, err := tlsutil.EncodeServerBundle(ca.CertPEM, ca.KeyPEM, serverCert, serverKey, nil)
			if err != nil {
				return err
			}
			if err := writeFile(out, bundleBytes, force); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "server bundle written to %s\n", out)
			return nil
		},
	}

	cmd.Flags().StringVar(&out, "out", "", "output bundle path (default $HOME/.lockd/server.pem)")
	cmd.Flags().StringVar(&cn, "cn", "lockd-server", "server certificate common name")
	cmd.Flags().StringVar(&hosts, "hosts", "*", "comma-separated hosts/IPs for server certificate")
	cmd.Flags().StringVar(&caCN, "ca-cn", "lockd-ca", "CA common name")
	cmd.Flags().DurationVar(&validity, "valid-for", 365*24*time.Hour, "certificate validity period")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing bundle if present")
	return cmd
}

func newAuthNewClientCommand() *cobra.Command {
	var out string
	var serverBundle string
	var cn string
	var validity time.Duration
	var force bool

	cmd := &cobra.Command{
		Use:   "new client",
		Short: "Issue a new client certificate using the server bundle",
		RunE: func(cmd *cobra.Command, args []string) error {
			if serverBundle == "" {
				path, err := lockd.DefaultBundlePath()
				if err != nil {
					return err
				}
				serverBundle = path
			}

			bundle, err := tlsutil.LoadBundle(serverBundle, "")
			if err != nil {
				return fmt.Errorf("load bundle: %w", err)
			}
			if bundle.CACertificate == nil || bundle.CAPrivateKey == nil {
				return fmt.Errorf("bundle does not contain CA private key")
			}
			ca, err := tlsutil.CAFromBundle(bundle)
			if err != nil {
				return err
			}

			if out == "" {
				dir, err := lockd.DefaultConfigDir()
				if err != nil {
					return err
				}
				out = filepath.Join(dir, fmt.Sprintf("client-%s.pem", uuid.NewString()))
			}

			certPEM, keyPEM, err := ca.IssueClient(cn, validity)
			if err != nil {
				return err
			}

			clientPEM, err := tlsutil.EncodeClientBundle(ca.CertPEM, certPEM, keyPEM)
			if err != nil {
				return err
			}
			if err := writeFile(out, clientPEM, force); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "client certificate written to %s\n", out)
			return nil
		},
	}
	cmd.Flags().StringVar(&serverBundle, "server-in", "", "path to server bundle (default $HOME/.lockd/server.pem)")
	cmd.Flags().StringVar(&out, "out", "", "client output path (default $HOME/.lockd/client-<uuid>.pem)")
	cmd.Flags().StringVar(&cn, "cn", "lockd-client", "client certificate common name")
	cmd.Flags().DurationVar(&validity, "valid-for", 365*24*time.Hour, "certificate validity period")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing file")
	return cmd
}

func newAuthRevokeClientCommand() *cobra.Command {
	var bundlePath string
	var out string
	var force bool

	cmd := &cobra.Command{
		Use:   "revoke client SERIAL [SERIAL...]",
		Short: "Revoke client certificates by serial number",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if bundlePath == "" {
				path, err := lockd.DefaultBundlePath()
				if err != nil {
					return err
				}
				bundlePath = path
			}
			bundle, err := tlsutil.LoadBundle(bundlePath, "")
			if err != nil {
				return fmt.Errorf("load bundle: %w", err)
			}
			if bundle.CAPrivateKey == nil || len(bundle.CAPrivateKeyPEM) == 0 {
				return fmt.Errorf("bundle does not contain CA private key")
			}
			serials := append([]string{}, bundle.DenylistEntries...)
			serials = append(serials, args...)
			serials = tlsutil.NormalizeSerials(serials)
			bundleBytes, err := tlsutil.EncodeServerBundle(bundle.CACertPEM, bundle.CAPrivateKeyPEM, bundle.ServerCertPEM, bundle.ServerKeyPEM, serials)
			if err != nil {
				return err
			}
			target := out
			if target == "" {
				target = bundlePath
			}
			if target != bundlePath {
				if err := writeFile(target, bundleBytes, force); err != nil {
					return err
				}
			} else {
				if err := os.WriteFile(target, bundleBytes, 0o600); err != nil {
					return fmt.Errorf("write bundle: %w", err)
				}
			}
			fmt.Fprintf(cmd.OutOrStdout(), "bundle updated: %s\n", target)
			return nil
		},
	}
	cmd.Flags().StringVar(&bundlePath, "server-in", "", "path to server bundle (default $HOME/.lockd/server.pem)")
	cmd.Flags().StringVar(&out, "out", "", "output bundle path (default overwrite input)")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite output bundle if it exists")
	return cmd
}

func newAuthInspectCommand() *cobra.Command {
	var in string

	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Display information about a server bundle",
		RunE: func(cmd *cobra.Command, args []string) error {
			if in == "" {
				path, err := lockd.DefaultBundlePath()
				if err != nil {
					return err
				}
				in = path
			}
			bundle, err := tlsutil.LoadBundle(in, "")
			if err != nil {
				return fmt.Errorf("load bundle: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Bundle: %s\n", in)
			if bundle.CACertificate != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "  CA: %s\n", bundle.CACertificate.Subject.String())
				fmt.Fprintf(cmd.OutOrStdout(), "    Serial: %s\n", bundle.CACertificate.SerialNumber.Text(16))
				fmt.Fprintf(cmd.OutOrStdout(), "    Expires: %s\n", bundle.CACertificate.NotAfter.Format(time.RFC3339))
			}
			serverCert := bundle.ServerCert
			if serverCert == nil {
				if cert, err := tlsutil.FirstCertificateFromPEM(bundle.ServerCertPEM); err == nil {
					serverCert = cert
				}
			}
			if serverCert != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "  Server: %s\n", serverCert.Subject.String())
				fmt.Fprintf(cmd.OutOrStdout(), "    Serial: %s\n", serverCert.SerialNumber.Text(16))
				fmt.Fprintf(cmd.OutOrStdout(), "    Expires: %s\n", serverCert.NotAfter.Format(time.RFC3339))
				if len(serverCert.DNSNames) > 0 {
					fmt.Fprintf(cmd.OutOrStdout(), "    DNS: %s\n", strings.Join(serverCert.DNSNames, ", "))
				}
				if len(serverCert.IPAddresses) > 0 {
					var ips []string
					for _, ip := range serverCert.IPAddresses {
						ips = append(ips, ip.String())
					}
					fmt.Fprintf(cmd.OutOrStdout(), "    IPs: %s\n", strings.Join(ips, ", "))
				}
			}
			if len(bundle.DenylistEntries) == 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "  Denylist: (empty)\n")
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "  Denylist (%d):\n", len(bundle.DenylistEntries))
				for _, serial := range bundle.DenylistEntries {
					fmt.Fprintf(cmd.OutOrStdout(), "    - %s\n", serial)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&in, "in", "", "path to server bundle (default $HOME/.lockd/server.pem)")
	return cmd
}

func ensureDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create dir %s: %w", path, err)
	}
	return nil
}

func writeFile(path string, data []byte, force bool) error {
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return err
	}
	if !force {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s already exists (use --force)", path)
		}
	}
	return os.WriteFile(path, data, 0o600)
}
