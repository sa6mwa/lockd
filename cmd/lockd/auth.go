package main

import (
	"crypto/x509"
	"errors"
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
	newCmd := &cobra.Command{
		Use:   "new",
		Short: "Create new certificate bundles",
	}
	newCmd.AddCommand(newAuthNewServerCommand())
	newCmd.AddCommand(newAuthNewClientCommand())
	cmd.AddCommand(newCmd)
	cmd.AddCommand(newAuthRevokeClientCommand())
	inspectCmd := &cobra.Command{
		Use:   "inspect",
		Short: "Display bundle details",
	}
	inspectCmd.AddCommand(newAuthInspectServerCommand())
	inspectCmd.AddCommand(newAuthInspectClientCommand())
	cmd.AddCommand(inspectCmd)
	verifyCmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify certificate bundles",
	}
	verifyCmd.AddCommand(newAuthVerifyServerCommand())
	verifyCmd.AddCommand(newAuthVerifyClientCommand())
	cmd.AddCommand(verifyCmd)
	return cmd
}

func newAuthNewServerCommand() *cobra.Command {
	var out string
	var cn string
	var hosts string
	var caCN string
	var validity time.Duration
	var force bool
	var caOut string

	cmd := &cobra.Command{
		Use:   "server",
		Short: "Create a new server bundle (CA + server certificate)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if out == "" {
				path, err := lockd.DefaultBundlePath()
				if err != nil {
					return err
				}
				out = path
			}
			if caOut == "" {
				dir, err := lockd.DefaultConfigDir()
				if err != nil {
					return err
				}
				caOut = filepath.Join(dir, "ca.pem")
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
			bundleBytes, err := tlsutil.EncodeServerBundle(ca.CertPEM, nil, serverCert, serverKey, nil)
			if err != nil {
				return err
			}
			if err := writeFile(out, bundleBytes, force); err != nil {
				return err
			}
			caBundle, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
			if err != nil {
				return err
			}
			if err := writeFile(caOut, caBundle, force); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "server bundle written to %s\n", out)
			fmt.Fprintf(cmd.OutOrStdout(), "ca certificate written to %s\n", caOut)
			return nil
		},
	}

	cmd.Flags().StringVar(&out, "out", "", "output bundle path (default $HOME/.lockd/server.pem)")
	cmd.Flags().StringVar(&cn, "cn", "lockd-server", "server certificate common name")
	cmd.Flags().StringVar(&hosts, "hosts", "*", "comma-separated hosts/IPs for server certificate")
	cmd.Flags().StringVar(&caCN, "ca-cn", "lockd-ca", "CA common name")
	cmd.Flags().DurationVar(&validity, "valid-for", 365*24*time.Hour, "certificate validity period")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing bundle if present")
	cmd.Flags().StringVar(&caOut, "ca-out", "", "output path for CA certificate (default $HOME/.lockd/ca.pem)")
	return cmd
}

func newAuthNewClientCommand() *cobra.Command {
	var out string
	var caBundle string
	var cn string
	var validity time.Duration
	var force bool

	cmd := &cobra.Command{
		Use:   "client",
		Short: "Issue a new client certificate using the CA bundle",
		RunE: func(cmd *cobra.Command, args []string) error {
			if caBundle == "" {
				path, err := lockd.DefaultBundlePath()
				if err != nil {
					return err
				}
				caBundle = filepath.Join(filepath.Dir(path), "ca.pem")
			}

			ca, err := tlsutil.LoadCA(caBundle)
			if err != nil {
				return fmt.Errorf("load ca: %w", err)
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
	cmd.Flags().StringVar(&caBundle, "ca-in", "", "path to CA bundle (default $HOME/.lockd/ca.pem)")
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

func newAuthInspectServerCommand() *cobra.Command {
	var in string

	cmd := &cobra.Command{
		Use:   "server",
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

func newAuthInspectClientCommand() *cobra.Command {
	var inputs []string
	cmd := &cobra.Command{
		Use:   "client",
		Short: "Display information about client bundles",
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := resolveClientBundlePaths(inputs)
			if err != nil {
				return err
			}
			if len(targets) == 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "No client bundles found.\n")
				return nil
			}
			for _, path := range targets {
				if err := inspectClientBundle(cmd, path); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&inputs, "in", nil, "path(s) to client bundle (default all client*.pem under config dir)")
	return cmd
}

func inspectClientBundle(cmd *cobra.Command, path string) error {
	bundle, err := tlsutil.LoadClientBundle(path)
	if err != nil {
		return err
	}
	clientCert := bundle.ClientCert
	fmt.Fprintf(cmd.OutOrStdout(), "Client bundle: %s\n", path)
	fmt.Fprintf(cmd.OutOrStdout(), "  Client: %s\n", clientCert.Subject.String())
	fmt.Fprintf(cmd.OutOrStdout(), "    Serial: %s\n", clientCert.SerialNumber.Text(16))
	fmt.Fprintf(cmd.OutOrStdout(), "    Expires: %s\n", clientCert.NotAfter.Format(time.RFC3339))
	for idx, ca := range bundle.CACerts {
		fmt.Fprintf(cmd.OutOrStdout(), "  CA[%d]: %s (serial %s, expires %s)\n", idx, ca.Subject.String(), ca.SerialNumber.Text(16), ca.NotAfter.Format(time.RFC3339))
	}
	return nil
}

func newAuthVerifyServerCommand() *cobra.Command {
	var in string
	cmd := &cobra.Command{
		Use:          "server",
		Short:        "Verify server bundle integrity",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if in == "" {
				path, err := lockd.DefaultBundlePath()
				if err != nil {
					return err
				}
				in = path
			}
			out := cmd.OutOrStdout()
			errOut := cmd.ErrOrStderr()
			bundle, err := tlsutil.LoadBundle(in, "")
			if err != nil {
				return fmt.Errorf("load bundle: %w", err)
			}
			issues := serverVerificationIssues(bundle)
			if len(issues) > 0 {
				fmt.Fprintf(errOut, "Server bundle %s verification failed:\n", in)
				for _, issue := range issues {
					fmt.Fprintf(errOut, "  - %s\n", issue)
				}
				return fmt.Errorf("server verification failed")
			}
			caSerial := "<unknown>"
			if bundle.CACertificate != nil {
				caSerial = bundle.CACertificate.SerialNumber.Text(16)
			}
			serverSerial := "<unknown>"
			if bundle.ServerCert != nil {
				serverSerial = bundle.ServerCert.SerialNumber.Text(16)
			}
			fmt.Fprintf(out, "Server bundle %s verified OK (CA serial %s, server serial %s)\n", in, caSerial, serverSerial)
			return nil
		},
	}
	cmd.Flags().StringVar(&in, "in", "", "path to server bundle (default $HOME/.lockd/server.pem)")
	return cmd
}

func newAuthVerifyClientCommand() *cobra.Command {
	var inputs []string
	var serverBundlePath string
	cmd := &cobra.Command{
		Use:          "client",
		Short:        "Verify client bundle integrity",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if serverBundlePath == "" {
				path, err := lockd.DefaultBundlePath()
				if err != nil {
					return err
				}
				serverBundlePath = path
			}
			out := cmd.OutOrStdout()
			errOut := cmd.ErrOrStderr()
			serverBundle, err := tlsutil.LoadBundle(serverBundlePath, "")
			if err != nil {
				return fmt.Errorf("load server bundle: %w", err)
			}
			if serverBundle.ServerCert == nil || !hasExtKeyUsage(serverBundle.ServerCert, x509.ExtKeyUsageServerAuth) {
				return fmt.Errorf("server bundle %s does not contain a server certificate with ServerAuth usage", serverBundlePath)
			}
			targets, err := resolveClientBundlePaths(inputs)
			if err != nil {
				return err
			}
			if len(targets) == 0 {
				fmt.Fprintf(errOut, "No client bundles found.\n")
				return nil
			}
			var failures int
			for _, path := range targets {
				clientBundle, err := tlsutil.LoadClientBundle(path)
				if err != nil {
					fmt.Fprintf(errOut, "Client bundle %s verification failed: %v\n", path, err)
					failures++
					continue
				}
				issues := clientVerificationIssues(clientBundle, serverBundle)
				if len(issues) > 0 {
					fmt.Fprintf(errOut, "Client bundle %s verification failed:\n", path)
					for _, issue := range issues {
						fmt.Fprintf(errOut, "  - %s\n", issue)
					}
					failures++
					continue
				}
				fmt.Fprintf(out, "Client bundle %s verified OK (serial %s)\n", path, clientBundle.ClientCert.SerialNumber.Text(16))
			}
			if failures > 0 {
				fmt.Fprintf(errOut, "Client verification failed for %d bundle(s).\n", failures)
				return fmt.Errorf("client verification failed for %d bundle(s)", failures)
			}
			fmt.Fprintf(out, "Client verification succeeded for %d bundle(s).\n", len(targets))
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&inputs, "in", nil, "path(s) to client bundle (default all client*.pem under config dir)")
	cmd.Flags().StringVar(&serverBundlePath, "server-in", "", "path to server bundle used for verification (default $HOME/.lockd/server.pem)")
	return cmd
}

func resolveClientBundlePaths(inputs []string) ([]string, error) {
	targets := append([]string(nil), inputs...)
	if len(targets) > 0 {
		return targets, nil
	}
	dir, err := lockd.DefaultConfigDir()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return targets, nil
		}
		return nil, fmt.Errorf("read config dir %s: %w", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "client") && strings.HasSuffix(name, ".pem") {
			targets = append(targets, filepath.Join(dir, name))
		}
	}
	return targets, nil
}

func serverVerificationIssues(bundle *tlsutil.Bundle) []string {
	var issues []string
	now := time.Now()
	if bundle.CACertificate == nil {
		issues = append(issues, "missing CA certificate")
	} else {
		if !bundle.CACertificate.IsCA {
			issues = append(issues, fmt.Sprintf("CA certificate %s missing CA flag", bundle.CACertificate.Subject.String()))
		}
		if bundle.CACertificate.NotAfter.Before(now) {
			issues = append(issues, fmt.Sprintf("CA certificate expired at %s", bundle.CACertificate.NotAfter.Format(time.RFC3339)))
		}
		if bundle.CACertificate.NotBefore.After(now) {
			issues = append(issues, fmt.Sprintf("CA certificate not valid until %s", bundle.CACertificate.NotBefore.Format(time.RFC3339)))
		}
	}
	if bundle.CAPrivateKey == nil {
		issues = append(issues, "missing CA private key")
	}
	if bundle.ServerCert == nil {
		issues = append(issues, "missing server certificate")
	} else {
		if bundle.ServerCert.NotAfter.Before(now) {
			issues = append(issues, fmt.Sprintf("server certificate expired at %s", bundle.ServerCert.NotAfter.Format(time.RFC3339)))
		}
		if bundle.ServerCert.NotBefore.After(now) {
			issues = append(issues, fmt.Sprintf("server certificate not valid until %s", bundle.ServerCert.NotBefore.Format(time.RFC3339)))
		}
		if !hasExtKeyUsage(bundle.ServerCert, x509.ExtKeyUsageServerAuth) {
			issues = append(issues, "server certificate missing ServerAuth extended key usage")
		}
		pool := x509.NewCertPool()
		if bundle.CACertificate != nil {
			pool.AddCert(bundle.CACertificate)
		}
		opts := x509.VerifyOptions{
			Roots:     pool,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		if _, err := bundle.ServerCert.Verify(opts); err != nil {
			issues = append(issues, fmt.Sprintf("server certificate failed verification: %v", err))
		}
	}
	if bundle.ServerCertificate.PrivateKey == nil {
		issues = append(issues, "missing server private key")
	}
	return issues
}

func clientVerificationIssues(clientBundle *tlsutil.ClientBundle, serverBundle *tlsutil.Bundle) []string {
	var issues []string
	now := time.Now()
	clientCert := clientBundle.ClientCert
	if clientCert.NotAfter.Before(now) {
		issues = append(issues, fmt.Sprintf("client certificate expired at %s", clientCert.NotAfter.Format(time.RFC3339)))
	}
	if clientCert.NotBefore.After(now) {
		issues = append(issues, fmt.Sprintf("client certificate not valid until %s", clientCert.NotBefore.Format(time.RFC3339)))
	}
	if len(clientBundle.CACerts) == 0 {
		issues = append(issues, "missing CA certificate")
		return issues
	}
	pool := x509.NewCertPool()
	for _, ca := range clientBundle.CACerts {
		pool.AddCert(ca)
		if !ca.IsCA {
			issues = append(issues, fmt.Sprintf("embedded CA %s missing CA flag", ca.Subject.String()))
		}
		if ca.NotAfter.Before(now) {
			issues = append(issues, fmt.Sprintf("embedded CA %s expired at %s", ca.Subject.String(), ca.NotAfter.Format(time.RFC3339)))
		}
		if ca.NotBefore.After(now) {
			issues = append(issues, fmt.Sprintf("embedded CA %s not valid until %s", ca.Subject.String(), ca.NotBefore.Format(time.RFC3339)))
		}
	}
	opts := x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if _, err := clientCert.Verify(opts); err != nil {
		issues = append(issues, fmt.Sprintf("client certificate failed verification: %v", err))
	}
	if serverBundle != nil && serverBundle.CACertificate != nil {
		if err := clientCert.CheckSignatureFrom(serverBundle.CACertificate); err != nil {
			issues = append(issues, fmt.Sprintf("client certificate not signed by server CA (serial %s): %v", serverBundle.CACertificate.SerialNumber.Text(16), err))
		}
		if _, revoked := serverBundle.Denylist[strings.ToLower(clientCert.SerialNumber.Text(16))]; revoked {
			issues = append(issues, fmt.Sprintf("client certificate serial %s is revoked in denylist", clientCert.SerialNumber.Text(16)))
		}
	}
	return issues
}

func hasExtKeyUsage(cert *x509.Certificate, target x509.ExtKeyUsage) bool {
	for _, ku := range cert.ExtKeyUsage {
		if ku == target {
			return true
		}
	}
	return false
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
