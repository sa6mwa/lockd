package main

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/tlsutil"
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
	newCmd.AddCommand(newAuthNewCACommand())
	newCmd.AddCommand(newAuthNewServerCommand())
	newCmd.AddCommand(newAuthNewClientCommand())
	newCmd.AddCommand(newAuthNewTCClientCommand())
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

func newAuthNewCACommand() *cobra.Command {
	var out string
	var cn string
	var validity time.Duration
	var force bool

	cmd := &cobra.Command{
		Use:   "ca",
		Short: "Create a new certificate authority (CA) bundle",
		RunE: func(cmd *cobra.Command, args []string) error {
			if out == "" {
				path, err := lockd.DefaultCAPath()
				if err != nil {
					return err
				}
				out = path
			}
			ca, err := tlsutil.GenerateCA(cn, validity)
			if err != nil {
				return err
			}
			data, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
			if err != nil {
				return err
			}
			if err := writeFile(out, data, force); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "ca bundle written to %s\n", out)
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "output path for CA bundle (default $HOME/.lockd/ca.pem)")
	cmd.Flags().StringVar(&cn, "cn", "lockd-ca", "CA certificate common name")
	cmd.Flags().DurationVar(&validity, "valid-for", 10*365*24*time.Hour, "certificate validity period")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing CA bundle if present")
	return cmd
}

func newAuthNewServerCommand() *cobra.Command {
	var out string
	var cn string
	var hosts string
	var validity time.Duration
	var force bool
	var caIn string

	cmd := &cobra.Command{
		Use:   "server",
		Short: "Create a new server bundle signed by an existing CA",
		RunE: func(cmd *cobra.Command, args []string) error {
			if out == "" {
				path, err := lockd.DefaultBundlePath()
				if err != nil {
					return err
				}
				out = path
			}
			if caIn == "" {
				path, err := lockd.DefaultCAPath()
				if err != nil {
					return err
				}
				caIn = path
			}
			ca, err := tlsutil.LoadCA(caIn)
			if err != nil {
				return fmt.Errorf("load ca: %w (run 'lockd auth new ca' first)", err)
			}
			material, err := cryptoutil.EnsureCAMetadataMaterial(caIn, ca.CertPEM)
			if err != nil {
				return err
			}

			if !force && !cmd.Flags().Changed("out") {
				deduped, err := nextSequentialPath(out)
				if err != nil {
					return err
				}
				out = deduped
			}

			hostList := strings.Split(hosts, ",")
			nodeID := ""
			if info, err := os.Stat(out); err == nil && !info.IsDir() {
				if existing, ok := serverNodeIDFromBundle(out); ok {
					nodeID = existing
				}
			}
			if nodeID == "" {
				nodeID = uuidv7.NewString()
			}
			spiffeURI, err := lockd.SPIFFEURIForServer(nodeID)
			if err != nil {
				return fmt.Errorf("spiffe uri: %w", err)
			}
			allClaim, err := nsauth.ClaimURI("ALL", nsauth.PermissionReadWrite)
			if err != nil {
				return fmt.Errorf("all namespace claim: %w", err)
			}
			issued, err := ca.IssueServerWithRequest(tlsutil.ServerCertRequest{
				CommonName: cn,
				Validity:   validity,
				Hosts:      hostList,
				URIs:       []*url.URL{spiffeURI, allClaim},
			})
			if err != nil {
				return err
			}
			bundleBytes, err := tlsutil.EncodeServerBundle(ca.CertPEM, nil, issued.CertPEM, issued.KeyPEM, nil)
			if err != nil {
				return err
			}
			augmented, err := cryptoutil.ApplyMetadataMaterial(bundleBytes, material)
			if err != nil {
				return err
			}
			if err := writeFile(out, augmented, force); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "server bundle written to %s\n", out)
			fmt.Fprintf(cmd.OutOrStdout(), "ca certificate referenced from %s\n", caIn)
			return nil
		},
	}

	cmd.Flags().StringVar(&out, "out", "", "output bundle path (default $HOME/.lockd/server.pem)")
	cmd.Flags().StringVar(&cn, "cn", "lockd-server", "server certificate common name")
	cmd.Flags().StringVar(&hosts, "hosts", "*", "comma-separated hosts/IPs for server certificate")
	cmd.Flags().DurationVar(&validity, "valid-for", 365*24*time.Hour, "certificate validity period")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing bundle if present")
	cmd.Flags().StringVar(&caIn, "ca-in", "", "path to CA bundle (default $HOME/.lockd/ca.pem)")
	return cmd
}

func serverNodeIDFromBundle(path string) (string, bool) {
	bundle, err := tlsutil.LoadBundle(path, "")
	if err != nil {
		return "", false
	}
	nodeID := serverNodeIDFromCert(bundle.ServerCert)
	return nodeID, nodeID != ""
}

func serverNodeIDFromCert(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	for _, uri := range cert.URIs {
		if uri == nil {
			continue
		}
		if uri.Scheme != "spiffe" || !strings.EqualFold(uri.Host, "lockd") {
			continue
		}
		if !strings.HasPrefix(uri.Path, "/server/") {
			continue
		}
		nodeID := strings.Trim(strings.TrimPrefix(uri.Path, "/server/"), "/")
		if nodeID != "" {
			return nodeID
		}
	}
	return ""
}

func newAuthNewClientCommand() *cobra.Command {
	var out string
	var caBundle string
	var cn string
	var validity time.Duration
	var force bool
	var namespaceInputs []string
	var readAll bool
	var writeAll bool
	var rwAll bool

	cmd := &cobra.Command{
		Use:   "client",
		Short: "Issue a new client certificate using the CA bundle",
		RunE: func(cmd *cobra.Command, args []string) error {
			if caBundle == "" {
				path, err := lockd.DefaultCAPath()
				if err != nil {
					return err
				}
				caBundle = path
			}

			ca, err := tlsutil.LoadCA(caBundle)
			if err != nil {
				return fmt.Errorf("load ca: %w (run 'lockd auth new ca' first)", err)
			}

			defaultOut := !cmd.Flags().Changed("out")
			if out == "" {
				dir, err := lockd.DefaultConfigDir()
				if err != nil {
					return err
				}
				out = filepath.Join(dir, "client.pem")
			}
			if !force && defaultOut {
				deduped, err := nextSequentialPath(out)
				if err != nil {
					return err
				}
				out = deduped
			}

			effectiveCN := strings.TrimSpace(cn)
			if effectiveCN == "" {
				effectiveCN = "lockd-client"
			}
			spiffeURI, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleSDK, effectiveCN)
			if err != nil {
				return err
			}
			namespacePerms, hasExplicit, err := parseNamespacePermissions(namespaceInputs, readAll, writeAll, rwAll)
			if err != nil {
				return err
			}
			if !hasExplicit {
				mergeNamespacePermission(namespacePerms, lockd.DefaultNamespace, nsauth.PermissionReadWrite)
			}
			namespaceClaims, err := namespacePermissionsToURIs(namespacePerms)
			if err != nil {
				return err
			}
			uris := []*url.URL{spiffeURI}
			uris = append(uris, namespaceClaims...)
			issued, err := ca.IssueClient(tlsutil.ClientCertRequest{
				CommonName: effectiveCN,
				Validity:   validity,
				URIs:       uris,
			})
			if err != nil {
				return err
			}

			clientPEM, err := tlsutil.EncodeClientBundle(ca.CertPEM, issued.CertPEM, issued.KeyPEM)
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
	cmd.Flags().StringVar(&out, "out", "", "client output path (default $HOME/.lockd/client.pem)")
	cmd.Flags().StringVar(&cn, "cn", "lockd-client", "client certificate common name")
	cmd.Flags().DurationVar(&validity, "valid-for", 365*24*time.Hour, "certificate validity period")
	cmd.Flags().StringSliceVarP(&namespaceInputs, "namespace", "n", nil, "namespace claims, repeatable and/or csv (example: -n default=rw,orders=w,stash)")
	cmd.Flags().BoolVar(&readAll, "read-all", false, "alias for -n ALL=r")
	cmd.Flags().BoolVar(&writeAll, "write-all", false, "alias for -n ALL=w")
	cmd.Flags().BoolVar(&rwAll, "rw-all", false, "alias for -n ALL=rw")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing file")
	return cmd
}

func newAuthNewTCClientCommand() *cobra.Command {
	var out string
	var caBundle string
	var cn string
	var validity time.Duration
	var force bool

	cmd := &cobra.Command{
		Use:   "tcclient",
		Short: "Issue a new TC client certificate using the CA bundle",
		RunE: func(cmd *cobra.Command, args []string) error {
			if caBundle == "" {
				path, err := lockd.DefaultCAPath()
				if err != nil {
					return err
				}
				caBundle = path
			}

			ca, err := tlsutil.LoadCA(caBundle)
			if err != nil {
				return fmt.Errorf("load ca: %w (run 'lockd auth new ca' first)", err)
			}

			defaultOut := !cmd.Flags().Changed("out")
			if out == "" {
				dir, err := lockd.DefaultConfigDir()
				if err != nil {
					return err
				}
				out = filepath.Join(dir, "tc-client.pem")
			}
			if !force && defaultOut {
				deduped, err := nextSequentialPath(out)
				if err != nil {
					return err
				}
				out = deduped
			}

			effectiveCN := strings.TrimSpace(cn)
			if effectiveCN == "" {
				effectiveCN = "lockd-tc-client"
			}
			spiffeURI, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleTC, effectiveCN)
			if err != nil {
				return err
			}
			allClaim, err := nsauth.ClaimURI("ALL", nsauth.PermissionReadWrite)
			if err != nil {
				return err
			}
			issued, err := ca.IssueClient(tlsutil.ClientCertRequest{
				CommonName: effectiveCN,
				Validity:   validity,
				URIs:       []*url.URL{spiffeURI, allClaim},
			})
			if err != nil {
				return err
			}

			clientPEM, err := tlsutil.EncodeClientBundle(ca.CertPEM, issued.CertPEM, issued.KeyPEM)
			if err != nil {
				return err
			}
			if err := writeFile(out, clientPEM, force); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "tc client certificate written to %s\n", out)
			return nil
		},
	}
	cmd.Flags().StringVar(&caBundle, "ca-in", "", "path to CA bundle (default $HOME/.lockd/ca.pem)")
	cmd.Flags().StringVar(&out, "out", "", "tc client output path (default $HOME/.lockd/tc-client.pem)")
	cmd.Flags().StringVar(&cn, "cn", "lockd-tc-client", "tc client certificate common name")
	cmd.Flags().DurationVar(&validity, "valid-for", 365*24*time.Hour, "certificate validity period")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing file")
	return cmd
}

func newAuthRevokeClientCommand() *cobra.Command {
	var bundlePath string
	var out string
	var force bool
	propagate := true

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
			material, err := cryptoutil.MetadataMaterialFromBundle(bundlePath)
			if err != nil {
				return err
			}
			serials := append([]string{}, bundle.DenylistEntries...)
			serials = append(serials, args...)
			serials = tlsutil.NormalizeSerials(serials)
			bundleBytes, err := tlsutil.EncodeServerBundle(bundle.CACertPEM, bundle.CAPrivateKeyPEM, bundle.ServerCertPEM, bundle.ServerKeyPEM, serials)
			if err != nil {
				return err
			}
			bundleBytes, err = cryptoutil.ApplyMetadataMaterial(bundleBytes, material)
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
			if propagate {
				count, err := propagateDenylistBundles(filepath.Dir(bundlePath), serials)
				if err != nil {
					return err
				}
				if count > 0 {
					fmt.Fprintf(cmd.OutOrStdout(), "denylist propagated to %d server bundle(s)\n", count)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&bundlePath, "server-in", "", "path to server bundle (default $HOME/.lockd/server.pem)")
	cmd.Flags().StringVar(&out, "out", "", "output bundle path (default overwrite input)")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite output bundle if it exists")
	cmd.Flags().BoolVar(&propagate, "propagate", true, "propagate denylist to all server*.pem bundles in the same directory")
	return cmd
}

func newAuthInspectServerCommand() *cobra.Command {
	var inputs []string
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Display information about a server bundle",
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := resolveServerBundlePaths(inputs)
			if err != nil {
				return err
			}
			if len(targets) == 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "No server bundles found.\n")
				return nil
			}
			for _, path := range targets {
				if err := inspectServerBundle(cmd, path); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&inputs, "in", nil, "path(s) to server bundle (default all server*.pem under config dir)")
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
	cmd.Flags().StringSliceVar(&inputs, "in", nil, "path(s) to client bundle (default client*.pem + tc-client*.pem under config dir)")
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
	if len(clientCert.URIs) > 0 {
		for idx, uri := range clientCert.URIs {
			if uri == nil {
				continue
			}
			fmt.Fprintf(cmd.OutOrStdout(), "    URI[%d]: %s\n", idx, uri.String())
		}
	}
	for idx, ca := range bundle.CACerts {
		fmt.Fprintf(cmd.OutOrStdout(), "  CA[%d]: %s (serial %s, expires %s)\n", idx, ca.Subject.String(), ca.SerialNumber.Text(16), ca.NotAfter.Format(time.RFC3339))
	}
	return nil
}

func inspectServerBundle(cmd *cobra.Command, path string) error {
	bundle, err := tlsutil.LoadBundle(path, "")
	if err != nil {
		return fmt.Errorf("load bundle %s: %w", path, err)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Bundle: %s\n", path)
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
}

func newAuthVerifyServerCommand() *cobra.Command {
	var inputs []string
	cmd := &cobra.Command{
		Use:          "server",
		Short:        "Verify server bundle integrity",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := resolveServerBundlePaths(inputs)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			errOut := cmd.ErrOrStderr()
			if len(targets) == 0 {
				fmt.Fprintf(errOut, "No server bundles found.\n")
				return nil
			}
			var failures int
			for _, path := range targets {
				bundle, err := tlsutil.LoadBundle(path, "")
				if err != nil {
					fmt.Fprintf(errOut, "Server bundle %s verification failed: %v\n", path, err)
					failures++
					continue
				}
				issues := serverVerificationIssues(bundle)
				if len(issues) > 0 {
					fmt.Fprintf(errOut, "Server bundle %s verification failed:\n", path)
					for _, issue := range issues {
						fmt.Fprintf(errOut, "  - %s\n", issue)
					}
					failures++
					continue
				}
				caSerial := "<unknown>"
				if bundle.CACertificate != nil {
					caSerial = bundle.CACertificate.SerialNumber.Text(16)
				}
				serverSerial := "<unknown>"
				if bundle.ServerCert != nil {
					serverSerial = bundle.ServerCert.SerialNumber.Text(16)
				}
				fmt.Fprintf(out, "Server bundle %s verified OK (CA serial %s, server serial %s)\n", path, caSerial, serverSerial)
			}
			if failures > 0 {
				fmt.Fprintf(errOut, "Server verification failed for %d bundle(s).\n", failures)
				return fmt.Errorf("server verification failed for %d bundle(s)", failures)
			}
			fmt.Fprintf(out, "Server verification succeeded for %d bundle(s).\n", len(targets))
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&inputs, "in", nil, "path(s) to server bundle (default all server*.pem under config dir)")
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
				clientSerial := clientBundle.ClientCert.SerialNumber.Text(16)
				var caSerials []string
				for _, caCert := range clientBundle.CACerts {
					caSerials = append(caSerials, caCert.SerialNumber.Text(16))
				}
				var caSummary string
				switch len(caSerials) {
				case 0:
					caSummary = "no CA serials"
				case 1:
					caSummary = fmt.Sprintf("CA serial %s", caSerials[0])
				default:
					caSummary = fmt.Sprintf("CA serials [%s]", strings.Join(caSerials, ", "))
				}
				fmt.Fprintf(out, "Client bundle %s verified OK (%s, client serial %s)\n", path, caSummary, clientSerial)
			}
			if failures > 0 {
				fmt.Fprintf(errOut, "Client verification failed for %d bundle(s).\n", failures)
				return fmt.Errorf("client verification failed for %d bundle(s)", failures)
			}
			fmt.Fprintf(out, "Client verification succeeded for %d bundle(s).\n", len(targets))
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&inputs, "in", nil, "path(s) to client bundle (default client*.pem + tc-client*.pem under config dir)")
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
		if (strings.HasPrefix(name, "client") || strings.HasPrefix(name, "tc-client")) && strings.HasSuffix(name, ".pem") {
			targets = append(targets, filepath.Join(dir, name))
		}
	}
	return targets, nil
}

func resolveServerBundlePaths(inputs []string) ([]string, error) {
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
		if strings.HasPrefix(name, "server") && strings.HasSuffix(name, ".pem") {
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
	return slices.Contains(cert.ExtKeyUsage, target)
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

func nextSequentialPath(path string) (string, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return path, nil
		}
		return "", fmt.Errorf("stat %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	ext := filepath.Ext(base)
	if ext == "" {
		ext = ".pem"
	}
	const maxAttempts = 999
	for i := 2; i <= maxAttempts; i++ {
		candidate := fmt.Sprintf("%s%02d%s", name, i, ext)
		full := filepath.Join(dir, candidate)
		if _, err := os.Stat(full); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return full, nil
			}
			return "", fmt.Errorf("stat %s: %w", full, err)
		}
	}
	return "", fmt.Errorf("unable to find available file name under %s", dir)
}

func propagateDenylistBundles(dir string, serials []string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("read server bundle dir %s: %w", dir, err)
	}
	var count int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "server") || !strings.HasSuffix(name, ".pem") {
			continue
		}
		path := filepath.Join(dir, name)
		bundle, err := tlsutil.LoadBundle(path, "")
		if err != nil {
			return count, fmt.Errorf("load bundle %s: %w", path, err)
		}
		material, err := cryptoutil.MetadataMaterialFromBundle(path)
		if err != nil {
			return count, err
		}
		updated, err := tlsutil.EncodeServerBundle(bundle.CACertPEM, bundle.CAPrivateKeyPEM, bundle.ServerCertPEM, bundle.ServerKeyPEM, serials)
		if err != nil {
			return count, fmt.Errorf("encode bundle %s: %w", path, err)
		}
		updated, err = cryptoutil.ApplyMetadataMaterial(updated, material)
		if err != nil {
			return count, err
		}
		if err := os.WriteFile(path, updated, 0o600); err != nil {
			return count, fmt.Errorf("write bundle %s: %w", path, err)
		}
		count++
	}
	return count, nil
}
