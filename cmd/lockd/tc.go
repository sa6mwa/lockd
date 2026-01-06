package main

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"pkt.systems/lockd"
	"pkt.systems/lockd/tlsutil"
)

func newTCCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tc",
		Short: "Transaction coordinator federation helpers",
	}
	cmd.AddCommand(newTCExportCACommand())
	cmd.AddCommand(newTCTrustCommand())
	cmd.AddCommand(newTCAnnounceCommand())
	cmd.AddCommand(newTCLeaveCommand())
	cmd.AddCommand(newTCListCommand())
	return cmd
}

func newTCExportCACommand() *cobra.Command {
	var bundlePath string
	var outPath string
	cmd := &cobra.Command{
		Use:   "ca-export",
		Short: "Export the local CA certificate",
		RunE: func(cmd *cobra.Command, args []string) error {
			if bundlePath == "" {
				if def, err := lockd.DefaultBundlePath(); err == nil {
					bundlePath = def
				}
			}
			var err error
			if bundlePath, err = expandPath(bundlePath); err != nil {
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
			if outPath, err = expandPath(outPath); err != nil {
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

func newTCTrustCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trust",
		Short: "Manage trusted TC CA certificates",
	}
	cmd.AddCommand(newTCTrustAddCommand())
	cmd.AddCommand(newTCTrustListCommand())
	return cmd
}

func newTCTrustAddCommand() *cobra.Command {
	var caPath string
	var trustDir string
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Trust a remote TC CA certificate",
		RunE: func(cmd *cobra.Command, args []string) error {
			if caPath == "" {
				return fmt.Errorf("--ca is required")
			}
			var err error
			if caPath, err = expandPath(caPath); err != nil {
				return err
			}
			if trustDir == "" {
				if def, derr := lockd.DefaultTCTrustDir(); derr == nil {
					trustDir = def
				}
			}
			if trustDir, err = expandPath(trustDir); err != nil {
				return err
			}
			data, err := os.ReadFile(caPath)
			if err != nil {
				return fmt.Errorf("read ca: %w", err)
			}
			cert, err := firstCertFromPEM(data)
			if err != nil {
				return fmt.Errorf("parse ca: %w", err)
			}
			fingerprint := sha256.Sum256(cert.Raw)
			name := fmt.Sprintf("ca-%s.pem", hex.EncodeToString(fingerprint[:8]))
			if err := os.MkdirAll(trustDir, 0o755); err != nil {
				return fmt.Errorf("create trust dir: %w", err)
			}
			dest := filepath.Join(trustDir, name)
			if _, err := os.Stat(dest); err == nil {
				fmt.Fprintf(cmd.OutOrStdout(), "already trusted: %s (%s)\n", dest, cert.Subject.String())
				return nil
			}
			if err := os.WriteFile(dest, data, 0o644); err != nil {
				return fmt.Errorf("write trust entry: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "trusted CA %s -> %s\n", cert.Subject.String(), dest)
			return nil
		},
	}
	cmd.Flags().StringVar(&caPath, "ca", "", "path to CA certificate PEM to trust")
	cmd.Flags().StringVar(&trustDir, "trust-dir", "", "trust directory (default $HOME/.lockd/tc-trust.d)")
	return cmd
}

func newTCTrustListCommand() *cobra.Command {
	var trustDir string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List trusted TC CA certificates",
		RunE: func(cmd *cobra.Command, args []string) error {
			if trustDir == "" {
				if def, err := lockd.DefaultTCTrustDir(); err == nil {
					trustDir = def
				}
			}
			var err error
			if trustDir, err = expandPath(trustDir); err != nil {
				return err
			}
			entries, err := os.ReadDir(trustDir)
			if err != nil {
				if os.IsNotExist(err) {
					fmt.Fprintln(cmd.OutOrStdout(), "(no trusted TC CAs)")
					return nil
				}
				return fmt.Errorf("read trust dir: %w", err)
			}
			if len(entries) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "(no trusted TC CAs)")
				return nil
			}
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				path := filepath.Join(trustDir, entry.Name())
				data, readErr := os.ReadFile(path)
				if readErr != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "%s: %v\n", path, readErr)
					continue
				}
				cert, parseErr := firstCertFromPEM(data)
				if parseErr != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "%s: %v\n", path, parseErr)
					continue
				}
				fp := sha256.Sum256(cert.Raw)
				fmt.Fprintf(cmd.OutOrStdout(), "%s  %s  (sha256:%s)\n", path, cert.Subject.String(), hex.EncodeToString(fp[:8]))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&trustDir, "trust-dir", "", "trust directory (default $HOME/.lockd/tc-trust.d)")
	return cmd
}

func firstCertFromPEM(data []byte) (*x509.Certificate, error) {
	var block *pem.Block
	rest := data
	for {
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			return x509.ParseCertificate(block.Bytes)
		}
	}
	return nil, fmt.Errorf("no certificate found in PEM")
}
