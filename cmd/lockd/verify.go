package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

func newVerifyCommand(logger pslog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Run diagnostic checks",
	}
	cmd.AddCommand(newVerifyStoreCommand(logger))
	return cmd
}

func newVerifyStoreCommand(logger pslog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "store",
		Short:        "Verify storage configuration",
		SilenceUsage: true,
		Example: strings.TrimSpace(`
# Verify disk backend
LOCKD_STORE=disk:///var/lib/lockd lockd verify store

# Verify Azure Blob using Shared Key credentials
LOCKD_STORE=azure://myacct/lockd LOCKD_AZURE_ACCOUNT_KEY=... lockd verify store

# Verify S3-compatible service (MinIO)
LOCKD_STORE=s3://localhost:9000/lockd?insecure=1 LOCKD_S3_ACCESS_KEY_ID=minio LOCKD_S3_SECRET_ACCESS_KEY=minio123 lockd verify store

# Verify AWS S3
LOCKD_STORE=aws://my-bucket LOCKD_AWS_REGION=us-west-2 lockd verify store
`),
		RunE: func(cmd *cobra.Command, args []string) error {
			var cfg lockd.Config
			if err := bindConfig(&cfg); err != nil {
				return err
			}
			if err := cfg.Validate(); err != nil {
				return err
			}

			res, err := storagecheck.VerifyStore(cmd.Context(), cfg)
			if errors.Is(err, storage.ErrNotImplemented) {
				fmt.Fprintln(cmd.OutOrStdout(), "Storage verification not implemented for this backend")
				return nil
			}
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Store: %s\n", cfg.Store)
			if res.Provider != "" {
				fmt.Fprintf(out, "Provider: %s\n", res.Provider)
			}
			if res.Endpoint != "" {
				fmt.Fprintf(out, "Endpoint: %s (insecure:%t)\n", res.Endpoint, res.Insecure)
			}
			if res.Bucket != "" {
				fmt.Fprintf(out, "Bucket/Container: %s\n", res.Bucket)
			}
			if res.Prefix != "" {
				fmt.Fprintf(out, "Prefix: %s\n", res.Prefix)
			}
			cred := res.Credentials
			if cred.Source != "" || cred.AccessKey != "" {
				accessKey := cred.AccessKey
				if accessKey == "" {
					accessKey = "(none)"
				}
				fmt.Fprintf(out, "AccessKey: %s (has_secret:%t source:%s)\n", accessKey, cred.HasSecret, cred.Source)
			}
			if res.AdditionalMessage != "" {
				fmt.Fprintln(out, res.AdditionalMessage)
			}
			fmt.Fprintln(out)

			for _, check := range res.Checks {
				if check.Err == nil {
					fmt.Fprintf(out, "✔ %s\n", check.Name)
				} else {
					fmt.Fprintf(out, "✘ %s: %v\n", check.Name, check.Err)
				}
			}
			if res.Passed() {
				fmt.Fprintln(out, "Storage verification succeeded.")
				return nil
			}
			if res.RecommendedPolicy != "" {
				fmt.Fprintf(out, "\nRecommended AWS IAM policy:\n%s\n", res.RecommendedPolicy)
			}
			return fmt.Errorf("storage verification failed")
		},
	}
	return cmd
}
