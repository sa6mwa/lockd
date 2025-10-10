package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	port "pkt.systems/logport"
)

func newVerifyCommand(logger port.ForLogging) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Run diagnostic checks",
	}
	cmd.AddCommand(newVerifyStoreCommand(logger))
	return cmd
}

func newVerifyStoreCommand(logger port.ForLogging) *cobra.Command {
	return &cobra.Command{
		Use:   "store",
		Short: "Verify storage configuration",
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
			for _, check := range res.Checks {
				if check.Err == nil {
					fmt.Fprintf(cmd.OutOrStdout(), "✔ %s\n", check.Name)
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "✘ %s: %v\n", check.Name, check.Err)
				}
			}
			if res.Passed() {
				fmt.Fprintln(cmd.OutOrStdout(), "Storage verification succeeded.")
				return nil
			}
			if res.RecommendedPolicy != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "\nRecommended AWS IAM policy:\n%s\n", res.RecommendedPolicy)
			}
			return fmt.Errorf("storage verification failed")
		},
	}
}
