package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"pkt.systems/lockd/internal/version"
)

func newVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the lockd version",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "%s %s\n", version.Module(), version.Current())
			return err
		},
	}
	return cmd
}
