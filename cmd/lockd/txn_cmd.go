package main

import (
	"github.com/spf13/cobra"
)

// newTxnRootCommand builds the canonical root-level txn command.
// A hidden alias lives under "lockd client txn".
func newTxnRootCommand(cfg *clientCLIConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "txn",
		Short:        "Transaction coordination helpers",
		SilenceUsage: true,
	}
	cmd.AddCommand(
		newClientTxPrepareCommand(cfg),
		newClientTxCommitCommand(cfg),
		newClientTxRollbackCommand(cfg),
		newClientTxReplayCommand(cfg),
	)
	return cmd
}
