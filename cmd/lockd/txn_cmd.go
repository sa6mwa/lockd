package main

import (
	"github.com/spf13/cobra"
)

// newTxnRootCommand builds the canonical root-level txn command with its own
// connection flags. A hidden alias lives under "lockd client txn".
func newTxnRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "txn",
		Short:        "Transaction coordination helpers",
		SilenceUsage: true,
	}
	cfg := addClientConnectionFlags(cmd, true)
	cmd.AddCommand(
		newClientTxPrepareCommand(cfg),
		newClientTxCommitCommand(cfg),
		newClientTxRollbackCommand(cfg),
		newClientTxReplayCommand(cfg),
	)
	return cmd
}
