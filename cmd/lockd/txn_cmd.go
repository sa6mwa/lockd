package main

import (
	"github.com/spf13/cobra"
	"pkt.systems/pslog"
)

// newTxnRootCommand builds the canonical root-level txn command with its own
// connection flags. A hidden alias lives under "lockd client txn".
func newTxnRootCommand(baseLogger pslog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "txn",
		Short:        "Transaction coordination helpers",
		SilenceUsage: true,
	}
	cfg := addClientConnectionFlags(cmd, true, baseLogger)
	cmd.AddCommand(
		newClientTxPrepareCommand(cfg),
		newClientTxCommitCommand(cfg),
		newClientTxRollbackCommand(cfg),
		newClientTxReplayCommand(cfg),
	)
	return cmd
}
