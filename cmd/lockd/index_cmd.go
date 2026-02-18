package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

func newIndexCommand(cfg *clientCLIConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "Manage namespace indexing",
	}
	var namespace string
	cmd.PersistentFlags().StringVarP(&namespace, "namespace", "n", "", "namespace name (defaults to client default)")
	cmd.AddCommand(newIndexFlushCommand(cfg, &namespace))
	cmd.AddCommand(newIndexRebuildCommand())
	return cmd
}

func newIndexFlushCommand(cfg *clientCLIConfig, namespace *string) *cobra.Command {
	var mode string
	var output string
	cmd := &cobra.Command{
		Use:   "flush",
		Short: "Force a namespace index flush",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			client, err := cfg.client()
			if err != nil {
				return err
			}
			defer cfg.cleanup()
			ns := resolveNamespaceInput(*namespace)
			var flushOpts []lockdclient.FlushOption
			switch strings.ToLower(strings.TrimSpace(mode)) {
			case "wait":
				flushOpts = append(flushOpts, lockdclient.WithFlushModeWait())
			case "async", "":
				flushOpts = append(flushOpts, lockdclient.WithFlushModeAsync())
			default:
				return fmt.Errorf("unsupported mode %q (expected wait or async)", mode)
			}
			resp, err := client.FlushIndex(ctx, ns, flushOpts...)
			if err != nil {
				return err
			}
			return printIndexFlushResponse(cmd, resp, output)
		},
	}
	cmd.Flags().StringVarP(&mode, "mode", "m", "async", "flush mode (wait or async)")
	cmd.Flags().StringVarP(&output, "output", "o", "json", "output format (json|text)")
	return cmd
}

func printIndexFlushResponse(cmd *cobra.Command, resp *api.IndexFlushResponse, format string) error {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "json":
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(resp)
	case "text":
		w := cmd.OutOrStdout()
		fmt.Fprintf(w, "Namespace: %s\n", resp.Namespace)
		fmt.Fprintf(w, "Mode: %s\n", resp.Mode)
		fmt.Fprintf(w, "Accepted: %t\n", resp.Accepted)
		fmt.Fprintf(w, "Flushed: %t\n", resp.Flushed)
		fmt.Fprintf(w, "Pending: %t\n", resp.Pending)
		if resp.IndexSeq != 0 {
			fmt.Fprintf(w, "Index sequence: %d\n", resp.IndexSeq)
		}
		if resp.FlushID != "" {
			fmt.Fprintf(w, "Flush ID: %s\n", resp.FlushID)
		}
		return nil
	default:
		return fmt.Errorf("unsupported output format %q (expected json or text)", format)
	}
}
