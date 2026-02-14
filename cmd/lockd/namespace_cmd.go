package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/pslog"
)

func newNamespaceCommand(baseLogger pslog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "namespace",
		Aliases: []string{"ns"},
		Short:   "Inspect and update namespace configuration",
	}
	cfg := addClientConnectionFlags(cmd, false, baseLogger)
	cmd.AddCommand(
		newNamespaceGetCommand(cfg),
		newNamespaceSetCommand(cfg),
	)
	return cmd
}

func newNamespaceGetCommand(cfg *clientCLIConfig) *cobra.Command {
	var namespace string
	var outputType string
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Display namespace configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			client, err := cfg.client()
			if err != nil {
				return err
			}
			defer cfg.cleanup()
			result, err := client.GetNamespaceConfig(ctx, namespace)
			if err != nil {
				return err
			}
			return printNamespaceConfig(cmd, result.Config, result.ETag, outputType)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace name (defaults to client default)")
	cmd.Flags().StringVarP(&outputType, "output", "o", "json", "output format (json|text)")
	return cmd
}

func newNamespaceSetCommand(cfg *clientCLIConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Mutate namespace configuration",
	}
	cmd.AddCommand(newNamespaceSetQueryCommand(cfg))
	return cmd
}

func newNamespaceSetQueryCommand(cfg *clientCLIConfig) *cobra.Command {
	var namespace, preferred, fallback, ifMatch, outputType string
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Update namespace query engine preferences",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if strings.TrimSpace(preferred) == "" && strings.TrimSpace(fallback) == "" {
				return fmt.Errorf("at least one of --preferred or --fallback must be provided")
			}
			client, err := cfg.client()
			if err != nil {
				return err
			}
			defer cfg.cleanup()
			request := api.NamespaceConfigRequest{
				Namespace: namespace,
				Query: &api.NamespaceQueryConfig{
					PreferredEngine: preferred,
					FallbackEngine:  fallback,
				},
			}
			result, err := client.UpdateNamespaceConfig(ctx, request, lockdclient.NamespaceConfigOptions{IfMatch: ifMatch})
			if err != nil {
				return err
			}
			return printNamespaceConfig(cmd, result.Config, result.ETag, outputType)
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace name (defaults to client default)")
	cmd.Flags().StringVar(&preferred, "preferred", "", "preferred query engine (index or scan)")
	cmd.Flags().StringVar(&fallback, "fallback", "", "fallback query engine (scan or none)")
	cmd.Flags().StringVar(&ifMatch, "if-match", "", "ETag to enforce when updating (optional)")
	cmd.Flags().StringVarP(&outputType, "output", "o", "json", "output format (json|text)")
	return cmd
}

func printNamespaceConfig(cmd *cobra.Command, resp *api.NamespaceConfigResponse, etag string, outputType string) error {
	switch strings.ToLower(strings.TrimSpace(outputType)) {
	case "", "json":
		payload := struct {
			*api.NamespaceConfigResponse
			ETag string `json:"etag,omitempty"`
		}{
			NamespaceConfigResponse: resp,
			ETag:                    etag,
		}
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(payload)
	case "text":
		writer := cmd.OutOrStdout()
		fmt.Fprintf(writer, "Namespace: %s\n", resp.Namespace)
		fmt.Fprintf(writer, "Preferred query engine: %s\n", resp.Query.PreferredEngine)
		fmt.Fprintf(writer, "Fallback query engine: %s\n", resp.Query.FallbackEngine)
		if etag != "" {
			fmt.Fprintf(writer, "ETag: %s\n", etag)
		}
		return nil
	default:
		return fmt.Errorf("unsupported output format %q (expected json or text)", outputType)
	}
}
