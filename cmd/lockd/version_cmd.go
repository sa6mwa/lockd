package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"pkt.systems/version"
)

func printModuleAndCurrentVersion(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%s %s\n", version.Module(), version.Current())
	return err
}

func printCurrentVersion(w io.Writer) error {
	_, err := fmt.Fprintln(w, version.Current())
	return err
}

func printCurrentSemver(w io.Writer) error {
	_, err := fmt.Fprintln(w, version.CurrentSemver())
	return err
}

func printRequestedVersion(w io.Writer, showVersion bool, showSemver bool) error {
	if showVersion && showSemver {
		return fmt.Errorf("--version and --semver are mutually exclusive")
	}
	if showSemver {
		return printCurrentSemver(w)
	}
	return printCurrentVersion(w)
}

func newVersionCommand() *cobra.Command {
	var showVersion bool
	var showSemver bool

	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the lockd version",
		RunE: func(cmd *cobra.Command, args []string) error {
			if showVersion || showSemver {
				return printRequestedVersion(cmd.OutOrStdout(), showVersion, showSemver)
			}
			return printModuleAndCurrentVersion(cmd.OutOrStdout())
		},
	}
	cmd.Flags().BoolVar(&showVersion, "version", false, "print lockd version only")
	cmd.Flags().BoolVar(&showSemver, "semver", false, "print lockd semantic version only")
	return cmd
}
