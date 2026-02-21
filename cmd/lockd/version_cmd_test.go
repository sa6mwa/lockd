package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"pkt.systems/pslog"
	"pkt.systems/version"
)

func executeRootCommand(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return stdout.String(), stderr.String(), err
}

func TestVersionCommandPrintsCurrentVersion(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")

	stdout, stderr, err := executeRootCommand(t, "version")
	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	want := version.Module() + " " + version.Current() + "\n"
	if stdout != want {
		t.Fatalf("unexpected stdout: got %q want %q", stdout, want)
	}
}

func TestVersionCommandVersionFlagPrintsCurrentVersion(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")

	stdout, stderr, err := executeRootCommand(t, "version", "--version")
	if err != nil {
		t.Fatalf("version --version failed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	want := version.Current() + "\n"
	if stdout != want {
		t.Fatalf("unexpected stdout: got %q want %q", stdout, want)
	}
}

func TestVersionCommandSemverFlagPrintsCurrentSemver(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")

	stdout, stderr, err := executeRootCommand(t, "version", "--semver")
	if err != nil {
		t.Fatalf("version --semver failed: %v", err)
	}
	if stderr != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	want := version.CurrentSemver() + "\n"
	if stdout != want {
		t.Fatalf("unexpected stdout: got %q want %q", stdout, want)
	}
}

func TestVersionCommandFlagsAreMutuallyExclusive(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")

	_, _, err := executeRootCommand(t, "version", "--version", "--semver")
	if err == nil {
		t.Fatal("expected error when both --version and --semver are set")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRootVersionFlagIsUnknown(t *testing.T) {
	t.Setenv("LOCKD_CONFIG", "")

	_, _, err := executeRootCommand(t, "--version")
	if err == nil {
		t.Fatal("expected unknown flag error for root --version")
	}
	if !strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("unexpected error: %v", err)
	}
}
