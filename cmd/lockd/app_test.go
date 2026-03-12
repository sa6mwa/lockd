package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"pkt.systems/pslog"
	"pkt.systems/version"
)

func TestInvocationTargetsRootCommand(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{name: "no args", args: nil, want: true},
		{name: "root flag only", args: []string{"--store", "mem://"}, want: true},
		{name: "root shorthand with value", args: []string{"-c", "/tmp/cfg.yaml"}, want: true},
		{name: "global client shorthand with value", args: []string{"-b", "/tmp/client.pem"}, want: true},
		{name: "subcommand", args: []string{"namespace", "get"}, want: false},
		{name: "subcommand alias", args: []string{"ns", "get"}, want: false},
		{name: "subcommand after root flag", args: []string{"--config", "/tmp/cfg.yaml", "namespace", "get"}, want: false},
		{name: "unknown shorthand no subcommand", args: []string{"-z"}, want: true},
		{name: "unknown shorthand before subcommand", args: []string{"-z", "namespace", "get"}, want: false},
		{name: "unknown long before subcommand", args: []string{"--bogus", "namespace", "get"}, want: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := invocationTargetsRootCommand(root, tc.args)
			if got != tc.want {
				t.Fatalf("invocationTargetsRootCommand(%v)=%v want %v", tc.args, got, tc.want)
			}
		})
	}
}

func TestSubmainInvalidFlagLikeTokenBeforeSubcommand(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()
	os.Args = []string{"lockd", "-z", "namespace", "get"}

	stderr := captureStderr(t, func() {
		exitCode := submain(context.Background())
		if exitCode != 1 {
			t.Fatalf("submain() exitCode=%d want 1", exitCode)
		}
	})
	if !strings.Contains(stderr, `unknown command "get" for "lockd"`) {
		t.Fatalf("expected parser failure routed to stderr, got %q", stderr)
	}
}

func TestRootHasGlobalClientShorthands(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	if flag := root.PersistentFlags().ShorthandLookup("v"); flag == nil || flag.Name != "verbose" {
		t.Fatalf("expected global -v shorthand for --verbose, got %#v", flag)
	}
	if flag := root.PersistentFlags().ShorthandLookup("b"); flag == nil || flag.Name != "bundle" {
		t.Fatalf("expected global -b shorthand for --bundle, got %#v", flag)
	}
	if flag := root.PersistentFlags().ShorthandLookup("s"); flag == nil || flag.Name != "server" {
		t.Fatalf("expected global -s shorthand for --server, got %#v", flag)
	}
}

func TestBootstrapFlagIsRootOnly(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	if flag := root.Flags().Lookup("bootstrap"); flag == nil {
		t.Fatalf("expected --bootstrap on root local flags")
	}
	if flag := root.PersistentFlags().Lookup("bootstrap"); flag != nil {
		t.Fatalf("expected --bootstrap to not be persistent, got %#v", flag)
	}
}

func TestRootMissingConfigFailsWithoutBootstrap(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("LOCKD_CONFIG_DIR", t.TempDir())

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	root.SetArgs([]string{
		"--config", cfgPath,
		"--store", "invalid://store",
	})
	err := root.Execute()
	if err == nil {
		t.Fatalf("expected missing config to fail without bootstrap")
	}
	if !strings.Contains(err.Error(), "config file") || !strings.Contains(err.Error(), "no such file or directory") {
		t.Fatalf("expected missing config error, got %v", err)
	}
}

func TestRootBootstrapCreatesMissingConfigForExplicitConfigPath(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("LOCKD_CONFIG_DIR", t.TempDir())

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	configDir := t.TempDir()
	bootstrapDir := t.TempDir()
	cfgPath := filepath.Join(configDir, "config.yaml")
	root.SetArgs([]string{
		"--config", cfgPath,
		"--bootstrap", bootstrapDir,
		"--store", "invalid://store",
	})
	err := root.Execute()
	if err == nil {
		t.Fatalf("expected invalid store error after bootstrap")
	}
	if strings.Contains(err.Error(), "config file") && strings.Contains(err.Error(), "no such file or directory") {
		t.Fatalf("bootstrap should have created config file, got %v", err)
	}
	if _, statErr := os.Stat(cfgPath); statErr != nil {
		t.Fatalf("expected bootstrap config at %s: %v", cfgPath, statErr)
	}
	if _, statErr := os.Stat(filepath.Join(bootstrapDir, "config.yaml")); !os.IsNotExist(statErr) {
		t.Fatalf("expected bootstrap to use explicit --config path, but found bootstrap-dir config: %v", statErr)
	}
}

func TestRootBootstrapCreatesMissingConfigWhenLOCKD_CONFIGIsSet(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("LOCKD_CONFIG_DIR", t.TempDir())

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	t.Setenv("LOCKD_CONFIG", cfgPath)

	root := newRootCommand(pslog.NewStructured(context.Background(), io.Discard))
	root.SetArgs([]string{
		"--bootstrap", dir,
		"--store", "invalid://store",
	})
	err := root.Execute()
	if err == nil {
		t.Fatalf("expected invalid store error after bootstrap")
	}
	if strings.Contains(err.Error(), "config file") && strings.Contains(err.Error(), "no such file or directory") {
		t.Fatalf("bootstrap should have created LOCKD_CONFIG file, got %v", err)
	}
	if _, statErr := os.Stat(cfgPath); statErr != nil {
		t.Fatalf("expected bootstrap config at %s: %v", cfgPath, statErr)
	}
}

func TestRootStartupLogIncludesVersion(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("LOCKD_CONFIG", "")

	var buf bytes.Buffer
	root := newRootCommand(pslog.NewStructured(context.Background(), &buf))
	root.SetArgs([]string{"--store", "invalid://store"})
	err := root.Execute()
	if err == nil {
		t.Fatalf("expected invalid store error")
	}

	output := buf.String()
	wantMsg := `"msg":"welcome to lockd ` + version.Current() + `"`
	if !strings.Contains(output, wantMsg) {
		t.Fatalf("expected startup log message %q in %q", wantMsg, output)
	}
	wantField := `"version":"` + version.Current() + `"`
	if !strings.Contains(output, wantField) {
		t.Fatalf("expected startup log version field %q in %q", wantField, output)
	}
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer r.Close()
	os.Stderr = w
	defer func() {
		os.Stderr = orig
	}()

	done := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(r)
		done <- string(data)
	}()

	fn()
	_ = w.Close()
	return <-done
}
