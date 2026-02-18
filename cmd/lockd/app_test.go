package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"pkt.systems/pslog"
)

func TestInvocationTargetsRootCommand(t *testing.T) {
	root := newRootCommand(pslog.NewStructured(io.Discard))
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
	root := newRootCommand(pslog.NewStructured(io.Discard))
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
	root := newRootCommand(pslog.NewStructured(io.Discard))
	if flag := root.Flags().Lookup("bootstrap"); flag == nil {
		t.Fatalf("expected --bootstrap on root local flags")
	}
	if flag := root.PersistentFlags().Lookup("bootstrap"); flag != nil {
		t.Fatalf("expected --bootstrap to not be persistent, got %#v", flag)
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
