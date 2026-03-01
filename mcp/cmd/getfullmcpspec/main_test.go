package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrettyJSONL(t *testing.T) {
	t.Parallel()

	in := []byte("{\"kind\":\"one\",\"n\":1}\n{\"kind\":\"two\",\"n\":2}\n")
	out, err := prettyJSONL(in)
	if err != nil {
		t.Fatalf("prettyJSONL: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "\n  \"kind\": \"one\",\n") {
		t.Fatalf("expected indented first record, got: %q", got)
	}
	if !strings.Contains(got, "\n  \"kind\": \"two\",\n") {
		t.Fatalf("expected indented second record, got: %q", got)
	}
}

func TestPrettyJSONLInvalidLine(t *testing.T) {
	t.Parallel()

	_, err := prettyJSONL([]byte("{\"ok\":1}\n{bad-json}\n"))
	if err == nil {
		t.Fatalf("expected invalid json error")
	}
	if !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("expected line number in error, got: %v", err)
	}
}

func TestWriteOutputToFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	outPath := filepath.Join(dir, "mcp.jsonl")
	payload := []byte("{\"ok\":true}\n")
	if err := writeOutput(outPath, payload); err != nil {
		t.Fatalf("writeOutput: %v", err)
	}
	got, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected output file content: got %q want %q", string(got), string(payload))
	}
}
