package client

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLocalMutateStreamingPathDoesNotUseReadAll(t *testing.T) {
	assertLocalMutateFileDoesNotContain(t, "client/local_mutate.go", "io.ReadAll(")
}

func TestLocalMutateStreamingPathDoesNotUseBytesHelpers(t *testing.T) {
	assertLocalMutateFileDoesNotContain(t, "client/local_mutate.go", ".Bytes(")
}

func assertLocalMutateFileDoesNotContain(t *testing.T, rel, needle string) {
	t.Helper()
	root := localMutateRepoRootFromThisFile(t)
	data, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	if strings.Contains(string(data), needle) {
		t.Fatalf("%s contains prohibited token %q", rel, needle)
	}
}

func localMutateRepoRootFromThisFile(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), ".."))
}
