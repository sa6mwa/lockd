package search

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestQueryStreamingPathsDoNotUseReadAll(t *testing.T) {
	assertFilesDoNotContain(t, []string{
		"internal/search/scan/adapter.go",
		"internal/search/index/adapter.go",
		"internal/core/query.go",
		"internal/httpapi/handler.go",
	}, "io.ReadAll(")
}

func TestLoadStreamingPathsDoNotUseReadAll(t *testing.T) {
	assertFilesDoNotContain(t, []string{
		"internal/core/update.go",
		"internal/jsonutil/compact_writer.go",
	}, "io.ReadAll(")
}

func assertFilesDoNotContain(t *testing.T, files []string, needle string) {
	t.Helper()
	root := repoRootFromThisFile(t)
	for _, rel := range files {
		path := filepath.Join(root, rel)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		if strings.Contains(string(data), needle) {
			t.Fatalf("%s contains prohibited token %q", rel, needle)
		}
	}
}

func repoRootFromThisFile(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// internal/search -> internal -> repo root.
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
