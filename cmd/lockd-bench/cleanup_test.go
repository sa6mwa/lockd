package main

import (
	"path/filepath"
	"testing"
)

func TestIsBenchTempRoot(t *testing.T) {
	root := filepath.Join(benchTempBase(), "root-123")
	if !isBenchTempRoot(root) {
		t.Fatalf("expected temp root detection for %s", root)
	}

	other := t.TempDir()
	if isBenchTempRoot(other) {
		t.Fatalf("did not expect temp root detection for %s", other)
	}
}

func TestShouldSkipDiskCleanup(t *testing.T) {
	cfg := benchConfig{backend: "disk"}
	root := filepath.Join(benchTempBase(), "root-123")
	if !shouldSkipDiskCleanup(cfg, root) {
		t.Fatalf("expected cleanup skip for temp root")
	}
	cfg.keepRoot = true
	if shouldSkipDiskCleanup(cfg, root) {
		t.Fatalf("did not expect cleanup skip when keepRoot=true")
	}
}
