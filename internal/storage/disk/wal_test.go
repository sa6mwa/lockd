package disk

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALReplayAppliesWrites(t *testing.T) {
	root := t.TempDir()
	store, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	wal, err := store.walForNamespace("default")
	if err != nil {
		t.Fatalf("wal init: %v", err)
	}
	if wal == nil {
		t.Fatalf("wal disabled unexpectedly")
	}

	rel := filepath.ToSlash(filepath.Join("meta", "test.pb"))
	if _, err := wal.append([]walEntry{{op: walOpWrite, path: rel, data: []byte("hello")}}); err != nil {
		t.Fatalf("wal append: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store2, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("new store2: %v", err)
	}
	defer store2.Close()

	if _, err := store2.walForNamespace("default"); err != nil {
		t.Fatalf("wal recover: %v", err)
	}
	metaPath := filepath.Join(root, "default", "meta", "test.pb")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("unexpected payload: %q", string(data))
	}
}
