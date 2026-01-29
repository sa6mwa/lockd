package disk

import (
	"path/filepath"
	"testing"
)

func TestLockFileCacheEvictsIdle(t *testing.T) {
	dir := t.TempDir()
	cache := newLockFileCache(1)
	if cache == nil {
		t.Fatal("expected cache")
	}
	pathA := filepath.Join(dir, "a.lock")
	pathB := filepath.Join(dir, "b.lock")

	entryA, err := cache.acquire(pathA)
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	cache.release(entryA)

	entryB, err := cache.acquire(pathB)
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	cache.release(entryB)

	if len(cache.entries) != 1 {
		t.Fatalf("expected 1 cached entry, got %d", len(cache.entries))
	}
	if _, ok := cache.entries[pathB]; !ok {
		t.Fatalf("expected path %q to remain in cache", pathB)
	}
	cache.close()
}
