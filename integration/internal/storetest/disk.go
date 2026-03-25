package storetest

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pkt.systems/lockd/internal/uuidv7"
)

// RequireDiskStoreRoot parses LOCKD_STORE and returns the backing filesystem root.
func RequireDiskStoreRoot(tb testing.TB, suite string) string {
	tb.Helper()
	store := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
	if store == "" {
		tb.Fatalf("LOCKD_STORE must be set to disk:///absolute/path (source .env.%s before running %s)", suite, suite)
	}
	root, err := DiskStoreRoot(store)
	if err != nil {
		tb.Fatalf("invalid LOCKD_STORE for %s: %v", suite, err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		tb.Fatalf("LOCKD_STORE root %q unavailable for %s: %v", root, suite, err)
	}
	return root
}

// PrepareDiskStoreSubdir allocates a temporary subdirectory beneath the disk store root.
func PrepareDiskStoreSubdir(tb testing.TB, suite, base, prefix string) string {
	tb.Helper()
	if base == "" {
		base = RequireDiskStoreRoot(tb, suite)
	} else {
		info, err := os.Stat(base)
		if err != nil || !info.IsDir() {
			tb.Fatalf("disk base %q unavailable for %s: %v", base, suite, err)
		}
	}
	root := filepath.Join(base, prefix+"-"+uuidv7.NewString())
	if err := os.MkdirAll(root, 0o755); err != nil {
		tb.Fatalf("mkdir disk root %q: %v", root, err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

// DiskStoreRoot parses a disk:// store URL into an absolute filesystem root.
func DiskStoreRoot(store string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(store))
	if err != nil {
		return "", fmt.Errorf("parse LOCKD_STORE: %w", err)
	}
	if !strings.EqualFold(parsed.Scheme, "disk") {
		return "", fmt.Errorf("expected disk:// store URL, got %q", parsed.Scheme)
	}
	if parsed.Host != "" {
		return "", fmt.Errorf("expected disk:///absolute/path, found host %q", parsed.Host)
	}
	root := filepath.Clean(parsed.Path)
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("expected absolute disk store path, got %q", parsed.Path)
	}
	return root, nil
}

// DiskStoreURL returns a normalized disk:// URL for the provided absolute path.
func DiskStoreURL(root string) string {
	clean := filepath.Clean(root)
	if !filepath.IsAbs(clean) {
		clean = "/" + strings.TrimPrefix(clean, "/")
	}
	return (&url.URL{Scheme: "disk", Path: clean}).String()
}
