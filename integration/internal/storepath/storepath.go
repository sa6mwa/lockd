package storepath

import (
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"

	"pkt.systems/lockd/internal/uuidv7"
)

var (
	runPrefixOnce sync.Once
	runPrefix     string
)

// RunPrefix returns a stable per-run prefix for integration suites.
func RunPrefix() string {
	runPrefixOnce.Do(func() {
		runPrefix = "it-" + uuidv7.NewString()
	})
	return runPrefix
}

// Append returns store with suffix appended to the URL path.
func Append(tb testing.TB, store, suffix string) string {
	tb.Helper()
	if suffix == "" {
		return store
	}
	parsed, err := url.Parse(store)
	if err != nil {
		tb.Fatalf("parse store %q: %v", store, err)
	}
	pathPart := strings.Trim(strings.TrimPrefix(parsed.Path, "/"), "/")
	if pathPart == "" {
		pathPart = suffix
	} else {
		pathPart = path.Join(pathPart, suffix)
	}
	parsed.Path = "/" + pathPart
	return parsed.String()
}

// Scoped returns store with a shared per-run prefix and optional scope.
func Scoped(tb testing.TB, store, scope string) string {
	tb.Helper()
	prefix := RunPrefix()
	if scope != "" {
		return Append(tb, store, path.Join(prefix, scope))
	}
	return Append(tb, store, prefix)
}
