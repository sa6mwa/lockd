package benchenv

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func ensureBenchmarkPrefix(tb testing.TB, store string) string {
	tb.Helper()
	u, err := url.Parse(store)
	if err != nil {
		tb.Fatalf("parse LOCKD_STORE: %v", err)
	}
	prefix := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	path := strings.Trim(strings.TrimPrefix(u.Path, "/"), "/")
	if path == "" {
		switch strings.ToLower(strings.TrimSpace(u.Scheme)) {
		case "aws", "s3":
			u.Path = "/" + prefix
			return u.String()
		default:
			return store
		}
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
		return store
	}
	u.Path = "/" + parts[0] + "/" + prefix
	return u.String()
}
