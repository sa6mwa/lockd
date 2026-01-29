//go:build bench

package benchenv

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/storage"
	awsstore "pkt.systems/lockd/internal/storage/aws"
	"pkt.systems/lockd/internal/storage/azure"
	"pkt.systems/lockd/internal/storage/s3"
)

// CleanupBenchmarkPrefix deletes all objects under the benchmark prefix.
// Safety: skips cleanup unless the store prefix contains a "bench-" segment.
func CleanupBenchmarkPrefix(tb testing.TB, cfg lockd.Config) {
	tb.Helper()
	store := strings.TrimSpace(cfg.Store)
	if store == "" {
		return
	}
	u, err := url.Parse(store)
	if err != nil {
		tb.Logf("bench cleanup: parse store: %v", err)
		return
	}
	switch strings.ToLower(strings.TrimSpace(u.Scheme)) {
	case "aws":
		awsResult, err := lockd.BuildAWSConfig(cfg)
		if err != nil {
			tb.Logf("bench cleanup: build aws config: %v", err)
			return
		}
		prefix := strings.Trim(awsResult.Config.Prefix, "/")
		if !containsBenchPrefix(prefix) {
			tb.Logf("bench cleanup: skip aws (prefix %q)", awsResult.Config.Prefix)
			return
		}
		store, err := awsstore.New(awsResult.Config)
		if err != nil {
			tb.Logf("bench cleanup: init aws store: %v", err)
			return
		}
		defer store.Close()
		cleanupBackendPrefix(tb, store, prefix)
	case "s3":
		s3Result, err := lockd.BuildGenericS3Config(cfg)
		if err != nil {
			tb.Logf("bench cleanup: build s3 config: %v", err)
			return
		}
		prefix := strings.Trim(s3Result.Config.Prefix, "/")
		if !containsBenchPrefix(prefix) {
			tb.Logf("bench cleanup: skip s3 (prefix %q)", s3Result.Config.Prefix)
			return
		}
		store, err := s3.New(s3Result.Config)
		if err != nil {
			tb.Logf("bench cleanup: init s3 store: %v", err)
			return
		}
		defer store.Close()
		cleanupBackendPrefix(tb, store, prefix)
	case "azure":
		azureCfg, err := lockd.BuildAzureConfig(cfg)
		if err != nil {
			tb.Logf("bench cleanup: build azure config: %v", err)
			return
		}
		prefix := strings.Trim(azureCfg.Prefix, "/")
		if !containsBenchPrefix(prefix) {
			tb.Logf("bench cleanup: skip azure (prefix %q)", azureCfg.Prefix)
			return
		}
		store, err := azure.New(azureCfg)
		if err != nil {
			tb.Logf("bench cleanup: init azure store: %v", err)
			return
		}
		defer store.Close()
		cleanupBackendPrefix(tb, store, prefix)
	default:
		tb.Logf("bench cleanup: unsupported scheme %q", u.Scheme)
	}
}

func cleanupBackendPrefix(tb testing.TB, store storage.Backend, prefix string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var removed int
	opts := storage.ListOptions{Prefix: "", Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, "", opts)
		if err != nil {
			tb.Logf("bench cleanup: list objects: %v", err)
			return
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, "", obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("bench cleanup: delete %q: %v", obj.Key, err)
				continue
			}
			removed++
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		opts.StartAfter = res.NextStartAfter
	}
	tb.Logf("bench cleanup: removed %d objects under prefix %q", removed, prefix)
}

func containsBenchPrefix(prefix string) bool {
	for _, part := range strings.Split(strings.Trim(prefix, "/"), "/") {
		if strings.HasPrefix(part, "bench-") {
			return true
		}
	}
	return false
}
