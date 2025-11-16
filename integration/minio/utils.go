//go:build integration && minio

package miniointegration

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	querydata "pkt.systems/lockd/integration/query/querydata"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/namespaces"
)

var minioStorageMu sync.Mutex

// WithMinioStorageLock ensures MinIO storage cleanup and verification steps run serially.
func WithMinioStorageLock(tb testing.TB, fn func()) {
	tb.Helper()
	minioStorageMu.Lock()
	defer minioStorageMu.Unlock()
	fn()
}

// ResetMinioBucketForCrypto removes leftover diagnostics artifacts when storage
// encryption is enabled. This keeps verification checks from failing while
// iterating existing metadata.
func ResetMinioBucketForCrypto(tb testing.TB, cfg lockd.Config) {
	tb.Helper()
	if os.Getenv(cryptotest.EnvVar) != "1" {
		return
	}
	minioCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new minio store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := store.Client()
	listPrefix := strings.Trim(minioCfg.Prefix, "/")
	listOpts := minio.ListObjectsOptions{Recursive: true}
	if listPrefix != "" {
		listOpts.Prefix = listPrefix
	}
	for obj := range client.ListObjects(ctx, minioCfg.Bucket, listOpts) {
		if obj.Err != nil {
			tb.Logf("minio cleanup list objects error: %v", obj.Err)
			continue
		}
		logicalKey := obj.Key
		if listPrefix != "" {
			logicalKey = strings.TrimPrefix(logicalKey, listPrefix+"/")
		}
		if !shouldCleanupMinioObject(logicalKey) {
			continue
		}
		if err := client.RemoveObject(ctx, minioCfg.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			tb.Logf("minio cleanup remove object %q: %v", obj.Key, err)
		}
	}
	if keys, err := store.ListMetaKeys(ctx, namespaces.Default); err == nil {
		for _, key := range keys {
			if err := store.Remove(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("minio cleanup state %q: %v", key, err)
			}
			if err := store.DeleteMeta(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("minio cleanup meta %q: %v", key, err)
			}
		}
	} else {
		tb.Logf("minio cleanup list meta keys: %v", err)
	}
}

// CleanupQueryNamespaces removes query fixtures for all namespaces the suite depends on.
func CleanupQueryNamespaces(tb testing.TB, cfg lockd.Config) {
	CleanupNamespaces(tb, cfg, querydata.QueryNamespaces()...)
}

// CleanupNamespaces removes all state/meta/index objects for the provided namespaces.
func CleanupNamespaces(tb testing.TB, cfg lockd.Config, namespaces ...string) {
	tb.Helper()
	if len(namespaces) == 0 {
		return
	}
	minioCfg, _, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new minio store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, ns := range namespaces {
		cleanupMinioNamespace(tb, store, ctx, ns)
	}
}

func cleanupMinioNamespace(tb testing.TB, store storage.Backend, ctx context.Context, namespace string) {
	tb.Logf("minio cleanup: sweeping namespace %s", namespace)
	cleanupMinioIndex(tb, store, ctx, namespace)
	keys, err := store.ListMetaKeys(ctx, namespace)
	if err != nil {
		tb.Logf("minio cleanup list meta (%s): %v", namespace, err)
		return
	}
	tb.Logf("minio cleanup: removing %d meta keys from %s", len(keys), namespace)
	for _, key := range keys {
		if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Logf("minio cleanup state %s/%s: %v", namespace, key, err)
		}
		if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Logf("minio cleanup meta %s/%s: %v", namespace, key, err)
		}
	}
}

func cleanupMinioIndex(tb testing.TB, store storage.Backend, ctx context.Context, namespace string) {
	listOpts := storage.ListOptions{Prefix: "index/", Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			tb.Logf("minio cleanup list index (%s): %v", namespace, err)
			return
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("minio cleanup index %s/%s: %v", namespace, obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		listOpts.StartAfter = res.NextStartAfter
	}
}

func shouldCleanupMinioObject(key string) bool {
	key = strings.TrimPrefix(key, "/")
	if key == "" {
		return false
	}

	namespaceAwarePrefixes := []string{
		"lockd-diagnostics/",
		"meta/minio-",
		"meta/crypto-minio-",
		"state/minio-",
		"state/crypto-minio-",
		"q/minio-",
		"q/crypto-minio-",
	}
	namespaceAgnosticPrefixes := []string{
		"minio-",
		"crypto-minio-",
	}

	parts := strings.SplitN(key, "/", 2)
	rest := key
	if len(parts) == 2 {
		rest = parts[1]
	}

	for _, prefix := range namespaceAwarePrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
		if strings.HasPrefix(rest, prefix) {
			return true
		}
	}
	for _, prefix := range namespaceAgnosticPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
