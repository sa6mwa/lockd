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
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
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
		if err := client.RemoveObject(ctx, minioCfg.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			tb.Logf("minio cleanup remove object %q: %v", obj.Key, err)
		}
	}
	if keys, err := store.ListMetaKeys(ctx); err == nil {
		for _, key := range keys {
			if err := store.Remove(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("minio cleanup state %q: %v", key, err)
			}
			if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("minio cleanup meta %q: %v", key, err)
			}
		}
	} else {
		tb.Logf("minio cleanup list meta keys: %v", err)
	}
}
