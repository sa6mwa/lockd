//go:build integration && aws

package awsintegration

import (
	"context"
	"errors"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/namespaces"
)

// ResetAWSBucketForCrypto clears cryptotest buckets when storage encryption is enabled.
func ResetAWSBucketForCrypto(tb testing.TB, cfg lockd.Config) {
	if os.Getenv(cryptotest.EnvVar) != "1" {
		return
	}
	s3cfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client := store.Client()
	listPrefix := strings.Trim(s3cfg.Prefix, "/")
	opts := minio.ListObjectsOptions{Recursive: true}
	if listPrefix != "" {
		opts.Prefix = listPrefix
	}
	for obj := range client.ListObjects(ctx, s3cfg.Bucket, opts) {
		if obj.Err != nil {
			tb.Logf("aws cleanup list error: %v", obj.Err)
			continue
		}
		logicalKey := obj.Key
		if listPrefix != "" {
			logicalKey = strings.TrimPrefix(logicalKey, listPrefix+"/")
		}
		if !shouldCleanupAWSObject(logicalKey) {
			continue
		}
		if err := client.RemoveObject(ctx, s3cfg.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			tb.Logf("aws cleanup remove %q: %v", obj.Key, err)
		}
	}
	if keys, err := store.ListMetaKeys(ctx, namespaces.Default); err == nil {
		for _, key := range keys {
			if !shouldCleanupAWSObject(key) {
				continue
			}
			if err := store.Remove(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("aws cleanup state %q: %v", key, err)
			}
			if err := store.DeleteMeta(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("aws cleanup meta %q: %v", key, err)
			}
		}
	} else {
		tb.Logf("aws cleanup list meta: %v", err)
	}
}

func shouldCleanupAWSObject(key string) bool {
	key = strings.TrimPrefix(key, "/")
	if key == "" {
		return false
	}

	namespaceAwarePrefixes := []string{
		"lockd-diagnostics/",
		"meta/aws-",
		"meta/crypto-aws-",
		"state/aws-",
		"state/crypto-aws-",
		"q/aws-",
		"q/crypto-aws-",
	}
	namespaceAgnosticPrefixes := []string{
		"aws-",
		"crypto-aws-",
	}

	segments := strings.SplitN(key, "/", 2)
	rest := key
	if len(segments) == 2 {
		rest = segments[1]
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

// CleanupQueue removes queue blobs and metadata for the given queue.
func CleanupQueue(tb testing.TB, cfg lockd.Config, namespace, queue string) {
	tb.Helper()
	s3cfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	prefix := path.Join("q", queue) + "/"
	listOpts := storage.ListOptions{Prefix: prefix, Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			tb.Fatalf("list aws queue objects: %v", err)
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Fatalf("delete aws queue object %s: %v", obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		listOpts.StartAfter = res.NextStartAfter
	}

	keys, err := store.ListMetaKeys(ctx, namespace)
	if err != nil {
		tb.Fatalf("list aws queue meta keys: %v", err)
	}
	metaPrefix := path.Join("q", queue)
	for _, key := range keys {
		if !strings.HasPrefix(key, metaPrefix) {
			continue
		}
		if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Fatalf("cleanup aws queue state %s: %v", key, err)
		}
		if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Fatalf("cleanup aws queue meta %s: %v", key, err)
		}
	}
}

// CleanupKey removes state and metadata for a lock key.
func CleanupKey(tb testing.TB, cfg lockd.Config, namespace, key string) {
	tb.Helper()
	s3cfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup aws state %s: %v", key, err)
	}
	if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup aws meta %s: %v", key, err)
	}
}
