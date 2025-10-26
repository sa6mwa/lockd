//go:build integration && aws

package awsintegration

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
)

func resetAWSBucketForCrypto(tb testing.TB, cfg lockd.Config) {
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
	if keys, err := store.ListMetaKeys(ctx); err == nil {
		for _, key := range keys {
			if !shouldCleanupAWSObject(key) {
				continue
			}
			if err := store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("aws cleanup state %q: %v", key, err)
			}
			if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("aws cleanup meta %q: %v", key, err)
			}
		}
	} else {
		tb.Logf("aws cleanup list meta: %v", err)
	}
}

func shouldCleanupAWSObject(key string) bool {
	key = strings.TrimPrefix(key, "/")
	prefixes := []string{
		"lockd-diagnostics/",
		"meta/aws-",
		"meta/crypto-aws-",
		"state/aws-",
		"state/crypto-aws-",
		"q/aws-",
		"q/crypto-aws-",
		"aws-",
		"crypto-aws-",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
