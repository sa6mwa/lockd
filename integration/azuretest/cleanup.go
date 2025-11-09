//go:build integration && azure

package azuretest

import (
	"context"
	"errors"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/storage"
	azurestore "pkt.systems/lockd/internal/storage/azure"
	"pkt.systems/lockd/namespaces"
)

var (
	cleanupOnce sync.Once
	cleanupErr  error
)

// ResetContainerForCrypto removes leftover integration objects when storage encryption is enabled.
func ResetContainerForCrypto(tb testing.TB, cfg lockd.Config) {
	if os.Getenv(cryptotest.EnvVar) != "1" {
		return
	}
	cleanupOnce.Do(func() {
		cleanupErr = cleanAzureContainer(cfg)
	})
	if cleanupErr != nil {
		tb.Fatalf("azure cleanup: %v", cleanupErr)
	}
}

func cleanAzureContainer(cfg lockd.Config) error {
	azureCfg, err := lockd.BuildAzureConfig(cfg)
	if err != nil {
		return err
	}
	store, err := azurestore.New(azureCfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client := store.Client()
	trimmed := strings.Trim(azureCfg.Prefix, "/")
	var prefixes []*string
	if trimmed != "" {
		pref := trimmed + "/"
		prefixes = append(prefixes, &pref)
	}
	prefixes = append(prefixes, nil)
	deleteSnapshots := azblob.DeleteSnapshotsOptionTypeInclude
	deleteOpts := azblob.DeleteBlobOptions{DeleteSnapshots: to.Ptr(deleteSnapshots)}
	for _, prefix := range prefixes {
		pager := client.NewListBlobsFlatPager(azureCfg.Container, &azblob.ListBlobsFlatOptions{Prefix: prefix})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return err
			}
			for _, item := range page.Segment.BlobItems {
				if item.Name == nil {
					continue
				}
				if _, err := client.DeleteBlob(ctx, azureCfg.Container, *item.Name, &deleteOpts); err != nil {
					return err
				}
			}
		}
	}
	if keys, err := store.ListMetaKeys(ctx, namespaces.Default); err == nil {
		for _, key := range keys {
			if err := store.Remove(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			if err := store.DeleteMeta(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		}
	} else {
		return err
	}
	return nil
}

// CleanupQueue removes objects and metadata for the given queue within the namespace.
func CleanupQueue(tb testing.TB, cfg lockd.Config, namespace, queue string) {
	tb.Helper()
	azureCfg, err := lockd.BuildAzureConfig(cfg)
	if err != nil {
		tb.Fatalf("build azure config: %v", err)
	}
	store, err := azurestore.New(azureCfg)
	if err != nil {
		tb.Fatalf("new azure store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	listOpts := storage.ListOptions{Prefix: path.Join("q", queue) + "/", Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			tb.Fatalf("list azure queue objects: %v", err)
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Fatalf("delete azure queue object %s: %v", obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		listOpts.StartAfter = res.NextStartAfter
	}

	keys, err := store.ListMetaKeys(ctx, namespace)
	if err != nil {
		tb.Fatalf("list azure queue meta keys: %v", err)
	}
	prefix := path.Join("q", queue)
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Fatalf("cleanup azure queue state %s: %v", key, err)
		}
		if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Fatalf("cleanup azure queue meta %s: %v", key, err)
		}
	}
}

// CleanupKey removes meta and state objects for the provided logical key.
func CleanupKey(tb testing.TB, cfg lockd.Config, namespace, key string) {
	tb.Helper()
	azureCfg, err := lockd.BuildAzureConfig(cfg)
	if err != nil {
		tb.Fatalf("build azure config: %v", err)
	}
	store, err := azurestore.New(azureCfg)
	if err != nil {
		tb.Fatalf("new azure store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup azure state %s: %v", key, err)
	}
	if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup azure meta %s: %v", key, err)
	}
}
