//go:build integration && azure

package azuretest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"pkt.systems/lockd"
	"pkt.systems/lockd/integration/internal/cryptotest"
	querydata "pkt.systems/lockd/integration/query/querydata"
	"pkt.systems/lockd/internal/storage"
	azurestore "pkt.systems/lockd/internal/storage/azure"
	locknamespaces "pkt.systems/lockd/namespaces"
)

var (
	cleanupMu   sync.Mutex
	cleanupDone = map[string]bool{}
)

// ResetContainerForCrypto removes leftover integration objects when storage encryption is enabled.
func ResetContainerForCrypto(tb testing.TB, cfg lockd.Config) {
	if os.Getenv(cryptotest.EnvVar) != "1" {
		return
	}
	key, err := cleanupScopeKey(cfg)
	if err != nil {
		tb.Fatalf("azure cleanup: %v", err)
	}
	if err := runScopedCleanupOnce(key, func() error {
		return cleanAzureContainer(cfg)
	}); err != nil {
		tb.Fatalf("azure cleanup: %v", err)
	}
}

func cleanupScopeKey(cfg lockd.Config) (string, error) {
	azureCfg, err := lockd.BuildAzureConfig(cfg)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/%s", azureCfg.Account, azureCfg.Container, strings.Trim(azureCfg.Prefix, "/")), nil
}

func runScopedCleanupOnce(key string, fn func() error) error {
	cleanupMu.Lock()
	if cleanupDone[key] {
		cleanupMu.Unlock()
		return nil
	}
	cleanupMu.Unlock()

	if err := fn(); err != nil {
		return err
	}

	cleanupMu.Lock()
	cleanupDone[key] = true
	cleanupMu.Unlock()
	return nil
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
	prefixes := cleanupBlobPrefixes(strings.Trim(azureCfg.Prefix, "/"))
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
				if _, err := client.DeleteBlob(ctx, azureCfg.Container, *item.Name, &deleteOpts); err != nil && !azureCleanupIgnoreDeleteError(err) {
					return err
				}
			}
		}
	}
	return nil
}

func cleanupBlobPrefixes(trimmedPrefix string) []*string {
	if trimmedPrefix == "" {
		return []*string{nil}
	}
	prefix := trimmedPrefix + "/"
	return []*string{&prefix}
}

func azureCleanupIgnoreDeleteError(err error) bool {
	if err == nil {
		return false
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		return respErr.StatusCode == http.StatusNotFound
	}
	return false
}

// CleanupQueryNamespaces removes seeded query documents for configured namespaces.
func CleanupQueryNamespaces(tb testing.TB, cfg lockd.Config) {
	tb.Helper()
	names := querydata.QueryNamespaces()
	if len(names) == 0 {
		return
	}
	azureCfg, err := lockd.BuildAzureConfig(cfg)
	if err != nil {
		tb.Fatalf("build azure config: %v", err)
	}
	store, err := azurestore.New(azureCfg)
	if err != nil {
		tb.Fatalf("new azure store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, ns := range names {
		cleanupNamespace(tb, store, ctx, ns)
	}
}

func cleanupNamespace(tb testing.TB, store storage.Backend, ctx context.Context, namespace string) {
	cleanupIndex(tb, store, ctx, namespace)
	removeStagingObjects(tb, store, ctx, namespace)
	keys, err := store.ListMetaKeys(ctx, namespace)
	if err != nil {
		tb.Logf("azure cleanup list meta (%s): %v", namespace, err)
		return
	}
	for _, key := range keys {
		if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Logf("azure cleanup state %s/%s: %v", namespace, key, err)
		}
		if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Logf("azure cleanup meta %s/%s: %v", namespace, key, err)
		}
	}
}

func cleanupIndex(tb testing.TB, store storage.Backend, ctx context.Context, namespace string) {
	listOpts := storage.ListOptions{Prefix: "index/", Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			tb.Logf("azure cleanup list index (%s): %v", namespace, err)
			return
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("azure cleanup index %s/%s: %v", namespace, obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		listOpts.StartAfter = res.NextStartAfter
	}
}

func removePrefix(tb testing.TB, store storage.Backend, ctx context.Context, namespace, prefix string) {
	listOpts := storage.ListOptions{Prefix: prefix, Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			tb.Logf("azure cleanup list prefix (%s/%s): %v", namespace, prefix, err)
			return
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("azure cleanup delete %s/%s: %v", namespace, obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		listOpts.StartAfter = res.NextStartAfter
	}
}

func removeStagingObjects(tb testing.TB, store storage.Backend, ctx context.Context, namespace string) {
	listOpts := storage.ListOptions{Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			tb.Logf("azure cleanup list staging (%s): %v", namespace, err)
			return
		}
		for _, obj := range res.Objects {
			if !storage.IsStagingObjectKey(obj.Key) {
				continue
			}
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("azure cleanup delete staging %s/%s: %v", namespace, obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		listOpts.StartAfter = res.NextStartAfter
	}
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
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup azure state %s: %v", key, err)
	}
	if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		tb.Fatalf("cleanup azure meta %s: %v", key, err)
	}
}

// CleanupNamespaceIndexes removes all index artifacts for the provided namespaces.
func CleanupNamespaceIndexes(tb testing.TB, cfg lockd.Config, namespaces ...string) {
	tb.Helper()
	if len(namespaces) == 0 {
		namespaces = []string{locknamespaces.Default}
	}
	azureCfg, err := lockd.BuildAzureConfig(cfg)
	if err != nil {
		tb.Fatalf("build azure config: %v", err)
	}
	store, err := azurestore.New(azureCfg)
	if err != nil {
		tb.Fatalf("new azure store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, ns := range namespaces {
		tb.Logf("azure cleanup: removing index artifacts for namespace %s", ns)
		cleanupIndex(tb, store, ctx, ns)
	}
}

// CleanupNamespaceIndexesWithStore removes index artifacts using an existing store.
func CleanupNamespaceIndexesWithStore(tb testing.TB, store storage.Backend, namespaces ...string) {
	tb.Helper()
	if store == nil {
		tb.Fatalf("cleanup azure indexes: store required")
	}
	if len(namespaces) == 0 {
		namespaces = []string{locknamespaces.Default}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, ns := range namespaces {
		tb.Logf("azure cleanup: removing index artifacts for namespace %s", ns)
		cleanupIndex(tb, store, ctx, ns)
	}
}
