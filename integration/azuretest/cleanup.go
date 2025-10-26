//go:build integration && azure

package azuretest

import (
	"context"
	"errors"
	"os"
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
				logical := *item.Name
				if trimmed != "" {
					logical = strings.TrimPrefix(logical, trimmed+"/")
				}
				if !shouldCleanupAzureObject(logical) {
					continue
				}
				if _, err := client.DeleteBlob(ctx, azureCfg.Container, *item.Name, &deleteOpts); err != nil {
					return err
				}
			}
		}
	}
	if keys, err := store.ListMetaKeys(ctx); err == nil {
		for _, key := range keys {
			if !shouldCleanupAzureObject(key) {
				continue
			}
			if err := store.RemoveState(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			if err := store.DeleteMeta(ctx, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
		}
	} else {
		return err
	}
	return nil
}

func shouldCleanupAzureObject(name string) bool {
	name = strings.TrimPrefix(name, "/")
	prefixes := []string{
		"lockd-diagnostics/",
		"meta/azure-",
		"meta/crypto-azure-",
		"state/azure-",
		"state/crypto-azure-",
		"q/azure-",
		"q/crypto-azure-",
		"azure-",
		"crypto-azure-",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
