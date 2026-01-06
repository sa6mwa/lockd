package storagecheck

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/storage"
	azurestore "pkt.systems/lockd/internal/storage/azure"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
)

func verifyAzure(ctx context.Context, azureCfg azurestore.Config, crypto *storage.Crypto) (Result, error) {
	store, err := azurestore.New(azureCfg)
	if err != nil {
		return Result{}, err
	}
	defer store.Close()

	service := store.Client()
	containerClient := service.ServiceClient().NewContainerClient(azureCfg.Container)

	endpoint := azureCfg.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf(lockd.DefaultAzureEndpointPattern, azureCfg.Account)
	}

	result := Result{
		Provider: "azure-blob",
		Bucket:   azureCfg.Container,
		Prefix:   strings.Trim(azureCfg.Prefix, "/"),
		Endpoint: endpoint,
		Credentials: lockd.CredentialSummary{
			AccessKey: azureCfg.Account,
			HasSecret: azureCfg.AccountKey != "" || azureCfg.SASToken != "",
			Source:    azureCredentialSource(azureCfg),
		},
	}

	run := func(name string, fn func(context.Context) error) {
		err := fn(ctx)
		result.Checks = append(result.Checks, CheckResult{Name: name, Err: err})
	}

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	run("ContainerExists", func(ctx context.Context) error {
		_, err := containerClient.GetProperties(ctx, nil)
		if err != nil {
			var respErr *azcore.ResponseError
			if errors.As(err, &respErr) && respErr.StatusCode == 404 {
				return fmt.Errorf("container %s does not exist", azureCfg.Container)
			}
			return err
		}
		return nil
	})

	run("ListBlobs", func(ctx context.Context) error {
		prefix := strings.Trim(result.Prefix, "/")
		if prefix != "" {
			prefix = path.Join(prefix, "meta")
		} else {
			prefix = "meta"
		}
		prefix = strings.Trim(prefix, "/") + "/"
		pager := service.NewListBlobsFlatPager(azureCfg.Container, &azblob.ListBlobsFlatOptions{
			MaxResults: toPtr[int32](1),
			Prefix:     toPtr(prefix),
		})
		if pager.More() {
			_, err := pager.NextPage(ctx)
			return err
		}
		return nil
	})

	namespace := namespaces.Default
	diagKey := path.Join("lockd-diagnostics", uuidv7.NewString())
	var (
		metaETag  string
		stateETag string
	)

	run("PutMeta", func(ctx context.Context) error {
		var err error
		meta := &storage.Meta{Version: 1}
	metaETag, err = store.StoreMeta(ctx, namespace, diagKey, meta, "")
		return err
	})

	run("GetMeta", func(ctx context.Context) error {
		_, err := store.LoadMeta(ctx, namespace, diagKey)
		return err
	})

	run("PutState", func(ctx context.Context) error {
	res, err := store.WriteState(ctx, namespace, diagKey, strings.NewReader("{}"), storage.PutStateOptions{})
		if err != nil {
			return err
		}
		if res != nil {
			stateETag = res.NewETag
		}
		return nil
	})

	if crypto != nil && crypto.Enabled() {
		run("CryptoMetaStateRoundTrip", func(ctx context.Context) error {
		return verifyMetaStateDecryption(ctx, store, crypto)
		})
		run("CryptoQueueRoundTrip", func(ctx context.Context) error {
		return verifyQueueEncryption(ctx, store, crypto)
		})
	}

	run("DeleteObjects", func(ctx context.Context) error {
	if err := store.Remove(ctx, namespace, diagKey, stateETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
	if err := store.DeleteMeta(ctx, namespace, diagKey, metaETag); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return nil
	})

	return result, nil
}

func azureCredentialSource(cfg azurestore.Config) string {
	switch {
	case cfg.SASToken != "":
		return "sas_token"
	case cfg.AccountKey != "":
		return "account_key"
	default:
		return ""
	}
}

func toPtr[T any](v T) *T {
	return &v
}
