package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"pkt.systems/kryptograf/keymgmt"

	lockd "pkt.systems/lockd"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/cryptoutil"
	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"

	"github.com/spf13/cobra"
)

func newIndexRebuildCommand() *cobra.Command {
	var all bool
	var cleanup bool
	var cleanupDelay time.Duration
	cmd := &cobra.Command{
		Use:          "rebuild",
		Short:        "Rebuild namespace indexes (server-side)",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			namespace, _ := cmd.Flags().GetString("namespace")
			if strings.TrimSpace(namespace) == "" && !all {
				return fmt.Errorf("namespace required (use --namespace or --all)")
			}
			if strings.TrimSpace(namespace) != "" && all {
				return fmt.Errorf("specify either --namespace or --all")
			}

			var cfg lockd.Config
			if err := bindConfig(&cfg); err != nil {
				return err
			}
			if err := cfg.Validate(); err != nil {
				return err
			}
			crypto, err := prepareStorageCrypto(&cfg)
			if err != nil {
				return err
			}
			backend, err := lockd.OpenBackend(cfg, crypto)
			if err != nil {
				return err
			}
			defer backend.Close()

			indexStore := indexer.NewStore(backend, crypto)
			indexManager := indexer.NewManager(indexStore, indexer.WriterOptions{
				FlushDocs:     cfg.IndexerFlushDocs,
				FlushInterval: cfg.IndexerFlushInterval,
				Clock:         clock.Real{},
				Logger:        pslog.NoopLogger(),
			})

			defaultCfg := namespaces.DefaultConfig()
			if provider, ok := backend.(namespaces.ConfigProvider); ok && provider != nil {
				defaultCfg = provider.DefaultNamespaceConfig()
			}
			svc := core.New(core.Config{
				Store:                  backend,
				Crypto:                 crypto,
				IndexManager:           indexManager,
				DefaultNamespace:       cfg.DefaultNamespace,
				DefaultNamespaceConfig: defaultCfg,
				JSONMaxBytes:           cfg.JSONMaxBytes,
				HAMode:                 "failover",
				HALeaseTTL:             -1,
				Clock:                  clock.Real{},
				Logger:                 pslog.NoopLogger(),
			})

			ctx := cmd.Context()
			names, err := resolveRebuildNamespaces(ctx, backend, namespace, all)
			if err != nil {
				return err
			}

			for _, ns := range names {
				res, err := svc.RebuildIndex(ctx, ns, core.IndexRebuildOptions{
					Mode:         "wait",
					Cleanup:      cleanup,
					CleanupDelay: cleanupDelay,
				})
				if err != nil {
					return fmt.Errorf("index rebuild %s: %w", ns, err)
				}
				fmt.Fprintf(cmd.OutOrStdout(), "namespace=%s rebuilt=%t index_seq=%d\n", res.Namespace, res.Rebuilt, res.IndexSeq)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "rebuild all namespaces (when supported by backend)")
	cmd.Flags().BoolVar(&cleanup, "cleanup", true, "delete legacy index segments after rebuild")
	cmd.Flags().DurationVar(&cleanupDelay, "cleanup-delay", 30*time.Second, "delay before cleaning legacy segments (0 for immediate)")
	return cmd
}

func resolveRebuildNamespaces(ctx context.Context, backend storage.Backend, namespace string, all bool) ([]string, error) {
	if !all {
		return []string{strings.TrimSpace(namespace)}, nil
	}
	lister, ok := backend.(storage.NamespaceLister)
	if !ok {
		return nil, fmt.Errorf("backend does not support namespace listing; use --namespace")
	}
	names, err := lister.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no namespaces found")
	}
	sort.Strings(names)
	return names, nil
}

func prepareStorageCrypto(cfg *lockd.Config) (*storage.Crypto, error) {
	if cfg == nil || !cfg.StorageEncryptionEnabled() {
		return nil, nil
	}
	root := cfg.MetadataRootKey
	desc := cfg.MetadataDescriptor
	contextID := strings.TrimSpace(cfg.MetadataContext)
	if root == (keymgmt.RootKey{}) || desc == (keymgmt.Descriptor{}) || contextID == "" {
		var bundle *tlsutil.Bundle
		var err error
		switch {
		case len(cfg.BundlePEM) > 0:
			bundle, err = tlsutil.LoadBundleFromBytes(cfg.BundlePEM)
		default:
			bundle, err = tlsutil.LoadBundle(cfg.BundlePath, cfg.DenylistPath)
		}
		if err != nil {
			return nil, err
		}
		if bundle.MetadataRootKey == (keymgmt.RootKey{}) {
			return nil, fmt.Errorf("config: server bundle missing kryptograf root key (reissue with 'lockd auth new server')")
		}
		if bundle.MetadataDescriptor == (keymgmt.Descriptor{}) {
			return nil, fmt.Errorf("config: server bundle missing metadata descriptor (reissue with 'lockd auth new server')")
		}
		caID, err := cryptoutil.CACertificateID(bundle.CACertPEM)
		if err != nil {
			return nil, fmt.Errorf("config: derive ca id: %w", err)
		}
		root = bundle.MetadataRootKey
		desc = bundle.MetadataDescriptor
		contextID = caID
	}
	if root == (keymgmt.RootKey{}) || desc == (keymgmt.Descriptor{}) || contextID == "" {
		return nil, fmt.Errorf("config: metadata crypto material missing")
	}
	cfg.MetadataRootKey = root
	cfg.MetadataDescriptor = desc
	cfg.MetadataContext = contextID
	return storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: desc,
		MetadataContext:    []byte(contextID),
		Snappy:             cfg.StorageEncryptionSnappy,
		DisableBufferPool:  cfg.DisableKryptoPool,
	})
}
