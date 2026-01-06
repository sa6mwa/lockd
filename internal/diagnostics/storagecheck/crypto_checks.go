package storagecheck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"pkt.systems/kryptograf/keymgmt"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
)

// prepareCryptoForVerify ensures kryptograf material is available and constructs
// a storage.Crypto helper when encryption is enabled.
func prepareCryptoForVerify(cfg lockd.Config) (*storage.Crypto, lockd.Config, error) {
	if !cfg.StorageEncryptionEnabled() {
		return nil, cfg, nil
	}

	out := cfg
	var (
		root         keymgmt.RootKey
		desc         keymgmt.Descriptor
		contextID    string
		bundleSource string
	)
	if out.MetadataRootKey == (keymgmt.RootKey{}) ||
		out.MetadataDescriptor == (keymgmt.Descriptor{}) ||
		strings.TrimSpace(out.MetadataContext) == "" {
		var (
			bundle *tlsutil.Bundle
			err    error
		)
		switch {
		case len(cfg.BundlePEM) > 0:
			bundleSource = "<inline>"
			bundle, err = tlsutil.LoadBundleFromBytes(cfg.BundlePEM)
		default:
			bundleSource = cfg.BundlePath
			bundle, err = tlsutil.LoadBundle(cfg.BundlePath, cfg.DenylistPath)
		}
		if err != nil {
			return nil, cfg, fmt.Errorf("storage verify: load bundle %q: %w", bundleSource, err)
		}
		caID, err := cryptoutil.CACertificateID(bundle.CACertPEM)
		if err != nil {
			return nil, cfg, fmt.Errorf("storage verify: derive ca id: %w", err)
		}
		root = bundle.MetadataRootKey
		desc = bundle.MetadataDescriptor
		contextID = caID
	} else {
		root = out.MetadataRootKey
		desc = out.MetadataDescriptor
		contextID = out.MetadataContext
	}
	if root == (keymgmt.RootKey{}) {
		return nil, cfg, fmt.Errorf("storage verify: bundle missing kryptograf root key (reissue with 'lockd auth new server')")
	}
	if desc == (keymgmt.Descriptor{}) {
		return nil, cfg, fmt.Errorf("storage verify: bundle missing metadata descriptor (reissue with 'lockd auth new server')")
	}
	if strings.TrimSpace(contextID) == "" {
		return nil, cfg, fmt.Errorf("storage verify: kryptograf metadata context required")
	}
	out.MetadataRootKey = root
	out.MetadataDescriptor = desc
	out.MetadataContext = contextID

	crypto, err := storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: desc,
		MetadataContext:    []byte(contextID),
		Snappy:             cfg.StorageEncryptionSnappy,
	})
	if err != nil {
		return nil, cfg, fmt.Errorf("storage verify: init crypto: %w", err)
	}
	return crypto, out, nil
}

func joinNamespace(namespace, key string) string {
	if strings.TrimSpace(namespace) == "" {
		return key
	}
	return namespace + "/" + strings.TrimPrefix(key, "/")
}

func verifyMetaStateDecryption(ctx context.Context, backend storage.Backend, crypto *storage.Crypto) error {
	if crypto == nil || !crypto.Enabled() {
		return nil
	}
	namespace := namespaces.Default
	keys, err := backend.ListMetaKeys(ctx, namespace)
	if err != nil {
		return fmt.Errorf("list meta keys: %w", err)
	}
	const sampleTarget = 2
	withDescriptors := 0
	succeeded := 0
	for _, key := range keys {
		if succeeded >= sampleTarget {
			break
		}
		if strings.Contains(key, "/q/") {
			continue
		}
		fullKey := joinNamespace(namespace, key)
		metaRes, err := backend.LoadMeta(ctx, namespace, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return fmt.Errorf("load meta %q: %w", fullKey, err)
		}
		meta := metaRes.Meta
		if meta == nil {
			continue
		}
		var metaDescriptor []byte
		if len(meta.StateDescriptor) > 0 {
			metaDescriptor = append([]byte(nil), meta.StateDescriptor...)
		}
		stateCtx := ctx
		if len(metaDescriptor) > 0 {
			stateCtx = storage.ContextWithStateDescriptor(stateCtx, metaDescriptor)
		}
		if meta.StatePlaintextBytes > 0 {
			stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
		}
		stateRes, err := backend.ReadState(stateCtx, namespace, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return fmt.Errorf("read state %q: %w", fullKey, err)
		}
		reader := stateRes.Reader
		info := stateRes.Info
		var descriptor []byte
		if info != nil && len(info.Descriptor) > 0 {
			descriptor = append([]byte(nil), info.Descriptor...)
		} else if len(metaDescriptor) > 0 {
			descriptor = append([]byte(nil), metaDescriptor...)
		}
		if len(descriptor) == 0 {
			_ = reader.Close()
			continue
		}
		withDescriptors++
		mat, err := crypto.MaterialFromDescriptor(storage.StateObjectContext(fullKey), descriptor)
		if err != nil {
			_ = reader.Close()
			return fmt.Errorf("reconstruct state material %q: %w", fullKey, err)
		}
		mat.Zero()
		if _, err := io.Copy(io.Discard, reader); err != nil {
			_ = reader.Close()
			return fmt.Errorf("consume state %q: %w", fullKey, err)
		}
		if err := reader.Close(); err != nil {
			return fmt.Errorf("close state %q: %w", fullKey, err)
		}
		succeeded++
	}
	if withDescriptors == 0 {
		return syntheticStateRoundTrip(ctx, backend, crypto)
	}
	if succeeded >= sampleTarget || succeeded >= withDescriptors {
		return nil
	}
	return fmt.Errorf("verified %d/%d state objects (need at least %d)", succeeded, withDescriptors, sampleTarget)
}

func syntheticStateRoundTrip(ctx context.Context, backend storage.Backend, crypto *storage.Crypto) error {
	namespace := namespaces.Default
	key := path.Join(diagnosticsKeyPrefix, uuidv7.NewString())
	meta := &storage.Meta{
		Version:       1,
		UpdatedAtUnix: time.Now().Unix(),
	}
	meta.MarkQueryExcluded()
	etag, err := backend.StoreMeta(ctx, namespace, key, meta, "")
	if err != nil {
		return fmt.Errorf("store diagnostics meta %q: %w", key, err)
	}
	defer func() {
		_ = backend.Remove(ctx, namespace, key, "")
		_ = backend.DeleteMeta(ctx, namespace, key, "")
	}()
	stateRes, err := backend.WriteState(ctx, namespace, key, strings.NewReader("{}"), storage.PutStateOptions{})
	if err != nil {
		return fmt.Errorf("write diagnostics state %q: %w", joinNamespace(namespace, key), err)
	}
	if stateRes != nil && len(stateRes.Descriptor) > 0 {
		meta.StateDescriptor = append([]byte(nil), stateRes.Descriptor...)
	}
	if stateRes != nil {
		meta.StatePlaintextBytes = stateRes.BytesWritten
	}
	meta.Version++
	meta.UpdatedAtUnix = time.Now().Unix()
	if _, err := backend.StoreMeta(ctx, namespace, key, meta, etag); err != nil {
		return fmt.Errorf("update diagnostics meta %q: %w", joinNamespace(namespace, key), err)
	}
	metaLoadedRes, err := backend.LoadMeta(ctx, namespace, key)
	if err != nil {
		return fmt.Errorf("reload diagnostics meta %q: %w", joinNamespace(namespace, key), err)
	}
	metaLoaded := metaLoadedRes.Meta
	if metaLoaded == nil || len(metaLoaded.StateDescriptor) == 0 {
		return fmt.Errorf("diagnostics meta %q missing state descriptor", joinNamespace(namespace, key))
	}
	stateCtx := ctx
	if len(metaLoaded.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, metaLoaded.StateDescriptor)
	}
	if metaLoaded.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, metaLoaded.StatePlaintextBytes)
	}
	readRes, err := backend.ReadState(stateCtx, namespace, key)
	if err != nil {
		return fmt.Errorf("read diagnostics state %q: %w", joinNamespace(namespace, key), err)
	}
	reader := readRes.Reader
	info := readRes.Info
	var descriptor []byte
	if info != nil && len(info.Descriptor) > 0 {
		descriptor = append([]byte(nil), info.Descriptor...)
	} else if len(metaLoaded.StateDescriptor) > 0 {
		descriptor = append([]byte(nil), metaLoaded.StateDescriptor...)
	}
	if crypto != nil && crypto.Enabled() {
		contextID := storage.StateObjectContext(joinNamespace(namespace, key))
		mat, err := crypto.MaterialFromDescriptor(contextID, descriptor)
		if err != nil {
			_ = reader.Close()
			return fmt.Errorf("reconstruct diagnostics state material %q: %w", joinNamespace(namespace, key), err)
		}
		mat.Zero()
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		_ = reader.Close()
		return fmt.Errorf("consume diagnostics state %q: %w", joinNamespace(namespace, key), err)
	}
	return reader.Close()
}

func verifyQueueEncryption(ctx context.Context, backend storage.Backend, crypto *storage.Crypto) error {
	if crypto == nil || !crypto.Enabled() {
		return nil
	}
	svc, err := queue.New(backend, clock.Real{}, queue.Config{Crypto: crypto})
	if err != nil {
		return fmt.Errorf("queue diagnostics: init service: %w", err)
	}
	queueName := "lockd-diagnostics-" + uuidv7.NewString()
	msg, err := svc.Enqueue(ctx, diagnosticsNamespace, queueName, strings.NewReader(`{"diagnostic":true}`), queue.EnqueueOptions{})
	if err != nil {
		return fmt.Errorf("queue diagnostics: enqueue: %w", err)
	}
	msgRes, err := svc.GetMessage(ctx, diagnosticsNamespace, queueName, msg.ID)
	if err != nil {
		return fmt.Errorf("queue diagnostics: load message: %w", err)
	}
	doc := msgRes.Document
	etag := msgRes.ETag

	if err := decryptQueueObject(ctx, backend, crypto, msg.Namespace, msg.MetadataObject, storage.QueueMetaContext(msg.Namespace, msg.MetadataObject), doc.MetaDescriptor); err != nil {
		return fmt.Errorf("queue diagnostics: decrypt metadata: %w", err)
	}
	if err := decryptQueueObject(ctx, backend, crypto, msg.Namespace, msg.PayloadObject, storage.QueuePayloadContext(msg.Namespace, msg.PayloadObject), doc.PayloadDescriptor); err != nil {
		return fmt.Errorf("queue diagnostics: decrypt payload: %w", err)
	}

	if err := svc.DeleteMessage(ctx, diagnosticsNamespace, queueName, msg.ID, etag); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("queue diagnostics: cleanup message: %w", err)
	}
	return nil
}

func decryptQueueObject(ctx context.Context, backend storage.Backend, crypto *storage.Crypto, namespace, relKey, context string, fallbackDescriptor []byte) error {
	obj, err := backend.GetObject(ctx, namespace, relKey)
	if err != nil {
		return err
	}
	reader := obj.Reader
	info := obj.Info
	var descriptor []byte
	if info != nil && len(info.Descriptor) > 0 {
		descriptor = append([]byte(nil), info.Descriptor...)
	} else if len(fallbackDescriptor) > 0 {
		descriptor = append([]byte(nil), fallbackDescriptor...)
	}
	if len(descriptor) == 0 {
		_ = reader.Close()
		return fmt.Errorf("missing descriptor for %s", joinNamespace(namespace, relKey))
	}
	mat, err := crypto.MaterialFromDescriptor(context, descriptor)
	if err != nil {
		_ = reader.Close()
		return err
	}
	mat.Zero()
	if _, err := io.Copy(io.Discard, reader); err != nil {
		_ = reader.Close()
		return fmt.Errorf("consume %s: %w", joinNamespace(namespace, relKey), err)
	}
	return reader.Close()
}

const (
	diagnosticsCleanupGuard = 30 * time.Second
	diagnosticsNamespace    = namespaces.Default
	diagnosticsKeyPrefix    = "lockd-diagnostics"
)

func cleanupSyntheticDiagnostics(ctx context.Context, backend storage.Backend) error {
	keys, err := backend.ListMetaKeys(ctx, diagnosticsNamespace)
	if err != nil {
		return fmt.Errorf("list meta keys: %w", err)
	}
	now := time.Now()
	for _, key := range keys {
		if !strings.HasPrefix(key, diagnosticsKeyPrefix+"/") {
			continue
		}
		fullKey := joinNamespace(diagnosticsNamespace, key)
		if shouldDeferDiagnosticsCleanup(fullKey, now) {
			continue
		}
		if err := backend.Remove(ctx, diagnosticsNamespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("remove diagnostics state %q: %w", key, err)
		}
		if err := backend.DeleteMeta(ctx, diagnosticsNamespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("remove diagnostics meta %q: %w", key, err)
		}
	}
	return nil
}

func shouldDeferDiagnosticsCleanup(key string, now time.Time) bool {
	idx := strings.LastIndexByte(key, '/')
	if idx >= 0 && idx+1 < len(key) {
		key = key[idx+1:]
	}
	ts, ok := uuidv7.ParseTime(key)
	if !ok {
		return false
	}
	return now.Sub(ts) < diagnosticsCleanupGuard
}
