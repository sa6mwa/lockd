package storagecheck

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"pkt.systems/kryptograf"

	lockdproto "pkt.systems/lockd/internal/proto"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

func TestVerifyMetaStateDecryptionDetectsCorruption(t *testing.T) {
	ctx := context.Background()
	crypto := mustNewDiagnosticsCrypto(t, false)
	base := memory.NewWithConfig(memory.Config{Crypto: crypto})
	t.Cleanup(func() { _ = base.Close() })

	const metaKey = "verify-meta-state"
	stateObjectKey := metaKey

	const namespace = diagnosticsNamespace
	meta := &storage.Meta{Version: 1}
	metaETag, err := base.StoreMeta(ctx, namespace, metaKey, meta, "")
	if err != nil {
		t.Fatalf("store meta: %v", err)
	}
	stateRes, err := base.WriteState(ctx, namespace, metaKey, bytes.NewReader([]byte("{}")), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	if stateRes != nil && len(stateRes.Descriptor) > 0 {
		meta.StateDescriptor = append([]byte(nil), stateRes.Descriptor...)
		meta.Version++
		if _, err := base.StoreMeta(ctx, namespace, metaKey, meta, metaETag); err != nil {
			t.Fatalf("update meta with descriptor: %v", err)
		}
	}
	reader, _, err := base.ReadState(ctx, namespace, metaKey)
	if err != nil {
		t.Fatalf("baseline read state: %v", err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("baseline consume state: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("baseline close reader: %v", err)
	}

	// Baseline should succeed.
	if err := verifyMetaStateDecryption(ctx, base, crypto); err != nil {
		t.Fatalf("verification should succeed: %v", err)
	}

	var tampered bool
	wrapper := &backendWrapper{
		Backend: base,
		readStateHook: func(ctx context.Context, namespace, key string, reader io.ReadCloser, info *storage.StateInfo) (io.ReadCloser, *storage.StateInfo, error) {
			if tampered {
				return reader, info, nil
			}
			if namespace == diagnosticsNamespace && key == stateObjectKey {
				tampered = true
				infoCopy := *info
				infoCopy.Descriptor = []byte{0xFF}
				return reader, &infoCopy, nil
			}
			return reader, info, nil
		},
	}

	if err := verifyMetaStateDecryption(ctx, wrapper, crypto); err == nil {
		t.Fatalf("expected corrupted descriptor to surface error")
	}
}

func TestVerifyQueueEncryptionDetectsMissingDescriptor(t *testing.T) {
	ctx := context.Background()
	crypto := mustNewDiagnosticsCrypto(t, false)
	base := memory.NewWithConfig(memory.Config{Crypto: crypto})
	t.Cleanup(func() { _ = base.Close() })

	// Baseline should succeed.
	if err := verifyQueueEncryption(ctx, base, crypto); err != nil {
		t.Fatalf("queue verification should succeed: %v", err)
	}

	var tampered bool
	wrapper := &backendWrapper{
		Backend: base,
		getObjectHook: func(ctx context.Context, namespace, key string, reader io.ReadCloser, info *storage.ObjectInfo) (io.ReadCloser, *storage.ObjectInfo, error) {
			if tampered {
				return reader, info, nil
			}
			if namespace == diagnosticsNamespace && strings.Contains(key, "/msg/") && strings.HasSuffix(key, ".bin") {
				tampered = true
				infoCopy := *info
				infoCopy.Descriptor = nil
				return reader, &infoCopy, nil
			}
			return reader, info, nil
		},
		putObjectHook: func(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (io.Reader, storage.PutObjectOptions, error) {
			data, err := io.ReadAll(body)
			if err != nil {
				return nil, opts, err
			}
			switch {
			case namespace == diagnosticsNamespace && strings.Contains(key, "/msg/") && strings.HasSuffix(key, ".pb"):
				var meta lockdproto.QueueMessageMeta
				if err := proto.Unmarshal(data, &meta); err != nil {
					return nil, opts, err
				}
				meta.MetaDescriptor = nil
				meta.PayloadDescriptor = nil
				encoded, err := proto.Marshal(&meta)
				if err != nil {
					return nil, opts, err
				}
				opts.Descriptor = nil
				return bytes.NewReader(encoded), opts, nil
			case namespace == diagnosticsNamespace && strings.Contains(key, "/msg/") && strings.HasSuffix(key, ".bin"):
				opts.Descriptor = nil
				return bytes.NewReader(data), opts, nil
			default:
				return bytes.NewReader(data), opts, nil
			}
		},
	}
	if err := verifyQueueEncryption(ctx, wrapper, crypto); err == nil {
		t.Fatalf("expected missing descriptor error")
	}
}

type backendWrapper struct {
	storage.Backend
	readStateHook func(context.Context, string, string, io.ReadCloser, *storage.StateInfo) (io.ReadCloser, *storage.StateInfo, error)
	getObjectHook func(context.Context, string, string, io.ReadCloser, *storage.ObjectInfo) (io.ReadCloser, *storage.ObjectInfo, error)
	putObjectHook func(context.Context, string, string, io.Reader, storage.PutObjectOptions) (io.Reader, storage.PutObjectOptions, error)
}

func (b *backendWrapper) ReadState(ctx context.Context, namespace, key string) (io.ReadCloser, *storage.StateInfo, error) {
	reader, info, err := b.Backend.ReadState(ctx, namespace, key)
	if err != nil {
		return reader, info, err
	}
	if b.readStateHook == nil {
		return reader, info, nil
	}
	return b.readStateHook(ctx, namespace, key, reader, info)
}

func (b *backendWrapper) GetObject(ctx context.Context, namespace, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	reader, info, err := b.Backend.GetObject(ctx, namespace, key)
	if err != nil {
		return reader, info, err
	}
	if b.getObjectHook == nil {
		return reader, info, nil
	}
	return b.getObjectHook(ctx, namespace, key, reader, info)
}

func (b *backendWrapper) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	if b.putObjectHook != nil {
		var err error
		body, opts, err = b.putObjectHook(ctx, namespace, key, body, opts)
		if err != nil {
			return nil, err
		}
	}
	return b.Backend.PutObject(ctx, namespace, key, body, opts)
}

func mustNewDiagnosticsCrypto(t *testing.T, snappy bool) *storage.Crypto {
	t.Helper()
	root := kryptograf.MustGenerateRootKey()
	kg := kryptograf.New(root)
	material, err := kg.MintDEK([]byte("diag-meta"))
	if err != nil {
		t.Fatalf("mint metadata material: %v", err)
	}
	desc := material.Descriptor
	material.Zero()
	crypto, err := storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: desc,
		MetadataContext:    []byte("diag-meta"),
		Snappy:             snappy,
	})
	if err != nil {
		t.Fatalf("init crypto: %v", err)
	}
	return crypto
}
