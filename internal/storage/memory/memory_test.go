package memory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
)

func TestStoreMetaCAS(t *testing.T) {
	store := New()
	ctx := context.Background()

	meta := &storage.Meta{Version: 1}
	namespace := namespaces.Default
	key := "alpha"
	if _, err := store.StoreMeta(ctx, namespace, key, meta, ""); err != nil {
		t.Fatalf("store meta create: %v", err)
	}
	metaRes, err := store.LoadMeta(ctx, namespace, key)
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if metaRes.Meta.Version != 1 {
		t.Fatalf("expected version 1, got %d", metaRes.Meta.Version)
	}
	meta.Version = 2
	if _, err := store.StoreMeta(ctx, namespace, key, meta, metaRes.ETag); err != nil {
		t.Fatalf("store meta cas: %v", err)
	}
	if _, err := store.StoreMeta(ctx, namespace, key, meta, "wrong"); !errors.Is(err, storage.ErrCASMismatch) {
		t.Fatalf("expected cas mismatch, got %v", err)
	}
	keys, err := store.ListMetaKeys(ctx, namespace)
	if err != nil {
		t.Fatalf("list meta keys: %v", err)
	}
	if len(keys) != 1 || keys[0] != key {
		t.Fatalf("unexpected keys: %v", keys)
	}
}

func TestWriteStateCAS(t *testing.T) {
	store := New()
	ctx := context.Background()

	body := bytes.NewBufferString(`{"cursor":1}`)
	namespace := namespaces.Default
	stateKey := "alpha"
	res, err := store.WriteState(ctx, namespace, stateKey, body, storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	if res.BytesWritten == 0 || res.NewETag == "" {
		t.Fatalf("expected write metadata, got %+v", res)
	}

	readRes, err := store.ReadState(ctx, namespace, stateKey)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	defer readRes.Reader.Close()
	if readRes.Info.ETag != res.NewETag {
		t.Fatalf("etag mismatch: %s vs %s", readRes.Info.ETag, res.NewETag)
	}

	newBody := bytes.NewBufferString(`{"cursor":2}`)
	if _, err := store.WriteState(ctx, namespace, stateKey, newBody, storage.PutStateOptions{ExpectedETag: "wrong"}); !errors.Is(err, storage.ErrCASMismatch) {
		t.Fatalf("expected cas mismatch, got %v", err)
	}
}

func TestListObjectsPrefixAndStartAfter(t *testing.T) {
	store := New()
	ctx := context.Background()
	namespace := namespaces.Default
	keys := []string{
		"meta/alpha.pb",
		"q/a/msg/001.pb",
		"q/a/msg/002.pb",
		"q/a/msg/003.pb",
		"q/b/msg/001.pb",
	}
	for _, key := range keys {
		if _, err := store.PutObject(ctx, namespace, key, bytes.NewBufferString("body"), storage.PutObjectOptions{}); err != nil {
			t.Fatalf("put object %s: %v", key, err)
		}
	}

	result, err := store.ListObjects(ctx, namespace, storage.ListOptions{Prefix: "q/a/msg/", Limit: 2})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}
	if len(result.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(result.Objects))
	}
	if result.Objects[0].Key != "q/a/msg/001.pb" || result.Objects[1].Key != "q/a/msg/002.pb" {
		t.Fatalf("unexpected keys: %#v", result.Objects)
	}
	if !result.Truncated || result.NextStartAfter != "q/a/msg/002.pb" {
		t.Fatalf("unexpected truncation metadata: %+v", result)
	}

	result2, err := store.ListObjects(ctx, namespace, storage.ListOptions{Prefix: "q/a/msg/", StartAfter: "q/a/msg/002.pb", Limit: 2})
	if err != nil {
		t.Fatalf("list objects start after: %v", err)
	}
	if len(result2.Objects) != 1 || result2.Objects[0].Key != "q/a/msg/003.pb" {
		t.Fatalf("unexpected keys after start after: %#v", result2.Objects)
	}
	if result2.Truncated {
		t.Fatalf("did not expect truncation after consuming tail: %+v", result2)
	}

	result3, err := store.ListObjects(ctx, namespace, storage.ListOptions{Prefix: "q/a/msg/", StartAfter: "meta/alpha.pb", Limit: 2})
	if err != nil {
		t.Fatalf("list objects start after meta: %v", err)
	}
	if len(result3.Objects) == 0 || result3.Objects[0].Key != "q/a/msg/001.pb" {
		t.Fatalf("expected to resume at first queue key, got %#v", result3.Objects)
	}
}

func TestMemoryBackendHashEncrypted(t *testing.T) {
	root := kryptograf.MustGenerateRootKey()
	kg := kryptograf.New(root)
	material, err := kg.MintDEK([]byte("mem-backend-hash"))
	if err != nil {
		t.Fatalf("mint metadata dek: %v", err)
	}
	crypto, err := storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: material.Descriptor,
		MetadataContext:    []byte("mem-backend-hash"),
	})
	if err != nil {
		t.Fatalf("new crypto: %v", err)
	}

	store := NewWithConfig(Config{QueueWatch: false, Crypto: crypto})
	ctx := context.Background()
	hash, err := store.BackendHash(ctx)
	if err != nil {
		t.Fatalf("backend hash: %v", err)
	}

	obj, err := store.GetObject(ctx, ".lockd", "backend-id")
	if err != nil {
		t.Fatalf("get backend-id: %v", err)
	}
	defer obj.Reader.Close()
	if obj.Info == nil || obj.Info.ContentType != storage.ContentTypeTextEncrypted {
		got := ""
		if obj.Info != nil {
			got = obj.Info.ContentType
		}
		t.Fatalf("content-type = %q want %q", got, storage.ContentTypeTextEncrypted)
	}
	cipher, err := io.ReadAll(obj.Reader)
	if err != nil {
		t.Fatalf("read backend-id: %v", err)
	}
	plain, err := crypto.DecryptMetadata(cipher)
	if err != nil {
		t.Fatalf("decrypt backend-id: %v", err)
	}
	if strings.TrimSpace(string(plain)) != hash {
		t.Fatalf("backend-id mismatch")
	}
}
