package disk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"path/filepath"
	"testing"

	"pkt.systems/kryptograf"
	"pkt.systems/lockd/internal/storage"
)

func TestDiskPromoteStagedStatePreservesPayload(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "store")
	crypto := mustNewDiskTestCrypto(t)
	store, err := New(Config{Root: root, Crypto: crypto})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	key := "orders/1"
	ctx := storage.ContextWithStateObjectContext(context.Background(), storage.StateObjectContext(path.Join(testNamespace, key)))
	txnID := "txn-1"
	payload := []byte(`{"hello":"world"}`)

	if _, err := store.StageState(ctx, testNamespace, key, txnID, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
		t.Fatalf("stage state: %v", err)
	}
	staged, err := store.ReadState(ctx, testNamespace, store.stagingKey(key, txnID))
	if err != nil {
		t.Fatalf("read staged state: %v", err)
	}
	stagedBody, err := io.ReadAll(staged.Reader)
	_ = staged.Reader.Close()
	if err != nil {
		t.Fatalf("read staged body: %v", err)
	}
	if !bytes.Equal(stagedBody, payload) {
		t.Fatalf("staged payload mismatch")
	}
	if _, err := store.PromoteStagedState(ctx, testNamespace, key, txnID, storage.PromoteStagedOptions{}); err != nil {
		t.Fatalf("promote staged: %v", err)
	}

	state, err := store.ReadState(ctx, testNamespace, key)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	defer state.Reader.Close()
	body, err := io.ReadAll(state.Reader)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("payload mismatch")
	}

	if _, err := store.ReadState(ctx, testNamespace, store.stagingKey(key, txnID)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected staged state cleanup, got %v", err)
	}
}

func TestDiskPromoteStagedStatePreservesPayloadLarge(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "store")
	crypto := mustNewDiskTestCrypto(t)
	store, err := New(Config{Root: root, Crypto: crypto})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	key := "orders/large"
	ctx := storage.ContextWithStateObjectContext(context.Background(), storage.StateObjectContext(path.Join(testNamespace, key)))
	txnID := "txn-large"
	payload := bytes.Repeat([]byte("L"), 2<<20)

	if _, err := store.StageState(ctx, testNamespace, key, txnID, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
		t.Fatalf("stage state: %v", err)
	}
	if _, err := store.PromoteStagedState(ctx, testNamespace, key, txnID, storage.PromoteStagedOptions{}); err != nil {
		t.Fatalf("promote staged: %v", err)
	}

	state, err := store.ReadState(ctx, testNamespace, key)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	defer state.Reader.Close()
	body, err := io.ReadAll(state.Reader)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("payload mismatch")
	}
}

func mustNewDiskTestCrypto(t *testing.T) *storage.Crypto {
	t.Helper()
	root := kryptograf.MustGenerateRootKey()
	kg := kryptograf.New(root)
	material, err := kg.MintDEK([]byte("disk-test-meta"))
	if err != nil {
		t.Fatalf("mint metadata material: %v", err)
	}
	desc := material.Descriptor
	material.Zero()
	crypto, err := storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: desc,
		MetadataContext:    []byte("disk-test-meta"),
	})
	if err != nil {
		t.Fatalf("init crypto: %v", err)
	}
	return crypto
}
