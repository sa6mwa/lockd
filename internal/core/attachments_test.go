package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"pkt.systems/kryptograf"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

func TestAttachmentLifecycle(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	key := "attachment-life"

	lease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	payload := []byte("hello")
	res, err := svc.Attach(ctx, AttachCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
		Name:         "payload.txt",
		Body:         bytes.NewReader(payload),
	})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if res.Noop {
		t.Fatalf("expected attachment to stage")
	}
	if res.Attachment.ID == "" || res.Attachment.Name != "payload.txt" {
		t.Fatalf("unexpected attachment metadata: %+v", res.Attachment)
	}
	if res.Attachment.Size != int64(len(payload)) {
		t.Fatalf("expected attachment size %d, got %d", len(payload), res.Attachment.Size)
	}

	listRes, err := svc.ListAttachments(ctx, ListAttachmentsCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
	})
	if err != nil {
		t.Fatalf("list attachments: %v", err)
	}
	if len(listRes.Attachments) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(listRes.Attachments))
	}
	if listRes.Attachments[0].Size != int64(len(payload)) {
		t.Fatalf("expected list attachment size %d, got %d", len(payload), listRes.Attachments[0].Size)
	}

	getRes, err := svc.RetrieveAttachment(ctx, RetrieveAttachmentCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
		Selector:     AttachmentSelector{Name: "payload.txt"},
	})
	if err != nil {
		t.Fatalf("retrieve attachment: %v", err)
	}
	if getRes.Attachment.Size != int64(len(payload)) {
		t.Fatalf("expected retrieve attachment size %d, got %d", len(payload), getRes.Attachment.Size)
	}
	data, err := io.ReadAll(getRes.Reader)
	getRes.Reader.Close()
	if err != nil {
		t.Fatalf("read attachment: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("unexpected payload %q", data)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		TxnID:        lease.TxnID,
		FencingToken: lease.FencingToken,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	publicList, err := svc.ListAttachments(ctx, ListAttachmentsCommand{
		Namespace: "default",
		Key:       key,
		Public:    true,
	})
	if err != nil {
		t.Fatalf("public list: %v", err)
	}
	if len(publicList.Attachments) != 1 {
		t.Fatalf("expected public attachment, got %d", len(publicList.Attachments))
	}
}

func TestAttachmentPlaintextSizeWithCrypto(t *testing.T) {
	ctx := context.Background()
	crypto := newTestCrypto(t)
	store := memory.NewWithConfig(memory.Config{Crypto: crypto})
	t.Cleanup(func() { _ = store.Close() })
	svc := New(Config{
		Store:            store,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
		Crypto:           crypto,
	})
	key := "attachment-crypto"

	lease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	payload := bytes.Repeat([]byte("a"), 64)
	res, err := svc.Attach(ctx, AttachCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
		Name:         "payload.txt",
		Body:         bytes.NewReader(payload),
	})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if res.Attachment.Size != int64(len(payload)) {
		t.Fatalf("expected attachment size %d, got %d", len(payload), res.Attachment.Size)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		TxnID:        lease.TxnID,
		FencingToken: lease.FencingToken,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	publicList, err := svc.ListAttachments(ctx, ListAttachmentsCommand{
		Namespace: "default",
		Key:       key,
		Public:    true,
	})
	if err != nil {
		t.Fatalf("public list: %v", err)
	}
	if len(publicList.Attachments) != 1 {
		t.Fatalf("expected public attachment, got %d", len(publicList.Attachments))
	}
	if publicList.Attachments[0].Size != int64(len(payload)) {
		t.Fatalf("expected list attachment size %d, got %d", len(payload), publicList.Attachments[0].Size)
	}

	getRes, err := svc.RetrieveAttachment(ctx, RetrieveAttachmentCommand{
		Namespace: "default",
		Key:       key,
		Public:    true,
		Selector:  AttachmentSelector{Name: "payload.txt"},
	})
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if getRes.Attachment.Size != int64(len(payload)) {
		getRes.Reader.Close()
		t.Fatalf("expected retrieve attachment size %d, got %d", len(payload), getRes.Attachment.Size)
	}
	data, err := io.ReadAll(getRes.Reader)
	getRes.Reader.Close()
	if err != nil {
		t.Fatalf("read attachment: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("unexpected payload %q", data)
	}
}

func newTestCrypto(t testing.TB) *storage.Crypto {
	t.Helper()
	root := kryptograf.MustGenerateRootKey()
	kg := kryptograf.New(root)
	material, err := kg.MintDEK([]byte("test-meta-context"))
	if err != nil {
		t.Fatalf("mint metadata dek: %v", err)
	}
	metaDescriptor := material.Descriptor
	material.Zero()

	crypto, err := storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: metaDescriptor,
		MetadataContext:    []byte("test-meta-context"),
	})
	if err != nil {
		t.Fatalf("init crypto: %v", err)
	}
	return crypto
}

func TestAttachmentPreventOverwrite(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	key := "attachment-noop"

	lease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	first, err := svc.Attach(ctx, AttachCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
		Name:         "payload.txt",
		Body:         bytes.NewReader([]byte("hello")),
	})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	second, err := svc.Attach(ctx, AttachCommand{
		Namespace:        "default",
		Key:              key,
		LeaseID:          lease.LeaseID,
		FencingToken:     lease.FencingToken,
		TxnID:            lease.TxnID,
		Name:             "payload.txt",
		Body:             bytes.NewReader([]byte("ignored")),
		PreventOverwrite: true,
	})
	if err != nil {
		t.Fatalf("attach noop: %v", err)
	}
	if !second.Noop {
		t.Fatalf("expected noop on prevent overwrite")
	}
	if second.Attachment.ID != first.Attachment.ID {
		t.Fatalf("expected id reuse, got %q vs %q", second.Attachment.ID, first.Attachment.ID)
	}
}

func TestAttachmentMaxBytes(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	key := "attachment-max"

	lease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	_, err = svc.Attach(ctx, AttachCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
		Name:         "payload.txt",
		Body:         bytes.NewReader([]byte("payload")),
		MaxBytes:     3,
		MaxBytesSet:  true,
	})
	if err == nil {
		t.Fatalf("expected max bytes error")
	}
	var fail Failure
	if !errors.As(err, &fail) || fail.Code != "attachment_too_large" {
		t.Fatalf("expected attachment_too_large, got %v", err)
	}

	storageKey, err := svc.namespacedKey("default", key)
	if err != nil {
		t.Fatalf("namespaced key: %v", err)
	}
	keyComponent := relativeKey("default", storageKey)
	list, err := svc.store.ListObjects(ctx, "default", storage.ListOptions{Prefix: storage.StagedAttachmentPrefix(keyComponent, lease.TxnID)})
	if err != nil {
		t.Fatalf("list staged: %v", err)
	}
	if list != nil && len(list.Objects) > 0 {
		t.Fatalf("expected staged attachments cleared")
	}
}

func TestAttachmentRollbackClearsStaging(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	key := "attachment-rollback"

	lease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	_, err = svc.Attach(ctx, AttachCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
		Name:         "payload.txt",
		Body:         bytes.NewReader([]byte("payload")),
	})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}

	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		TxnID:        lease.TxnID,
		FencingToken: lease.FencingToken,
		Rollback:     true,
	}); err != nil {
		t.Fatalf("release rollback: %v", err)
	}

	storageKey, err := svc.namespacedKey("default", key)
	if err != nil {
		t.Fatalf("namespaced key: %v", err)
	}
	keyComponent := relativeKey("default", storageKey)
	list, err := svc.store.ListObjects(ctx, "default", storage.ListOptions{Prefix: storage.StagedAttachmentPrefix(keyComponent, lease.TxnID)})
	if err != nil {
		t.Fatalf("list staged: %v", err)
	}
	if list != nil && len(list.Objects) > 0 {
		t.Fatalf("expected staged attachments cleared")
	}
}

func TestAttachmentOverwriteReusesID(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	key := "attachment-overwrite"

	lease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	first, err := svc.Attach(ctx, AttachCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
		Name:         "payload.txt",
		Body:         bytes.NewReader([]byte("first")),
	})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		TxnID:        lease.TxnID,
		FencingToken: lease.FencingToken,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	secondLease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester-2", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	second, err := svc.Attach(ctx, AttachCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      secondLease.LeaseID,
		FencingToken: secondLease.FencingToken,
		TxnID:        secondLease.TxnID,
		Name:         "payload.txt",
		Body:         bytes.NewReader([]byte("second")),
	})
	if err != nil {
		t.Fatalf("attach overwrite: %v", err)
	}
	if second.Attachment.ID != first.Attachment.ID {
		t.Fatalf("expected id reuse, got %q vs %q", second.Attachment.ID, first.Attachment.ID)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      secondLease.LeaseID,
		TxnID:        secondLease.TxnID,
		FencingToken: secondLease.FencingToken,
	}); err != nil {
		t.Fatalf("release overwrite: %v", err)
	}

	publicGet, err := svc.RetrieveAttachment(ctx, RetrieveAttachmentCommand{
		Namespace: "default",
		Key:       key,
		Public:    true,
		Selector:  AttachmentSelector{Name: "payload.txt"},
	})
	if err != nil {
		t.Fatalf("public retrieve: %v", err)
	}
	data, err := io.ReadAll(publicGet.Reader)
	publicGet.Reader.Close()
	if err != nil {
		t.Fatalf("read public: %v", err)
	}
	if string(data) != "second" {
		t.Fatalf("expected overwritten payload, got %q", data)
	}
}

func TestAttachmentDeleteAndDeleteAll(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	key := "attachment-delete"

	lease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := svc.Attach(ctx, AttachCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
		Name:         "alpha.txt",
		Body:         bytes.NewReader([]byte("alpha")),
	}); err != nil {
		t.Fatalf("attach alpha: %v", err)
	}
	if _, err := svc.Attach(ctx, AttachCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		FencingToken: lease.FencingToken,
		TxnID:        lease.TxnID,
		Name:         "bravo.txt",
		Body:         bytes.NewReader([]byte("bravo")),
	}); err != nil {
		t.Fatalf("attach bravo: %v", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      lease.LeaseID,
		TxnID:        lease.TxnID,
		FencingToken: lease.FencingToken,
	}); err != nil {
		t.Fatalf("release: %v", err)
	}

	deleteLease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester-2", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire delete: %v", err)
	}
	if _, err := svc.DeleteAttachment(ctx, DeleteAttachmentCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      deleteLease.LeaseID,
		FencingToken: deleteLease.FencingToken,
		TxnID:        deleteLease.TxnID,
		Selector:     AttachmentSelector{Name: "alpha.txt"},
	}); err != nil {
		t.Fatalf("delete attachment: %v", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      deleteLease.LeaseID,
		TxnID:        deleteLease.TxnID,
		FencingToken: deleteLease.FencingToken,
	}); err != nil {
		t.Fatalf("release delete: %v", err)
	}

	publicList, err := svc.ListAttachments(ctx, ListAttachmentsCommand{Namespace: "default", Key: key, Public: true})
	if err != nil {
		t.Fatalf("public list: %v", err)
	}
	if len(publicList.Attachments) != 1 || publicList.Attachments[0].Name != "bravo.txt" {
		t.Fatalf("expected bravo remaining, got %+v", publicList.Attachments)
	}

	clearLease, err := svc.Acquire(ctx, AcquireCommand{Namespace: "default", Key: key, Owner: "tester-3", TTLSeconds: 30})
	if err != nil {
		t.Fatalf("acquire clear: %v", err)
	}
	if _, err := svc.DeleteAllAttachments(ctx, DeleteAllAttachmentsCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      clearLease.LeaseID,
		FencingToken: clearLease.FencingToken,
		TxnID:        clearLease.TxnID,
	}); err != nil {
		t.Fatalf("delete all: %v", err)
	}
	if _, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      clearLease.LeaseID,
		TxnID:        clearLease.TxnID,
		FencingToken: clearLease.FencingToken,
	}); err != nil {
		t.Fatalf("release clear: %v", err)
	}

	finalList, err := svc.ListAttachments(ctx, ListAttachmentsCommand{Namespace: "default", Key: key, Public: true})
	if err != nil {
		t.Fatalf("final list: %v", err)
	}
	if len(finalList.Attachments) != 0 {
		t.Fatalf("expected no attachments, got %+v", finalList.Attachments)
	}
}
